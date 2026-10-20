"""Native DuckDB fidelity, offline guards and atomic failure regressions."""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys

import pytest
from typer.testing import CliRunner

from undatum.cmds.repacker import Repacker
from undatum.common.errors import DependencyError, UndatumError, ValidationError
from undatum.common.repack_types import RepackBudget, RepackError, RepackOptions, ResourceLimitError

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")


def database(path, *, fragmented=False, empty=False):
    with duckdb.connect(str(path)) as connection:
        if empty:
            return
        connection.execute('CREATE SCHEMA "data\'s"')
        connection.execute(
            "CREATE TABLE \"data's\".kept (i INTEGER PRIMARY KEY, label VARCHAR DEFAULT 'keep', "
            "precise DECIMAL(38,8), stamp TIMESTAMP_NS, payload BLOB, "
            "nested STRUCT(labels VARCHAR[], value DOUBLE), mapping MAP(VARCHAR, DOUBLE))"
        )
        connection.execute(
            'INSERT INTO "data\'s".kept VALUES '
            "(1, 'keep  text', 123456789012345678901.12345678, "
            "TIMESTAMP_NS '2020-01-01 00:00:00.123456789', '\\x00\\xFF', "
            "{'labels':['keep', NULL], 'value':'-0.0'::DOUBLE}, map(['nan'], ['NaN'::DOUBLE])), "
            "(2, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        connection.execute('CREATE INDEX label_index ON "data\'s".kept(label)')
        connection.execute("COMMENT ON TABLE \"data's\".kept IS 'table comment'")
        connection.execute("COMMENT ON COLUMN \"data's\".kept.label IS 'column comment'")
        connection.execute("CREATE TABLE empty_table (i INTEGER, value VARCHAR)")
        connection.execute(
            'CREATE TABLE "data\'s".child (id INTEGER PRIMARY KEY CHECK(id > 0), parent INTEGER)'
        )
        connection.execute('INSERT INTO "data\'s".child VALUES (1, 1)')
        if fragmented:
            connection.execute(
                "CREATE TABLE junk AS SELECT range AS id, "
                "repeat(md5(range::VARCHAR), 32) AS value FROM range(20000)"
            )
            connection.execute("CHECKPOINT")
            connection.execute("DELETE FROM junk WHERE id > 0")
            connection.execute("CHECKPOINT")


def repack(source, output=None, **options):
    return Repacker().repack(
        str(source), str(output) if output is not None else None, {"progress": False, **options}
    )


def assert_clean(directory):
    assert not list(directory.glob(".undatum-tmp-*"))


def contents(path):
    with duckdb.connect(str(path), read_only=True) as connection:
        return (
            connection.execute(
                "SELECT i, label, precise::VARCHAR, stamp::VARCHAR, payload, "
                'nested::VARCHAR, mapping::VARCHAR FROM "data\'s".kept'
            ).fetchall(),
            connection.execute(
                "SELECT schema_name, table_name, sql, comment, tags FROM duckdb_tables() "
                "WHERE NOT internal ORDER BY schema_name, table_name"
            ).fetchall(),
            connection.execute(
                "SELECT schema_name, table_name, column_name, comment, column_default "
                "FROM duckdb_columns() WHERE NOT internal ORDER BY schema_name, table_name, column_index"
            ).fetchall(),
            connection.execute('SELECT * FROM "data\'s".child').fetchall(),
        )


@pytest.mark.parametrize("supervised", [False, True])
def test_native_compaction_preserves_catalog_and_values(tmp_path, supervised):
    source, output = tmp_path / "source.duckdb", tmp_path / "out.duckdb"
    database(source, fragmented=True)
    expected, original = contents(source), source.read_bytes()
    os.chmod(source, 0o640)
    state = source.stat()
    result = repack(source, output, **({"timeout": 30} if supervised else {}))
    assert result["status"] == "replaced" and result["changed"]
    assert result["candidate_size"] < len(original)
    assert result["rows"] == 4
    assert contents(output) == expected
    assert source.read_bytes() == original
    assert output.stat().st_mode == state.st_mode
    assert output.stat().st_mtime_ns == state.st_mtime_ns
    assert result["effective_settings"] == {
        "codec": "auto",
        "profile": "max",
        "level": None,
        "threads": 1,
        "applied": True,
        "strategy": "copy_database",
    }
    assert result["resources"]["decoded_bytes_processed"] >= 3 * result["decoded_bytes"]
    assert result["verification"]["passed"]
    assert_clean(tmp_path)


@pytest.mark.parametrize("name", ["source.db", "source.ddb", "source.DUCKDB", "source"])
def test_header_detection_and_explicit_same_path(tmp_path, name):
    source = tmp_path / name
    database(source)
    expected = contents(source)
    result = repack(
        source, source, allow_grow=True, compression="auto", profile="fast", batch_size=1
    )
    assert result["mode"] == "builtin" and result["compression"] == "auto"
    assert contents(source) == expected
    assert_clean(tmp_path)


@pytest.mark.parametrize("distinct", [False, True])
def test_empty_database_no_benefit(tmp_path, distinct):
    source = tmp_path / "empty.duckdb"
    database(source, empty=True)
    original = source.read_bytes()
    output = tmp_path / "out.db" if distinct else source
    result = repack(source, output)
    assert result["status"] == "unchanged" and result["reason_code"] == "no_benefit"
    assert result["published"] == distinct and not result["changed"]
    assert result["rows"] == 0 and output.read_bytes() == original
    assert_clean(tmp_path)


@pytest.mark.parametrize("options", [{"dry_run": True}, {"min_savings": 100}])
def test_dry_run_and_savings_preserve_prior_files(tmp_path, options):
    source, output = tmp_path / "source.duckdb", tmp_path / "out.duckdb"
    database(source)
    original = source.read_bytes()
    output.write_bytes(b"prior destination")
    result = repack(source, output, **options)
    assert source.read_bytes() == original
    assert result["verification"]["passed"]
    if options.get("dry_run"):
        assert output.read_bytes() == b"prior destination" and not result["published"]
    else:
        assert output.read_bytes() == original and not result["changed"]
        assert result["reason_code"] == "below_min_savings"
    assert_clean(tmp_path)


@pytest.mark.parametrize("fault", ["value", "nested_float", "timestamp", "index", "rows"])
def test_changed_candidate_is_not_published(tmp_path, monkeypatch, fault):
    import undatum.common.repack_duckdb as adapter

    source, output = tmp_path / "source.duckdb", tmp_path / "out.duckdb"
    database(source)
    original = source.read_bytes()
    output.write_bytes(b"prior")
    real = adapter._rewrite

    def corrupt(source, target, budget):
        real(source, target, budget)
        with duckdb.connect(target) as connection:
            connection.execute(
                {
                    "value": "UPDATE \"data's\".kept SET label='wrong' WHERE i=1",
                    "nested_float": 'UPDATE "data\'s".kept SET nested=struct_pack(labels := nested.labels, value := 0.0::DOUBLE) WHERE i=1',
                    "timestamp": "UPDATE \"data's\".kept SET stamp=TIMESTAMP_NS '2020-01-01 00:00:00.123456788' WHERE i=1",
                    "index": 'DROP INDEX "data\'s".label_index',
                    "rows": 'DELETE FROM "data\'s".kept WHERE i=2',
                }[fault]
            )

    monkeypatch.setattr(adapter, "_rewrite", corrupt)
    with pytest.raises(RepackError, match="changed"):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE VIEW v AS SELECT 1 AS i",
        "CREATE SEQUENCE s",
        "CREATE MACRO m(x) AS x+1",
        "CREATE TYPE e AS ENUM ('a', 'b')",
        "CREATE TABLE t(value INTERVAL)",
        "CREATE TABLE parent(i INTEGER PRIMARY KEY); CREATE TABLE child(i INTEGER REFERENCES parent(i))",
    ],
)
def test_unsupported_objects_fail_before_staging(tmp_path, monkeypatch, statement):
    import undatum.cmds.repacker as command

    source = tmp_path / "source.duckdb"
    with duckdb.connect(str(source)) as connection:
        connection.execute(statement)
    original = source.read_bytes()
    monkeypatch.setattr(command, "AtomicFileTarget", lambda *_: pytest.fail("Candidate allocated"))
    with pytest.raises(ValidationError, match="Unsupported"):
        repack(source)
    assert source.read_bytes() == original


@pytest.mark.parametrize("options", [{"compression": "zstd"}, {"level": 9}, {"threads": 2}])
def test_unsupported_controls_fail_before_staging(tmp_path, monkeypatch, options):
    import undatum.cmds.repacker as command

    source = tmp_path / "source.duckdb"
    database(source, empty=True)
    monkeypatch.setattr(command, "AtomicFileTarget", lambda *_: pytest.fail("Candidate allocated"))
    with pytest.raises(ValidationError, match="DuckDB"):
        repack(source, **options)


@pytest.mark.parametrize(
    "extension", ["csv", "parquet", "orc", "avro", "sqlite", "xlsx", "arrow", "db.gz"]
)
def test_output_format_conflicts(tmp_path, extension):
    source, output = tmp_path / "source.duckdb", tmp_path / ("out." + extension)
    database(source, empty=True)
    with pytest.raises(ValidationError):
        repack(source, output)
    assert not output.exists()


def test_invalid_header_and_wrong_native_suffix(tmp_path):
    source = tmp_path / "source.duckdb"
    source.write_bytes(b"not a database")
    with pytest.raises(RepackError, match="header"):
        repack(source)
    source.unlink()
    source = tmp_path / "source.csv"
    database(source, empty=True)
    with pytest.raises(ValidationError, match="header"):
        repack(source)


@pytest.mark.parametrize("location", ["source", "output", "realpath"])
def test_wal_refused_and_never_removed(tmp_path, location):
    source, output = tmp_path / "source.duckdb", tmp_path / "out.duckdb"
    database(source, empty=True)
    original = source.read_bytes()
    target = output if location == "output" else source
    wal = target.with_name(target.name + ".wal")
    wal.write_bytes(b"pending")
    if location == "realpath":
        alias = tmp_path / "alias.db"
        alias.symlink_to(source)
        source = alias
    with pytest.raises(ValidationError, match="WAL"):
        repack(source, output)
    assert source.read_bytes() == original and wal.read_bytes() == b"pending"
    assert not output.exists()
    assert_clean(tmp_path)


@pytest.mark.parametrize("location", ["source", "output", "candidate"])
def test_new_wal_before_commit_aborts_publication(tmp_path, monkeypatch, location):
    from undatum.common.writer import AtomicFileTarget

    source = tmp_path / "source.duckdb"
    database(source)
    original = source.read_bytes()
    output = tmp_path / "output.duckdb"
    output.write_bytes(b"prior")
    sidecars = []
    real = AtomicFileTarget.commit

    def commit(stage):
        from pathlib import Path

        target = (
            source if location == "source" else output if location == "output" else stage.target
        )
        wal = Path(str(target) + ".wal")
        sidecars.append(wal)
        wal.write_bytes(b"new transaction")
        real(stage)

    monkeypatch.setattr(AtomicFileTarget, "commit", commit)
    with pytest.raises(ValidationError, match="WAL"):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert sidecars
    if location == "candidate":
        assert not sidecars[0].exists()
    else:
        assert sidecars[0].read_bytes() == b"new transaction"
    assert_clean(tmp_path)


@pytest.mark.parametrize(
    "limit", ["MAX_FILE_BYTES", "MAX_ROWS", "MAX_CATALOG_BYTES", "MAX_VALUE_BYTES"]
)
def test_supported_bounds_leave_prior_files(tmp_path, monkeypatch, limit):
    import undatum.common.repack_duckdb as adapter

    source, output = tmp_path / "source.duckdb", tmp_path / "out.duckdb"
    database(source)
    original = source.read_bytes()
    output.write_bytes(b"prior")
    monkeypatch.setattr(adapter, limit, 1)
    with pytest.raises(ValidationError, match="limit|bounds"):
        repack(source, output)
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


@pytest.mark.parametrize(
    "options",
    [
        {"max_decoded_bytes": 1},
        {"temp_limit": 1},
        {"memory_limit": 1, "timeout": 30},
    ],
)
def test_resource_limits_preserve_source(tmp_path, options):
    source = tmp_path / "source.duckdb"
    database(source)
    original = source.read_bytes()
    with pytest.raises(ResourceLimitError):
        repack(source, **options)
    assert source.read_bytes() == original
    assert_clean(tmp_path)


def test_worker_failure_cleans_owned_wal(tmp_path, monkeypatch):
    import undatum.common.repack_worker as worker

    source = tmp_path / "source.duckdb"
    database(source, empty=True)
    original = source.read_bytes()

    def fail(_plan, target, _options, _started):
        from pathlib import Path

        Path(target + ".wal").write_bytes(b"owned transaction")
        raise ResourceLimitError("worker terminated")

    monkeypatch.setattr(worker, "supervised_repack", fail)
    with pytest.raises(ResourceLimitError):
        repack(source, timeout=30)
    assert source.read_bytes() == original
    assert_clean(tmp_path)


def test_writer_close_failure_preserves_prior_files(tmp_path, monkeypatch):
    import undatum.common.repack_duckdb as adapter

    source, output = tmp_path / "source.duckdb", tmp_path / "out.duckdb"
    database(source, empty=True)
    original = source.read_bytes()
    output.write_bytes(b"prior")
    real = adapter._backends

    class Connection:
        def __init__(self, connection):
            self.connection = connection
            self.writer = False

        def execute(self, query):
            self.writer |= " AS candidate" in query
            return self.connection.execute(query)

        def close(self):
            self.connection.close()
            if self.writer:
                raise duckdb.IOException("candidate finalization failed")

    class Backend:
        def connect(self, *args, **kwargs):
            return Connection(real().connect(*args, **kwargs))

        def __getattr__(self, name):
            return getattr(real(), name)

    monkeypatch.setattr(adapter, "_backends", Backend)
    with pytest.raises(UndatumError, match="finalization") as error:
        repack(source, output, allow_grow=True)
    assert error.value.exit_code == 3
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


@pytest.mark.parametrize("module", ["duckdb", "pyarrow"])
def test_missing_backend_is_actionable_before_staging(tmp_path, monkeypatch, module):
    import builtins

    source = tmp_path / "source.duckdb"
    database(source, empty=True)
    original = source.read_bytes()
    real = builtins.__import__

    def missing(name, *args, **kwargs):
        if name == module:
            raise ImportError("backend unavailable", name=module)
        return real(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing)
    with pytest.raises(DependencyError, match=module):
        repack(source)
    assert source.read_bytes() == original
    assert_clean(tmp_path)


def test_scratch_includes_wal(tmp_path):
    candidate, wal = tmp_path / "candidate", tmp_path / "candidate.wal"
    candidate.write_bytes(b"small")
    wal.write_bytes(b"pending transaction")
    budget = RepackBudget(RepackOptions(temp_limit=20), str(candidate), scratch_paths=(str(wal),))
    with pytest.raises(ResourceLimitError, match="Temporary"):
        budget.check()


def test_typed_comparator_handles_batch_boundaries_and_nested_float_bits():
    from undatum.common.repack_duckdb import _compare_batches

    dtype = pa.struct([("values", pa.list_(pa.float64()))])
    original = pa.record_batch(
        [pa.array([{"values": [-0.0, float("nan")]}, None], type=dtype)], names=["nested"]
    )
    changed = pa.record_batch(
        [pa.array([{"values": [0.0, float("nan")]}, None], type=dtype)], names=["nested"]
    )
    budget = RepackBudget(RepackOptions(), "")
    _compare_batches(
        iter([original.slice(0, 1), original.slice(1)]), iter([original]), [0, 0, 0], budget
    )
    with pytest.raises(RepackError, match="values"):
        _compare_batches(iter([original]), iter([changed]), [0, 0, 0], budget)


def test_locked_database_is_an_actionable_database_error(tmp_path):
    source = tmp_path / "source.duckdb"
    database(source, empty=True)
    original = source.read_bytes()
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import duckdb,sys; db=duckdb.connect(sys.argv[1]); print('ready',flush=True); input()",
            str(source),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        with pytest.raises(UndatumError, match="lock") as error:
            repack(source)
        assert error.value.exit_code == 3
    finally:
        process.kill()
        process.communicate()
    assert source.read_bytes() == original
    assert_clean(tmp_path)


def test_outer_compression_does_not_compact_database(tmp_path):
    database_file = tmp_path / "source.duckdb"
    database(database_file)
    original = database_file.read_bytes()
    source = tmp_path / "source.duckdb.gz"
    source.write_bytes(gzip.compress(original, compresslevel=1))
    result = repack(source)
    assert result["mode"] == "container" and gzip.decompress(source.read_bytes()) == original


def test_cli_json_database_summary(tmp_path):
    from undatum.core import app

    source = tmp_path / "source.duckdb"
    database(source, empty=True)
    result = CliRunner().invoke(app, ["repack", str(source), "--dry-run", "--json"])
    assert result.exit_code == 0, result.stdout
    report = json.loads(result.stdout)
    assert report["schema"] == "undatum.repack/1" and report["status"] == "dry_run"
    assert report["effective_settings"]["strategy"] == "copy_database"
    assert report["verification"]["duckdb_version"] == duckdb.__version__
    assert_clean(tmp_path)
