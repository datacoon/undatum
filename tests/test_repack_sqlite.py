"""SQLite compaction preserves logical identities and never publishes a broken candidate."""

from __future__ import annotations

import gzip
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from typer.testing import CliRunner

from undatum.cmds.repacker import Repacker
from undatum.common.errors import ConfigurationError, UndatumError, ValidationError
from undatum.common.repack_types import RepackBudget, RepackError, RepackOptions, ResourceLimitError


def database(path, *, fragmented=False, empty=False, journal="DELETE", encoding="UTF-8"):
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA page_size=8192")
        connection.execute("PRAGMA encoding='" + encoding + "'")
        connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
        connection.execute("PRAGMA user_version=42")
        connection.execute("PRAGMA application_id=1196437808")
        if not empty:
            connection.executescript(
                "CREATE TABLE kept(value);"
                "CREATE INDEX text_values ON kept(value) WHERE typeof(value)='text';"
                "CREATE TABLE accounts(id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "label TEXT UNIQUE NOT NULL DEFAULT 'fallback');"
                "CREATE TABLE audit(account_id INTEGER);"
                "CREATE TRIGGER audit_insert AFTER INSERT ON accounts BEGIN "
                "INSERT INTO audit VALUES(new.id); END;"
                "INSERT INTO accounts(id,label) VALUES(1,'kept'),(5,'deleted');"
                "DELETE FROM accounts WHERE id=5;"
                "CREATE TABLE child(id INTEGER PRIMARY KEY CHECK(id>0), "
                "parent INTEGER REFERENCES accounts(id) DEFERRABLE INITIALLY DEFERRED);"
                "INSERT INTO child VALUES(1,1);"
                "CREATE VIEW account_labels AS SELECT label FROM accounts;"
                "CREATE TABLE wr(a TEXT COLLATE NOCASE, b INTEGER, data BLOB, "
                "PRIMARY KEY(a DESC,b)) WITHOUT ROWID;"
                "INSERT INTO wr VALUES('Zulu',2,x'0001'),('alpha',1,x'ff'),('quote''key',3,NULL);"
                "CREATE TABLE generated(i INTEGER, doubled INTEGER GENERATED ALWAYS AS(i*2) STORED, "
                "label TEXT GENERATED ALWAYS AS(printf('%d',i)) VIRTUAL);"
                "INSERT INTO generated(i) VALUES(7);"
                "CREATE TABLE empty_table(value TEXT);"
            )
            connection.executemany(
                "INSERT INTO kept(rowid,value) VALUES (?,?)",
                [
                    (-10, -(2**63)),
                    (10, "42"),
                    (99, b"42"),
                    (1000, None),
                    (1001, 2**63 - 1),
                    (1002, 2.5),
                    (1003, -0.0),
                    (1004, float("inf")),
                    (1005, -float("inf")),
                    (1006, "Юникод\0text"),
                    (1007, b"\x00\xff\x00"),
                ],
            )
            if encoding == "UTF-8":
                connection.execute(
                    "INSERT INTO kept(rowid,value) VALUES (1008,CAST(x'ff0000' AS TEXT))"
                )
            if fragmented:
                connection.execute("CREATE TABLE junk(value BLOB)")
                connection.executemany(
                    "INSERT INTO junk VALUES (?)", [(b"x" * 2048,) for _ in range(2000)]
                )
                connection.commit()
                connection.execute("DELETE FROM junk")
            connection.execute("ANALYZE")
        connection.commit()
        connection.execute("PRAGMA journal_mode=" + journal).fetchone()


def repack(source, output=None, **options):
    return Repacker().repack(
        str(source),
        str(output) if output is not None else None,
        {"progress": False, "sqlite_offline": True, **options},
    )


def assert_clean(directory):
    assert not list(directory.glob(".undatum-tmp-*"))


def contents(path):
    with closing(
        sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
    ) as connection:
        return (
            connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall(),
            connection.execute(
                "SELECT rowid, typeof(value), CASE WHEN typeof(value) IN ('text','blob') "
                "THEN hex(value) ELSE value END FROM kept ORDER BY rowid"
            ).fetchall(),
            connection.execute("SELECT * FROM wr ORDER BY a COLLATE BINARY,b").fetchall(),
            connection.execute("SELECT * FROM generated").fetchall(),
            connection.execute("SELECT * FROM audit ORDER BY rowid").fetchall(),
            connection.execute("SELECT * FROM sqlite_sequence").fetchall(),
            tuple(
                connection.execute("PRAGMA " + setting).fetchone()
                for setting in (
                    "application_id",
                    "user_version",
                    "encoding",
                    "page_size",
                    "auto_vacuum",
                )
            ),
            path.read_bytes()[18:20],
        )


@pytest.mark.parametrize("supervised", [False, True])
def test_compacts_and_preserves_full_database(tmp_path, supervised):
    source, output = tmp_path / "source.db", tmp_path / "out.sqlite"
    database(source, fragmented=True)
    original, expected = source.read_bytes(), contents(source)
    os.chmod(source, 0o640)
    state = source.stat()
    result = repack(source, output, **({"timeout": 30} if supervised else {}))
    assert result["changed"] and result["candidate_size"] < len(original)
    assert contents(output) == expected and source.read_bytes() == original
    assert result["verification"]["passed"]
    assert result["resources"]["decoded_bytes_processed"] == 3 * result["decoded_bytes"]
    assert result["effective_settings"] == {
        "codec": "none",
        "profile": "max",
        "level": None,
        "threads": 1,
        "applied": True,
        "strategy": "vacuum_into",
    }
    assert output.stat().st_mode == state.st_mode
    assert output.stat().st_mtime_ns == state.st_mtime_ns
    with closing(sqlite3.connect(output)) as connection:
        connection.execute("INSERT INTO accounts(label) VALUES('next')")
        assert connection.execute("SELECT id FROM accounts WHERE label='next'").fetchone() == (6,)
        assert connection.execute("SELECT count(*) FROM audit").fetchone() == (3,)
    assert_clean(tmp_path)


@pytest.mark.parametrize("encoding", ["UTF-8", "UTF-16le", "UTF-16be"])
@pytest.mark.parametrize("journal", ["DELETE", "WAL"])
def test_encoding_and_persistent_journal_mode(tmp_path, encoding, journal):
    source, output = tmp_path / "source.db", tmp_path / "out.db"
    database(source, encoding=encoding, journal=journal)
    expected = contents(source)
    result = repack(source, output, allow_grow=True, batch_size=1)
    assert contents(output) == expected
    assert result["verification"]["journal_mode"] == journal.lower()
    assert not any(Path(str(output) + suffix).exists() for suffix in ("-wal", "-shm", "-journal"))
    assert_clean(tmp_path)


@pytest.mark.parametrize(
    "name",
    [
        "source.db",
        "source.sqlite",
        "source.sqlite3",
        "source.gpkg",
        "source.mbtiles",
        "source.sqlitedb",
        "source.vscdb",
        "source",
    ],
)
def test_header_detection_and_new_output_without_assertion(tmp_path, name):
    source, output = tmp_path / name, tmp_path / "out.db"
    database(source, empty=True)
    result = repack(source, output, sqlite_offline=False, allow_grow=True, compression="auto")
    assert result["mode"] == "builtin" and result["compression"] == "none"
    assert result["rows"] == 0 and output.read_bytes().startswith(b"SQLite format 3\0")
    assert_clean(tmp_path)


@pytest.mark.parametrize("distinct", [False, True])
def test_empty_no_benefit_and_atomic_fallback(tmp_path, distinct):
    source = tmp_path / "empty.db"
    database(source, empty=True)
    original = source.read_bytes()
    output = tmp_path / "out.db" if distinct else source
    result = repack(source, output)
    assert result["reason_code"] == "no_benefit" and not result["changed"]
    assert result["published"] == distinct and result["rows"] == 0
    assert output.read_bytes() == original
    assert_clean(tmp_path)


@pytest.mark.parametrize("distinct", [False, True])
def test_replacement_requires_explicit_offline_assertion(tmp_path, monkeypatch, distinct):
    import undatum.cmds.repacker as command

    source = tmp_path / "source.db"
    database(source, empty=True)
    original = source.read_bytes()
    output = tmp_path / "out.db" if distinct else source
    if distinct:
        output.write_bytes(b"prior")
    monkeypatch.setattr(command, "AtomicFileTarget", lambda *_: pytest.fail("Candidate allocated"))
    with pytest.raises(ValidationError, match="sqlite-offline"):
        repack(source, output, sqlite_offline=False)
    assert source.read_bytes() == original
    if distinct:
        assert output.read_bytes() == b"prior"


def test_dry_run_does_not_require_offline_assertion(tmp_path):
    source, output = tmp_path / "source.db", tmp_path / "out.db"
    database(source)
    original = source.read_bytes()
    output.write_bytes(b"prior")
    result = repack(source, output, sqlite_offline=False, dry_run=True)
    assert result["status"] == "dry_run" and not result["published"]
    assert result["verification"]["passed"]
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


def test_minimum_savings_copies_original(tmp_path):
    source, output = tmp_path / "source.db", tmp_path / "out.db"
    database(source, fragmented=True)
    original = source.read_bytes()
    result = repack(source, output, min_savings=100)
    assert result["candidate_savings_percent"] > 50
    assert result["reason_code"] == "below_min_savings" and not result["changed"]
    assert output.read_bytes() == original
    assert_clean(tmp_path)


@pytest.mark.parametrize(
    ("ddl", "identity"),
    [
        (
            "CREATE TABLE t(rowid TEXT, value); INSERT INTO t(_rowid_,rowid,value) VALUES(99,'shadow',42)",
            "_rowid_",
        ),
        (
            "CREATE TABLE t(rowid TEXT, _rowid_ TEXT, value); INSERT INTO t(oid,value) VALUES(99,42)",
            "oid",
        ),
        (
            "CREATE TABLE t(id INTEGER PRIMARY KEY DESC, value); INSERT INTO t(rowid,id,value) VALUES(99,7,42)",
            "rowid",
        ),
        (
            "CREATE TABLE t(rowid TEXT, _rowid_ INTEGER, oid BLOB, PRIMARY KEY(rowid,_rowid_)) WITHOUT ROWID; INSERT INTO t VALUES('key',42,x'00')",
            None,
        ),
        (
            "CREATE TABLE t(value TEXT CHECK(value != 'WITHOUT ROWID')); INSERT INTO t(rowid,value) VALUES(99,'keep')",
            "rowid",
        ),
        (
            "CREATE TABLE t(value TEXT DEFAULT 'CREATE VIRTUAL TABLE harmless'); INSERT INTO t(rowid,value) VALUES(99,'keep')",
            "rowid",
        ),
    ],
)
def test_row_identity_profiles_are_not_inferred_from_sql_substrings(tmp_path, ddl, identity):
    source, output = tmp_path / "source.db", tmp_path / "out.db"
    native = tmp_path / "native.db"
    query = "SELECT " + (identity + ", " if identity else "") + "* FROM t"
    with closing(sqlite3.connect(source)) as connection:
        connection.executescript(ddl)
        expected = connection.execute(query).fetchall()
        connection.execute("VACUUM INTO ?", (str(native),))
    with closing(sqlite3.connect(native)) as connection:
        preserved = connection.execute(query).fetchall() == expected
    if not preserved:
        # Some SQLite runtimes renumber hidden rowids when a CHECK prevents
        # native table transfer. Refusal must preserve both existing files.
        original = source.read_bytes()
        output.write_bytes(b"prior")
        with pytest.raises(RepackError, match="rowids"):
            repack(source, output, allow_grow=True)
        assert source.read_bytes() == original and output.read_bytes() == b"prior"
        assert_clean(tmp_path)
        return
    repack(source, output, allow_grow=True)
    with closing(sqlite3.connect(output)) as connection:
        assert connection.execute(query).fetchall() == expected
    assert_clean(tmp_path)


def test_shadowed_rowid_aliases_refused_before_staging(tmp_path, monkeypatch):
    import undatum.cmds.repacker as command

    source = tmp_path / "source.db"
    with closing(sqlite3.connect(source)) as connection:
        connection.execute("CREATE TABLE t(rowid, _rowid_, oid)")
    monkeypatch.setattr(command, "AtomicFileTarget", lambda *_: pytest.fail("Candidate allocated"))
    with pytest.raises(ValidationError, match="rowid aliases"):
        repack(source)


@pytest.mark.parametrize(
    "fault", ["value", "storage_class", "rowid", "rows", "index", "version", "sequence"]
)
def test_changed_candidate_is_refused(tmp_path, monkeypatch, fault):
    import undatum.common.repack_sqlite as adapter

    source, output = tmp_path / "source.db", tmp_path / "out.db"
    database(source)
    original = source.read_bytes()
    output.write_bytes(b"prior")
    real = adapter._rewrite

    def corrupt(source, target, mode, budget):
        real(source, target, mode, budget)
        with closing(sqlite3.connect(target)) as connection:
            connection.execute(
                {
                    "value": "UPDATE kept SET value='wrong' WHERE rowid=10",
                    "storage_class": "UPDATE kept SET value=CAST(value AS BLOB) WHERE rowid=10",
                    "rowid": "UPDATE kept SET rowid=-11 WHERE rowid=-10",
                    "rows": "DELETE FROM kept WHERE rowid=10",
                    "index": "DROP INDEX text_values",
                    "version": "PRAGMA user_version=99",
                    "sequence": "UPDATE sqlite_sequence SET seq=1 WHERE name='accounts'",
                }[fault]
            )
            connection.commit()

    monkeypatch.setattr(adapter, "_rewrite", corrupt)
    with pytest.raises(RepackError, match="changed"):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
@pytest.mark.parametrize("location", ["source", "output", "realpath"])
def test_sidecars_are_refused_and_never_deleted(tmp_path, suffix, location):
    source, output = tmp_path / "source.db", tmp_path / "out.db"
    database(source, empty=True)
    original = source.read_bytes()
    target = output if location == "output" else source
    sidecar = Path(str(target) + suffix)
    sidecar.write_bytes(b"pending")
    if location == "realpath":
        alias = tmp_path / "alias.db"
        alias.symlink_to(source)
        source = alias
    with pytest.raises(ValidationError, match="sidecar"):
        repack(source, output)
    assert source.read_bytes() == original and sidecar.read_bytes() == b"pending"
    assert not output.exists()
    assert_clean(tmp_path)


@pytest.mark.parametrize("location", ["source", "output", "candidate"])
def test_new_sidecar_before_commit_aborts_publication(tmp_path, monkeypatch, location):
    from undatum.common.writer import AtomicFileTarget

    source, output = tmp_path / "source.db", tmp_path / "out.db"
    database(source, empty=True)
    original = source.read_bytes()
    output.write_bytes(b"prior")
    sidecars = []
    real = AtomicFileTarget.commit

    def commit(stage):
        target = (
            source if location == "source" else output if location == "output" else stage.target
        )
        sidecar = Path(str(target) + "-journal")
        sidecars.append(sidecar)
        sidecar.write_bytes(b"new transaction")
        real(stage)

    monkeypatch.setattr(AtomicFileTarget, "commit", commit)
    with pytest.raises(ValidationError, match="sidecar"):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert sidecars
    if location == "candidate":
        assert not sidecars[0].exists()
    else:
        assert sidecars[0].read_bytes() == b"new transaction"
    assert_clean(tmp_path)


@pytest.mark.parametrize(
    "options", [{"level": 9}, {"compression": "zstd"}, {"threads": 2}, {"sqlite_offline": "yes"}]
)
def test_invalid_controls_fail_before_staging(tmp_path, monkeypatch, options):
    import undatum.cmds.repacker as command

    source = tmp_path / "source.db"
    database(source, empty=True)
    monkeypatch.setattr(command, "AtomicFileTarget", lambda *_: pytest.fail("Candidate allocated"))
    with pytest.raises(ValidationError):
        repack(source, **options)


@pytest.mark.parametrize("extension", ["csv", "parquet", "duckdb", "orc", "avro", "db.gz"])
def test_output_format_conflicts(tmp_path, extension):
    source, output = tmp_path / "source.db", tmp_path / ("out." + extension)
    database(source, empty=True)
    with pytest.raises(ValidationError):
        repack(source, output)
    assert not output.exists()


def test_invalid_header_and_wrong_native_suffix(tmp_path):
    source = tmp_path / "source.sqlite"
    source.write_bytes(b"not a database")
    with pytest.raises(RepackError, match="header"):
        repack(source)
    source.unlink()
    database(source, empty=True)
    wrong = tmp_path / "source.csv"
    source.rename(wrong)
    with pytest.raises(ValidationError, match="header"):
        repack(wrong)


@pytest.mark.parametrize("with_comment", [False, True])
def test_virtual_table_refused_before_staging(tmp_path, monkeypatch, with_comment):
    import undatum.cmds.repacker as command

    source = tmp_path / "source.db"
    with closing(sqlite3.connect(source)) as connection:
        try:
            connection.execute(
                "CREATE "
                + ("/* comment */ " if with_comment else "")
                + "VIRTUAL TABLE t USING fts5(value)"
            )
        except sqlite3.OperationalError:
            pytest.skip("SQLite FTS5 unavailable")
    monkeypatch.setattr(command, "AtomicFileTarget", lambda *_: pytest.fail("Candidate allocated"))
    with pytest.raises(ValidationError, match="virtual table"):
        repack(source)


def test_unknown_collation_refused_before_staging(tmp_path, monkeypatch):
    import undatum.cmds.repacker as command

    source = tmp_path / "source.db"
    with closing(sqlite3.connect(source)) as connection:
        connection.create_collation("external_custom", lambda a, b: (a > b) - (a < b))
        connection.execute("CREATE TABLE t(value TEXT COLLATE external_custom)")
    monkeypatch.setattr(command, "AtomicFileTarget", lambda *_: pytest.fail("Candidate allocated"))
    with pytest.raises(ValidationError, match="collation"):
        repack(source)


@pytest.mark.parametrize("limit", ["MAX_CATALOG_BYTES", "MAX_COLUMNS"])
def test_profile_bounds(tmp_path, monkeypatch, limit):
    import undatum.common.repack_sqlite as adapter

    source = tmp_path / "source.db"
    database(source)
    original = source.read_bytes()
    monkeypatch.setattr(adapter, limit, 1)
    with pytest.raises(ValidationError, match="limit|columns"):
        repack(source)
    assert source.read_bytes() == original
    assert_clean(tmp_path)


def test_old_runtime_is_actionable(tmp_path, monkeypatch):
    source = tmp_path / "source.db"
    database(source, empty=True)
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 30, 0))
    with pytest.raises(ConfigurationError, match="3.31"):
        repack(source)
    assert_clean(tmp_path)


@pytest.mark.parametrize("fault", ["reserved_bytes", "partial_trailer", "full_page_trailer"])
def test_unverifiable_physical_payload_refused_before_staging(tmp_path, monkeypatch, fault):
    import undatum.cmds.repacker as command

    source = tmp_path / "source.db"
    database(source, empty=True)
    original = bytearray(source.read_bytes())
    if fault == "reserved_bytes":
        original[20] = 1
    else:
        original.extend(b"opaque" if fault == "partial_trailer" else b"x" * 8192)
    source.write_bytes(original)
    monkeypatch.setattr(command, "AtomicFileTarget", lambda *_: pytest.fail("Candidate allocated"))
    with pytest.raises(ValidationError, match="reserved|trailing|pages"):
        repack(source)
    assert source.read_bytes() == original


def test_large_blob_keeps_exact_bytes(tmp_path):
    source, output = tmp_path / "source.db", tmp_path / "out.db"
    payload = bytes(range(256)) * 16384
    with closing(sqlite3.connect(source)) as connection:
        connection.execute("CREATE TABLE t(value BLOB)")
        connection.execute("INSERT INTO t(rowid,value) VALUES(99,?)", (payload,))
        connection.commit()
    repack(source, output, allow_grow=True)
    with closing(sqlite3.connect(output)) as connection:
        assert connection.execute("SELECT rowid,value FROM t").fetchone() == (99, payload)
    assert_clean(tmp_path)


def test_fragmented_database_is_replaced_in_place(tmp_path):
    source = tmp_path / "source.sqlite"
    database(source, fragmented=True)
    expected, size = contents(source), source.stat().st_size
    result = repack(source, source)
    assert result["changed"] and source.stat().st_size < size
    assert contents(source) == expected
    assert_clean(tmp_path)


def test_quoted_identifiers_and_paths(tmp_path):
    source, output = tmp_path / "source's data.db", tmp_path / "compact's data.sqlite"
    table, column = '"odd ""table\'s"', '"value ""key\'s"'
    with closing(sqlite3.connect(source)) as connection:
        connection.execute(f"CREATE TABLE {table}({column} TEXT, other BLOB)")
        connection.execute(
            f"INSERT INTO {table}(rowid,{column},other) VALUES (99,?,?)", ("keep'\"\0", b"\x00\xff")
        )
        connection.commit()
    repack(source, output, allow_grow=True)
    with closing(sqlite3.connect(output)) as connection:
        assert connection.execute(f"SELECT rowid,* FROM {table}").fetchone() == (
            99,
            "keep'\"\0",
            b"\x00\xff",
        )
    assert_clean(tmp_path)


@pytest.mark.parametrize(
    "options", [{"max_decoded_bytes": 1}, {"temp_limit": 1}, {"memory_limit": 1, "timeout": 30}]
)
def test_resource_limits_preserve_prior_files(tmp_path, options):
    source, output = tmp_path / "source.db", tmp_path / "out.db"
    database(source)
    original = source.read_bytes()
    output.write_bytes(b"prior")
    with pytest.raises(ResourceLimitError):
        repack(source, output, **options)
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


@pytest.mark.parametrize("exception", [ResourceLimitError("callback budget"), KeyboardInterrupt()])
def test_progress_callback_preserves_original_exception(tmp_path, monkeypatch, exception):
    from undatum.common.repack_sqlite import _connection

    source = tmp_path / "source.db"
    database(source, empty=True)
    budget = RepackBudget(RepackOptions(), "")

    def fail(*_args):
        raise exception

    monkeypatch.setattr(budget, "check", fail)
    with pytest.raises(type(exception)), _connection(str(source), budget) as connection:
        connection.execute(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n"
        ).fetchone()


def test_worker_failure_cleans_all_owned_sidecars(tmp_path, monkeypatch):
    import undatum.common.repack_worker as worker

    source = tmp_path / "source.db"
    database(source, empty=True)
    original = source.read_bytes()

    def fail(_plan, target, _options, _started):
        for suffix in ("-wal", "-shm", "-journal"):
            Path(target + suffix).write_bytes(b"owned")
        raise ResourceLimitError("worker terminated")

    monkeypatch.setattr(worker, "supervised_repack", fail)
    with pytest.raises(ResourceLimitError):
        repack(source, timeout=30)
    assert source.read_bytes() == original
    assert_clean(tmp_path)


def test_encoder_failure_never_falls_back_to_plain_vacuum(tmp_path, monkeypatch):
    import undatum.common.repack_sqlite as adapter

    source, output = tmp_path / "source.db", tmp_path / "out.db"
    database(source)
    original = source.read_bytes()
    output.write_bytes(b"prior")

    def fail(_source, target, _mode, _budget):
        Path(target).write_bytes(b"partial")
        Path(target + "-journal").write_bytes(b"pending")
        raise UndatumError("encoder failure", exit_code=3)

    monkeypatch.setattr(adapter, "_rewrite", fail)
    with pytest.raises(UndatumError, match="encoder"):
        repack(source, output)
    assert source.read_bytes() == original and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


def test_outer_stream_keeps_exact_database_bytes(tmp_path):
    plain = tmp_path / "source.db"
    database(plain)
    original = plain.read_bytes()
    source = tmp_path / "source.sqlite.gz"
    source.write_bytes(gzip.compress(original, compresslevel=1))
    result = repack(source, sqlite_offline=False)
    assert result["mode"] == "container" and gzip.decompress(source.read_bytes()) == original


def test_cli_json_and_offline_flag(tmp_path):
    from undatum.core import app

    source = tmp_path / "source.db"
    database(source, empty=True)
    runner = CliRunner()
    refused = runner.invoke(app, ["repack", str(source), "--json"])
    assert refused.exit_code == 1 and "sqlite-offline" in refused.stderr
    result = runner.invoke(app, ["repack", str(source), "--sqlite-offline", "--json"])
    assert result.exit_code == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["schema"] == "undatum.repack/1"
    assert report["effective_settings"]["strategy"] == "vacuum_into"
    assert report["verification"]["sqlite_version"] == sqlite3.sqlite_version
    assert_clean(tmp_path)
