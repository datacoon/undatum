"""Real format fidelity and publication-fault regressions for repack."""

from __future__ import annotations

import bz2
import gzip
import json
import lzma
import os
import struct
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from undatum.cmds.repacker import Repacker
from undatum.common.errors import UndatumError, ValidationError
from undatum.common.file_snapshot import FileConflictError, FileSnapshot
from undatum.common.repack_types import RepackError, ResourceLimitError
from undatum.common.writer import AtomicFileTarget

PAYLOAD = b'\xef\xbb\xbfa,b\r\n"quoted,cell",42\r\n\r\n' * 200


def encoded(codec, payload=PAYLOAD):
    if codec == "gz":
        return gzip.compress(payload, compresslevel=1, mtime=0)
    if codec == "bz2":
        return bz2.compress(payload, compresslevel=1)
    if codec in {"xz", "lzma"}:
        return lzma.compress(
            payload, format=lzma.FORMAT_ALONE if codec == "lzma" else lzma.FORMAT_XZ, preset=1
        )
    if codec == "zst":
        return pytest.importorskip("zstandard").ZstdCompressor(level=1).compress(payload)
    if codec == "br":
        pytest.importorskip("brotli_file")
        return pytest.importorskip("brotli").compress(payload, quality=1)
    return pytest.importorskip("lz4.frame").compress(payload)


def repack(source, output=None, **options):
    return Repacker().repack(
        str(source), str(output) if output is not None else None, {"progress": False, **options}
    )


def assert_clean(directory):
    assert not list(directory.glob(".undatum-tmp-*"))
    assert not list(directory.glob(".undatum_repack_*"))


@pytest.mark.parametrize("codec", ["gz", "bz2", "xz", "lzma", "zst", "lz4", "br"])
def test_explicit_same_file_and_full_payload(tmp_path, codec):
    source = tmp_path / f"source.csv.{codec}"
    source.write_bytes(encoded(codec))
    result = repack(source, source, allow_grow=True)
    assert result["published"] and result["verification"]["passed"]
    assert result["decoded_bytes"] == len(PAYLOAD)
    assert result["level"] is not None
    assert_clean(tmp_path)


@pytest.mark.parametrize("codec", ["gz", "bz2", "xz", "lzma", "zst", "lz4", "br"])
def test_truncated_stream_preserves_existing_output(tmp_path, codec):
    source, output = tmp_path / f"source.csv.{codec}", tmp_path / f"out.csv.{codec}"
    source.write_bytes(encoded(codec)[:-1])
    before = source.read_bytes()
    output.write_bytes(b"existing destination")
    with pytest.raises(UndatumError):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == before
    assert output.read_bytes() == b"existing destination"
    assert_clean(tmp_path)


@pytest.mark.parametrize("codec", ["gz", "bz2", "xz", "lzma", "zst", "lz4"])
def test_concatenated_streams_preserve_every_member(tmp_path, codec):
    source = tmp_path / f"source.csv.{codec}"
    source.write_bytes(encoded(codec, b"first\r\n") + encoded(codec, b"second\r\n"))
    result = repack(source, allow_grow=True)
    assert result["decoded_bytes"] == len(b"first\r\nsecond\r\n")
    assert result["verification"]["passed"]


def test_unknown_inner_format_does_not_hide_gzip(tmp_path):
    source, output = tmp_path / "opaque.gz", tmp_path / "output.gz"
    source.write_bytes(encoded("gz"))
    assert repack(source, output)["decoded_bytes"] == len(PAYLOAD)
    assert gzip.decompress(output.read_bytes()) == PAYLOAD


def test_no_growth_inplace_and_explicit_output_fallback(tmp_path):
    source, output = tmp_path / "tiny.csv.gz", tmp_path / "copy.csv.gz"
    source.write_bytes(gzip.compress(b"a,b\n1,2\n", mtime=0))
    before = source.read_bytes()
    result = repack(source)
    assert source.read_bytes() == before
    assert result["status"] == "unchanged" and not result["published"]
    result = repack(source, output, min_savings=100)
    assert output.read_bytes() == before
    assert result["published"] and not result["changed"]
    assert not result["effective_settings"]["applied"]
    assert result["reason_code"] == "below_min_savings"


def test_migration_never_copies_wrong_codec(tmp_path):
    zstd = pytest.importorskip("zstandard")
    source, output = tmp_path / "data.csv.gz", tmp_path / "data.csv.zst"
    source.write_bytes(encoded("gz", b"small"))
    result = repack(source, output)
    assert result["published"]
    with zstd.ZstdDecompressor().stream_reader(output.open("rb")) as reader:
        assert reader.read() == b"small"
    before = output.read_bytes()
    result = repack(source, output, min_savings=100)
    assert result["status"] == "skipped" and not result["published"]
    assert output.read_bytes() == before


@pytest.mark.parametrize("exists", [True, False])
def test_measured_dry_run_changes_nothing(tmp_path, exists):
    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(encoded("gz"))
    before = source.read_bytes()
    if exists:
        output.write_bytes(b"prior output")
    result = repack(source, output, dry_run=True)
    assert result["status"] == "dry_run" and not result["published"]
    assert result["savings_bytes"] == 0
    assert result["candidate_size"] > 0 and result["verification"]["passed"]
    assert result["output_size"] == (len(b"prior output") if exists else None)
    assert source.read_bytes() == before
    assert output.read_bytes() == b"prior output" if exists else not output.exists()
    assert_clean(tmp_path)


def test_source_mode_mtime_and_supported_xattrs(tmp_path):
    source = tmp_path / "data.csv.gz"
    source.write_bytes(encoded("gz"))
    source.chmod(0o644)
    os.utime(source, ns=(1600000000000000000, 1600000000123456789))
    attribute = "user.undatum" if os.name != "nt" else None
    if attribute and hasattr(os, "setxattr"):
        os.setxattr(source, attribute, b"binary\x00\xff")
    repack(source, allow_grow=True)
    assert source.stat().st_mode & 0o777 == 0o644
    assert source.stat().st_mtime_ns == 1600000000123456789
    if attribute and hasattr(os, "getxattr"):
        assert os.getxattr(source, attribute) == b"binary\x00\xff"


@pytest.mark.parametrize("link", ["symbolic", "hard"])
def test_linked_source_is_only_allowed_for_distinct_output(tmp_path, link):
    source, alias, output = (
        tmp_path / "data.csv.gz",
        tmp_path / "alias.csv.gz",
        tmp_path / "out.csv.gz",
    )
    source.write_bytes(encoded("gz"))
    if link == "symbolic":
        alias.symlink_to(source)
    else:
        os.link(source, alias)
    with pytest.raises(ValidationError, match="links"):
        repack(alias)
    repack(alias, output, allow_grow=True)
    assert gzip.decompress(output.read_bytes()) == PAYLOAD
    assert alias.is_symlink() if link == "symbolic" else source.stat().st_nlink == 2


@pytest.mark.parametrize("mutate", ["source", "destination", "candidate"])
def test_mutation_after_encoding_refuses_publication(tmp_path, monkeypatch, mutate):
    import undatum.cmds.repacker as module

    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(encoded("gz"))
    output.write_bytes(b"prior destination")
    before = source.read_bytes()
    real = module.perform_repack

    def operation(plan, target, options):
        result = real(plan, target, options)
        if mutate != "candidate":
            (source if mutate == "source" else output).write_bytes(b"concurrent writer")
        return result

    monkeypatch.setattr(module, "perform_repack", operation)
    if mutate == "candidate":
        real_commit = AtomicFileTarget.commit

        def commit(stage):
            Path(stage.target).write_bytes(b"changed after verification")
            real_commit(stage)

        monkeypatch.setattr(AtomicFileTarget, "commit", commit)
    with pytest.raises(FileConflictError):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == (b"concurrent writer" if mutate == "source" else before)
    assert output.read_bytes() == (
        b"concurrent writer" if mutate == "destination" else b"prior destination"
    )
    assert_clean(tmp_path)


@pytest.mark.parametrize("fault", ["changed", "unverified"])
def test_candidate_must_match_completed_proof(tmp_path, monkeypatch, fault):
    import undatum.cmds.repacker as module

    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(encoded("gz"))
    before = source.read_bytes()
    output.write_bytes(b"prior")
    real = module.perform_repack

    def operation(plan, target, options):
        result = real(plan, target, options)
        if fault == "changed":
            Path(target).write_bytes(encoded("gz", b"changed but valid stream"))
        else:
            result["verification"]["passed"] = False
        return result

    monkeypatch.setattr(module, "perform_repack", operation)
    with pytest.raises(RepackError, match="preservation proof"):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == before and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


def test_cleanup_failure_preserves_original_error(tmp_path, monkeypatch):
    stage = AtomicFileTarget(str(tmp_path / "output"))
    Path(stage.target).write_bytes(b"candidate")

    def failure():
        raise OSError("cleanup fault")

    with monkeypatch.context() as patch:
        patch.setattr(stage, "abort", failure)
        with pytest.raises(RepackError, match="original fault"), stage:
            raise RepackError("original fault")
    stage.abort()


def test_gzip_header_name_comment_time_and_extra_policy(tmp_path):
    import zlib

    from undatum.common.repack_streams import gzip_header

    source = tmp_path / "named.csv.gz"
    header = (
        b"\x1f\x8b\x08\x1a" + struct.pack("<I", 1600000000) + b"\x00\xfforiginal.csv\0comment\0"
    )
    header += struct.pack("<H", zlib.crc32(header) & 0xFFFF)
    compressor = zlib.compressobj(1, zlib.DEFLATED, -15)
    source.write_bytes(
        header
        + compressor.compress(PAYLOAD)
        + compressor.flush()
        + struct.pack("<II", zlib.crc32(PAYLOAD), len(PAYLOAD))
    )
    repack(source, allow_grow=True)
    assert gzip_header(str(source)) == header
    assert gzip.decompress(source.read_bytes()) == PAYLOAD
    before = source.read_bytes()
    source.write_bytes(before[:3] + b"\x04" + before[4:])
    with pytest.raises(ValidationError, match="headers"):
        repack(source)


@pytest.mark.parametrize("fault", ["finalize", "metadata", "replace", "interrupt"])
def test_publication_faults_preserve_old_files(tmp_path, monkeypatch, fault):
    import undatum.cmds.repacker as module
    from undatum.common.repack_streams import _GzipWriter

    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(encoded("gz"))
    before = source.read_bytes()
    output.write_bytes(b"prior")

    def failure(*_args, **_kwargs):
        if fault == "interrupt":
            raise KeyboardInterrupt
        raise OSError("injected failure")

    if fault == "finalize":
        monkeypatch.setattr(_GzipWriter, "finish", failure)
    elif fault == "metadata":
        monkeypatch.setattr(FileSnapshot, "apply_metadata", failure)
    elif fault == "replace":
        monkeypatch.setattr(os, "replace", failure)
    else:
        monkeypatch.setattr(module, "perform_repack", failure)
    with pytest.raises(KeyboardInterrupt if fault == "interrupt" else UndatumError):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == before and output.read_bytes() == b"prior"
    assert_clean(tmp_path)


def test_preflight_change_and_new_destination_are_conflicts(tmp_path, monkeypatch):
    import undatum.cmds.repacker as module

    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(encoded("gz"))
    real = module.prepare_plan

    def prepare(*args):
        plan = real(*args)
        source.write_bytes(encoded("gz", b"new source"))
        return plan

    monkeypatch.setattr(module, "prepare_plan", prepare)
    with pytest.raises(FileConflictError):
        repack(source, output)
    assert not output.exists()
    assert_clean(tmp_path)


@pytest.mark.parametrize(
    "options",
    [
        {"level": 10},
        {"profile": "bad"},
        {"level": True},
        {"min_savings": float("nan")},
        {"threads": 0},
        {"compression": "zip"},
        {"compression": "unknown"},
    ],
)
def test_invalid_options_do_not_create_output(tmp_path, options):
    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(encoded("gz"))
    with pytest.raises(ValidationError):
        repack(source, output, **options)
    assert not output.exists()
    assert_clean(tmp_path)


def test_archives_rejected_without_dropping_members(tmp_path):
    source, output = tmp_path / "data.csv.zip", tmp_path / "out.csv.gz"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("one.csv", b"one")
        archive.writestr("two.csv", b"two")
    before = source.read_bytes()
    with pytest.raises(ValidationError, match="archive"):
        repack(source, output)
    assert source.read_bytes() == before and not output.exists()


def rich_table():
    import pyarrow as pa

    nan = struct.unpack(">d", bytes.fromhex("7ff8000000000001"))[0]
    schema = pa.schema(
        [
            pa.field("id", pa.uint64(), nullable=False, metadata={b"PARQUET:field_id": b"42"}),
            pa.field("d", pa.decimal128(20, 4)),
            pa.field("time", pa.timestamp("ns", tz="Europe/Moscow")),
            pa.field("nested", pa.list_(pa.float64())),
        ],
        metadata={b"app": b"keep", b"binary": b"\xff\0"},
    )
    return pa.Table.from_arrays(
        [
            pa.array([2**63 + 1, 7, 3], type=schema.field(0).type),
            pa.array([Decimal("12.3400"), None, Decimal("-0.0001")], type=schema.field(1).type),
            pa.array([1710000000000000001, None, 1710000000000000002], type=schema.field(2).type),
            pa.array([[nan, -0.0], None, []], type=schema.field(3).type),
        ],
        schema=schema,
    )


@pytest.mark.parametrize("rows", [0, 30])
@pytest.mark.parametrize("int96", [False, True])
def test_parquet_preserves_schema_metadata_values_and_empty(tmp_path, rows, int96):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    source, output = tmp_path / "data.parquet", tmp_path / "out.parquet"
    table = rich_table().take(pa.array([i % 3 for i in range(rows)], type=pa.int64()))
    pq.write_table(
        table, source, compression="NONE", row_group_size=7, use_deprecated_int96_timestamps=int96
    )
    result = repack(source, output, allow_grow=True, batch_size=3, row_group_size=5)
    from undatum.common.repack_columnar import arrow_values_equal

    with pq.ParquetFile(source) as before, pq.ParquetFile(output) as after:
        assert before.schema.equals(after.schema)
        assert before.schema_arrow.equals(after.schema_arrow, check_metadata=True)
        assert before.metadata.metadata == after.metadata.metadata
        a, b = before.read(), after.read()
        assert all(
            arrow_values_equal(a.column(i).combine_chunks(), b.column(i).combine_chunks())
            for i in range(a.num_columns)
        )
    assert result["rows"] == rows and result["verification"]["passed"]


def test_parquet_sorting_declaration_and_output_validation(tmp_path):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    source, output = tmp_path / "data.parquet", tmp_path / "out.parquet"
    pq.write_table(pa.table({"n": list(range(20))}), source, sorting_columns=[pq.SortingColumn(0)])
    repack(source, output, allow_grow=True, row_group_size=3)
    metadata = pq.read_metadata(output)
    assert all(
        metadata.row_group(i).sorting_columns == (pq.SortingColumn(0),)
        for i in range(metadata.num_row_groups)
    )
    for name in ("out.parquet.gz", "out.avro"):
        with pytest.raises(ValidationError):
            repack(source, tmp_path / name)
        assert not (tmp_path / name).exists()


@pytest.mark.parametrize("fault", ["value", "order", "rows", "metadata"])
def test_parquet_corruption_is_not_published(tmp_path, monkeypatch, fault):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    source, output = tmp_path / "data.parquet", tmp_path / "out.parquet"
    pq.write_table(rich_table(), source)
    before = source.read_bytes()
    real = pq.ParquetWriter

    class CorruptingWriter:
        def __init__(self, target, *args, **kwargs):
            self.target = target
            self.writer = real(target, *args, **kwargs)

        def __enter__(self):
            return self

        def __getattr__(self, name):
            return getattr(self.writer, name)

        def __exit__(self, *args):
            self.writer.close()
            table = pq.read_table(self.target)
            if fault == "value":
                table = table.set_column(
                    0, table.schema.field(0), pa.array([9, 7, 3], type=pa.uint64())
                )
            elif fault == "order":
                table = table.take(pa.array([2, 1, 0]))
            elif fault == "rows":
                table = table.slice(0, 2)
            else:
                table = table.replace_schema_metadata({b"changed": b"yes"})
            pq.write_table(table, self.target)

    monkeypatch.setattr(pq, "ParquetWriter", CorruptingWriter)
    with pytest.raises(RepackError):
        repack(source, output, allow_grow=True)
    assert source.read_bytes() == before and not output.exists()
    assert_clean(tmp_path)


@pytest.mark.parametrize("empty", [False, True])
def test_orc_preserves_types_metadata_and_nanoseconds(tmp_path, empty):
    pyorc = pytest.importorskip("pyorc")
    from pyorc.converters import TimestampConverter

    class ExactTimestamp(TimestampConverter):
        @staticmethod
        def from_orc(seconds, nanoseconds, timezone):
            return seconds, nanoseconds

        @staticmethod
        def to_orc(value, timezone):
            return value

    converters = {pyorc.TypeKind.TIMESTAMP: ExactTimestamp}
    schema = "struct<n:bigint,b:boolean,d:decimal(20,4),t:timestamp,l:array<int>>"
    source, output = tmp_path / "data.orc", tmp_path / "out.orc"
    rows = [] if empty else [(42, True, Decimal("12.3400"), (1710000000, 123456789), [1, None, 2])]
    with source.open("wb") as raw:
        with pyorc.Writer(raw, schema, converters=converters) as writer:
            writer.set_user_metadata(app=b"binary\0\xff")
            writer.writerows(rows)
    result = repack(source, output, allow_grow=True)
    with output.open("rb") as raw:
        reader = pyorc.Reader(raw, converters=converters)
        assert str(reader.schema) == schema
        assert list(reader) == rows
        assert reader.user_metadata == {"app": b"binary\0\xff"}
    assert result["rows"] == len(rows)
    if rows:
        assert result["decoded_bytes"] > 0
    with pytest.raises(ValidationError, match="level"):
        repack(source, level=3)
    with pytest.raises(ValidationError, match="LZO"):
        repack(source, compression="lzo")


@pytest.mark.parametrize("codec", ["deflate", "zstandard", "bzip2", "null", "snappy"])
@pytest.mark.parametrize("empty", [False, True])
def test_avro_preserves_encoded_unions_schema_sync_and_metadata(tmp_path, codec, empty):
    if codec == "snappy":
        pytest.importorskip("snappy")
    import avro.schema
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import DatumReader, DatumWriter

    from undatum.common.repack_avro import _blocks, read_header
    from undatum.common.repack_types import RepackBudget, RepackOptions

    source, output = tmp_path / "data.avro", tmp_path / "out.avro"
    schema = avro.schema.parse(
        json.dumps(
            {
                "type": "record",
                "name": "Typed",
                "namespace": "original",
                "fields": [
                    {"name": "n", "type": ["int", "long"]},
                    {"name": "b", "type": "boolean"},
                ],
            }
        )
    )
    rows = [] if empty else [{"n": 42, "b": True}, {"n": 2**40, "b": False}]
    with DataFileWriter(source.open("wb"), DatumWriter(), schema, codec="deflate") as writer:
        writer.set_meta("app", b"binary\0\xff")
        for row in rows:
            writer.append(row)
    result = repack(source, output, compression=codec, allow_grow=True)
    with source.open("rb") as raw:
        metadata, sync = read_header(raw)
        original = list(_blocks(raw, "deflate", sync, RepackBudget(RepackOptions(), "")))
    with output.open("rb") as raw:
        actual, new_sync = read_header(raw)
        candidate = list(_blocks(raw, codec, sync, RepackBudget(RepackOptions(), "")))
    assert new_sync == sync and candidate == original
    assert actual == {**metadata, b"avro.codec": codec.encode()}
    with DataFileReader(output.open("rb"), DatumReader()) as reader:
        assert list(reader) == rows
    assert result["rows"] == len(rows)


@pytest.mark.parametrize("option", [{"max_decoded_bytes": 10}, {"temp_limit": 10}])
def test_resource_budgets_do_not_publish(tmp_path, option):
    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(encoded("gz"))
    before = source.read_bytes()
    with pytest.raises(ResourceLimitError):
        repack(source, output, **option)
    assert source.read_bytes() == before and not output.exists()
    assert_clean(tmp_path)


def test_cli_profiles_json_output_alias_and_dry_run(tmp_path):
    from undatum.core import app

    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(encoded("gz"))
    runner = CliRunner()
    for json_flag in ("--json", "-Ojson"):
        result = runner.invoke(
            app,
            [
                "repack",
                str(source),
                "-o",
                str(output),
                "--dry-run",
                "--profile",
                "fast",
                "--level",
                "6",
                json_flag,
            ],
        )
        assert result.exit_code == 0, result.stdout + result.stderr
        document = json.loads(result.stdout)
        assert document["schema"] == "undatum.repack/1"
        assert document["effective_settings"]["level"] == 6
        assert document["status"] == "dry_run" and not output.exists()
    result = runner.invoke(
        app, ["repack", str(source), str(output), "-o", str(tmp_path / "other.csv.gz")]
    )
    assert result.exit_code != 0 and not output.exists()
