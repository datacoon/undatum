"""Tests for the repack command."""

from __future__ import annotations

import gzip

import pytest

from undatum.cmds.repacker import Repacker
from undatum.common.errors import ValidationError


def _write_csv(path, rows="a,b\n1,2\n3,4\n"):
    path.write_text(rows)


def _write_low_gzip(path, payload: bytes):
    with gzip.open(path, "wb", compresslevel=1) as handle:
        handle.write(payload)


class TestRepackContainer:
    def test_repack_gzip_to_output(self, tmp_path):
        payload = ("a,b\n" + ("hello world," + "x" * 80 + "\n") * 200).encode()
        src = tmp_path / "in.csv.gz"
        dst = tmp_path / "out.csv.gz"
        _write_low_gzip(src, payload)

        result = Repacker().repack(str(src), str(dst), {"progress": False})
        assert dst.exists()
        assert gzip.decompress(dst.read_bytes()) == payload
        assert result["mode"] == "container"
        assert result["codec"] == "gz"
        # Max compression should not grow the low-level input for this payload.
        assert dst.stat().st_size <= src.stat().st_size

    def test_repack_level_overrides_max(self, tmp_path):
        payload = ("a,b\n" + ("hello world," + "x" * 80 + "\n") * 200).encode()
        src = tmp_path / "in.csv.gz"
        out_max = tmp_path / "max.csv.gz"
        out_fast = tmp_path / "fast.csv.gz"
        _write_low_gzip(src, payload)

        Repacker().repack(str(src), str(out_max), {"progress": False})
        Repacker().repack(str(src), str(out_fast), {"progress": False, "level": 1})

        assert out_fast.stat().st_size >= out_max.stat().st_size
        assert gzip.decompress(out_fast.read_bytes()) == payload

    def test_repack_inplace_atomic(self, tmp_path):
        payload = b"a,b\n1,2\n3,4\n"
        src = tmp_path / "data.csv.gz"
        _write_low_gzip(src, payload)

        result = Repacker().repack(str(src), None, {"progress": False})
        assert result["output"] == str(src)
        assert src.exists()
        assert gzip.decompress(src.read_bytes()) == payload
        # Temp leftovers from atomic rewrite must not remain.
        leftovers = list(tmp_path.glob(".undatum_repack_*"))
        assert leftovers == []

    def test_wrap_uncompressed_to_codec_output(self, tmp_path):
        src = tmp_path / "data.csv"
        dst = tmp_path / "data.csv.gz"
        _write_csv(src)

        Repacker().repack(str(src), str(dst), {"progress": False})
        content = gzip.decompress(dst.read_bytes()).decode("utf-8")
        assert "1,2" in content and "3,4" in content


class TestRepackBuiltin:
    def test_repack_parquet_zstd(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        import pyarrow.parquet as pq

        src = tmp_path / "in.parquet"
        dst = tmp_path / "out.parquet"
        table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        pq.write_table(table, src, compression="snappy")

        result = Repacker().repack(str(src), str(dst), {"progress": False})
        assert result["mode"] == "builtin"
        assert result["compression"] == "zstd"
        pf = pq.ParquetFile(dst)
        assert pf.metadata.row_group(0).column(0).compression == "ZSTD"
        assert pq.read_table(dst).to_pydict() == table.to_pydict()

    def test_repack_parquet_level_option(self, tmp_path):
        pytest.importorskip("pyarrow")
        import pyarrow as pa
        import pyarrow.parquet as pq

        src = tmp_path / "in.parquet"
        dst = tmp_path / "out.parquet"
        table = pa.table({"a": list(range(100)), "b": ["word"] * 100})
        pq.write_table(table, src, compression="snappy")

        result = Repacker().repack(
            str(src), str(dst), {"progress": False, "level": 3, "compression": "zstd"}
        )
        assert result["level"] == 3
        assert pq.read_table(dst).num_rows == 100


class TestRepackValidation:
    def test_uncompressed_without_codec_errors(self, tmp_path):
        src = tmp_path / "plain.csv"
        _write_csv(src)
        with pytest.raises(ValidationError, match="no container compression"):
            Repacker().repack(str(src), None, {"progress": False})

    def test_missing_input_raises(self, tmp_path):
        from undatum.common.errors import FileNotFoundError as UndatumFileNotFound

        with pytest.raises(UndatumFileNotFound):
            Repacker().repack(str(tmp_path / "missing.csv.gz"), str(tmp_path / "out.csv.gz"), {})


class TestRepackCli:
    def test_cli_help(self):
        from typer.testing import CliRunner

        from undatum.core import app

        runner = CliRunner()
        result = runner.invoke(app, ["repack", "--help"])
        assert result.exit_code == 0
        assert "--level" in result.stdout
        assert "progress" in result.stdout.lower()

    def test_cli_repack_gzip(self, tmp_path):
        from typer.testing import CliRunner

        from undatum.core import app

        payload = b"a,b\n1,2\n"
        src = tmp_path / "in.csv.gz"
        dst = tmp_path / "out.csv.gz"
        _write_low_gzip(src, payload)

        runner = CliRunner()
        result = runner.invoke(app, ["repack", str(src), str(dst), "--no-progress"])
        assert result.exit_code == 0, result.stdout + result.stderr
        assert gzip.decompress(dst.read_bytes()) == payload
