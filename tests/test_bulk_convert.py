"""Tests for the iterabledata-backed convert/bulk_convert refactor."""

import json

import pytest

from undatum.cmds.converter import Converter


def _write_csv(path, rows="a,b\n1,2\n3,4\n"):
    path.write_text(rows)


class TestSingleConvert:
    def test_csv_to_jsonl_roundtrip(self, tmp_path):
        src = tmp_path / "in.csv"
        dst = tmp_path / "out.jsonl"
        _write_csv(src)
        Converter().convert(str(src), str(dst), {"progress": False})
        lines = [json.loads(line) for line in dst.read_text().splitlines() if line.strip()]
        assert lines == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

    def test_csv_to_parquet_roundtrip(self, tmp_path):
        src = tmp_path / "in.csv"
        dst = tmp_path / "out.parquet"
        _write_csv(src)
        Converter().convert(str(src), str(dst), {"progress": False})
        assert dst.exists() and dst.stat().st_size > 0

    def test_jsonl_to_csv_roundtrip(self, tmp_path):
        src = tmp_path / "in.jsonl"
        dst = tmp_path / "out.csv"
        src.write_text('{"a": "1", "b": "2"}\n{"a": "3", "b": "4"}\n')
        Converter().convert(str(src), str(dst), {"progress": False})
        text = dst.read_text()
        assert "a" in text and "1" in text and "3" in text

    def test_csv_to_compressed_output(self, tmp_path):
        """Compressed/multi-extension outputs must not break detection (no .tmp)."""
        import gzip

        src = tmp_path / "in.csv"
        dst = tmp_path / "out.csv.gz"
        _write_csv(src)
        Converter().convert(str(src), str(dst), {"progress": False})
        assert dst.exists() and dst.stat().st_size > 0
        content = gzip.decompress(dst.read_bytes()).decode("utf-8")
        assert "1,2" in content and "3,4" in content

    def test_jsonl_to_compressed_output(self, tmp_path):
        import gzip

        src = tmp_path / "in.jsonl"
        dst = tmp_path / "out.jsonl.gz"
        src.write_text('{"a": "1", "b": "2"}\n')
        Converter().convert(str(src), str(dst), {"progress": False})
        content = gzip.decompress(dst.read_bytes()).decode("utf-8")
        assert json.loads(content.splitlines()[0]) == {"a": "1", "b": "2"}

    def test_missing_input_raises(self, tmp_path):
        from undatum.common.errors import FileNotFoundError as UndatumFileNotFound

        with pytest.raises(UndatumFileNotFound):
            Converter().convert(str(tmp_path / "nope.csv"), str(tmp_path / "out.jsonl"), {})

    @pytest.mark.parametrize("ext", ["xlsx", "xls", "ods", "xml"])
    def test_readonly_output_raises_clear_error(self, tmp_path, ext):
        """Converting to a read-only format gives an actionable ValidationError."""
        from undatum.common.errors import ValidationError

        src = tmp_path / "in.csv"
        _write_csv(src)
        with pytest.raises(ValidationError, match="read-only"):
            Converter().convert(str(src), str(tmp_path / f"out.{ext}"), {"progress": False})

    @pytest.mark.parametrize("ext", ["pb", "thrift", "capnp"])
    def test_schema_required_output_raises_clear_error(self, tmp_path, ext):
        """Schema-required formats (protobuf/thrift/capnp) give an actionable error."""
        from undatum.common.errors import ValidationError

        src = tmp_path / "in.csv"
        _write_csv(src)
        with pytest.raises(ValidationError, match="generic conversion target"):
            Converter().convert(str(src), str(tmp_path / f"out.{ext}"), {"progress": False})

    def test_writable_output_not_blocked(self, tmp_path):
        """A writable format with a codec suffix is not mistakenly blocked."""
        src = tmp_path / "in.csv"
        dst = tmp_path / "out.csv.gz"
        _write_csv(src)
        Converter().convert(str(src), str(dst), {"progress": False})
        assert dst.exists()


class TestResolveOutputFormat:
    def test_plain_extension(self):
        assert Converter._resolve_output_format("a/b/out.parquet", {}) == "parquet"

    def test_codec_suffix_stripped(self):
        assert Converter._resolve_output_format("out.csv.gz", {}) == "csv"
        assert Converter._resolve_output_format("out.jsonl.zst", {}) == "jsonl"

    def test_format_out_option_wins(self):
        assert Converter._resolve_output_format("out.bin", {"format_out": "JSONL"}) == "jsonl"

    def test_no_extension(self):
        assert Converter._resolve_output_format("out", {}) is None


class TestBulkConvert:
    def test_bulk_directory(self, tmp_path):
        raw = tmp_path / "raw"
        out = tmp_path / "out"
        raw.mkdir()
        _write_csv(raw / "one.csv")
        _write_csv(raw / "two.csv", "x,y\n5,6\n")
        Converter().bulk_convert(str(raw), str(out), {"progress": False}, to_ext="jsonl")
        assert (out / "one.jsonl").exists()
        assert (out / "two.jsonl").exists()
        rows = [json.loads(line) for line in (out / "one.jsonl").read_text().splitlines() if line]
        assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

    def test_bulk_glob(self, tmp_path):
        raw = tmp_path / "raw"
        out = tmp_path / "out"
        raw.mkdir()
        _write_csv(raw / "one.csv")
        Converter().bulk_convert(str(raw / "*.csv"), str(out), {"progress": False}, to_ext="jsonl")
        assert (out / "one.jsonl").exists()

    def test_bulk_requires_target_ext(self, tmp_path):
        raw = tmp_path / "raw"
        raw.mkdir()
        _write_csv(raw / "one.csv")
        with pytest.raises(ValueError):
            Converter().bulk_convert(str(raw), str(tmp_path / "out"), {"progress": False})
