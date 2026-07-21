"""Tests for convert --low-memory / DuckDB parquet path and pyarrow availability."""

from undatum.cmds.converter import LOW_MEMORY_BATCH_SIZE, Converter


def test_pyarrow_importable():
    import pyarrow  # noqa: F401


def test_low_memory_reduces_default_batch_size():
    kwargs = Converter()._build_convert_kwargs({"low_memory": True, "progress": False}, limit=100)
    assert kwargs["batch_size"] == LOW_MEMORY_BATCH_SIZE


def test_convert_csv_to_parquet_low_memory(tmp_path):
    src = tmp_path / "in.csv"
    dst = tmp_path / "out.parquet"
    src.write_text("name,age\nAda,36\nGrace,85\n", encoding="utf-8")
    Converter().convert(
        str(src),
        str(dst),
        {"progress": False, "low_memory": True, "summary": False},
    )
    assert dst.exists() and dst.stat().st_size > 0


def test_convert_csv_to_parquet_default(tmp_path):
    src = tmp_path / "in.csv"
    dst = tmp_path / "out.parquet"
    src.write_text("name,age\nAda,36\n", encoding="utf-8")
    Converter().convert(str(src), str(dst), {"progress": False, "summary": False})
    assert dst.exists() and dst.stat().st_size > 0
