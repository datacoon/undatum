"""Regression tests for uniq DuckDB engine path."""

import csv

import pytest

from undatum.cmds.selector import Selector

pytest.importorskip("duckdb")


def test_uniq_duckdb_success_writes_output(tmp_path, caplog):
    """DuckDB uniq must write results instead of reporting unsupported engine."""
    import duckdb

    parquet_file = tmp_path / "sample.parquet"
    duckdb.execute(
        "COPY (SELECT * FROM (VALUES ('Alice'), ('Bob'), ('Alice')) t(creator)) "
        f"TO '{parquet_file}' (FORMAT PARQUET)"
    )

    out = tmp_path / "out.csv"
    with caplog.at_level("INFO"):
        Selector().uniq(
            str(parquet_file),
            {
                "fields": "creator",
                "output": str(out),
                "engine": "duckdb",
                "filetype": "parquet",
            },
        )

    assert "Engine not supported" not in caplog.text
    assert out.exists()
    rows = list(csv.reader(out.read_text().splitlines()))
    # Header + unique values (Alice, Bob) — order may vary
    assert rows[0] == ["creator"]
    values = {row[0] for row in rows[1:]}
    assert values == {"Alice", "Bob"}


def test_uniq_auto_engine_parquet(tmp_path, caplog):
    """Auto engine on parquet should use DuckDB and still emit unique values."""
    import duckdb

    parquet_file = tmp_path / "sample.parquet"
    duckdb.execute(
        "COPY (SELECT * FROM (VALUES ('a'), ('b'), ('a')) t(creator)) "
        f"TO '{parquet_file}' (FORMAT PARQUET)"
    )

    out = tmp_path / "out.csv"
    with caplog.at_level("INFO"):
        Selector().uniq(
            str(parquet_file),
            {"fields": "creator", "output": str(out), "engine": "auto"},
        )

    assert "Engine not supported" not in caplog.text
    assert out.exists() and out.stat().st_size > 0
    rows = list(csv.reader(out.read_text().splitlines()))
    values = {row[0] for row in rows[1:]}
    assert values == {"a", "b"}
