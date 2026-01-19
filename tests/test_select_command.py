"""Tests for select command behavior."""
import json
from pathlib import Path

import pytest

from undatum.cmds.selector import Selector


def read_jsonl(path: str) -> list[dict]:
    """Read JSONL file into list of dicts."""
    content = Path(path).read_text().splitlines()
    return [json.loads(line) for line in content if line.strip()]


def test_select_iterable_jsonl_output(sample_csv_file, tmp_path):
    output_file = tmp_path / "select_iterable.jsonl"
    options = {
        "fields": "name,city",
        "output": str(output_file),
        "engine": "iterable",
    }

    Selector().select(sample_csv_file, options)

    assert read_jsonl(str(output_file)) == [
        {"name": "Alice", "city": "New York"},
        {"name": "Bob", "city": "London"},
    ]


def test_select_duckdb_matches_iterable(sample_csv_file, tmp_path):
    iterable_output = tmp_path / "select_iterable.jsonl"
    duckdb_output = tmp_path / "select_duckdb.jsonl"

    Selector().select(
        sample_csv_file,
        {"fields": "name,age", "output": str(iterable_output), "engine": "iterable"},
    )
    Selector().select(
        sample_csv_file,
        {"fields": "name,age", "output": str(duckdb_output), "engine": "duckdb"},
    )

    assert read_jsonl(str(iterable_output)) == read_jsonl(str(duckdb_output))


def test_select_requires_fields(sample_csv_file):
    with pytest.raises(ValueError, match="fields"):
        Selector().select(sample_csv_file, {"output": None})


def test_select_filter_translation_matches_iterable(sample_csv_file, tmp_path):
    iterable_output = tmp_path / "select_filter_iterable.jsonl"
    duckdb_output = tmp_path / "select_filter_duckdb.jsonl"
    options = {"fields": "name,age", "filter": "`age` >= 30"}

    Selector().select(
        sample_csv_file,
        {**options, "output": str(iterable_output), "engine": "iterable"},
    )
    Selector().select(
        sample_csv_file,
        {**options, "output": str(duckdb_output), "engine": "duckdb"},
    )

    assert read_jsonl(str(iterable_output)) == read_jsonl(str(duckdb_output))


def test_select_stdout_jsonl(capsys, sample_csv_file):
    Selector().select(sample_csv_file, {"fields": "name,city"})
    captured = capsys.readouterr().out.strip().splitlines()

    assert [json.loads(line) for line in captured] == [
        {"name": "Alice", "city": "New York"},
        {"name": "Bob", "city": "London"},
    ]


def test_select_large_batch(sample_csv_file, tmp_path):
    large_csv = tmp_path / "large.csv"
    rows = ["name,age,city"]
    for i in range(1505):
        rows.append(f"User{i},{20 + (i % 10)},City{i}")
    large_csv.write_text("\n".join(rows) + "\n")

    output_file = tmp_path / "select_large.jsonl"
    Selector().select(
        str(large_csv),
        {"fields": "name,age", "output": str(output_file), "engine": "iterable"},
    )

    assert len(read_jsonl(str(output_file))) == 1505
