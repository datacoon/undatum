# -*- coding: utf8 -*-
"""Tests for diff command behavior."""
import pytest

from undatum.cmds.differ import Differ


def _summary_changed_count(capsys):
    summary_line = capsys.readouterr().out.strip().splitlines()[0]
    parts = summary_line.split("changed=")
    assert len(parts) == 2
    return int(parts[1])


def test_diff_numeric_tolerance(tmp_path, capsys):
    file1 = tmp_path / "file1.csv"
    file2 = tmp_path / "file2.csv"
    file1.write_text("id,value\n1,10.00\n2,20.00\n")
    file2.write_text("id,value\n1,10.05\n2,20.00\n")

    output_file = tmp_path / "diff.json"
    differ = Differ()
    differ.diff(str(file1), str(file2), {
        "key": "id",
        "numeric_tolerance": 0.1,
        "output_format": "json",
        "output": str(output_file),
    })

    assert output_file.exists()
    assert _summary_changed_count(capsys) == 0


def test_diff_ignore_case(tmp_path, capsys):
    file1 = tmp_path / "file1.csv"
    file2 = tmp_path / "file2.csv"
    file1.write_text("id,name\n1,Alice\n")
    file2.write_text("id,name\n1,alice\n")

    output_file = tmp_path / "diff.json"
    differ = Differ()
    differ.diff(str(file1), str(file2), {
        "key": "id",
        "ignore_case": True,
        "output_format": "json",
        "output": str(output_file),
    })

    assert output_file.exists()
    assert _summary_changed_count(capsys) == 0


def test_diff_threshold_exceeded(tmp_path):
    file1 = tmp_path / "file1.csv"
    file2 = tmp_path / "file2.csv"
    file1.write_text("id,name\n1,Alice\n")
    file2.write_text("id,name\n1,Alicia\n")

    differ = Differ()
    with pytest.raises(SystemExit):
        differ.diff(str(file1), str(file2), {
            "key": "id",
            "max_changed_rows": 0,
        })


def test_diff_markdown_output(tmp_path):
    file1 = tmp_path / "file1.csv"
    file2 = tmp_path / "file2.csv"
    file1.write_text("id,name\n1,Alice\n")
    file2.write_text("id,name\n1,Alicia\n")

    output_file = tmp_path / "diff.md"
    differ = Differ()
    differ.diff(str(file1), str(file2), {
        "key": "id",
        "output_format": "markdown",
        "output": str(output_file),
    })

    content = output_file.read_text()
    assert "Diff Report" in content
