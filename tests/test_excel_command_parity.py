"""Tests for Excel parity on select/uniq/frequency."""

from pathlib import Path

import pytest

from undatum.cmds.selector import Selector

FIXTURES = Path(__file__).parent / "fixtures"
XLSX = FIXTURES / "2cols6rows.xlsx"


@pytest.mark.skipif(not XLSX.exists(), reason="xlsx fixture missing")
def test_select_excel(tmp_path):
    out = tmp_path / "out.jsonl"
    Selector().select(
        str(XLSX),
        {
            "fields": "id,name",
            "output": str(out),
            "format_out": "jsonl",
            "engine": "python",
            "start_page": 0,
        },
    )
    assert out.exists() and out.stat().st_size > 0


@pytest.mark.skipif(not XLSX.exists(), reason="xlsx fixture missing")
def test_uniq_excel(tmp_path):
    out = tmp_path / "out.jsonl"
    Selector().uniq(
        str(XLSX),
        {"fields": "name", "output": str(out), "engine": "python", "start_page": 0},
    )
    assert out.exists() and out.stat().st_size > 0
