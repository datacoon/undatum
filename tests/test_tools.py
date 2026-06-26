"""Tests for undatum's agent tool layer (undatum.tools)."""

import json
import os
import tempfile

from undatum import tools
from undatum.tools import schemas

CSV = "tests/fixtures/2cols6rows.csv"


class TestToolRegistry:
    def test_combines_foundation_and_undatum_tools(self):
        names = {d["name"] for d in schemas.TOOL_DEFINITIONS}
        # Foundation tools from iterabledata
        assert "detect_format" in names
        assert "infer_schema" in names
        assert "convert_file" in names
        # undatum-specific tools
        assert {"query_sql", "frequency", "deduplicate", "mask_fields", "sample_data"} <= names

    def test_handlers_match_definitions(self):
        names = {d["name"] for d in schemas.TOOL_DEFINITIONS}
        # translate_filter is the schema name for translate_filter_tool handler
        assert names <= set(schemas.TOOL_HANDLERS)

    def test_openai_and_anthropic_exports(self):
        openai = schemas.to_openai_functions()
        anthropic = schemas.to_anthropic_tools()
        assert len(openai) == len(schemas.TOOL_DEFINITIONS)
        assert all(f["type"] == "function" for f in openai)
        assert all("input_schema" in t for t in anthropic)


class TestFoundationDelegation:
    def test_detect_format(self):
        result = schemas.call_tool("detect_format", {"path": CSV})
        assert result["ok"] is True
        assert result["data"]["format"] == "csv"

    def test_describe_capabilities(self):
        result = tools.describe_capabilities("csv")
        assert result["ok"] is True
        assert result["data"]["id"] == "csv"


class TestUndatumTools:
    def test_frequency(self):
        result = tools.frequency(CSV, "id")
        assert result["ok"] is True
        assert result["data"]["total_rows"] == 6
        assert result["data"]["unique_values"] == 6

    def test_query_sql(self):
        result = tools.query_sql(CSV, "SELECT count(*) AS n FROM data")
        assert result["ok"] is True
        assert result["data"]["rows"][0]["n"] == 6

    def test_query_sql_rejects_empty(self):
        result = tools.query_sql(CSV, "   ")
        assert result["ok"] is False
        assert result["code"] == "invalid_query"

    def test_write_tools_require_confirm(self):
        for fn, kwargs in (
            (tools.deduplicate, {}),
            (tools.mask_fields, {"fields": ["id"]}),
            (tools.sample_data, {"n": 2}),
        ):
            result = fn("a.jsonl", "b.jsonl", **kwargs)
            assert result["ok"] is False
            assert result["code"] == "confirmation_required"

    def test_deduplicate_with_confirm(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out.jsonl")
            result = tools.deduplicate(CSV, out, confirm=True)
            assert result["ok"] is True
            assert os.path.exists(out)


class TestCallToolRouting:
    def test_unknown_tool(self):
        result = schemas.call_tool("does_not_exist", {})
        assert result["ok"] is False

    def test_string_arguments(self):
        result = schemas.call_tool("frequency", json.dumps({"path": CSV, "field": "id"}))
        assert result["ok"] is True
