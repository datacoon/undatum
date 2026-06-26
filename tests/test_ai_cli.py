"""Tests for the `undatum ai` commands backed by iterable.ai."""

import json
from unittest.mock import patch

from typer.testing import CliRunner

from undatum.core import app

runner = CliRunner()


class TestAiFilter:
    def test_dsl_filter_no_llm(self):
        """Simple DSL is parsed locally without a provider."""
        result = runner.invoke(app, ["ai", "filter", "age > 30"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["ast"]["op"] == "gt"
        assert data["ast"]["field"] == "age"
        assert data["source"] == "dsl"

    def test_apply_requires_file(self):
        result = runner.invoke(app, ["ai", "filter", "age > 30", "--apply"])
        assert result.exit_code == 1


class TestAiPlan:
    def test_plan_uses_catalog(self):
        """Plan works offline using catalog metadata (no LLM)."""
        result = runner.invoke(app, ["ai", "plan", "data.csv", "out.parquet"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["source"]["format"] == "csv"
        assert data["target"]["format"] == "parquet"


class TestAiDoc:
    @patch("iterable.ai.doc.generate")
    def test_doc_delegates(self, mock_generate):
        mock_generate.return_value = "# Documentation"
        result = runner.invoke(app, ["ai", "doc", "data.csv"])
        assert result.exit_code == 0
        assert "# Documentation" in result.stdout
        mock_generate.assert_called_once()
        # Filename forwarded as first positional argument.
        assert mock_generate.call_args[0][0] == "data.csv"

    @patch("iterable.ai.doc.generate")
    def test_doc_blocks_and_format(self, mock_generate):
        mock_generate.return_value = {"general": "x"}
        result = runner.invoke(
            app,
            ["ai", "doc", "data.csv", "--format", "json", "--blocks", "general,schema"],
        )
        assert result.exit_code == 0
        kwargs = mock_generate.call_args[1]
        assert kwargs["format"] == "json"
        assert kwargs["blocks"] == ["general", "schema"]


class TestAiSuggest:
    @patch("iterable.ai.suggest.suggest_transform")
    def test_suggest_delegates(self, mock_suggest):
        mock_suggest.return_value = {"operations": []}
        result = runner.invoke(app, ["ai", "suggest", "data.csv", "drop empty columns"])
        assert result.exit_code == 0
        mock_suggest.assert_called_once()
        assert mock_suggest.call_args[0][0] == "data.csv"
        assert mock_suggest.call_args[0][1] == "drop empty columns"
