"""Tests for the `undatum formats` catalog command."""

import json

from typer.testing import CliRunner

from undatum.core import app

runner = CliRunner()


class TestFormatsCommand:
    def test_list_default(self):
        result = runner.invoke(app, ["formats", "list"])
        assert result.exit_code == 0
        assert "csv" in result.stdout
        assert "parquet" in result.stdout

    def test_list_json(self):
        result = runner.invoke(app, ["formats", "list", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        ids = {row["id"] for row in data}
        assert "csv" in ids
        assert len(data) > 50

    def test_list_writable_filter(self):
        result = runner.invoke(app, ["formats", "list", "--writable", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert all(row["writable"] for row in data)

    def test_list_json_includes_capabilities(self):
        result = runner.invoke(app, ["formats", "list", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert "capabilities" in data[0]
        csv_row = next(row for row in data if row["id"] == "csv")
        assert csv_row["capabilities"].get("readable") is True

    def test_list_capabilities_columns(self):
        result = runner.invoke(app, ["formats", "list", "--capabilities"])
        assert result.exit_code == 0
        # Capability table still lists formats
        assert "csv" in result.stdout

    def test_describe(self):
        result = runner.invoke(app, ["formats", "describe", "parquet"])
        assert result.exit_code == 0
        assert "parquet" in result.stdout.lower()

    def test_describe_json(self):
        result = runner.invoke(app, ["formats", "describe", "csv", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["id"] == "csv"
        assert "capabilities" in data

    def test_describe_unknown(self):
        result = runner.invoke(app, ["formats", "describe", "totallynotaformat"])
        assert result.exit_code == 1

    def test_export(self):
        result = runner.invoke(app, ["formats", "export", "--no-capabilities"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert "csv" in data
        assert len(data) > 50

    def test_export_to_file(self, tmp_path):
        out = tmp_path / "catalog.json"
        result = runner.invoke(app, ["formats", "export", "--output", str(out)])
        assert result.exit_code == 0
        assert out.exists()
        data = json.loads(out.read_text())
        assert "csv" in data
