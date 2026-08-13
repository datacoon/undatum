"""Tests for core CLI module."""

import logging

from typer.testing import CliRunner

from undatum import __version__
from undatum.core import app, enable_verbose

runner = CliRunner()


class TestVersionOption:
    """Test ``undatum --version``."""

    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.stdout


class TestEnableVerbose:
    """Test enable_verbose function."""

    def test_enable_verbose(self, monkeypatch):
        recorded = {}

        def fake_basic_config(**kwargs):
            recorded.update(kwargs)

        monkeypatch.setattr("undatum.cli.common.logging.basicConfig", fake_basic_config)
        enable_verbose()
        assert recorded["level"] == logging.DEBUG


class TestCoreCommands:
    """Deterministic CLI tests against fixture files."""

    def test_convert_command(self, sample_csv_file, tmp_path):
        output = tmp_path / "out.jsonl"
        result = runner.invoke(app, ["convert", sample_csv_file, str(output)])
        assert result.exit_code == 0, result.stdout + result.stderr
        text = output.read_text(encoding="utf-8")
        assert "Alice" in text
        assert "Bob" in text

    def test_count_command(self, sample_csv_file):
        result = runner.invoke(app, ["count", sample_csv_file])
        assert result.exit_code == 0, result.stdout + result.stderr
        assert result.stdout.strip() == "2"

    def test_headers_command(self, sample_csv_file):
        result = runner.invoke(app, ["headers", sample_csv_file])
        assert result.exit_code == 0, result.stdout + result.stderr
        assert "name" in result.stdout
        assert "city" in result.stdout

    def test_validate_command_missing_file(self):
        result = runner.invoke(app, ["validate", "input.csv"])
        assert result.exit_code != 0

    def test_pipeline_validate_command(self, tmp_path):
        pipeline = tmp_path / "pipe.yml"
        pipeline.write_text(
            "steps:\n"
            "  - name: convert_csv\n"
            "    command: convert\n"
            "    args:\n"
            "      input: in.csv\n"
            "      output: out.jsonl\n",
            encoding="utf-8",
        )
        result = runner.invoke(app, ["pipeline", "validate", str(pipeline)])
        assert result.exit_code == 0, result.stdout + result.stderr

    def test_pipeline_doc_command(self, tmp_path):
        pipeline = tmp_path / "pipe.yml"
        pipeline.write_text(
            "steps:\n"
            "  - name: convert_csv\n"
            "    command: convert\n"
            "    args:\n"
            "      input: in.csv\n"
            "      output: out.jsonl\n"
            "  - name: profile\n"
            "    command: stats\n"
            "    args:\n"
            "      input: out.jsonl\n",
            encoding="utf-8",
        )
        result = runner.invoke(app, ["pipeline", "doc", str(pipeline)])
        assert result.exit_code == 0, result.stdout + result.stderr
        assert "```mermaid" in result.stdout
        assert "convert_csv" in result.stdout
        assert "-->" in result.stdout
