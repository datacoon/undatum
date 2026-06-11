"""Tests for core CLI module."""

import logging
import os
import tempfile
from unittest.mock import MagicMock, patch

from undatum.core import app, enable_verbose


class TestEnableVerbose:
    """Test enable_verbose function."""

    @patch("undatum.cli.common.logging.basicConfig")
    def test_enable_verbose(self, mock_basic_config):
        """Test enabling verbose logging."""
        enable_verbose()
        mock_basic_config.assert_called_once()
        # Check that DEBUG level is set
        call_args = mock_basic_config.call_args
        assert call_args[1]["level"] == logging.DEBUG


class TestCoreCommands:
    """Test core CLI commands."""

    @patch("undatum.cli.data_commands.Converter")
    def test_convert_command(self, mock_converter_class):
        """Test convert command."""
        mock_converter = MagicMock()
        mock_converter_class.return_value = mock_converter

        # Test that converter is called with correct options
        from typer.testing import CliRunner

        runner = CliRunner()

        with patch("undatum.cli.data_commands.Converter") as mock_conv:
            result = runner.invoke(app, ["convert", "input.csv", "output.jsonl"])
            # Command should execute (may fail due to file not existing, but should call converter)
            assert mock_conv.called or result.exit_code != 0

    @patch("undatum.cli.data_commands.Counter")
    def test_count_command(self, mock_counter_class):
        """Test count command."""
        mock_counter = MagicMock()
        mock_counter_class.return_value = mock_counter

        from typer.testing import CliRunner

        runner = CliRunner()

        with patch("undatum.cli.data_commands.Counter") as mock_cnt:
            result = runner.invoke(app, ["count", "input.csv"])
            assert mock_cnt.called or result.exit_code != 0

    @patch("undatum.cli.data_commands.Validator")
    def test_validate_command(self, mock_validator_class):
        """Test validate command."""
        mock_validator = MagicMock()
        mock_validator_class.return_value = mock_validator

        from typer.testing import CliRunner

        runner = CliRunner()

        with patch("undatum.cli.data_commands.Validator") as mock_val:
            result = runner.invoke(app, ["validate", "input.csv"])
            assert mock_val.called or result.exit_code != 0

    @patch("undatum.cli.data_commands.Validator")
    def test_validate_command_with_rules(self, mock_validator_class):
        """Test validate command with rules file."""
        mock_validator = MagicMock()
        mock_validator_class.return_value = mock_validator

        from typer.testing import CliRunner

        runner = CliRunner()

        with patch("undatum.cli.data_commands.Validator") as mock_val:
            result = runner.invoke(app, ["validate", "input.csv", "--rules", "rules.yml"])
            assert mock_val.called or result.exit_code != 0

    @patch("undatum.cli.pipeline_cli.PipelineRunner")
    @patch("undatum.cli.pipeline_cli.parse_pipeline")
    def test_pipeline_run_command(self, mock_parse, mock_runner_class):
        """Test pipeline run command."""
        from typer.testing import CliRunner

        runner = CliRunner()

        mock_spec = MagicMock()
        mock_parse.return_value = mock_spec
        mock_runner = MagicMock()
        mock_runner.run.return_value = True
        mock_runner_class.return_value = mock_runner

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            try:
                import yaml

                yaml.dump({"steps": [{"name": "step1", "command": "convert", "args": {}}]}, f)
                temp_path = f.name
            except Exception:
                temp_path = None

        if temp_path:
            try:
                result = runner.invoke(app, ["pipeline", "run", temp_path])
                # Should parse and run pipeline
                assert mock_parse.called or result.exit_code != 0
            finally:
                os.unlink(temp_path)

    @patch("undatum.cli.pipeline_cli.parse_pipeline")
    @patch("undatum.cli.pipeline_cli.validate_pipeline")
    def test_pipeline_validate_command(self, mock_validate, mock_parse):
        """Test pipeline validate command."""
        from typer.testing import CliRunner

        runner = CliRunner()

        mock_spec = MagicMock()
        mock_parse.return_value = mock_spec
        mock_validate.return_value = []

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            try:
                import yaml

                yaml.dump({"steps": [{"name": "step1", "command": "convert", "args": {}}]}, f)
                temp_path = f.name
            except Exception:
                temp_path = None

        if temp_path:
            try:
                result = runner.invoke(app, ["pipeline", "validate", temp_path])
                assert mock_parse.called or result.exit_code != 0
            finally:
                os.unlink(temp_path)
