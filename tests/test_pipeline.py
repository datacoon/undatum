"""Tests for pipeline execution."""

import os
from unittest.mock import patch

from undatum.cmds.pipeline import PipelineRunner
from undatum.common.pipeline_parser import PipelineSpec


class TestPipelineRunner:
    """Test PipelineRunner class."""

    def test_init(self):
        """Test PipelineRunner initialization."""
        runner = PipelineRunner()
        assert runner.dry_run is False
        assert runner.working_dir == os.getcwd()

    def test_init_dry_run(self):
        """Test PipelineRunner initialization with dry_run."""
        runner = PipelineRunner(dry_run=True)
        assert runner.dry_run is True

    def test_run_dry_run(self):
        """Test running pipeline in dry run mode."""
        steps = [{"name": "step1", "command": "convert", "args": {"input": "test.csv"}}]
        spec = PipelineSpec(steps)
        runner = PipelineRunner(dry_run=True)

        result = runner.run(spec)
        assert result is True

    def test_run_validation_fails(self):
        """Test running pipeline with validation errors."""
        steps = [{"name": "step1", "command": "invalid_command", "args": {}}]
        spec = PipelineSpec(steps)
        runner = PipelineRunner()

        result = runner.run(spec)
        assert result is False

    @patch("undatum.cmds.pipeline.logger")
    def test_run_with_variables(self, mock_logger):
        """Test running pipeline with variable overrides."""
        steps = [{"name": "step1", "command": "convert", "args": {"input": "${INPUT_FILE}"}}]
        variables = {"INPUT_FILE": "data.csv"}
        spec = PipelineSpec(steps, variables)
        runner = PipelineRunner(dry_run=True)

        result = runner.run(spec, variables={"INPUT_FILE": "override.csv"})
        assert result is True
