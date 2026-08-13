"""Tests for pipeline execution."""

import os
from pathlib import Path
from unittest.mock import patch

from undatum.cmds.pipeline import PipelineRunner, build_pipeline_argv
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


def test_render_pipeline_mermaid():
    from undatum.common.pipeline_parser import render_pipeline_mermaid

    spec = PipelineSpec(
        [
            {"name": "convert_csv", "command": "convert", "args": {"input": "in.csv"}},
            {"name": "profile", "command": "stats", "args": {"input": "out.jsonl"}},
        ]
    )
    text = render_pipeline_mermaid(spec)
    assert text.startswith("flowchart LR")
    assert "convert_csv" in text
    assert "profile" in text
    assert "-->" in text


class TestBuildPipelineArgv:
    """Map pipeline YAML args onto live CLI positionals and options."""

    def test_convert_uses_positional_input_and_output(self):
        assert build_pipeline_argv("convert", {"input": "a.csv", "output": "b.jsonl"}) == [
            "convert",
            "a.csv",
            "b.jsonl",
        ]

    def test_count_uses_positional_input(self):
        assert build_pipeline_argv("count", {"input": "a.csv"}) == ["count", "a.csv"]

    def test_sql_maps_query_input_and_output_option(self):
        assert build_pipeline_argv(
            "sql",
            {"query": "SELECT * FROM data", "input": "a.csv", "output": "b.csv"},
        ) == ["sql", "SELECT * FROM data", "a.csv", "--output", "b.csv"]

    def test_dedup_maps_keys_alias_to_key_fields(self):
        assert build_pipeline_argv(
            "dedup",
            {"input": "a.csv", "output": "b.csv", "keys": "id"},
        ) == ["dedup", "a.csv", "--output", "b.csv", "--key-fields", "id"]

    def test_false_flag_emits_no_option(self):
        argv = build_pipeline_argv(
            "convert", {"input": "a.csv", "output": "b.jsonl", "progress": False}
        )
        assert argv[:3] == ["convert", "a.csv", "b.jsonl"]
        assert "--no-progress" in argv


def test_pipeline_convert_writes_jsonl(sample_csv_file, tmp_path):
    output = tmp_path / "out.jsonl"
    spec = PipelineSpec(
        [
            {
                "name": "to_jsonl",
                "command": "convert",
                "args": {"input": sample_csv_file, "output": str(output)},
            }
        ]
    )
    assert PipelineRunner().run(spec) is True
    text = Path(output).read_text(encoding="utf-8")
    assert "Alice" in text
    assert "Bob" in text


def test_pipeline_count_does_not_require_output(sample_csv_file):
    spec = PipelineSpec([{"name": "n", "command": "count", "args": {"input": sample_csv_file}}])
    assert PipelineRunner().run(spec) is True


def test_pipeline_step_output_reference(sample_csv_file, tmp_path):
    converted = tmp_path / "people.jsonl"
    spec = PipelineSpec(
        [
            {
                "name": "to_jsonl",
                "command": "convert",
                "args": {"input": sample_csv_file, "output": str(converted)},
            },
            {"name": "n", "command": "count", "args": {"input": "$to_jsonl"}},
        ]
    )
    assert PipelineRunner().run(spec) is True
    assert "Alice" in Path(converted).read_text(encoding="utf-8")


def test_pipeline_implicit_previous_output(sample_csv_file):
    spec = PipelineSpec(
        [
            {"name": "to_jsonl", "command": "convert", "args": {"input": sample_csv_file}},
            {"name": "n", "command": "count", "args": {}},
        ]
    )
    assert PipelineRunner().run(spec) is True
