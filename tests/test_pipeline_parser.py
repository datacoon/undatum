"""Tests for pipeline parser."""
import json
import tempfile
import os
from unittest.mock import patch

import pytest

from undatum.common.pipeline_parser import (
    PipelineParseError,
    PipelineSpec,
    parse_pipeline,
    validate_pipeline,
)


class TestPipelineSpec:
    """Test PipelineSpec class."""

    def test_init(self):
        """Test PipelineSpec initialization."""
        steps = [{'name': 'step1', 'command': 'convert'}]
        spec = PipelineSpec(steps)
        assert spec.steps == steps
        assert spec.variables == {}

    def test_init_with_variables(self):
        """Test PipelineSpec initialization with variables."""
        steps = [{'name': 'step1', 'command': 'convert'}]
        variables = {'VAR1': 'value1', 'VAR2': 'value2'}
        spec = PipelineSpec(steps, variables)
        assert spec.steps == steps
        assert spec.variables == variables

    def test_resolve_variables_simple(self):
        """Test resolving simple variables."""
        steps = [{'name': 'step1', 'command': 'convert', 'args': {'input': '${INPUT_FILE}'}}]
        variables = {'INPUT_FILE': 'data.csv'}
        spec = PipelineSpec(steps, variables)
        
        resolved = spec.resolve_variables()
        assert resolved.steps[0]['args']['input'] == 'data.csv'

    def test_resolve_variables_with_override(self):
        """Test resolving variables with CLI overrides."""
        steps = [{'name': 'step1', 'command': 'convert', 'args': {'input': '${INPUT_FILE}'}}]
        variables = {'INPUT_FILE': 'data.csv'}
        spec = PipelineSpec(steps, variables)
        
        resolved = spec.resolve_variables(overrides={'INPUT_FILE': 'override.csv'})
        assert resolved.steps[0]['args']['input'] == 'override.csv'

    def test_resolve_variables_nested(self):
        """Test resolving variables in nested structures."""
        steps = [{
            'name': 'step1',
            'command': 'convert',
            'args': {
                'input': '${INPUT}',
                'options': {
                    'format': '${FORMAT}'
                }
            }
        }]
        variables = {'INPUT': 'data.csv', 'FORMAT': 'jsonl'}
        spec = PipelineSpec(steps, variables)
        
        resolved = spec.resolve_variables()
        assert resolved.steps[0]['args']['input'] == 'data.csv'
        assert resolved.steps[0]['args']['options']['format'] == 'jsonl'

    def test_resolve_variables_list(self):
        """Test resolving variables in lists."""
        steps = [{'name': 'step1', 'command': 'convert', 'args': {'fields': ['${FIELD1}', '${FIELD2}']}}]
        variables = {'FIELD1': 'field1', 'FIELD2': 'field2'}
        spec = PipelineSpec(steps, variables)
        
        resolved = spec.resolve_variables()
        assert resolved.steps[0]['args']['fields'] == ['field1', 'field2']

    def test_resolve_variables_env_vars(self):
        """Test resolving environment variables."""
        steps = [{'name': 'step1', 'command': 'convert', 'args': {'input': '${HOME}'}}]
        spec = PipelineSpec(steps)
        
        with patch.dict(os.environ, {'HOME': '/test/home'}):
            resolved = spec.resolve_variables()
            assert resolved.steps[0]['args']['input'] == '/test/home'

    def test_resolve_variables_missing_var(self):
        """Test resolving with missing variable."""
        steps = [{'name': 'step1', 'command': 'convert', 'args': {'input': '${MISSING}'}}]
        spec = PipelineSpec(steps)
        
        resolved = spec.resolve_variables()
        # Missing variables should remain as-is
        assert resolved.steps[0]['args']['input'] == '${MISSING}'

    def test_substitute_vars_dollar_brace(self):
        """Test variable substitution with ${VAR} syntax."""
        spec = PipelineSpec([], {})
        result = spec._substitute_vars('Hello ${NAME}', {'NAME': 'World'})
        assert result == 'Hello World'

    def test_substitute_vars_dollar_var(self):
        """Test variable substitution with $VAR syntax."""
        spec = PipelineSpec([], {})
        result = spec._substitute_vars('Hello $NAME', {'NAME': 'World'})
        assert result == 'Hello World'


class TestParsePipeline:
    """Test parse_pipeline function."""

    def test_parse_pipeline_yaml(self):
        """Test parsing YAML pipeline."""
        pipeline_data = {
            'steps': [
                {'name': 'step1', 'command': 'convert', 'args': {}}
            ],
            'variables': {}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            import yaml
            yaml.dump(pipeline_data, f)
            temp_path = f.name

        try:
            spec = parse_pipeline(temp_path)
            assert len(spec.steps) == 1
            assert spec.steps[0]['name'] == 'step1'
        finally:
            os.unlink(temp_path)

    def test_parse_pipeline_json(self):
        """Test parsing JSON pipeline."""
        pipeline_data = {
            'steps': [
                {'name': 'step1', 'command': 'convert', 'args': {}}
            ],
            'variables': {}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(pipeline_data, f)
            temp_path = f.name

        try:
            spec = parse_pipeline(temp_path)
            assert len(spec.steps) == 1
            assert spec.steps[0]['name'] == 'step1'
        finally:
            os.unlink(temp_path)

    def test_parse_pipeline_file_not_found(self):
        """Test parsing non-existent file."""
        with pytest.raises(PipelineParseError, match="Pipeline file not found"):
            parse_pipeline('/nonexistent/pipeline.yaml')

    def test_parse_pipeline_no_steps(self):
        """Test parsing pipeline without steps."""
        pipeline_data = {'variables': {}}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(pipeline_data, f)
            temp_path = f.name

        try:
            with pytest.raises(PipelineParseError, match="must contain 'steps' key"):
                parse_pipeline(temp_path)
        finally:
            os.unlink(temp_path)

    def test_parse_pipeline_empty_steps(self):
        """Test parsing pipeline with empty steps."""
        pipeline_data = {'steps': [], 'variables': {}}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(pipeline_data, f)
            temp_path = f.name

        try:
            with pytest.raises(PipelineParseError, match="must contain at least one step"):
                parse_pipeline(temp_path)
        finally:
            os.unlink(temp_path)

    def test_parse_pipeline_missing_command(self):
        """Test parsing pipeline with missing command."""
        pipeline_data = {
            'steps': [
                {'name': 'step1', 'args': {}}
            ],
            'variables': {}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(pipeline_data, f)
            temp_path = f.name

        try:
            with pytest.raises(PipelineParseError, match="must contain 'command' key"):
                parse_pipeline(temp_path)
        finally:
            os.unlink(temp_path)

    def test_parse_pipeline_missing_name(self):
        """Test parsing pipeline with missing name."""
        pipeline_data = {
            'steps': [
                {'command': 'convert', 'args': {}}
            ],
            'variables': {}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(pipeline_data, f)
            temp_path = f.name

        try:
            with pytest.raises(PipelineParseError, match="must contain 'name' key"):
                parse_pipeline(temp_path)
        finally:
            os.unlink(temp_path)

    def test_parse_pipeline_invalid_json(self):
        """Test parsing invalid JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('invalid json {')
            temp_path = f.name

        try:
            with pytest.raises(PipelineParseError, match="Failed to parse pipeline file"):
                parse_pipeline(temp_path)
        finally:
            os.unlink(temp_path)

    def test_parse_pipeline_not_dict(self):
        """Test parsing pipeline that's not a dictionary."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([1, 2, 3], f)
            temp_path = f.name

        try:
            with pytest.raises(PipelineParseError, match="must be a dictionary"):
                parse_pipeline(temp_path)
        finally:
            os.unlink(temp_path)

    def test_parse_pipeline_invalid_variables(self):
        """Test parsing pipeline with invalid variables."""
        pipeline_data = {
            'steps': [
                {'name': 'step1', 'command': 'convert', 'args': {}}
            ],
            'variables': 'invalid'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(pipeline_data, f)
            temp_path = f.name

        try:
            with pytest.raises(PipelineParseError, match="'variables' must be a dictionary"):
                parse_pipeline(temp_path)
        finally:
            os.unlink(temp_path)

    @patch('undatum.common.pipeline_parser.YAML_AVAILABLE', False)
    def test_parse_pipeline_yaml_not_available(self):
        """Test parsing YAML when pyyaml is not available."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write('steps: []')
            temp_path = f.name

        try:
            with pytest.raises(PipelineParseError, match="YAML support requires pyyaml"):
                parse_pipeline(temp_path)
        finally:
            os.unlink(temp_path)


class TestValidatePipeline:
    """Test validate_pipeline function."""

    def test_validate_pipeline_valid(self):
        """Test validating valid pipeline."""
        steps = [
            {'name': 'step1', 'command': 'convert', 'args': {}}
        ]
        spec = PipelineSpec(steps)
        errors = validate_pipeline(spec)
        assert errors == []

    def test_validate_pipeline_missing_command(self):
        """Test validating pipeline with missing command."""
        steps = [
            {'name': 'step1', 'args': {}}
        ]
        spec = PipelineSpec(steps)
        errors = validate_pipeline(spec)
        assert len(errors) > 0
        assert any('missing' in error.lower() and 'command' in error.lower() for error in errors)

    def test_validate_pipeline_invalid_command(self):
        """Test validating pipeline with invalid command."""
        steps = [
            {'name': 'step1', 'command': 'invalid_command', 'args': {}}
        ]
        spec = PipelineSpec(steps)
        errors = validate_pipeline(spec)
        assert len(errors) > 0
        assert any('unknown command' in error.lower() for error in errors)

    def test_validate_pipeline_missing_args(self):
        """Test validating pipeline with missing args."""
        steps = [
            {'name': 'step1', 'command': 'convert'}
        ]
        spec = PipelineSpec(steps)
        errors = validate_pipeline(spec)
        assert len(errors) > 0
        assert any('missing' in error.lower() and 'args' in error.lower() for error in errors)

    def test_validate_pipeline_invalid_args(self):
        """Test validating pipeline with invalid args type."""
        steps = [
            {'name': 'step1', 'command': 'convert', 'args': 'invalid'}
        ]
        spec = PipelineSpec(steps)
        errors = validate_pipeline(spec)
        assert len(errors) > 0
        assert any("'args' must be a dictionary" in error for error in errors)
