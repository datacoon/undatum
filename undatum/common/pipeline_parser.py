# -*- coding: utf8 -*-
"""Pipeline specification parser for YAML/JSON pipeline definitions."""
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    yaml = None

logger = logging.getLogger(__name__)


class PipelineParseError(Exception):
    """Error parsing pipeline specification."""
    pass


class PipelineSpec:
    """Parsed pipeline specification."""
    
    def __init__(self, steps: List[Dict[str, Any]], variables: Optional[Dict[str, str]] = None):
        """Initialize pipeline specification.
        
        Args:
            steps: List of step definitions
            variables: Optional variable definitions
        """
        self.steps = steps
        self.variables = variables or {}
    
    def resolve_variables(self, overrides: Optional[Dict[str, str]] = None) -> 'PipelineSpec':
        """Resolve variables in steps using environment and overrides.
        
        Args:
            overrides: Optional variable overrides from CLI
            
        Returns:
            New PipelineSpec with resolved variables
        """
        # Merge variables: env vars < spec vars < CLI overrides
        resolved_vars = {}
        
        # Start with environment variables
        for key, value in os.environ.items():
            resolved_vars[key] = value
        
        # Add spec variables (can reference env vars)
        for key, value in self.variables.items():
            resolved_vars[key] = self._substitute_vars(str(value), resolved_vars)
        
        # Apply CLI overrides (highest precedence)
        if overrides:
            resolved_vars.update(overrides)
        
        # Resolve variables in steps
        resolved_steps = []
        for step in self.steps:
            resolved_step = self._resolve_step_vars(step, resolved_vars)
            resolved_steps.append(resolved_step)
        
        return PipelineSpec(resolved_steps, resolved_vars)
    
    def _resolve_step_vars(self, step: Dict[str, Any], vars: Dict[str, str]) -> Dict[str, Any]:
        """Resolve variables in a step definition.
        
        Args:
            step: Step definition
            vars: Variable dictionary
            
        Returns:
            Step with resolved variables
        """
        resolved = {}
        for key, value in step.items():
            if isinstance(value, dict):
                resolved[key] = self._resolve_step_vars(value, vars)
            elif isinstance(value, list):
                resolved[key] = [self._substitute_vars(str(item), vars) if isinstance(item, str) else item for item in value]
            elif isinstance(value, str):
                resolved[key] = self._substitute_vars(value, vars)
            else:
                resolved[key] = value
        return resolved
    
    def _substitute_vars(self, text: str, vars: Dict[str, str]) -> str:
        """Substitute variables in text using ${VAR} syntax.
        
        Args:
            text: Text with variable references
            vars: Variable dictionary
            
        Returns:
            Text with variables substituted
        """
        def replace_var(match):
            var_name = match.group(1)
            return vars.get(var_name, match.group(0))  # Return original if not found
        
        # Match ${VAR} or $VAR patterns
        pattern = r'\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)'
        return re.sub(pattern, lambda m: replace_var(m) if m.group(1) else vars.get(m.group(2), m.group(0)), text)


def parse_pipeline(file_path: str) -> PipelineSpec:
    """Parse pipeline specification from YAML or JSON file.
    
    Args:
        file_path: Path to pipeline specification file
        
    Returns:
        Parsed PipelineSpec object
        
    Raises:
        PipelineParseError: If file cannot be parsed or is invalid
    """
    path = Path(file_path)
    if not path.exists():
        raise PipelineParseError(f"Pipeline file not found: {file_path}")
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            if path.suffix.lower() in ('.yaml', '.yml'):
                if not YAML_AVAILABLE:
                    raise PipelineParseError("YAML support requires pyyaml. Install with: pip install pyyaml")
                data = yaml.safe_load(f)
            elif path.suffix.lower() == '.json':
                data = json.load(f)
            else:
                # Try to detect format
                content = f.read()
                f.seek(0)
                try:
                    data = json.loads(content)
                except json.JSONDecodeError:
                    if not YAML_AVAILABLE:
                        raise PipelineParseError("YAML support requires pyyaml. Install with: pip install pyyaml")
                    data = yaml.safe_load(content)
    except (yaml.YAMLError, json.JSONDecodeError) as e:
        raise PipelineParseError(f"Failed to parse pipeline file: {e}") from e
    except Exception as e:
        raise PipelineParseError(f"Error reading pipeline file: {e}") from e
    
    # Validate structure
    if not isinstance(data, dict):
        raise PipelineParseError("Pipeline specification must be a dictionary/object")
    
    # Extract steps
    if 'steps' not in data:
        raise PipelineParseError("Pipeline specification must contain 'steps' key")
    
    steps = data.get('steps', [])
    if not isinstance(steps, list):
        raise PipelineParseError("'steps' must be a list")
    
    if len(steps) == 0:
        raise PipelineParseError("Pipeline must contain at least one step")
    
    # Validate each step
    for i, step in enumerate(steps):
        if not isinstance(step, dict):
            raise PipelineParseError(f"Step {i+1} must be a dictionary/object")
        
        if 'command' not in step:
            raise PipelineParseError(f"Step {i+1} must contain 'command' key")
        
        if 'name' not in step:
            raise PipelineParseError(f"Step {i+1} must contain 'name' key")
    
    # Extract variables (optional)
    variables = data.get('variables', {})
    if not isinstance(variables, dict):
        raise PipelineParseError("'variables' must be a dictionary/object")
    
    return PipelineSpec(steps, variables)


def validate_pipeline(spec: PipelineSpec) -> List[str]:
    """Validate pipeline specification.
    
    Args:
        spec: PipelineSpec to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Valid command names (all existing undatum commands)
    valid_commands = {
        'convert', 'analyze', 'doc', 'package', 'extract', 'count', 'head', 'tail',
        'enum', 'reverse', 'table', 'fixlengths', 'headers', 'stats', 'frequency',
        'uniq', 'sort', 'sample', 'search', 'dedup', 'fill', 'rename', 'explode',
        'replace', 'cat', 'join', 'diff', 'exclude', 'transpose', 'sniff', 'slice',
        'fmt', 'select', 'split', 'validate', 'schema', 'query', 'flatten', 'apply',
        'ingest', 'mask'
    }
    
    for i, step in enumerate(spec.steps):
        step_name = step.get('name', f'step_{i+1}')
        command = step.get('command')
        
        if not command:
            errors.append(f"Step '{step_name}': missing 'command'")
            continue
        
        if command not in valid_commands:
            errors.append(f"Step '{step_name}': unknown command '{command}'")
        
        # Check for required 'args' key
        if 'args' not in step:
            errors.append(f"Step '{step_name}': missing 'args' key")
        elif not isinstance(step['args'], dict):
            errors.append(f"Step '{step_name}': 'args' must be a dictionary/object")
    
    return errors
