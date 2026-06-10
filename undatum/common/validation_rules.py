# -*- coding: utf8 -*-
"""Validation rule parser and evaluator for rich validation rules."""
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    yaml = None

from ..utils import get_dict_value
from ..validate import VALIDATION_RULEMAP

logger = logging.getLogger(__name__)


class ValidationRuleError(Exception):
    """Error parsing or evaluating validation rules."""
    pass


class ValidationRule:
    """Represents a single validation rule."""
    
    def __init__(self, rule_def: Dict[str, Any]):
        """Initialize validation rule from definition.
        
        Args:
            rule_def: Rule definition dictionary
        """
        self.rule_def = rule_def
        self.field = rule_def.get('field')
        self.rule_type = rule_def.get('type', 'field')
        self.severity = rule_def.get('severity', 'error')
        self.name = rule_def.get('name', '')
        self.description = rule_def.get('description', '')
        
        # Validate severity
        if self.severity not in ('error', 'warning', 'info'):
            raise ValidationRuleError(f"Invalid severity: {self.severity}. Must be 'error', 'warning', or 'info'")
    
    def evaluate(self, record: Dict[str, Any], record_index: int = 0) -> Tuple[bool, Optional[str]]:
        """Evaluate rule against a record.
        
        Args:
            record: Record to validate
            record_index: Index of record (for error messages)
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if self.rule_type == 'field':
            return self._evaluate_field_rule(record, record_index)
        elif self.rule_type == 'cross-field':
            return self._evaluate_cross_field_rule(record, record_index)
        else:
            raise ValidationRuleError(f"Unknown rule type: {self.rule_type}")
    
    def _evaluate_field_rule(self, record: Dict[str, Any], record_index: int) -> Tuple[bool, Optional[str]]:
        """Evaluate field-level rule."""
        if not self.field:
            raise ValidationRuleError("Field-level rule must specify 'field'")
        
        # Get field value (support nested fields)
        field_parts = self.field.split('.')
        values = get_dict_value(record, field_parts)
        value = values[0] if len(values) > 0 else None
        
        # Check required
        if self.rule_def.get('required', False):
            if value is None or value == '':
                return False, f"Field '{self.field}' is required but is missing or empty"
        
        # Skip other validations if value is None/empty (unless required)
        if value is None or value == '':
            return True, None
        
        # Type validation
        expected_type = self.rule_def.get('type')
        if expected_type:
            type_valid, type_msg = self._validate_type(value, expected_type)
            if not type_valid:
                return False, type_msg
        
        # Format validation
        format_type = self.rule_def.get('format')
        if format_type:
            format_valid, format_msg = self._validate_format(value, format_type)
            if not format_valid:
                return False, format_msg
        
        # Range validation for numbers
        if isinstance(value, (int, float)) or (isinstance(value, str) and self._is_numeric(value)):
            num_value = float(value) if isinstance(value, str) else value
            if 'min' in self.rule_def:
                if num_value < self.rule_def['min']:
                    return False, f"Field '{self.field}' value {value} is less than minimum {self.rule_def['min']}"
            if 'max' in self.rule_def:
                if num_value > self.rule_def['max']:
                    return False, f"Field '{self.field}' value {value} is greater than maximum {self.rule_def['max']}"
        
        # Length validation for strings
        if isinstance(value, str):
            if 'min_length' in self.rule_def:
                if len(value) < self.rule_def['min_length']:
                    return False, f"Field '{self.field}' length {len(value)} is less than minimum {self.rule_def['min_length']}"
            if 'max_length' in self.rule_def:
                if len(value) > self.rule_def['max_length']:
                    return False, f"Field '{self.field}' length {len(value)} is greater than maximum {self.rule_def['max_length']}"
        
        # Enum/whitelist validation
        if 'enum' in self.rule_def:
            allowed_values = self.rule_def['enum']
            if value not in allowed_values:
                return False, f"Field '{self.field}' value '{value}' is not in allowed values: {allowed_values}"
        
        # Regex validation
        if 'pattern' in self.rule_def:
            pattern = self.rule_def['pattern']
            if not re.match(pattern, str(value)):
                return False, f"Field '{self.field}' value '{value}' does not match pattern '{pattern}'"
        
        # Custom validation function
        if 'custom' in self.rule_def:
            custom_func_name = self.rule_def['custom']
            if custom_func_name in VALIDATION_RULEMAP:
                val_func = VALIDATION_RULEMAP[custom_func_name]
                if not val_func(value):
                    return False, f"Field '{self.field}' failed custom validation '{custom_func_name}'"
            else:
                logger.warning(f"Custom validation function '{custom_func_name}' not found in VALIDATION_RULEMAP")
        
        return True, None
    
    def _evaluate_cross_field_rule(self, record: Dict[str, Any], record_index: int) -> Tuple[bool, Optional[str]]:
        """Evaluate cross-field rule."""
        condition = self.rule_def.get('condition')
        fields = self.rule_def.get('fields', [])
        
        if not condition:
            raise ValidationRuleError("Cross-field rule must specify 'condition'")
        if not fields:
            raise ValidationRuleError("Cross-field rule must specify 'fields'")
        
        # Get field values
        field_values = {}
        for field in fields:
            field_parts = field.split('.')
            values = get_dict_value(record, field_parts)
            field_values[field] = values[0] if len(values) > 0 else None
        
        # Evaluate condition (simple expression evaluation)
        try:
            # Create a safe evaluation context with field values
            # Replace field names with safe variable names
            safe_vars = {}
            eval_expr = condition
            for field in fields:
                # Create safe variable name (replace dots with underscores)
                safe_var = field.replace('.', '_').replace('-', '_')
                safe_vars[safe_var] = field_values[field]
                # Replace field references in condition
                eval_expr = eval_expr.replace(field, safe_var)
            
            # Evaluate the expression with safe context
            # Only allow basic comparisons and arithmetic
            allowed_names = {
                'None': None,
                'True': True,
                'False': False,
                **safe_vars
            }
            result = eval(eval_expr, {"__builtins__": {}}, allowed_names)
            
            if not result:
                return False, f"Cross-field condition '{condition}' failed for fields {fields}"
        except Exception as e:
            logger.warning(f"Error evaluating cross-field condition '{condition}': {e}")
            return False, f"Error evaluating cross-field condition: {str(e)}"
        
        return True, None
    
    def _validate_type(self, value: Any, expected_type: str) -> Tuple[bool, Optional[str]]:
        """Validate value type."""
        type_map = {
            'string': str,
            'number': (int, float),
            'integer': int,
            'float': float,
            'boolean': bool,
            'date': str,  # Date validation would need date parsing
        }
        
        if expected_type not in type_map:
            logger.warning(f"Unknown type '{expected_type}', skipping type validation")
            return True, None
        
        expected = type_map[expected_type]
        
        # Special handling for number/integer/float with string values
        if expected_type in ('number', 'integer', 'float') and isinstance(value, str):
            try:
                if expected_type == 'integer':
                    int(value)
                elif expected_type == 'float':
                    float(value)
                else:  # number
                    float(value)
                return True, None
            except ValueError:
                return False, f"Value '{value}' cannot be converted to {expected_type}"
        
        if isinstance(value, expected):
            return True, None
        else:
            return False, f"Value '{value}' is not of type {expected_type}"
    
    def _validate_format(self, value: Any, format_type: str) -> Tuple[bool, Optional[str]]:
        """Validate value format."""
        if not isinstance(value, str):
            return False, f"Format validation requires string value, got {type(value).__name__}"
        
        format_validators = {
            'email': lambda v: '@' in v and '.' in v.split('@')[1] if '@' in v else False,
            'url': lambda v: v.startswith(('http://', 'https://', 'ftp://')),
            'uuid': lambda v: bool(re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', v, re.I)),
        }
        
        if format_type in format_validators:
            if format_type in VALIDATION_RULEMAP:
                # Use existing validation function if available
                val_func = VALIDATION_RULEMAP.get(f'common.{format_type}') or VALIDATION_RULEMAP.get(format_type)
                if val_func:
                    if val_func(value):
                        return True, None
                    else:
                        return False, f"Value '{value}' does not match {format_type} format"
            else:
                # Use built-in validator
                if format_validators[format_type](value):
                    return True, None
                else:
                    return False, f"Value '{value}' does not match {format_type} format"
        else:
            logger.warning(f"Unknown format type '{format_type}', skipping format validation")
            return True, None
    
    def _is_numeric(self, value: str) -> bool:
        """Check if string value is numeric."""
        try:
            float(value)
            return True
        except ValueError:
            return False


class ValidationRuleSet:
    """Collection of validation rules."""
    
    def __init__(self, rules: List[ValidationRule]):
        """Initialize rule set.
        
        Args:
            rules: List of ValidationRule objects
        """
        self.rules = rules
    
    def validate_record(self, record: Dict[str, Any], record_index: int = 0) -> List[Dict[str, Any]]:
        """Validate a record against all rules.
        
        Args:
            record: Record to validate
            record_index: Index of record
            
        Returns:
            List of violation dictionaries
        """
        violations = []
        
        for rule in self.rules:
            is_valid, error_msg = rule.evaluate(record, record_index)
            if not is_valid:
                violations.append({
                    'rule_name': rule.name or f"Rule for {rule.field or 'cross-field'}",
                    'field': rule.field,
                    'severity': rule.severity,
                    'message': error_msg or f"Validation failed for {rule.field or 'cross-field'}",
                    'record_index': record_index,
                    'rule_type': rule.rule_type
                })
        
        return violations


def parse_validation_rules(file_path: str) -> ValidationRuleSet:
    """Parse validation rules from YAML or JSON file.
    
    Args:
        file_path: Path to rule file
        
    Returns:
        ValidationRuleSet object
        
    Raises:
        ValidationRuleError: If file cannot be parsed or is invalid
    """
    path = Path(file_path)
    if not path.exists():
        raise ValidationRuleError(f"Rule file not found: {file_path}")
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            if path.suffix.lower() in ('.yaml', '.yml'):
                if not YAML_AVAILABLE:
                    raise ValidationRuleError("YAML support requires pyyaml. Install with: pip install pyyaml")
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
                        raise ValidationRuleError("YAML support requires pyyaml. Install with: pip install pyyaml")
                    data = yaml.safe_load(content)
    except (yaml.YAMLError, json.JSONDecodeError) as e:
        raise ValidationRuleError(f"Failed to parse rule file: {e}") from e
    except Exception as e:
        raise ValidationRuleError(f"Error reading rule file: {e}") from e
    
    # Validate structure
    if not isinstance(data, dict):
        raise ValidationRuleError("Rule file must contain a dictionary/object")
    
    if 'rules' not in data:
        raise ValidationRuleError("Rule file must contain 'rules' key")
    
    rules_list = data.get('rules', [])
    if not isinstance(rules_list, list):
        raise ValidationRuleError("'rules' must be a list")
    
    # Parse rules
    validation_rules = []
    for i, rule_def in enumerate(rules_list):
        if not isinstance(rule_def, dict):
            raise ValidationRuleError(f"Rule {i+1} must be a dictionary/object")
        
        try:
            rule = ValidationRule(rule_def)
            validation_rules.append(rule)
        except Exception as e:
            raise ValidationRuleError(f"Error parsing rule {i+1}: {e}") from e
    
    return ValidationRuleSet(validation_rules)
