# Change: Add Rich Validation Rules

## Why

The current `validate` command supports only simple, single-rule validation per field via CLI options. This limits users who need complex validation scenarios with multiple rules, different severity levels (errors vs warnings), and comprehensive violation reporting. Adding YAML/JSON rule definitions would enable declarative, reusable validation configurations that are easier to maintain and share.

**Current Issues:**
1. **Single rule per field**: Can only validate one rule at a time
2. **No severity levels**: All violations are treated the same (no distinction between errors and warnings)
3. **Limited reporting**: Basic stats only, no detailed violation summaries
4. **No rule reuse**: Must specify rules via CLI each time
5. **No complex rules**: Can't combine multiple conditions or cross-field validation

**Expected Benefits:**
- **Declarative validation** via YAML/JSON rule files
- **Multiple rules per field** with different severity levels
- **Comprehensive violation reporting** with summaries and details
- **Reusable rule definitions** that can be shared across projects
- **Complex validation scenarios** with cross-field rules and conditional logic

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 3.1)

## What Changes

- **ENHANCED**: `undatum validate` command to support rule files:
  - YAML/JSON rule definition format
  - Multiple rules per field
  - Hard errors vs soft warnings
  - Cross-field validation rules
- **ADDED**: Violation summary reporting:
  - Summary statistics (total violations, by severity, by rule)
  - Detailed violation reports with record context
  - Optional JSON output for programmatic access
- **ADDED**: Rule definition format with:
  - Field-level rules (required, type, format, range)
  - Cross-field rules (dependencies, consistency)
  - Custom validation functions
  - Severity levels (error, warning, info)

All changes are backward compatible. Existing CLI-based validation continues to work.

## Impact

- **Affected specs**: `data-validation` capability
- **Affected code**:
  - `undatum/cmds/validator.py` - Enhance Validator class
  - New `undatum/common/validation_rules.py` - Rule parser and evaluator
  - `undatum/core.py` - Update validate command to support rule files
- **Dependencies**: `pyyaml` (already used in project)
- **Backward compatibility**: Fully backward compatible - existing CLI validation unchanged, new rule file support added
