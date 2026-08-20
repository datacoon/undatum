---
title: "validate"
description: "undatum validate command reference"
---
# `validate`

Validates data against validation rules. Supports two modes: **rich validation with rule files** (recommended) and **legacy single-rule mode** (backward compatible).

#### Rich Validation with Rule Files

Use YAML/JSON rule files for comprehensive, reusable validation:

```bash
# Validate with rule file
undatum validate data.csv --rules validation-rules.yml
undatum validate workbook.xlsx --table Sheet2 --rules validation-rules.yml
undatum validate nested.jsonl --rules rules.yml --flatten-nested

# Filter by severity
undatum validate data.jsonl --rules rules.yml --severity error

# JSON output for CI/CD integration
undatum validate data.csv --rules rules.yml --output-format json

# Generate detailed violation report
undatum validate data.jsonl --rules rules.yml --violation-report violations.json

# Treat warnings as errors
undatum validate data.csv --rules rules.yml --fail-on-warnings
```

**Rule File Format:**

Rule files support field-level and cross-field validation with severity levels:

```yaml
rules:
  # Field-level rules
  - field: email
    name: Email Required
    description: Email field must be present
    required: true
    type: string
    format: email
    severity: error

  - field: age
    name: Age Range
    description: Age must be between 0 and 120
    type: number
    min: 0
    max: 120
    severity: warning

  - field: status
    name: Status Values
    type: string
    enum: [active, inactive, pending]
    severity: error

  # Cross-field validation
  - type: cross-field
    name: Date Range Validation
    description: End date must be after start date
    condition: "end_date >= start_date"
    fields: [start_date, end_date]
    severity: error
```

**Rule Types:**

- **Required**: `required: true` - Field must be present and non-empty
- **Type**: `type: string|number|integer|float|boolean` - Value type validation
- **Format**: `format: email|url|uuid` - Format validation
- **Range**: `min`, `max` for numbers; `min_length`, `max_length` for strings
- **Enum**: `enum: [value1, value2, ...]` - Whitelist validation
- **Pattern**: `pattern: 'regex'` - Regular expression validation
- **Custom**: `custom: 'rule_name'` - Use custom validation function from VALIDATION_RULEMAP
- **Cross-field**: `type: cross-field` with `condition` expression

**Severity Levels:**

- `error`: Hard errors that should block processing
- `warning`: Soft warnings that don't block processing
- `info`: Informational violations

**Violation Reporting:**

The validation command provides comprehensive reporting:

- **Summary statistics**: Total violations by severity, by field, by rule
- **Detailed violations**: Record-level violation details with context
- **JSON output**: Machine-readable format for CI/CD integration
- **Violation report file**: Detailed JSON report with all violations

**Example Rule Files:**

Example rule files are available in `examples/validation-rules/`:
- `basic-validation.yml` - Common field-level validation rules
- `cross-field-validation.yml` - Cross-field validation examples
- `complex-validation.yml` - Comprehensive validation scenario

#### Legacy Mode (Backward Compatible)

Simple single-rule validation for quick checks:

```bash
# Validate email addresses
undatum validate --rule common.email --fields email data.jsonl

# Validate Russian INN
undatum validate --rule ru.org.inn --fields VendorINN data.jsonl --mode stats

# Output invalid records
undatum validate --rule ru.org.inn --fields VendorINN data.jsonl --mode invalid
```

**Available built-in validation rules:**
- `common.email` - Email address validation
- `common.url` - URL validation
- `ru.org.inn` - Russian organization INN identifier
- `ru.org.ogrn` - Russian organization OGRN identifier
- `integer` - Integer validation

#### Validation Best Practices

1. **Use errors for critical issues**: Fields that must be correct for data processing
2. **Use warnings for data quality**: Issues that should be reviewed but don't block processing
3. **Organize rules by domain**: Group related rules in separate files (e.g., `user-validation.yml`, `order-validation.yml`)
4. **Version control rule files**: Track rule changes and share across teams
5. **Use cross-field rules sparingly**: They're more complex and slower to evaluate
6. **Test rules incrementally**: Start with basic rules, add complexity as needed
