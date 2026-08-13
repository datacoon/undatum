## MODIFIED Requirements

### Requirement: Validation Command
The system SHALL provide comprehensive data validation capabilities with support for rule-based validation.

#### Scenario: Validate with rule file
- **WHEN** user runs `undatum validate data.csv --rules validation-rules.yml`
- **THEN** the system SHALL parse the rule file
- **AND** apply all defined rules to the dataset
- **AND** report violations grouped by severity (errors, warnings, info)
- **AND** provide summary statistics

#### Scenario: Validate with CLI options (backward compatibility)
- **WHEN** user runs `undatum validate data.csv --fields email --rule email`
- **THEN** the system SHALL behave as before
- **AND** apply the specified rule to the specified field
- **AND** maintain existing output format

#### Scenario: Violation summary reporting
- **WHEN** user runs `undatum validate data.csv --rules rules.yml`
- **THEN** the system SHALL display:
  - Total records processed
  - Violations by severity (errors, warnings, info)
  - Violations by rule type
  - Violations by field
  - Pass/fail rate

### Requirement: Rule Definition Format
Validation rules SHALL be defined in YAML or JSON format with support for multiple rule types.

#### Scenario: Field-level rules
- **WHEN** user creates a rule file with field-level rules:
  ```yaml
  rules:
    - field: email
      required: true
      type: string
      format: email
      severity: error
    
    - field: age
      type: number
      min: 0
      max: 120
      severity: warning
  ```
- **THEN** the system SHALL validate each field according to its rules
- **AND** report violations with appropriate severity

#### Scenario: Cross-field rules
- **WHEN** user creates a rule file with cross-field validation:
  ```yaml
  rules:
    - type: cross-field
      condition: "start_date <= end_date"
      fields: [start_date, end_date]
      severity: error
  ```
- **THEN** the system SHALL validate the condition across multiple fields
- **AND** report violations when the condition fails

### Requirement: Severity Levels
Validation rules SHALL support different severity levels.

#### Scenario: Hard errors
- **WHEN** a rule with `severity: error` is violated
- **THEN** the system SHALL treat it as a hard error
- **AND** count it in error statistics
- **AND** include it in error reports

#### Scenario: Soft warnings
- **WHEN** a rule with `severity: warning` is violated
- **THEN** the system SHALL treat it as a warning
- **AND** count it separately from errors
- **AND** allow validation to continue

### Requirement: Violation Reporting
The validation command SHALL provide comprehensive violation reporting.

#### Scenario: Summary report
- **WHEN** validation completes
- **THEN** the system SHALL display:
  - Total records: 1000
  - Errors: 15 (1.5%)
  - Warnings: 42 (4.2%)
  - Passed: 943 (94.3%)

#### Scenario: Detailed violation report
- **WHEN** user requests detailed report with `--violation-report violations.json`
- **THEN** the system SHALL output:
  - Record-level violation details
  - Field values that failed validation
  - Rule descriptions
  - Severity levels

### Requirement: Rule Types
The system SHALL support multiple rule types for comprehensive validation.

#### Scenario: Required field validation
- **WHEN** a field has `required: true`
- **THEN** the system SHALL report violations for missing/null values

#### Scenario: Type validation
- **WHEN** a field has `type: number`
- **THEN** the system SHALL validate that values are numeric
- **AND** report violations for non-numeric values

#### Scenario: Format validation
- **WHEN** a field has `format: email`
- **THEN** the system SHALL validate email format
- **AND** report violations for invalid formats

#### Scenario: Range validation
- **WHEN** a field has `min: 0` and `max: 100`
- **THEN** the system SHALL validate values are within range
- **AND** report violations for out-of-range values
