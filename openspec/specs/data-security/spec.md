# data-security Specification

## Purpose
Mask or redact sensitive fields (`undatum mask`) so datasets can be shared without exposing PII.
## Requirements
### Requirement: Mask Command
The system SHALL provide a `mask` command for anonymizing sensitive data fields.

#### Scenario: Mask fields with redact method
- **WHEN** user runs `undatum mask input.csv --fields email,phone --method redact --output masked.csv`
- **THEN** email and phone fields are replaced with fixed token (e.g., `***`)

#### Scenario: Mask fields with hash method
- **WHEN** user runs `undatum mask input.jsonl --fields user_id --method hash --output masked.jsonl`
- **THEN** user_id field is replaced with deterministic hash, preserving ability to join on hashed
  values while hiding original identities

#### Scenario: Mask fields with randomize method
- **WHEN** user runs `undatum mask input.csv --fields age,email --method randomize --output masked.csv`
- **THEN** age and email fields are replaced with random but type-compatible values (e.g., random
  age in reasonable range, random email format)

### Requirement: Multiple Field Masking
The mask command SHALL support masking multiple fields in a single operation.

#### Scenario: Mask multiple fields
- **WHEN** user runs `undatum mask input.csv --fields ssn,email,phone --method hash`
- **THEN** all specified fields are masked using the same method

### Requirement: Format Support
The mask command SHALL support masking data in multiple formats (CSV, JSONL, JSON).

#### Scenario: Mask CSV data
- **WHEN** user runs `undatum mask data.csv --fields email --method redact`
- **THEN** CSV data is masked and output in CSV format

#### Scenario: Mask JSONL data
- **WHEN** user runs `undatum mask data.jsonl --fields phone --method hash`
- **THEN** JSONL data is masked and output in JSONL format

