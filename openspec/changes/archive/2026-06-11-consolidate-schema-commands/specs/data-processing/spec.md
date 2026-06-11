## ADDED Requirements

### Requirement: Schema Extraction Command
The system SHALL provide a unified `schema` command that supports multiple schema output formats through a `--format` parameter.

#### Scenario: Extract schema in Cerberus format
- **WHEN** user runs `undatum schema data.jsonl --format cerberus`
- **THEN** the system outputs Cerberus validation schema in JSON format

#### Scenario: Extract schema in JSON Schema format
- **WHEN** user runs `undatum schema data.csv --format jsonschema`
- **THEN** the system outputs JSON Schema (W3C/IETF standard) format

#### Scenario: Extract schema in Avro format
- **WHEN** user runs `undatum schema data.jsonl --format avro`
- **THEN** the system outputs Avro schema format

#### Scenario: Extract schema in Parquet format
- **WHEN** user runs `undatum schema data.parquet --format parquet`
- **THEN** the system outputs Parquet schema format

#### Scenario: Extract schema in default YAML format
- **WHEN** user runs `undatum schema data.csv` (without --format)
- **THEN** the system outputs schema in YAML format (default)

#### Scenario: Bulk extraction with format selection
- **WHEN** user runs `undatum schema_bulk data/ --format jsonschema --output schemas/`
- **THEN** the system extracts schemas in JSON Schema format for all files

### Requirement: Scheme Command Deprecation
The legacy `scheme` command SHALL be deprecated in favor of `undatum schema --format cerberus` while continuing to work with a deprecation warning during the transition period.

#### Scenario: Deprecated scheme command invocation
- **WHEN** user runs `undatum scheme data.jsonl`
- **THEN** the system shows a deprecation warning recommending `undatum schema --format cerberus`
- **AND** still produces the Cerberus schema output for backward compatibility
