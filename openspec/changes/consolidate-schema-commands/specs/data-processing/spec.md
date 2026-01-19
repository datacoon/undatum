## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: Scheme Command
**Reason**: Consolidated into unified `schema` command with format selection. The `scheme` command created user confusion and maintenance burden.

**Migration**: Users should replace `undatum scheme` with `undatum schema --format cerberus`. The `scheme` command will show a deprecation warning and redirect to the new command during the transition period.
