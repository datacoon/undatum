# data-processing Specification

## Purpose
TBD - created by archiving change add-schema-format-exports. Update Purpose after archive.
## Requirements
### Requirement: JSON Schema Export
The system SHALL support exporting schemas in JSON Schema format (W3C/IETF standard).

#### Scenario: Export schema as JSON Schema
- **WHEN** user runs `undatum schema data.csv --format jsonschema`
- **THEN** the system outputs a valid JSON Schema document following JSON Schema draft-07 specification

#### Scenario: JSON Schema includes type information
- **WHEN** user runs `undatum schema data.jsonl --format jsonschema`
- **THEN** the output includes proper type mappings (string, integer, number, boolean, object, array)

#### Scenario: JSON Schema handles nested structures
- **WHEN** user runs `undatum schema nested_data.jsonl --format jsonschema`
- **THEN** the output includes nested object definitions for STRUCT types

#### Scenario: JSON Schema includes field descriptions
- **WHEN** user runs `undatum schema data.csv --format jsonschema --autodoc`
- **THEN** the output includes description fields for each property

### Requirement: Avro Schema Export
The system SHALL support exporting schemas in Avro schema format.

#### Scenario: Export schema as Avro schema
- **WHEN** user runs `undatum schema data.jsonl --format avro`
- **THEN** the system outputs a valid Avro schema JSON document

#### Scenario: Avro schema includes proper type mappings
- **WHEN** user runs `undatum schema data.csv --format avro`
- **THEN** the output maps data types to Avro types (string, int, long, double, boolean, etc.)

#### Scenario: Avro schema handles nested records
- **WHEN** user runs `undatum schema nested_data.jsonl --format avro`
- **THEN** the output includes nested record definitions for STRUCT types

### Requirement: Parquet Schema Export
The system SHALL support exporting schemas in Parquet schema format.

#### Scenario: Export schema as Parquet schema
- **WHEN** user runs `undatum schema data.parquet --format parquet`
- **THEN** the system outputs Parquet schema information

#### Scenario: Parquet schema from other formats
- **WHEN** user runs `undatum schema data.csv --format parquet`
- **THEN** the system converts the CSV schema to Parquet schema format

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

