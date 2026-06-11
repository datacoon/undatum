## ADDED Requirements

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
