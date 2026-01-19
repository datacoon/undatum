## MODIFIED Requirements

### Requirement: Schema Extraction Command
The system SHALL provide a `schema` command that extracts schema information from data files in multiple output formats with AI-powered documentation support.

#### Scenario: Extract schema with text output format
- **WHEN** user runs `undatum schema data.csv --outtype text`
- **THEN** the system outputs a human-readable formatted table showing field names, types, and descriptions

#### Scenario: Extract schema with JSON output format
- **WHEN** user runs `undatum schema data.jsonl --outtype json`
- **THEN** the system outputs schema as JSON with proper indentation

#### Scenario: Extract schema with YAML output format
- **WHEN** user runs `undatum schema data.csv --outtype yaml`
- **THEN** the system outputs schema as YAML

#### Scenario: Extract schema to file
- **WHEN** user runs `undatum schema data.csv --output schema.yaml`
- **THEN** the system writes schema to the specified file instead of stdout

#### Scenario: Extract schema with AI documentation
- **WHEN** user runs `undatum schema data.csv --autodoc --lang English`
- **THEN** the system generates field descriptions using AI service and includes them in the schema output

#### Scenario: Extract schema with AI provider selection
- **WHEN** user runs `undatum schema data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini`
- **THEN** the system uses the specified AI provider and model for documentation generation

#### Scenario: Extract schema includes record count
- **WHEN** user runs `undatum schema data.csv`
- **THEN** the system includes the total number of records in the schema output

#### Scenario: Extract schema from XLSX file
- **WHEN** user runs `undatum schema data.xlsx`
- **THEN** the system extracts schema from Excel file format

#### Scenario: Extract schema from XML file
- **WHEN** user runs `undatum schema data.xml`
- **THEN** the system extracts schema from XML file format

#### Scenario: Extract schema with engine selection
- **WHEN** user runs `undatum schema data.csv --engine auto`
- **THEN** the system automatically selects the best engine (DuckDB or iterable) for the file type

#### Scenario: Extract schema with DuckDB engine
- **WHEN** user runs `undatum schema data.csv --engine duckdb`
- **THEN** the system uses DuckDB engine for schema extraction

#### Scenario: Extract schema with iterable engine
- **WHEN** user runs `undatum schema data.xml --engine iterable`
- **THEN** the system uses iterable processing engine for schema extraction

#### Scenario: Extract schema handles compression
- **WHEN** user runs `undatum schema data.csv.gz`
- **THEN** the system correctly detects compression and file type separately

#### Scenario: Extract schema with error handling
- **WHEN** user runs `undatum schema nonexistent.csv`
- **THEN** the system provides a clear error message indicating the file was not found

#### Scenario: Bulk schema extraction with glob patterns
- **WHEN** user runs `undatum schema_bulk "data/*.csv" --output schemas/`
- **THEN** the system processes all matching CSV files using the glob pattern

#### Scenario: Bulk schema extraction from directory
- **WHEN** user runs `undatum schema_bulk data/ --output schemas/`
- **THEN** the system processes all supported files in the directory

## ADDED Requirements

### Requirement: Shared Schema Utilities
The system SHALL provide shared schema utility functions to eliminate code duplication between schema extraction and analysis commands.

#### Scenario: Schema utilities used by multiple commands
- **WHEN** both `schema` and `analyze` commands need to decompose file structures
- **THEN** they use the same shared `duckdb_decompose()` function from `undatum/common/schema_utils.py`

#### Scenario: Schema utilities support different query types
- **WHEN** analyzer uses `summarize` and schemer uses `describe`
- **THEN** the shared utility function accepts a `use_summarize` parameter to support both use cases
