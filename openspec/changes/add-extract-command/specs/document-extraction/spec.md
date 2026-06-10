## ADDED Requirements
### Requirement: Extract Command
The system SHALL provide an `extract` command to ingest supported document files and emit
tabular outputs.

#### Scenario: Basic extraction to CSV
- **WHEN** the user runs `undatum extract report.pdf --output-format csv`
- **THEN** the system outputs CSV to stdout or the `--output` path if provided

### Requirement: Supported Input Formats
The system SHALL accept PDF, DOC, DOCX, XLS, and XLSX inputs for extraction.

#### Scenario: Docx table extraction
- **WHEN** the user runs `undatum extract survey.docx --output-format parquet`
- **THEN** the system emits a valid parquet file containing extracted tables

### Requirement: Output Formats and Destinations
The system SHALL support `--output-format {csv,json,ndjson,parquet,datapackage}` and SHALL
allow `--output` for single-file output and `--output-dir` for multi-resource output.

#### Scenario: Multiple resources to output directory
- **WHEN** the user runs `undatum extract data/*.pdf --output-format csv --output-dir out/`
- **THEN** the system writes one CSV file per detected table into `out/`

### Requirement: PDF Extraction Controls
For PDF inputs, the system SHALL support `--method {tables,text,ocr}` and `--pages` to limit
extraction to specific page ranges.

#### Scenario: Page range extraction
- **WHEN** the user runs `undatum extract report.pdf --method tables --pages 1-3`
- **THEN** the system extracts tables only from pages 1-3

### Requirement: Multi-Table Handling
By default, the system SHALL emit each detected table as a separate resource or file. When
`--flatten` is provided, it SHALL concatenate tables and add `source_table_index` and
`source_page` columns.

#### Scenario: Flattening multiple tables
- **WHEN** the user runs `undatum extract report.pdf --flatten --output-format csv`
- **THEN** the output contains a single table with `source_table_index` and `source_page`

### Requirement: Optional Dependency Handling
The system SHALL keep extraction dependencies optional and provide a clear error message with
install guidance when dependencies are missing.

#### Scenario: Missing dependencies
- **WHEN** the user runs `undatum extract report.pdf` without extraction extras installed
- **THEN** the system exits with an error that suggests installing the extract extra or plugin

### Requirement: No Tables Found Guidance
When `--method tables` finds no tables, the system SHALL return a clear message suggesting
`--method text` or `--method ocr` if appropriate.

#### Scenario: No tables detected
- **WHEN** a PDF contains no detectable tables and the user runs `--method tables`
- **THEN** the system explains no tables were found and suggests alternate methods
