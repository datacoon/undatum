## MODIFIED Requirements

### Requirement: Statistics Command
The system SHALL provide a `stats` command that generates detailed statistical analysis of data files with optional DuckDB engine for improved performance on supported formats.

#### Scenario: Generate statistics with auto engine detection (DuckDB for supported formats)
- **WHEN** user runs `undatum stats data.csv` on a CSV file
- **THEN** the system automatically detects that CSV is DuckDB-supported and uses DuckDB engine
- **AND** the system computes statistics significantly faster (10-100x) than iterable engine
- **AND** the system produces identical statistics output as iterable engine

#### Scenario: Generate statistics with explicit DuckDB engine
- **WHEN** user runs `undatum stats data.jsonl --engine duckdb` on a JSONL file
- **THEN** the system uses DuckDB engine for statistics computation
- **AND** the system processes the file using DuckDB's optimized columnar processing
- **AND** the system produces accurate statistics output

#### Scenario: Generate statistics with explicit iterable engine
- **WHEN** user runs `undatum stats data.csv --engine iterable`
- **THEN** the system uses iterable engine (row-by-row processing)
- **AND** the system produces identical statistics output regardless of engine choice
- **AND** the system maintains backward compatibility with existing behavior

#### Scenario: DuckDB engine fallback on unsupported format
- **WHEN** user runs `undatum stats data.xml --engine auto` on an XML file (not DuckDB-supported)
- **THEN** the system automatically falls back to iterable engine
- **AND** the system processes the file successfully with iterable engine
- **AND** the system logs a debug message indicating engine selection

#### Scenario: DuckDB engine fallback on query failure
- **WHEN** user runs `undatum stats data.csv --engine duckdb` and DuckDB query fails
- **THEN** the system catches the error and falls back to iterable engine
- **AND** the system logs a warning message about the fallback
- **AND** the system processes the file successfully with iterable engine

#### Scenario: Statistics accuracy maintained across engines
- **WHEN** user runs `undatum stats data.jsonl` with DuckDB engine
- **THEN** the system computes identical statistics as iterable engine:
  - **AND** unique value counts match exactly (using `COUNT(DISTINCT ...)` for accuracy)
  - **AND** uniqueness percentages match exactly
  - **AND** min/max/avg lengths match exactly
  - **AND** field types detected correctly (using hybrid sampling approach)

#### Scenario: DuckDB statistics for nested JSON structures
- **WHEN** user runs `undatum stats nested_data.jsonl` with DuckDB engine on nested JSON
- **THEN** the system handles nested structures using DuckDB's `unnest()` function
- **AND** the system correctly computes statistics for nested field paths (e.g., `user.address.city`)
- **AND** the system produces field path statistics matching iterable engine format

#### Scenario: Dictionary construction with DuckDB engine
- **WHEN** user runs `undatum stats data.csv --dictshare 70` with DuckDB engine
- **AND** a field has uniqueness percentage below 70%
- **THEN** the system uses DuckDB `GROUP BY` queries to efficiently build value frequency dictionary
- **AND** the system produces dictionary structure identical to iterable engine

#### Scenario: Progress indication with DuckDB engine
- **WHEN** user runs `undatum stats data.csv` with DuckDB engine and progress enabled
- **THEN** the system displays progress bar showing:
  - **AND** Phase 1: Row counting progress (fast COUNT query)
  - **AND** Phase 2: Statistics computation progress (indicated as single operation)
  - **AND** Processing rate or completion status

#### Scenario: Supported file formats for DuckDB engine
- **WHEN** user runs `undatum stats` with DuckDB engine on supported formats
- **THEN** the system uses DuckDB engine for:
  - **AND** CSV files (`read_csv()`)
  - **AND** JSONL files (`read_json()`)
  - **AND** JSON files (`read_json()`)
  - **AND** Parquet files (direct table reading)

#### Scenario: Compression support for DuckDB engine
- **WHEN** user runs `undatum stats data.csv.gz` with DuckDB engine
- **THEN** the system detects gzip compression
- **AND** the system uses DuckDB engine if compression is supported (gzip, zstd, raw)
- **AND** the system processes compressed file directly without manual decompression
