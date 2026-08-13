## MODIFIED Requirements

### Requirement: Data Processing Engine Selection
Commands SHALL support engine selection via `--engine` option with values `auto`, `duckdb`, or
`python`. When `auto` is selected (default), the system SHALL automatically choose DuckDB for
supported formats (CSV, JSON, Parquet) when the operation is expressible as SQL, otherwise falling
back to Python engine.

#### Scenario: Automatic engine selection for supported format
- **WHEN** user runs `undatum sort data.csv --engine auto`
- **THEN** DuckDB engine is automatically selected and used for sorting

#### Scenario: Automatic fallback to Python engine
- **WHEN** user runs `undatum sort data.xml --engine auto` (XML not supported by DuckDB)
- **THEN** Python engine is automatically selected and used

#### Scenario: Force DuckDB engine
- **WHEN** user runs `undatum frequency data.csv --engine duckdb`
- **THEN** DuckDB engine is used regardless of format support

#### Scenario: Force Python engine
- **WHEN** user runs `undatum sort data.csv --engine python`
- **THEN** Python engine is used even if DuckDB would be faster

### Requirement: DuckDB Performance Tuning
Commands SHALL support DuckDB-specific tuning options to optimize performance for large datasets.

#### Scenario: Configure DuckDB threads
- **WHEN** user runs `undatum stats large.csv --duckdb-threads 8`
- **THEN** DuckDB uses 8 threads for processing

#### Scenario: Configure DuckDB memory limit
- **WHEN** user runs `undatum join left.csv right.csv --duckdb-memory 4GB`
- **THEN** DuckDB memory limit is set to 4GB

#### Scenario: Configure DuckDB temp directory
- **WHEN** user runs `undatum sort huge.csv --duckdb-temp-dir /tmp/duckdb`
- **THEN** DuckDB uses specified directory for temporary files

## ADDED Requirements

### Requirement: DuckDB Engine for Sort Operations
The `sort` command SHALL support DuckDB engine for sorting operations on supported formats.

#### Scenario: Sort with DuckDB engine
- **WHEN** user runs `undatum sort data.csv --engine duckdb --key age`
- **THEN** sorting is performed using DuckDB SQL ORDER BY clause

### Requirement: DuckDB Engine for Frequency Operations
The `frequency` command SHALL support DuckDB engine for frequency analysis on supported formats.

#### Scenario: Frequency analysis with DuckDB engine
- **WHEN** user runs `undatum frequency data.csv --engine duckdb --field country`
- **THEN** frequency analysis is performed using DuckDB SQL GROUP BY with COUNT(*)

### Requirement: DuckDB Engine for Unique Operations
The `uniq` command SHALL support DuckDB engine for finding unique values on supported formats.

#### Scenario: Unique values with DuckDB engine
- **WHEN** user runs `undatum uniq data.csv --engine duckdb --field email`
- **THEN** unique values are found using DuckDB SQL DISTINCT or GROUP BY

### Requirement: DuckDB Engine for Sample Operations
The `sample` command SHALL support DuckDB engine for sampling operations on supported formats.

#### Scenario: Sample with DuckDB engine
- **WHEN** user runs `undatum sample data.csv --engine duckdb --size 1000`
- **THEN** sampling is performed using DuckDB SQL TABLESAMPLE or random sampling

### Requirement: DuckDB Engine for Search Operations
The `search` command SHALL support DuckDB engine for search/filter operations on supported formats.

#### Scenario: Search with DuckDB engine
- **WHEN** user runs `undatum search data.csv --engine duckdb --pattern "error"`
- **THEN** search is performed using DuckDB SQL WHERE clause filtering

### Requirement: DuckDB Engine for Deduplication Operations
The `dedup` command SHALL support DuckDB engine for deduplication on supported formats.

#### Scenario: Deduplication with DuckDB engine
- **WHEN** user runs `undatum dedup data.csv --engine duckdb --key user_id`
- **THEN** deduplication is performed using DuckDB SQL DISTINCT ON or window functions

### Requirement: DuckDB Engine for Slice Operations
The `slice` command SHALL support DuckDB engine for slicing operations on supported formats.

#### Scenario: Slice with DuckDB engine
- **WHEN** user runs `undatum slice data.csv --engine duckdb --start 100 --end 200`
- **THEN** slicing is performed using DuckDB SQL LIMIT/OFFSET

### Requirement: DuckDB Engine for Join Operations
The `join` command SHALL support DuckDB engine for join operations on supported formats.

#### Scenario: Join with DuckDB engine
- **WHEN** user runs `undatum join left.csv right.csv --engine duckdb --on user_id`
- **THEN** join is performed using DuckDB SQL JOIN operations
