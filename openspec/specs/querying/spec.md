# querying Specification

## Purpose
TBD - created by archiving change improve-select-command. Update Purpose after archive.
## Requirements
### Requirement: DuckDB-accelerated select with safe fallback
The system SHALL support a DuckDB execution path for the `select` command on
duckable formats, and SHALL fall back to the iterable path when DuckDB cannot be
used.

#### Scenario: DuckDB path for compatible input
- **WHEN** the input format is duckable and engine is `auto`
- **THEN** the system uses DuckDB to select the requested fields

#### Scenario: Fallback when DuckDB is unavailable or fails
- **WHEN** DuckDB is unavailable, errors, or the input format is not duckable
- **THEN** the system falls back to the iterable path without aborting

### Requirement: Explicit engine override for select
The system SHALL allow users to force the `select` execution engine via a CLI
option.

#### Scenario: User forces iterable engine
- **WHEN** the user passes `--engine iterable`
- **THEN** the system uses the iterable path even if DuckDB is available

### Requirement: Filter handling across engines
The system SHALL apply filter expressions in both execution paths, translating
filters to SQL where safe and otherwise falling back to the iterable path.

#### Scenario: Filter translated to SQL for DuckDB
- **WHEN** a filter expression is provided and is safely translatable to SQL
- **THEN** the system applies it in the DuckDB query

#### Scenario: Filter not translatable to SQL
- **WHEN** a filter expression cannot be safely translated to SQL
- **THEN** the system falls back to the iterable path and applies the filter

### Requirement: Bounded batching during selection
The system SHALL flush output in fixed-size batches to avoid unbounded memory
growth.

#### Scenario: Large input with default batching
- **WHEN** processing large input files
- **THEN** the system flushes output at the configured batch size

### Requirement: Required fields validation
The system SHALL validate that the `fields` option is provided for `select`.

#### Scenario: Missing fields option
- **WHEN** the user omits `fields`
- **THEN** the system returns a clear error before processing begins

