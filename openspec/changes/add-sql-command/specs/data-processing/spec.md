# Spec Delta: Data Processing — SQL Query

## ADDED Requirements

### Requirement: Ad-hoc SQL Query Command

The system SHALL provide a `sql` command that executes a user-supplied DuckDB SQL query over one or more data files and outputs the result.

#### Scenario: Query a single file

- **WHEN** the user runs `undatum sql "SELECT * FROM data LIMIT 5" data.csv`
- **THEN** the file is registered as view `data` and the first five rows are written to stdout as JSON lines

#### Scenario: Query multiple files

- **WHEN** the user runs `undatum sql "SELECT * FROM a JOIN b USING (id)" a.csv b.parquet`
- **THEN** each file is registered as a view named after its sanitized file stem and the join result is output

#### Scenario: Output format selection

- **WHEN** the user passes `--format csv` or `--format parquet` with `--output result.ext`
- **THEN** the result is written to the file in the requested format

#### Scenario: Invalid query

- **WHEN** the SQL query fails to execute
- **THEN** the command raises an `UndatumError` with the DuckDB error message and exits non-zero

#### Scenario: Missing input file

- **WHEN** an input file does not exist
- **THEN** the command raises `FileNotFoundError` with suggestions and exits non-zero
