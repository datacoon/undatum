## ADDED Requirements

### Requirement: Database Dump Command
The system SHALL provide a `db dump` command (or an equivalently documented first-class recipe)
that exports database query/table results to a file format such as Parquet, CSV, or JSONL.

#### Scenario: Dump table to Parquet
- **WHEN** a user runs `undatum db dump` with a connection URI, table (or query), and
  `--to parquet` (or equivalent) output path
- **THEN** the system writes a Parquet file containing the exported rows

#### Scenario: Dump streams large results
- **WHEN** a dump targets a large result set
- **THEN** rows are streamed/batched to the output format rather than requiring the full result
  set in memory when the format path supports it

#### Scenario: Connection errors are actionable
- **WHEN** the database connection fails
- **THEN** the error uses the project's database error handling (masked credentials, actionable
  message)
