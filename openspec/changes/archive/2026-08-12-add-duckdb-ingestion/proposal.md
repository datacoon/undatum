# Change: Add DuckDB Database Ingestion Support

## Why

DuckDB is already a dependency in undatum and is used for counting records during ingestion. Adding DuckDB as a target database for ingestion would leverage existing integration and provide a fast analytical database option. DuckDB excels at analytical workloads and provides excellent performance for bulk loading.

DuckDB ingestion provides:
- Fast bulk loading via `COPY FROM` (similar to PostgreSQL)
- Appender API for efficient programmatic insertion
- Parquet intermediate format support
- No separate server required (embedded database)
- Excellent for analytical workloads

## What Changes

- **ADDED**: `DuckDBIngester` class implementing DuckDB-specific bulk loading
- **ADDED**: Support for `COPY FROM` command for bulk loading
- **ADDED**: Support for Appender API for streaming insertion
- **ADDED**: Support for Parquet intermediate format for large datasets
- **ADDED**: CLI support for `duckdb` as `--dbtype` value
- **MODIFIED**: `Ingester` class to support DuckDB database type
- **MODIFIED**: CLI `ingest` command to accept `duckdb` as `--dbtype` value

All changes maintain backward compatibility with existing database support. DuckDB is already a dependency, so no new dependencies are required.

## Impact

- **Affected specs**: `database-ingestion` capability
- **Affected code**:
  - `undatum/cmds/ingester.py` - Add `DuckDBIngester` class
  - `undatum/core.py` - Update CLI to support DuckDB
  - `README.md` - Add DuckDB examples and documentation
- **Dependencies**: 
  - `duckdb` (already a dependency, no new dependency required)
- **Backward compatibility**: No breaking changes, only additions
