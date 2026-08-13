# Change: Add PostgreSQL Database Ingestion Support

## Why

PostgreSQL is the most popular open-source relational database and is widely used in production environments. Adding PostgreSQL support to undatum's `ingest` command would significantly expand its utility and address a major gap in database coverage. Based on comprehensive research (see `dev/docs/DATABASE_INGESTION_RESEARCH_REPORT.md`), PostgreSQL support should be the highest priority addition after fixing existing bugs.

PostgreSQL ingestion provides:
- Fast bulk loading via `COPY FROM` (10-100x faster than INSERT)
- Upsert support via `INSERT ... ON CONFLICT`
- Transaction support for atomic batch operations
- Connection pooling for efficient connection management
- Schema validation and auto-creation capabilities

## What Changes

- **ADDED**: `PostgresIngester` class implementing PostgreSQL-specific bulk loading
- **ADDED**: Support for `COPY FROM` command for maximum performance
- **ADDED**: Support for `INSERT ... ON CONFLICT` for upsert operations
- **ADDED**: Connection pooling via `psycopg2.pool` or SQLAlchemy
- **ADDED**: Schema validation and auto-creation from data schema
- **ADDED**: Transaction management for atomic batch operations
- **ADDED**: CLI option `--mode` with values: `append`, `replace`, `upsert`
- **ADDED**: CLI option `--create-table` to auto-create tables from schema
- **MODIFIED**: `Ingester` class to support PostgreSQL database type
- **MODIFIED**: CLI `ingest` command to accept `postgresql` as `--dbtype` value

All changes maintain backward compatibility with existing MongoDB and Elasticsearch support.

## Impact

- **Affected specs**: `database-ingestion` capability
- **Affected code**:
  - `undatum/cmds/ingester.py` - Add `PostgresIngester` class
  - `undatum/core.py` - Update CLI to support PostgreSQL
  - `README.md` - Add PostgreSQL examples and documentation
- **Dependencies**: 
  - `psycopg2` or `psycopg3` (required for PostgreSQL support)
  - `sqlalchemy` (optional, for connection abstraction)
- **Backward compatibility**: No breaking changes, only additions
