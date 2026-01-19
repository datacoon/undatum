# Change: Add MySQL and SQLite Database Ingestion Support

## Why

MySQL and SQLite are widely used relational databases that complement PostgreSQL support. MySQL is the second most popular open-source relational database, and SQLite is essential for embedded and local development scenarios. Adding support for these databases expands undatum's database coverage and addresses diverse use cases.

MySQL and SQLite ingestion provides:
- Fast bulk loading via `LOAD DATA INFILE` (MySQL) and optimized INSERT (SQLite)
- Multi-row INSERT statements for efficient batch operations
- Connection pooling for MySQL
- PRAGMA optimizations for SQLite bulk loading
- Support for embedded and local database scenarios

## What Changes

- **ADDED**: `MySQLIngester` class implementing MySQL-specific bulk loading
- **ADDED**: `SQLiteIngester` class implementing SQLite-specific bulk loading
- **ADDED**: Support for `LOAD DATA INFILE` for MySQL (fastest method)
- **ADDED**: Support for multi-row INSERT for MySQL when LOAD DATA not available
- **ADDED**: Support for `INSERT ... ON DUPLICATE KEY UPDATE` for MySQL upsert
- **ADDED**: PRAGMA optimizations for SQLite bulk loading
- **ADDED**: Connection pooling for MySQL via `mysql-connector-python` or SQLAlchemy
- **ADDED**: CLI support for `mysql` and `sqlite` as `--dbtype` values
- **MODIFIED**: `Ingester` class to support MySQL and SQLite database types
- **MODIFIED**: CLI `ingest` command to accept `mysql` and `sqlite` as `--dbtype` values

All changes maintain backward compatibility with existing database support.

## Impact

- **Affected specs**: `database-ingestion` capability
- **Affected code**:
  - `undatum/cmds/ingester.py` - Add `MySQLIngester` and `SQLiteIngester` classes
  - `undatum/core.py` - Update CLI to support MySQL and SQLite
  - `README.md` - Add MySQL and SQLite examples and documentation
- **Dependencies**: 
  - `mysql-connector-python` or `PyMySQL` (required for MySQL support)
  - Built-in `sqlite3` module (no additional dependency for SQLite)
  - `sqlalchemy` (optional, for connection abstraction)
- **Backward compatibility**: No breaking changes, only additions
