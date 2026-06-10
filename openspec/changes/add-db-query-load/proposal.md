# Change: Add Database Query and Load Commands

## Why

While `undatum` has an `ingest` command for loading data into databases, it lacks a unified interface for querying databases and a simplified `db load` command. Adding `db query` and `db load` commands would provide a cleaner, more intuitive interface for database operations, making databases first-class sources and sinks in the undatum ecosystem.

**Current Issues:**
1. **No query command**: Cannot query databases directly to extract data
2. **Ingest command complexity**: The `ingest` command has many options and is not intuitive for simple load operations
3. **No unified database interface**: Database operations are scattered and inconsistent
4. **Limited database querying**: Users must use external tools to query databases before processing with undatum

**Expected Benefits:**
- **Direct database querying** with `db query` command
- **Simplified data loading** with `db load` command
- **Unified database interface** for common operations
- **Better integration** between files, databases, and undatum commands
- **Streaming support** for large query results

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 5.2)

## What Changes

- **ADDED**: `undatum db query` command:
  - Execute SQL queries against databases
  - Support PostgreSQL, MySQL/MariaDB, SQLite
  - Output results in multiple formats (JSONL, CSV, Parquet)
  - Streaming support for large result sets
- **ADDED**: `undatum db load` command:
  - Simplified interface for loading data to databases
  - Wrapper around `ingest` with cleaner syntax
  - Support for same databases as `ingest`
- **ADDED**: Database connection handling:
  - Connection URI parsing and validation
  - Connection pooling for performance
  - Error handling and retry logic

All changes are additive. Existing `ingest` command continues to work unchanged.

## Impact

- **Affected specs**: `database-integration` capability
- **Affected code**:
  - New `undatum/cmds/db_query.py` module for query execution
  - New `undatum/cmds/db_load.py` module for simplified loading
  - New `undatum/core.py` - Add `db` command group with subcommands
- **Dependencies**: Existing database drivers (psycopg2, pymysql, sqlite3)
- **Backward compatibility**: Fully backward compatible - new command group only
