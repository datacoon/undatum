# Tasks

## 1. Implementation

- [x] 1.1 Create `undatum/cmds/sql.py` with `SqlExecutor` class
- [x] 1.2 Register input files as DuckDB views (sanitized stem names; `data` alias for single file)
- [x] 1.3 Support jsonl (default), csv, and parquet output to stdout or `--output`
- [x] 1.4 Wire `sql` command in `undatum/cli/data_commands.py` with DuckDB resource options
- [x] 1.5 Raise `ValidationError` / `UndatumError` subclasses for invalid input

## 2. Testing

- [x] 2.1 Unit tests for view registration and name sanitization
- [x] 2.2 Tests for query execution over CSV/JSONL/Parquet inputs
- [x] 2.3 Tests for output formats and error cases

## 3. Documentation

- [x] 3.1 Add `sql` to README command list
- [x] 3.2 CHANGELOG entry
