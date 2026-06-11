# Add `sql` Command — Ad-hoc DuckDB SQL Over Files

## Why

Competing tools (qsv, dsq, miller, duckdb CLI) all offer ad-hoc SQL over data files; undatum has the DuckDB infrastructure in place (engine selection, config helpers) but no way to run a user-supplied SQL query against files. This is the largest competitive feature gap identified in the 2026-06 improvement plan (workstream D2.1).

## What Changes

- New top-level command `undatum sql QUERY FILE...` executing a DuckDB SQL query over one or more data files.
- Each input file is registered as a view named after its sanitized file stem; a single input file is additionally exposed as `data`.
- Output: JSON lines (default), CSV, or Parquet via `--format`; written to stdout or `--output` file.
- Supports DuckDB resource options (`--duckdb-threads`, `--duckdb-memory`) consistent with other commands.
- The experimental MistQL-based `query` command remains unchanged but `sql` becomes the recommended ad-hoc query path.

## Impact

- Affected specs: data-processing (new capability `sql-query`)
- Affected code: `undatum/cmds/sql.py` (new), `undatum/cli/data_commands.py` (command registration), tests
