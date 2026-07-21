# Change: Add Database Dump Command

## Why
Issue #13 requested DB dumps. `db query` / `db load` partially cover the need, but users still
want a clear dump path (e.g. `db dump --to parquet`) or a documented recipe. Closing the loop
serves open-data and engineering segments that move data out of databases into files.

## What Changes
- Add `db dump` (or equivalent) that exports a table/query result to a file format such as
  Parquet/CSV/JSONL.
- Reuse existing DB connection handling from `db query` / `db load`.
- Alternatively (minimum viable): document an official recipe using `db query` → file and close
  #13 — prefer a real `db dump` if effort stays small.
- Support `--to` / output format selection aligned with convert formats where practical.

## Impact
- Affected specs: `database-integration`
- Affected code: `undatum/cmds/db_*.py`, CLI wiring in `undatum/core.py` / `cli/`
- Related issues: #13; builds on `add-db-query-load`
