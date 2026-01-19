# Change: Improve select command performance and reliability

## Why
The `select` command lacks DuckDB acceleration, uses unbounded batching, and has
inconsistent output handling. These gaps make large-file processing slow and
memory-intensive and provide limited user control for engine selection.

## What Changes
- Add DuckDB engine support with auto-detection and safe fallback to iterable
- Add `--engine` option to the CLI `select` command
- Fix batching to ensure bounded memory usage
- Unify output handling using `open_iterable` for file and stdout
- Validate required `fields` option with clear error messaging
- Apply filters in DuckDB when translatable; otherwise fallback to iterable

## Impact
- Affected specs: `querying` (new capability)
- Affected code: `undatum/cmds/selector.py`, `undatum/core.py`,
  `undatum/common/filter.py`, tests under `tests/`
