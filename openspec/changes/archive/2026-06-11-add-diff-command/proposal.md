# Change: Add diff command for dataset comparison

## Why
Users need a structured way to compare dataset versions for debugging pipelines,
regression testing, and data quality audits.

## What Changes
- Add a `diff` CLI command that compares two datasets across supported formats.
- Support key-based row matching (`--key`) and unordered comparisons (`--ignore-order`).
- Add type-aware options such as `--numeric-tolerance` and `--ignore-case`.
- Provide summary output by default and optional detailed output in `json`, `csv`,
  `markdown`, or `html` via `--output-format` and `--output`.
- Add CI thresholds (`--max-added-rows`, `--max-removed-rows`,
  `--max-changed-rows`) that trigger non-zero exit status.

## Impact
- Affected specs: `data-diff` (new capability)
- Affected code: `undatum/cmds/diff.py`, `undatum/core.py`, output helpers, tests
