# Change: Excel Parity Across Analysis Commands

## Why
Issue #11: Excel is readable via iterabledata but gated out of `analyze` / `uniq` /
`frequency` / `select`. Open-data publishers rely on Excel sources; removing artificial format
gates is high value and relatively low effort because commands already consume row iterators.

## What Changes
- Allow XLS/XLSX inputs in analyze, uniq, frequency, select (and any sibling commands with the
  same artificial gate).
- Reuse existing iterabledata Excel readers; no new Excel parser required.
- Add tests covering Excel inputs for each unlocked command.
- Update format docs/matrix once Excel analysis support lands.

## Impact
- Affected specs: `data-processing` (and potentially `querying` for select)
- Affected code: command format allow-lists in analyzer/selector/related cmds, tests
- Related issues: #11
