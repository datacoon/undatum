# Change: Streaming Sort and Dedup

## Why
`sort` and `dedup` currently load the entire dataset — contradicting large-file positioning and
likely the next OOM reports after #34. CLI data engineers need external merge sort and
disk-backed deduplication.

## What Changes
- Implement external merge sort (spill sorted runs to temp files, then merge) for `sort`.
- Implement disk-backed uniqueness (spill set / Bloom-filter-with-exact-pass) for `dedup`.
- Keep in-memory path for small inputs; auto-select or expose flags for spill behavior.
- Document memory characteristics for sort/dedup.

## Impact
- Affected specs: `data-processing`
- Affected code: `undatum/cmds/sorter.py`, `undatum/cmds/deduplicator.py`, temp-file utilities
- Related: complements `improve-duckdb-operations` (DuckDB path may cover some formats; streaming
  Python path still required for non-duckable inputs)
