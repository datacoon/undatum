## Context
Multi-GB compressed JSONL → Parquet conversions OOM because writes materialize too much data.
DuckDB already spills; pyarrow `ParquetWriter` can write row-groups incrementally.

## Goals / Non-Goals
- Goals: bounded-memory Parquet convert path; explicit `--low-memory`; docs for large files.
- Non-Goals: rewriting all commands for disk-backed processing (see `add-streaming-sort-dedup`).

## Decisions
- Decision: Prefer batched `ParquetWriter` for Python path; prefer DuckDB for duckable formats
  when `--engine auto` or `--low-memory`.
- Alternatives considered: always force DuckDB (rejects non-duckable inputs); only document
  workarounds (insufficient for #34).

## Risks / Trade-offs
- Slower small-file converts if batching is too aggressive → tune row-group size; keep fast path
  for small inputs.
- DuckDB type quirks on nested JSON → fall back to streaming Python writer with clear warning.

## Migration Plan
- Additive CLI flag; default behavior improved for large files without breaking small-file UX.
- Rollback: feature-flag or revert converter write path.

## Open Questions
- Exact threshold for auto low-memory vs always-stream Parquet writes?
- Should `--low-memory` apply globally to stats/select or convert-only in v1?
