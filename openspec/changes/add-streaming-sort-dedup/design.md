## Context
Sort and dedup are whole-dataset operations today. For multi-GB files they must spill.

## Goals / Non-Goals
- Goals: bounded-memory sort and dedup for record streams; correct ordered output for sort;
  exact dedup semantics by default.
- Non-Goals: distributed sort; approximate-only dedup as the default.

## Decisions
- Decision: external merge sort with configurable run size; exact dedup with optional Bloom
  prefilter only if exactness preserved via final pass.
- Alternatives considered: DuckDB-only (insufficient for non-duckable formats); always OOM with
  docs workaround (rejects positioning).

## Risks / Trade-offs
- Temp disk usage can exceed input size → document `--temp-dir` / cleanup on failure.
- Key extraction for nested records must match current sort/dedup field semantics.

## Migration Plan
- Additive/automatic spill; preserve CLI flags and output ordering guarantees.
- Rollback per-command feature flags if needed.

## Open Questions
- Default run size / memory budget?
- Interaction with existing `--engine duckdb` path when both apply?
