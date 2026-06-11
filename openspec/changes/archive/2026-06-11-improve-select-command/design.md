## Context
The `select` command currently uses iterable-only processing, which is slower
on large CSV/JSONL/Parquet files and does not cap memory usage when batching.
Other commands (e.g., `uniq`, `frequency`) already support DuckDB and engine
auto-detection. Filters use MistQL via `match_filter`, but there is no SQL
translation for DuckDB.

## Goals / Non-Goals
- Goals:
  - Provide DuckDB-accelerated selection for duckable formats
  - Preserve streaming behavior and bounded memory usage
  - Keep output behavior consistent between file and stdout
  - Maintain compatibility with existing CLI usage
- Non-Goals:
  - Full MistQL-to-SQL compiler
  - Changing default output formats or file-type detection rules

## Decisions
- Decision: Add DuckDB path with auto-detection and explicit `--engine` override.
  - Alternatives considered: always use DuckDB when available (rejected due to
    filter translation gaps and possible unsupported formats).
- Decision: Implement minimal filter translation for simple expressions and
  fallback to iterable if translation is not possible.
  - Alternatives considered: ignore filters in DuckDB path (rejected as unsafe).
- Decision: Use `open_iterable` for output in all cases and keep batch size
  consistent with other commands (default 1000).

## Risks / Trade-offs
- Risk: Incorrect SQL translation could yield wrong results.
  - Mitigation: Conservative translation; fallback to iterable when unsure.
- Risk: Performance regressions for small files due to overhead of engine
  detection.
  - Mitigation: Keep detection lightweight and retain iterable path for
    non-duckable formats.

## Migration Plan
No user migration required. Existing command usage continues to work. A new
optional `--engine` argument provides explicit control.

## Open Questions
- Should batch size be user-configurable for `select` to match other commands?
