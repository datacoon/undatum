## Context

Issue [#18](https://github.com/datacoon/undatum/issues/18) asks for multiprocessing on conversion
and processing, pointing at batch-oriented parallel patterns (split work into batches, map over a
process pool, reassemble). undatum already has helper modules and multi-file bulk parallelism via
iterabledata; the gap is **safe single-file / stream chunk parallelism** on the Python engine.

Constraints:
- Streaming / low memory remains a product requirement (see #34 and `--low-memory`)
- Many formats go through iterators; workers must receive picklable chunk payloads
- DuckDB paths already use internal parallelism — do not double-parallelize naively
- Default CLI behavior must stay sequential and order-preserving

## Goals / Non-Goals

**Goals:**
- Expose `--threads N` for single-file convert (Python engine) using process-pool chunk batches
- Keep peak memory roughly O(window × chunk_size), not O(file size)
- Document when parallelism helps (CPU-bound transforms on large files) vs hurts (tiny files, I/O-bound)
- Reuse existing `parallel.py` / `chunked_io.py` rather than adding joblib

**Non-Goals:**
- Replacing DuckDB’s own threading model
- Distributed / multi-machine processing
- Parallelizing order-sensitive whole-file ops (`sort`, global `dedup`) in this change
- Adding `joblib` or other new parallel frameworks

## Decisions

1. **Process pool for CPU-bound chunks; thread pool for I/O-bound**
   - Rationale: CPython GIL limits thread speedups for pure-Python record transforms; processes match
     the #18 / KDnuggets multiprocessing approach.
   - Alternative considered: joblib — rejected (new dependency; stdlib is enough).

2. **Batch/chunk tasks, not per-row `map`**
   - Default chunk size aligns with convert `batch_size` (or a dedicated parallel chunk size).
   - Alternative: one future per row — rejected (scheduling overhead dominates).

3. **Sliding window of in-flight chunks**
   - Submit at most `k * workers` chunks at a time; read next chunk as results complete.
   - Fixes current helper flaw of `list(chunks)` materializing the full dataset.

4. **Default sequential; opt-in via `--threads`**
   - `N <= 1` or omitted → sequential path (current behavior).
   - `N > 1` → process pool for eligible Python-engine convert/processing.
   - Bulk recursive convert: keep iterabledata `parallel=` / `workers=` behavior.

5. **Order policy**
   - Convert single-file parallel path SHALL preserve record order by default (tag chunks with
     sequence ids and write in order), unless a documented `--unordered` (or equivalent) is added
     later for maximum throughput.
   - Stats/validate aggregations may merge out-of-order partial results.

6. **Engine interaction**
   - If `--engine duckdb` (or auto selects DuckDB), prefer DuckDB parallelism; `--threads` may map
     to DuckDB thread settings where already supported, and MUST NOT spawn an additional process
     pool around DuckDB COPY.
   - Process-pool path activates for Python-engine conversions only.

7. **Pickling / worker entrypoints**
   - Worker functions MUST be top-level (importable) callables; pass plain dict/list chunks, not
     closures capturing open file handles.

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| Process spawn overhead on small files | Document; auto-skip parallel below a size/record threshold optional later; users omit `--threads` |
| Peak memory from N large chunks | Cap in-flight window; respect `--low-memory` / smaller `batch_size` |
| Non-picklable processors | Keep worker API narrow; fail with clear `ConfigurationError` / `ValidationError` |
| Out-of-order writes | Ordered reassembly by chunk index for convert |
| Nested parallelism with DuckDB | Gate process pool on Python engine only |

## Migration Plan

- Additive CLI behavior; no breaking flag changes.
- Update help examples: `undatum convert big.csv out.jsonl --threads 8`.
- After ship, close or comment on GitHub #18 with usage notes.

## Open Questions

- Should convert auto-enable `--threads` equal to CPU count above a file-size threshold, or remain
  strictly opt-in forever?
- Exact name for optional unordered mode (defer unless benchmarks show large gains)?
- Which follow-on command after convert is highest value: `validate` or `stats` Python path?
