# Change: Add Multiprocessing for Data Conversion and Processing

## Why

GitHub issue [#18](https://github.com/datacoon/undatum/issues/18) requests multiprocessing for
data conversion and processing so large files can use multiple CPU cores. Today, recursive
multi-file conversion already accepts `--threads`, but **single-file** conversion and other
CPU-bound Python-engine paths still run sequentially. On multi-core machines that leaves large
CSV/JSONL transforms much slower than necessary (same class of speedup described in the
[KDnuggets parallel large-file article](https://www.kdnuggets.com/2022/07/parallel-processing-large-file-python.html)
referenced in #18).

This is roadmap item **P1.8** in `dev/docs/undatum-improvement-recommendations.md`.

## What Changes

- **Harden** `undatum/common/parallel.py` for production use:
  - Prefer **process pools** for CPU-bound chunk work (bypass GIL), with threads only for I/O-bound
    workloads
  - Process **batches/chunks** (not one task per record) to keep overhead low
  - Bound memory with a sliding window of in-flight chunks (do not materialize the whole file)
  - Optional ordered reassembly when callers require row order
- **Wire** `--threads N` into single-file Python-engine paths where parallelism is safe:
  - Primary: `convert` (chunked record transform / format write helpers)
  - Next: order-insensitive or mergeable ops (`validate`, `stats`/`frequency` aggregation merge)
- **Keep** existing multi-file bulk convert parallelism (`--recursive` + `--threads`) as-is
- **Preserve** sequential, order-preserving behavior as the default when `--threads` is omitted
- **Clarify** CLI help: `--threads` controls worker processes/threads for undatum parallel paths;
  DuckDB continues to use its own `--duckdb-threads` / engine settings where applicable
- Progress indication already exists for several commands; ensure parallel convert reports useful
  chunk/file progress without corrupting stdout when piping

Infrastructure already present (partial): `chunked_io.py`, `parallel.py`, `progress.py`, unit tests
for helpers, CLI `--threads` on `convert`/`stats` (bulk path only for convert today).

## Impact

- Affected specs: `data-processing`
- Affected code:
  - `undatum/common/parallel.py` — windowed process-pool chunk processing, order option
  - `undatum/common/chunked_io.py` — integrate with parallel convert loop if needed
  - `undatum/cmds/converter.py` — single-file parallel path when `--threads` > 1 and engine is Python
  - Selected CPU-bound commands (`validate`, stats/frequency merge paths) as follow-on wiring
  - `undatum/cli/data_commands.py` — help text / examples for `--threads`
  - Tests + README / large-file docs
- Dependencies: no new packages (stdlib `concurrent.futures` / `multiprocessing`)
- Backward compatibility: additive; omit `--threads` → current sequential behavior
- Related changes: complements `add-streaming-parquet-low-memory` and DuckDB engine routing (prefer
  DuckDB when it already parallelizes; use process pools for non-duckable Python paths)
