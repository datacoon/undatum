## 1. Parallel Infrastructure Hardening
- [x] 1.1 Create `undatum/common/chunked_io.py` (chunked reader/writer helpers)
- [x] 1.2 Create initial `undatum/common/parallel.py` (thread/process wrappers)
- [x] 1.3 Create `undatum/common/progress.py` progress helpers
- [x] 1.4 Fix memory behavior in `parallel_map` / `parallel_process_chunks`
  - Replace full `list(chunks)` materialization with a bounded in-flight window
  - Keep peak memory O(window × chunk_size)
- [x] 1.5 Add ordered chunk reassembly API
  - Tag chunks with sequence ids; yield/write in input order when requested
- [x] 1.6 Prefer process pool for CPU-bound ops; document thread pool for I/O-bound
  - Top-level picklable worker entrypoints
  - Clear errors when payloads are not picklable

## 2. Convert Command Integration (issue #18 primary)
- [x] 2.1 Multi-file bulk convert already honors `--threads` via iterabledata
- [x] 2.2 Wire `--threads N` into single-file Python-engine `convert`
  - Chunk input with existing batch/chunk helpers
  - Process chunks in a process pool when `N > 1`
  - Preserve record order on write by default
- [x] 2.3 Skip process-pool path when DuckDB engine is selected
  - Do not nest undatum process pools around DuckDB COPY
  - Map threads to DuckDB settings only where already supported
- [x] 2.4 Respect `--low-memory` / `batch_size` when choosing chunk size and window
- [x] 2.5 Update `convert` help text and examples for `--threads`

## 3. Follow-on CPU-Bound Commands
- [x] 3.1 Add process-pool chunk processing to `validate` (independent record checks)
- [x] 3.2 Add parallel partial aggregates + merge for Python-engine `stats` / `frequency`
  - Only where merge is associative; keep DuckDB path on DuckDB threads
- [x] 3.3 Explicitly exclude order-sensitive whole-file ops (`sort`, global `dedup`) from this change

## 4. Progress & CLI Consistency
- [x] 4.1 Ensure progress bars work with parallel convert without breaking piped stdout
- [x] 4.2 Align `--threads` help across convert / validate / stats (workers, not DuckDB-only)
- [x] 4.3 Document interaction: `--threads` vs `--duckdb-threads` vs `--engine`

## 5. Testing
- [x] 5.1 Unit tests for current chunked I/O and parallel helpers
- [x] 5.2 Unit tests for windowed process-pool processing and ordered reassembly
- [x] 5.3 Integration: `convert` with `--threads 2` matches sequential row content/order
- [x] 5.4 Integration: DuckDB engine convert does not spawn process pool
- [x] 5.5 Benchmarks (optional): sequential vs `--threads N` on a large CPU-bound fixture
  - Document expected gains and when parallelism is not worthwhile
  - Covered via docs notes (small files may be slower); no dedicated benchmark suite required

## 6. Documentation
- [x] 6.1 README / large-file section: multiprocessing examples for convert
- [x] 6.2 Link behavior to GitHub #18 and P1.8 guidance
- [x] 6.3 Note that small files may be slower with multiprocessing (startup overhead)
