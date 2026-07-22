## ADDED Requirements

### Requirement: Chunked Streaming I/O
Commands SHALL process data in chunks so peak memory stays roughly proportional to chunk size
(and parallel window size), not to full input file size.

#### Scenario: Process large file with chunked I/O
- **WHEN** user runs `undatum convert large.csv --output out.jsonl` on a multi-GB file using the
  Python engine path
- **THEN** the file is processed in record batches and memory usage remains roughly constant aside
  from configured batch/window buffers

#### Scenario: Incremental output writing
- **WHEN** processing large files with chunked I/O
- **THEN** output is written incrementally as chunks complete, not buffered entirely in memory

### Requirement: Multiprocessing for CPU-Bound Conversion
The system SHALL support opt-in multiprocessing for single-file conversion on the Python engine via
`--threads N`, using process-pool batch processing suitable for large CPU-bound transforms.

#### Scenario: Parallel single-file convert with worker count
- **WHEN** user runs `undatum convert data.csv out.jsonl --threads 4 --engine python`
- **THEN** conversion processes record chunks with up to 4 worker processes
- **AND** the output record content matches sequential conversion

#### Scenario: Sequential convert remains default
- **WHEN** user runs `undatum convert data.csv out.jsonl` without `--threads`
- **THEN** processing uses the sequential path (backward compatible)

#### Scenario: Record order preserved by default
- **WHEN** user runs single-file convert with `--threads N` where `N > 1`
- **THEN** output records appear in the same order as the input

#### Scenario: Bounded in-flight work
- **WHEN** parallel convert runs on a file larger than available RAM
- **THEN** the system does not materialize all input chunks before processing
- **AND** only a bounded window of chunks is in flight

#### Scenario: DuckDB engine is not double-parallelized
- **WHEN** user runs convert with DuckDB selected (`--engine duckdb` or auto) and `--threads N`
- **THEN** the system does not wrap DuckDB COPY in an additional undatum process pool

### Requirement: Multiprocessing for Multi-File Bulk Convert
The system SHALL continue to support parallel conversion of many files via recursive/bulk convert
and `--threads`.

#### Scenario: Bulk convert uses workers
- **WHEN** user runs `undatum convert ./raw ./out --recursive --to-ext parquet --threads 4`
- **THEN** multiple input files may be converted concurrently with up to 4 workers

### Requirement: Multiprocessing for Safe Follow-on Processing Commands
CPU-bound, order-insensitive or mergeable processing commands SHALL support `--threads N` for
process-pool or equivalent parallel chunk work on the Python engine where results can be merged
correctly.

#### Scenario: Parallel validate
- **WHEN** user runs `undatum validate data.csv --threads 4` on a path that uses Python-engine
  validation
- **THEN** independent record/chunk checks may run across workers
- **AND** aggregated validation results remain correct

#### Scenario: Parallel stats merge
- **WHEN** user runs Python-engine `stats` or `frequency` with `--threads N`
- **THEN** partial aggregates may be computed per chunk and merged into the final result
- **AND** numeric aggregates match the sequential engine within documented tolerances

### Requirement: Progress Indication Under Parallel Work
Commands that support progress indication SHALL remain usable when multiprocessing is enabled,
without corrupting redirected stdout/stderr used for data output.

#### Scenario: Progress with parallel convert
- **WHEN** user runs `undatum convert large.csv out.jsonl --threads 4` with progress enabled on a TTY
- **THEN** a progress indicator shows forward progress (chunks/records/files as applicable)

#### Scenario: Non-TTY or disabled progress
- **WHEN** progress is disabled or stdout is not a TTY
- **THEN** no progress bar is written to the data output stream
