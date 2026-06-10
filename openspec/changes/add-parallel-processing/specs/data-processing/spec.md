## ADDED Requirements

### Requirement: Chunked Streaming I/O
Commands SHALL process data in chunks to maintain constant memory usage regardless of input file
size.

#### Scenario: Process large file with chunked I/O
- **WHEN** user runs `undatum convert large.csv --output out.jsonl` on a 10GB file
- **THEN** file is processed in chunks (e.g., 1000 records at a time) and memory usage remains
  roughly constant

#### Scenario: Incremental output writing
- **WHEN** processing large files with chunked I/O
- **THEN** output is written incrementally as chunks are processed, not buffered entirely in memory

### Requirement: Parallel Processing for CPU-Bound Operations
Commands SHALL support parallel processing via `--threads N` option for CPU-bound operations.

#### Scenario: Parallel processing with specified thread count
- **WHEN** user runs `undatum stats data.csv --threads 4`
- **THEN** statistics computation uses 4 threads for parallel processing

#### Scenario: Default to CPU count
- **WHEN** user runs `undatum convert data.csv --threads` without specifying count
- **THEN** system uses number of CPU cores as default thread count

#### Scenario: Single-threaded by default
- **WHEN** user runs `undatum stats data.csv` without `--threads` option
- **THEN** processing uses single thread (backward compatible)

### Requirement: Progress Indication
Commands SHALL support progress indication via `--progress` flag showing percentage, estimated
time, and throughput.

#### Scenario: Progress bar for long-running operation
- **WHEN** user runs `undatum convert large.csv --output out.jsonl --progress`
- **THEN** progress bar is displayed showing percentage complete, ETA, and throughput (records/sec)

#### Scenario: Textual progress indicator
- **WHEN** progress bar is not available (non-TTY output)
- **THEN** textual progress updates are shown periodically

#### Scenario: No progress indication by default
- **WHEN** user runs command without `--progress` flag
- **THEN** no progress indication is shown (backward compatible)
