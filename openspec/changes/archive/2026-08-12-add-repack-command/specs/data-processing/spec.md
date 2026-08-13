## ADDED Requirements

### Requirement: Repack Command
The system SHALL provide a `repack` command that recompresses a selected file while preserving
its data format, using maximum compression by default.

#### Scenario: Repack container-compressed file at max compression
- **WHEN** a user runs `undatum repack data.csv.gz out.csv.gz` (or omits OUTPUT for in-place)
- **THEN** the system decompresses and recompresses using the same container codec (gzip) at
  maximum compression (or the codec’s `max` profile)
- **AND** the output remains CSV data in a `.gz` container

#### Scenario: Compression level option
- **WHEN** a user runs `undatum repack data.jsonl.zst out.jsonl.zst --level 3`
- **THEN** the system recompresses with zstd at level 3 instead of the default maximum

#### Scenario: Progress option
- **WHEN** a user runs `undatum repack large.csv.gz out.csv.gz --progress`
- **THEN** the system shows a progress indicator while recompressing
- **WHEN** a user runs with `--no-progress`
- **THEN** the system does not show a progress bar

#### Scenario: Built-in compression formats
- **WHEN** a user runs `undatum repack data.parquet out.parquet`
- **THEN** the system rewrites the Parquet file using Parquet’s built-in compression at maximum
  strength (default codec `zstd`, overridable via `--compression`)
- **AND** equivalent behavior applies to ORC and AVRO using each format’s native compression

#### Scenario: In-place atomic rewrite
- **WHEN** a user runs `undatum repack data.csv.gz` without an OUTPUT path
- **THEN** the system writes via a temporary file and atomically replaces the original on success

#### Scenario: Unsupported uncompressed input
- **WHEN** a user runs `undatum repack data.csv` with no container codec and no built-in
  compression format, and without an OUTPUT that implies a compression codec
- **THEN** the system fails with an actionable validation error explaining how to compress via
  `convert` or a compressed output path
