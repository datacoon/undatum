## ADDED Requirements

### Requirement: Streaming Parquet Conversion
The system SHALL convert to Parquet using a streaming or batched write path that does not require
loading the entire dataset into memory.

#### Scenario: Large JSONL to Parquet uses bounded memory path
- **WHEN** a user converts a multi-gigabyte JSONL (optionally compressed) file to Parquet
- **THEN** the conversion writes Parquet incrementally (row groups / spill-to-disk engine) rather
  than materializing all records in RAM before write

#### Scenario: Small file Parquet convert still works
- **WHEN** a user converts a small CSV file to Parquet without special flags
- **THEN** the conversion succeeds with correct schema and row count

### Requirement: Low-Memory Mode
The system SHALL provide an explicit `--low-memory` mode for conversion that prefers
streaming/spill-to-disk execution over in-memory materialization.

#### Scenario: User requests low-memory convert
- **WHEN** a user runs convert with `--low-memory` on a supported input
- **THEN** the system uses the low-memory execution path and documents or logs that choice

#### Scenario: Large-file behavior is documented
- **WHEN** a user consults project documentation for large-file conversion
- **THEN** docs describe recommended flags (including `--low-memory` / engine options) and
  expected memory behavior
