## ADDED Requirements

### Requirement: Unified Iterable Data Processing
The system SHALL use the external `iterabledata` library as the single source of truth for all iterable data operations across all commands.

#### Scenario: All commands use external library
- **WHEN** any command processes iterable data files (CSV, JSONL, BSON, XML, XLS, XLSX, Parquet, AVRO, ORC)
- **THEN** the command SHALL use `open_iterable()` from `iterable.helpers.detect` module
- **AND** the command SHALL pass format-specific options via `iterableargs` parameter

#### Scenario: Format support consistency
- **WHEN** a command processes data files
- **THEN** all supported formats (CSV, JSON, JSONL, BSON, XML, XLS, XLSX, Parquet, AVRO, ORC, Pickle) SHALL be available consistently across all commands
- **AND** format detection SHALL be handled by the external library

### Requirement: Context Manager Resource Management
All iterable data operations SHALL use context managers (`with` statements) for proper resource cleanup.

#### Scenario: Automatic resource cleanup
- **WHEN** a command opens an iterable data file using `open_iterable()`
- **THEN** the command SHALL use a `with` statement or explicitly call `close()` on the iterable object
- **AND** file handles and compression streams SHALL be properly closed even if errors occur

#### Scenario: Multiple iterables in same command
- **WHEN** a command opens multiple iterable files (e.g., input and output)
- **THEN** each iterable SHALL be managed within its own context manager or explicitly closed
- **AND** all resources SHALL be cleaned up before command completion

### Requirement: Batch Write Operations
Commands that write iterable data SHALL use `write_bulk()` method for batch operations to improve performance.

#### Scenario: Batch writing in converter
- **WHEN** the `convert` command writes output data
- **THEN** it SHALL collect records in batches
- **AND** it SHALL use `write_bulk()` method instead of individual `write()` calls for batches
- **AND** it SHALL flush remaining records using `write_bulk()` at the end

#### Scenario: Batch writing in transformer
- **WHEN** the `apply` (transformer) command writes transformed output
- **THEN** it SHALL use `write_bulk()` for batch writes when processing multiple records
- **AND** individual writes MAY be used only for single-record outputs or streaming scenarios

### Requirement: Iterator Reset Capability
Commands that require multiple passes over the same data SHALL use the `reset()` method when available.

#### Scenario: Schema extraction then conversion
- **WHEN** a command needs to scan data twice (e.g., extract schema then process)
- **THEN** it SHALL use `reset()` method on the iterable object to restart iteration
- **AND** it SHALL NOT reopen the file if `reset()` is available
- **AND** it SHALL fall back to reopening the file if `reset()` is not available

### Requirement: DuckDB Engine Integration
Performance-critical operations SHALL support optional DuckDB engine for improved query performance.

#### Scenario: DuckDB engine for statistics
- **WHEN** the `stats` command processes large files in supported formats (CSV, JSONL, Parquet)
- **THEN** it SHALL support using DuckDB engine via `engine='duckdb'` parameter
- **AND** it SHALL automatically select DuckDB engine when appropriate (format and compression compatible)
- **AND** it SHALL fall back to iterable engine if DuckDB is not available or incompatible

#### Scenario: DuckDB engine for select operations
- **WHEN** the `select` command processes large files with filtering
- **THEN** it SHALL support DuckDB engine for improved performance on supported formats
- **AND** it SHALL use iterable engine for formats not supported by DuckDB

## MODIFIED Requirements

### Requirement: Data Format Support
The system SHALL support reading and writing multiple data formats with consistent behavior across all commands.

#### Scenario: Format detection
- **WHEN** a command processes a data file
- **THEN** format detection SHALL be performed by the external `iterabledata` library
- **AND** format-specific options (encoding, delimiter, tagname) SHALL be passed via `iterableargs`
- **AND** manual format override SHALL be supported via options

#### Scenario: Compression support
- **WHEN** a command processes compressed files (GZ, BZ2, XZ, ZIP, 7Z, LZ4, ZSTD)
- **THEN** compression SHALL be handled automatically by the external library
- **AND** compression detection SHALL be based on file extension and content
- **AND** nested compression (e.g., ZIP containing compressed files) SHALL be supported

## REMOVED Requirements

### Requirement: Local IterableData Class
The local `IterableData` class in `undatum/common/iterable.py` SHALL be removed after migration is complete.

**Reason**: Functionality is fully replaced by the external `iterabledata` library which provides superior format support, better compression handling, and advanced features like `reset()` and `write_bulk()`.

**Migration**: All commands using local `IterableData` SHALL be migrated to use `open_iterable()` from external library. The local class MAY be marked as deprecated first with a migration period.

### Requirement: Local DataWriter Class
The local `DataWriter` class in `undatum/common/iterable.py` SHALL be removed after migration is complete.

**Reason**: The external `iterabledata` library provides writing capabilities through `open_iterable(mode='w')` with better format support and `write_bulk()` for performance.

**Migration**: All commands using local `DataWriter` SHALL be migrated to use `open_iterable(mode='w')` from external library. Writing operations SHALL use `write()` or `write_bulk()` methods as appropriate.
