## 1. Engine Detection and Structure

### 1.1 Engine Detection Function
- [x] 1.1.1 Create `_detect_engine()` helper function in `StatProcessor` class (reuse pattern from `counter.py`)
- [x] 1.1.2 Import `DUCKABLE_FILE_TYPES` and `DUCKABLE_CODECS` from `undatum/constants.py`
- [x] 1.1.3 Implement file type detection using `detect_file_type()` from `iterable.helpers.detect`
- [x] 1.1.4 Implement compression detection from file type detection result
- [x] 1.1.5 Add logic to select DuckDB engine when: `filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS`
- [x] 1.1.6 Add logic to honor explicit `engine` parameter ('auto', 'duckdb', 'iterable')

### 1.2 CLI Integration
- [x] 1.2.1 Add `--engine` option to `stats` command in `undatum/core.py`
- [x] 1.2.2 Set default value to 'auto' for automatic engine detection
- [x] 1.2.3 Add help text: "Engine to use for statistics computation: 'auto' (detect), 'duckdb' (DuckDB engine), or 'iterable' (row-by-row)"
- [x] 1.2.4 Pass `engine` option to `StatProcessor.stats()` method
- [x] 1.2.5 Update `stats()` method signature to accept `engine` parameter

## 2. DuckDB Statistics Implementation

### 2.1 Basic Statistics Using duckdb_decompose
- [x] 2.1.1 Import `duckdb_decompose` from `undatum/common/schema_utils`
- [x] 2.1.2 Create `_compute_duckdb_basic_stats()` method in `StatProcessor`
- [x] 2.1.3 Call `duckdb_decompose()` with `use_summarize=True` for basic statistics
- [x] 2.1.4 Extract field paths, base types, unique counts, total counts, and uniqueness percentages
- [x] 2.1.5 Handle nested structures correctly using recursive decomposition
- [x] 2.1.6 Map results to existing `fielddata` and `fieldtypes` dictionary structures

### 2.2 Length Statistics Queries
- [x] 2.2.1 Create `_compute_duckdb_length_stats()` method for min/max/avg length
- [x] 2.2.2 For each field path, construct SQL query with `MIN(LENGTH(CAST(...)))`, `MAX(LENGTH(CAST(...)))`, `AVG(LENGTH(CAST(...)))`
- [x] 2.2.3 Handle NULL values correctly in length queries
- [x] 2.2.4 Use appropriate DuckDB read function (`read_csv()`, `read_json()`, etc.) based on file type
- [x] 2.2.5 Add `ignore_errors=true` parameter when appropriate
- [x] 2.2.6 Execute queries and extract minlen, maxlen, avglen for each field
- [x] 2.2.7 Merge length statistics into `fielddata` dictionaries

### 2.3 Type Detection Sampling
- [x] 2.3.1 Create `_detect_types_from_sample()` method for hybrid type detection
- [x] 2.3.2 Query sample of records: Uses iterable engine for sampling (handles nested structures correctly)
- [x] 2.3.3 For each sampled record, use existing `guess_datatype()` function on field values
- [x] 2.3.4 Build type distribution dictionary for each field
- [x] 2.3.5 Determine primary field type based on distribution (matching current logic)
- [x] 2.3.6 Merge type distributions into `fieldtypes` dictionaries

### 2.4 Dictionary Construction with GROUP BY
- [x] 2.4.1 Create `_compute_duckdb_dictionaries()` method for value frequencies
- [x] 2.4.2 For fields with uniqueness percentage below `dictshare` threshold:
  - [x] 2.4.2.1 Construct SQL query: `SELECT field_path, COUNT(*) as freq FROM ... GROUP BY field_path ORDER BY freq DESC`
  - [x] 2.4.2.2 Execute query and fetch all frequency results
  - [x] 2.4.2.3 Build dictionary structure: `{'items': {value: count}, 'count': n_uniq, 'type': field_type}`
  - [x] 2.4.2.4 Handle field paths with special characters (quote properly)
- [x] 2.4.3 Integrate dictionary construction into main statistics profile

### 2.5 Main DuckDB Statistics Method
- [x] 2.5.1 Create `_stats_duckdb()` method as main entry point for DuckDB engine
- [x] 2.5.2 Call basic statistics computation (`duckdb_decompose`)
- [x] 2.5.3 Call length statistics computation
- [x] 2.5.4 Call type detection sampling
- [x] 2.5.5 Call dictionary construction
- [x] 2.5.6 Combine all results into unified profile structure matching iterable engine output
- [x] 2.5.7 Handle edge cases: empty files, single-column files, very wide files

## 3. Integration and Error Handling

### 3.1 Engine Selection in stats() Method
- [x] 3.1.1 Call `_detect_engine()` at start of `stats()` method
- [x] 3.1.2 If DuckDB engine selected, wrap `_stats_duckdb()` call in try-except block
- [x] 3.1.3 On DuckDB failure, log warning and fallback to iterable engine
- [x] 3.1.4 Ensure iterable engine path remains unchanged (current implementation)

### 3.2 Error Handling
- [x] 3.2.1 Catch `duckdb.Error` exceptions specifically
- [x] 3.2.2 Catch generic `Exception` for other DuckDB-related failures
- [x] 3.2.3 Log warning message with error details when fallback occurs
- [x] 3.2.4 Ensure fallback is transparent to user (same output format)

### 3.3 Progress Indication Updates
- [x] 3.3.1 Update progress indication for DuckDB engine two-phase approach:
  - [x] 3.3.1.1 Phase 1: Show "Counting rows..." progress for COUNT query
  - [x] 3.3.1.2 Phase 2: Show "Computing statistics..." for main queries
- [x] 3.3.2 Use tqdm context manager for progress indication
- [x] 3.3.3 Ensure progress indication works correctly with DuckDB's query-based processing

## 4. Testing

### 4.1 Unit Tests
- [x] 4.1.1 Test `_detect_engine()` with various file types (CSV, JSONL, JSON, Parquet, XML, BSON)
- [x] 4.1.2 Test `_detect_engine()` with compressed files (gzip, zstd)
- [x] 4.1.3 Test `_detect_engine()` with explicit engine parameter
- [x] 4.1.4 Test `_compute_duckdb_basic_stats()` with flat CSV file
- [x] 4.1.5 Test `_compute_duckdb_basic_stats()` with nested JSON file
- [x] 4.1.6 Test `_compute_duckdb_length_stats()` with various field types
- [x] 4.1.7 Test `_detect_types_from_sample()` matches iterable engine type detection
- [x] 4.1.8 Test `_compute_duckdb_dictionaries()` builds correct frequency dictionaries

### 4.2 Integration Tests
- [x] 4.2.1 Test full `_stats_duckdb()` workflow with small CSV file (< 1000 rows)
- [ ] 4.2.2 Test full `_stats_duckdb()` workflow with large CSV file (> 100K rows) - Deferred (benchmark scale)
- [x] 4.2.3 Test full `_stats_duckdb()` workflow with JSONL file
- [x] 4.2.4 Test full `_stats_duckdb()` workflow with nested JSON file
- [x] 4.2.5 Test engine selection with `--engine auto` on supported format
- [x] 4.2.6 Test engine selection with `--engine auto` on unsupported format
- [x] 4.2.7 Test explicit `--engine duckdb` on supported format
- [x] 4.2.8 Test explicit `--engine iterable` on supported format
- [x] 4.2.9 Test fallback behavior when DuckDB query fails
- [x] 4.2.10 Compare statistics output between DuckDB and iterable engines (should match)

### 4.3 Performance Tests
- [ ] 4.3.1 Benchmark DuckDB engine vs iterable engine on 10K row CSV file - Deferred
- [ ] 4.3.2 Benchmark DuckDB engine vs iterable engine on 100K row CSV file - Deferred
- [ ] 4.3.3 Benchmark DuckDB engine vs iterable engine on 1M row CSV file - Deferred
- [ ] 4.3.4 Benchmark DuckDB engine vs iterable engine on JSONL files (various sizes) - Deferred
- [ ] 4.3.5 Verify performance improvements (expect 10-100x speedup for large files) - Deferred

### 4.4 Edge Cases
- [x] 4.4.1 Test with empty file
- [x] 4.4.2 Test with single-row file
- [x] 4.4.3 Test with single-column file
- [x] 4.4.4 Test with file containing all NULL values in a column
- [x] 4.4.5 Test with file containing very long string values
- [x] 4.4.6 Test with deeply nested JSON structures (4+ levels)
- [x] 4.4.7 Test with malformed file that causes DuckDB error (verify fallback)

## 5. Documentation

### 5.1 Code Documentation
- [x] 5.1.1 Add docstrings to all new methods (`_detect_engine`, `_stats_duckdb`, etc.)
- [x] 5.1.2 Document engine selection logic and supported formats
- [x] 5.1.3 Document fallback behavior and error handling

### 5.2 User Documentation
- [x] 5.2.1 Update CLI help text for `--engine` option
- [x] 5.2.2 Update README.md with DuckDB engine information
- [x] 5.2.3 Add examples showing `--engine` option usage
- [x] 5.2.4 Document performance benefits of DuckDB engine
- [x] 5.2.5 Document which formats support DuckDB engine

## 6. Validation

### 6.1 OpenSpec Validation
- [x] 6.1.1 Run `openspec validate optimize-stats-command-duckdb --strict`
- [x] 6.1.2 Fix any validation errors or warnings
- [x] 6.1.3 Ensure all scenarios are properly formatted

### 6.2 Code Quality
- [x] 6.2.1 Run linter (flake8/pylint) on modified files
- [x] 6.2.2 Fix any linting errors
- [x] 6.2.3 Ensure code follows existing patterns (match `counter.py`, `analyzer.py`)

### 6.3 Regression Testing
- [x] 6.3.1 Run existing stats command tests to ensure no regressions
- [x] 6.3.2 Verify iterable engine path still works correctly
- [x] 6.3.3 Verify backward compatibility (default behavior unchanged)
