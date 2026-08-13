## 1. Shared Engine Infrastructure
- [x] 1.1 Create `undatum/common/engine_selector.py` module
  - Implement engine detection logic (auto/duckdb/python)
  - Add format support detection (CSV, JSON, Parquet)
  - Add operation compatibility checking (SQL-expressible operations)
- [x] 1.2 Create `undatum/common/duckdb_config.py` module
  - Implement DuckDB connection configuration
  - Add thread/memory/temp-dir option parsing
  - Create reusable DuckDB connection factory
- [x] 1.3 Add shared DuckDB query builder utilities
  - Common SQL generation patterns
  - Format-specific table creation (CSV, JSON, Parquet)
  - Error handling and fallback logic

## 2. Command Implementations
- [x] 2.1 Implement DuckDB engine for `sort` command
  - Add `--engine` option to sorter CLI
  - Implement SQL-based sorting with ORDER BY
  - Add fallback to Python engine
- [x] 2.2 Implement DuckDB engine for `frequency` command
  - Add `--engine` option to frequency CLI
  - Implement GROUP BY with COUNT(*) for frequency analysis
  - Add fallback to Python engine
- [x] 2.3 Implement DuckDB engine for `uniq` command
  - Add `--engine` option to uniq CLI
  - Implement DISTINCT or GROUP BY for unique values
  - Add fallback to Python engine
- [x] 2.4 Implement DuckDB engine for `sample` command
  - Add `--engine` option to sampler CLI
  - Implement TABLESAMPLE or random sampling in SQL
  - Add fallback to Python engine
- [x] 2.5 Implement DuckDB engine for `search` command
  - Add `--engine` option to searcher CLI
  - Implement WHERE clause filtering in SQL
  - Add fallback to Python engine
- [x] 2.6 Implement DuckDB engine for `dedup` command
  - Add `--engine` option to deduplicator CLI
  - Implement DISTINCT ON or window functions for deduplication
  - Add fallback to Python engine
- [x] 2.7 Implement DuckDB engine for `slice` command
  - Add `--engine` option to slicer CLI
  - Implement LIMIT/OFFSET in SQL
  - Add fallback to Python engine
- [x] 2.8 Implement DuckDB engine for `join` command
  - Add `--engine` option to joiner CLI
  - Implement JOIN operations in SQL
  - Add fallback to Python engine

## 3. CLI Integration
- [x] 3.1 Add `--engine` option to all affected commands in `undatum/core.py`
  - Add `--engine auto|duckdb|python` parameter
  - Pass engine option to command processors
- [x] 3.2 Add DuckDB tuning options to CLI
  - Add `--duckdb-threads N` option
  - Add `--duckdb-memory <bytes|MB|GB>` option
  - Add `--duckdb-temp-dir /path/to/tmp` option
- [x] 3.3 Update help text and documentation
  - Document engine selection behavior
  - Add examples for DuckDB tuning options
  - Update command-specific help text

## 4. Testing
- [x] 4.1 Unit tests for engine selector
  - Test auto-detection logic
  - Test format support detection
  - Test operation compatibility checking
- [x] 4.2 Unit tests for DuckDB configuration
  - Test thread/memory/temp-dir parsing
  - Test connection factory
- [x] 4.3 Integration tests for each command
  - Test DuckDB engine path for each command
  - Test fallback to Python engine
  - Test with various formats (CSV, JSONL, JSON, Parquet)
- [x] 4.4 Performance benchmarks
  - Compare DuckDB vs Python engine performance
  - Test with large datasets (1M+ rows)
  - Document performance improvements

## 5. Documentation
- [x] 5.1 Update README with engine selection examples
- [x] 5.2 Document DuckDB tuning options
- [x] 5.3 Add performance comparison examples
- [x] 5.4 Update command-specific documentation
