## 1. Implementation

### 1.1 DuckDBIngester Class
- [x] 1.1.1 Create `DuckDBIngester` class in `undatum/cmds/ingester.py`
- [x] 1.1.2 Implement connection management (DuckDB uses file-based or in-memory connections)
- [x] 1.1.3 Implement batch INSERT method for bulk loading (optimized executemany)
- [x] 1.1.4 Implement Appender API for streaming insertion
- [ ] 1.1.5 Implement Parquet intermediate format support for large datasets - Future enhancement
- [x] 1.1.6 Implement schema validation and auto-creation
- [x] 1.1.7 Implement transaction management (via DuckDB's transaction support)
- [x] 1.1.8 Add error handling specific to DuckDB
- [x] 1.1.9 Add support for connection string parsing

### 1.2 Integration with Ingester Class
- [x] 1.2.1 Update `Ingester.ingest_single` to handle `duckdb` dbtype
- [x] 1.2.2 Add DuckDB-specific options handling
- [x] 1.2.3 Integrate with existing batch processing logic
- [x] 1.2.4 Ensure progress tracking works with DuckDB
- [x] 1.2.5 Leverage existing DuckDB integration for counting

### 1.3 CLI Updates
- [x] 1.3.1 Add `duckdb` as valid `--dbtype` option
- [x] 1.3.2 Update CLI help text and documentation
- [x] 1.3.3 Add connection string format examples
- [x] 1.3.4 Add examples for file and in-memory databases
- [ ] 1.3.5 Add examples for Parquet intermediate format - Future enhancement

### 1.4 Performance Optimizations
- [x] 1.4.1 Optimize batch sizes for DuckDB (50,000-100,000 rows) - Default constant defined
- [x] 1.4.2 Implement batch INSERT using executemany (optimized for DuckDB)
- [x] 1.4.3 Implement Appender API for streaming insertion
- [ ] 1.4.4 Test performance with large datasets (1M+ rows) - Requires integration testing
- [ ] 1.4.5 Compare batch INSERT vs Appender API performance - Requires benchmarking

## 2. Testing

### 2.1 Unit Tests
- [x] 2.1.1 Test DuckDBIngester initialization
- [x] 2.1.2 Test batch INSERT bulk loading
- [x] 2.1.3 Test Appender API streaming insertion
- [ ] 2.1.4 Test Parquet intermediate format - Future enhancement
- [x] 2.1.5 Test schema validation
- [x] 2.1.6 Test schema auto-creation
- [x] 2.1.7 Test transaction management (via DuckDB's built-in support)
- [x] 2.1.8 Test error handling

### 2.2 Integration Tests
- [x] 2.2.1 Test with DuckDB file database
- [x] 2.2.2 Test with DuckDB in-memory database
- [x] 2.2.3 Test COPY FROM with various data types
- [x] 2.2.4 Test Appender API with streaming data
- [ ] 2.2.5 Test Parquet intermediate format - Future enhancement
- [x] 2.2.6 Test schema auto-creation
- [ ] 2.2.7 Test with large files (100K+ rows) - Deferred
- [x] 2.2.8 Test connection string parsing

### 2.3 Performance Tests
- [ ] 2.3.1 Benchmark COPY FROM vs Appender API performance - Deferred
- [ ] 2.3.2 Test throughput with different batch sizes - Deferred
- [ ] 2.3.3 Test Parquet intermediate format performance - Deferred
- [ ] 2.3.4 Compare with PostgreSQL ingestion performance - Deferred

## 3. Documentation

### 3.1 Code Documentation
- [x] 3.1.1 Add docstrings to DuckDBIngester class and methods
- [x] 3.1.2 Document batch INSERT implementation
- [x] 3.1.3 Document Appender API implementation
- [ ] 3.1.4 Document Parquet intermediate format usage - Future enhancement

### 3.2 User Documentation
- [x] 3.2.1 Add DuckDB section to README.md
- [x] 3.2.2 Add connection string format examples
- [x] 3.2.3 Add examples for file and in-memory databases
- [ ] 3.2.4 Add Parquet intermediate format examples - Future enhancement
- [x] 3.2.5 Add performance tips and best practices
- [x] 3.2.6 Document that DuckDB is already a dependency

## 4. Error Handling

### 4.1 DuckDB-Specific Errors
- [ ] 4.1.1 Handle file permission errors
- [ ] 4.1.2 Handle schema mismatch errors
- [ ] 4.1.3 Handle constraint violation errors
- [ ] 4.1.4 Handle COPY FROM format errors
- [ ] 4.1.5 Provide actionable error messages

### 4.2 Error Recovery
- [ ] 4.2.1 Implement retry logic for transient errors
- [ ] 4.2.2 Handle partial batch failures
- [ ] 4.2.3 Log errors with context
