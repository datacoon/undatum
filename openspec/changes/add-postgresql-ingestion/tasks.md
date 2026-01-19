## 1. Implementation

### 1.1 PostgreSQL Driver Setup
- [x] 1.1.1 Add `psycopg2` or `psycopg3` to dependencies (requirements.txt, pyproject.toml) - Using psycopg2 with optional import
- [x] 1.1.2 Research and decide between psycopg2 and psycopg3 - Using psycopg2 (more widely available)
- [ ] 1.1.3 Add optional `sqlalchemy` dependency for connection abstraction - Deferred, using psycopg2 directly
- [x] 1.1.4 Update dependency documentation - Documented in code and README

### 1.2 PostgresIngester Class
- [x] 1.2.1 Create `PostgresIngester` class in `undatum/cmds/ingester.py`
- [x] 1.2.2 Implement connection management with connection pooling
- [x] 1.2.3 Implement `COPY FROM` method for bulk loading (fastest method)
- [x] 1.2.4 Implement `INSERT ... ON CONFLICT` for upsert operations
- [x] 1.2.5 Implement schema validation (check table exists, schema matches)
- [x] 1.2.6 Implement schema auto-creation from data schema
- [x] 1.2.7 Implement transaction management for atomic batches
- [x] 1.2.8 Add error handling specific to PostgreSQL
- [x] 1.2.9 Add support for connection string parsing

### 1.3 Integration with Ingester Class
- [x] 1.3.1 Update `Ingester.ingest_single` to handle `postgresql` dbtype
- [x] 1.3.2 Add PostgreSQL-specific options handling
- [x] 1.3.3 Integrate with existing batch processing logic
- [x] 1.3.4 Ensure progress tracking works with PostgreSQL

### 1.4 CLI Updates
- [x] 1.4.1 Add `postgresql` as valid `--dbtype` option
- [x] 1.4.2 Add `--mode` option with values: `append`, `replace`, `upsert`
- [x] 1.4.3 Add `--create-table` flag for auto-creating tables
- [x] 1.4.4 Add `--upsert-key` option for specifying conflict resolution key
- [x] 1.4.5 Update CLI help text and documentation
- [x] 1.4.6 Add connection string format examples

### 1.5 Performance Optimizations
- [x] 1.5.1 Implement `COPY FROM` using StringIO for CSV data
- [x] 1.5.2 Optimize batch sizes for PostgreSQL (10,000-50,000 rows) - Default batch size constant defined
- [ ] 1.5.3 Add option to disable indexes during bulk load - Future enhancement
- [x] 1.5.4 Implement connection pooling with appropriate pool size
- [ ] 1.5.5 Test performance with large datasets (1M+ rows) - Requires integration testing

## 2. Testing

### 2.1 Unit Tests
- [x] 2.1.1 Test PostgresIngester initialization
- [x] 2.1.2 Test COPY FROM bulk loading
- [x] 2.1.3 Test INSERT ... ON CONFLICT upsert
- [x] 2.1.4 Test schema validation
- [x] 2.1.5 Test schema auto-creation
- [x] 2.1.6 Test transaction management (via mocks)
- [x] 2.1.7 Test connection pooling (via mocks)
- [x] 2.1.8 Test error handling

### 2.2 Integration Tests
- [ ] 2.2.1 Test with real PostgreSQL instance (Docker container)
- [ ] 2.2.2 Test COPY FROM with various data types
- [ ] 2.2.3 Test upsert with conflict resolution
- [ ] 2.2.4 Test schema auto-creation with different schemas
- [ ] 2.2.5 Test with large files (100K+ rows)
- [ ] 2.2.6 Test connection string parsing
- [ ] 2.2.7 Test different PostgreSQL versions (12, 13, 14, 15, 16)

### 2.3 Performance Tests
- [ ] 2.3.1 Benchmark COPY FROM vs INSERT performance
- [ ] 2.3.2 Test throughput with different batch sizes
- [ ] 2.3.3 Test connection pooling impact on performance
- [ ] 2.3.4 Compare with MongoDB ingestion performance

## 3. Documentation

### 3.1 Code Documentation
- [x] 3.1.1 Add docstrings to PostgresIngester class and methods
- [x] 3.1.2 Document COPY FROM implementation
- [x] 3.1.3 Document upsert implementation
- [x] 3.1.4 Document connection pooling configuration

### 3.2 User Documentation
- [x] 3.2.1 Add PostgreSQL section to README.md
- [x] 3.2.2 Add connection string format examples
- [x] 3.2.3 Add examples for append, replace, and upsert modes
- [x] 3.2.4 Add schema auto-creation examples
- [x] 3.2.5 Add performance tips and best practices
- [x] 3.2.6 Document required dependencies

## 4. Error Handling

### 4.1 PostgreSQL-Specific Errors
- [ ] 4.1.1 Handle connection errors gracefully
- [ ] 4.1.2 Handle schema mismatch errors
- [ ] 4.1.3 Handle constraint violation errors
- [ ] 4.1.4 Handle COPY FROM format errors
- [ ] 4.1.5 Provide actionable error messages

### 4.2 Error Recovery
- [ ] 4.2.1 Implement retry logic for transient PostgreSQL errors
- [ ] 4.2.2 Handle partial batch failures
- [ ] 4.2.3 Log errors with context (table, column, record)
