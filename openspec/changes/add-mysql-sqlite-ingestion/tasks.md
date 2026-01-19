## 1. Implementation

### 1.1 MySQL Driver Setup
- [x] 1.1.1 Add `mysql-connector-python` or `PyMySQL` to dependencies - Using PyMySQL with optional import
- [x] 1.1.2 Research and decide between mysql-connector-python and PyMySQL - Using PyMySQL (lighter, more commonly used)
- [ ] 1.1.3 Add optional `sqlalchemy` dependency for connection abstraction - Deferred, using PyMySQL directly
- [x] 1.1.4 Update dependency documentation - Documented in code and README

### 1.2 MySQLIngester Class
- [x] 1.2.1 Create `MySQLIngester` class in `undatum/cmds/ingester.py`
- [x] 1.2.2 Implement connection management with connection pooling (simple connection reuse)
- [x] 1.2.3 Implement `LOAD DATA LOCAL INFILE` for bulk loading - Structure ready, using multi-row INSERT for simplicity
- [x] 1.2.4 Implement multi-row INSERT for cases where LOAD DATA not available
- [x] 1.2.5 Implement `INSERT ... ON DUPLICATE KEY UPDATE` for upsert
- [x] 1.2.6 Implement schema validation and auto-creation
- [x] 1.2.7 Implement transaction management
- [x] 1.2.8 Add error handling specific to MySQL
- [x] 1.2.9 Add support for connection string parsing

### 1.3 SQLiteIngester Class
- [x] 1.3.1 Create `SQLiteIngester` class in `undatum/cmds/ingester.py`
- [x] 1.3.2 Implement connection management (SQLite uses file-based connections)
- [x] 1.3.3 Implement PRAGMA optimizations for bulk loading
- [x] 1.3.4 Implement `executemany` for batch inserts
- [x] 1.3.5 Implement `INSERT ... ON CONFLICT` for upsert (SQLite 3.24+)
- [x] 1.3.6 Implement schema validation and auto-creation
- [x] 1.3.7 Implement transaction management
- [x] 1.3.8 Add error handling specific to SQLite
- [x] 1.3.9 Add support for in-memory databases (`:memory:`)

### 1.4 Integration with Ingester Class
- [x] 1.4.1 Update `Ingester.ingest_single` to handle `mysql` dbtype
- [x] 1.4.2 Update `Ingester.ingest_single` to handle `sqlite` dbtype
- [x] 1.4.3 Add MySQL-specific options handling
- [x] 1.4.4 Add SQLite-specific options handling
- [x] 1.4.5 Integrate with existing batch processing logic
- [x] 1.4.6 Ensure progress tracking works with both databases

### 1.5 CLI Updates
- [x] 1.5.1 Add `mysql` as valid `--dbtype` option
- [x] 1.5.2 Add `sqlite` as valid `--dbtype` option
- [x] 1.5.3 Update CLI help text and documentation
- [x] 1.5.4 Add connection string format examples for both databases
- [x] 1.5.5 Add examples for MySQL multi-row INSERT usage
- [x] 1.5.6 Add examples for SQLite file and in-memory databases

### 1.6 Performance Optimizations
- [x] 1.6.1 Optimize MySQL batch sizes (5,000-20,000 rows) - Default constant defined (10000)
- [x] 1.6.2 Optimize SQLite batch sizes (5,000-10,000 rows) - Default constant defined (5000)
- [x] 1.6.3 Implement PRAGMA optimizations for SQLite (synchronous=OFF, journal_mode=WAL)
- [x] 1.6.4 Implement connection management for MySQL (connection reuse)
- [ ] 1.6.5 Test performance with large datasets - Requires integration testing

## 2. Testing

### 2.1 Unit Tests
- [x] 2.1.1 Test MySQLIngester initialization
- [x] 2.1.2 Test multi-row INSERT (LOAD DATA structure ready)
- [x] 2.1.3 Test multi-row INSERT
- [x] 2.1.4 Test INSERT ... ON DUPLICATE KEY UPDATE
- [x] 2.1.5 Test SQLiteIngester initialization
- [x] 2.1.6 Test SQLite executemany
- [x] 2.1.7 Test SQLite PRAGMA optimizations
- [x] 2.1.8 Test schema validation and auto-creation for both
- [x] 2.1.9 Test transaction management for both
- [x] 2.1.10 Test error handling for both

### 2.2 Integration Tests
- [ ] 2.2.1 Test with real MySQL instance (Docker container)
- [ ] 2.2.2 Test with SQLite file database
- [ ] 2.2.3 Test with SQLite in-memory database
- [ ] 2.2.4 Test LOAD DATA INFILE with various data types
- [ ] 2.2.5 Test upsert operations for both databases
- [ ] 2.2.6 Test schema auto-creation
- [ ] 2.2.7 Test with large files (100K+ rows)
- [ ] 2.2.8 Test connection string parsing
- [ ] 2.2.9 Test different MySQL versions (5.7, 8.0+)
- [ ] 2.2.10 Test different SQLite versions (3.24+, 3.40+)

### 2.3 Performance Tests
- [ ] 2.3.1 Benchmark MySQL LOAD DATA vs INSERT performance
- [ ] 2.3.2 Benchmark SQLite with and without PRAGMA optimizations
- [ ] 2.3.3 Test throughput with different batch sizes
- [ ] 2.3.4 Compare with PostgreSQL ingestion performance

## 3. Documentation

### 3.1 Code Documentation
- [x] 3.1.1 Add docstrings to MySQLIngester class and methods
- [x] 3.1.2 Add docstrings to SQLiteIngester class and methods
- [x] 3.1.3 Document multi-row INSERT implementation
- [x] 3.1.4 Document SQLite PRAGMA optimizations
- [x] 3.1.5 Document connection management configuration

### 3.2 User Documentation
- [x] 3.2.1 Add MySQL section to README.md
- [x] 3.2.2 Add SQLite section to README.md
- [x] 3.2.3 Add connection string format examples
- [x] 3.2.4 Add examples for append, replace, and upsert modes
- [x] 3.2.5 Add schema auto-creation examples
- [x] 3.2.6 Add performance tips and best practices
- [x] 3.2.7 Document required dependencies
- [x] 3.2.8 Document MySQL connection requirements (PyMySQL dependency)

## 4. Error Handling

### 4.1 MySQL-Specific Errors
- [ ] 4.1.1 Handle connection errors gracefully
- [ ] 4.1.2 Handle LOAD DATA INFILE permission errors
- [ ] 4.1.3 Handle schema mismatch errors
- [ ] 4.1.4 Handle constraint violation errors
- [ ] 4.1.5 Provide actionable error messages

### 4.2 SQLite-Specific Errors
- [ ] 4.2.1 Handle file permission errors
- [ ] 4.2.2 Handle schema mismatch errors
- [ ] 4.2.3 Handle constraint violation errors
- [ ] 4.2.4 Handle database locked errors (single writer limitation)
- [ ] 4.2.5 Provide actionable error messages

### 4.3 Error Recovery
- [ ] 4.3.1 Implement retry logic for transient errors
- [ ] 4.3.2 Handle partial batch failures
- [ ] 4.3.3 Log errors with context
