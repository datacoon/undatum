# Database Ingestion Research Report for undatum

**Date:** 2025-01-27  
**Purpose:** Research and evaluation of database ingestion capabilities and improvements for undatum's `ingest` command  
**Status:** Research Phase - No Code Changes

---

## Executive Summary

This report evaluates the current state of undatum's `ingest` command and provides comprehensive research on:
1. Current implementation analysis
2. Database systems that should be supported
3. Best practices for database ingestion
4. Improvement opportunities and recommendations
5. Implementation strategies

**Key Finding:** The `ingest` command currently supports only MongoDB and Elasticsearch. There is significant opportunity to expand support to relational databases (PostgreSQL, MySQL, SQLite, DuckDB) and improve existing functionality with better error handling, connection pooling, and performance optimizations.

---

## 1. Current State Analysis

### 1.1 Current Implementation

**Location:** `undatum/cmds/ingester.py`

**Supported Databases:**
- MongoDB (`MongoIngester`)
- Elasticsearch (`ElasticIngester`)

**Key Features:**
- Batch processing (default batch size: 50,000 in code, 1,000 in CLI)
- Progress tracking with `tqdm`
- Support for skipping records
- Optional collection/table dropping
- Record counting for progress (via DuckDB for supported file types)

**Supported Input Formats:**
- CSV, JSONL, JSON, Parquet (via iterabledata)
- Compressed formats (GZ, ZST)

**Architecture:**
- Base class `BasicIngester` for extensibility
- Main `Ingester` class handles file reading and batching
- Database-specific ingester classes handle actual insertion

### 1.2 Current Limitations & Issues

**Code Issues:**
1. **Typo in line 85:** `'dro[]'` should be `'drop'` - causes drop option to not work
2. **Batch size mismatch:** Default is 50,000 in code but CLI shows 1,000
3. **Incomplete error handling:** No retry logic or detailed error reporting
4. **No connection pooling:** Each ingester creates new connections
5. **Limited transaction control:** No explicit transaction management
6. **No schema validation:** No pre-flight checks for schema compatibility
7. **Timeout option not used:** CLI accepts `--timeout` but it's not passed to MongoDB client

**Functional Limitations:**
1. **No relational database support:** PostgreSQL, MySQL, SQLite not supported
2. **No DuckDB export:** Despite using DuckDB for counting, can't ingest to DuckDB
3. **No bulk insert optimization:** Uses `insert_many` for MongoDB but no COPY for SQL DBs
4. **No upsert support:** Only insert operations, no update-on-conflict
5. **Limited authentication options:** Basic URI-based auth only
6. **No connection string parsing:** Limited URI validation and parsing
7. **No streaming for large batches:** All records in batch loaded into memory

**Error Handling Gaps:**
- No retry logic for transient failures
- No partial batch failure handling
- No detailed error logging per record
- No resume capability for interrupted ingestion

---

## 2. Similar Tools and Their Approaches

### 2.1 CLI Data Ingestion Tools

#### 2.1.1 dsq (DataStation Query)
**Approach:** Query structured files via SQL, outputs SQL or other formats

**Key Features:**
- Supports CSV, TSV, JSON, JSONL, Parquet, Avro, Excel, ODS, YAML
- SQL interface for filtering/transforming before import
- Useful for preprocessing data before database ingestion

**Insights for undatum:**
- SQL-based querying could be useful for pre-filtering data
- Multi-format support aligns with undatum's strengths
- Could integrate SQL filtering before ingestion

#### 2.1.2 csvkit
**Approach:** Suite of CSV tools for cleaning, converting, analyzing

**Key Features:**
- CSV-focused toolset
- Can generate SQL statements
- Good for data cleaning before ingestion

**Insights for undatum:**
- undatum already has better format support (JSONL, Parquet, etc.)
- SQL generation could be useful for schema creation
- Data cleaning features could enhance pre-ingestion workflow

#### 2.1.3 Ingestr (by Bruin Data)
**Approach:** CLI tool for copying data between databases via URIs

**Key Features:**
- Supports Postgres, MySQL, SQL Server, BigQuery, Snowflake
- Supports incremental loading modes (append, replace, etc.)
- Simple URI-based connection strings
- Incremental loading with change detection

**Insights for undatum:**
- URI-based approach similar to undatum's current design
- Incremental loading modes (append, replace, upsert) would be valuable
- Multi-database support demonstrates feasibility
- Simple CLI interface aligns with undatum's philosophy

#### 2.1.4 Airbyte
**Approach:** Full ETL/ELT framework with hundreds of connectors

**Key Features:**
- CLI and Docker deployment
- Automated pipeline orchestration
- Extensive connector library
- Configuration as code

**Insights for undatum:**
- Connector pattern could inspire database-specific ingester classes
- Configuration management could inform undatum's config approach
- Too complex for undatum's scope, but connector abstraction is valuable

#### 2.1.5 Meltano
**Approach:** Open-source, CLI-driven ETL built on Singer taps/targets

**Key Features:**
- Pipeline as code
- Extensible tap/target system
- CLI-first approach

**Insights for undatum:**
- Tap/target pattern could inspire plugin architecture
- CLI-first aligns with undatum's design
- Extensibility model could inform future plugin system

### 2.2 Native Database Import Tools

#### 2.2.1 pgloader
**Approach:** Fast PostgreSQL loading from multiple sources

**Key Features:**
- PostgreSQL-specific optimization
- Supports CSV, fixed-width, dBase, SQLite, MySQL, MS SQL Server
- Very fast bulk loading
- Complex configuration

**Insights for undatum:**
- PostgreSQL-specific optimizations (COPY FROM) are critical
- Multi-source support demonstrates value
- Performance-focused approach is important

#### 2.2.2 mysqlimport / mariadb-import
**Approach:** Native MySQL/MariaDB bulk import utility

**Key Features:**
- Wraps `LOAD DATA INFILE`
- Fast file-based loading
- Command-line interface

**Insights for undatum:**
- Native bulk load methods are fastest
- File-based approach could be optimized
- Simple CLI interface is effective

### 2.3 Key Insights from Similar Tools

**Common Patterns:**
1. **URI-based connection strings** - Simple, standardized approach (used by Ingestr, Airbyte)
2. **Incremental loading modes** - Append, replace, upsert options (Ingestr)
3. **SQL preprocessing** - Filter/transform before ingestion (dsq)
4. **Native bulk methods** - Use database-specific optimizations (pgloader, mysqlimport)
5. **Configuration as code** - YAML/JSON config files (Airbyte, Meltano)
6. **Connector pattern** - Database-specific implementations (Airbyte, Meltano)
7. **CLI-first design** - Terminal-friendly interfaces (all tools)

**What undatum should adopt:**
- ✅ URI-based connections (already partially implemented)
- ✅ Incremental loading modes (append, replace, upsert)
- ✅ Native bulk methods for each database
- ✅ Configuration file support (YAML/JSON)
- ✅ Connector pattern for extensibility
- ⚠️ SQL preprocessing (could be useful but adds complexity)

**What undatum should avoid:**
- ❌ Over-complexity (Airbyte is too heavy)
- ❌ Database-specific tools (pgloader is PostgreSQL-only)
- ❌ GUI dependencies (undatum is CLI-only)

---

## 3. Database Systems to Support

### 2.1 High Priority Databases

#### 2.1.1 PostgreSQL
**Why:** Most popular open-source relational database, widely used in production

**Key Features for Ingestion:**
- `COPY FROM` command: Extremely fast bulk loading (10-100x faster than INSERT)
- `INSERT ... ON CONFLICT`: Upsert support for idempotent ingestion
- Transaction support: Atomic batch operations
- Connection pooling: Built-in support via `psycopg2.pool` or SQLAlchemy
- Schema support: Can query schema before ingestion

**Recommended Library:** `psycopg2` or `psycopg3` for performance, SQLAlchemy for abstraction

**Performance Tips:**
- Use `COPY FROM` for bulk loads (fastest method)
- Disable indexes/constraints during bulk load, re-enable after
- Use unlogged tables for staging, then copy to logged tables
- Batch size: 10,000-50,000 rows per batch
- Wrap batches in transactions for atomicity

**Connection String Format:**
```
postgresql://user:password@host:port/database
postgresql+psycopg2://user:password@host:port/database  # SQLAlchemy
```

#### 2.1.2 MySQL / MariaDB
**Why:** Second most popular open-source relational database

**Key Features for Ingestion:**
- `LOAD DATA INFILE`: Fast bulk loading from CSV files
- `INSERT ... ON DUPLICATE KEY UPDATE`: Upsert support
- Multi-row INSERT: Better than single-row inserts
- Connection pooling: Via `mysql.connector.pooling` or SQLAlchemy

**Recommended Library:** `mysql-connector-python` or `PyMySQL`, SQLAlchemy for abstraction

**Performance Tips:**
- Use `LOAD DATA INFILE` when possible (fastest)
- Use multi-row INSERT statements (1000-5000 rows per INSERT)
- Disable indexes during bulk load
- Use InnoDB engine (transactional) over MyISAM
- Batch size: 5,000-20,000 rows per batch

**Connection String Format:**
```
mysql://user:password@host:port/database
mysql+pymysql://user:password@host:port/database  # SQLAlchemy
```

#### 2.1.3 SQLite
**Why:** Embedded database, great for local development and small datasets

**Key Features for Ingestion:**
- Zero-configuration: No server setup needed
- Fast for small-medium datasets
- Transaction support for atomic operations
- PRAGMA optimizations: Can tune for bulk loading

**Recommended Library:** Built-in `sqlite3`, or `sqlalchemy` for consistency

**Performance Tips:**
- Use PRAGMA optimizations: `synchronous=OFF`, `journal_mode=WAL`
- Wrap batches in transactions
- Disable foreign key checks during load
- Batch size: 5,000-10,000 rows per batch (limited by single writer)
- Use memory mapping for large databases

**Connection String Format:**
```
sqlite:///path/to/database.db
sqlite:///:memory:  # In-memory database
```

#### 2.1.4 DuckDB
**Why:** Already used in undatum for counting, analytical database with excellent performance

**Key Features for Ingestion:**
- Columnar storage: Excellent for analytical workloads
- Parquet integration: Native support for Parquet files
- COPY command: Fast bulk loading similar to PostgreSQL
- Appender API: Efficient programmatic insertion
- No separate server: Embedded database

**Recommended Library:** `duckdb` (already a dependency)

**Performance Tips:**
- Use COPY FROM for bulk loads (similar to PostgreSQL)
- Use Appender API for streaming insertion
- Disable row group synchronization during bulk load
- Batch size: 50,000-100,000 rows per batch
- Use Parquet format for intermediate storage

**Connection String Format:**
```
duckdb://path/to/database.db
duckdb://:memory:  # In-memory database
```

### 3.2 Additional NoSQL Databases

#### 3.2.1 Apache Cassandra
**Why:** Distributed NoSQL database, excellent for write-heavy workloads

**Key Features for Ingestion:**
- CQL (Cassandra Query Language) for data definition
- Batch statements and prepared statements for bulk inserts
- Async/concurrent writes for high throughput
- Token-aware routing for optimal performance

**Recommended Library:** `cassandra-driver` (DataStax) or `acsylla` (async)

**Performance Tips:**
- Use prepared statements for repeated inserts
- Batch writes (but don't overfill batches)
- Use token-aware load balancing
- Batch size: 1,000-5,000 rows per batch
- Async execution for concurrent writes

**Connection String Format:**
```
cassandra://host1,host2/keyspace?username=user&password=pass
```

**Use Cases:**
- Time-series data
- High-volume writes
- Distributed systems
- Event logging

#### 3.2.2 ScyllaDB
**Why:** High-performance Cassandra-compatible database

**Key Features for Ingestion:**
- Drop-in Cassandra replacement with better performance
- CQL compatibility
- DynamoDB-compatible API via "Alternator"
- Shard-aware routing

**Recommended Library:** `scylla-driver` or `scyllapy` (async)

**Performance Tips:**
- Similar to Cassandra but faster
- Use shard-aware routing when available
- Prepared statements and batch operations
- Batch size: 1,000-5,000 rows per batch

**Connection String Format:**
```
scylladb://host1,host2/keyspace?username=user&password=pass
```

#### 3.2.3 CouchDB
**Why:** Document database with REST API, good for JSON data

**Key Features for Ingestion:**
- JSON document storage
- Bulk document endpoints (`_bulk_docs`)
- RESTful API
- Built-in replication

**Recommended Library:** `py-couchdb` or `couchdbkit`

**Performance Tips:**
- Use `bulk_docs` for batch writes
- Minimize network overhead with batching
- Handle document revision conflicts
- Batch size: 1,000-5,000 documents per batch

**Connection String Format:**
```
couchdb://user:pass@host:5984/database
http://user:pass@host:5984/database
```

**Use Cases:**
- JSON document storage
- Content management
- Mobile app backends
- Replication scenarios

#### 3.2.4 Neo4j
**Why:** Graph database for relationship-heavy data

**Key Features for Ingestion:**
- Graph model (nodes, relationships, properties)
- Cypher query language
- `UNWIND` for bulk operations
- `LOAD CSV` for file imports

**Recommended Library:** `neo4j` (official Python driver)

**Performance Tips:**
- Use `UNWIND` to batch multiple rows in single Cypher query
- Use MERGE carefully (more expensive than CREATE)
- Periodic commits for large loads
- Batch size: 1,000-10,000 nodes/relationships per batch

**Connection String Format:**
```
neo4j://user:pass@host:7687
bolt://user:pass@host:7687
```

**Use Cases:**
- Social networks
- Recommendation systems
- Fraud detection
- Knowledge graphs

#### 3.2.5 DynamoDB (AWS)
**Why:** Managed NoSQL database, widely used in cloud environments

**Key Features for Ingestion:**
- Key-value store with optional sort keys
- Batch operations (`batch_write_item`)
- `batch_writer()` context manager for automatic chunking
- DynamoDB Streams for change data capture

**Recommended Library:** `boto3` (AWS SDK)

**Performance Tips:**
- Use `batch_writer()` for automatic chunking (max 25 items)
- Automatic retry for unprocessed items
- Design partition keys to avoid hot partitions
- Batch size: 25 items per batch (DynamoDB limit)

**Connection String Format:**
```
dynamodb://region/table?access_key=...&secret_key=...
aws://region/table  # Uses AWS credentials from environment
```

**Use Cases:**
- Cloud-native applications
- Serverless architectures
- High-throughput key-value lookups
- AWS ecosystem integration

**Note:** Requires AWS credentials and boto3 dependency

### 3.3 Additional RDBMS Databases

#### 3.3.1 Oracle Database
**Why:** Enterprise database, widely used in large organizations

**Key Features for Ingestion:**
- Direct Path Loads (python-oracledb 3.4+): Extremely fast bulk loading
- `executemany()` for batch inserts
- SQL*Loader for file-based loading
- External tables for staging

**Recommended Library:** `python-oracledb` (formerly cx_Oracle)

**Performance Tips:**
- Use Direct Path Loads for maximum performance (bypasses SQL buffer)
- Direct Path Loads have restrictions (no triggers, minimal constraints)
- Batch size: 10,000-100,000 rows for Direct Path Loads
- Use `executemany()` for moderate-sized loads

**Connection String Format:**
```
oracle://user:pass@host:1521/service_name
oracle+cx_oracle://user:pass@host:1521/service_name  # SQLAlchemy
```

**Use Cases:**
- Enterprise applications
- Large-scale data warehousing
- Legacy system integration
- High-performance OLTP

**Note:** Direct Path Loads require specific privileges and have schema restrictions

#### 3.3.2 Microsoft SQL Server
**Why:** Enterprise database, widely used in Windows environments

**Key Features for Ingestion:**
- `BULK INSERT` / `OPENROWSET (BULK...)` for file-based loading
- `fast_executemany` with pyodbc for parameter arrays
- SQL Server Integration Services (SSIS) for complex ETL
- Table-valued parameters for bulk operations

**Recommended Library:** `pyodbc` with `fast_executemany=True` or `pymssql`

**Performance Tips:**
- Use `BULK INSERT` when file is accessible to SQL Server (fastest)
- Use `fast_executemany` with pyodbc for code-based loading
- File must be accessible to SQL Server process (UNC share or local)
- Batch size: 5,000-10,000 rows per batch
- Disable indexes during bulk load

**Connection String Format:**
```
mssql+pyodbc://user:pass@host:1433/database?driver=ODBC+Driver+17+for+SQL+Server
mssql+pymssql://user:pass@host:1433/database
```

**Use Cases:**
- Windows-based enterprise systems
- .NET application integration
- Business intelligence workloads
- Legacy system migration

**Note:** `BULK INSERT` requires file access permissions and specific server privileges

#### 3.3.3 MariaDB
**Why:** MySQL-compatible database with enhanced features

**Key Features for Ingestion:**
- `LOAD DATA [LOCAL] INFILE` for fast file-based loading
- `executemany()` with MariaDB Connector/Python
- `mariadb-import` utility (formerly mysqlimport)
- ColumnStore for analytical workloads

**Recommended Library:** `mariadb` (MariaDB Connector/Python)

**Performance Tips:**
- Use `LOAD DATA LOCAL INFILE` for fastest file-based loading
- Enable `local_infile` option in connection
- Format must match SQL specification (delimiters, quoting, line endings)
- Batch size: 5,000-20,000 rows per batch
- Table locking during `LOAD DATA` (use `LOW_PRIORITY` if needed)

**Connection String Format:**
```
mariadb://user:pass@host:3306/database
mariadb+pymysql://user:pass@host:3306/database  # SQLAlchemy
```

**Use Cases:**
- MySQL replacement with enhanced features
- High-performance web applications
- Data warehousing (with ColumnStore)
- Open-source database solutions

**Note:** `LOAD DATA LOCAL INFILE` requires `FILE` privilege and `local_infile` enabled

### 3.4 Cloud Data Warehouses (Future Consideration)

#### 3.4.1 Snowflake
**Approach:** Cloud data warehouse with excellent scalability

**Key Features:**
- `COPY INTO` for bulk loading
- `PUT`/`GET` for staging files
- Excellent for analytics workloads
- Automatic scaling

**Recommended Library:** `snowflake-connector-python`

**Connection String Format:**
```
snowflake://user:pass@account.region.snowflakecomputing.com/database?warehouse=WH&role=ROLE
```

#### 3.4.2 Google BigQuery
**Approach:** Serverless data warehouse

**Key Features:**
- `load_table_from_dataframe` for bulk loading
- `LOAD DATA` for file-based loading
- Excellent for analytics
- Pay-per-query pricing

**Recommended Library:** `google-cloud-bigquery`

**Connection String Format:**
```
bigquery://project_id/dataset?credentials_path=/path/to/key.json
```

#### 3.4.3 Amazon Redshift
**Approach:** Cloud data warehouse (PostgreSQL-compatible)

**Key Features:**
- `COPY` command (similar to PostgreSQL)
- Columnar storage
- Excellent for analytics
- AWS ecosystem integration

**Recommended Library:** `redshift-connector` or `psycopg2` (PostgreSQL-compatible)

**Connection String Format:**
```
redshift+psycopg2://user:pass@host:5439/database
```

### 3.5 Medium Priority Databases (Previously Listed)

#### 3.5.1 Redis
**Why:** Popular for caching, queues, and real-time data

**Use Cases:**
- Redis Streams: Persistent ingestion pipeline
- Lists/Sorted Sets: Simple queue or buffer
- Time Series: Metrics ingestion (via RedisTimeSeries module)
- Pub/Sub: Event streaming

**Recommended Library:** `redis` or `redis-py`

**Implementation Considerations:**
- Use pipelining for batch writes
- Use Redis Streams for persistent ingestion
- Consider consumer groups for multiple consumers
- Batch size: 1,000-5,000 operations per pipeline

#### 3.5.2 ClickHouse
**Why:** Fast analytical database, excellent for time-series and analytics

**Key Features:**
- Very fast inserts (millions of rows per second)
- Columnar storage
- Native support for JSON and CSV
- HTTP interface for easy integration

**Recommended Library:** `clickhouse-driver` or `clickhouse-connect`

**Performance Tips:**
- Use batch INSERT (10,000-100,000 rows)
- Disable indexes during bulk load
- Use native format for best performance
- Use async inserts for better throughput

#### 3.5.3 InfluxDB
**Why:** Popular time-series database for metrics and monitoring

**Key Features:**
- Line Protocol: Simple text-based ingestion format
- High write throughput
- Built-in time-series optimizations

**Recommended Library:** `influxdb-client`

### 3.6 Database Support Priority Summary

**High Priority (Implement First):**
1. PostgreSQL - Most popular open-source RDBMS
2. MySQL/MariaDB - Second most popular RDBMS
3. SQLite - Embedded database, great for local development
4. DuckDB - Already a dependency, analytical database

**Medium Priority (Implement After Core RDBMS):**
5. Redis - Caching and streaming use cases
6. ClickHouse - Analytical database
7. InfluxDB - Time-series database

**Lower Priority (Consider Based on Demand):**
8. Cassandra/ScyllaDB - Distributed NoSQL
9. CouchDB - Document database
10. Neo4j - Graph database
11. DynamoDB - AWS NoSQL
12. Oracle - Enterprise database
13. SQL Server - Enterprise database
14. Cloud warehouses (Snowflake, BigQuery, Redshift) - Specialized use cases

**Snowflake:**
- Bulk loading via `COPY INTO` or `PUT`/`GET`
- Excellent for cloud data warehousing
- Library: `snowflake-connector-python`

**Google BigQuery:**
- Bulk loading via `load_table_from_dataframe` or `LOAD DATA`
- Excellent for analytics
- Library: `google-cloud-bigquery`

**Amazon Redshift:**
- Bulk loading via `COPY` command
- Similar to PostgreSQL for ingestion
- Library: `redshift-connector` or `psycopg2`

---

## 4. Best Practices for Database Ingestion

### 4.1 General Best Practices

#### 4.1.1 Batch Processing
- **Optimal batch sizes:** 1,000-50,000 rows depending on database
  - PostgreSQL: 10,000-50,000
  - MySQL: 5,000-20,000
  - SQLite: 5,000-10,000
  - MongoDB: 10,000-100,000 (already good at 50,000)
  - Elasticsearch: 1,000-5,000 (already using bulk API)

- **Memory considerations:** Keep batches small enough to fit in memory
- **Balance overhead vs throughput:** Smaller batches = more overhead, larger batches = more memory

#### 4.1.2 Transaction Management
- **Wrap batches in transactions:** Ensures atomicity
- **Commit after each batch:** Prevents long-running transactions
- **Handle rollback on errors:** Maintain data consistency
- **Use savepoints:** For partial batch recovery

#### 4.1.3 Connection Management
- **Connection pooling:** Reuse connections, avoid per-batch connection overhead
- **Connection timeout:** Set appropriate timeouts for network issues
- **Retry logic:** Handle transient connection failures
- **Connection validation:** Verify connections before starting ingestion

#### 4.1.4 Error Handling
- **Retry with exponential backoff:** Handle transient failures
- **Partial batch handling:** Continue after single-record failures
- **Error logging:** Log failed records for later retry
- **Resume capability:** Track progress for interrupted ingestion

#### 4.1.5 Schema Management
- **Pre-flight checks:** Verify table exists and schema matches
- **Auto-create tables:** Option to create tables from data schema
- **Schema evolution:** Handle schema changes gracefully
- **Type mapping:** Map Python types to database types correctly

### 4.2 Database-Specific Best Practices

#### 4.2.1 PostgreSQL
- **Use COPY FROM:** Fastest bulk loading method
- **Disable constraints during load:** Drop indexes, foreign keys, triggers
- **Use unlogged tables:** For staging, then copy to logged tables
- **ANALYZE after load:** Update query planner statistics
- **Parallel loading:** Use multiple connections for parallel batches

#### 4.2.2 MySQL
- **Use LOAD DATA INFILE:** Fastest method for CSV files
- **Multi-row INSERT:** Better than single-row inserts
- **Disable indexes:** Drop indexes before bulk load
- **InnoDB settings:** Tune `innodb_buffer_pool_size`, `innodb_flush_log_at_trx_commit`
- **Connection pooling:** Use connector pooling features

#### 4.2.3 SQLite
- **PRAGMA optimizations:** 
  - `PRAGMA synchronous = OFF` (temporarily)
  - `PRAGMA journal_mode = WAL`
  - `PRAGMA foreign_keys = OFF` (during load)
- **Batch transactions:** Wrap multiple inserts in transaction
- **Memory mapping:** For large databases
- **Single writer:** SQLite allows one writer at a time

#### 4.2.4 DuckDB
- **COPY FROM:** Similar to PostgreSQL
- **Appender API:** For streaming insertion
- **Disable row group sync:** During bulk load
- **Parquet intermediate:** Use Parquet for staging large loads
- **Preserve insertion order:** Disable if not needed for memory efficiency

#### 4.2.5 NoSQL Databases

**MongoDB:**
- Use `insert_many()` for batch operations
- Batch size: 10,000-100,000 documents
- Use ordered=false for better performance (if order doesn't matter)
- Connection pooling via pymongo

**Elasticsearch:**
- Use bulk API with proper formatting
- Batch size: 1,000-5,000 documents
- Use pipeline parameter for preprocessing
- Connection pooling and retry logic

**Cassandra/ScyllaDB:**
- Use prepared statements
- Batch size: 1,000-5,000 rows
- Token-aware routing
- Async execution for concurrency

**CouchDB:**
- Use `bulk_docs` endpoint
- Batch size: 1,000-5,000 documents
- Handle revision conflicts
- Minimize network round-trips

**Neo4j:**
- Use `UNWIND` for batching in Cypher
- Batch size: 1,000-10,000 nodes/relationships
- Periodic commits
- Use MERGE carefully

**DynamoDB:**
- Use `batch_writer()` context manager
- Batch size: 25 items (DynamoDB limit)
- Automatic retry for unprocessed items
- Design partition keys to avoid hotspots

**Redis:**
- Use pipelining for batch writes
- Batch size: 1,000-5,000 operations
- Use Redis Streams for persistent ingestion
- Consider consumer groups for multiple consumers

### 4.3 Performance Optimization

#### 4.3.1 Bulk Insert Methods (Fastest to Slowest)
1. **Native COPY/LOAD DATA:** Fastest (PostgreSQL COPY, MySQL LOAD DATA)
2. **Bulk INSERT statements:** Multi-row INSERT (MySQL, PostgreSQL)
3. **Prepared statements with executemany:** Good performance
4. **Row-by-row INSERT:** Slowest, avoid for bulk loads

#### 4.3.2 Index Management
- **Drop indexes before bulk load:** Rebuild after load
- **Defer constraint checks:** For foreign keys
- **Disable triggers:** Temporarily during bulk load
- **Create indexes in parallel:** After data load

#### 4.3.3 Memory Management
- **Streaming processing:** Don't load entire file into memory
- **Generator-based batching:** Use iterators for memory efficiency
- **Clear batches after commit:** Free memory between batches
- **Monitor memory usage:** Especially for large batches

### 4.4 Data Type Mapping

**Python to Database Type Mapping:**
- `int` → INTEGER/BIGINT
- `float` → FLOAT/DOUBLE
- `str` → VARCHAR/TEXT
- `bool` → BOOLEAN/TINYINT
- `datetime` → TIMESTAMP/DATETIME
- `date` → DATE
- `None` → NULL

**Considerations:**
- String length limits (VARCHAR vs TEXT)
- Numeric precision (FLOAT vs DECIMAL)
- Date/time timezone handling
- JSON/JSONB for nested structures
- Array types for lists

### 4.5 Insights from Similar Tools

**From Ingestr:**
- Incremental loading modes (append, replace, upsert) are essential
- URI-based connection strings are user-friendly
- Simple CLI interface is effective

**From pgloader:**
- Database-specific optimizations are critical for performance
- Multi-source support demonstrates value
- Complex configuration can be optional

**From Airbyte/Meltano:**
- Connector pattern enables extensibility
- Configuration as code (YAML/JSON) improves usability
- Plugin architecture allows community contributions

**From dsq:**
- SQL preprocessing can be useful for filtering
- Multi-format support is valuable
- Query interface could enhance pre-ingestion workflow

**From Native Tools (mysqlimport, etc.):**
- Native bulk load methods are fastest
- File-based approach can be optimized
- Simple CLI interface is effective

---

## 5. Improvement Recommendations

### 5.1 Critical Fixes

#### 5.1.1 Fix Bugs
1. **Fix typo in line 85:** Change `'dro[]'` to `'drop'`
2. **Fix batch size inconsistency:** Align code default with CLI default
3. **Use timeout option:** Pass timeout to database clients
4. **Fix Elasticsearch doc_id handling:** Handle missing document ID field gracefully

#### 5.1.2 Improve Error Handling
1. **Add retry logic:** Exponential backoff for transient failures
2. **Partial batch recovery:** Continue after single-record failures
3. **Error logging:** Log failed records with context
4. **Resume capability:** Track progress for interrupted ingestion

### 5.2 Feature Additions

#### 5.2.1 Relational Database Support
**Priority: HIGH**

**PostgreSQL Support:**
- Use `psycopg2` COPY FROM for best performance
- Support upsert with `ON CONFLICT`
- Connection pooling via SQLAlchemy or psycopg2.pool
- Transaction management

**MySQL Support:**
- Use `LOAD DATA INFILE` when possible
- Multi-row INSERT for other cases
- Connection pooling
- Upsert with `ON DUPLICATE KEY UPDATE`

**SQLite Support:**
- Use built-in `sqlite3` or SQLAlchemy
- PRAGMA optimizations for bulk load
- Transaction batching

**DuckDB Support:**
- Use COPY FROM or Appender API
- Leverage existing DuckDB dependency
- Parquet intermediate format support

#### 5.2.2 Enhanced Features
1. **Upsert support:** `INSERT ... ON CONFLICT` / `ON DUPLICATE KEY UPDATE`
2. **Schema auto-creation:** Create tables from data schema
3. **Schema validation:** Pre-flight checks for compatibility
4. **Connection pooling:** Reuse connections across batches
5. **Progress tracking:** Better progress reporting with ETA
6. **Resume capability:** Track progress and resume from checkpoint
7. **Data transformation:** Apply transformations during ingestion
8. **Parallel ingestion:** Multiple workers for large files

### 5.3 Performance Improvements

#### 5.3.1 Bulk Insert Optimization
- Use database-specific bulk insert methods (COPY, LOAD DATA)
- Optimize batch sizes per database type
- Disable indexes/constraints during bulk load
- Re-enable and rebuild after load

#### 5.3.2 Memory Optimization
- Ensure streaming (already done with iterabledata)
- Clear batches after commit
- Monitor memory usage
- Use generators for very large datasets

#### 5.3.3 Connection Optimization
- Connection pooling for relational databases
- Reuse connections across batches
- Validate connections before use
- Handle connection failures gracefully

### 5.4 Usability Improvements

#### 5.4.1 Better CLI Options
- `--upsert`: Enable upsert mode
- `--create-table`: Auto-create tables
- `--validate-schema`: Pre-flight schema validation
- `--chunk-size`: Override batch size per database
- `--max-retries`: Configure retry attempts
- `--resume`: Resume from checkpoint

#### 5.4.2 Better Output
- More detailed progress information
- Error summary at end
- Performance metrics (rows/second, time elapsed)
- Success/failure statistics

#### 5.4.3 Documentation
- Examples for each database type
- Connection string formats
- Performance tuning guide
- Troubleshooting common issues

### 5.5 Features Inspired by Similar Tools

**From Ingestr:**
- Incremental loading modes: `--mode append|replace|upsert`
- Simple URI-based connections
- Progress tracking and resume capability

**From Airbyte/Meltano:**
- Configuration file support (YAML/JSON)
- Connector pattern for extensibility
- Plugin architecture for community contributions

**From dsq:**
- SQL-based filtering before ingestion
- Query interface for data transformation

**From pgloader:**
- Database-specific optimizations
- Performance-focused approach
- Multi-source support

---

## 6. Implementation Strategy

### 6.1 Architecture Improvements

#### 6.1.1 Refactor Ingester Base Class
```python
class BasicIngester:
    """Base class for data ingestion."""
    def __init__(self, uri, **options):
        self.uri = uri
        self.options = options
        self.connection = None
        
    def connect(self):
        """Establish database connection."""
        raise NotImplementedError
        
    def disconnect(self):
        """Close database connection."""
        raise NotImplementedError
        
    def validate_schema(self, schema):
        """Validate data schema against database schema."""
        raise NotImplementedError
        
    def create_table(self, schema, table_name):
        """Create table from schema."""
        raise NotImplementedError
        
    def ingest(self, batch):
        """Ingest a batch of records."""
        raise NotImplementedError
        
    def upsert(self, batch):
        """Upsert a batch of records."""
        raise NotImplementedError
```

#### 6.1.2 Database-Specific Implementations
- `PostgresIngester`: Uses psycopg2 COPY FROM
- `MySQLIngester`: Uses LOAD DATA INFILE or multi-row INSERT
- `SQLiteIngester`: Uses executemany with transactions
- `DuckDBIngester`: Uses COPY or Appender API
- `RedisIngester`: Uses pipelining or streams
- `MongoIngester`: Improve existing implementation
- `ElasticsearchIngester`: Improve existing implementation

#### 6.1.3 Connection Management
- Connection pool per database type
- Connection validation before use
- Automatic reconnection on failure
- Connection lifecycle management

### 6.2 Implementation Phases

#### Phase 1: Fixes and Improvements (Quick Wins)
**Duration:** 1-2 weeks

1. Fix existing bugs (typo, batch size, timeout)
2. Improve error handling (retry logic, error logging)
3. Add connection pooling for MongoDB/Elasticsearch
4. Better progress reporting

#### Phase 2: PostgreSQL Support
**Duration:** 2-3 weeks

1. Implement `PostgresIngester` with COPY FROM
2. Add upsert support (`ON CONFLICT`)
3. Schema validation and auto-creation
4. Connection pooling
5. Tests and documentation

#### Phase 3: MySQL and SQLite Support
**Duration:** 2-3 weeks

1. Implement `MySQLIngester` (LOAD DATA INFILE + multi-row INSERT)
2. Implement `SQLiteIngester` (executemany with PRAGMA optimizations)
3. Shared connection management
4. Tests and documentation

#### Phase 4: DuckDB Support
**Duration:** 1-2 weeks

1. Implement `DuckDBIngester` (COPY or Appender API)
2. Leverage existing DuckDB integration
3. Parquet intermediate format support
4. Tests and documentation

#### Phase 5: Additional NoSQL Databases
**Duration:** 2-3 weeks

1. Implement `CassandraIngester` (prepared statements, batch operations)
2. Implement `CouchDBIngester` (bulk_docs endpoint)
3. Implement `Neo4jIngester` (UNWIND batching)
4. Implement `DynamoDBIngester` (batch_writer)
5. Tests and documentation

#### Phase 6: Enterprise RDBMS Support
**Duration:** 2-3 weeks

1. Implement `OracleIngester` (Direct Path Loads, executemany)
2. Implement `SQLServerIngester` (BULK INSERT, fast_executemany)
3. Implement `MariaDBIngester` (LOAD DATA INFILE)
4. Tests and documentation

#### Phase 7: Advanced Features
**Duration:** 2-3 weeks

1. Resume capability (checkpoint tracking)
2. Parallel ingestion (multiple workers)
3. Schema evolution handling
4. Performance optimizations (index management)

#### Phase 8: Cloud Databases (Optional)
**Duration:** As needed

1. Redis support
2. ClickHouse support
3. Cloud databases (Snowflake, BigQuery, Redshift)

### 6.3 Dependencies to Add

**Required for Core RDBMS:**
- `psycopg2` or `psycopg3`: PostgreSQL support
- `mysql-connector-python` or `PyMySQL`: MySQL support
- `sqlalchemy`: Database abstraction (optional but recommended)

**Optional for Additional RDBMS:**
- `python-oracledb`: Oracle Database support (Direct Path Loads)
- `pyodbc` or `pymssql`: SQL Server support
- `mariadb`: MariaDB support (or use MySQL connector)

**Optional for NoSQL Databases:**
- `cassandra-driver` or `acsylla`: Cassandra/ScyllaDB support
- `py-couchdb` or `couchdbkit`: CouchDB support
- `neo4j`: Neo4j graph database support
- `boto3`: DynamoDB support (AWS)
- `redis`: Redis support

**Optional for Specialized Databases:**
- `clickhouse-driver` or `clickhouse-connect`: ClickHouse support
- `influxdb-client`: InfluxDB support
- `snowflake-connector-python`: Snowflake support
- `google-cloud-bigquery`: BigQuery support
- `redshift-connector`: Redshift support

**Note:** 
- SQLite support can use built-in `sqlite3` module, no additional dependency needed
- DuckDB is already a dependency
- MongoDB (`pymongo`) and Elasticsearch (`elasticsearch`) are already dependencies

### 6.4 Testing Strategy

#### Unit Tests
- Test each ingester class independently
- Test batch processing logic
- Test error handling and retries
- Test connection management

#### Integration Tests
- Test with real database instances (Docker containers)
- Test with various input formats (CSV, JSONL, Parquet)
- Test with large datasets (performance testing)
- Test error scenarios (connection failures, schema mismatches)

#### Performance Tests
- Benchmark batch sizes
- Compare performance across databases
- Measure memory usage
- Test with very large files (100M+ rows)

---

## 7. Comparison with Existing Tools

### 7.1 Similar Tools

| Tool | Strengths | Weaknesses | undatum Advantage |
|------|-----------|------------|-------------------|
| **pgloader** | Fast PostgreSQL loading, multiple sources | PostgreSQL only, complex setup | Multi-database support, simpler CLI |
| **csvkit** | CSV tools, database import | Limited database support | Better format support, streaming |
| **mongoimport** | MongoDB native import | MongoDB only | Multi-database, better error handling |
| **mysqlimport** | MySQL native import | MySQL only | Multi-database, unified interface |
| **pandas.to_sql()** | Easy Python API | Memory-intensive, slower | Streaming, CLI-friendly, faster |
| **Custom scripts** | Flexible | Requires coding | Built-in, no coding needed |

### 7.2 undatum's Unique Value

1. **Multi-database support:** Single tool for multiple databases
2. **Format abstraction:** Works with CSV, JSONL, Parquet, etc.
3. **Streaming efficiency:** Low memory footprint for large files
4. **CLI simplicity:** No need to write code
5. **Integrated workflow:** Part of larger data processing toolkit

---

## 8. Technical Considerations

### 8.1 Performance Targets

**Throughput Goals:**
- PostgreSQL (COPY): 100,000+ rows/second
- MySQL (LOAD DATA): 50,000+ rows/second
- SQLite: 10,000+ rows/second
- DuckDB (COPY): 200,000+ rows/second
- MongoDB (insert_many): 50,000+ rows/second (already achieved)

**Memory Goals:**
- Keep memory usage under 500MB for typical batch sizes
- Stream processing (no full file loading)
- Clear batches after commit

### 8.2 Error Handling Strategy

**Transient Errors (Retry):**
- Connection timeouts
- Network errors
- Database busy/locked
- Rate limiting

**Permanent Errors (Skip/Log):**
- Schema mismatches
- Invalid data types
- Constraint violations
- Missing required fields

**Error Reporting:**
- Log failed records to separate file
- Summary statistics at end
- Exit codes for automation

### 8.3 Configuration

**CLI Options:**
```bash
undatum ingest input.jsonl \
  --uri postgresql://user:pass@host:port/db \
  --dbtype postgresql \
  --table users \
  --batch 10000 \
  --upsert \
  --create-table \
  --max-retries 3 \
  --resume
```

**Config File Support:**
```yaml
ingestion:
  postgresql:
    batch_size: 10000
    use_copy: true
    connection_pool_size: 5
  mysql:
    batch_size: 5000
    use_load_data: true
  mongodb:
    batch_size: 50000
    retry_on_failure: true
```

### 8.4 Security Considerations

**Connection Security:**
- Support SSL/TLS connections
- Encrypted connection strings
- Environment variable for sensitive credentials

**Input Validation:**
- Validate connection strings
- Sanitize table/collection names
- Prevent SQL injection (use parameterized queries)

**Access Control:**
- Respect database user permissions
- No privilege escalation
- Safe error messages (no credential leaks)

---

## 9. Open Questions

1. **Batch Size Defaults:** Should batch sizes be database-specific defaults or single default for all?

2. **Upsert Strategy:** Should upsert be default or opt-in? How to handle conflict resolution?

3. **Schema Auto-Creation:** Should tables be auto-created by default or require explicit flag?

4. **Error Handling:** Should failures stop ingestion or continue with logging? Per-record or per-batch?

5. **Connection Pooling:** Should pooling be enabled by default or opt-in? What pool sizes?

6. **DuckDB Integration:** Should DuckDB be a database target or just used for intermediate processing?

7. **Transaction Boundaries:** Should each batch be a separate transaction or larger transaction groups?

8. **Resume Capability:** How to implement checkpoints? File-based or database-backed?

---

## 10. Recommendations

### 10.1 Immediate Actions

1. **Fix critical bugs:** Typo fix, batch size alignment, timeout usage
2. **Add PostgreSQL support:** Highest priority, most requested database
3. **Improve error handling:** Retry logic, better error messages
4. **Add connection pooling:** Performance improvement for all databases

### 10.2 Short-term Improvements (1-3 months)

1. **Add MySQL and SQLite support:** Expand relational database coverage
2. **Add DuckDB support:** Leverage existing integration
3. **Add upsert support:** Critical for idempotent ingestion
4. **Schema auto-creation:** Improve usability

### 10.3 Long-term Enhancements (3-6 months)

1. **Resume capability:** Handle interrupted ingestion
2. **Parallel ingestion:** Performance for very large files
3. **Additional databases:** Redis, ClickHouse, cloud databases
4. **Performance optimizations:** Index management, bulk load optimizations

### 10.4 Success Metrics

**Technical Metrics:**
- Throughput: Rows/second per database
- Memory usage: MB per million rows
- Error rate: Percentage of failed records
- Reliability: Success rate for full file ingestion

**User Metrics:**
- Command usage frequency
- Most popular database types
- User feedback and feature requests
- Integration with existing workflows

---

## 11. Conclusion

The `ingest` command is a valuable feature of undatum but has significant room for improvement. Expanding support to relational databases (especially PostgreSQL) would dramatically increase its utility, while fixing existing bugs and improving error handling would improve reliability.

**Recommended Next Steps:**
1. **Create OpenSpec proposal** - Document database ingestion improvements as formal change proposal
2. **Start with Phase 1** - Fix bugs and improve error handling
3. **Implement PostgreSQL support** - Highest priority addition
4. **Iterate based on feedback** - Add other databases based on user demand

**Key Success Factors:**
- Maintain undatum's simplicity and CLI-first approach
- Leverage database-specific optimizations for performance
- Provide consistent interface across all databases
- Ensure backward compatibility with existing MongoDB/Elasticsearch support
- Focus on reliability and error handling

The improvements outlined in this report would transform `ingest` from a niche tool for NoSQL databases into a comprehensive database ingestion solution, significantly enhancing undatum's value proposition.

---

## 12. References

### Research Sources
- PostgreSQL bulk loading best practices (PostgreSQL documentation)
- MySQL bulk data loading guide (MySQL documentation)
- SQLite performance optimization (SQLite documentation)
- DuckDB ingestion guide (DuckDB documentation)
- Database ingestion tools comparison (industry research)
- SQLAlchemy documentation (database abstraction)
- Python database drivers (psycopg2, mysql-connector-python)

### undatum Codebase
- `undatum/cmds/ingester.py` - Current implementation
- `undatum/core.py` - CLI command definitions
- `undatum/common/iterable.py` - Iterable data processing
- `requirements.txt` / `pyproject.toml` - Dependencies
- `README.md` - Current feature documentation

### Related Libraries
- `iterabledata` - Streaming data processing (already integrated)
- `duckdb` - Analytical database (already integrated)
- `pymongo` - MongoDB driver (already integrated)
- `elasticsearch` - Elasticsearch client (already integrated)
- `psycopg2` / `psycopg3` - PostgreSQL driver (to be added)
- `mysql-connector-python` - MySQL driver (to be added)
- `sqlalchemy` - Database abstraction (optional)

---

---

## 13. Summary of Key Additions

### 13.1 Similar Tools Analysis

This report now includes comprehensive analysis of similar data ingestion tools:
- **dsq**: SQL-based querying and preprocessing
- **csvkit**: CSV-focused toolset with SQL generation
- **Ingestr**: Multi-database CLI tool with incremental loading
- **Airbyte/Meltano**: Full ETL frameworks with connector patterns
- **pgloader**: PostgreSQL-specific optimizations
- **Native tools**: mysqlimport, mariadb-import, etc.

**Key Insights:**
- URI-based connection strings are standard and user-friendly
- Incremental loading modes (append, replace, upsert) are essential
- Database-specific optimizations are critical for performance
- Connector pattern enables extensibility
- Configuration as code (YAML/JSON) improves usability

### 13.2 Expanded Database Coverage

**NoSQL Databases Added:**
- **Cassandra/ScyllaDB**: Distributed NoSQL with CQL, prepared statements, token-aware routing
- **CouchDB**: Document database with bulk_docs endpoint
- **Neo4j**: Graph database with Cypher UNWIND batching
- **DynamoDB**: AWS NoSQL with batch_writer context manager

**RDBMS Databases Added:**
- **Oracle**: Direct Path Loads for maximum performance
- **SQL Server**: BULK INSERT and fast_executemany optimizations
- **MariaDB**: LOAD DATA INFILE for fast file-based loading

**Cloud Data Warehouses:**
- **Snowflake**: COPY INTO and PUT/GET for staging
- **Google BigQuery**: load_table_from_dataframe and LOAD DATA
- **Amazon Redshift**: COPY command (PostgreSQL-compatible)

### 13.3 Database Support Priority

**High Priority (Core Implementation):**
1. PostgreSQL, MySQL, SQLite, DuckDB

**Medium Priority (After Core):**
2. Redis, ClickHouse, InfluxDB

**Lower Priority (Based on Demand):**
3. Cassandra, CouchDB, Neo4j, DynamoDB
4. Oracle, SQL Server, MariaDB
5. Cloud warehouses (Snowflake, BigQuery, Redshift)

### 13.4 Features Inspired by Similar Tools

**From Ingestr:**
- Incremental loading modes: `--mode append|replace|upsert`
- Simple URI-based connections
- Progress tracking and resume capability

**From Airbyte/Meltano:**
- Configuration file support (YAML/JSON)
- Connector pattern for extensibility
- Plugin architecture for community contributions

**From pgloader:**
- Database-specific optimizations
- Performance-focused approach
- Multi-source support

**From dsq:**
- SQL-based filtering before ingestion
- Query interface for data transformation

---

**Report Prepared By:** AI Assistant  
**Review Status:** Ready for stakeholder review  
**Last Updated:** 2025-01-27  
**Next Action:** Create OpenSpec proposal for database ingestion improvements
