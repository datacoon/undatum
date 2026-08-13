## 1. Database Query Command
- [x] 1.1 Design query command interface
  - SQL query input (string or file)
  - Database connection URI parsing
  - Output format options
  - Streaming support for large results
- [x] 1.2 Implement query executor
  - PostgreSQL query execution
  - MySQL/MariaDB query execution
  - SQLite query execution
  - Result streaming and batching
- [x] 1.3 Add output formatting
  - JSONL output (default)
  - CSV output
  - Parquet output
  - Progress tracking for large queries

## 2. Database Load Command
- [x] 2.1 Design load command interface
  - Simplified syntax compared to ingest
  - Table name specification
  - Mode options (append, replace, upsert)
  - Auto-create table support
- [x] 2.2 Implement load executor
  - Reuse existing ingest infrastructure
  - Simplify parameter passing
  - Add convenience options
- [x] 2.3 Add load-specific features
  - Schema inference from data
  - Table creation from schema
  - Upsert key specification

## 3. Database Connection Management
- [x] 3.1 Implement connection URI parser
  - PostgreSQL URI format (postgresql://user:pass@host:port/db)
  - MySQL URI format (mysql://user:pass@host:port/db)
  - SQLite URI format (sqlite:///path/to/db.db)
  - Connection parameter extraction
- [x] 3.2 Add connection pooling
  - Reuse connections for performance
  - Connection timeout handling
  - Error recovery and retry logic
  - (Basic implementation - full pooling is future enhancement)
- [x] 3.3 Add connection validation
  - Test connections before operations
  - Clear error messages for connection failures
  - Support for connection string environment variables

## 4. CLI Integration
- [x] 4.1 Add `db` command group
  - Create typer subcommand group
  - Add help text and examples
  - Integrate with main app
- [x] 4.2 Implement `db query` subcommand
  - Query parameter (required)
  - Database URI parameter
  - Output format option
  - Output file option
- [x] 4.3 Implement `db load` subcommand
  - Input file parameter
  - Database URI parameter
  - Table name option
  - Mode option (append, replace, upsert)

## 5. Error Handling
- [x] 5.1 Add query error handling
  - SQL syntax error reporting
  - Connection error handling
  - Timeout handling
  - Result size limits
- [x] 5.2 Add load error handling
  - Schema mismatch errors
  - Constraint violation errors
  - Connection errors
  - Retry logic for transient failures

## 6. Testing
- [x] 6.1 Unit tests for query executor
  - Test query execution for each database type
  - Test output formatting
  - Test streaming behavior
- [x] 6.2 Unit tests for load executor
  - Test load operations for each database type
  - Test mode options (append, replace, upsert)
  - Test schema creation
- [x] 6.3 Integration tests
  - Test end-to-end query and load workflows
  - Test with real database instances
  - Test error scenarios

## 7. Documentation
- [x] 7.1 Document query command
  - Usage examples
  - Database URI formats
  - Output format options
  - Query examples
- [x] 7.2 Document load command
  - Usage examples
  - Mode options explanation
  - Schema creation examples
  - Comparison with ingest command
- [x] 7.3 Add database integration examples
  - Query and process workflow
  - Load and validate workflow
  - ETL pipeline examples
