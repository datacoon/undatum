# database-integration Specification

## Purpose
TBD - created by archiving change add-db-query-load. Update Purpose after archive.
## Requirements
### Requirement: Database Query Command
The system SHALL provide a `db query` command for executing SQL queries against databases and outputting results.

#### Scenario: Query PostgreSQL database
- **WHEN** user runs `undatum db query "SELECT * FROM users LIMIT 100" --db postgresql://user:pass@host/db --output results.jsonl`
- **THEN** the system SHALL connect to PostgreSQL database
- **AND** execute the SQL query
- **AND** output results in JSONL format
- **AND** stream results for large result sets

#### Scenario: Query MySQL database
- **WHEN** user runs `undatum db query "SELECT name, email FROM customers WHERE status='active'" --db mysql://user:pass@host:3306/mydb`
- **THEN** the system SHALL connect to MySQL database
- **AND** execute the query
- **AND** output results in default format (JSONL)

#### Scenario: Query SQLite database
- **WHEN** user runs `undatum db query "SELECT * FROM data" --db sqlite:///path/to/db.db --output-format csv`
- **THEN** the system SHALL connect to SQLite database
- **AND** execute the query
- **AND** output results in CSV format

#### Scenario: Query from file
- **WHEN** user runs `undatum db query --query-file query.sql --db postgresql://...`
- **THEN** the system SHALL read SQL query from file
- **AND** execute it against the database
- **AND** output results

### Requirement: Database Load Command
The system SHALL provide a `db load` command for loading data files into databases.

#### Scenario: Load data to PostgreSQL
- **WHEN** user runs `undatum db load data.parquet --db postgresql://user:pass@host/db --table users`
- **THEN** the system SHALL connect to PostgreSQL
- **AND** load data from file to specified table
- **AND** use append mode by default

#### Scenario: Load with replace mode
- **WHEN** user runs `undatum db load data.csv --db mysql://... --table customers --mode replace`
- **THEN** the system SHALL replace existing table data
- **AND** load new data from file

#### Scenario: Load with upsert mode
- **WHEN** user runs `undatum db load data.jsonl --db postgresql://... --table orders --mode upsert --key id`
- **THEN** the system SHALL update existing records or insert new ones
- **AND** use specified key field for conflict resolution

#### Scenario: Auto-create table
- **WHEN** user runs `undatum db load data.parquet --db sqlite:///db.db --table new_table --create-table`
- **THEN** the system SHALL infer schema from data
- **AND** create table if it doesn't exist
- **AND** load data

### Requirement: Database Connection Management
The system SHALL handle database connections efficiently and securely.

#### Scenario: Connection URI parsing
- **WHEN** user provides database URI
- **THEN** the system SHALL parse connection parameters
- **AND** validate URI format
- **AND** extract host, port, user, password, database name

#### Scenario: Connection pooling
- **WHEN** executing multiple operations
- **THEN** the system SHALL reuse connections when possible
- **AND** manage connection lifecycle
- **AND** handle connection errors gracefully

#### Scenario: Connection validation
- **WHEN** user provides database URI
- **THEN** the system SHALL test connection before operations
- **AND** provide clear error messages for connection failures
- **AND** support environment variable substitution

### Requirement: Output Format Support
Database query results SHALL support multiple output formats.

#### Scenario: JSONL output
- **WHEN** user runs `db query` without format specification
- **THEN** the system SHALL output results in JSONL format (default)

#### Scenario: CSV output
- **WHEN** user runs `db query ... --output-format csv`
- **THEN** the system SHALL output results in CSV format

#### Scenario: Parquet output
- **WHEN** user runs `db query ... --output-format parquet`
- **THEN** the system SHALL output results in Parquet format

### Requirement: Streaming Support
Database queries SHALL support streaming for large result sets.

#### Scenario: Large query results
- **WHEN** query returns many rows
- **THEN** the system SHALL stream results without loading all into memory
- **AND** provide progress indication
- **AND** handle memory efficiently

