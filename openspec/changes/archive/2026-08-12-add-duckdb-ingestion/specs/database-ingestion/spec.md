## ADDED Requirements

### Requirement: DuckDB Database Ingestion
The system SHALL support ingesting data from files into DuckDB databases using optimized bulk loading methods.

#### Scenario: Ingest to DuckDB with COPY FROM
- **WHEN** user runs `undatum ingest file.csv duckdb:///path/to/db.db table --dbtype duckdb`
- **THEN** the system SHALL use DuckDB COPY FROM command for bulk loading
- **AND** the system SHALL achieve high throughput (200,000+ rows/second)
- **AND** the system SHALL display progress during ingestion

#### Scenario: Ingest to DuckDB with Appender API
- **WHEN** user prefers streaming insertion or COPY FROM is not suitable
- **THEN** the system SHALL use DuckDB Appender API for efficient insertion
- **AND** the system SHALL stream data without loading entire batch into memory
- **AND** the system SHALL achieve good performance (100,000+ rows/second)

#### Scenario: Ingest to DuckDB in-memory database
- **WHEN** user runs `undatum ingest file.jsonl duckdb:///:memory: table --dbtype duckdb`
- **THEN** the system SHALL create an in-memory DuckDB database
- **AND** the system SHALL ingest data into the in-memory database
- **AND** the system SHALL handle the temporary nature of in-memory databases

#### Scenario: Ingest with Parquet intermediate format
- **WHEN** user ingests very large datasets to DuckDB
- **THEN** the system SHALL support using Parquet as intermediate format
- **AND** the system SHALL leverage DuckDB's native Parquet support
- **AND** the system SHALL achieve optimal performance for large datasets

#### Scenario: Auto-create table from schema (DuckDB)
- **WHEN** user runs `undatum ingest file.jsonl duckdb:///... table --create-table`
- **THEN** the system SHALL analyze the data schema
- **AND** the system SHALL create the table with appropriate column types
- **AND** the system SHALL ingest data into the newly created table

#### Scenario: Schema validation before ingestion (DuckDB)
- **WHEN** user runs `undatum ingest file.jsonl duckdb:///... table`
- **THEN** the system SHALL validate that the table exists
- **AND** the system SHALL validate that data schema matches table schema
- **AND** the system SHALL report schema mismatches before starting ingestion

### Requirement: DuckDB Connection String Support
The system SHALL support DuckDB connection strings in standard formats.

#### Scenario: DuckDB file connection string
- **WHEN** user provides `duckdb:///path/to/database.db` as URI
- **THEN** the system SHALL parse the connection string correctly
- **AND** the system SHALL connect to the specified DuckDB file
- **AND** the system SHALL create the file if it doesn't exist

#### Scenario: DuckDB in-memory connection string
- **WHEN** user provides `duckdb:///:memory:` as URI
- **THEN** the system SHALL create an in-memory DuckDB database
- **AND** the system SHALL handle the temporary nature appropriately
