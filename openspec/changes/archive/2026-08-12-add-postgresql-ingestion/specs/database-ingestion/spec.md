## ADDED Requirements

### Requirement: PostgreSQL Database Ingestion
The system SHALL support ingesting data from files into PostgreSQL databases using optimized bulk loading methods.

#### Scenario: Ingest to PostgreSQL with COPY FROM
- **WHEN** user runs `undatum ingest file.csv postgresql://user:pass@host:5432/db table --dbtype postgresql`
- **THEN** the system SHALL use PostgreSQL COPY FROM command for bulk loading
- **AND** the system SHALL achieve high throughput (100,000+ rows/second)
- **AND** the system SHALL display progress during ingestion

#### Scenario: Ingest with upsert mode
- **WHEN** user runs `undatum ingest file.jsonl postgresql://... table --mode upsert --upsert-key id`
- **THEN** the system SHALL use INSERT ... ON CONFLICT for upsert operations
- **AND** the system SHALL update existing records based on the conflict key
- **AND** the system SHALL insert new records that don't conflict

#### Scenario: Auto-create table from schema
- **WHEN** user runs `undatum ingest file.jsonl postgresql://... table --create-table`
- **THEN** the system SHALL analyze the data schema
- **AND** the system SHALL create the table with appropriate column types
- **AND** the system SHALL ingest data into the newly created table

#### Scenario: Ingest with replace mode
- **WHEN** user runs `undatum ingest file.csv postgresql://... table --mode replace`
- **THEN** the system SHALL truncate the existing table
- **AND** the system SHALL ingest all records from the file
- **AND** the system SHALL maintain table structure

#### Scenario: Schema validation before ingestion
- **WHEN** user runs `undatum ingest file.jsonl postgresql://... table`
- **THEN** the system SHALL validate that the table exists
- **AND** the system SHALL validate that data schema matches table schema
- **AND** the system SHALL report schema mismatches before starting ingestion

#### Scenario: Connection pooling for PostgreSQL
- **WHEN** ingesting data to PostgreSQL
- **THEN** the system SHALL use connection pooling to reuse connections
- **AND** the system SHALL manage connection lifecycle efficiently
- **AND** the system SHALL handle connection failures gracefully

### Requirement: PostgreSQL Connection String Support
The system SHALL support PostgreSQL connection strings in standard formats.

#### Scenario: PostgreSQL URI connection string
- **WHEN** user provides `postgresql://user:pass@host:5432/database` as URI
- **THEN** the system SHALL parse the connection string correctly
- **AND** the system SHALL connect to the specified PostgreSQL instance
- **AND** the system SHALL use the specified database and credentials

#### Scenario: PostgreSQL connection with SQLAlchemy format
- **WHEN** user provides `postgresql+psycopg2://user:pass@host:5432/database` as URI
- **THEN** the system SHALL parse the connection string correctly
- **AND** the system SHALL use SQLAlchemy for connection management if available
