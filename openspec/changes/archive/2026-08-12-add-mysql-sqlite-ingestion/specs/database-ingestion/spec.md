## ADDED Requirements

### Requirement: MySQL Database Ingestion
The system SHALL support ingesting data from files into MySQL databases using optimized bulk loading methods.

#### Scenario: Ingest to MySQL with LOAD DATA INFILE
- **WHEN** user runs `undatum ingest file.csv mysql://user:pass@host:3306/db table --dbtype mysql`
- **THEN** the system SHALL use MySQL LOAD DATA LOCAL INFILE for bulk loading
- **AND** the system SHALL achieve high throughput (50,000+ rows/second)
- **AND** the system SHALL display progress during ingestion

#### Scenario: Ingest to MySQL with multi-row INSERT
- **WHEN** LOAD DATA INFILE is not available or user prefers INSERT
- **THEN** the system SHALL use multi-row INSERT statements
- **AND** the system SHALL batch multiple rows per INSERT statement
- **AND** the system SHALL achieve good performance (10,000+ rows/second)

#### Scenario: Ingest with upsert mode (MySQL)
- **WHEN** user runs `undatum ingest file.jsonl mysql://... table --mode upsert --upsert-key id`
- **THEN** the system SHALL use INSERT ... ON DUPLICATE KEY UPDATE
- **AND** the system SHALL update existing records based on the conflict key
- **AND** the system SHALL insert new records that don't conflict

#### Scenario: Auto-create table from schema (MySQL)
- **WHEN** user runs `undatum ingest file.jsonl mysql://... table --create-table`
- **THEN** the system SHALL analyze the data schema
- **AND** the system SHALL create the table with appropriate column types
- **AND** the system SHALL ingest data into the newly created table

### Requirement: SQLite Database Ingestion
The system SHALL support ingesting data from files into SQLite databases using optimized bulk loading methods.

#### Scenario: Ingest to SQLite file database
- **WHEN** user runs `undatum ingest file.csv sqlite:///path/to/db.db table --dbtype sqlite`
- **THEN** the system SHALL use executemany with PRAGMA optimizations
- **AND** the system SHALL apply PRAGMA settings (synchronous=OFF, journal_mode=WAL)
- **AND** the system SHALL achieve good performance (10,000+ rows/second)
- **AND** the system SHALL display progress during ingestion

#### Scenario: Ingest to SQLite in-memory database
- **WHEN** user runs `undatum ingest file.jsonl sqlite:///:memory: table --dbtype sqlite`
- **THEN** the system SHALL create an in-memory SQLite database
- **AND** the system SHALL ingest data into the in-memory database
- **AND** the system SHALL handle the temporary nature of in-memory databases

#### Scenario: Ingest with upsert mode (SQLite)
- **WHEN** user runs `undatum ingest file.jsonl sqlite:///... table --mode upsert --upsert-key id`
- **THEN** the system SHALL use INSERT ... ON CONFLICT for upsert (SQLite 3.24+)
- **AND** the system SHALL update existing records based on the conflict key
- **AND** the system SHALL insert new records that don't conflict

#### Scenario: Auto-create table from schema (SQLite)
- **WHEN** user runs `undatum ingest file.jsonl sqlite:///... table --create-table`
- **THEN** the system SHALL analyze the data schema
- **AND** the system SHALL create the table with appropriate column types
- **AND** the system SHALL ingest data into the newly created table

#### Scenario: SQLite PRAGMA optimizations
- **WHEN** ingesting data to SQLite
- **THEN** the system SHALL apply PRAGMA synchronous=OFF during bulk load
- **AND** the system SHALL apply PRAGMA journal_mode=WAL
- **AND** the system SHALL restore PRAGMA settings after ingestion
- **AND** the system SHALL handle foreign key constraints appropriately

### Requirement: MySQL and SQLite Connection String Support
The system SHALL support MySQL and SQLite connection strings in standard formats.

#### Scenario: MySQL URI connection string
- **WHEN** user provides `mysql://user:pass@host:3306/database` as URI
- **THEN** the system SHALL parse the connection string correctly
- **AND** the system SHALL connect to the specified MySQL instance
- **AND** the system SHALL use the specified database and credentials

#### Scenario: SQLite file connection string
- **WHEN** user provides `sqlite:///path/to/database.db` as URI
- **THEN** the system SHALL parse the connection string correctly
- **AND** the system SHALL connect to the specified SQLite file
- **AND** the system SHALL create the file if it doesn't exist

#### Scenario: SQLite in-memory connection string
- **WHEN** user provides `sqlite:///:memory:` as URI
- **THEN** the system SHALL create an in-memory SQLite database
- **AND** the system SHALL handle the temporary nature appropriately
