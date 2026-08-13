## ADDED Requirements

### Requirement: Database Ingestion Error Handling
The system SHALL provide robust error handling for database ingestion operations, including retry logic for transient failures and detailed error reporting.

#### Scenario: Retry on transient failure
- **WHEN** a transient database connection error occurs during ingestion
- **THEN** the system SHALL automatically retry the operation with exponential backoff
- **AND** the system SHALL log retry attempts
- **AND** the system SHALL continue processing after successful retry

#### Scenario: Partial batch failure handling
- **WHEN** a batch contains some records that fail to insert
- **THEN** the system SHALL log failed records with context
- **AND** the system SHALL continue processing remaining records
- **AND** the system SHALL provide a summary of failed records at completion

#### Scenario: Error summary reporting
- **WHEN** ingestion completes (successfully or with errors)
- **THEN** the system SHALL display a summary including total rows, successful rows, failed rows, and error types
- **AND** the system SHALL provide actionable error messages

### Requirement: Database Connection Management
The system SHALL manage database connections efficiently using connection pooling and proper lifecycle management.

#### Scenario: Connection pooling
- **WHEN** ingesting data to MongoDB or Elasticsearch
- **THEN** the system SHALL use connection pooling to reuse connections
- **AND** the system SHALL validate connections before use
- **AND** the system SHALL handle connection failures gracefully

#### Scenario: Connection timeout
- **WHEN** a connection timeout is specified via CLI option
- **THEN** the system SHALL apply the timeout to database client connections
- **AND** the system SHALL respect the timeout for all database operations

### Requirement: Database Ingestion Progress Reporting
The system SHALL provide detailed progress information during ingestion operations.

#### Scenario: Enhanced progress display
- **WHEN** ingesting data to a database
- **THEN** the system SHALL display progress with ETA and throughput (rows/second)
- **AND** the system SHALL show current batch number and total batches
- **AND** the system SHALL update progress in real-time

#### Scenario: Completion summary
- **WHEN** ingestion completes
- **THEN** the system SHALL display total rows processed, time elapsed, and average throughput
- **AND** the system SHALL display success and failure statistics

## MODIFIED Requirements

### Requirement: Database Ingestion Command
The system SHALL provide a command to ingest data from files into databases, supporting MongoDB and Elasticsearch with batch processing, progress tracking, and error handling.

#### Scenario: Ingest to MongoDB with drop option
- **WHEN** user runs `undatum ingest file.jsonl mongodb://host/db collection --drop`
- **THEN** the system SHALL drop the existing collection before ingestion
- **AND** the system SHALL ingest all records from the file
- **AND** the system SHALL display progress with accurate batch information

#### Scenario: Ingest with custom batch size
- **WHEN** user runs `undatum ingest file.jsonl uri db table --batch 10000`
- **THEN** the system SHALL use the specified batch size consistently
- **AND** the system SHALL process data in batches of the specified size
- **AND** the system SHALL display accurate progress based on batch size

#### Scenario: Ingest with timeout
- **WHEN** user runs `undatum ingest file.jsonl uri db table --timeout 60`
- **THEN** the system SHALL apply the timeout to database connections
- **AND** the system SHALL respect the timeout for all operations
- **AND** the system SHALL report timeout errors appropriately
