## ADDED Requirements

### Requirement: S3 URI Support
Commands SHALL support S3 URIs in the format `s3://bucket/path` for input and output paths.

#### Scenario: Read from S3
- **WHEN** user runs `undatum convert s3://my-bucket/data.csv --output local.jsonl`
- **THEN** data is read from S3 bucket and converted to local file

#### Scenario: Write to S3
- **WHEN** user runs `undatum convert local.csv --output s3://my-bucket/output.parquet`
- **THEN** converted data is written to S3 bucket

#### Scenario: S3 to S3 conversion
- **WHEN** user runs `undatum convert s3://bucket/input.jsonl --output s3://bucket/output.parquet`
- **THEN** data is read from S3, converted, and written back to S3

### Requirement: AWS Credential Handling
The system SHALL respect standard AWS environment variables and configuration for credentials.

#### Scenario: Credentials from environment variables
- **WHEN** `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set
- **THEN** S3 operations use these credentials

#### Scenario: Credentials from AWS profile
- **WHEN** `AWS_PROFILE` environment variable is set to a named profile
- **THEN** S3 operations use credentials from the specified profile

#### Scenario: Region specification
- **WHEN** `AWS_REGION` environment variable is set
- **THEN** S3 operations use the specified region

### Requirement: S3 Support in Major Commands
Major commands SHALL support S3 URIs for input and output paths.

#### Scenario: Stats on S3 file
- **WHEN** user runs `undatum stats s3://bucket/data.csv`
- **THEN** statistics are computed on data from S3

#### Scenario: Ingest from S3
- **WHEN** user runs `undatum ingest s3://bucket/data.jsonl --dbtype postgresql`
- **THEN** data is read from S3 and ingested into database
