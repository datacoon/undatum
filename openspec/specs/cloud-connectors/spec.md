# cloud-connectors Specification

## Purpose
Object-storage connectors for AWS S3, Google Cloud Storage, and Azure Blob/ADLS
so commands, the SDK, TUI/web, and the Data API can read and write cloud URIs
with standard provider credentials.
## Requirements
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

### Requirement: GCS URI Support
Commands SHALL support Google Cloud Storage URIs in the format `gs://bucket/path`
and `gcs://bucket/path` for input and output paths when GCS dependencies are
installed.

#### Scenario: Read from GCS
- **WHEN** user runs `undatum convert gs://my-bucket/data.csv --output local.jsonl`
- **THEN** data is read from the GCS bucket and converted to a local file

#### Scenario: Write to GCS
- **WHEN** user runs `undatum convert local.csv --output gs://my-bucket/output.parquet`
- **THEN** converted data is written to the GCS bucket

#### Scenario: GCS alias scheme
- **WHEN** user runs `undatum stats gcs://my-bucket/data.csv`
- **THEN** statistics are computed on data from GCS using the same connector as `gs://`

### Requirement: GCS Credential Handling
The system SHALL respect standard Google Cloud credential sources and SHALL NOT
require undatum-specific secret files.

#### Scenario: Application Default Credentials
- **WHEN** Application Default Credentials or `GOOGLE_APPLICATION_CREDENTIALS` are configured
- **THEN** GCS operations use those credentials

### Requirement: Azure URI Support
Commands SHALL support Azure Blob Storage and Azure Data Lake URIs in the
formats `az://container/path`, `abfs://container/path`, and
`abfss://container/path` for input and output paths when Azure dependencies
are installed.

#### Scenario: Read from Azure
- **WHEN** user runs `undatum convert az://my-container/data.csv --output local.jsonl`
- **THEN** data is read from Azure and converted to a local file

#### Scenario: Write to Azure
- **WHEN** user runs `undatum convert local.csv --output az://my-container/output.parquet`
- **THEN** converted data is written to Azure

#### Scenario: ADLS schemes
- **WHEN** user runs `undatum stats abfs://my-container/data.csv`
- **THEN** statistics are computed using the same Azure connector as `az://`

### Requirement: Azure Credential Handling
The system SHALL respect standard Azure credential sources and SHALL NOT require
undatum-specific secret files.

#### Scenario: Account key from environment
- **WHEN** `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` are set
- **THEN** Azure operations use these credentials

#### Scenario: Azure identity chain
- **WHEN** account key variables are unset and Azure identity is available
- **THEN** Azure operations use the adlfs / Azure identity chain

### Requirement: Missing Cloud Extra Guidance
When a GCS or Azure URI is used without the required optional dependency, the
system SHALL raise a dependency error that names the package and the install extra.

#### Scenario: Missing gcsfs
- **WHEN** user runs a command with a `gs://` path and `gcsfs` is not installed
- **THEN** the system raises a dependency error
- **AND** the message tells the user to install with `pip install "undatum[gcs]"`
  or `pip install "undatum[cloud]"`
- **AND** the process exits with code 2

#### Scenario: Missing adlfs
- **WHEN** user runs a command with an `az://` path and `adlfs` is not installed
- **THEN** the system raises a dependency error
- **AND** the message tells the user to install with `pip install "undatum[azure]"`
  or `pip install "undatum[cloud]"`
- **AND** the process exits with code 2

### Requirement: Cloud-to-Cloud Conversion
The system SHALL support converting between local files and any supported cloud
URI, including different cloud providers.

#### Scenario: S3 to GCS
- **WHEN** user runs `undatum convert s3://bucket/input.jsonl --output gs://bucket/output.parquet`
- **THEN** data is read from S3 and written to GCS

#### Scenario: GCS to Azure
- **WHEN** user runs `undatum convert gs://bucket/data.csv --output az://container/data.csv`
- **THEN** data is read from GCS and written to Azure

