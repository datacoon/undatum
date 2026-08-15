## ADDED Requirements

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
