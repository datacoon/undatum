## RENAMED Requirements
- FROM: `### Requirement: S3 Resource Paths`
- TO: `### Requirement: Cloud Resource Paths`

## MODIFIED Requirements
### Requirement: Cloud Resource Paths
The system SHALL support `s3://`, `gs://`/`gcs://`, and `az://`/`abfs://`/`abfss://`
URIs as resource paths in API config when the matching cloud dependencies are
available. Cloud objects SHALL be materialized to a temporary local file for
DuckDB-backed endpoints.

#### Scenario: Serve S3-backed resource
- **WHEN** a config resource points to `s3://bucket/path/data.parquet`
- **THEN** the resource endpoint serves rows from that object

#### Scenario: Serve GCS-backed resource
- **WHEN** a config resource points to `gs://bucket/path/data.parquet`
- **THEN** the resource endpoint serves rows from that object

#### Scenario: Serve Azure-backed resource
- **WHEN** a config resource points to `az://container/path/data.csv`
- **THEN** the resource endpoint serves rows from that object

#### Scenario: Reject unsupported remote URIs
- **WHEN** a config resource path is an HTTP URL or another non-cloud URI
- **THEN** the system rejects the config with an error naming supported schemes
