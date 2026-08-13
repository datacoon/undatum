# data-api Specification

## Purpose
Read-only HTTP API over file-backed datasets (CSV, JSON/JSONL, Parquet), including optional
API-key authentication, CORS, S3 resource paths, and OpenAPI documentation.
## Requirements
### Requirement: API Config Format
The system SHALL support a YAML or JSON API config that defines file-backed resources, including
resource name, file path, format, optional primary key, field schema, pagination defaults, and
query options.

#### Scenario: Load valid config
- **WHEN** a user provides a config with one or more resources
- **THEN** the system accepts the config and makes those resources available to commands

### Requirement: Discover Command
The system SHALL provide `undatum api discover` to generate an API config from one or more input
files by inferring field names, types, and primary key candidates, with CLI overrides for output
path and resource options.

#### Scenario: Discover config from files
- **WHEN** a user runs `undatum api discover sales.csv customers.parquet --output api.yml`
- **THEN** a config file is written with inferred fields and resource definitions

### Requirement: Serve Command
The system SHALL provide `undatum api serve` to start a read-only HTTP server from an API config
and expose a resource endpoint per dataset backed by DuckDB queries.

#### Scenario: Serve a resource list endpoint
- **WHEN** a user runs `undatum api serve --config api.yml`
- **THEN** a GET request to `/sales` returns a paginated list of rows

### Requirement: Query Semantics
The system SHALL support pagination, sorting, and filtering for resource endpoints using
`limit`, `offset`, `order_by`, `order_dir`, optional `sort` alias, optional `include_total`,
and `field__op` query parameters constrained by the allowed operators and sortable fields
defined in the config. List responses SHALL use a `{data, pagination}` envelope.

#### Scenario: Filtered query
- **WHEN** a client requests `/sales?amount__gt=100&order_by=sold_at&order_dir=desc`
- **THEN** the response includes matching rows in `data` and pagination metadata

### Requirement: OpenAPI Documentation
The system SHALL expose OpenAPI/Swagger documentation for the running API with per-resource
query parameters, response schemas derived from config field definitions, and filter parameters
documented via `field__op` conventions.

#### Scenario: View OpenAPI docs
- **WHEN** the server is running
- **THEN** the OpenAPI UI is available at `/docs` with documented pagination, sorting, and filter parameters

#### Scenario: Export OpenAPI schema
- **WHEN** a user runs `undatum api openapi --config api.yml --output openapi.json`
- **THEN** a valid OpenAPI 3.x schema is written without starting the server

### Requirement: Run Convenience Command
The system SHALL provide `undatum api run` to discover resources from files in-memory and serve
them immediately without requiring a persisted config file.

#### Scenario: Run without config file
- **WHEN** a user runs `undatum api run sales.csv`
- **THEN** the server starts and exposes the `sales` resource endpoint

### Requirement: API Key Authentication
The system SHALL support optional API-key authentication for `api serve` and `api run` when
configured via `--api-key` or the `UNDATUM_API_KEY` environment variable.

#### Scenario: Reject unauthenticated request
- **WHEN** the server is started with an API key configured
- **AND** a client omits the key
- **THEN** the server responds with HTTP 401

### Requirement: S3 Resource Paths
The system SHALL support `s3://` URIs as resource paths in API config when cloud dependencies
are available.

#### Scenario: Serve S3-backed resource
- **WHEN** a config resource points to `s3://bucket/path/data.parquet`
- **THEN** the resource endpoint serves rows from that object

### Requirement: CORS Configuration
The system SHALL support optional CORS origins for browser clients via `--cors-origins`.

#### Scenario: Browser preflight
- **WHEN** CORS origins are configured
- **THEN** cross-origin requests from allowed origins succeed

