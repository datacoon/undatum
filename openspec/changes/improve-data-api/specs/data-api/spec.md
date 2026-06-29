## MODIFIED Requirements

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

### Requirement: Query Semantics
The system SHALL support pagination, sorting, and filtering for resource endpoints using
`limit`, `offset`, `order_by`, `order_dir`, optional `sort` alias, optional `include_total`,
and `field__op` query parameters constrained by the allowed operators and sortable fields
defined in the config. List responses SHALL use a `{data, pagination}` envelope.

#### Scenario: Filtered query
- **WHEN** a client requests `/sales?amount__gt=100&order_by=sold_at&order_dir=desc`
- **THEN** the response includes matching rows in `data` and pagination metadata

## ADDED Requirements

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
