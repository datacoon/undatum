## MODIFIED Requirements
### Requirement: Sampled preview
The web UI SHALL display a bounded sample of records in an HTML table together
with detected field names.

#### Scenario: Default sample
- **WHEN** a session opens a CSV with more rows than the sample limit
- **THEN** the page SHALL show at most the configured sample limit (default 200)
- **AND** the status area SHALL indicate that the view is a sample

#### Scenario: Custom sample limit
- **WHEN** the user passes `--limit N` to `undatum web`
- **THEN** the grid SHALL sample at most N rows
- **AND** N SHALL be capped at a documented maximum so the UI cannot request an
  unbounded in-browser table

#### Scenario: Open by path or upload
- **WHEN** the user submits a local path, an `s3://` / `gs://` / `az://` URI, or a file upload
- **THEN** the system SHALL open that source through existing path helpers
- **AND** uploads SHALL be written to a temporary working directory, not held
  entirely as an in-memory table
