# web-ui Specification

## Purpose
Optional local browser session (`undatum web`) over the same sampled processors as
the TUI. Default bind is localhost. Not a spreadsheet and not the read-only Data API.
## Requirements
### Requirement: Optional web UI entry point
The system SHALL provide a local browser UI via `undatum web` that is optional to
install and does not change default CLI or Data API behavior.

#### Scenario: Missing web extra
- **WHEN** the user runs `undatum web` without the web extra installed
- **THEN** the system SHALL raise a dependency error naming `fastapi`
- **AND** SHALL tell the user to install with `pip install "undatum[web]"`
- **AND** SHALL exit with code 2

#### Scenario: Default bind is localhost
- **WHEN** the user runs `undatum web` without `--host`
- **THEN** the server SHALL bind to `127.0.0.1`
- **AND** SHALL NOT listen on all interfaces unless `--host` is set explicitly

#### Scenario: Launch on a file
- **WHEN** the user runs `undatum web data.csv` with the extra installed
- **THEN** the system SHALL start a local HTTP session on that path
- **AND** SHALL NOT send the entire file to the browser as a single table

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

### Requirement: Exploration actions reuse CLI processors
Web UI exploration actions SHALL call the same command processors as the CLI and
TUI and SHALL surface an equivalent `undatum` invocation.

#### Scenario: Profile from the web UI
- **WHEN** the user triggers profile in the web UI
- **THEN** the system SHALL compute statistics via the existing stats/profile
  processor
- **AND** SHALL show the equivalent `undatum profile` command on the page

#### Scenario: Filter the loaded sample
- **WHEN** the user enters a filter expression in the web UI
- **THEN** the table SHALL show only sample rows that match, without sending the
  source file to the browser
- **AND** SHALL show the equivalent `undatum select --filter` command

#### Scenario: SQL from the web UI
- **WHEN** the user runs SQL in the web UI without a LIMIT clause
- **THEN** the system SHALL execute via the existing SQL processor
- **AND** SHALL apply a default LIMIT so the result table stays bounded
- **AND** SHALL show the equivalent `undatum sql` command

#### Scenario: Export or convert
- **WHEN** the user exports the current view or converts/saves-as from the web UI
- **THEN** the system SHALL write through existing convert/write paths
- **AND** SHALL NOT introduce a web-only file format

### Requirement: Web UI is not the Data API
The web UI SHALL be a local human session and SHALL NOT replace or mutate the
read-only Data API resource contract.

#### Scenario: Data API remains read-only
- **WHEN** the web UI is implemented
- **THEN** `undatum api serve` SHALL continue to expose read-only resource
  endpoints from an API config
- **AND** convert, mask, and validate SHALL NOT be added as verbs on those
  resource paths as part of this change

### Requirement: Web UI is not a spreadsheet
The web UI SHALL NOT edit individual cells in the source file.

#### Scenario: No in-place cell edit
- **WHEN** the user views a cell in the sample table
- **THEN** the system SHALL allow inspection
- **AND** SHALL NOT write that cell back to the source path as an editor would

