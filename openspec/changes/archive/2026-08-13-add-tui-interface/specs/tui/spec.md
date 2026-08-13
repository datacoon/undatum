## ADDED Requirements

### Requirement: Optional TUI entry point
The system SHALL provide an interactive terminal UI via `undatum tui` that is
optional to install and does not change default CLI behavior.

#### Scenario: Missing TUI extra
- **WHEN** the user runs `undatum tui` without the `textual` extra installed
- **THEN** the system SHALL raise a dependency error naming `textual`
- **AND** SHALL tell the user to install with `pip install "undatum[tui]"`
- **AND** SHALL exit with code 2

#### Scenario: Non-interactive terminal
- **WHEN** the user runs `undatum tui` without a TTY
- **THEN** the system SHALL refuse to start the UI
- **AND** SHALL tell the user to use `table`, `profile`, or `sql` instead
- **AND** SHALL exit with code 1

#### Scenario: Launch on a file
- **WHEN** the user runs `undatum tui data.csv` in a TTY with the extra installed
- **THEN** the system SHALL open an interactive session on that path
- **AND** SHALL NOT load the entire file into memory as a single table

### Requirement: Sampled preview
The TUI SHALL display a bounded sample of records in a scrollable grid together
with detected field names.

#### Scenario: Default sample
- **WHEN** a session opens a CSV with more rows than the sample limit
- **THEN** the grid SHALL show at most the configured sample limit (default 200)
- **AND** the status area SHALL indicate that the view is a sample

#### Scenario: Custom sample limit
- **WHEN** the user passes `--limit N` to `undatum tui`
- **THEN** the grid SHALL sample at most N rows
- **AND** N SHALL be capped at a documented maximum so the UI cannot request an
  unbounded in-memory table

### Requirement: Exploration actions reuse CLI processors
TUI exploration actions SHALL call the same command processors as the CLI and
SHALL surface an equivalent `undatum` invocation.

#### Scenario: Profile from the TUI
- **WHEN** the user triggers profile in the TUI
- **THEN** the system SHALL compute statistics via the existing stats/profile
  processor
- **AND** SHALL show the equivalent `undatum profile` command in the session log

#### Scenario: Frequency on selected field
- **WHEN** the user triggers frequency on a selected grid column
- **THEN** the system SHALL count values for that field on the current sample
  (after any session filter)
- **AND** SHALL show the equivalent `undatum frequency` command in the session log

#### Scenario: Filter the loaded sample
- **WHEN** the user enters a filter expression in the TUI
- **THEN** the grid SHALL show only sample rows that match, without re-reading
  the source file
- **AND** SHALL show the equivalent `undatum select --filter` command
- **AND** an empty expression SHALL clear the filter

#### Scenario: Export current view
- **WHEN** the user exports the current sample or extract from the TUI
- **THEN** the system SHALL write through existing convert/write paths
- **AND** SHALL NOT introduce a TUI-only file format

#### Scenario: SQL from the TUI
- **WHEN** the user runs SQL in the TUI without a LIMIT clause
- **THEN** the system SHALL execute via the existing SQL processor
- **AND** SHALL apply a default LIMIT so the result grid stays bounded
- **AND** SHALL show the equivalent `undatum sql` command in the session log

#### Scenario: Command palette
- **WHEN** the user opens the command palette
- **THEN** the system SHALL list exploration actions with an equivalent CLI
  template
- **AND** selecting an action SHALL update the session command log

#### Scenario: Convert / save as
- **WHEN** the user converts or saves-as from the TUI
- **THEN** the system SHALL write the source file through the existing convert
  processor with low-memory mode
- **AND** SHALL show the equivalent `undatum convert --low-memory` command

#### Scenario: Validate sample
- **WHEN** the user validates from the TUI
- **THEN** the system SHALL evaluate rules against the loaded sample only
- **AND** SHALL note that full-file validate is a CLI command

#### Scenario: Mask preview
- **WHEN** the user masks selected fields in the TUI
- **THEN** the system SHALL preview masking on the sample via existing mask
  helpers
- **AND** writing a masked file SHALL call the existing mask processor

#### Scenario: Pipeline YAML export
- **WHEN** the user exports a pipeline snippet from the TUI
- **THEN** the system SHALL write a YAML step list using the pipeline spec
  format
- **AND** SHALL include the current source path and session filter when set

### Requirement: Recent files are paths only
The TUI MAY persist recently opened dataset locations for the file picker.
History SHALL store paths (or URIs) only, never sample row contents.

#### Scenario: Record opened path
- **WHEN** the user opens a dataset in the TUI
- **THEN** the system MAY append that path to `~/.undatum/tui-history.json`
- **AND** SHALL NOT write record values from the sample into that file

#### Scenario: Open an S3 URI
- **WHEN** the user enters an `s3://` URI in the file picker
- **THEN** the system SHALL open it through the existing path/S3 helpers
- **AND** SHALL NOT require a local DirectoryTree listing of the bucket

### Requirement: TUI is not a spreadsheet
The TUI SHALL NOT edit individual cells in the source file.

#### Scenario: No in-place cell edit
- **WHEN** the user focuses a cell in the preview grid
- **THEN** the system SHALL allow inspection and selection
- **AND** SHALL NOT write that cell back to the source path as an editor would
