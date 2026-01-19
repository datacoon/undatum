## MODIFIED Requirements

### Requirement: Statistics Command
The system SHALL provide a `stats` command that generates detailed statistical analysis of data files with user-visible progress indication.

#### Scenario: Generate statistics with progress indication
- **WHEN** user runs `undatum stats data.csv`
- **THEN** the system displays a progress bar showing current record count, estimated time remaining, and processing rate (rows/second)

#### Scenario: Generate statistics with progress bar disabled
- **WHEN** user runs `undatum stats data.csv --no-progress`
- **THEN** the system processes the file without displaying a progress bar (suitable for non-interactive use)

#### Scenario: Generate statistics with progress bar explicitly enabled
- **WHEN** user runs `undatum stats data.csv --progress`
- **THEN** the system displays a progress bar (default behavior)

#### Scenario: Progress bar shows descriptive label
- **WHEN** user runs `undatum stats data.csv`
- **THEN** the progress bar displays "Analyzing statistics" as the description

#### Scenario: Progress bar shows row count
- **WHEN** user runs `undatum stats data.csv`
- **THEN** the progress bar displays the number of records processed with "rows" as the unit

#### Scenario: Progress bar shows throughput
- **WHEN** user runs `undatum stats data.csv` and processing is in progress
- **THEN** the progress bar displays processing rate (e.g., "1234 rows/s") as supplementary information

#### Scenario: Statistics output unchanged
- **WHEN** user runs `undatum stats data.csv` with or without progress indication
- **THEN** the statistics output (Rich table) remains identical and accurate

#### Scenario: Progress indication works with all formats
- **WHEN** user runs `undatum stats` on CSV, JSONL, or BSON files
- **THEN** the progress bar works correctly for all supported formats
