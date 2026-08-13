## ADDED Requirements

### Requirement: Count Command
The system SHALL provide a `count` command that counts the number of rows in a data file.

#### Scenario: Count rows in CSV file
- **WHEN** user runs `undatum count data.csv`
- **THEN** the system outputs the total number of data rows (excluding header if present)

#### Scenario: Count rows with DuckDB optimization
- **WHEN** user runs `undatum count data.csv` on a supported format
- **THEN** the system uses DuckDB engine for instant counting when available

#### Scenario: Count rows in JSONL file
- **WHEN** user runs `undatum count data.jsonl`
- **THEN** the system counts and outputs the number of JSON objects (one per line)

### Requirement: Table Command
The system SHALL provide a `table` command that displays data in a formatted, aligned table for inspection.

#### Scenario: Display table with default limit
- **WHEN** user runs `undatum table data.csv`
- **THEN** the system displays first 20 rows in a formatted table with aligned columns

#### Scenario: Display table with custom limit
- **WHEN** user runs `undatum table data.csv --limit 50`
- **THEN** the system displays first 50 rows in formatted table

#### Scenario: Display table with selected fields
- **WHEN** user runs `undatum table data.jsonl --fields name,email,status`
- **THEN** the system displays only the specified fields in the table

### Requirement: Reverse Command
The system SHALL provide a `reverse` command that reverses the order of rows in a data file.

#### Scenario: Reverse rows in CSV file
- **WHEN** user runs `undatum reverse data.csv output.csv`
- **THEN** the system writes all rows in reverse order to output file

#### Scenario: Reverse rows with streaming
- **WHEN** user runs `undatum reverse large_file.jsonl output.jsonl`
- **THEN** the system processes rows efficiently without loading entire file into memory

### Requirement: Enum Command
The system SHALL provide an `enum` command that adds sequential numbers, UUIDs, or constant values to records.

#### Scenario: Add row numbers
- **WHEN** user runs `undatum enum data.csv --field row_id output.csv`
- **THEN** the system adds a `row_id` field with sequential numbers starting from 1

#### Scenario: Add UUIDs
- **WHEN** user runs `undatum enum data.jsonl --field id --type uuid output.jsonl`
- **THEN** the system adds an `id` field with unique UUIDs for each row

#### Scenario: Add constant value
- **WHEN** user runs `undatum enum data.csv --field status --value "active" output.csv`
- **THEN** the system adds a `status` field with the constant value "active" for all rows

#### Scenario: Add row numbers with custom start
- **WHEN** user runs `undatum enum data.csv --field num --start 100 output.csv`
- **THEN** the system adds row numbers starting from 100

### Requirement: Head Command
The system SHALL provide a `head` command that extracts the first N rows from a data file.

#### Scenario: Extract first 10 rows
- **WHEN** user runs `undatum head data.csv --n 10`
- **THEN** the system outputs the first 10 rows to stdout

#### Scenario: Extract first rows to file
- **WHEN** user runs `undatum head data.jsonl --n 20 output.jsonl`
- **THEN** the system writes first 20 rows to output file

### Requirement: Tail Command
The system SHALL provide a `tail` command that extracts the last N rows from a data file.

#### Scenario: Extract last 10 rows
- **WHEN** user runs `undatum tail data.csv --n 10`
- **THEN** the system outputs the last 10 rows to stdout

#### Scenario: Extract last rows with buffering
- **WHEN** user runs `undatum tail large_file.jsonl --n 50 output.jsonl`
- **THEN** the system efficiently extracts last 50 rows without loading entire file

### Requirement: Fixlengths Command
The system SHALL provide a `fixlengths` command that ensures all rows have the same number of fields.

#### Scenario: Pad rows with empty string
- **WHEN** user runs `undatum fixlengths data.csv --strategy pad --value "" output.csv`
- **THEN** the system pads shorter rows with empty strings to match the maximum field count

#### Scenario: Truncate rows
- **WHEN** user runs `undatum fixlengths data.csv --strategy truncate output.csv`
- **THEN** the system truncates longer rows to match the minimum field count

#### Scenario: Pad rows with custom value
- **WHEN** user runs `undatum fixlengths data.jsonl --strategy pad --value "N/A" output.jsonl`
- **THEN** the system pads shorter rows with "N/A" value
