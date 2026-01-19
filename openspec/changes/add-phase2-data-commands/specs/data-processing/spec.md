## ADDED Requirements

### Requirement: Sort Command
The system SHALL provide a `sort` command that sorts rows by one or more columns.

#### Scenario: Sort by single column ascending
- **WHEN** user runs `undatum sort data.csv --by name output.csv`
- **THEN** the system sorts rows by the `name` column in ascending order

#### Scenario: Sort by multiple columns
- **WHEN** user runs `undatum sort data.jsonl --by name,age output.jsonl`
- **THEN** the system sorts rows first by `name`, then by `age`

#### Scenario: Sort descending
- **WHEN** user runs `undatum sort data.csv --by date --desc output.csv`
- **THEN** the system sorts rows by `date` in descending order

#### Scenario: Sort large file with external merge
- **WHEN** user runs `undatum sort large_file.csv --by id output.csv`
- **THEN** the system uses external merge sort to handle the file efficiently

#### Scenario: Numeric sort
- **WHEN** user runs `undatum sort data.csv --by price --numeric output.csv`
- **THEN** the system sorts `price` as numbers rather than strings

### Requirement: Sample Command
The system SHALL provide a `sample` command that randomly selects rows from a data file.

#### Scenario: Sample fixed number of rows
- **WHEN** user runs `undatum sample data.csv --n 1000 output.csv`
- **THEN** the system randomly selects 1000 rows using reservoir sampling

#### Scenario: Sample by percentage
- **WHEN** user runs `undatum sample data.jsonl --percent 10 output.jsonl`
- **THEN** the system randomly selects 10% of rows

#### Scenario: Sample without loading entire file
- **WHEN** user runs `undatum sample large_file.csv --n 100 output.csv`
- **THEN** the system uses reservoir sampling algorithm that doesn't require loading all data

### Requirement: Search Command
The system SHALL provide a `search` command that filters rows using regex patterns.

#### Scenario: Search across all fields
- **WHEN** user runs `undatum search data.csv --pattern "error|warning"`
- **THEN** the system outputs rows where any field matches the pattern

#### Scenario: Search in specific fields
- **WHEN** user runs `undatum search data.jsonl --pattern "^[0-9]+$" --fields id,code`
- **THEN** the system outputs rows where `id` or `code` fields match the pattern

#### Scenario: Case-insensitive search
- **WHEN** user runs `undatum search data.csv --pattern "ERROR" --ignore-case`
- **THEN** the system matches regardless of case

### Requirement: Dedup Command
The system SHALL provide a `dedup` command that removes duplicate rows.

#### Scenario: Deduplicate by all fields
- **WHEN** user runs `undatum dedup data.csv output.csv`
- **THEN** the system removes rows that are identical in all fields

#### Scenario: Deduplicate by key fields
- **WHEN** user runs `undatum dedup data.jsonl --key-fields email output.jsonl`
- **THEN** the system removes rows with duplicate `email` values, keeping the first occurrence

#### Scenario: Keep last duplicate
- **WHEN** user runs `undatum dedup data.csv --key-fields id --keep last output.csv`
- **THEN** the system removes duplicates keeping the last occurrence

#### Scenario: Deduplicate large file externally
- **WHEN** user runs `undatum dedup large_file.jsonl output.jsonl`
- **THEN** the system uses external approach for memory efficiency

### Requirement: Fill Command
The system SHALL provide a `fill` command that fills empty or null values.

#### Scenario: Fill with constant value
- **WHEN** user runs `undatum fill data.csv --fields name,email --value "N/A" output.csv`
- **THEN** the system fills empty values in `name` and `email` fields with "N/A"

#### Scenario: Forward fill
- **WHEN** user runs `undatum fill data.jsonl --fields status --strategy forward output.jsonl`
- **THEN** the system fills empty values with the previous non-empty value

#### Scenario: Backward fill
- **WHEN** user runs `undatum fill data.csv --fields category --strategy backward output.csv`
- **THEN** the system fills empty values with the next non-empty value

### Requirement: Rename Command
The system SHALL provide a `rename` command that renames fields.

#### Scenario: Rename by exact mapping
- **WHEN** user runs `undatum rename data.csv --map "old_name:new_name,old2:new2" output.csv`
- **THEN** the system renames `old_name` to `new_name` and `old2` to `new2`

#### Scenario: Rename using regex
- **WHEN** user runs `undatum rename data.jsonl --pattern "^prefix_" --replacement "" output.jsonl`
- **THEN** the system removes "prefix_" from the beginning of all field names

### Requirement: Explode Command
The system SHALL provide an `explode` command that splits a column by separator into multiple rows.

#### Scenario: Explode comma-separated values
- **WHEN** user runs `undatum explode data.csv --field tags --separator "," output.csv`
- **THEN** the system creates one row per tag value, duplicating other fields

#### Scenario: Explode pipe-separated values
- **WHEN** user runs `undatum explode data.jsonl --field categories --separator "|" output.jsonl`
- **THEN** the system splits `categories` by "|" and creates multiple rows

### Requirement: Replace Command
The system SHALL provide a `replace` command that performs string replacement in fields.

#### Scenario: Simple string replacement
- **WHEN** user runs `undatum replace data.csv --field name --pattern "Mr\." --replacement "Mr" output.csv`
- **THEN** the system replaces "Mr." with "Mr" in the `name` field

#### Scenario: Regex replacement
- **WHEN** user runs `undatum replace data.jsonl --field email --pattern "@old.com" --replacement "@new.com" --regex output.jsonl`
- **THEN** the system replaces email domain using regex pattern

#### Scenario: Global replacement
- **WHEN** user runs `undatum replace data.csv --field text --pattern "old" --replacement "new" --global output.csv`
- **THEN** the system replaces all occurrences of "old" with "new" in the `text` field

### Requirement: Cat Command
The system SHALL provide a `cat` command that concatenates files.

#### Scenario: Concatenate files by rows
- **WHEN** user runs `undatum cat file1.csv file2.csv --mode rows output.csv`
- **THEN** the system appends rows from file2 to file1, handling headers appropriately

#### Scenario: Concatenate files by columns
- **WHEN** user runs `undatum cat file1.csv file2.csv --mode columns output.csv`
- **THEN** the system combines files side-by-side, matching rows by position
