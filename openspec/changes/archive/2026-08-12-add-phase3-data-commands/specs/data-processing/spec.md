## ADDED Requirements

### Requirement: Join Command
The system SHALL provide a `join` command that performs relational joins between two data files.

#### Scenario: Inner join by key field
- **WHEN** user runs `undatum join data1.csv data2.csv --on email --type inner output.csv`
- **THEN** the system performs an inner join matching rows where `email` values are equal

#### Scenario: Left join
- **WHEN** user runs `undatum join data1.jsonl data2.jsonl --on id --type left output.jsonl`
- **THEN** the system performs a left join, keeping all rows from the first file

#### Scenario: Right join
- **WHEN** user runs `undatum join data1.csv data2.csv --on id --type right output.csv`
- **THEN** the system performs a right join, keeping all rows from the second file

#### Scenario: Full outer join
- **WHEN** user runs `undatum join data1.jsonl data2.jsonl --on id --type full output.jsonl`
- **THEN** the system performs a full outer join, keeping all rows from both files

#### Scenario: Join with hash-based algorithm
- **WHEN** user runs `undatum join large1.csv large2.csv --on key output.csv`
- **THEN** the system uses hash-based join algorithm for memory efficiency

#### Scenario: Join with DuckDB optimization
- **WHEN** user runs `undatum join data1.parquet data2.parquet --on id output.parquet`
- **THEN** the system uses DuckDB SQL join for optimal performance

### Requirement: Diff Command
The system SHALL provide a `diff` command that compares two data files and shows differences.

#### Scenario: Compare files by key
- **WHEN** user runs `undatum diff file1.csv file2.csv --key id`
- **THEN** the system outputs added, removed, and changed rows

#### Scenario: Output differences to file
- **WHEN** user runs `undatum diff file1.jsonl file2.jsonl --key email --output changes.jsonl`
- **THEN** the system writes differences to the output file

#### Scenario: Show unified diff format
- **WHEN** user runs `undatum diff file1.csv file2.csv --key id --format unified`
- **THEN** the system outputs differences in unified diff format

### Requirement: Exclude Command
The system SHALL provide an `exclude` command that removes rows from one file based on keys in another file.

#### Scenario: Exclude rows by key
- **WHEN** user runs `undatum exclude data.csv blacklist.csv --on email output.csv`
- **THEN** the system removes rows from `data.csv` where `email` matches values in `blacklist.csv`

#### Scenario: Exclude with multiple key fields
- **WHEN** user runs `undatum exclude data.jsonl exclude.jsonl --on id,email output.jsonl`
- **THEN** the system removes rows where both `id` and `email` match

### Requirement: Transpose Command
The system SHALL provide a `transpose` command that swaps rows and columns.

#### Scenario: Transpose CSV file
- **WHEN** user runs `undatum transpose data.csv output.csv`
- **THEN** the system swaps rows and columns, handling headers appropriately

#### Scenario: Transpose with header handling
- **WHEN** user runs `undatum transpose data.jsonl --header output.jsonl`
- **THEN** the system uses the first row as column headers in the transposed output

### Requirement: Sniff Command
The system SHALL provide a `sniff` command that detects file properties.

#### Scenario: Detect file properties
- **WHEN** user runs `undatum sniff data.csv`
- **THEN** the system outputs delimiter, encoding, field types, record count, and header detection

#### Scenario: Output sniff results as JSON
- **WHEN** user runs `undatum sniff data.jsonl --format json`
- **THEN** the system outputs detection results in JSON format

#### Scenario: Detect delimiter
- **WHEN** user runs `undatum sniff data.csv`
- **THEN** the system detects and reports the delimiter character (comma, semicolon, tab, etc.)

#### Scenario: Detect encoding
- **WHEN** user runs `undatum sniff data.csv`
- **THEN** the system detects and reports the file encoding (UTF-8, Latin-1, etc.)

#### Scenario: Estimate record count
- **WHEN** user runs `undatum sniff large_file.csv`
- **THEN** the system estimates the total number of records

## MODIFIED Requirements

### Requirement: Slice Command
The system SHALL provide a `slice` command that extracts specific rows by range or index.

**Note:** This enhances the existing partial functionality available via `convert --start_line`.

#### Scenario: Slice by range
- **WHEN** user runs `undatum slice data.csv --start 100 --end 200 output.csv`
- **THEN** the system extracts rows 100-200 (inclusive) to the output file

#### Scenario: Slice with DuckDB optimization
- **WHEN** user runs `undatum slice data.parquet --start 0 --end 1000 output.parquet`
- **THEN** the system uses DuckDB for efficient random access when supported

#### Scenario: Slice by index list
- **WHEN** user runs `undatum slice data.jsonl --indices 1,5,10,20 output.jsonl`
- **THEN** the system extracts rows at the specified indices

### Requirement: Format Command
The system SHALL provide a `fmt` command that reformats CSV data with specific formatting options.

**Note:** This enhances the existing partial functionality available via `convert`.

#### Scenario: Change delimiter
- **WHEN** user runs `undatum fmt data.csv --delimiter ";" output.csv`
- **THEN** the system converts the CSV to use semicolon as delimiter

#### Scenario: Change quote style
- **WHEN** user runs `undatum fmt data.csv --quote always output.csv`
- **THEN** the system quotes all fields in the output

#### Scenario: Change escape character
- **WHEN** user runs `undatum fmt data.csv --escape backslash output.csv`
- **THEN** the system uses backslash as escape character

#### Scenario: Change line endings
- **WHEN** user runs `undatum fmt data.csv --line-ending crlf output.csv`
- **THEN** the system uses CRLF line endings in the output
