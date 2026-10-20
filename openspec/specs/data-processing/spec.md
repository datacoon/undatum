# data-processing Specification

## Purpose
Infer and export schemas (`schema` / `schema_bulk`) in JSON Schema, YAML, Frictionless, and Cerberus formats.
## Requirements
### Requirement: JSON Schema Export
The system SHALL support exporting schemas in JSON Schema format (W3C/IETF standard).

#### Scenario: Export schema as JSON Schema
- **WHEN** user runs `undatum schema data.csv --format jsonschema`
- **THEN** the system outputs a valid JSON Schema document following JSON Schema draft-07 specification

#### Scenario: JSON Schema includes type information
- **WHEN** user runs `undatum schema data.jsonl --format jsonschema`
- **THEN** the output includes proper type mappings (string, integer, number, boolean, object, array)

#### Scenario: JSON Schema handles nested structures
- **WHEN** user runs `undatum schema nested_data.jsonl --format jsonschema`
- **THEN** the output includes nested object definitions for STRUCT types

#### Scenario: JSON Schema includes field descriptions
- **WHEN** user runs `undatum schema data.csv --format jsonschema --autodoc`
- **THEN** the output includes description fields for each property

### Requirement: Avro Schema Export
The system SHALL support exporting schemas in Avro schema format.

#### Scenario: Export schema as Avro schema
- **WHEN** user runs `undatum schema data.jsonl --format avro`
- **THEN** the system outputs a valid Avro schema JSON document

#### Scenario: Avro schema includes proper type mappings
- **WHEN** user runs `undatum schema data.csv --format avro`
- **THEN** the output maps data types to Avro types (string, int, long, double, boolean, etc.)

#### Scenario: Avro schema handles nested records
- **WHEN** user runs `undatum schema nested_data.jsonl --format avro`
- **THEN** the output includes nested record definitions for STRUCT types

### Requirement: Parquet Schema Export
The system SHALL support exporting schemas in Parquet schema format.

#### Scenario: Export schema as Parquet schema
- **WHEN** user runs `undatum schema data.parquet --format parquet`
- **THEN** the system outputs Parquet schema information

#### Scenario: Parquet schema from other formats
- **WHEN** user runs `undatum schema data.csv --format parquet`
- **THEN** the system converts the CSV schema to Parquet schema format

### Requirement: Schema Extraction Command
The system SHALL provide a unified `schema` command that supports multiple schema output formats through a `--format` parameter.

#### Scenario: Extract schema in Cerberus format
- **WHEN** user runs `undatum schema data.jsonl --format cerberus`
- **THEN** the system outputs Cerberus validation schema in JSON format

#### Scenario: Extract schema in JSON Schema format
- **WHEN** user runs `undatum schema data.csv --format jsonschema`
- **THEN** the system outputs JSON Schema (W3C/IETF standard) format

#### Scenario: Extract schema in Avro format
- **WHEN** user runs `undatum schema data.jsonl --format avro`
- **THEN** the system outputs Avro schema format

#### Scenario: Extract schema in Parquet format
- **WHEN** user runs `undatum schema data.parquet --format parquet`
- **THEN** the system outputs Parquet schema format

#### Scenario: Extract schema in default YAML format
- **WHEN** user runs `undatum schema data.csv` (without --format)
- **THEN** the system outputs schema in YAML format (default)

#### Scenario: Bulk extraction with format selection
- **WHEN** user runs `undatum schema_bulk data/ --format jsonschema --output schemas/`
- **THEN** the system extracts schemas in JSON Schema format for all files

### Requirement: Scheme Command Deprecation
The legacy `scheme` command SHALL be deprecated in favor of `undatum schema --format cerberus` while continuing to work with a deprecation warning during the transition period.

#### Scenario: Deprecated scheme command invocation
- **WHEN** user runs `undatum scheme data.jsonl`
- **THEN** the system shows a deprecation warning recommending `undatum schema --format cerberus`
- **AND** still produces the Cerberus schema output for backward compatibility

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

### Requirement: Plot Command
The system SHALL provide a `plot` command that writes matplotlib charts from
tabular files. Bar charts SHALL support filtering and aggregation.

#### Scenario: Filter then plot
- **WHEN** user runs `undatum plot data.csv --field city --type bar --filter 'age >= 30' --output cities.png`
- **THEN** the system plots only matching rows and writes a PNG file

#### Scenario: Aggregate bar chart
- **WHEN** user runs `undatum plot data.csv --field city --type bar --aggregate sum --value-field amount --output totals.png`
- **THEN** the system groups by city, sums `amount`, and writes a PNG file

### Requirement: Stats profiling reports
The system SHALL emit dataset profiles as a terminal table by default, and as
JSON, HTML, or Markdown when requested via `--format-out` or inferred from the
output file extension.

#### Scenario: HTML profile report
- **WHEN** user runs `undatum stats data.csv --format-out html --output profile.html`
- **THEN** the system writes an HTML document with a field-statistics table

#### Scenario: Markdown inferred from extension
- **WHEN** user runs `undatum stats data.csv --output profile.md`
- **THEN** the system writes a Markdown profiling report

### Requirement: JSON analysis output
The system SHALL emit JSON for `headers`, `frequency`, `uniq`, `sniff`, and
`analyze` when `--format-out json` (or `--outtype json` for analyze) is set or
the output path ends in `.json`.

#### Scenario: Headers as JSON
- **WHEN** user runs `undatum headers data.csv --format-out json`
- **THEN** the system prints a JSON object with a `fields` array

#### Scenario: Frequency as JSON
- **WHEN** user runs `undatum frequency --fields city --format-out json data.csv`
- **THEN** the system prints a JSON array of `{field, count}` records

#### Scenario: Analyze as Markdown
- **WHEN** user runs `undatum analyze data.csv --output report.md`
- **THEN** the system writes a Markdown analysis report with field tables

### Requirement: CLI defaults configuration
The system SHALL load command defaults from `undatum.yaml` in the current
directory, `~/.undatum/config.yaml`, and `UNDATUM_*` environment variables.
Explicit CLI flags SHALL override config. Project file SHALL override the home
file. Config files SHALL override environment variables.

#### Scenario: Engine default from project file
- **WHEN** `undatum.yaml` contains `defaults.engine: python` and the user omits `--engine`
- **THEN** commands that select an engine use the Python/iterable engine

#### Scenario: Inspect resolved defaults
- **WHEN** user runs `undatum config show`
- **THEN** the system prints the merged defaults and which config files were found

### Requirement: Manual page
The system SHALL ship an `undatum(1)` manual page that lists commands, config
files, environment variables, and exit codes.

#### Scenario: Man page is generated from the CLI
- **WHEN** a maintainer runs `make man`
- **THEN** `man/undatum.1` is regenerated from the current Typer command tree

