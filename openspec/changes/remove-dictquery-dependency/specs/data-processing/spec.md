## MODIFIED Requirements

### Requirement: Data Filtering

The system SHALL provide filtering capabilities for dictionary records using query expressions that support comparison operators, logical operators, nested key access, and pattern matching.

#### Scenario: Filter with comparison operator
- **WHEN** user provides a filter expression `"age >= 18"` to validator or selector commands
- **THEN** the system filters records where the `age` field is greater than or equal to 18

#### Scenario: Filter with logical operators
- **WHEN** user provides a filter expression `"gender == \"female\" && age > 18"` to validator or selector commands
- **THEN** the system filters records matching both conditions

#### Scenario: Filter with nested keys
- **WHEN** user provides a filter expression with nested keys like `"user.name == 'John'"` 
- **THEN** the system accesses nested dictionary values and applies the filter

#### Scenario: Filter using DuckDB engine
- **WHEN** user provides a filter expression and the data is processed via DuckDB engine
- **THEN** the system translates the filter expression to SQL WHERE clause for optimal performance

#### Scenario: Filter using iterable engine
- **WHEN** user provides a filter expression and the data is processed via iterable engine
- **THEN** the system uses mistql-based filtering for dictionary records

#### Scenario: Filter in validator command
- **WHEN** user runs validator command with `--filter` option on CSV, JSONL, or BSON files
- **THEN** the system filters records before validation using the provided expression

#### Scenario: Filter in selector command
- **WHEN** user runs selector command methods (select, split, frequency) with `--filter` option
- **THEN** the system filters records before processing using the provided expression

#### Scenario: Filter expression error handling
- **WHEN** user provides an invalid or malformed filter expression
- **THEN** the system reports a clear error message indicating the problem with the filter expression
