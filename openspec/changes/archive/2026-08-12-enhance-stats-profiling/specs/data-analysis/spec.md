## MODIFIED Requirements

### Requirement: Statistics Command
The system SHALL provide comprehensive dataset statistics and profiling capabilities.

#### Scenario: Enhanced statistics with profiling
- **WHEN** user runs `undatum stats data.csv`
- **THEN** the system SHALL display:
  - Field types and structure
  - Missing value rates per field (count and percentage)
  - Distinct counts and cardinality percentages
  - Type classification (categorical vs numerical)
  - Distribution statistics for numerical fields (mean, median, percentiles)
  - Min/max values and standard deviation

#### Scenario: Profile command alias
- **WHEN** user runs `undatum profile data.csv`
- **THEN** the system SHALL behave identically to `undatum stats data.csv`
- **AND** provide the same enhanced profiling output

#### Scenario: JSON output for profiling
- **WHEN** user runs `undatum stats data.csv --output-format json`
- **THEN** the system SHALL output structured JSON with all profiling metrics
- **AND** include missing values, cardinality, type inference, and distributions

### Requirement: Type Inference
The statistics command SHALL infer field types (categorical vs numerical) based on data characteristics.

#### Scenario: Categorical field detection
- **WHEN** a field has low cardinality (e.g., < 10% distinct values) and string-like values
- **THEN** the system SHALL classify it as categorical
- **AND** display this classification in statistics output

#### Scenario: Numerical field detection
- **WHEN** a field has numeric type and high cardinality
- **THEN** the system SHALL classify it as numerical
- **AND** compute distribution statistics (mean, median, percentiles)

### Requirement: Missing Value Analysis
The statistics command SHALL analyze and report missing values per field.

#### Scenario: Missing value reporting
- **WHEN** user runs `undatum stats data.csv`
- **THEN** for each field, the system SHALL display:
  - Count of missing/null values
  - Percentage of missing values
  - Total non-null count

### Requirement: Cardinality Analysis
The statistics command SHALL analyze and report field cardinality (distinct value counts).

#### Scenario: Cardinality reporting
- **WHEN** user runs `undatum stats data.csv`
- **THEN** for each field, the system SHALL display:
  - Distinct count
  - Cardinality percentage (distinct/total)
  - Classification as high/low cardinality

### Requirement: Distribution Statistics
The statistics command SHALL compute distribution statistics for numerical fields.

#### Scenario: Distribution reporting
- **WHEN** user runs `undatum stats data.csv` on data with numerical fields
- **THEN** for each numerical field, the system SHALL display:
  - Mean, median, mode
  - Percentiles (25th, 50th, 75th, 90th, 95th, 99th)
  - Min and max values
  - Standard deviation
