# sdk Specification

## Purpose
TBD - created by archiving change add-python-sdk. Update Purpose after archive.
## Requirements
### Requirement: Dataset Class
The system SHALL provide a `Dataset` class as the primary interface for programmatic data
processing.

#### Scenario: Read data into Dataset
- **WHEN** user calls `ds = Dataset.read("data.jsonl")`
- **THEN** Dataset instance is created with data loaded from file

#### Scenario: Write Dataset to file
- **WHEN** user calls `ds.write("output.parquet")`
- **THEN** Dataset data is written to specified file

### Requirement: Transform Methods
The Dataset class SHALL provide transform methods that map to CLI commands.

#### Scenario: Fill missing values
- **WHEN** user calls `ds = ds.fill("age", value=0)`
- **THEN** missing values in "age" field are filled with 0, returning new Dataset instance

#### Scenario: Deduplicate records
- **WHEN** user calls `ds = ds.dedup(keys=["user_id"])`
- **THEN** duplicate records based on user_id are removed, returning new Dataset instance

#### Scenario: Sort data
- **WHEN** user calls `ds = ds.sort(key="age", reverse=True)`
- **THEN** data is sorted by age field in descending order, returning new Dataset instance

### Requirement: Method Chaining
Dataset transform methods SHALL support method chaining for fluent pipeline syntax.

#### Scenario: Chain multiple transforms
- **WHEN** user calls `ds = Dataset.read("data.jsonl").fill("age", 0).dedup(keys=["id"]).sort(key="age")`
- **THEN** all transforms are applied in sequence, returning final Dataset instance

### Requirement: Analysis Methods
The Dataset class SHALL provide analysis methods that return results rather than new Dataset
instances.

#### Scenario: Compute statistics
- **WHEN** user calls `stats = ds.stats()`
- **THEN** statistics object is returned with computed statistics

#### Scenario: Count records
- **WHEN** user calls `count = ds.count()`
- **THEN** integer count of records is returned

### Requirement: Consistent Behavior
SDK methods SHALL behave consistently with corresponding CLI commands.

#### Scenario: SDK matches CLI behavior
- **WHEN** same operation is performed via SDK and CLI
- **THEN** results are identical

