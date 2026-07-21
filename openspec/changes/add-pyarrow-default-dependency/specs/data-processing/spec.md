## ADDED Requirements

### Requirement: Default Parquet Support
The system SHALL support Parquet read and write in a default installation without requiring
users to discover an undocumented optional extra after a failed convert.

#### Scenario: Convert CSV to Parquet after default install
- **WHEN** a user installs undatum with its default dependencies and runs
  `undatum convert data.csv data.parquet`
- **THEN** the conversion succeeds (or, if Parquet remains optional by explicit design decision,
  fails with a precise install hint naming the required package/extra)

#### Scenario: Missing dependency guidance is actionable
- **WHEN** Parquet support cannot run due to a missing optional dependency
- **THEN** the error message names the package or extra to install and does not present a raw
  `ModuleNotFoundError` alone
