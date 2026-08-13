## ADDED Requirements

### Requirement: Excel Input Parity for Analysis Commands
Commands that consume tabular row iterators (`analyze`, `uniq`, `frequency`, `select`) SHALL
accept Excel (XLS/XLSX) inputs where iterabledata can read them, without artificial format
exclusion.

#### Scenario: Analyze an Excel workbook
- **WHEN** a user runs `undatum analyze data.xlsx` (with sheet options if required)
- **THEN** the command analyzes rows successfully instead of rejecting Excel as unsupported

#### Scenario: Select from Excel
- **WHEN** a user runs `undatum select data.xlsx` with field selection
- **THEN** selected columns are emitted for Excel rows

#### Scenario: Frequency / uniq on Excel
- **WHEN** a user runs `undatum frequency` or `undatum uniq` on an Excel file
- **THEN** the command completes using the same semantics as for CSV/JSONL tabular inputs
