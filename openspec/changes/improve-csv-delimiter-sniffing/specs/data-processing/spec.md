## ADDED Requirements

### Requirement: Quote-Aware CSV Dialect Sniffing
The system SHALL detect CSV delimiters using quote-aware sampling over multiple lines rather
than relying solely on first-line character counts.

#### Scenario: Semicolon-delimited European CSV
- **WHEN** a CSV file uses `;` as delimiter and commas appear inside quoted fields
- **THEN** auto-detection selects `;` as the delimiter

#### Scenario: Explicit delimiter overrides sniffing
- **WHEN** a user passes an explicit delimiter option
- **THEN** that delimiter is used regardless of sniffed dialect

#### Scenario: Inconclusive sniff falls back safely
- **WHEN** dialect sniffing cannot confidently determine a delimiter
- **THEN** the system falls back to a documented default or existing heuristic without crashing
