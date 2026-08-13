## MODIFIED Requirements

### Requirement: Filter handling across engines
The system SHALL apply comparison and boolean filter expressions in `select`,
`frequency`, and `uniq` on both execution paths, translating filters to SQL
where safe and otherwise evaluating the same subset on the iterable path.
Boolean operators `AND`/`OR` and `&&`/`||` are both accepted, as are single-
and double-quoted strings. The system SHALL NOT depend on mistql.

#### Scenario: Filter translated to SQL for DuckDB
- **WHEN** a filter expression is provided and is safely translatable to SQL
- **THEN** the system applies it in the DuckDB query for `select`, `frequency`, and `uniq`

#### Scenario: Filter not translatable to SQL
- **WHEN** a filter expression cannot be safely translated to SQL (`IN`, `LIKE`, `match`, nested dotted fields)
- **THEN** the system SHALL reject the expression
- **AND** SHALL tell the user to use `undatum sql` for SQL-only constructs
