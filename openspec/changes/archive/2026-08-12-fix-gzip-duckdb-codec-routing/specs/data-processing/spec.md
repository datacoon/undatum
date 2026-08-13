## ADDED Requirements

### Requirement: Gzip Codec DuckDB Eligibility
The system SHALL treat gzip-compressed duckable formats as eligible for the DuckDB engine by
recognizing the compression identifier used by iterabledata for `.gz` files.

#### Scenario: Gzip CSV routes to DuckDB when auto engine selected
- **WHEN** a `.csv.gz` (or equivalently gzip-compressed CSV) file is processed with `--engine auto`
  and the operation supports DuckDB
- **THEN** the engine selector chooses DuckDB rather than falling back solely because the codec
  id is `"gz"` instead of `"gzip"`

#### Scenario: Both gz and gzip identifiers accepted
- **WHEN** compression is reported as either `"gz"` or `"gzip"`
- **THEN** both identifiers are treated as DuckDB-eligible codecs alongside `"zst"` and `"raw"`
