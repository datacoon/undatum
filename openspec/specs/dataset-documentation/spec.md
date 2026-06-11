# dataset-documentation Specification

## Purpose
TBD - created by archiving change add-dataset-doc-command. Update Purpose after archive.
## Requirements
### Requirement: Document command availability
The system SHALL provide a CLI command `doc` with alias `document` to generate
dataset documentation for a provided input file.

#### Scenario: Generate documentation for a dataset
- **WHEN** the user runs `undatum doc <input-file>`
- **THEN** the system outputs a documentation report for that dataset

#### Scenario: Alias uses the same behavior
- **WHEN** the user runs `undatum document <input-file>`
- **THEN** the system behaves identically to `undatum doc`

### Requirement: Output formats
The system SHALL support documentation output in `markdown` (default), `json`,
`yaml`, and `text` formats.

#### Scenario: Generate JSON output
- **WHEN** the user runs `undatum doc <input-file> --format json`
- **THEN** the system outputs structured JSON documentation

### Requirement: Documentation contents
The documentation MUST include dataset metadata, schema field listings,
statistics summaries, and sample records when available.

#### Scenario: Core sections are present
- **WHEN** the system generates a documentation report
- **THEN** the output includes metadata, schema, statistics, and samples sections

### Requirement: Output destination
The system SHALL write documentation to stdout by default and to a file path
when `--output` is provided.

#### Scenario: Write to a file
- **WHEN** the user provides `--output docs/dataset.md`
- **THEN** the system writes the documentation to that file

### Requirement: Optional AI augmentation
When `--autodoc` is enabled and an AI provider is configured, the system SHALL
enrich dataset summaries and field descriptions; if AI initialization fails, it
SHALL continue without AI enhancements.

#### Scenario: AI provider unavailable
- **WHEN** `--autodoc` is enabled but the provider cannot initialize
- **THEN** the documentation is still generated without AI content

### Requirement: Streaming-safe processing
The documentation command MUST avoid loading entire datasets into memory and
MUST limit sample output to a configurable size.

#### Scenario: Large dataset sampling
- **WHEN** the input dataset is large
- **THEN** the system produces documentation using streaming analysis and
  includes only a bounded sample of records

