## ADDED Requirements

### Requirement: Package command availability
The system SHALL provide a CLI command `package` with a `create` subcommand that
generates a Frictionless Data Package descriptor (`datapackage.json`) for one or
more input data files.

#### Scenario: Create a package for a single file
- **WHEN** the user runs `undatum package create data.csv`
- **THEN** the system outputs a valid `datapackage.json` describing `data.csv`

#### Scenario: Create a package for multiple files
- **WHEN** the user runs `undatum package create data.csv data.jsonl`
- **THEN** the system generates a package with two resources

### Requirement: Output destination
The system SHALL write `datapackage.json` to the current working directory by
default and to a provided path when `--output` is supplied.

#### Scenario: Write package to a custom path
- **WHEN** the user runs `undatum package create data.csv --output out/package.json`
- **THEN** the system writes the package descriptor to `out/package.json`

### Requirement: Package directory output
The system SHALL create a package directory containing `datapackage.json` and
copies of local input files when `--package-dir` is provided.

#### Scenario: Create a package directory
- **WHEN** the user runs `undatum package create data.csv --package-dir out/package`
- **THEN** the system writes `out/package/datapackage.json` and copies `data.csv`
  into the package directory

### Requirement: Resource schema inference
Each resource in the package MUST include a schema derived from undatum's
existing field type inference.

#### Scenario: Include inferred field types
- **WHEN** the user packages a CSV with inferred field types
- **THEN** each resource schema includes field names and types derived from
  undatum's inference logic

### Requirement: Resource path mapping
Each input file or URI MUST be represented as a resource with its `path` set to
the provided input and a resource name derived from the input filename or URI.

#### Scenario: Preserve remote resource paths
- **WHEN** the user runs `undatum package create https://example.org/data.csv`
- **THEN** the resource `path` is set to the provided URL

### Requirement: Optional metadata fields
The system SHALL include optional metadata fields (`name`, `title`,
`description`, `licenses`, `sources`, `contributors`) in the package descriptor
when they are provided by the user.

#### Scenario: Provide package metadata
- **WHEN** the user runs `undatum package create data.csv --name demo --title "Demo"`
- **THEN** the generated package includes the provided metadata fields

### Requirement: Autodoc metadata generation
When `--autodoc` is enabled and an AI provider is configured, the system SHALL
reuse the `doc` command metadata generation logic to populate required package
metadata fields; if AI initialization fails, it SHALL proceed without AI
enhancements.

#### Scenario: Autodoc fills package metadata
- **WHEN** the user runs `undatum package create data.csv --autodoc`
- **THEN** the system populates package metadata fields using the same LLM-backed
  logic as the `doc` command

#### Scenario: Autodoc fallback on provider failure
- **WHEN** `--autodoc` is enabled but the AI provider cannot initialize
- **THEN** the package is generated without AI-derived metadata

### Requirement: Streaming-safe processing
Package generation MUST avoid loading entire datasets into memory and SHOULD use
streaming analysis consistent with undatum's other commands.

#### Scenario: Large dataset packaging
- **WHEN** the user packages a large dataset
- **THEN** the system generates the package without loading the full dataset into memory
