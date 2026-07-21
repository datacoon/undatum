# data-packaging Specification

## Purpose
Generate, extend, and validate Frictionless Data Package descriptors from undatum data files.

## Requirements

### Requirement: Package command availability
The system SHALL provide a CLI command `package` with `create`, `add-resource`, and
`validate` subcommands for Frictionless Data Package workflows.

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

### Requirement: Frictionless profile and resource metadata
Generated packages MUST include a Frictionless profile and resource metadata
(`format`, `mediatype`, and `encoding` when known).

#### Scenario: Include Frictionless profile
- **WHEN** the user packages a CSV file
- **THEN** the generated descriptor includes `profile: tabular-data-package`
- **AND** each resource includes `format` and `mediatype`

### Requirement: Resource schema inference
Each resource in the package MUST include a schema derived from undatum's
existing field type inference, including optional uniqueness constraints when
stats indicate all values are unique.

#### Scenario: Include inferred field types
- **WHEN** the user packages a CSV with inferred field types
- **THEN** each resource schema includes field names and types derived from
  undatum's inference logic

### Requirement: Resource path mapping
Each input file or URI MUST be represented as a resource with a portable `path`
(relative when co-located with the descriptor, remote URL when remote).

#### Scenario: Preserve remote resource paths
- **WHEN** the user runs `undatum package create https://example.org/data.csv`
- **THEN** the resource `path` is set to the provided URL

### Requirement: Optional metadata fields
The system SHALL include optional metadata fields (`name`, `title`,
`description`, `licenses`, `sources`, `contributors`, inferred coverage fields)
in the package descriptor when provided or inferred.

#### Scenario: Provide package metadata
- **WHEN** the user runs `undatum package create data.csv --name demo --title "Demo"`
- **THEN** the generated package includes the provided metadata fields

### Requirement: Autodoc metadata generation
When `--autodoc` is enabled and an AI provider is configured, the system SHALL
reuse the `doc` command metadata generation logic without duplicate AI calls;
if AI initialization fails, it SHALL proceed without AI enhancements.

#### Scenario: Autodoc fills package metadata
- **WHEN** the user runs `undatum package create data.csv --autodoc`
- **THEN** the system populates package metadata fields using the same LLM-backed
  logic as the `doc` command

#### Scenario: Autodoc fallback on provider failure
- **WHEN** `--autodoc` is enabled but the AI provider cannot initialize
- **THEN** the package is generated without AI-derived metadata

### Requirement: Package validation
The system SHALL provide `package validate` to validate descriptors, using the
optional `frictionless` dependency when installed and basic structural checks
otherwise.

#### Scenario: Validate an existing package
- **WHEN** the user runs `undatum package validate datapackage.json`
- **THEN** the system reports whether the descriptor is valid

### Requirement: Incremental resource addition
The system SHALL provide `package add-resource` to append resources to an
existing package descriptor.

#### Scenario: Add a resource to an existing package
- **WHEN** the user runs `undatum package add-resource datapackage.json new.csv`
- **THEN** the system appends a new resource entry and copies local data when needed

### Requirement: Streaming-safe processing
Package generation MUST avoid loading entire datasets into memory and SHOULD use
streaming analysis consistent with undatum's other commands.

#### Scenario: Large dataset packaging
- **WHEN** the user packages a large dataset
- **THEN** the system generates the package without loading the full dataset into memory

### Requirement: SDK and pipeline integration
The system SHALL expose packaging through the Python SDK and pipeline runner.

#### Scenario: SDK packaging
- **WHEN** a user calls `Dataset.read("data.csv").package(output="datapackage.json")`
- **THEN** the system generates a Frictionless Data Package descriptor

#### Scenario: Pipeline package step
- **WHEN** a pipeline step uses `command: package` with `subcommand: create`
- **THEN** the pipeline runner generates the package without invoking a nested CLI group incorrectly
