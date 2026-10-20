# workflow-automation Specification

## Purpose
YAML/JSON multi-step workflows (`pipeline run` / `validate` / `doc`) plus built-in templates (`pipeline templates`).
## Requirements
### Requirement: Pipeline Templates
The system SHALL provide reusable pipeline templates for common workflows.

#### Scenario: List available templates
- **WHEN** user runs `undatum pipeline templates list`
- **THEN** the system SHALL display all available templates with descriptions
- **AND** show template variables and usage examples

#### Scenario: Initialize template
- **WHEN** user runs `undatum pipeline templates init basic-cleaning`
- **THEN** the system SHALL prompt for template variables
- **AND** generate a customized pipeline file based on user input
- **AND** save the pipeline to the specified output file

### Requirement: Template Library
The system SHALL ship with a library of common workflow templates.

#### Scenario: Basic cleaning template
- **WHEN** user initializes `basic-cleaning` template
- **THEN** the generated pipeline SHALL include steps for:
  - Converting input format
  - Filling missing values
  - Removing duplicates
  - Basic validation

#### Scenario: Dataset profiling template
- **WHEN** user initializes `profile-dataset` template
- **THEN** the generated pipeline SHALL include steps for:
  - Sampling large datasets
  - Computing statistics
  - Generating frequency distributions
  - Creating documentation

### Requirement: Template Customization
Templates SHALL support variable substitution for customization.

#### Scenario: Customize template variables
- **WHEN** user runs `pipeline templates init` with a template containing variables
- **THEN** the system SHALL prompt for each variable
- **AND** substitute variables in the generated pipeline
- **AND** validate variable values before generating pipeline

#### Scenario: Template with defaults
- **WHEN** a template variable has a default value
- **THEN** the system SHALL use the default if user doesn't provide a value
- **AND** allow user to override defaults

### Requirement: Template Metadata
Templates SHALL include metadata describing their purpose and usage.

#### Scenario: Template description
- **WHEN** user views template list or details
- **THEN** the system SHALL display template description
- **AND** show required and optional variables
- **AND** provide usage examples

### Requirement: Pipeline Command
The system SHALL provide a `pipeline` command group for executing declarative data processing workflows.

#### Scenario: Run pipeline from YAML file
- **WHEN** user runs `undatum pipeline run pipeline.yml`
- **THEN** the system SHALL parse the pipeline specification
- **AND** execute each step in sequence
- **AND** handle step dependencies (outputs from one step become inputs to next)
- **AND** report execution status for each step

#### Scenario: Validate pipeline specification
- **WHEN** user runs `undatum pipeline validate pipeline.yml`
- **THEN** the system SHALL parse and validate the pipeline specification
- **AND** check that all commands are valid
- **AND** check that required arguments are provided
- **AND** report any validation errors without executing the pipeline

#### Scenario: Current commands are valid steps
- **WHEN** a pipeline step uses `sql`, `plot`, or `repack`
- **THEN** `pipeline validate` accepts those commands

#### Scenario: Step output references
- **WHEN** a later step sets `input: $earlier_step`
- **THEN** the system SHALL use that step's output file as input
- **AND** a step with no `input` SHALL use the previous step's output when one exists

#### Scenario: Document pipeline as Mermaid
- **WHEN** user runs `undatum pipeline doc pipeline.yml`
- **THEN** the system SHALL print Markdown containing a Mermaid flowchart of the steps

### Requirement: Pipeline Specification Format
Pipeline specifications SHALL support YAML and JSON formats with a defined structure.

#### Scenario: YAML pipeline with multiple steps
- **WHEN** user creates a pipeline.yml file:
  ```yaml
  steps:
    - name: load_data
      command: convert
      args:
        input: s3://bucket/raw.ndjson
        output: /tmp/data.parquet
        format_out: parquet
    - name: clean
      command: fill
      args:
        input: /tmp/data.parquet
        output: /tmp/data_cleaned.parquet
        fields: age
        value: 0
    - name: deduplicate
      command: dedup
      args:
        input: /tmp/data_cleaned.parquet
        output: /tmp/data_final.parquet
        keys: user_id
  ```
- **THEN** the system SHALL parse and execute each step in order
- **AND** use outputs from previous steps as inputs to subsequent steps

#### Scenario: Pipeline with variables
- **WHEN** user creates a pipeline with variables:
  ```yaml
  variables:
    input_bucket: ${AWS_S3_BUCKET}
    output_dir: /tmp/output
  
  steps:
    - name: load
      command: convert
      args:
        input: s3://${input_bucket}/data.jsonl
        output: ${output_dir}/data.parquet
  ```
- **THEN** the system SHALL substitute variables from environment or CLI overrides
- **AND** support `${VAR}` syntax for variable references
- **AND** allow `--var key=value` CLI overrides

### Requirement: Command Integration
Pipeline steps SHALL support all existing undatum commands.

#### Scenario: Pipeline with various commands
- **WHEN** user creates a pipeline with multiple command types:
  ```yaml
  steps:
    - name: convert
      command: convert
      args: {...}
    - name: stats
      command: stats
      args: {...}
    - name: mask
      command: mask
      args: {...}
  ```
- **THEN** the system SHALL execute each command with provided arguments
- **AND** map pipeline args to CLI command options correctly

### Requirement: Error Handling
Pipeline execution SHALL handle errors gracefully and provide useful feedback.

#### Scenario: Step failure
- **WHEN** a pipeline step fails during execution
- **THEN** the system SHALL stop execution (unless configured to continue)
- **AND** report which step failed and why
- **AND** clean up temporary files created by previous steps
- **AND** return non-zero exit code

#### Scenario: Validation errors
- **WHEN** user runs `pipeline validate` on an invalid specification
- **THEN** the system SHALL report all validation errors
- **AND** provide clear error messages indicating what is wrong
- **AND** return non-zero exit code

