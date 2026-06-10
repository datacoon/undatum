# Change: Add Pipeline Command for Workflow Automation

## Why

Undatum currently requires users to chain commands manually via shell scripts, making workflows hard to document, version, and reproduce. Adding a `pipeline` command with YAML/JSON pipeline definitions would enable declarative, repeatable data processing workflows that are easier to maintain and share.

**Current Issues:**
1. **Manual workflow chaining**: Users must write shell scripts to chain commands
2. **No workflow documentation**: Workflows are scattered across scripts and README files
3. **Hard to reproduce**: Manual steps are error-prone and difficult to replicate
4. **No validation**: No way to validate workflow definitions before execution

**Expected Benefits:**
- **Declarative workflows** defined in YAML/JSON files
- **Version-controlled pipelines** that can be shared and reviewed
- **Workflow validation** before execution
- **Variable substitution** for flexible, reusable pipelines
- **Better documentation** of data processing workflows

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 2.1)

## What Changes

- **ADDED**: `undatum pipeline run <pipeline.yml>` command to execute pipeline workflows
- **ADDED**: `undatum pipeline validate <pipeline.yml>` command to validate pipeline definitions
- **ADDED**: YAML/JSON pipeline specification format with:
  - Step definitions (name, command, args)
  - Variable substitution (environment variables, CLI overrides)
  - Input/output path resolution
  - Error handling and validation
- **ADDED**: Support for all existing undatum commands in pipeline steps
- **ADDED**: Pipeline execution context (working directory, temp files, step dependencies)

All changes are additive. CLI commands continue to work independently.

## Impact

- **Affected specs**: `workflow-automation` capability
- **Affected code**:
  - New `undatum/cmds/pipeline.py` module for pipeline execution
  - New `undatum/common/pipeline_parser.py` for parsing YAML/JSON specs
  - `undatum/core.py` - Add `pipeline` command group with subcommands
- **Dependencies**: `pyyaml` (for YAML parsing, already used in project)
- **Backward compatibility**: Fully backward compatible - new command group only
