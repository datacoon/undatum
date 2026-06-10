# Change: Add Pipeline Templates for Reusable Workflows

## Why

While the pipeline command enables declarative workflows, users must write pipeline specifications from scratch for common tasks. Providing reusable pipeline templates would accelerate adoption, standardize workflows, and reduce boilerplate. Templates can encapsulate best practices and common patterns.

**Current Issues:**
1. **No reusable templates**: Users must write pipelines from scratch
2. **Repeated boilerplate**: Common patterns are reimplemented repeatedly
3. **Inconsistent workflows**: No standardized templates for common tasks
4. **Learning curve**: Users must learn pipeline syntax before being productive

**Expected Benefits:**
- **Faster onboarding** with ready-to-use templates
- **Standardized workflows** across teams and projects
- **Best practices** embedded in templates
- **Easy customization** of templates for specific needs

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 2.2)

## What Changes

- **ADDED**: `undatum pipeline templates list` command to list available templates
- **ADDED**: `undatum pipeline templates init <template-name>` command to initialize a template
- **ADDED**: Template library with common workflow templates:
  - `basic-cleaning` - CSV/JSONL cleaning workflow
  - `jsonl-normalization` - JSONL normalization and validation
  - `profile-dataset` - Dataset profiling and documentation
  - `s3-etl` - S3-based ETL workflow
  - `data-quality` - Data quality checks and validation
- **ADDED**: Template customization via variables and prompts
- **ADDED**: Template metadata (description, variables, examples)

All changes are additive. Existing pipeline functionality remains unchanged.

## Impact

- **Affected specs**: `workflow-automation` capability
- **Affected code**:
  - New `undatum/cmds/pipeline_templates.py` module
  - New `undatum/templates/` directory with template files
  - `undatum/core.py` - Add `pipeline templates` subcommands
- **Dependencies**: No new dependencies
- **Backward compatibility**: Fully backward compatible - new commands only
