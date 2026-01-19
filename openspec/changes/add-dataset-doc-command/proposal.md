# Change: Add dataset documentation command

## Why
Users need a dedicated way to generate dataset documentation in Markdown and other formats.
Current commands provide partial outputs and Markdown is not available for analysis results.

## What Changes
- Add `doc` command with `document` alias to generate dataset documentation
- Support Markdown (default), JSON, YAML, and text outputs
- Integrate schema, statistics, and sample records into a single report
- Optionally enhance descriptions using existing AI providers
- Allow output to stdout or a file path

## Impact
- Affected specs: `specs/dataset-documentation/spec.md`
- Affected code: `undatum/core.py`, `undatum/cmds/doc.py`, shared formatters/utilities
- Tests: new CLI and output format coverage under `tests/`
