# Change: Add enriched metadata and PII insights to doc

## Why
Users need catalog-ready dataset documentation with title, keywords, coverage, language, and theme.
They also need visibility into PII risks and semantic field types for governance and sharing.

## What Changes
- Extend `doc` output metadata with title, keywords, geographic coverage, temporal coverage, languages, and EU data theme classification
- Augment AI autodoc to optionally produce structured metadata fields with confidence and evidence
- Add optional semantic typing and PII detection using Metacrafter, including PII summaries
- Add optional sample masking when PII detection is enabled
- Update tests and documentation for new outputs and flags

## Impact
- Affected specs: `specs/dataset-documentation/spec.md`
- Affected code: `undatum/cmds/doc.py`, `undatum/cmds/analyzer.py`, `undatum/ai/*`, optional dependency integration
- Tests: update/add coverage for new metadata and PII outputs
