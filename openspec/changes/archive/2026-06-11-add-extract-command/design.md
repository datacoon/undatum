## Context
Document extraction introduces heavier dependencies (PDF parsing, OCR) and ambiguous outputs.
We need a clear, optional dependency strategy and predictable output structure.

## Goals / Non-Goals
- Goals:
  - Provide a stable `extract` command for PDF/DOCX/XLS/XLSX ingestion.
  - Keep dependencies optional and isolated from core installs.
  - Ensure outputs are compatible with existing undatum workflows.
- Non-Goals:
  - Perfect reconstruction of complex layouts or scanned PDFs by default.
  - Full support for PPTX/ODS or arbitrary archival formats in the first release.

## Decisions
- Decision: Implement `extract` as an optional extra or plugin entry point.
  - Alternatives considered: Bundling dependencies in core (rejected: heavy installs).
- Decision: Standardize output as tabular resources with per-table output by default.
  - Alternatives considered: Single mixed output (rejected: harder to consume downstream).
- Decision: Provide PDF controls via `--method`, `--pages`, `--ocr`, `--flatten`.
  - Alternatives considered: Auto-detect all options (rejected: opaque behavior).

## Risks / Trade-offs
- Dependency bloat and platform-specific OCR issues → optional extras and clear errors.
- Extraction ambiguity → document best-effort behavior and offer `--method` overrides.

## Migration Plan
- Add new command without impacting existing CLI routes.
- Introduce extras/plugin in packaging; core install remains unchanged.

## Open Questions
- Which PDF library should be the default (pdfplumber vs Camelot vs Tabula)?
- What is the canonical `datapackage.json` layout for multi-table extraction?
