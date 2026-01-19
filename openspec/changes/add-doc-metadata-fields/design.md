## Context
The `doc` command currently outputs core metadata, schema, statistics, and samples, with optional AI
descriptions. Users want catalog-ready metadata (title, keywords, coverage, language, data theme) and
PII visibility. These additions introduce new metadata extraction and optional dependencies.

## Goals / Non-Goals
- Goals:
  - Provide deterministic metadata extraction for title/coverage/language where feasible
  - Add structured AI metadata augmentation with confidence and evidence
  - Support EU Data Theme classification aligned with DCAT-AP
  - Add optional semantic typing and PII detection via Metacrafter
  - Maintain streaming-first constraints and graceful fallbacks
- Non-Goals:
  - Automated data correction or enrichment beyond documentation
  - Mandatory network dependency for metadata extraction
  - Full dataset scanning for PII (sampling only)

## Decisions
- Decision: Use a hybrid pipeline (deterministic first, AI augmentation optional).
  - Alternatives considered: AI-only extraction (rejected due to reliability and privacy risk).
- Decision: Keep AI metadata behind existing `--autodoc` flag and extend output schema.
  - Alternatives considered: new `--autodoc-metadata` flag (adds complexity).
- Decision: Add optional flags for semantic typing/PII detection and PII masking.
  - Rationale: avoid performance or privacy impact by default.
- Decision: Cache the Metacrafter registry locally with a pinned version.
  - Rationale: avoid runtime network dependency and ensure reproducibility.

## Risks / Trade-offs
- Risk: AI hallucinations in metadata → Mitigation: evidence + confidence fields
- Risk: PII false positives/negatives → Mitigation: rule IDs and thresholds
- Risk: Performance regressions → Mitigation: sampling, reuse DuckDB stats
- Risk: Privacy exposure → Mitigation: redact sensitive fields in AI prompts

## Migration Plan
- Backward-compatible defaults; new fields appear but can be empty
- Optional features gated by flags; no breaking CLI changes

## Open Questions
- Should PII-masked samples be the default when PII detection is enabled?
- Do we store AI raw JSON for auditability?
- What minimum confidence should be required to classify data theme?
