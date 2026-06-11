# Proposal Status Summary

**Last updated:** 2026-06-11

> This file is a point-in-time snapshot. For live status, run `openspec list`.

## Archived (complete, moved to `archive/`)

| Change | Archived |
|--------|----------|
| add-data-api | 2026-06-11 |
| add-dataset-doc-command | 2026-06-11 |
| add-diff-command | 2026-06-11 |
| add-extract-command | 2026-06-11 |
| add-schema-format-exports | 2026-06-11 |
| consolidate-schema-commands | 2026-06-11 |
| improve-command-error-handling | 2026-06-11 |
| improve-select-command | 2026-06-11 |

## Active — implemented, remaining tasks are mostly tests/docs

These changes have working code in the repository; open checkboxes are primarily
integration tests, benchmarks, and documentation polish.

- add-phase1-data-commands (51/52)
- add-phase2-data-commands (78/79)
- add-phase3-data-commands (71/72)
- migrate-to-iterabledata (29/31) — deprecated `IterableData`/`DataWriter` removal deferred
- add-frictionless-package-command (7/8)
- add-examples-command (21/25)
- add-plot-command (18/23)
- add-db-query-load (17/20)
- add-rich-validation-rules (16/19)
- add-pipeline-templates (14/18)
- add-pipeline-command (11/14)
- add-python-sdk (10/15)
- enhance-stats-profiling (12/16)
- improve-schema-command (10/14)
- add-mask-command (6/12)
- improve-duckdb-operations (13/22)
- remove-dictquery-dependency (50/64)
- optimize-stats-command-duckdb (50/98)
- add-duckdb-ingestion (35/62)
- add-mysql-sqlite-ingestion (60/89)
- add-postgresql-ingestion (43/65)
- improve-database-ingestion-phase1 (27/39)
- improve-stats-command (8/18)
- add-plugin-system (16/24) — connector/transform integration pending

## Active — partially implemented (integration incomplete)

- add-s3-connector (7/16) — core connector done; rollout to all commands pending
- add-parallel-processing (3/16) — infrastructure modules exist; command integration pending

## Active — not started

- add-doc-metadata-fields (0/10)
- improve-doc-markdown-metadata (0/3)

## Meta

- add-undatum-improvement-roadmap (16/28) — Phase 3 child proposals (cloud connectors,
  streaming, synthetic data, TUI/web UI, quality monitoring, pipeline autodoc,
  CLI ergonomics) not yet created.
