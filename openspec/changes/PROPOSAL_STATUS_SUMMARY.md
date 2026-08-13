# Proposal Status Summary

**Last updated:** 2026-08-13

> This file is a point-in-time snapshot. For live status, run `openspec list`.

## Archived (complete)

Many previously active changes were archived on 2026-08-12 after implementation
and/or verification, including P0/P1 user-needs work, Data API hardening, S3,
plugins, doc metadata, stats JSON, schema Excel/XML/DOCX, SDK result objects,
distribution binaries, community process docs, plot filtering/aggregation,
dictquery removal (DuckDB WHERE on frequency/uniq), stats DuckDB/progress tests,
and database ingestion leftovers (Docker/benchmarks deferred).

On 2026-08-13:

- `add-tui-interface` — `undatum tui` (extra `tui`, Textual); specs in `openspec/specs/tui/`
- `add-web-ui` — `undatum web` (extra `web`, FastAPI + Jinja2 + HTMX); specs in `openspec/specs/web-ui/`
- `remove-mistql-support` — removed `undatum query` and the `mistql` dependency; `--filter` is comparison/boolean only

## Active — remaining

- `add-undatum-improvement-roadmap` — remaining Phase 3 child proposals (cloud
  beyond S3, Kafka, synth, drift, LLM pipeline autodoc) explicitly deferred.
  Visual pipeline DAG, inline plot images, and Data API embed from the web UI
  remain follow-on (not in the archived web UI change).

## Maintainer follow-up (not code)

- GitHub Discussions: https://github.com/datacoon/undatum/discussions
- structured-text-tools listing: https://github.com/dbohdan/structured-text-tools/pull/139
- CLI defaults: `defaults:` in `undatum.yaml` / `~/.undatum/config.yaml`; `undatum config show`
- Man page: `man/undatum.1` (`make man`); installed to `share/man/man1`
- `pipeline doc` emits Mermaid/Markdown diagrams; LLM pipeline autodoc remains deferred
- `pipeline run` executes live CLI commands in-process (`convert` positionals, `$step` outputs)
- Snyk bot PRs were closed; Dependabot handles security bumps
- Issue #18 (multiprocessing) closed: `--threads` ships on convert/validate/stats/frequency
