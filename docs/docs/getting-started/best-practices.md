---
title: "Best practices"
description: "Practical defaults for conversion, validation, and pipelines"
---
# Best practices

## Convert and store

- Prefer **Parquet** or **JSON Lines** for large or nested data. Avoid in-memory JSON arrays.
- Use `--low-memory` on multi-GB `convert`, `sort`, and `dedup`. See [performance](/getting-started/performance).
- Inspect the live catalog before choosing an output: `undatum formats list --writable`.
- Pass `--on-error skip` (and `--error-log`) when a few malformed rows should not abort a job.

## Validate before you publish

- Encode expectations in a rules file and run `undatum validate --rules rules.yml`.
- Pair validation with `analyze` / `profile` and `package validate`.
- For schema-only checks, use `undatum schema --validate`; keep `validate` for rule packs.

## Query and transform

- Use `--filter` comparison expressions for simple subsets; use [`sql`](/commands/sql) for `LIKE`, `IN`, joins, and aggregations.
- Apply filters early (`select --filter`, `search`) to cut volume before heavier steps.
- Prefer DuckDB (`--engine duckdb` or `auto`) on CSV/JSONL/Parquet.

## Pipelines and agents

- Validate pipeline YAML with `undatum pipeline validate` before `run`.
- Give agents [`mcp serve`](/integrations/mcp) rather than unconstrained shell access.
- Keep core verbs (`convert`, `stats`, `validate`, `select`) stable in scripts; check `CHANGELOG.md` before pinning flags.

## Configuration

Put shared defaults in `undatum.yaml` or `~/.undatum/config.yaml`, then inspect with `undatum config show`. See [config](/commands/config).
