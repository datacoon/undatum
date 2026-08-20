# Operations

How commands actually run, and which knobs matter in production.

## Read path

1. Path or URI (`s3://`, `gs://`/`gcs://`, `az://`/`abfs://`/`abfss://`) → iterabledata (cloud extras as needed).
2. Dialect: `--format-in`, `--table`/`--sheet`, `--delimiter`, `--quotechar`, `--encoding`, `--tagname`.
3. Optional `--flatten-nested` projection.
4. Optional `--filter` (DuckDB `WHERE` when translatable, else in-process).
5. `--on-error raise|skip|warn` plus optional `--error-log` JSONL.

Shared flags: `docs/docs/commands/shared-options.md`.

## Convert and large files

- Default convert goes through iterabledata. `--low-memory` enables spill/COPY paths when both sides support them.
- `--native-batch`, `--columns`, `--row-range`, `--batch-size`, `--row-group-size` size columnar I/O.
- `--profile fast|balanced|max` and `--level` set codec strength (`--level` skips DuckDB COPY).
- `--write-mode` applies to lakehouse outputs. `--trust` is required for pickle reads.
- `--threads` is process-pool parallelism for Python-engine convert/validate/stats/frequency (not DuckDB).

## Databases

- `db query` / `db dump` — PostgreSQL, MySQL, SQLite, MSSQL, ClickHouse, plus read-only MongoDB/Elasticsearch URIs.
- `db load` — URI-detected wrapper around `ingest` for SQL engines (append/replace/upsert).
- `ingest` — MongoDB, Elasticsearch, DuckDB, and the SQL engines with batch/retry/create-table options.

## Pipelines

`pipeline run` maps YAML `args` onto Typer argv in-process (`undatum/cmds/pipeline.py`). `tui` and `web` are not valid steps. Templates live in `undatum/templates/` (`basic-cleaning`, `data-quality`, `profile-dataset`, `s3-etl`, `jsonl-normalization`).

## Config

`undatum config show` prints merged `defaults:`. Later wins: `UNDATUM_*` env → `~/.undatum/config.yaml` → `./undatum.yaml`. CLI flags override. AI `ai:` uses env, then the first config file found (project preferred), then CLI flags; provider API keys stay in the environment.

## Related

- [Domain concepts](domain.md)
- [Integrations](integrations.md)
- User docs: `docs/docs/getting-started/performance.md`, `docs/docs/getting-started/troubleshooting.md`
