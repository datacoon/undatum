---
title: "convert"
description: "undatum convert command reference"
---
# `convert`

Converts data between any formats supported by iterabledata (140+, see `undatum formats list`). Reading and writing are handled by the iterabledata engine, including cloud URIs (`s3://`, `gs://`, `az://`). Use `--recursive` to bulk-convert a directory or glob pattern.

```bash
# XML to JSON Lines
undatum convert --tagname item data.xml data.jsonl

# CSV to Parquet
undatum convert data.csv data.parquet

# JSON Lines to CSV
undatum convert data.jsonl data.csv

# Convert from S3 to local
undatum convert s3://my-bucket/data.csv output.jsonl

# Bulk-convert a directory of CSVs to Parquet
undatum convert ./raw ./processed --recursive --to-ext parquet
undatum convert ./raw ./out --recursive --to-ext jsonl --filename-pattern "{stem}.converted.jsonl"

# Convert local to S3
undatum convert input.csv s3://my-bucket/output.parquet

# Convert S3 to S3
undatum convert s3://bucket/input.jsonl s3://bucket/output.parquet
```

**Cloud storage:** Input and output paths support `s3://`, `gs://`/`gcs://`, and `az://`/`abfs://` URIs when the cloud extra is installed. See [Cloud Storage Support](/integrations/cloud).

**Key options:**
- `--format-in` / `--format-out` — override format detection
- `--table` / `--sheet` — named table or Excel sheet (keep `--start-page` for a 0-based index)
- `--native-batch` / `--columns` / `--row-range` — native columnar batch convert (auto with `--low-memory` when both formats support it); `--batch-size` also sizes native scanner chunks
- `--profile fast|balanced|max` — codec performance profile for compressed output
- `--level N` — explicit compression level for compressed output (overrides `--profile`; skips DuckDB COPY)
- `--write-mode append|overwrite|error|ignore|create` — lakehouse write mode (Delta / Iceberg / DuckLake / Lance)
- `--row-group-size N` — Parquet write row-group size (skips DuckDB COPY; pair with `--batch-size` if you need groups smaller than convert's write batches)
- `--use-totals` — use format-reported row totals for progress when available
- `--trust` — acknowledge pickle deserialization risk
- `--on-error raise|skip|warn` — parse-error policy for malformed rows (default: raise)
- `--error-log PATH` — append skipped/warned parse errors as JSONL
- `--delimiter`, `--quotechar`, `--encoding`, `--tagname` — passed through to the reader (delimiter auto-detected for CSV when omitted)
- `--recursive` / `--to-ext` / `--filename-pattern` — bulk-convert directories or globs (`{name}`, `{stem}`, `{ext}` in the output name)
- `--flatten` — flatten nested records to a flat schema
- `--atomic` — write to a temp file and rename on success (local paths only)
- `--threads`, `--batch-size`, `--progress` — throughput and feedback controls (`--threads` enables process-pool chunk parallelism for single-file Python-engine convert; also used as concurrent workers for `--recursive` bulk convert)

Reader, error-policy, table, and nested-flatten flags that convert shares with other commands: [Shared CLI options](/commands/shared-options).
