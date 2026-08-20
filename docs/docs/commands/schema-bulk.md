---
title: "schema-bulk"
description: "undatum schema-bulk command reference"
---
# `schema-bulk`

Extracts schemas from multiple files at once using a glob pattern or directory path. Either extracts distinct unique schemas (`--mode distinct`, default) or one schema per file (`--mode perfile`).

The CLI command is **`schema-bulk`** (hyphen), not `schema_bulk`.

`--autodoc` uses the same five providers as [`analyze`](/commands/analyze): openai, openrouter, ollama, lmstudio, perplexity.

```bash
# Distinct schemas across all CSV files in a directory
undatum schema-bulk "data/*.csv" --output schemas/

# One schema per file, JSON Schema format
undatum schema-bulk data/ --mode perfile --format jsonschema --output schemas/

# With AI-powered field documentation
undatum schema-bulk "data/*.jsonl" --autodoc --output schemas/
```

**Key options:** `--mode distinct|perfile`, `--format` (`yaml`, `json`, `cerberus`, `jsonschema`, `avro`, `parquet`), `--output` (directory), `--autodoc`, `--lang`, `--engine auto|duckdb|iterable`. See [`schema`](/commands/schema) for single-file extraction.
