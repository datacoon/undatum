---
title: "schema_bulk"
description: "undatum schema_bulk command reference"
---
# `schema_bulk`

Extracts schemas from multiple files at once using a glob pattern or directory path. Either extracts distinct unique schemas (`--mode distinct`, default) or one schema per file (`--mode perfile`).

```bash
# Distinct schemas across all CSV files in a directory
undatum schema_bulk "data/*.csv" --output schemas/

# One schema per file, JSON Schema format
undatum schema_bulk data/ --mode perfile --format jsonschema --output schemas/

# With AI-powered field documentation
undatum schema_bulk "data/*.jsonl" --autodoc --output schemas/
```
