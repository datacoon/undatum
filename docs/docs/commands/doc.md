---
title: "doc"
description: "undatum doc command reference"
---
# `doc`

Generates dataset documentation with schema, statistics, and samples in Markdown (default), JSON, YAML, or text. Supports AI-powered descriptions with `--autodoc`. Also available as the `document` alias.

```bash
# Markdown documentation (default)
undatum doc data.jsonl

# JSON documentation with samples
undatum doc data.jsonl --format json --sample-size 5 --output report.json
undatum doc nested.jsonl --flatten-nested --format json

# With AI-powered descriptions
undatum doc data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini
```

**Output includes:**
- Dataset metadata and summary counts
- Schema fields with types and descriptions
- Field-level uniqueness statistics (when available)
- Sample records (configurable via `--sample-size`)

**Extended metadata and PII options:**
- `--semantic-types`: annotate fields with semantic types (requires `metacrafter` CLI)
- `--pii-detect`: detect PII fields and include a PII summary (requires `metacrafter` CLI)
- `--pii-mask-samples`: redact detected PII values in samples (use with `--pii-detect`)

```bash
# Semantic typing and PII summary
undatum doc data.csv --semantic-types --pii-detect --format json

# Mask PII values in samples
undatum doc data.csv --pii-detect --pii-mask-samples --format json
```

**Optional dependencies:**
- `metacrafter` (for semantic types and PII detection)
- `langdetect` (for language detection in metadata)
