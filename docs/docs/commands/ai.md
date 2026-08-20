---
title: "ai"
description: "undatum ai command reference"
---
# `ai`

AI-assisted workflows backed by iterabledata's `iterable.ai` stack. Subcommands: `doc`, `filter`, `plan`, and `suggest`. Supports OpenAI, Anthropic, Gemini, Azure OpenAI, OpenRouter, Ollama, LM Studio, and Perplexity — configure via `undatum.yaml`, environment variables, or CLI flags (see [AI Provider Options](/commands/analyze) under `analyze`).

```bash
# Block-based dataset documentation (markdown default). Default blocks:
# general, schema, quality, examples, statistics, agent_skill, codebook
undatum ai doc data.csv

# JSON output with selected blocks; schema enrichment maps LLM field names
# to canonical columns and fills SDMX-style hints (e.g. FREQ:Frequency)
undatum ai doc data.csv --format json --blocks general,schema,quality

# Natural-language filter translation (use --apply to stream matching rows)
undatum ai filter data.csv "active users in New York" --apply
undatum ai filter workbook.xlsx "city is Dushanbe" --table Sheet2 --apply
undatum ai filter "age > 30" data.csv --sample-size 500
undatum ai filter "name == 'Alice'" quoted.csv --quotechar "'"
undatum ai filter "lat > 40" nested.jsonl --flatten-nested --apply
undatum ai filter "lat > 40" nested.jsonl --flatten-nested --max-nested-depth 2 --no-keep-nested-parents

# Conversion planning and transform suggestions
undatum ai plan data.csv --to parquet
undatum ai suggest data.csv "normalize phone numbers"
undatum ai suggest data.csv "normalize phone numbers" --sample-size 20

# Apply a suggested transform spec (JSONL; --yes skips the confirm prompt)
undatum ai suggest data.csv "rename id to user_id" --apply --yes --output out.jsonl

# Document a named Excel sheet; cache and mask PII samples
undatum ai doc workbook.xlsx --tables Sheet2 --cache --pii-mask-samples
undatum ai doc data.csv --context '{"title": "City register"}'
undatum ai doc data.csv --progress
undatum ai doc data.csv --sample-size 20 --no-detect-constraints --no-statistics
undatum ai doc data.csv --temperature 0.2 --max-tokens 2048
undatum ai doc data.csv --job-id run-42 --progress
```

For block-based documentation with schema enrichment, prefer `ai doc` over legacy `analyze --autodoc` / `schema --autodoc` workflows.
