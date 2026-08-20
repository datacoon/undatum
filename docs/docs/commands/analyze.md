---
title: "analyze"
description: "undatum analyze command reference"
---
# `analyze`

Analyzes data files and provides human-readable insights about structure, encoding, fields, and data types. With `--autodoc`, automatically generates field descriptions and dataset summaries using AI.

For block-based documentation with a wider provider set (including Anthropic, Gemini, and Azure via iterabledata), prefer [`ai doc`](/commands/ai).

```bash
# Basic analysis
undatum analyze data.jsonl

# With AI-powered documentation
undatum analyze data.jsonl --autodoc

# Using a supported autodoc provider
undatum analyze data.jsonl --autodoc --ai-provider openai --ai-model gpt-4o-mini

# Output to file (format inferred from --output, or set --format-out / --outtype)
undatum analyze data.jsonl --output report.yaml --autodoc

# Named Excel sheet
undatum analyze workbook.xlsx --table Sheet2

# Nested JSONL: unfold dict fields onto dotted paths
undatum analyze nested.jsonl --flatten-nested
```

**Output includes:**
- File type, encoding, compression
- Number of records and fields
- Field types and structure
- Per-field uniqueness statistics (unique count, total count, uniqueness %)
- Table detection for nested data (JSON/XML)
- AI-generated field descriptions (with `--autodoc`)
- AI-generated dataset summary (with `--autodoc`)

**Read and scan options:**
- `--delimiter` — CSV/TSV separator (auto-detected when omitted: comma, semicolon, tab, or pipe)
- `--quotechar` — CSV quote character
- `--encoding` — file encoding
- `--engine` — `auto` (default), `duckdb`, or `iterable`
- `--table` / `--sheet` — named Excel sheet or multi-table source
- `--objects-limit` — max records to scan (default 10000)
- `--no-scan` / `--no-stats` — skip structure scan or uniqueness stats
- `--use-pandas` — use pandas for the analysis path
- `--outtype` / `--format-out` — `text`, `json`, `yaml`, or `markdown` (also inferred from `--output`)
- `--lang` — language for `--autodoc` text (default `English`)

**`--autodoc` providers** (undatum's own stack; unknown ids disable autodoc):

`openai`, `openrouter`, `ollama`, `lmstudio`, `perplexity`

```bash
export OPENAI_API_KEY=sk-...
undatum analyze data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini

export OPENROUTER_API_KEY=sk-or-...
undatum analyze data.csv --autodoc --ai-provider openrouter --ai-model openai/gpt-4o-mini

# Local (no API key)
undatum analyze data.csv --autodoc --ai-provider ollama --ai-model llama3.2
undatum analyze data.csv --autodoc --ai-provider lmstudio --ai-model local-model

export PERPLEXITY_API_KEY=pplx-...
undatum analyze data.csv --autodoc --ai-provider perplexity
```

`--ai-model` and `--ai-base-url` override the provider defaults (`OLLAMA_BASE_URL`, `LMSTUDIO_BASE_URL`).

**Configuration** (lowest to highest): environment (`UNDATUM_AI_PROVIDER`, provider API keys), `undatum.yaml` / `~/.undatum/config.yaml`, then CLI flags. Inspect with `undatum config show`. See [AI documentation](/integrations/ai) and [`config`](/commands/config).
