---
title: "analyze"
description: "undatum analyze command reference"
---
# `analyze`

Analyzes data files and provides human-readable insights about structure, encoding, fields, and data types. With `--autodoc`, automatically generates field descriptions and dataset summaries using AI.

```bash
# Basic analysis
undatum analyze data.jsonl

# With AI-powered documentation
undatum analyze data.jsonl --autodoc

# Using specific AI provider
undatum analyze data.jsonl --autodoc --ai-provider openai --ai-model gpt-4o-mini

# Output to file
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

**Read options** (auto-detected when omitted):
- `--delimiter` — CSV/TSV separator (comma, semicolon, tab, or pipe)
- `--quotechar` — CSV quote character
- `--encoding` — file encoding
- `--engine` — `auto` (default) or `duckdb` for accelerated tabular analysis
- `--table` / `--sheet` — named Excel sheet or multi-table source

**AI Provider Options:**
- `--ai-provider`: Provider id (`openai`, `anthropic`, `gemini`, `azure`, `openrouter`, `ollama`, `lmstudio`, `perplexity`)
- `--ai-model`: Model name (provider-specific)
- `--ai-base-url`: Custom API endpoint URL

**Supported AI Providers:**

1. **OpenAI** (default if `OPENAI_API_KEY` is set)
   ```bash
   export OPENAI_API_KEY=sk-...
   undatum analyze data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini
   ```

2. **Anthropic**
   ```bash
   export ANTHROPIC_API_KEY=sk-ant-...
   undatum analyze data.csv --autodoc --ai-provider anthropic --ai-model claude-3-5-haiku-latest
   ```

3. **Google Gemini**
   ```bash
   export GEMINI_API_KEY=...
   undatum analyze data.csv --autodoc --ai-provider gemini --ai-model gemini-2.0-flash
   ```

4. **Azure OpenAI**
   ```bash
   export AZURE_OPENAI_API_KEY=...
   export AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
   undatum analyze data.csv --autodoc --ai-provider azure --ai-model gpt-4o-mini
   ```

5. **OpenRouter** (unified API for many hosted models)
   ```bash
   export OPENROUTER_API_KEY=sk-or-...
   undatum analyze data.csv --autodoc --ai-provider openrouter --ai-model openai/gpt-4o-mini
   ```

6. **Ollama** (local models, no API key required)
   ```bash
   # Start Ollama and pull a model first: ollama pull llama3.2
   undatum analyze data.csv --autodoc --ai-provider ollama --ai-model llama3.2
   # Or set custom URL: export OLLAMA_BASE_URL=http://localhost:11434
   ```

7. **LM Studio** (local models, OpenAI-compatible API)
   ```bash
   # Start LM Studio and load a model
   undatum analyze data.csv --autodoc --ai-provider lmstudio --ai-model local-model
   # Or set custom URL: export LMSTUDIO_BASE_URL=http://localhost:1234/v1
   ```

8. **Perplexity** (backward compatible, uses `PERPLEXITY_API_KEY`)
   ```bash
   export PERPLEXITY_API_KEY=pplx-...
   undatum analyze data.csv --autodoc --ai-provider perplexity
   ```

**Configuration Methods:**

AI provider can be configured via:
1. **Environment variables** (lowest precedence):
   ```bash
   export UNDATUM_AI_PROVIDER=openai
   export OPENAI_API_KEY=sk-...
   ```

2. **Config file** (medium precedence):
   Create `undatum.yaml` in your project root or `~/.undatum/config.yaml`:
   ```yaml
   ai:
     provider: openai
     api_key: ${OPENAI_API_KEY}  # Can reference env vars
     model: gpt-4o-mini
     timeout: 30
   defaults:
     engine: duckdb
     threads: 4
     progress: true
     encoding: utf8
     # delimiter: ";"   # omit to keep CSV auto-detection
     # quotechar: "'"   # omit to keep iterabledata default '"'
     format_out: json
   ```

   Inspect the resolved values with `undatum config show`. Environment variables
   `UNDATUM_ENGINE`, `UNDATUM_THREADS`, `UNDATUM_PROGRESS`, `UNDATUM_ENCODING`,
   `UNDATUM_DELIMITER`, `UNDATUM_QUOTECHAR`, and `UNDATUM_FORMAT_OUT` are the
   lowest-precedence source.
   Explicit CLI flags always win.

3. **CLI arguments** (highest precedence):
   ```bash
   undatum analyze data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini
   ```
