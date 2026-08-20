---
title: "AI documentation"
description: "Block-based docs, providers, and configuration"
---
# AI-powered documentation

undatum offers several AI documentation paths:

| Command | Best for |
|---------|----------|
| `ai doc` | Block-based docs (general, schema, quality, …) with schema enrichment — **recommended** |
| `doc --autodoc` | Markdown/JSON/YAML dataset documentation with metadata and PII options |
| `analyze --autodoc` | Human-readable analysis report with field descriptions |
| `schema --autodoc` / `schema_bulk --autodoc` | Schema files with AI field descriptions |
| `package create --autodoc` | Frictionless Data Package metadata |

All paths share the same provider configuration (`undatum.yaml`, environment variables, CLI flags). Supported providers: OpenAI, Anthropic, Gemini, Azure OpenAI, OpenRouter, Ollama, LM Studio, Perplexity.

### Quick Examples

```bash
# Recommended: block-based documentation
undatum ai doc data.csv --format json --blocks general,schema,quality

# Legacy analyze autodoc (still supported)
undatum analyze data.csv --autodoc

# Dataset documentation with PII detection
undatum doc data.csv --autodoc --pii-detect --format markdown

# Schema with AI field descriptions
undatum schema data.csv --autodoc --format jsonschema --output schema.json
```

### Configuration File Example

Create `undatum.yaml` in your project:

```yaml
ai:
  provider: openai
  model: gpt-4o-mini
  timeout: 30
```

Or use `~/.undatum/config.yaml` for global settings:

```yaml
ai:
  provider: ollama
  model: llama3.2
  ollama_base_url: http://localhost:11434
```

### Language Support

Generate descriptions in different languages:

```bash
# English (default)
undatum analyze data.csv --autodoc --lang English

# Russian
undatum analyze data.csv --autodoc --lang Russian

# Spanish
undatum analyze data.csv --autodoc --lang Spanish
```

### What Gets Generated

With `--autodoc` enabled, the analyzer will:

1. **Field Descriptions**: Generate clear, concise descriptions for each field explaining what it represents
2. **Dataset Summary**: Provide an overall description of the dataset based on sample data

Example output:

```yaml
tables:
  - id: data.csv
    fields:
      - name: customer_id
        ftype: VARCHAR
        description: "Unique identifier for each customer"
      - name: purchase_date
        ftype: DATE
        description: "Date when the purchase was made"
    description: "Customer purchase records containing transaction details"
```

Provider connection issues: [troubleshooting](/getting-started/troubleshooting#ai-provider-troubleshooting).
Command reference: [`ai`](/commands/ai), [`doc`](/commands/doc).
