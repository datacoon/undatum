---
title: "ai"
description: "undatum ai command reference"
---
# `ai`

AI-assisted workflows backed by iterabledata's `iterable.ai` stack. Subcommands: `doc`, `filter`, `plan`, and `suggest`.

Configure providers via `undatum.yaml`, `~/.undatum/config.yaml`, environment variables, or CLI flags. Supported providers: OpenAI, Anthropic, Gemini, Azure OpenAI, OpenRouter, Ollama, LM Studio, and Perplexity. See [AI documentation](/integrations/ai) and the provider flags under [`analyze`](/commands/analyze).

For block-based documentation with schema enrichment, prefer `ai doc` over legacy `analyze --autodoc` / `schema --autodoc`.

## `ai doc`

Block-based dataset documentation. Default blocks: `general`, `schema`, `quality`, `examples`, `statistics`, `agent_skill`, `codebook`.

```bash
undatum ai doc data.csv
undatum ai doc data.csv --format json --blocks general,schema,quality
undatum ai doc workbook.xlsx --tables Sheet2 --cache --pii-mask-samples
undatum ai doc data.csv --context '{"title": "City register"}'
undatum ai doc data.csv --progress --job-id run-42
undatum ai doc data.csv --sample-size 20 --no-detect-constraints --no-statistics
undatum ai doc data.csv --temperature 0.2 --max-tokens 2048
```

Notable flags: `--format`, `--blocks`, `--tables`, `--cache`, `--pii-mask-samples`, `--context`, `--progress`, `--job-id`, `--sample-size`, `--detect-constraints` / `--no-detect-constraints`, `--statistics` / `--no-statistics`, `--temperature`, `--max-tokens`, `--include-field-descriptions`, `--validate-output`.

## `ai filter`

Translate a natural-language or expression filter. Use `--apply` to stream matching rows.

```bash
undatum ai filter data.csv "active users in New York" --apply
undatum ai filter workbook.xlsx "city is Dushanbe" --table Sheet2 --apply
undatum ai filter "age > 30" data.csv --sample-size 500
undatum ai filter "name == 'Alice'" quoted.csv --quotechar "'"
undatum ai filter "lat > 40" nested.jsonl --flatten-nested --apply
```

`--flatten-nested`, `--max-nested-depth`, and `--keep-nested-parents` apply when unfolding nested fields for schema context and `--apply`. See [shared options](/commands/shared-options).

## `ai plan`

Produce a declarative conversion plan between two paths or format ids. **No conversion is performed.** Both arguments are positional (not `--to`).

```bash
undatum ai plan data.csv data.parquet
undatum ai plan data.json data.geojson --use-llm
```

`--use-llm` adds LLM reasoning on top of catalog metadata.

## `ai suggest`

Suggest a transform spec from a natural-language goal. `--apply` writes transformed rows (confirm unless `--yes`).

```bash
undatum ai suggest data.csv "normalize phone numbers"
undatum ai suggest data.csv "normalize phone numbers" --sample-size 20
undatum ai suggest data.csv "rename id to user_id" --apply --yes --output out.jsonl
```
