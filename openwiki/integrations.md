# Integrations

How undatum talks to clouds, databases, agents, and Python.

## Cloud

Extras: `undatum[s3]`, `[gcs]`, `[azure]`, or `[cloud]`. URIs work on CLI paths, SDK `Dataset.read`/`write`, TUI/web open, and Data API resources. Credentials follow the provider SDK (AWS chain, ADC / `GOOGLE_APPLICATION_CREDENTIALS`, Azure env or identity).

## Databases

Optional extras: `postgres`, `mysql`, `mssql`, `clickhouse`. MongoDB and Elasticsearch client libraries are default dependencies; `db load` still does not target those (use `ingest`).

## Data API

`pip install "undatum[api]"`. `discover` / `serve` / `run` / `openapi`. Optional `--api-key` / `UNDATUM_API_KEY`. Implementation: `undatum/cli/api_cli.py`, `undatum/cmds/api.py`.

## MCP and tools

`pip install "undatum[mcp]"`. `undatum mcp serve` (stdio) or `undatum-mcp`. Tool schemas: `undatum/tools/schemas.py`. LangChain: `undatum[langchain]` → `undatum.tools.langchain.get_tools`. Client JSON: `docs/docs/commands/mcp.md`.

## AI providers

`undatum/ai/` — OpenAI, Anthropic, Gemini, Azure, OpenRouter, Ollama, LM Studio, Perplexity. Config merge in `undatum/ai/config.py`. Prefer `ai doc` over legacy `--autodoc` on analyze/schema.

## Plugins

Entry point group `undatum.plugins`. Base classes: `undatum/plugins/base.py`. Transform plugins run via `undatum apply --plugin`. Examples: `examples/plugins/`.

## SDK

`from undatum import Dataset`. Interop: `to_pandas` (bundled), `to_polars` / `to_dask` extras, `as_dataclasses` / `as_pydantic`.

## Related

- [Architecture](architecture.md)
- [Source maps](source-maps.md)
- User docs: `docs/docs/integrations/`
