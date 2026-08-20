---
title: "api"
description: "undatum api command reference"
---
# `api`

Serves files as a read-only HTTP API (FastAPI + DuckDB). Supports CSV, JSON/JSONL, and Parquet files. Requires the `api` extra:

```bash
pip install "undatum[api]"
```

`discover` works without the extra; `serve`, `run`, and `openapi` require it and show an install hint if missing.

**Subcommands:**

| Command | Description |
|---------|-------------|
| `api discover` | Infer schema from files and write a YAML/JSON API config |
| `api serve` | Start the HTTP server from a config file |
| `api run` | Discover in memory and serve immediately (no config file) |
| `api openapi` | Export OpenAPI 3.x schema without starting the server |

```bash
# Discover resources and serve in one step
undatum api run data.csv

# Generate an API config (YAML) for multiple files
undatum api discover data.csv other.parquet --output api.yml

# Serve from a config file
undatum api serve --config api.yml --host 127.0.0.1 --port 8000

# Optional API key (or UNDATUM_API_KEY) and CORS for browser clients
undatum api serve --config api.yml --api-key "$UNDATUM_API_KEY" --cors-origins https://app.example.com

# Export OpenAPI schema to a file
undatum api openapi --config api.yml --output openapi.json
undatum api openapi --config api.yml --output openapi.yaml --format yaml
```

On startup, the server prints a banner with the base URL, resource endpoints, and links to `/docs`, `/redoc`, and `/openapi.json`.

**Endpoints:**

- `GET /` — API discovery (resource list and documentation links)
- `GET /{resource}` — list records with filtering, sorting, and pagination
- `GET /{resource}/{pk}` — fetch a single record (when a single-column primary key is inferred or configured)
- `GET /docs` — interactive Swagger UI
- `GET /redoc` — ReDoc documentation
- `GET /openapi.json` — OpenAPI schema

**List response format:**

```json
{
  "data": [{ "id": 1, "name": "Alice" }],
  "pagination": { "limit": 50, "offset": 0, "count": 1, "total": 100 }
}
```

The `total` field is included only when `include_total=true` is passed (may be slower on large files).

See [Data API security](/integrations/data-api) for API keys, CORS, reverse-proxy guidance, and cloud (`s3://` / `gs://` / `az://`) resource paths.

**Query parameters:**

- **Filters:** `field__op=value` where `op` is one of `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `like` (or `field=value` as shorthand for `eq`)
- **Sorting:** `order_by=field` with `order_dir=asc|desc`, or `sort=field` / `sort=-field` (descending alias)
- **Pagination:** `limit` (default 50, max 1000), `offset`, and optional `include_total=true`

**Discover options:**

- `--output` — write config to a file (stdout if omitted)
- `--format-in` — override format detection (`csv`, `json`, `jsonl`, `parquet`)
- `--config-format` — `yaml` or `json`
- `--default-limit`, `--max-limit` — pagination defaults for generated config
- `--allowed-ops` — comma-separated filter operators

**Serve / run options:**

- `--host` — bind address (default: `127.0.0.1`)
- `--port` — bind port (default: `8000`)

**Example requests:**

```bash
curl "http://127.0.0.1:8000/sales?limit=10"
curl "http://127.0.0.1:8000/sales?amount__gt=100&order_by=sold_at&order_dir=desc"
curl "http://127.0.0.1:8000/sales/42"
```

**Security notes:**

- The API is read-only; no mutations are possible
- Binds to `127.0.0.1` by default; there is no built-in authentication, so put it behind a reverse proxy with auth before exposing it publicly

See also: [examples/api/api-example.md](https://github.com/datenoio/undatum/blob/main/examples/api/api-example.md) and [Data API operations](/integrations/data-api).
