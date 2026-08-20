# Domain concepts

Vocabulary used across the CLI, SDK, Data API, and agent tools.

## Surfaces

- **CLI** — `undatum` / `data`. Top-level data commands plus groups (`ai`, `api`, `db`, `package`, `pipeline`, `formats`, `mcp`, `plugins`, `config`, `examples`) and optional `tui` / `web`.
- **SDK** — `undatum.sdk.dataset.Dataset` fluent API. Methods mirror CLI transforms; `convert_many` matches `convert --recursive`.
- **TUI / web** — sampled explorers over the same processors. Not a spreadsheet and not the Data API.
- **Data API** — read-only FastAPI + DuckDB HTTP server over files (`undatum api`).
- **MCP / tools** — JSON tool layer (`undatum.tools`) plus stdio MCP server (`undatum mcp serve`).

## Engines

- **iterabledata** — streaming I/O, format catalog, codecs, cloud URIs, nested flatten.
- **DuckDB** — accelerated `select` / `stats` / `sql` / `frequency` / `uniq` when the format is duckable. `--on-error skip|warn` forces the iterable path.

## Filters vs SQL

- `--filter` (alias `--filter-expr`) — comparison expressions (`==`, `AND`/`OR` or `&&`/`||`). No `LIKE`, `IN`, or regex.
- `undatum sql` — DuckDB SQL over files (view `data`, or stem names for multiple files).
- `undatum db query` — SQL against a database URI.

## Nested data

`--flatten-nested` unfolds dict / array-of-dict fields onto dotted paths. `--max-nested-depth` and `--keep-nested-parents` control depth and whether parent objects remain. Distinct from `undatum flatten`, which rewrites records to a flat key-value shape.

## Packaging and quality

- `validate` — YAML/JSON rule files, or legacy `--rule` names (`common.email`, `ru.org.inn`, …).
- `schema` / `schema-bulk` — inferred schemas; `--format cerberus` replaces deprecated `scheme`.
- `package` — Frictionless `datapackage.json`.
- `doc` / `ai doc` — dataset documentation; `ai doc` is the block-based path.

## Related

- [Architecture](architecture.md)
- [Operations](operations.md)
