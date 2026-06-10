# Change: Add file-backed Data API commands

## Why
Users need a fast, low-friction way to expose datasets and pipeline outputs over HTTP without
running a full database. A read-mostly Data API fits undatum's streaming and DuckDB strengths
while keeping the CLI core lightweight.

## What Changes
- Add optional Data API capability as a plugin/extra dependency.
- Introduce `undatum api discover`, `undatum api serve`, and `undatum api run` commands.
- Define a YAML/JSON API config describing resources, schemas, and query options.
- Serve read-only HTTP endpoints backed by DuckDB with OpenAPI documentation.

## Impact
- Affected specs: `data-api`
- Affected code: `undatum/cmds/`, `undatum/core.py`, new API modules, packaging extras
