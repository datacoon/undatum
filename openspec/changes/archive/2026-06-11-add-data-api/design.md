## Context
The Data API feature introduces a web server runtime and additional dependencies. It must remain
optional to avoid burdening CLI-only users and must preserve undatum's streaming, file-first model.

## Goals / Non-Goals
- Goals: provide read-only HTTP access to file-backed datasets with filtering and pagination.
- Goals: reuse DuckDB for query execution over CSV/JSONL/Parquet.
- Non-Goals: row-level CRUD or OLTP-style writes over static files.
- Non-Goals: mandatory installation for all undatum users.

## Decisions
- Decision: ship as an optional extra or plugin package (`undatum[api]` or `undatum-api`).
- Decision: use FastAPI for automatic OpenAPI docs and path generation.
- Decision: translate query params (`field__op`) to SQL with a restricted operator set.
- Alternatives considered: Eve/Flask. Rejected due to MongoDB coupling and weaker typing.

## Risks / Trade-offs
- Risk: resource configs become stale if files change schema. Mitigation: allow re-running
  `api discover` and manual edits to config.
- Risk: large files could lead to expensive queries. Mitigation: enforce `max_limit` defaults.

## Migration Plan
- No migration required for existing CLI workflows.
- Provide examples and documentation for the new API commands and config format.

## Open Questions
- Should `api run` persist a temporary config for reproducibility?
- Should `api serve` support auth in MVP or defer to reverse proxies?
