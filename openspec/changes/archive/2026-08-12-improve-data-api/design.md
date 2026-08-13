## Context

The Data API is an optional FastAPI + DuckDB layer for read-only access to local (and eventually cloud) files. Phase 1 improved OpenAPI quality and CLI UX. Phase 3 addresses production deployment concerns deferred from the original MVP.

## Goals / Non-Goals

- Goals: optional auth, S3 resources, CORS, config validation, security documentation.
- Non-Goals: row-level CRUD, built-in OAuth/OIDC, multi-tenant isolation.

## Decisions

- Decision: API-key auth is opt-in via CLI flag or environment variable; default remains open on bind address.
- Decision: defer OAuth to reverse-proxy integration; document recommended patterns.
- Decision: S3 paths reuse existing `path_utils` and iterabledata S3 support rather than a parallel client.
- Alternatives considered: mandatory auth in MVP — rejected to preserve local-dev ergonomics.

## Risks / Trade-offs

- API-key in environment variables may leak via process listings — document use of secrets managers.
- S3 latency on cold reads — mitigate with DuckDB view caching at startup (existing behavior).

## Migration Plan

- No breaking changes when new flags are omitted.
- Document new flags in README and `examples/api/api-example.md`.

## Open Questions

- Should API keys support rotation via config file with multiple valid keys?
- Should `api serve` reload config on SIGHUP?
