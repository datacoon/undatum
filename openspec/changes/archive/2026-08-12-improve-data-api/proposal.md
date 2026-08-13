# Change: Improve Data API hardening

## Why

The initial Data API MVP (`add-data-api`) ships read-only file-backed endpoints with basic OpenAPI support. Phase 1 improvements added typed OpenAPI schemas, pagination envelopes, static export, and UX fixes. Phase 3 covers security, cloud storage, and operational hardening needed before broader production use.

## What Changes

- Optional API-key authentication (`--api-key` / `UNDATUM_API_KEY` environment variable)
- S3-backed resource paths in API config (reuse `common/path_utils.py` S3 patterns)
- Optional CORS configuration (`--cors-origins`) for browser clients
- JSON Schema validation for API config at `discover` and `serve` time
- Security model documentation (reverse-proxy vs built-in auth guidance)
- Update `openspec/specs/data-api/spec.md` Purpose and envelope/OpenAPI requirements

## Impact

- Affected specs: `data-api`
- Affected code: `undatum/cmds/api.py`, `undatum/cli/api_cli.py`, `docs/`, `README.md`
- Non-breaking for existing deployments when auth and CORS remain disabled (default)
