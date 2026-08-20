# OpenWiki quickstart

## What this repository is
`undatum` is a Python command-line tool for data processing and analysis. It centers on the `undatum` / `data` CLI entrypoint, a programmatic `Dataset` API, and supporting subsystems for AI-assisted documentation, a file-backed Data API, database ingestion/querying, plugin loading, and agent/MCP tooling.

The canonical product overview and install examples live in `README.md`; package metadata and supported extras live in `pyproject.toml`.

## Start here
- [Architecture and command surface](architecture.md)
- [Contributor workflow and change process](workflows.md)
- [Domain concepts](domain.md)
- [Operations](operations.md)
- [Integrations](integrations.md)
- [Testing](testing.md)
- [Source maps](source-maps.md)
- Documentation site: `docs/` (Docusaurus; content in `docs/docs/`)
- OpenSpec capabilities: `openspec/specs/`

## High-level map
- `undatum/__main__.py` — process entrypoint; configures logging and dispatches to the Typer app.
- `undatum/core.py` — assembles the main CLI app, top-level commands, plugin registration, and version output.
- `undatum/cli/` — command-group wrappers for data, AI, API, DB, formats, MCP, packages, pipelines, plugins, TUI, and web.
- `undatum/tui/` — optional Textual session (`undatum tui`); `services.py` / `session.py` have no Textual import.
- `undatum/web/` — optional local FastAPI+Jinja2 session (`undatum web`) over the same TUI services.
- `undatum/cmds/` — implementation layer behind the CLI commands.
- `undatum/sdk/dataset.py` — fluent Python API for read/transform/write workflows.
- `undatum/tools/` — tool definitions and handlers for agent integrations.
- `tests/` — behavioral coverage for CLI, API, data commands, SDK, and integrations.
- `openspec/` — spec-driven development workflow and capability specs.
- `WORKFLOW_GUIDE.md` and `README.md` — user-facing contributor and usage documentation.

## User-facing capabilities
The repo currently exposes these major areas, based on the CLI assembly and tests:
- Data commands such as convert, analyze, validate, stats, sql, extract, head/tail/table, query/select/filter/sort, and related transforms in `undatum/cli/data_commands.py`.
- File-backed API commands in `undatum/cli/api_cli.py` and `undatum/cmds/api.py`.
- AI-assisted commands in `undatum/cli/ai_cli.py` and `undatum/ai/`.
- Database commands in `undatum/cli/db_cli.py` and the `undatum/cmds/db_*` modules.
- Format discovery/export in `undatum/cli/formats_cli.py`.
- Pipeline and template execution in `undatum/cli/pipeline_cli.py`.
- Plugin discovery and registration in `undatum/cli/plugins_cli.py` and `undatum/plugins/`.
- MCP and agent tool exports in `undatum/cli/mcp_cli.py`, `undatum/mcp/`, and `undatum/tools/`.
- Optional TUI (`undatum tui`, extra `tui`) and local web UI (`undatum web`, extra `web`) over a bounded sample; not a spreadsheet and not the Data API.
- Programmatic dataset composition in `undatum/sdk/dataset.py`.

## How the CLI boots
1. `undatum.__main__.main()` configures logging and runs the CLI app.
2. `undatum.core.app` builds the Typer application and merges top-level commands from `undatum.cli.data_commands`.
3. Sub-apps are attached for `ai`, `api`, `db`, `examples`, `formats`, `mcp`, `package`, `pipeline`, and `plugins`.
4. Plugin loading happens during app initialization; plugin registration failures are logged but do not crash startup.

Relevant source: `undatum/__main__.py`, `undatum/core.py`, `tests/test_main.py`, `tests/test_core.py`.

## Typical development tasks
- Changing command behavior: start in `undatum/cli/` to understand the arguments and user-facing options, then trace into `undatum/cmds/` for implementation details.
- Changing data-processing internals: inspect `undatum/common/` and the target command module in `undatum/cmds/`.
- Changing the SDK: use `undatum/sdk/dataset.py` and the SDK-related tests in `tests/test_sdk_interop.py`.
- Changing agent/tooling behavior: inspect `undatum/tools/`, `undatum/ai/`, and the MCP modules together.

## Checks that matter most
The repository is configured for a broad quality gate set in `pyproject.toml` and `Makefile`.

Recommended checks when editing core behavior:
- `pytest` or a targeted test module under `tests/`
- `make lint`
- `make type-check`
- `make format-check`
- `make check-all` for broader confidence

## Next page
- [Architecture and command surface](architecture.md)
