# Architecture and command surface

## Repository shape
`undatum` is organized around a thin entrypoint, a central CLI assembler, grouped Typer sub-apps, and a large implementation layer under `undatum/cmds/`.

The design is visible in `undatum/__main__.py`, `undatum/core.py`, and the command-group modules in `undatum/cli/`.

## Boot sequence
- `undatum.__main__.main()` is the process entrypoint for `undatum` and `python -m undatum`.
- It configures logging, invokes the Typer app, and converts known exceptions into user-facing exit codes.
- `undatum.core.app` is the main Typer application.
- `undatum.core` merges the top-level data commands, adds grouped sub-apps, and sorts registered commands for predictable help output.
- Plugin loading happens after the app is assembled so plugin command registration can extend the CLI.

Source evidence: `undatum/__main__.py`, `undatum/core.py`, `tests/test_main.py`, `tests/test_core.py`.

## Command groups
The current top-level shape is assembled in `undatum/core.py`:
- Top-level data commands are merged directly from `undatum.cli.data_commands.data_app`.
- Named groups are attached for `ai`, `api`, `package`, `pipeline`, `db`, `examples`, `formats`, `mcp`, and `plugins`.
- Optional session UIs are top-level commands: `tui` (`undatum/cli/tui_cli.py`) and `web` (`undatum/cli/web_cli.py`). Both call `undatum.tui.services.TuiServices`; the web extra must not import Textual screens.

Representative command-group modules:
- `undatum/cli/data_commands.py` — primary data workflows such as convert, extract, uniq, diff, filter, query, select, sort, sql, stats, validate, and transform-style operations.
- `undatum/cli/api_cli.py` — file-backed API discovery, serve, run, and OpenAPI export.
- `undatum/cli/plugins_cli.py` — plugin listing and inspection.
- `undatum/cli/mcp_cli.py` — MCP server support.
- `undatum/cli/package_cli.py` — data package generation.
- `undatum/cli/pipeline_cli.py` — pipeline execution and templates.

## Implementation layers
The codebase separates user-facing CLI wiring from the actual processing logic:
- `undatum/cli/` defines command arguments and forwards work.
- `undatum/cmds/` contains the operational classes and helpers that implement conversions, analysis, extraction, ingestion, querying, and other workflows.
- `undatum/common/` holds shared concerns such as error handling, path utilities, chunked I/O, filtering, schema helpers, and S3/parallel support.
- `undatum/formats/` contains format-specific helpers.
- `undatum/plugins/` implements plugin discovery and registration.
- `undatum/sdk/` provides the Python `Dataset` API.
- `undatum/tui/` is the optional terminal session (Textual screens plus Textual-free services).
- `undatum/web/` is the optional localhost browser session (FastAPI + Jinja2 + HTMX).
- `undatum/tools/` provides agent-tool exports and handlers.

## Why this structure exists
The repository is intentionally layered so the same capabilities can be exposed through multiple surfaces:
- CLI commands for interactive shell use.
- Optional TUI and local web UI for sampled exploration (same processors, different views).
- A programmatic SDK for pipelines and application code.
- Agent/MCP tools for model-driven workflows.
- Format and plugin subsystems to expand supported data sources without rewriting the core CLI.

That pattern is reinforced by the tests in `tests/`, which validate both command wiring and lower-level processing behavior.

## Change guidance for future agents
When changing core behavior:
- Start by locating the command wrapper in `undatum/cli/`.
- Trace into the corresponding `undatum/cmds/` class or helper.
- Check `tests/test_*.py` for existing behavioral coverage before editing.
- Be careful with startup code in `undatum/core.py`; plugin loading and command registration happen at import time.
- Re-run focused tests for the affected command group before broader checks.

## Related docs
- [Quickstart](quickstart.md)
- [Contributor workflow and change process](workflows.md)
