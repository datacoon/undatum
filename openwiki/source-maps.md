# Source maps

Where to look first. CLI wrappers are in `undatum/cli/`; behavior is in `undatum/cmds/` unless noted.

| Surface | Wrapper | Implementation |
|---------|---------|----------------|
| App boot | `undatum/__main__.py`, `undatum/core.py` | Plugin load at import time in `core.py` |
| Data commands | `undatum/cli/data_commands.py` | `undatum/cmds/*.py` (converter, selector, …) |
| AI | `undatum/cli/ai_cli.py` | `undatum/ai/`, iterabledata `iterable.ai` |
| API | `undatum/cli/api_cli.py` | `undatum/cmds/api.py` |
| DB | `undatum/cli/db_cli.py` | `undatum/cmds/db_query.py`, `db_load.py`, ingest in `data_commands` → `cmds` |
| Formats | `undatum/cli/formats_cli.py` | iterabledata catalog |
| Package | `undatum/cli/package_cli.py` | `undatum/cmds/packager.py` |
| Pipeline | `undatum/cli/pipeline_cli.py` | `undatum/cmds/pipeline.py`, `pipeline_templates.py`, `undatum/common/pipeline_parser.py` |
| Plugins | `undatum/cli/plugins_cli.py` | `undatum/plugins/` |
| MCP | `undatum/cli/mcp_cli.py` | `undatum/mcp/server.py`, `undatum/tools/` |
| Config | `undatum/cli/config_cli.py` | `undatum/common/app_config.py`, `undatum/ai/config.py` |
| TUI | `undatum/cli/tui_cli.py` | `undatum/tui/` (`services.py` has no Textual import) |
| Web | `undatum/cli/web_cli.py` | `undatum/web/` |
| SDK | — | `undatum/sdk/dataset.py` |
| Errors | — | `undatum/common/errors.py` |
| Filter expressions | — | `undatum/common/filter.py` |
| Docs site | — | `docs/docs/` (Docusaurus), `docs/sidebars.js` |
| Specs | — | `openspec/specs/` |
| Templates | — | `undatum/templates/*.yml` |
| Recipes | — | `undatum/recipes/`, `examples/recipes/` |

## Related

- [Architecture](architecture.md)
- [Quickstart](quickstart.md)
