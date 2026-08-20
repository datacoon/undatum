---
title: "MCP and agent tools"
description: "JSON tools, LangChain, and the MCP stdio server"
---
# AI agent tools and MCP server

undatum exposes its operations to LLM agents through a JSON tool layer that builds
on iterabledata's foundation tools and adds undatum-specific tools (ad-hoc DuckDB
SQL, value frequency, and confirm-gated dedup/mask/sample).

### JSON tools and function-calling schemas

```python
from undatum import tools
from undatum.tools import schemas

# Call a tool directly (returns {"ok": ..., "data"/"error": ...})
result = tools.detect_format("data.csv")
freq = tools.frequency("data.csv", "country")
freq = tools.frequency("nested.jsonl", "capital_city.lat", flatten_nested=True)

# Dispatch by name (handy for agent runtimes)
schemas.call_tool("query_sql", {"path": "data.parquet", "query": "SELECT * FROM data LIMIT 5"})

# Export schemas for LLM function calling
openai_fns = schemas.to_openai_functions()
anthropic_tools = schemas.to_anthropic_tools()
```

Write tools (`convert_file`, `deduplicate`, `mask_fields`, `sample_data`) require `confirm=True`
to prevent accidental writes. Pass `flatten_nested=True` to unfold nested fields
onto dotted paths (same as `--flatten-nested` on the CLI).

### Tool catalog

Foundation tools (from iterabledata) plus undatum extras. `undatum mcp tools` prints the live list.

| Tool | Writes? | Notes |
|------|---------|--------|
| `detect_format` | no | Format and compression for a path |
| `describe_capabilities` | no | Catalog metadata for a format id |
| `read_sample` | no | Bounded sample; optional `redact` |
| `infer_schema` | no | Inferred schema |
| `analyze_dataset` | no | Structure; optional `autodoc` |
| `compute_stats` | no | Column statistics |
| `convert_file` | yes | Requires `confirm=true`; `dry_run` available |
| `generate_documentation` | no | AI dataset documentation |
| `validate_data` | no | Field rules; default mode `stats` |
| `plan_conversion` | no | Declarative convert plan |
| `suggest_transform` | no | Natural-language transform spec |
| `translate_filter` | no | Filter expression → AST |
| `query_sql` | no | DuckDB SQL; file registered as view `data` |
| `frequency` | no | Value counts; optional `table`, `flatten_nested` |
| `deduplicate` | yes | Requires `confirm=true` |
| `mask_fields` | yes | Requires `confirm=true` |
| `sample_data` | yes | Requires `confirm=true` |

### LangChain

```python
from undatum.tools.langchain import get_tools  # pip install "undatum[langchain]"

lc_tools = get_tools()  # list[StructuredTool]
```

### MCP server

Expose the tools to MCP-compatible agents (Claude Desktop, Cursor, etc.) over stdio:

```bash
pip install "undatum[mcp]"

# List the tools the server exposes
undatum mcp tools

# Run the stdio server (wire this command into your MCP client)
undatum mcp serve

# Standalone console entry point (equivalent)
undatum-mcp
```

Copy-paste client config (Cursor `mcp.json` and Claude Desktop) is on the [`mcp`](/commands/mcp) command page.

See also the [`mcp`](/commands/mcp) command.
