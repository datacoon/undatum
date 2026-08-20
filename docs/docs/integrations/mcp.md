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

Write tools (`deduplicate`, `mask_fields`, `sample_data`) require `confirm=True`
to prevent accidental writes. Pass `flatten_nested=True` to unfold nested fields
onto dotted paths (same as `--flatten-nested` on the CLI).

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

See also the [`mcp`](/commands/mcp) command.
