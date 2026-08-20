---
title: "Agents and MCP"
description: "Give LLM agents controlled undatum tools"
---
# Agents and MCP

Give agents controlled dataset tools or add AI assistance to documentation.

## Connect undatum to an MCP client

```bash
pip install "undatum[mcp]"
undatum mcp tools
undatum mcp serve
```

Wire `undatum mcp serve` (stdio) into your MCP client. Confirm-gated writes stay explicit.

## Generate assisted dataset documentation

```bash
undatum ai doc data.csv --format json --blocks general,schema,quality
```

## Python tools without MCP

```python
from undatum.tools import call_tool, export_schemas

print(call_tool("headers", {"path": "data.csv"}))
print(export_schemas())
```

See [MCP](/integrations/mcp), [AI documentation](/integrations/ai), [`ai`](/commands/ai), and the [Python SDK](/integrations/sdk).
