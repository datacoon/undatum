---
title: "mcp"
description: "undatum mcp command reference"
---
# `mcp`

Expose undatum operations to MCP-compatible agents (Cursor, Claude Desktop, etc.) over stdio. Requires `pip install "undatum[mcp]"`. See also [AI Agent Tools and MCP Server](/integrations/mcp).

```bash
# List tools exposed to agents
undatum mcp tools

# Start the stdio MCP server
undatum mcp serve

# Standalone entry point (equivalent)
undatum-mcp
```
