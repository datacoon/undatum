---
title: "mcp"
description: "undatum mcp command reference"
---
# `mcp`

Expose undatum operations to MCP-compatible agents (Cursor, Claude Desktop, and others) over stdio. Requires `pip install "undatum[mcp]"`.

```bash
pip install "undatum[mcp]"

# List tools the server exposes
undatum mcp tools

# Start the stdio MCP server (this is the command to put in the client config)
undatum mcp serve

# Standalone console entry point (equivalent)
undatum-mcp
```

The server talks **stdio** only. Do not wrap it in a shell that prints other text; the client owns stdin/stdout.

## Client config

### Cursor (`mcp.json`)

```json
{
  "mcpServers": {
    "undatum": {
      "command": "undatum",
      "args": ["mcp", "serve"]
    }
  }
}
```

If `undatum` is not on `PATH` (for example a venv), set `command` to the absolute interpreter or binary and keep `args` as `["mcp", "serve"]`. The `undatum-mcp` script is an equivalent `command` with no `args`.

### Claude Desktop

In Claude Desktop's MCP settings (JSON), use the same `command` / `args` shape:

```json
{
  "mcpServers": {
    "undatum": {
      "command": "undatum",
      "args": ["mcp", "serve"]
    }
  }
}
```

Write tools (`convert_file`, `deduplicate`, `mask_fields`, `sample_data`) require `confirm=true`. Tool catalog: [MCP and agent tools](/integrations/mcp).
