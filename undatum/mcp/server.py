"""Model Context Protocol server exposing undatum agent tools.

Surfaces undatum's tool layer (iterabledata foundation tools plus undatum
extras) over MCP stdio transport, mirroring iterabledata's ``iterable-mcp``
server but with undatum's richer command set. Each tool returns a JSON string
envelope (``{"ok": ..., ...}``).
"""

from __future__ import annotations

import json
from typing import Any

from ..tools import schemas


def _call(name: str, arguments: dict[str, Any]) -> str:
    return json.dumps(schemas.call_tool(name, arguments))


def _register_tools(mcp: Any) -> None:
    """Register undatum tools on a FastMCP instance."""

    # --- iterabledata foundation tools ---

    @mcp.tool()
    def detect_format(path: str) -> str:
        """Detect data format and compression for a file."""
        return _call("detect_format", {"path": path})

    @mcp.tool()
    def describe_capabilities(format_id: str) -> str:
        """Describe format metadata and capabilities."""
        return _call("describe_capabilities", {"format_id": format_id})

    @mcp.tool()
    def read_sample(path: str, n: int = 10, redact: bool = False) -> str:
        """Read a bounded sample of rows from a data file."""
        return _call("read_sample", {"path": path, "n": n, "redact": redact})

    @mcp.tool()
    def infer_schema(path: str) -> str:
        """Infer schema for a data file."""
        return _call("infer_schema", {"path": path})

    @mcp.tool()
    def analyze_dataset(path: str, autodoc: bool = False) -> str:
        """Analyze dataset structure; optional AI documentation."""
        return _call("analyze_dataset", {"path": path, "autodoc": autodoc})

    @mcp.tool()
    def compute_stats(path: str) -> str:
        """Compute column statistics for a data file."""
        return _call("compute_stats", {"path": path})

    @mcp.tool()
    def convert_file(
        input_path: str,
        output_path: str,
        confirm: bool = False,
        dry_run: bool = False,
    ) -> str:
        """Convert between formats. Writes require confirm=True."""
        return _call(
            "convert_file",
            {
                "input_path": input_path,
                "output_path": output_path,
                "confirm": confirm,
                "dry_run": dry_run,
            },
        )

    @mcp.tool()
    def generate_documentation(
        path: str, provider: str = "openai", doc_format: str = "json"
    ) -> str:
        """Generate AI-powered dataset documentation."""
        return _call(
            "generate_documentation",
            {"path": path, "provider": provider, "doc_format": doc_format},
        )

    @mcp.tool()
    def validate_data(path: str, rules: dict[str, list[str]], mode: str = "stats") -> str:
        """Validate rows against field rules; returns stats by default."""
        return _call("validate_data", {"path": path, "rules": rules, "mode": mode})

    @mcp.tool()
    def plan_conversion(source: str, target: str, use_llm: bool = False) -> str:
        """Produce a declarative conversion plan without writing files."""
        return _call("plan_conversion", {"source": source, "target": target, "use_llm": use_llm})

    @mcp.tool()
    def suggest_transform(path: str, goal: str) -> str:
        """Suggest a declarative transform spec (requires AI extras)."""
        return _call("suggest_transform", {"path": path, "goal": goal})

    @mcp.tool()
    def translate_filter(expression: str) -> str:
        """Translate a filter expression into a validated AST (DSL parsing; no LLM by default)."""
        return _call("translate_filter", {"expression": expression})

    # --- undatum-specific tools ---

    @mcp.tool()
    def query_sql(path: str, query: str, limit: int = 1000) -> str:
        """Run an ad-hoc DuckDB SQL query over a file (registered as the 'data' view)."""
        return _call("query_sql", {"path": path, "query": query, "limit": limit})

    @mcp.tool()
    def frequency(path: str, field: str, limit: int = 20, table: str | None = None) -> str:
        """Compute the value-frequency distribution for a top-level field."""
        return _call("frequency", {"path": path, "field": field, "limit": limit, "table": table})

    @mcp.tool()
    def deduplicate(
        input_path: str,
        output_path: str,
        keys: list[str] | None = None,
        keep: str = "first",
        confirm: bool = False,
        table: str | None = None,
    ) -> str:
        """Remove duplicate rows and write the result. Writes require confirm=True."""
        return _call(
            "deduplicate",
            {
                "input_path": input_path,
                "output_path": output_path,
                "keys": keys,
                "keep": keep,
                "confirm": confirm,
                "table": table,
            },
        )

    @mcp.tool()
    def mask_fields(
        input_path: str,
        output_path: str,
        fields: list[str],
        method: str = "redact",
        salt: str | None = None,
        confirm: bool = False,
        table: str | None = None,
    ) -> str:
        """Mask sensitive fields and write the result. Writes require confirm=True."""
        return _call(
            "mask_fields",
            {
                "input_path": input_path,
                "output_path": output_path,
                "fields": fields,
                "method": method,
                "salt": salt,
                "confirm": confirm,
                "table": table,
            },
        )

    @mcp.tool()
    def sample_data(
        input_path: str,
        output_path: str,
        n: int | None = None,
        percent: float | None = None,
        confirm: bool = False,
        table: str | None = None,
    ) -> str:
        """Write a random sample of rows. Writes require confirm=True."""
        return _call(
            "sample_data",
            {
                "input_path": input_path,
                "output_path": output_path,
                "n": n,
                "percent": percent,
                "confirm": confirm,
                "table": table,
            },
        )


def create_mcp_server(name: str = "undatum") -> Any:
    """Create a FastMCP server with undatum tools registered."""
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as err:
        raise ImportError(
            "mcp package is required. Install with: pip install undatum[mcp]"
        ) from err

    mcp = FastMCP(name)
    _register_tools(mcp)
    return mcp


def main() -> None:
    """Entry point for the ``undatum-mcp`` console script (stdio transport)."""
    create_mcp_server().run()


if __name__ == "__main__":
    main()
