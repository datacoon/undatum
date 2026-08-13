"""Machine-readable tool schemas for LLM function calling.

Combines iterabledata's foundation tool schemas (``iterable.tools.schemas``)
with undatum-specific tools so a single registry covers both layers.
"""

from __future__ import annotations

import copy
import json
from typing import Any

from iterable.tools import schemas as _iter_schemas

from ._core import (
    deduplicate,
    frequency,
    mask_fields,
    query_sql,
    sample_data,
)

# undatum-only tools layered on top of the iterabledata foundation tools.
UNDATUM_TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "query_sql",
        "description": (
            "Run an ad-hoc DuckDB SQL query over a single file. The file is "
            "registered as the view 'data' (e.g. SELECT * FROM data LIMIT 5)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "query": {"type": "string", "description": "SQL referencing the 'data' view"},
                "limit": {"type": "integer", "default": 1000, "description": "0 returns all rows"},
            },
            "required": ["path", "query"],
        },
    },
    {
        "name": "frequency",
        "description": "Compute the value-frequency distribution for a single top-level field.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "field": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "table": {
                    "type": "string",
                    "description": "Table or sheet name for multi-table sources",
                },
                "flatten_nested": {
                    "type": "boolean",
                    "default": False,
                    "description": "Unfold nested dict / array-of-dict fields onto dotted paths",
                },
            },
            "required": ["path", "field"],
        },
    },
    {
        "name": "deduplicate",
        "description": "Remove duplicate rows and write the result. Writes require confirm=true.",
        "parameters": {
            "type": "object",
            "properties": {
                "input_path": {"type": "string"},
                "output_path": {"type": "string"},
                "keys": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Key fields; omit to dedup on all fields",
                },
                "keep": {"type": "string", "default": "first", "enum": ["first", "last"]},
                "confirm": {"type": "boolean", "default": False},
                "table": {
                    "type": "string",
                    "description": "Table or sheet name for multi-table sources",
                },
                "flatten_nested": {
                    "type": "boolean",
                    "default": False,
                    "description": "Unfold nested dict / array-of-dict fields onto dotted paths",
                },
            },
            "required": ["input_path", "output_path"],
        },
    },
    {
        "name": "mask_fields",
        "description": "Mask sensitive fields and write the result. Writes require confirm=true.",
        "parameters": {
            "type": "object",
            "properties": {
                "input_path": {"type": "string"},
                "output_path": {"type": "string"},
                "fields": {"type": "array", "items": {"type": "string"}},
                "method": {
                    "type": "string",
                    "default": "redact",
                    "enum": ["redact", "hash", "randomize"],
                },
                "salt": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
                "table": {
                    "type": "string",
                    "description": "Table or sheet name for multi-table sources",
                },
                "flatten_nested": {
                    "type": "boolean",
                    "default": False,
                    "description": "Unfold nested dict / array-of-dict fields onto dotted paths",
                },
            },
            "required": ["input_path", "output_path", "fields"],
        },
    },
    {
        "name": "sample_data",
        "description": "Write a random sample of rows. Writes require confirm=true.",
        "parameters": {
            "type": "object",
            "properties": {
                "input_path": {"type": "string"},
                "output_path": {"type": "string"},
                "n": {"type": "integer", "description": "Number of rows"},
                "percent": {"type": "number", "description": "Percentage of rows (0-100)"},
                "confirm": {"type": "boolean", "default": False},
                "table": {
                    "type": "string",
                    "description": "Table or sheet name for multi-table sources",
                },
                "flatten_nested": {
                    "type": "boolean",
                    "default": False,
                    "description": "Unfold nested dict / array-of-dict fields onto dotted paths",
                },
            },
            "required": ["input_path", "output_path"],
        },
    },
]

UNDATUM_TOOL_HANDLERS: dict[str, Any] = {
    "query_sql": query_sql,
    "frequency": frequency,
    "deduplicate": deduplicate,
    "mask_fields": mask_fields,
    "sample_data": sample_data,
}

# Combined registries (foundation tools first, undatum extras last).
TOOL_DEFINITIONS: list[dict[str, Any]] = _iter_schemas.TOOL_DEFINITIONS + UNDATUM_TOOL_DEFINITIONS
TOOL_HANDLERS: dict[str, Any] = {**_iter_schemas.TOOL_HANDLERS, **UNDATUM_TOOL_HANDLERS}


def to_openai_functions() -> list[dict[str, Any]]:
    """Export tool definitions in OpenAI function-calling format."""
    return [
        {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": copy.deepcopy(tool["parameters"]),
            },
        }
        for tool in TOOL_DEFINITIONS
    ]


def to_anthropic_tools() -> list[dict[str, Any]]:
    """Export tool definitions in Anthropic tools format."""
    return [
        {
            "name": tool["name"],
            "description": tool["description"],
            "input_schema": copy.deepcopy(tool["parameters"]),
        }
        for tool in TOOL_DEFINITIONS
    ]


def to_json_schema() -> dict[str, Any]:
    """Export all tool parameter schemas as a JSON Schema document."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "undatum Agent Tools",
        "type": "object",
        "properties": {
            tool["name"]: {
                "type": "object",
                "description": tool["description"],
                **tool["parameters"],
            }
            for tool in TOOL_DEFINITIONS
        },
    }


def call_tool(name: str, arguments: dict[str, Any] | str) -> dict[str, Any]:
    """Invoke a tool by name with JSON arguments (for agent runtimes)."""
    if name in UNDATUM_TOOL_HANDLERS:
        if isinstance(arguments, str):
            arguments = json.loads(arguments)
        return UNDATUM_TOOL_HANDLERS[name](**arguments)
    # Delegate foundation tools to iterabledata (preserves its arg mapping).
    return _iter_schemas.call_tool(name, arguments)


def export_schema_snapshot() -> str:
    """Serialize OpenAI function schemas for snapshot testing."""
    return json.dumps(to_openai_functions(), indent=2, sort_keys=True)
