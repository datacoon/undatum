"""Agent tool implementations for undatum.

These functions wrap undatum/iterabledata operations as JSON-serializable tools
suitable for LLM function-calling, LangChain, and MCP runtimes.

The foundation tools (detect/describe/read/schema/analyze/stats/convert/doc/
validate/plan/suggest/filter) are re-exported unchanged from iterabledata's
``iterable.tools`` so there is a single source of truth. undatum then layers on a
small set of tools for capabilities it adds on top of the foundation: ad-hoc
DuckDB SQL over files, value-frequency analysis, and confirm-gated dedup/mask/
sample transforms.

Every tool returns the same envelope as iterabledata:
``{"ok": True, "data": ...}`` on success or
``{"ok": False, "error": ..., "code": ...}`` on failure.
"""

from __future__ import annotations

from typing import Any

# Re-export the iterabledata foundation tools unchanged (single source of truth).
from iterable.tools import (
    analyze_dataset,
    compute_stats,
    convert_file,
    describe_capabilities,
    detect_format,
    generate_documentation,
    infer_schema,
    plan_conversion,
    read_sample,
    suggest_transform,
    tool_error,
    tool_success,
    translate_filter_tool,
    validate_data,
)

__all__ = [
    "analyze_dataset",
    "compute_stats",
    "convert_file",
    "deduplicate",
    "describe_capabilities",
    "detect_format",
    "frequency",
    "generate_documentation",
    "infer_schema",
    "mask_fields",
    "plan_conversion",
    "query_sql",
    "read_sample",
    "sample_data",
    "suggest_transform",
    "tool_error",
    "tool_success",
    "translate_filter_tool",
    "validate_data",
]


def _json_safe(value: Any) -> Any:
    """Best-effort coercion of arbitrary values into JSON-serializable forms."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return str(value)


def query_sql(path: str, query: str, limit: int = 1000, **options: Any) -> dict[str, Any]:
    """Run an ad-hoc DuckDB SQL query over a single file.

    The file is registered as the view ``data`` (mirroring ``undatum sql``), so
    queries should reference ``data`` (e.g. ``SELECT * FROM data LIMIT 5``). Up to
    ``limit`` rows are returned inline; pass ``limit=0`` to return all rows.
    """
    from ..common.duckdb_config import (
        create_duckdb_connection,
        get_duckdb_config_from_options,
    )
    from ..utils import normalize_for_json

    if not query or not query.strip():
        return tool_error("query must be a non-empty SQL string", code="invalid_query")

    try:
        conn = create_duckdb_connection(**get_duckdb_config_from_options(options))
    except Exception as exc:
        return tool_error(str(exc), code="duckdb_unavailable")

    try:
        escaped = path.replace("'", "''")
        conn.execute(f"CREATE VIEW \"data\" AS SELECT * FROM '{escaped}'")
        result = conn.execute(query)
        columns = [d[0] for d in result.description]
        rows = result.fetchall() if not limit else result.fetchmany(limit)
        data = [normalize_for_json(dict(zip(columns, row))) for row in rows]
        return tool_success({"columns": columns, "rows": _json_safe(data), "count": len(data)})
    except Exception as exc:
        return tool_error(str(exc), code="sql_failed")
    finally:
        try:
            conn.close()
        except Exception:  # pragma: no cover - defensive cleanup
            pass


def frequency(
    path: str,
    field: str,
    limit: int = 20,
    table: str | None = None,
    flatten_nested: bool = False,
    **options: Any,
) -> dict[str, Any]:
    """Compute the value-frequency distribution for a single field.

    After ``flatten_nested=True``, dotted names such as ``capital_city.lat`` are
    top-level keys.
    """
    from collections import Counter

    from ..common.command_utils import get_iterable_options, iter_command_rows
    from ..common.s3_iterable import open_path
    from ..utils import field_values

    try:
        counter: Counter = Counter()
        total = 0
        opts = {"table": table, "flatten_nested": flatten_nested, **options}
        iterableargs = get_iterable_options(opts)
        source = open_path(path, mode="r", iterableargs=iterableargs)
        try:
            for row in iter_command_rows(source, opts):
                total += 1
                if isinstance(row, dict):
                    found = field_values(row, field)
                    value = found[0] if found else None
                    counter[str(value)] += 1
        finally:
            if hasattr(source, "close"):
                source.close()
        top = counter.most_common(limit if limit else None)
        return tool_success(
            {
                "field": field,
                "total_rows": total,
                "unique_values": len(counter),
                "top_values": [{"value": value, "count": count} for value, count in top],
            }
        )
    except Exception as exc:
        return tool_error(str(exc), code="frequency_failed")


def deduplicate(
    input_path: str,
    output_path: str,
    *,
    keys: list[str] | None = None,
    keep: str = "first",
    confirm: bool = False,
    table: str | None = None,
    flatten_nested: bool = False,
    **options: Any,
) -> dict[str, Any]:
    """Remove duplicate rows and write the result. Writes require confirm=True."""
    if not confirm:
        return tool_error(
            "Deduplication writes a file; pass confirm=True to proceed.",
            code="confirmation_required",
        )
    try:
        from ..cmds.deduplicator import Deduplicator

        options = {
            "key_fields": ",".join(keys) if keys else None,
            "keep": keep,
            "output": output_path,
            "table": table,
            "flatten_nested": flatten_nested,
            **options,
        }
        Deduplicator().dedup(input_path, options)
        return tool_success({"output_path": output_path, "keys": keys, "keep": keep})
    except Exception as exc:
        return tool_error(str(exc), code="dedup_failed")


def mask_fields(
    input_path: str,
    output_path: str,
    fields: str | list[str],
    *,
    method: str = "redact",
    salt: str | None = None,
    confirm: bool = False,
    table: str | None = None,
    flatten_nested: bool = False,
    **options: Any,
) -> dict[str, Any]:
    """Mask sensitive fields and write the result. Writes require confirm=True."""
    if not confirm:
        return tool_error(
            "Masking writes a file; pass confirm=True to proceed.",
            code="confirmation_required",
        )
    try:
        from ..cmds.masker import Masker

        field_list = [fields] if isinstance(fields, str) else list(fields)
        options = {
            "fields": ",".join(field_list),
            "method": method,
            "salt": salt,
            "table": table,
            "flatten_nested": flatten_nested,
            **options,
        }
        Masker().mask(input_path, output_path, options)
        return tool_success({"output_path": output_path, "method": method})
    except Exception as exc:
        return tool_error(str(exc), code="mask_failed")


def sample_data(
    input_path: str,
    output_path: str,
    *,
    n: int | None = None,
    percent: float | None = None,
    confirm: bool = False,
    table: str | None = None,
    flatten_nested: bool = False,
    **options: Any,
) -> dict[str, Any]:
    """Write a random sample of rows. Writes require confirm=True."""
    if not confirm:
        return tool_error(
            "Sampling writes a file; pass confirm=True to proceed.",
            code="confirmation_required",
        )
    if n is None and percent is None:
        return tool_error("Provide either n or percent.", code="invalid_arguments")
    try:
        from ..cmds.sampler import Sampler

        options = {
            "n": n,
            "percent": percent,
            "output": output_path,
            "table": table,
            "flatten_nested": flatten_nested,
            **options,
        }
        Sampler().sample(input_path, options)
        return tool_success({"output_path": output_path, "n": n, "percent": percent})
    except Exception as exc:
        return tool_error(str(exc), code="sample_failed")
