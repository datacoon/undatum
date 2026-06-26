"""Agent tool wrappers for undatum returning JSON-serializable results.

Re-exports iterabledata's foundation tools and adds undatum-specific tools
(DuckDB SQL, value frequency, and confirm-gated dedup/mask/sample). Use
:mod:`undatum.tools.schemas` for LLM function-calling definitions and
:func:`undatum.tools.langchain.get_tools` for LangChain integration.
"""

from __future__ import annotations

from ._core import (
    analyze_dataset,
    compute_stats,
    convert_file,
    deduplicate,
    describe_capabilities,
    detect_format,
    frequency,
    generate_documentation,
    infer_schema,
    mask_fields,
    plan_conversion,
    query_sql,
    read_sample,
    sample_data,
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
