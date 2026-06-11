"""Shared helpers for command modules.

Centralizes logic that was previously duplicated across the ``undatum.cmds``
modules: iterable option extraction and the DuckDB-with-iterable-fallback
execution pattern.
"""

import logging
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

ITERABLE_OPTIONS_KEYS = ["tagname", "delimiter", "encoding", "start_line", "page"]


def get_iterable_options(options: dict) -> dict:
    """Extract iterable-specific options from options dictionary.

    Args:
        options: Full command options dictionary.

    Returns:
        Dictionary with only the keys consumed by ``iterabledata`` readers.
    """
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


def run_with_duckdb_fallback(
    operation: str,
    duckdb_fn: Callable[[], T],
    iterable_fn: Callable[[], T],
    engine: Optional[str] = "duckdb",
) -> T:
    """Run a DuckDB implementation, falling back to the iterable engine on failure.

    Args:
        operation: Operation name used in the fallback log message (e.g. "count").
        duckdb_fn: Zero-argument callable implementing the DuckDB path.
        iterable_fn: Zero-argument callable implementing the iterable path.
        engine: Detected engine; the DuckDB path is only attempted when "duckdb".

    Returns:
        The result of whichever implementation ran.
    """
    if engine == "duckdb":
        try:
            return duckdb_fn()
        except Exception as e:  # noqa: BLE001 - any DuckDB failure falls back
            logging.warning(f"DuckDB {operation} failed, falling back to iterable: {e}")
    return iterable_fn()
