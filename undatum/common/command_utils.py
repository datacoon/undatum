"""Shared helpers for command modules.

Centralizes logic that was previously duplicated across the ``undatum.cmds``
modules: iterable option extraction and the DuckDB-with-iterable-fallback
execution pattern.
"""

import logging
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

ITERABLE_OPTIONS_KEYS = [
    "tagname",
    "delimiter",
    "encoding",
    "start_line",
    "page",
    "start_page",
    "prefix_strip",
]


def resolve_csv_delimiter(
    iterableargs: dict | None = None,
    *,
    filename: str | None = None,
    filetype: str | None = None,
) -> str | None:
    """Resolve CSV/TSV delimiter from explicit options or auto-detection.

    Args:
        iterableargs: Iterable reader options (may include ``delimiter``, ``encoding``).
        filename: Path used for auto-detection when delimiter is not set.
        filetype: Optional file type hint (``csv``, ``tsv``, ...).

    Returns:
        Delimiter character, or ``None`` when the source is not CSV-like.
    """
    iterableargs = iterableargs or {}
    delimiter = iterableargs.get("delimiter")
    if delimiter:
        return delimiter

    is_csv = filetype in ("csv", "tsv")
    if filename and not is_csv:
        lower = filename.lower().split("?")[0]
        is_csv = lower.endswith(".csv") or lower.endswith(".tsv")

    if not is_csv or not filename:
        return None

    if filetype == "tsv" or filename.lower().split("?")[0].endswith(".tsv"):
        return "\t"

    import os

    if not os.path.isfile(filename):
        return None

    from ..utils import detect_delimiter

    encoding = iterableargs.get("encoding") or "utf8"
    return detect_delimiter(filename, encoding=encoding)


def apply_iterable_csv_delimiter(iterable, filename: str | None, iterableargs: dict | None) -> None:
    """Apply CSV delimiter on an open iterable.

    ``iterabledata.open_iterable`` passes ``delimiter`` via ``options=``, but
    ``CSVIterable`` only reads it from constructor kwargs. This sets delimiter
    on the iterable after open and re-initializes the reader.
    """
    if not hasattr(iterable, "delimiter"):
        return
    delimiter = resolve_csv_delimiter(iterableargs, filename=filename)
    if not delimiter or getattr(iterable, "delimiter", None) == delimiter:
        return
    iterable.delimiter = delimiter
    if hasattr(iterable, "reset"):
        iterable.reset()


def duckdb_read_csv_options(
    delimiter: str | None = None, *, ignore_errors: bool = True, all_varchar: bool = False
) -> str:
    """Build DuckDB ``read_csv`` option suffix (leading comma included)."""
    parts: list[str] = []
    if ignore_errors:
        parts.append("ignore_errors=true")
    if all_varchar:
        parts.append("all_varchar=true")
    if delimiter:
        escaped = delimiter.replace("\\", "\\\\").replace("'", "''")
        parts.append(f"delim='{escaped}'")
        parts.append('quote=\'"\'')
        parts.append("strict_mode=false")
    if not parts:
        return ""
    return ", " + ", ".join(parts)


def duckdb_read_csv_expr(
    filename: str,
    delimiter: str | None = None,
    *,
    ignore_errors: bool = True,
    sample_size: int | None = None,
    all_varchar: bool = False,
) -> str:
    """Build a DuckDB ``read_csv('path', ...)`` expression."""
    escaped = filename.replace("'", "''")
    opts = duckdb_read_csv_options(
        delimiter, ignore_errors=ignore_errors, all_varchar=all_varchar
    )
    if sample_size is not None:
        opts += f", sample_size={sample_size}"
    return f"read_csv('{escaped}'{opts})"


def duckdb_read_expr(
    filename: str,
    filetype: str | None = None,
    iterableargs: dict | None = None,
    *,
    all_varchar: bool = False,
) -> str:
    """Build a DuckDB read expression for CSV, JSON, or Parquet files."""
    from ..utils import get_file_type

    source_type = filetype or get_file_type(filename) or "csv"
    iterableargs = iterableargs or {}

    if source_type == "csv":
        delimiter = resolve_csv_delimiter(iterableargs, filename=filename, filetype=source_type)
        return duckdb_read_csv_expr(filename, delimiter, all_varchar=all_varchar)
    if source_type in ("json", "jsonl"):
        escaped = filename.replace("'", "''")
        return f"read_json_auto('{escaped}')"
    if source_type == "parquet":
        escaped = filename.replace("'", "''")
        return f"read_parquet('{escaped}')"
    raise ValueError(f"Unsupported file type for DuckDB: {source_type}")


def quote_sql_identifier(name: str) -> str:
    """Quote a SQL identifier for DuckDB."""
    name = name.strip()
    if name.startswith('"') and name.endswith('"'):
        return name
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def get_iterable_options(options: dict) -> dict:
    """Extract iterable-specific options from options dictionary.

    Maps ``start_page`` (undatum CLI name) to ``page`` (iterabledata name).

    Args:
        options: Full command options dictionary.

    Returns:
        Dictionary with only the keys consumed by ``iterabledata`` readers.
    """
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options and options[k] is not None:
            out[k] = options[k]
    if "start_page" in out and "page" not in out:
        out["page"] = out.pop("start_page")
    elif "start_page" in out:
        del out["start_page"]
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
