"""Sort command module - sort rows by columns."""

import logging
import sys
import uuid

from ..common.command_utils import ITERABLE_OPTIONS_KEYS, get_iterable_options  # noqa: F401
from ..common.duckdb_config import create_duckdb_connection, get_duckdb_config_from_options
from ..common.engine_selector import detect_engine
from ..common.errors import (
    FileNotFoundError,
    FormatError,
    PermissionError,
    ValidationError,
    find_similar_files,
)
from ..common.external_sort import DEFAULT_RUN_SIZE, external_merge_sort
from ..common.iterable import DataWriter
from ..common.path_utils import validate_file_path
from ..common.s3_iterable import open_path as open_iterable
from ..utils import get_file_type, get_option

# Threshold for using external sort (in-memory vs external merge)
EXTERNAL_SORT_THRESHOLD = 100000  # items


def _normalize_for_json(obj):
    """Convert non-JSON-serializable types to JSON-serializable ones.

    Recursively converts UUID objects and other non-serializable types to strings.

    Args:
        obj: Object to normalize (can be dict, list, or primitive type)

    Returns:
        Normalized object with non-serializable types converted to strings
    """
    if isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: _normalize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_normalize_for_json(item) for item in obj]
    else:
        return obj


def _get_sort_key(item, sort_fields, numeric_fields=None):
    """Get sort key for an item."""
    if not isinstance(item, dict):
        return item

    numeric_set = set(numeric_fields or [])
    keys = []
    for field in sort_fields:
        value = item.get(field)
        if field in numeric_set:
            # Try to convert to number for numeric sort
            try:
                keys.append(float(value) if value is not None else float("-inf"))
            except (ValueError, TypeError):
                keys.append(value)
        else:
            keys.append(value)
    return tuple(keys)


class Sorter:
    """Sorter command handler - sort rows."""

    def __init__(self):
        pass

    def sort(self, fromfile, options=None):
        """Sort rows by one or more columns."""
        if options is None:
            options = {}

        # Validate input file exists and is readable
        try:
            validate_file_path(fromfile, check_read=True)
        except FileNotFoundError as e:
            suggestions = find_similar_files(fromfile)
            raise FileNotFoundError(fromfile, suggestions) from e
        except PermissionError as e:
            raise PermissionError(fromfile, operation="read") from e

        logging.debug("Processing %s", fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, "filetype")
        engine = get_option(options, "engine") or "auto"
        by_fields = get_option(options, "by")
        descending = get_option(options, "desc") or False
        numeric_fields = get_option(options, "numeric")
        to_file = get_option(options, "output")

        if not by_fields:
            raise ValidationError("Sort fields (--by) are required", field="by")

        # Parse sort fields
        sort_fields = [f.strip() for f in by_fields.split(",")]
        numeric_set = {f.strip() for f in numeric_fields.split(",")} if numeric_fields else set()

        detected_engine = detect_engine(fromfile, engine, filetype, operation="sort")

        # Initialize items for output handling
        items = []

        if detected_engine == "duckdb":
            try:
                # Get DuckDB configuration from options
                duckdb_config = get_duckdb_config_from_options(options)
                conn = create_duckdb_connection(**duckdb_config)

                # Determine input format and build appropriate read expression
                source_type = filetype or get_file_type(fromfile) or "csv"
                if source_type == "csv":
                    read_expr = f"read_csv_auto('{fromfile}', all_varchar=true)"
                elif source_type in ("json", "jsonl"):
                    read_expr = f"read_json_auto('{fromfile}')"
                elif source_type == "parquet":
                    read_expr = f"read_parquet('{fromfile}')"
                else:
                    raise ValueError(f"Unsupported file type for DuckDB: {source_type}")

                # Build ORDER BY clause
                order_by_parts = []
                for field in sort_fields:
                    # Handle numeric fields if specified
                    if field in numeric_set:
                        # Cast to numeric for proper sorting
                        order_by_parts.append(
                            f"CAST({field} AS DOUBLE) {'DESC' if descending else 'ASC'}"
                        )
                    else:
                        order_by_parts.append(f"{field} {'DESC' if descending else 'ASC'}")

                order_by = ", ".join(order_by_parts)
                query = f"SELECT * FROM {read_expr} ORDER BY {order_by}"

                # Determine output format
                if to_file:
                    to_type = get_file_type(to_file) or "csv"
                    # Use COPY for file output
                    if to_type == "csv":
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT CSV, HEADER)"
                        conn.execute(copy_query)
                        logging.info("sort: completed using DuckDB")
                        conn.close()
                        return
                    elif to_type in ("json", "jsonl"):
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT JSON)"
                        conn.execute(copy_query)
                        logging.info("sort: completed using DuckDB")
                        conn.close()
                        return
                    elif to_type == "parquet":
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT PARQUET)"
                        conn.execute(copy_query)
                        logging.info("sort: completed using DuckDB")
                        conn.close()
                        return

                # For stdout or unsupported output format, read into memory
                relation = conn.execute(query)
                column_names = relation.columns
                rows = relation.fetchall()
                items = [dict(zip(column_names, row)) for row in rows]
                conn.close()
                logging.info(f"sort: completed using DuckDB, {len(items)} records")
            except Exception as e:
                logging.warning(f"DuckDB sort failed, falling back to iterable: {e}")
                detected_engine = "iterable"

        if detected_engine == "iterable":
            reverse = descending
            key_fn = lambda x: _get_sort_key(x, sort_fields, numeric_set)
            low_memory = bool(get_option(options, "low_memory"))
            run_size = int(get_option(options, "run_size") or DEFAULT_RUN_SIZE)
            temp_dir = get_option(options, "temp_dir") or get_option(options, "duckdb_temp_dir")

            iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
            try:
                if low_memory:
                    logging.info("sort: using external merge sort (--low-memory, run_size=%s)", run_size)
                    sorted_iter = external_merge_sort(
                        iterable,
                        key_fn,
                        reverse=reverse,
                        run_size=run_size,
                        temp_dir=temp_dir,
                    )
                    _write_sorted_stream(sorted_iter, to_file)
                    return

                buffered = []
                for item in iterable:
                    buffered.append(item)
                    if len(buffered) > EXTERNAL_SORT_THRESHOLD:
                        logging.info(
                            "sort: exceeded %d records, using external merge sort",
                            EXTERNAL_SORT_THRESHOLD,
                        )

                        def _record_stream(first=buffered, rest=iterable):
                            yield from first
                            yield from rest

                        sorted_iter = external_merge_sort(
                            _record_stream(),
                            key_fn,
                            reverse=reverse,
                            run_size=run_size,
                            temp_dir=temp_dir,
                        )
                        _write_sorted_stream(sorted_iter, to_file)
                        return

                buffered.sort(key=key_fn, reverse=reverse)
                items = buffered
                logging.debug("sort: sorted %d records in memory", len(items))
            finally:
                iterable.close()

        if to_file:
            to_type = get_file_type(to_file)
            if not to_type:
                raise FormatError(to_file, to_file.rsplit(".", 1)[-1])
            out = open(to_file, "w", encoding="utf8")
        else:
            to_type = "jsonl"
            out = sys.stdout

        # Normalize items to convert non-JSON-serializable types (e.g., UUID) to strings
        normalized_items = [_normalize_for_json(item) for item in items]

        # Extract fieldnames from items for CSV output
        fieldnames = None
        if to_type == "csv" and normalized_items:
            if isinstance(normalized_items[0], dict):
                fieldnames = list(normalized_items[0].keys())

        writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
        writer.write_items(normalized_items)

        if to_file:
            out.close()


def _write_sorted_stream(sorted_iter, to_file):
    """Stream sorted records to output without materializing the full list."""
    if to_file:
        to_type = get_file_type(to_file)
        if not to_type:
            raise FormatError(to_file, to_file.rsplit(".", 1)[-1])
        out = open(to_file, "w", encoding="utf8")
    else:
        to_type = "jsonl"
        out = sys.stdout

    writer = None
    count = 0
    try:
        for item in sorted_iter:
            item = _normalize_for_json(item)
            if writer is None:
                fieldnames = list(item.keys()) if to_type == "csv" and isinstance(item, dict) else None
                writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
            writer.write_items([item])
            count += 1
        logging.debug("sort: wrote %d records via external merge", count)
    finally:
        if to_file:
            out.close()