"""Slice command module - extract specific rows by range or index."""

import logging
import sys

from ..common.command_utils import ITERABLE_OPTIONS_KEYS, get_iterable_options  # noqa: F401
from ..common.duckdb_config import create_duckdb_connection, get_duckdb_config_from_options
from ..common.engine_selector import detect_engine
from ..common.errors import FormatError, ValidationError
from ..common.iterable import DataWriter
from ..common.s3_iterable import open_path as open_iterable
from ..utils import get_file_type, get_option, normalize_for_json


class Slicer:
    """Slicer command handler - extract rows by range or index."""

    def __init__(self):
        pass

    def slice(self, fromfile, options=None):
        """Extract specific rows by range or index list."""
        if options is None:
            options = {}
        logging.debug("Slicing %s", fromfile)

        start = get_option(options, "start")
        end = get_option(options, "end")
        indices_str = get_option(options, "indices")
        filetype = get_option(options, "filetype")
        engine = get_option(options, "engine") or "auto"

        # Determine slice mode
        if indices_str:
            # Index-based slicing
            indices = [int(i.strip()) for i in indices_str.split(",")]
            indices_set = set(indices)
            mode = "indices"
        elif start is not None or end is not None:
            # Range-based slicing
            start_idx = int(start) if start is not None else 0
            end_idx = int(end) + 1 if end is not None else None  # Make end inclusive
            mode = "range"
        else:
            raise ValidationError(
                "Either --start/--end or --indices must be specified", field="start"
            )

        detected_engine = detect_engine(fromfile, engine, filetype, operation="slice")

        if detected_engine == "duckdb" and mode == "range":
            # Use DuckDB for efficient range slicing
            try:
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
                    conn.close()
                    raise ValueError(f"Unsupported file type for DuckDB: {source_type}")

                # Build LIMIT/OFFSET clause
                limit_value = end_idx - start_idx if end_idx else None
                limit_clause = f"LIMIT {limit_value}" if limit_value else ""
                offset_clause = f"OFFSET {start_idx}" if start_idx > 0 else ""
                query = f"SELECT * FROM {read_expr} {offset_clause} {limit_clause}".strip()

                to_file = get_option(options, "output")
                if to_file:
                    to_type = get_file_type(to_file) or "csv"
                    # Use COPY for file output
                    if to_type == "csv":
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT CSV, HEADER)"
                    elif to_type in ("json", "jsonl"):
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT JSON)"
                    elif to_type == "parquet":
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT PARQUET)"
                    else:
                        # Fallback: read into memory
                        to_type = "jsonl"
                        copy_query = None
                else:
                    # For stdout, read into memory
                    to_type = "jsonl"
                    copy_query = None

                if copy_query:
                    conn.execute(copy_query)
                    logging.info("slice: completed using DuckDB")
                    conn.close()
                    return
                else:
                    # Read results into memory for stdout or unsupported output format
                    relation = conn.execute(query)
                    column_names = relation.columns
                    rows = relation.fetchall()
                    items = [dict(zip(column_names, row)) for row in rows]
                    conn.close()
                    logging.info(f"slice: completed using DuckDB, {len(items)} records")
                    # Write items and return
                    to_file = get_option(options, "output")
                    if to_file:
                        to_type = get_file_type(to_file) or "jsonl"
                        out = open(to_file, "w", encoding="utf8")
                    else:
                        to_type = "jsonl"
                        out = sys.stdout

                    normalized_items = [normalize_for_json(item) for item in items]
                    fieldnames = None
                    if to_type == "csv" and normalized_items:
                        if isinstance(normalized_items[0], dict):
                            fieldnames = list(normalized_items[0].keys())

                    writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
                    writer.write_items(normalized_items)

                    if to_file:
                        out.close()
                    return
            except Exception as e:
                logging.warning(f"DuckDB slice failed, falling back to iterable: {e}")
                detected_engine = "iterable"

        # Iterable-based slicing (fallback or for index-based slicing)
        if detected_engine == "iterable" or mode == "indices":
            items = []
            iterableargs = get_iterable_options(options)
            iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
            if "items" not in locals():
                items = []

        try:
            count = 0
            for item in iterable:
                include = False

                if mode == "indices":
                    include = count in indices_set
                elif mode == "range":
                    include = True
                    if start_idx is not None and count < start_idx:
                        include = False
                    if end_idx is not None and count >= end_idx:
                        include = False

                if include:
                    items.append(item)

                count += 1

                if mode == "range" and end_idx is not None and count >= end_idx:
                    break

                if count % 100000 == 0:
                    logging.debug("slice: processed %d records, selected %d", count, len(items))
        finally:
            iterable.close()

        to_file = get_option(options, "output")
        if to_file:
            to_type = get_file_type(to_file)
            if not to_type:
                raise FormatError(to_file, to_file.rsplit(".", 1)[-1])
            out = open(to_file, "w", encoding="utf8")
        else:
            to_type = "jsonl"
            out = sys.stdout

        # Normalize items to convert non-JSON-serializable types (e.g., UUID) to strings
        normalized_items = [normalize_for_json(item) for item in items]

        # Extract fieldnames from items for CSV output
        fieldnames = None
        if to_type == "csv" and normalized_items:
            if isinstance(normalized_items[0], dict):
                fieldnames = list(normalized_items[0].keys())

        writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
        writer.write_items(normalized_items)

        if to_file:
            out.close()

        logging.debug("slice: selected %d rows from %d total rows", len(items), count)
