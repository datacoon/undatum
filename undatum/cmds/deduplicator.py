"""Dedup command module - remove duplicate rows."""

import logging
import sys

from ..common.command_utils import ITERABLE_OPTIONS_KEYS, get_iterable_options  # noqa: F401
from ..common.disk_dedup import DiskDeduplicator
from ..common.duckdb_config import create_duckdb_connection, get_duckdb_config_from_options
from ..common.engine_selector import detect_engine
from ..common.errors import FileNotFoundError, FormatError, PermissionError, find_similar_files
from ..common.iterable import DataWriter
from ..common.path_utils import validate_file_path
from ..common.s3_iterable import open_path as open_iterable
from ..utils import get_file_type, get_option, normalize_for_json

# Switch to disk-backed dedup after this many unique keys in memory
DISK_DEDUP_THRESHOLD = 100000


def _get_key_value(item, key_fields):
    """Get key value for deduplication."""
    if not key_fields:
        # Use all fields
        return tuple(sorted((k, v) for k, v in item.items() if v is not None))
    else:
        # Use specified key fields
        return tuple(item.get(field) for field in key_fields)


class Deduplicator:
    """Deduplicator command handler - remove duplicates."""

    def __init__(self):
        pass

    def dedup(self, fromfile, options=None):
        """Remove duplicate rows."""
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
        key_fields = get_option(options, "key_fields")
        keep = get_option(options, "keep") or "first"
        to_file = get_option(options, "output")

        # Parse key fields
        key_field_list = None
        if key_fields:
            key_field_list = [f.strip() for f in key_fields.split(",")]

        detected_engine = detect_engine(fromfile, engine, filetype, operation="dedup")
        items = []  # Initialize items list
        count = 0  # Initialize count

        if detected_engine == "duckdb":
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

                # Build deduplication query
                if key_field_list:
                    # Deduplicate by specific key fields using window function
                    # Use ROW_NUMBER() to keep first or last occurrence
                    partition_by = ", ".join(key_field_list)
                    if keep == "last":
                        # Keep last: order descending, take row_number = 1
                        order_clause = ", ".join([f"{field} DESC" for field in key_field_list])
                    else:
                        # Keep first: order ascending, take row_number = 1
                        order_clause = ", ".join([f"{field} ASC" for field in key_field_list])

                    query = f"""
                        SELECT * FROM (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_clause}) as rn
                            FROM {read_expr}
                        ) WHERE rn = 1
                    """
                else:
                    # Deduplicate by all fields using DISTINCT
                    query = f"SELECT DISTINCT * FROM {read_expr}"

                # Prefer COPY for file outputs to avoid materializing results
                if to_file:
                    to_type = get_file_type(to_file) or "csv"
                    if to_type == "csv":
                        conn.execute(f"COPY ({query}) TO '{to_file}' (FORMAT CSV, HEADER)")
                        conn.close()
                        logging.info("dedup: completed using DuckDB COPY")
                        return
                    if to_type in ("json", "jsonl"):
                        conn.execute(f"COPY ({query}) TO '{to_file}' (FORMAT JSON)")
                        conn.close()
                        logging.info("dedup: completed using DuckDB COPY")
                        return
                    if to_type == "parquet":
                        conn.execute(f"COPY ({query}) TO '{to_file}' (FORMAT PARQUET)")
                        conn.close()
                        logging.info("dedup: completed using DuckDB COPY")
                        return

                relation = conn.execute(query)
                column_names = relation.columns
                rows = relation.fetchall()
                items = [dict(zip(column_names, row)) for row in rows]
                if key_field_list:
                    items = [{k: v for k, v in item.items() if k != "rn"} for item in items]
                conn.close()
                count = len(items)
                logging.info(f"dedup: completed using DuckDB, {len(items)} unique records")
            except Exception as e:
                logging.warning(f"DuckDB dedup failed, falling back to iterable: {e}")
                detected_engine = "iterable"

        if detected_engine == "iterable":
            low_memory = bool(get_option(options, "low_memory"))
            temp_dir = get_option(options, "temp_dir") or get_option(options, "duckdb_temp_dir")
            iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)

            try:
                if low_memory:
                    items = list(
                        _disk_dedup_stream(iterable, key_field_list, keep, temp_dir)
                    )
                    count = len(items)
                else:
                    seen = {}
                    count = 0
                    use_disk = False
                    for item in iterable:
                        count += 1
                        if isinstance(item, dict):
                            key = _get_key_value(item, key_field_list)
                        else:
                            key = item

                        if keep == "last":
                            seen[key] = item
                        elif key not in seen:
                            seen[key] = item

                        if not use_disk and len(seen) > DISK_DEDUP_THRESHOLD:
                            use_disk = True
                            logging.info(
                                "dedup: unique keys exceeded %d, switching to disk-backed path",
                                DISK_DEDUP_THRESHOLD,
                            )
                            # Re-process from scratch on disk for exactness
                            iterable.close()
                            iterable = open_iterable(
                                fromfile, mode="r", iterableargs=iterableargs
                            )
                            items = list(
                                _disk_dedup_stream(iterable, key_field_list, keep, temp_dir)
                            )
                            count = len(items)
                            seen = None
                            break

                        if count % 100000 == 0:
                            logging.debug(
                                "dedup: processed %d records, unique %d", count, len(seen)
                            )

                    if seen is not None:
                        items = list(seen.values())
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

        normalized_items = [normalize_for_json(item) for item in items]

        fieldnames = None
        if to_type == "csv" and normalized_items:
            if isinstance(normalized_items[0], dict):
                fieldnames = list(normalized_items[0].keys())

        writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
        writer.write_items(normalized_items)

        if to_file:
            out.close()

        logging.debug("dedup: processed %d records, unique %d", count, len(items))


def _disk_dedup_stream(iterable, key_field_list, keep, temp_dir):
    """Yield unique records using disk-backed exact deduplication."""

    def key_fn(item):
        if isinstance(item, dict):
            return _get_key_value(item, key_field_list)
        return item

    with DiskDeduplicator(keep=keep, temp_dir=temp_dir) as deduper:
        yield from deduper.process(iterable, key_fn)
        logging.info(
            "dedup: disk-backed complete, in=%d unique=%d",
            deduper.stats[0],
            deduper.stats[1],
        )
