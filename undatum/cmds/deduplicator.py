"""Dedup command module - remove duplicate rows."""
import logging
import sys

import duckdb
from iterable.helpers.detect import detect_file_type, open_iterable

from ..common.duckdb_config import create_duckdb_connection, get_duckdb_config_from_options
from ..common.engine_selector import detect_engine
from ..common.iterable import DataWriter
from ..common.errors import FileNotFoundError, PermissionError, find_similar_files
from ..common.path_utils import validate_file_path
from ..utils import get_file_type, get_option, normalize_for_json

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out




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
        
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, 'filetype')
        engine = get_option(options, 'engine') or 'auto'
        key_fields = get_option(options, 'key_fields')
        keep = get_option(options, 'keep') or 'first'
        to_file = get_option(options, 'output')

        # Parse key fields
        key_field_list = None
        if key_fields:
            key_field_list = [f.strip() for f in key_fields.split(',')]

        detected_engine = detect_engine(fromfile, engine, filetype, operation='dedup')
        items = []  # Initialize items list
        count = 0  # Initialize count

        if detected_engine == 'duckdb':
            try:
                duckdb_config = get_duckdb_config_from_options(options)
                conn = create_duckdb_connection(**duckdb_config)

                # Determine input format and build appropriate read expression
                source_type = filetype or get_file_type(fromfile) or 'csv'
                if source_type == 'csv':
                    read_expr = f"read_csv_auto('{fromfile}', all_varchar=true)"
                elif source_type in ('json', 'jsonl'):
                    read_expr = f"read_json_auto('{fromfile}')"
                elif source_type == 'parquet':
                    read_expr = f"read_parquet('{fromfile}')"
                else:
                    conn.close()
                    raise ValueError(f"Unsupported file type for DuckDB: {source_type}")

                # Build deduplication query
                if key_field_list:
                    # Deduplicate by specific key fields using window function
                    # Use ROW_NUMBER() to keep first or last occurrence
                    partition_by = ', '.join(key_field_list)
                    if keep == 'last':
                        # Keep last: order descending, take row_number = 1
                        order_clause = ', '.join([f"{field} DESC" for field in key_field_list])
                    else:
                        # Keep first: order ascending, take row_number = 1
                        order_clause = ', '.join([f"{field} ASC" for field in key_field_list])
                    
                    query = f"""
                        SELECT * FROM (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_clause}) as rn
                            FROM {read_expr}
                        ) WHERE rn = 1
                    """
                else:
                    # Deduplicate by all fields using DISTINCT
                    query = f"SELECT DISTINCT * FROM {read_expr}"

                # Execute query and get results
                relation = conn.execute(query)
                column_names = relation.columns
                rows = relation.fetchall()
                items = [dict(zip(column_names, row)) for row in rows]
                # Remove the rn column if it exists (from window function)
                if key_field_list:
                    items = [{k: v for k, v in item.items() if k != 'rn'} for item in items]
                conn.close()
                count = len(items)  # Approximate count
                logging.info(f'dedup: completed using DuckDB, {len(items)} unique records')
            except Exception as e:
                logging.warning(f'DuckDB dedup failed, falling back to iterable: {e}')
                detected_engine = 'iterable'

        if detected_engine == 'iterable':
            # Use hash-based deduplication
            seen = {}
            items = []
            iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)

            try:
                count = 0
                for item in iterable:
                    count += 1
                    if isinstance(item, dict):
                        key = _get_key_value(item, key_field_list)

                        if keep == 'last':
                            # Always update (will overwrite previous)
                            seen[key] = item
                        else:
                            # Keep first (default)
                            if key not in seen:
                                seen[key] = item
                    else:
                        # For non-dict items, use item itself as key
                        if keep == 'last' or item not in seen:
                            seen[item] = item

                    if count % 100000 == 0:
                        logging.debug('dedup: processed %d records, unique %d', count, len(seen))

                items = list(seen.values())
            finally:
                iterable.close()

        if to_file:
            to_type = get_file_type(to_file)
            if not to_type:
                logging.error('Output file type not supported')
                return
            out = open(to_file, 'w', encoding='utf8')
        else:
            to_type = 'jsonl'
            out = sys.stdout

        # Normalize items to convert non-JSON-serializable types (e.g., UUID) to strings
        normalized_items = [normalize_for_json(item) for item in items]

        # Extract fieldnames from items for CSV output
        fieldnames = None
        if to_type == 'csv' and normalized_items:
            if isinstance(normalized_items[0], dict):
                fieldnames = list(normalized_items[0].keys())

        writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
        writer.write_items(normalized_items)

        if to_file:
            out.close()

        logging.debug('dedup: processed %d records, unique %d', count, len(items))
