"""Search command module - regex-based row filtering."""
import logging
import re
import sys

import duckdb
from iterable.helpers.detect import detect_file_type, open_iterable

from ..common.duckdb_config import create_duckdb_connection, get_duckdb_config_from_options
from ..common.engine_selector import detect_engine
from ..common.iterable import DataWriter
from ..utils import get_file_type, get_option, normalize_for_json

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


class Searcher:
    """Searcher command handler - regex-based filtering."""
    def __init__(self):
        pass

    def search(self, fromfile, options=None):
        """Filter rows using regex patterns."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, 'filetype') or get_option(options, 'format_in')
        engine = get_option(options, 'engine') or 'auto'
        pattern = get_option(options, 'pattern')
        fields = get_option(options, 'fields')
        ignore_case = get_option(options, 'ignore_case') or False
        to_file = get_option(options, 'output')

        if not pattern:
            logging.error('Pattern is required')
            return

        # Prepare regex pattern for Python fallback
        flags = re.IGNORECASE if ignore_case else 0
        try:
            regex = re.compile(pattern, flags)
        except re.error as e:
            logging.error(f'Invalid regex pattern: {e}')
            return

        # Field list for field-specific search
        field_list = None
        if fields:
            field_list = [f.strip() for f in fields.split(',')]

        detected_engine = detect_engine(fromfile, engine, filetype, operation='search')
        items = []

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

                # Build WHERE clause for regex matching
                # DuckDB uses REGEXP_MATCHES function for regex
                # Escape single quotes in pattern for SQL
                escaped_pattern = pattern.replace("'", "''")
                
                if not field_list:
                    # For search across all fields, we need to check each column
                    # This requires knowing the schema first, so fall back to iterable
                    conn.close()
                    detected_engine = 'iterable'
                    logging.info('search: DuckDB requires --fields option for all-fields search, falling back to iterable')
                
                if detected_engine == 'duckdb':
                    # Search in specific fields
                    conditions = []
                    for field in field_list:
                        # Use REGEXP_MATCHES with case-insensitive flag if needed
                        if ignore_case:
                            conditions.append(
                                f"REGEXP_MATCHES(CAST({field} AS VARCHAR), '(?i){escaped_pattern}')"
                            )
                        else:
                            conditions.append(
                                f"REGEXP_MATCHES(CAST({field} AS VARCHAR), '{escaped_pattern}')"
                            )
                    where_clause = " OR ".join(conditions)
                    query = f"SELECT * FROM {read_expr} WHERE {where_clause}"

                    # Execute query and get results
                    relation = conn.execute(query)
                    column_names = relation.columns
                    rows = relation.fetchall()
                    items = [dict(zip(column_names, row)) for row in rows]
                    conn.close()
                    logging.info(f'search: completed using DuckDB, matched {len(items)} records')
            except Exception as e:
                logging.warning(f'DuckDB search failed, falling back to iterable: {e}')
                detected_engine = 'iterable'

        if detected_engine == 'iterable':
            iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
            try:
                count = 0
                matched = 0
                for item in iterable:
                    count += 1
                    if isinstance(item, dict):
                        # Search in specified fields or all fields
                        search_fields = field_list if field_list else list(item.keys())

                        # Check if pattern matches in any of the search fields
                        matches = False
                        for field in search_fields:
                            if field in item and item[field] is not None:
                                value_str = str(item[field])
                                if regex.search(value_str):
                                    matches = True
                                    break

                        if matches:
                            items.append(item)
                            matched += 1

                    if count % 10000 == 0:
                        logging.debug('search: processed %d records, matched %d', count, matched)
            finally:
                iterable.close()
            logging.debug('search: processed %d records, matched %d', count, matched)

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

        logging.debug('search: processed %d records, matched %d', count, matched)
