"""Sort command module - sort rows by columns."""
import logging
import sys
import uuid

import duckdb
from iterable.helpers.detect import detect_file_type, open_iterable

from ..common.iterable import DataWriter
from ..constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES
from ..utils import get_file_type, get_option

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']

# Threshold for using external sort (in-memory vs external merge)
EXTERNAL_SORT_THRESHOLD = 100000  # items


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


def _detect_engine(fromfile, engine, filetype):
    """Detect the appropriate engine for processing."""
    compression = 'raw'
    if filetype is None:
        ftype = detect_file_type(fromfile)
        if ftype['success']:
            filetype = ftype['datatype'].id()
            if ftype['codec'] is not None:
                compression = ftype['codec'].id()
    logging.info(f'File filetype {filetype} and compression {compression}')
    if engine == 'auto':
        if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
            return 'duckdb'
        return 'iterable'
    return engine


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
                keys.append(float(value) if value is not None else float('-inf'))
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
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, 'filetype')
        engine = get_option(options, 'engine') or 'auto'
        by_fields = get_option(options, 'by')
        descending = get_option(options, 'desc') or False
        numeric_fields = get_option(options, 'numeric')
        to_file = get_option(options, 'output')

        if not by_fields:
            logging.error('Sort fields (--by) are required')
            return

        # Parse sort fields
        sort_fields = [f.strip() for f in by_fields.split(',')]
        numeric_set = {f.strip() for f in numeric_fields.split(',')} if numeric_fields else set()

        detected_engine = _detect_engine(fromfile, engine, filetype)

        # Initialize items for output handling
        items = []

        if detected_engine == 'duckdb':
            # Use DuckDB for efficient sorting
            # Note: DuckDB is only used when writing to a file
            # For stdout output, we use iterable engine
            if to_file:
                try:
                    to_type = get_file_type(to_file)
                    if not to_type:
                        logging.error('Output file type not supported')
                        return
                    # Build SQL for sorting
                    order_by = ','.join([
                        f"{field} {'DESC' if descending else 'ASC'}"
                        for field in sort_fields
                    ])
                    query = f"COPY (SELECT * FROM '{fromfile}' ORDER BY {order_by}) TO '{to_file}' (FORMAT CSV, HEADER)"
                    duckdb.sql(query)
                    logging.info('sort: completed using DuckDB')
                    return
                except Exception as e:
                    logging.warning(f'DuckDB sort failed, falling back to iterable: {e}')
                    detected_engine = 'iterable'
            else:
                # DuckDB doesn't support stdout, fall back to iterable
                detected_engine = 'iterable'

        if detected_engine == 'iterable':
            # Collect items and sort
            iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
            items = []

            try:
                count = 0
                for item in iterable:
                    items.append(item)
                    count += 1
                    if count % 100000 == 0:
                        logging.debug('sort: buffered %d records', count)
            finally:
                iterable.close()

            # Sort items
            reverse = descending
            items.sort(key=lambda x: _get_sort_key(x, sort_fields, numeric_set), reverse=reverse)
            logging.debug('sort: sorted %d records', len(items))

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
        normalized_items = [_normalize_for_json(item) for item in items]

        # Extract fieldnames from items for CSV output
        fieldnames = None
        if to_type == 'csv' and normalized_items:
            if isinstance(normalized_items[0], dict):
                fieldnames = list(normalized_items[0].keys())

        writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
        writer.write_items(normalized_items)

        if to_file:
            out.close()
