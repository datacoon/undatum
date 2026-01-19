"""Dedup command module - remove duplicate rows."""
import logging
import sys

from iterable.helpers.detect import detect_file_type, open_iterable

from ..common.iterable import DataWriter
from ..constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES
from ..utils import get_file_type, get_option, normalize_for_json

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


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

        detected_engine = _detect_engine(fromfile, engine, filetype)
        items = []  # Initialize items list
        count = 0  # Initialize count

        if detected_engine == 'duckdb' and key_field_list:
            # Use DuckDB for efficient deduplication on supported formats
            try:
                if to_file:
                    to_type = get_file_type(to_file)
                    if not to_type:
                        logging.error('Output file type not supported')
                        return
                    # DuckDB doesn't support DISTINCT ON easily, use subquery approach
                    # For now, use iterable approach for all cases
                    detected_engine = 'iterable'
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
