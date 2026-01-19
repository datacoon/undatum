"""Reverse command module - reverse row order."""
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


class Reverser:
    """Reverser command handler - reverse row order."""
    def __init__(self):
        pass

    def reverse(self, fromfile, options=None):
        """Reverse the order of rows in a data file."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, 'filetype')
        engine = get_option(options, 'engine') or 'auto'
        to_file = get_option(options, 'output')

        detected_engine = _detect_engine(fromfile, engine, filetype)

        if detected_engine == 'duckdb':
            # Use DuckDB for efficient reversal on supported formats
            # Note: DuckDB doesn't have rowid for CSV, so we'll use iterable approach
            # DuckDB optimization can be added later if needed
            detected_engine = 'iterable'

        if detected_engine == 'iterable':
            # Collect all items and reverse
            iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
            items = []
            try:
                count = 0
                for item in iterable:
                    items.append(item)
                    count += 1
                    if count % 100000 == 0:
                        logging.debug('reverse: buffered %d records', count)
            finally:
                iterable.close()

            items.reverse()
            logging.debug('reverse: reversed %d records', len(items))

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
