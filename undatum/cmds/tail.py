"""Tail command module - extract last N rows."""
import logging
import sys
from collections import deque

from iterable.helpers.detect import open_iterable

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


class Tail:
    """Tail command handler - extract last N rows."""
    def __init__(self):
        pass

    def tail(self, fromfile, options=None):
        """Extract last N rows from a data file."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        n = get_option(options, 'n') or 10
        to_file = get_option(options, 'output')

        # Use deque with maxlen to efficiently keep only last N items
        buffer = deque(maxlen=n)

        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        try:
            count = 0
            for item in iterable:
                buffer.append(item)
                count += 1
                if count % 100000 == 0:
                    logging.debug('tail: processed %d records', count)
        finally:
            iterable.close()

        items = list(buffer)

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

        logging.debug('tail: extracted %d rows from %d total', len(items), count)
