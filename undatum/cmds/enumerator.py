"""Enum command module - add row numbers, UUIDs, or constants."""
import logging
import sys
import uuid

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


class Enumerator:
    """Enumerator command handler - add row numbers, UUIDs, or constants."""
    def __init__(self):
        pass

    def enum(self, fromfile, options=None):
        """Add row numbers, UUIDs, or constant values to records."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        field_name = get_option(options, 'field') or 'row_id'
        enum_type = get_option(options, 'type') or 'number'
        start = get_option(options, 'start') or 1
        value = get_option(options, 'value')
        to_file = get_option(options, 'output')

        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        try:
            count = start
            items = []
            for item in iterable:
                if isinstance(item, dict):
                    if enum_type == 'uuid':
                        item[field_name] = str(uuid.uuid4())
                    elif enum_type == 'constant' and value is not None:
                        item[field_name] = value
                    else:  # number
                        item[field_name] = count
                        count += 1
                items.append(item)
                if len(items) % 10000 == 0:
                    logging.debug('enum: processed %d records', len(items))
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

        logging.debug('enum: processed %d records', len(items))
