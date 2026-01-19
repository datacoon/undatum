"""Exclude command module - remove rows based on keys in another file."""
import logging
import sys

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


def _get_key_value(item, key_fields):
    """Get key value for exclusion comparison."""
    if not key_fields:
        # Use all fields as key
        return tuple(sorted((k, v) for k, v in item.items() if v is not None))
    else:
        # Use specified key fields
        return tuple(item.get(field) for field in key_fields)


class Excluder:
    """Excluder command handler - exclude rows based on keys."""
    def __init__(self):
        pass

    def exclude(self, fromfile, exclude_file, options=None):
        """Remove rows from fromfile where keys match exclude_file."""
        if options is None:
            options = {}
        logging.debug('Processing %s, excluding %s', fromfile, exclude_file)

        on_fields = get_option(options, 'on')
        if not on_fields:
            logging.error('Key fields (--on) are required')
            return

        key_field_list = [f.strip() for f in on_fields.split(',')]

        # Build exclusion set from exclude_file
        iterableargs = get_iterable_options(options)
        exclude_iterable = open_iterable(exclude_file, mode='r', iterableargs=iterableargs)
        exclude_keys = set()

        try:
            count_exclude = 0
            for item in exclude_iterable:
                count_exclude += 1
                if isinstance(item, dict):
                    key = _get_key_value(item, key_field_list)
                    exclude_keys.add(key)
        finally:
            exclude_iterable.close()

        logging.debug('exclude: loaded %d exclusion keys', len(exclude_keys))

        # Filter fromfile
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        items = []

        try:
            count = 0
            excluded = 0
            for item in iterable:
                count += 1
                if isinstance(item, dict):
                    key = _get_key_value(item, key_field_list)
                    if key not in exclude_keys:
                        items.append(item)
                    else:
                        excluded += 1
                else:
                    # For non-dict items, use item itself as key
                    if item not in exclude_keys:
                        items.append(item)
                    else:
                        excluded += 1

                if count % 100000 == 0:
                    logging.debug('exclude: processed %d records, excluded %d', count, excluded)
        finally:
            iterable.close()

        to_file = get_option(options, 'output')
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

        logging.debug('exclude: processed %d records, excluded %d, kept %d', count, excluded, len(items))
