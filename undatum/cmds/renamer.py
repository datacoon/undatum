"""Rename command module - rename fields."""
import logging
import re
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


class Renamer:
    """Renamer command handler - rename fields."""
    def __init__(self):
        pass

    def rename(self, fromfile, options=None):
        """Rename fields by exact mapping or regex patterns."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        mapping = get_option(options, 'map')
        pattern = get_option(options, 'pattern')
        replacement = get_option(options, 'replacement') or ''
        to_file = get_option(options, 'output')

        # Build rename mapping
        rename_map = {}
        if mapping:
            # Parse mapping: "old1:new1,old2:new2"
            for pair in mapping.split(','):
                if ':' in pair:
                    old_name, new_name = pair.split(':', 1)
                    rename_map[old_name.strip()] = new_name.strip()
        elif pattern:
            # Regex-based renaming
            try:
                regex = re.compile(pattern)
            except re.error as e:
                logging.error(f'Invalid regex pattern: {e}')
                return

        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        items = []
        try:
            count = 0
            for item in iterable:
                if isinstance(item, dict):
                    item_copy = {}
                    for key, value in item.items():
                        if mapping and key in rename_map:
                            # Exact mapping
                            new_key = rename_map[key]
                            item_copy[new_key] = value
                        elif pattern:
                            # Regex replacement
                            new_key = regex.sub(replacement, key)
                            item_copy[new_key] = value
                        else:
                            item_copy[key] = value
                    items.append(item_copy)
                    count += 1
                    if count % 10000 == 0:
                        logging.debug('rename: processed %d records', count)
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

        logging.debug('rename: processed %d records', count)
