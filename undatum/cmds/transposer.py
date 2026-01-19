"""Transpose command module - swap rows and columns."""
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


class Transposer:
    """Transposer command handler - transpose rows and columns."""
    def __init__(self):
        pass

    def transpose(self, fromfile, options=None):
        """Swap rows and columns."""
        if options is None:
            options = {}
        logging.debug('Transposing %s', fromfile)
        iterableargs = get_iterable_options(options)

        # Read all items into memory (required for transpose)
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        items = []

        try:
            count = 0
            for item in iterable:
                if isinstance(item, dict):
                    items.append(item)
                    count += 1
                    if count % 10000 == 0:
                        logging.debug('transpose: loaded %d records', count)
        finally:
            iterable.close()

        if not items:
            logging.warning('transpose: no data to transpose')
            return

        # Collect all field names (columns)
        all_fields = set()
        for item in items:
            all_fields.update(item.keys())
        all_fields = sorted(all_fields)

        # Transpose: create new items where each original column becomes a row
        # First item will have field names as values, subsequent items have values from that column
        transposed_items = []

        # First row: field names
        if all_fields:
            first_row = {}
            # Use a special field name for the first column (row index/field name)
            row_index_field = 'field_name'
            first_row[row_index_field] = 'row_index'
            for i in range(len(items)):
                first_row[f'row_{i}'] = all_fields[i] if i < len(all_fields) else None
            transposed_items.append(first_row)

        # Subsequent rows: one per original field (column)
        for _field_idx, field_name in enumerate(all_fields):
            new_row = {}
            new_row[row_index_field] = field_name
            for item_idx, item in enumerate(items):
                new_row[f'row_{item_idx}'] = item.get(field_name)
            transposed_items.append(new_row)

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
        normalized_items = [normalize_for_json(item) for item in transposed_items]

        # Extract fieldnames from items for CSV output
        fieldnames = None
        if to_type == 'csv' and normalized_items:
            if isinstance(normalized_items[0], dict):
                fieldnames = list(normalized_items[0].keys())

        writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
        writer.write_items(normalized_items)

        if to_file:
            out.close()

        logging.debug('transpose: transposed %d rows x %d columns to %d rows x %d columns',
                     len(items), len(all_fields), len(transposed_items), len(all_fields) + 1)
