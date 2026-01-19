"""Fill command module - fill empty/null values."""
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


class Filler:
    """Filler command handler - fill empty values."""
    def __init__(self):
        pass

    def fill(self, fromfile, options=None):
        """Fill empty or null values with specified values or strategies."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        fields = get_option(options, 'fields')
        strategy = get_option(options, 'strategy') or 'constant'
        value = get_option(options, 'value') or ''
        to_file = get_option(options, 'output')

        # Field list for field-specific filling
        field_list = None
        if fields:
            field_list = [f.strip() for f in fields.split(',')]

        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        items = []
        last_values = {}  # For forward/backward fill

        try:
            count = 0
            # For backward fill, need to process in reverse
            if strategy == 'backward':
                all_items = []
                for item in iterable:
                    if isinstance(item, dict):
                        all_items.append(item)
                iterable.close()

                # Process in reverse for backward fill
                next_values = {}  # Store next non-empty values
                for i in range(len(all_items) - 1, -1, -1):
                    item = all_items[i].copy()
                    if field_list:
                        fill_fields = field_list
                    else:
                        fill_fields = list(item.keys()) if isinstance(item, dict) else []

                    for field in fill_fields:
                        if field not in item or item[field] is None or item[field] == '':
                            # Use next non-empty value (stored from previous iteration in reverse)
                            if field in next_values:
                                item[field] = next_values[field]
                            elif value:
                                item[field] = value
                        else:
                            # Update next value for earlier items (processed later in reverse)
                            next_values[field] = item[field]

                    items.insert(0, item)  # Insert at beginning to maintain order
                    count += 1
            else:
                # Forward fill or constant fill
                for item in iterable:
                    if isinstance(item, dict):
                        item_copy = item.copy()
                        if field_list:
                            fill_fields = field_list
                        else:
                            fill_fields = list(item_copy.keys())

                        for field in fill_fields:
                            if field not in item_copy or item_copy[field] is None or item_copy[field] == '':
                                if strategy == 'forward':
                                    # Use previous non-empty value
                                    if field in last_values:
                                        item_copy[field] = last_values[field]
                                    elif value:
                                        item_copy[field] = value
                                else:  # constant
                                    item_copy[field] = value
                            else:
                                # Update last value for forward fill
                                last_values[field] = item_copy[field]

                        items.append(item_copy)
                        count += 1
                        if count % 10000 == 0:
                            logging.debug('fill: processed %d records', count)
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

        logging.debug('fill: processed %d records', count)
