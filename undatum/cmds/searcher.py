"""Search command module - regex-based row filtering."""
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
        pattern = get_option(options, 'pattern')
        fields = get_option(options, 'fields')
        ignore_case = get_option(options, 'ignore_case') or False
        to_file = get_option(options, 'output')

        if not pattern:
            logging.error('Pattern is required')
            return

        # Prepare regex pattern
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

        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        items = []
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
