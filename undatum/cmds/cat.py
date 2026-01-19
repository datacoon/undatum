"""Cat command module - concatenate files."""
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


class Cat:
    """Cat command handler - concatenate files."""
    def __init__(self):
        pass

    def cat(self, fromfiles, options=None):
        """Concatenate files by rows or columns."""
        if options is None:
            options = {}
        if not fromfiles:
            logging.error('At least one input file is required')
            return

        logging.debug('Processing %s files', len(fromfiles))
        mode = get_option(options, 'mode') or 'rows'
        to_file = get_option(options, 'output')

        if mode == 'rows':
            # Row concatenation: append files vertically
            iterableargs = get_iterable_options(options)
            all_items = []
            all_headers = set()

            for fromfile in fromfiles:
                iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
                try:
                    first = True
                    for item in iterable:
                        if isinstance(item, dict):
                            all_items.append(item)
                            if first:
                                all_headers.update(item.keys())
                                first = False
                finally:
                    iterable.close()

            items = all_items

        elif mode == 'columns':
            # Column concatenation: combine files side-by-side
            iterableargs = get_iterable_options(options)
            all_file_items = []

            # Read all files
            for fromfile in fromfiles:
                file_items = []
                iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
                try:
                    for item in iterable:
                        if isinstance(item, dict):
                            file_items.append(item)
                finally:
                    iterable.close()
                all_file_items.append(file_items)

            # Combine side-by-side
            max_len = max(len(items) for items in all_file_items) if all_file_items else 0
            items = []
            for i in range(max_len):
                combined = {}
                for file_items in all_file_items:
                    if i < len(file_items):
                        combined.update(file_items[i])
                items.append(combined)
        else:
            logging.error(f'Invalid mode: {mode}. Use "rows" or "columns"')
            return

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

        logging.debug('cat: concatenated %d files, %d total rows', len(fromfiles), len(items))
