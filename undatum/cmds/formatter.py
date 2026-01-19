"""Format command module - reformat CSV with specific formatting options."""
import csv
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


class Formatter:
    """Formatter command handler - reformat CSV data."""
    def __init__(self):
        pass

    def fmt(self, fromfile, options=None):
        """Reformat CSV data with specific formatting options."""
        if options is None:
            options = {}
        logging.debug('Formatting %s', fromfile)

        delimiter = get_option(options, 'delimiter') or ','
        quote_style = get_option(options, 'quote') or 'minimal'
        escape_char = get_option(options, 'escape') or 'double'
        line_ending = get_option(options, 'line_ending') or 'unix'

        # Map quote styles
        quoting_map = {
            'always': csv.QUOTE_ALL,
            'minimal': csv.QUOTE_MINIMAL,
            'none': csv.QUOTE_NONE,
            'nonnumeric': csv.QUOTE_NONNUMERIC
        }
        quoting = quoting_map.get(quote_style.lower(), csv.QUOTE_MINIMAL)

        # Map escape characters
        escapechar_map = {
            'double': None,  # Use double-quote escape (default CSV behavior)
            'backslash': '\\',
            'none': None
        }
        escapechar = escapechar_map.get(escape_char.lower(), None)

        # Map line endings
        lineterminator_map = {
            'unix': '\n',
            'windows': '\r\n',
            'crlf': '\r\n',
            'mac': '\r'
        }
        lineterminator = lineterminator_map.get(line_ending.lower(), '\n')

        # CSV module doesn't allow escapechar when quoting is used with QUOTE_MINIMAL/QUOTE_ALL
        # Adjust based on quoting style
        if quoting in (csv.QUOTE_MINIMAL, csv.QUOTE_ALL) and escapechar == '"':
            escapechar = None  # Use double-quote escape instead

        iterableargs = get_iterable_options(options)
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        items = []

        try:
            count = 0
            for item in iterable:
                items.append(item)
                count += 1
                if count % 10000 == 0:
                    logging.debug('fmt: processed %d records', count)
        finally:
            iterable.close()

        to_file = get_option(options, 'output')
        if to_file:
            to_type = get_file_type(to_file)
            if not to_type:
                logging.error('Output file type not supported')
                return
            # For CSV formatting, force CSV output
            if to_type != 'csv':
                logging.warning('fmt: formatting options apply to CSV only, output will be CSV format')
            out = open(to_file, 'w', encoding='utf8', newline='')
        else:
            to_type = 'csv'
            out = sys.stdout

        # Extract fieldnames from items for CSV output
        fieldnames = None
        if items and isinstance(items[0], dict):
            fieldnames = list(items[0].keys())

        if to_type == 'csv' and fieldnames:
            # Build writer kwargs, excluding escapechar if incompatible
            writer_kwargs = {
                'fieldnames': fieldnames,
                'delimiter': delimiter,
                'quoting': quoting,
                'lineterminator': lineterminator
            }
            # Only add escapechar if it's not None and compatible
            if escapechar is not None and quoting in (csv.QUOTE_NONE, csv.QUOTE_NONNUMERIC):
                writer_kwargs['escapechar'] = escapechar

            writer = csv.DictWriter(out, **writer_kwargs)
            writer.writeheader()
            for item in items:
                if isinstance(item, dict):
                    writer.writerow(item)
        else:
            # Normalize items to convert non-JSON-serializable types (e.g., UUID) to strings
            normalized_items = [normalize_for_json(item) for item in items]
            # Fall back to DataWriter for other formats
            writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
            writer.write_items(normalized_items)

        if to_file:
            out.close()

        logging.debug('fmt: formatted %d records', count)
