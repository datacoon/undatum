"""Fixlengths command module - ensure all rows have same number of fields."""
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


class FixLengths:
    """FixLengths command handler - normalize row field counts."""
    def __init__(self):
        pass

    def fixlengths(self, fromfile, options=None):
        """Ensure all rows have the same number of fields."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        strategy = get_option(options, 'strategy') or 'pad'
        value = get_option(options, 'value') or ''
        to_file = get_option(options, 'output')

        # First pass: determine max/min field count
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        max_fields = 0
        min_fields = float('inf')
        all_headers = set()
        sample_items = []

        try:
            count = 0
            for item in iterable:
                if isinstance(item, dict):
                    field_count = len(item)
                    max_fields = max(max_fields, field_count)
                    min_fields = min(min_fields, field_count)
                    # Filter out None keys
                    all_headers.update(k for k in item.keys() if k is not None)
                    sample_items.append(item)
                    count += 1
                    if count >= 1000:  # Sample first 1000 to determine structure
                        break
        finally:
            iterable.close()

        # Determine target field count
        if strategy == 'pad':
            target_count = max_fields
        else:  # truncate
            target_count = min_fields

        # Get all headers in consistent order, filter out None
        all_headers = sorted([h for h in all_headers if h is not None])

        # Second pass: process all items
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        items = []
        try:
            count = 0
            for item in iterable:
                if isinstance(item, dict):
                    # Normalize item
                    normalized = {}
                    for header in all_headers[:target_count]:
                        if header in item and item[header] is not None:
                            normalized[header] = item[header]
                        else:
                            # Field missing or None, pad with value
                            normalized[header] = value
                    items.append(normalized)
                    count += 1
                    if count % 10000 == 0:
                        logging.debug('fixlengths: processed %d records', count)
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

        writer = DataWriter(out, filetype=to_type, fieldnames=all_headers[:target_count])
        writer.write_items(normalized_items)

        if to_file:
            out.close()

        logging.debug('fixlengths: processed %d records, normalized to %d fields', count, target_count)
