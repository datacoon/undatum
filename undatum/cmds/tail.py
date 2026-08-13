"""Tail command module - extract last N rows."""

import logging
import sys
from collections import deque

from ..common.command_utils import (  # noqa: F401
    ITERABLE_OPTIONS_KEYS,
    get_iterable_options,
    iter_command_rows,
)
from ..common.errors import FileNotFoundError, FormatError, PermissionError, find_similar_files
from ..common.iterable import DataWriter
from ..common.path_utils import validate_file_path
from ..common.s3_iterable import open_path as open_iterable
from ..utils import get_file_type, get_option, normalize_for_json


class Tail:
    """Tail command handler - extract last N rows."""

    def __init__(self):
        pass

    def tail(self, fromfile, options=None):
        """Extract last N rows from a data file."""
        if options is None:
            options = {}

        # Validate input file exists and is readable
        try:
            validate_file_path(fromfile, check_read=True)
        except FileNotFoundError as e:
            suggestions = find_similar_files(fromfile)
            raise FileNotFoundError(fromfile, suggestions) from e
        except PermissionError as e:
            raise PermissionError(fromfile, operation="read") from e

        logging.debug("Processing %s", fromfile)
        iterableargs = get_iterable_options(options)
        n = get_option(options, "n") or 10
        to_file = get_option(options, "output")

        # Use deque with maxlen to efficiently keep only last N items
        buffer = deque(maxlen=n)

        iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
        try:
            count = 0
            for item in iter_command_rows(iterable, options):
                buffer.append(item)
                count += 1
                if count % 100000 == 0:
                    logging.debug("tail: processed %d records", count)
        finally:
            iterable.close()

        items = list(buffer)

        if to_file:
            to_type = get_file_type(to_file)
            if not to_type:
                raise FormatError(to_file, to_file.rsplit(".", 1)[-1])
            out = open(to_file, "w", encoding="utf8")
        else:
            to_type = "jsonl"
            out = sys.stdout

        # Normalize items to convert non-JSON-serializable types (e.g., UUID) to strings
        normalized_items = [normalize_for_json(item) for item in items]

        # Extract fieldnames from items for CSV output
        fieldnames = None
        if to_type == "csv" and normalized_items:
            if isinstance(normalized_items[0], dict):
                fieldnames = list(normalized_items[0].keys())

        writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
        writer.write_items(normalized_items)

        if to_file:
            out.close()

        logging.debug("tail: extracted %d rows from %d total", len(items), count)
