"""Replace command module - string replacement in fields."""

import logging
import re
import sys

from ..common.command_utils import ITERABLE_OPTIONS_KEYS, get_iterable_options  # noqa: F401
from ..common.errors import (
    FileNotFoundError,
    FormatError,
    PermissionError,
    ValidationError,
    find_similar_files,
)
from ..common.iterable import DataWriter
from ..common.path_utils import validate_file_path
from ..common.s3_iterable import open_path as open_iterable
from ..utils import get_file_type, get_option, normalize_for_json


class Replacer:
    """Replacer command handler - string replacement."""

    def __init__(self):
        pass

    def replace(self, fromfile, options=None):
        """Perform string replacement in specified fields."""
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
        field_name = get_option(options, "field")
        pattern = get_option(options, "pattern")
        replacement = get_option(options, "replacement") or ""
        use_regex = get_option(options, "regex") or False
        global_replace = get_option(options, "global") or False
        to_file = get_option(options, "output")

        if not field_name or not pattern:
            raise ValidationError("Field and pattern are required", field="field")

        # Prepare replacement function
        if use_regex:
            try:
                regex = re.compile(pattern)
                if global_replace:

                    def replace_func(text):
                        return regex.sub(replacement, str(text))

                else:

                    def replace_func(text):
                        return regex.sub(replacement, str(text), count=1)

            except re.error as e:
                raise ValidationError(f"Invalid regex pattern: {e}", field="pattern") from e
        else:
            if global_replace:

                def replace_func(text):
                    return str(text).replace(pattern, replacement)

            else:

                def replace_func(text):
                    return str(text).replace(pattern, replacement, 1)

        iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
        items = []
        try:
            count = 0
            for item in iterable:
                if isinstance(item, dict) and field_name in item:
                    item_copy = item.copy()
                    if item_copy[field_name] is not None:
                        item_copy[field_name] = replace_func(item_copy[field_name])
                    items.append(item_copy)
                    count += 1
                    if count % 10000 == 0:
                        logging.debug("replace: processed %d records", count)
                else:
                    items.append(item)
                    count += 1
        finally:
            iterable.close()

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

        logging.debug("replace: processed %d records", count)
