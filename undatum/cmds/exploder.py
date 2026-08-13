"""Explode command module - split column by separator into multiple rows."""

import logging
import sys

from ..common.command_utils import (
    ITERABLE_OPTIONS_KEYS,  # noqa: F401
    get_iterable_options,
    iter_command_rows,
)  # noqa: F401
from ..common.errors import FormatError, ValidationError
from ..common.iterable import DataWriter
from ..common.s3_iterable import open_path as open_iterable
from ..utils import get_file_type, get_option, normalize_for_json


class Exploder:
    """Exploder command handler - split column into rows."""

    def __init__(self):
        pass

    def explode(self, fromfile, options=None):
        """Split column by separator, creating multiple rows per original row."""
        if options is None:
            options = {}
        logging.debug("Processing %s", fromfile)
        iterableargs = get_iterable_options(options)
        field_name = get_option(options, "field")
        separator = get_option(options, "separator") or ","
        to_file = get_option(options, "output")

        if not field_name:
            raise ValidationError("explode requires a field name", field="field")

        iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
        items = []
        try:
            count = 0
            for item in iter_command_rows(iterable, options):
                if isinstance(item, dict) and field_name in item:
                    field_value = item[field_name]
                    if field_value is not None:
                        # Split the field value
                        values = str(field_value).split(separator)
                        # Create one row per value
                        for value in values:
                            item_copy = item.copy()
                            item_copy[field_name] = (
                                value.strip() if isinstance(value, str) else value
                            )
                            items.append(item_copy)
                    else:
                        items.append(item)
                    count += 1
                    if count % 10000 == 0:
                        logging.debug(
                            "explode: processed %d records, generated %d rows", count, len(items)
                        )
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

        logging.debug("explode: processed %d records, generated %d rows", count, len(items))
