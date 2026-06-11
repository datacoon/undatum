"""Data query module using mistql."""

import logging

from ..common.command_utils import ITERABLE_OPTIONS_KEYS, get_iterable_options  # noqa: F401
from ..common.errors import FormatError
from ..common.s3_iterable import open_path as open_iterable
from ..utils import get_file_type, get_option, strip_dict_fields

LINEEND = b"\n"


class DataQuery:
    """Data query handler using mistql."""

    def __init__(self):
        pass

    def query(self, fromfile, options=None):
        """Use mistql to query data."""
        if options is None:
            options = {}
        from mistql import query

        iterableargs = get_iterable_options(options)
        to_file = get_option(options, "output")

        if to_file:
            if not get_file_type(to_file):
                raise FormatError(to_file, to_file.rsplit(".", 1)[-1])
            out_iterable = open_iterable(to_file, mode="w", iterableargs={})
        else:
            out_iterable = None

        fields_value = get_option(options, "fields")
        fields = fields_value.split(",") if fields_value else None
        fields_list = [field.split(".") for field in fields] if fields else None

        iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
        try:
            n = 0
            for r in iterable:
                n += 1
                if fields_list:
                    r_selected = strip_dict_fields(r, fields_list, 0)
                else:
                    r_selected = r
                if options.get("query") is not None:
                    res = query(options["query"], r_selected)
                    if not res:
                        continue
                else:
                    res = r_selected

                if out_iterable:
                    out_iterable.write(res)
                else:
                    print(res)
        finally:
            iterable.close()

        logging.debug("query: %d records processed", n)
        if out_iterable:
            out_iterable.close()
