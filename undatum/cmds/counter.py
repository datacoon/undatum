"""Row counting module."""

import logging

import duckdb

from ..common.command_utils import (  # noqa: F401
    ITERABLE_OPTIONS_KEYS,
    force_iterable_if_table,
    get_iterable_options,
    run_with_duckdb_fallback,
)
from ..common.engine_selector import detect_engine
from ..common.errors import FileNotFoundError, PermissionError, find_similar_files
from ..common.path_utils import validate_file_path
from ..common.s3_iterable import open_iterable_with_s3
from ..utils import get_option


class Counter:
    """Row counting handler."""

    def __init__(self):
        pass

    def count(self, fromfile, options=None):
        """Count the number of rows in a data file."""
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
        filetype = get_option(options, "filetype")
        engine = get_option(options, "engine") or "auto"

        detected_engine = detect_engine(fromfile, engine, filetype, operation="count")
        detected_engine = force_iterable_if_table(options, detected_engine)

        def _count_duckdb():
            return duckdb.sql(f"SELECT COUNT(*) FROM '{fromfile}'").fetchone()[0]

        def _count_iterable():
            with open_iterable_with_s3(fromfile, mode="r", iterableargs=iterableargs) as iterable:
                count = 0
                try:
                    for _ in iterable:
                        count += 1
                        if count % 100000 == 0:
                            logging.debug("count: processed %d records", count)
                finally:
                    iterable.close()
                return count

        count = run_with_duckdb_fallback(
            "count", _count_duckdb, _count_iterable, engine=detected_engine
        )
        print(count)
