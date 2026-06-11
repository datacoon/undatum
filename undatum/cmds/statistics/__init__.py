"""Statistical analysis package.

Split from the former monolithic ``undatum/cmds/statistics.py`` into
engine detection, DuckDB engine, and iterable engine modules.
"""

import logging

import duckdb
from qddate import DateParser

from ...common.command_utils import ITERABLE_OPTIONS_KEYS, get_iterable_options  # noqa: F401
from ...utils import get_option
from .duckdb_engine import DuckDBStatsMixin
from .engine import _detect_engine
from .iterable_engine import IterableStatsMixin

__all__ = ["StatProcessor", "_detect_engine"]


class StatProcessor(DuckDBStatsMixin, IterableStatsMixin):
    """Statistical processing handler."""

    def __init__(self, nodates=True):
        if nodates:
            self.qd = None
        else:
            self.qd = DateParser(generate=True)
        pass

    def stats(self, fromfile, options):
        """Produces statistics and structure analysis of JSONlines, BSON or CSV file and produces stats.

        Args:
            fromfile: Path to input file
            options: Dictionary of options including:
                - engine: Engine to use ('auto', 'duckdb', or 'iterable')
                - dictshare: Dictionary share threshold
                - format_in: Override file type detection
                - progress: Show progress bar (default: True)
                - no_progress: Disable progress bar
                - Other iterable options (delimiter, encoding, etc.)
        """

        # Get engine preference and detect appropriate engine
        engine = get_option(options, "engine") or "auto"
        filetype = get_option(options, "format_in")
        detected_engine = _detect_engine(fromfile, engine, filetype)

        logging.info(f"Using {detected_engine} engine for statistics computation")

        # Validate input file before processing
        from ...common.errors import FileNotFoundError, PermissionError, find_similar_files
        from ...common.path_utils import validate_file_path

        try:
            validate_file_path(fromfile, check_read=True)
        except FileNotFoundError as e:
            suggestions = find_similar_files(fromfile)
            raise FileNotFoundError(fromfile, suggestions) from e
        except PermissionError as e:
            raise PermissionError(fromfile, operation="read") from e

        # Use DuckDB engine if selected
        if detected_engine == "duckdb":
            try:
                return self._stats_duckdb(fromfile, options)
            except duckdb.Error as e:
                # Check if this is a None column reference error (which is handled gracefully)
                error_msg = str(e)
                if "None" in error_msg and (
                    "not found" in error_msg or "Referenced column" in error_msg
                ):
                    # None column reference - this is expected and handled, suppress warning
                    logging.debug(
                        f"DuckDB stats: None column reference detected, falling back to iterable: {e}"
                    )
                else:
                    # DuckDB-specific errors (query failures, parsing errors, etc.)
                    logging.warning(
                        f"DuckDB stats failed (DuckDB error), falling back to iterable: {e}"
                    )
                detected_engine = "iterable"
            except Exception as e:
                # Check if this is a None column reference error
                error_msg = str(e)
                if "None" in error_msg and (
                    "not found" in error_msg or "Referenced column" in error_msg
                ):
                    # None column reference - suppress warning
                    logging.debug(
                        f"DuckDB stats: None column reference detected, falling back to iterable: {e}"
                    )
                else:
                    # Other errors (file not found, permission errors, etc.)
                    logging.warning(f"DuckDB stats failed, falling back to iterable: {e}")
                detected_engine = "iterable"

        # Use iterable engine (existing implementation)
        if detected_engine == "iterable":
            return self._stats_iterable(fromfile, options)
        else:
            from ...common.errors import ValidationError

            logging.error(f"Unsupported engine: {detected_engine}")
            raise ValidationError(
                f"Unsupported engine: '{detected_engine}'",
                field="engine",
                suggestions=["auto", "duckdb", "iterable"],
            )
