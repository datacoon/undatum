# -*- coding: utf8 -*-
"""Engine selection utilities for choosing between DuckDB and Python engines."""
import logging
from typing import Optional

from iterable.helpers.detect import detect_file_type

from ..constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES


def detect_engine(
    fromfile: str,
    engine: Optional[str] = None,
    filetype: Optional[str] = None,
    operation: Optional[str] = None,
) -> str:
    """Detect the appropriate engine for processing.

    Args:
        fromfile: Path to input file
        engine: Requested engine ('auto', 'duckdb', 'python', or None for auto)
        filetype: File type if already known (optional)
        operation: Operation name for compatibility checking (optional)

    Returns:
        Selected engine: 'duckdb' or 'iterable' (Python engine)
    """
    if engine is None:
        engine = 'auto'

    compression = 'raw'
    if filetype is None:
        ftype = detect_file_type(fromfile)
        if ftype['success']:
            filetype = ftype['datatype'].id()
            if ftype['codec'] is not None:
                compression = ftype['codec'].id()

    logging.info(f'File filetype {filetype} and compression {compression}')

    if engine == 'auto':
        if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
            # Check if operation is SQL-expressible (for future use)
            if operation and not _is_sql_expressible(operation):
                logging.debug(f'Operation {operation} not SQL-expressible, using iterable engine')
                return 'iterable'
            return 'duckdb'
        return 'iterable'

    if engine == 'duckdb':
        return 'duckdb'
    if engine == 'python':
        return 'iterable'

    # Default fallback
    return 'iterable'


def _is_sql_expressible(operation: str) -> bool:
    """Check if an operation can be expressed in SQL.

    Args:
        operation: Operation name (e.g., 'sort', 'filter', 'join')

    Returns:
        True if operation can be expressed in SQL, False otherwise
    """
    sql_expressible_operations = {
        'sort', 'frequency', 'uniq', 'sample', 'search', 'dedup', 'slice', 'join',
        'select', 'filter', 'count', 'stats'
    }
    return operation.lower() in sql_expressible_operations


def is_format_supported_by_duckdb(filetype: Optional[str], compression: Optional[str] = None) -> bool:
    """Check if a file format is supported by DuckDB.

    Args:
        filetype: File type identifier
        compression: Compression codec identifier (optional)

    Returns:
        True if format is supported by DuckDB, False otherwise
    """
    if filetype not in DUCKABLE_FILE_TYPES:
        return False
    if compression is None:
        compression = 'raw'
    return compression in DUCKABLE_CODECS
