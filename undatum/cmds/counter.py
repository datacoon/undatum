"""Row counting module."""
import logging

import duckdb
from iterable.helpers.detect import detect_file_type, open_iterable

from ..common.path_utils import is_s3_uri, validate_file_path
from ..common.s3_iterable import open_iterable_with_s3
from ..common.errors import FileNotFoundError, PermissionError, find_similar_files
from ..constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES
from ..utils import get_option

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


def _detect_engine(fromfile, engine, filetype):
    """Detect the appropriate engine for processing."""
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
            return 'duckdb'
        return 'iterable'
    return engine


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
        
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, 'filetype')
        engine = get_option(options, 'engine') or 'auto'

        detected_engine = _detect_engine(fromfile, engine, filetype)

        if detected_engine == 'duckdb':
            # Use DuckDB for fast counting on supported formats
            try:
                count = duckdb.sql(f"SELECT COUNT(*) FROM '{fromfile}'").fetchone()[0]
                print(count)
                return
            except Exception as e:
                logging.warning(f'DuckDB count failed, falling back to iterable: {e}')
                detected_engine = 'iterable'

        if detected_engine == 'iterable':
            # Stream through file and count
            iterable_context = open_iterable_with_s3(fromfile, mode='r', iterableargs=iterableargs)
            iterable = iterable_context.__enter__()
            count = 0
            try:
                for _ in iterable:
                    count += 1
                    if count % 100000 == 0:
                        logging.debug('count: processed %d records', count)
            finally:
                iterable.close()
                iterable_context.__exit__(None, None, None)
            print(count)
        else:
            logging.error('Engine not supported. Please choose duckdb or iterable')
