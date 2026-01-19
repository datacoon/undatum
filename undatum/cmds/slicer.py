"""Slice command module - extract specific rows by range or index."""
import logging
import sys

import duckdb
from iterable.helpers.detect import detect_file_type, open_iterable

from ..common.iterable import DataWriter
from ..constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES
from ..utils import get_file_type, get_option, normalize_for_json

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


class Slicer:
    """Slicer command handler - extract rows by range or index."""
    def __init__(self):
        pass

    def slice(self, fromfile, options=None):
        """Extract specific rows by range or index list."""
        if options is None:
            options = {}
        logging.debug('Slicing %s', fromfile)

        start = get_option(options, 'start')
        end = get_option(options, 'end')
        indices_str = get_option(options, 'indices')
        filetype = get_option(options, 'filetype')
        engine = get_option(options, 'engine') or 'auto'

        # Determine slice mode
        if indices_str:
            # Index-based slicing
            indices = [int(i.strip()) for i in indices_str.split(',')]
            indices_set = set(indices)
            mode = 'indices'
        elif start is not None or end is not None:
            # Range-based slicing
            start_idx = int(start) if start is not None else 0
            end_idx = int(end) + 1 if end is not None else None  # Make end inclusive
            mode = 'range'
        else:
            logging.error('Either --start/--end or --indices must be specified')
            return

        detected_engine = _detect_engine(fromfile, engine, filetype)

        if detected_engine == 'duckdb' and mode == 'range':
            # Use DuckDB for efficient range slicing
            try:
                to_file = get_option(options, 'output')
                if to_file:
                    to_type = get_file_type(to_file)
                    if not to_type:
                        logging.error('Output file type not supported')
                        return

                    # Build SQL for slicing
                    limit_clause = f"LIMIT {end_idx - start_idx}" if end_idx else ""
                    offset_clause = f"OFFSET {start_idx}" if start_idx else ""
                    query = f"COPY (SELECT * FROM '{fromfile}' {offset_clause} {limit_clause}) TO '{to_file}' (FORMAT CSV, HEADER)"
                    duckdb.sql(query)
                    logging.info('slice: completed using DuckDB')
                    return
            except Exception as e:
                logging.warning(f'DuckDB slice failed, falling back to iterable: {e}')
                detected_engine = 'iterable'

        # Iterable-based slicing
        iterableargs = get_iterable_options(options)
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        items = []

        try:
            count = 0
            for item in iterable:
                include = False

                if mode == 'indices':
                    include = count in indices_set
                elif mode == 'range':
                    include = True
                    if start_idx is not None and count < start_idx:
                        include = False
                    if end_idx is not None and count >= end_idx:
                        include = False

                if include:
                    items.append(item)

                count += 1

                if mode == 'range' and end_idx is not None and count >= end_idx:
                    break

                if count % 100000 == 0:
                    logging.debug('slice: processed %d records, selected %d', count, len(items))
        finally:
            iterable.close()

        to_file = get_option(options, 'output')
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

        # Extract fieldnames from items for CSV output
        fieldnames = None
        if to_type == 'csv' and normalized_items:
            if isinstance(normalized_items[0], dict):
                fieldnames = list(normalized_items[0].keys())

        writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
        writer.write_items(normalized_items)

        if to_file:
            out.close()

        logging.debug('slice: selected %d rows from %d total rows', len(items), count)
