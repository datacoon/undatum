"""Join command module - relational joins between two files."""
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


def _get_key_value(item, key_fields):
    """Get key value for joining."""
    if not key_fields:
        # Use first field if no key specified
        if isinstance(item, dict) and item:
            return list(item.values())[0]
        return None
    else:
        # Use specified key fields
        if isinstance(item, dict):
            if len(key_fields) == 1:
                return item.get(key_fields[0])
            else:
                return tuple(item.get(field) for field in key_fields)
        return None


class Joiner:
    """Joiner command handler - relational joins."""
    def __init__(self):
        pass

    def join(self, file1, file2, options=None):
        """Perform relational join between two files."""
        if options is None:
            options = {}
        logging.debug('Joining %s and %s', file1, file2)

        on_fields = get_option(options, 'on')
        join_type = get_option(options, 'type') or 'inner'
        filetype1 = get_option(options, 'filetype1')
        get_option(options, 'filetype2')
        engine = get_option(options, 'engine') or 'auto'

        if not on_fields:
            logging.error('Join key fields (--on) are required')
            return

        key_field_list = [f.strip() for f in on_fields.split(',')]

        detected_engine = _detect_engine(file1, engine, filetype1)

        if detected_engine == 'duckdb':
            # Try DuckDB SQL join for supported formats
            try:
                to_file = get_option(options, 'output')
                if to_file:
                    to_type = get_file_type(to_file)
                    if not to_type:
                        logging.error('Output file type not supported')
                        return

                    # Build SQL for join
                    ','.join(key_field_list)
                    join_type_sql = {
                        'inner': 'INNER',
                        'left': 'LEFT',
                        'right': 'RIGHT',
                        'full': 'FULL OUTER',
                        'outer': 'FULL OUTER'
                    }.get(join_type.lower(), 'INNER')

                    query = f"""
                    COPY (
                        SELECT *
                        FROM '{file1}' t1
                        {join_type_sql} JOIN '{file2}' t2
                        ON t1.{key_field_list[0]} = t2.{key_field_list[0]}
                    ) TO '{to_file}' (FORMAT CSV, HEADER)
                    """
                    duckdb.sql(query)
                    logging.info('join: completed using DuckDB')
                    return
            except Exception as e:
                logging.warning(f'DuckDB join failed, falling back to iterable: {e}')
                detected_engine = 'iterable'

        # Hash-based join implementation
        iterableargs = get_iterable_options(options)

        # Build hash index from file2 (right side)
        iterable2 = open_iterable(file2, mode='r', iterableargs=iterableargs)
        file2_index = {}

        try:
            count2 = 0
            for item in iterable2:
                count2 += 1
                if isinstance(item, dict):
                    key = _get_key_value(item, key_field_list)
                    if key is not None:
                        if key not in file2_index:
                            file2_index[key] = []
                        file2_index[key].append(item)
        finally:
            iterable2.close()

        logging.debug('join: indexed %d records from %s', len(file2_index), file2)

        # Process file1 and join
        iterable1 = open_iterable(file1, mode='r', iterableargs=iterableargs)
        items = []

        try:
            count1 = 0
            matched_keys = set()
            for item1 in iterable1:
                count1 += 1
                if isinstance(item1, dict):
                    key = _get_key_value(item1, key_field_list)
                    matched = key in file2_index

                    if matched:
                        matched_keys.add(key)
                        # Join with matching items from file2
                        for item2 in file2_index[key]:
                            # Merge items, handling field name conflicts
                            joined_item = item1.copy()
                            for field, value in item2.items():
                                # Prefix conflicting fields from file2
                                if field in item1 and item1[field] != value:
                                    joined_item[f'{field}_2'] = value
                                elif field not in item1:
                                    joined_item[field] = value
                            items.append(joined_item)
                    elif join_type in ('left', 'full', 'outer'):
                        # Left join: include unmatched items from file1
                        items.append(item1)

                if count1 % 100000 == 0:
                    logging.debug('join: processed %d records from %s, produced %d joined rows',
                                 count1, file1, len(items))
        finally:
            iterable1.close()

        # For right and full outer joins, include unmatched items from file2
        if join_type in ('right', 'full', 'outer'):
            for key, items2 in file2_index.items():
                if key not in matched_keys:
                    for item2 in items2:
                        items.append(item2)

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

        logging.debug('join: %s join completed, %d rows from file1, %d indexed from file2, %d joined rows',
                     join_type, count1, len(file2_index), len(items))
