"""Join command module - relational joins between two files."""
import logging
import sys

import duckdb
from iterable.helpers.detect import detect_file_type, open_iterable

from ..common.duckdb_config import create_duckdb_connection, get_duckdb_config_from_options
from ..common.engine_selector import detect_engine
from ..common.iterable import DataWriter
from ..common.errors import FileNotFoundError, PermissionError, ValidationError, find_similar_files
from ..common.path_utils import validate_file_path
from ..utils import get_file_type, get_option, normalize_for_json

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out




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
        
        # Validate both input files exist and are readable
        try:
            validate_file_path(file1, check_read=True)
        except FileNotFoundError as e:
            suggestions = find_similar_files(file1)
            raise FileNotFoundError(file1, suggestions) from e
        except PermissionError as e:
            raise PermissionError(file1, operation="read") from e
        
        try:
            validate_file_path(file2, check_read=True)
        except FileNotFoundError as e:
            suggestions = find_similar_files(file2)
            raise FileNotFoundError(file2, suggestions) from e
        except PermissionError as e:
            raise PermissionError(file2, operation="read") from e
        
        logging.debug('Joining %s and %s', file1, file2)

        on_fields = get_option(options, 'on')
        join_type = get_option(options, 'type') or 'inner'
        filetype1 = get_option(options, 'filetype1')
        get_option(options, 'filetype2')
        engine = get_option(options, 'engine') or 'auto'

        if not on_fields:
            raise ValidationError("Join key fields (--on) are required", field='on')

        key_field_list = [f.strip() for f in on_fields.split(',')]
        filetype2 = get_option(options, 'filetype2')

        # Check if both files support DuckDB
        detected_engine1 = detect_engine(file1, engine, filetype1, operation='join')
        detected_engine2 = detect_engine(file2, engine, filetype2, operation='join')
        detected_engine = 'duckdb' if (detected_engine1 == 'duckdb' and detected_engine2 == 'duckdb') else 'iterable'

        if detected_engine == 'duckdb':
            try:
                duckdb_config = get_duckdb_config_from_options(options)
                conn = create_duckdb_connection(**duckdb_config)

                # Determine input formats and build appropriate read expressions
                source_type1 = filetype1 or get_file_type(file1) or 'csv'
                source_type2 = filetype2 or get_file_type(file2) or 'csv'

                def build_read_expr(filename, filetype):
                    if filetype == 'csv':
                        return f"read_csv_auto('{filename}', all_varchar=true)"
                    elif filetype in ('json', 'jsonl'):
                        return f"read_json_auto('{filename}')"
                    elif filetype == 'parquet':
                        return f"read_parquet('{filename}')"
                    else:
                        raise ValueError(f"Unsupported file type for DuckDB: {filetype}")

                read_expr1 = build_read_expr(file1, source_type1)
                read_expr2 = build_read_expr(file2, source_type2)

                # Build ON clause for multiple keys
                on_conditions = []
                for key_field in key_field_list:
                    on_conditions.append(f"t1.{key_field} = t2.{key_field}")
                on_clause = " AND ".join(on_conditions)

                # Map join type
                join_type_sql = {
                    'inner': 'INNER',
                    'left': 'LEFT',
                    'right': 'RIGHT',
                    'full': 'FULL OUTER',
                    'outer': 'FULL OUTER'
                }.get(join_type.lower(), 'INNER')

                query = f"""
                    SELECT *
                    FROM ({read_expr1}) t1
                    {join_type_sql} JOIN ({read_expr2}) t2
                    ON {on_clause}
                """

                to_file = get_option(options, 'output')
                if to_file:
                    to_type = get_file_type(to_file) or 'csv'
                    # Use COPY for file output
                    if to_type == 'csv':
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT CSV, HEADER)"
                    elif to_type in ('json', 'jsonl'):
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT JSON)"
                    elif to_type == 'parquet':
                        copy_query = f"COPY ({query}) TO '{to_file}' (FORMAT PARQUET)"
                    else:
                        # Fallback: read into memory
                        to_type = 'jsonl'
                        copy_query = None
                else:
                    # For stdout, read into memory
                    to_type = 'jsonl'
                    copy_query = None

                if copy_query:
                    conn.execute(copy_query)
                    logging.info('join: completed using DuckDB')
                    conn.close()
                    return
                else:
                    # Read results into memory for stdout or unsupported output format
                    relation = conn.execute(query)
                    column_names = relation.columns
                    rows = relation.fetchall()
                    items = [dict(zip(column_names, row)) for row in rows]
                    conn.close()
                    logging.info(f'join: completed using DuckDB, {len(items)} joined rows')
                    # Write items and return
                    if to_file:
                        out = open(to_file, 'w', encoding='utf8')
                    else:
                        out = sys.stdout

                    normalized_items = [normalize_for_json(item) for item in items]
                    fieldnames = None
                    if to_type == 'csv' and normalized_items:
                        if isinstance(normalized_items[0], dict):
                            fieldnames = list(normalized_items[0].keys())

                    writer = DataWriter(out, filetype=to_type, fieldnames=fieldnames)
                    writer.write_items(normalized_items)

                    if to_file:
                        out.close()
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
