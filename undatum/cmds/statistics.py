"""Statistical analysis module."""
import logging
import time

import duckdb
from iterable.helpers.detect import detect_file_type, open_iterable
from qddate import DateParser
from tqdm import tqdm

from ..common.schema_utils import duckdb_decompose
from ..constants import DEFAULT_DICT_SHARE, DUCKABLE_CODECS, DUCKABLE_FILE_TYPES
from ..utils import dict_generator, get_option, guess_datatype

#STAT_READY_DATA_FORMATS = ['jsonl', 'bson', 'csv']

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


def _detect_engine(fromfile, engine, filetype):
    """Detect the appropriate engine for statistics processing.
    
    Args:
        fromfile: Path to input file
        engine: Engine preference ('auto', 'duckdb', or 'iterable')
        filetype: Optional file type override (if None, will be detected)
    
    Returns:
        Detected engine name: 'duckdb' or 'iterable'
    """
    compression = 'raw'
    if filetype is None:
        ftype = detect_file_type(fromfile)
        if ftype['success']:
            filetype = ftype['datatype'].id()
            if ftype['codec'] is not None:
                compression = ftype['codec'].id()
    logging.info(f'Stats engine detection: filetype={filetype}, compression={compression}')
    if engine == 'auto':
        if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
            return 'duckdb'
        return 'iterable'
    return engine


class StatProcessor:
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
        from rich import print
        from rich.table import Table

        # Get engine preference and detect appropriate engine
        engine = get_option(options, 'engine') or 'auto'
        filetype = get_option(options, 'format_in')
        detected_engine = _detect_engine(fromfile, engine, filetype)
        
        logging.info(f'Using {detected_engine} engine for statistics computation')
        
        # Use DuckDB engine if selected
        if detected_engine == 'duckdb':
            try:
                self._stats_duckdb(fromfile, options)
                return
            except duckdb.Error as e:
                # Check if this is a None column reference error (which is handled gracefully)
                error_msg = str(e)
                if "None" in error_msg and ("not found" in error_msg or "Referenced column" in error_msg):
                    # None column reference - this is expected and handled, suppress warning
                    logging.debug(f'DuckDB stats: None column reference detected, falling back to iterable: {e}')
                else:
                    # DuckDB-specific errors (query failures, parsing errors, etc.)
                    logging.warning(f'DuckDB stats failed (DuckDB error), falling back to iterable: {e}')
                detected_engine = 'iterable'
            except Exception as e:
                # Check if this is a None column reference error
                error_msg = str(e)
                if "None" in error_msg and ("not found" in error_msg or "Referenced column" in error_msg):
                    # None column reference - suppress warning
                    logging.debug(f'DuckDB stats: None column reference detected, falling back to iterable: {e}')
                else:
                    # Other errors (file not found, permission errors, etc.)
                    logging.warning(f'DuckDB stats failed, falling back to iterable: {e}')
                detected_engine = 'iterable'
        
        # Use iterable engine (existing implementation)
        if detected_engine == 'iterable':
            self._stats_iterable(fromfile, options)
        else:
            logging.error(f'Unsupported engine: {detected_engine}')
            raise ValueError(f'Engine {detected_engine} not supported')

    def _compute_duckdb_basic_stats(self, fromfile, filetype):
        """Compute basic statistics using DuckDB's duckdb_decompose with summarize.
        
        Args:
            fromfile: Path to input file
            filetype: File type ('csv', 'jsonl', 'json', etc.)
        
        Returns:
            tuple: (fielddata, fieldtypes, total_count) dictionaries matching iterable format
                - fielddata: Dict mapping field paths to statistics dicts
                - fieldtypes: Dict mapping field paths to type distribution dicts
                - total_count: Total number of records
        """
        # Call duckdb_decompose with use_summarize=True to get statistics
        # Use default limit (10000000) to process all rows - don't pass None as it creates invalid SQL
        columns_raw = duckdb_decompose(
            filename=fromfile,
            filetype=filetype,
            path='*',
            limit=10000000,  # Process all rows (default limit)
            recursive=True,
            ignore_errors=True,
            use_summarize=True
        )
        
        fielddata = {}
        fieldtypes = {}
        total_count = 0
        
        # Log if columns_raw is empty to help debug
        if not columns_raw:
            logging.debug('duckdb_decompose returned empty result - no columns found')
        
        # Process results from duckdb_decompose
        # Format: [field_path, base_type, is_array, unique_count, total_count, uniqueness_percentage]
        for column in columns_raw:
            if len(column) < 6:
                continue  # Skip incomplete entries
            
            field_path = column[0]
            base_type = column[1]
            is_array = column[2] == 'True'
            
            # Safely extract unique_count and total_count
            try:
                # column[3] and column[4] should be strings from duckdb_decompose
                unique_count_str = str(column[3]) if column[3] is not None else "0"
                count_str = str(column[4]) if column[4] is not None else "0"
                unique_count = int(unique_count_str) if unique_count_str.isdigit() else 0
                count = int(count_str) if count_str.isdigit() else 0
            except (ValueError, TypeError, IndexError, AttributeError):
                unique_count = 0
                count = 0
            
            try:
                uniqueness_percentage = float(column[5]) if column[5] else 0.0
            except (ValueError, TypeError, IndexError):
                uniqueness_percentage = 0.0
            
            # Track maximum total count (should be same for all fields, but use max)
            if count > total_count:
                total_count = count
            
            # Skip fields with empty names, None values, or invalid paths
            if not field_path or not isinstance(field_path, str) or field_path == "None":
                continue
            if field_path.startswith('.') or (field_path and field_path[0].isdigit()):
                continue
            
            # Initialize fielddata structure matching iterable format
            if field_path not in fielddata:
                fielddata[field_path] = {
                    'key': field_path,
                    'uniq': {},  # Will be populated later by dictionary construction
                    'n_uniq': unique_count,
                    'total': count,
                    'share_uniq': uniqueness_percentage,
                    'minlen': None,  # Will be computed separately
                    'maxlen': 0,
                    'avglen': 0.0,
                    'totallen': 0
                }
            
            # Initialize fieldtypes structure
            # Map DuckDB types to our type system
            # Note: We'll do proper type detection from samples later, this is just initial
            if field_path not in fieldtypes:
                # Map DuckDB types to our type names
                type_mapping = {
                    'VARCHAR': 'str',
                    'BIGINT': 'int',
                    'INTEGER': 'int',
                    'DOUBLE': 'float',
                    'FLOAT': 'float',
                    'BOOLEAN': 'bool',
                    'DATE': 'date',
                    'TIMESTAMP': 'date',
                    'JSON': 'str'  # JSON fields treated as strings initially
                }
                mapped_type = type_mapping.get(base_type, 'str')
                
                fieldtypes[field_path] = {
                    'key': field_path,
                    'types': {mapped_type: count}  # Initial type distribution
                }
        
        return fielddata, fieldtypes, total_count

    def _compute_duckdb_length_stats(self, fromfile, filetype, field_paths):
        """Compute length statistics (minlen, maxlen, avglen) for each field using DuckDB.
        
        Args:
            fromfile: Path to input file
            filetype: File type ('csv', 'jsonl', 'json', etc.)
            field_paths: List of field paths to compute length statistics for
        
        Returns:
            dict: Mapping from field_path to dict with 'minlen', 'maxlen', 'avglen' keys
        """
        length_stats = {}
        
        # Determine read function based on file type
        ignore_errors = ', ignore_errors=true'
        if filetype in ['csv', 'tsv']:
            read_func = f"read_csv('{fromfile}'{ignore_errors})"
        elif filetype in ['json', 'jsonl']:
            read_func = f"read_json('{fromfile}'{ignore_errors})"
        else:
            # For other formats (like Parquet), use direct table reference
            read_func = f"'{fromfile}'"
        
        # Compute length statistics for each field path
        for field_path in field_paths:
            # Skip None, empty, or invalid field paths
            if not field_path or not isinstance(field_path, str) or field_path == "None" or field_path.startswith('.') or (field_path and field_path[0].isdigit()):
                logging.debug(f'Skipping invalid field path: {field_path}')
                continue
            try:
                # Handle nested field paths - quote properly for SQL
                # For nested paths like "user.address.city", we need to access via dot notation
                # In DuckDB, we use bracket notation for nested fields: "user"."address"."city"
                field_parts = field_path.split('.')
                # Validate that no path part is None or "None"
                if any(not part or part == "None" or not isinstance(part, str) for part in field_parts):
                    logging.debug(f'Skipping field path with invalid parts: {field_path}')
                    continue
                
                if len(field_parts) == 1:
                    # Simple field path
                    quoted_field = f'"{field_path}"'
                else:
                    # Nested field path - construct path expression
                    # For DuckDB JSON, we might need to use JSON extraction
                    # For CSV with nested structures, this won't work directly
                    # Let's try the bracket notation approach first
                    quoted_field = '.'.join([f'"{part}"' for part in field_parts])
                
                # Construct SQL query to compute length statistics
                # Cast to VARCHAR first, then compute length
                # Handle NULL values - only compute length for non-NULL values
                query = f"""
                SELECT 
                    MIN(CASE WHEN {quoted_field} IS NOT NULL THEN LENGTH(CAST({quoted_field} AS VARCHAR)) ELSE NULL END) as minlen,
                    MAX(CASE WHEN {quoted_field} IS NOT NULL THEN LENGTH(CAST({quoted_field} AS VARCHAR)) ELSE NULL END) as maxlen,
                    AVG(CASE WHEN {quoted_field} IS NOT NULL THEN LENGTH(CAST({quoted_field} AS VARCHAR)) ELSE NULL END) as avglen,
                    COUNT(*) as total_count
                FROM {read_func}
                """
                
                # Execute query
                result = duckdb.sql(query).fetchone()
                
                if result:
                    minlen, maxlen, avglen, total_count = result
                    length_stats[field_path] = {
                        'minlen': int(minlen) if minlen is not None else None,
                        'maxlen': int(maxlen) if maxlen is not None else 0,
                        'avglen': float(avglen) if avglen is not None else 0.0,
                        'total_count': int(total_count) if total_count else 0
                    }
                else:
                    # No results, set defaults
                    length_stats[field_path] = {
                        'minlen': None,
                        'maxlen': 0,
                        'avglen': 0.0,
                        'total_count': 0
                    }
            except Exception as e:
                # If query fails for this field, log and continue with defaults
                logging.debug(f'Failed to compute length stats for field {field_path}: {e}')
                length_stats[field_path] = {
                    'minlen': None,
                    'maxlen': 0,
                    'avglen': 0.0,
                    'total_count': 0
                }
        
        return length_stats

    def _detect_types_from_sample(self, fromfile, filetype, field_paths, show_progress=False):
        """Detect field types by sampling records and using guess_datatype.
        
        This maintains compatibility with iterable engine's type detection logic.
        Uses iterable engine to sample records (fast for small samples) to preserve
        nested structure handling, while leveraging DuckDB for bulk aggregations.
        
        Args:
            fromfile: Path to input file
            filetype: File type ('csv', 'jsonl', 'json', etc.)
            field_paths: List of field paths to detect types for
            show_progress: Whether to show progress indication during sampling
        
        Returns:
            dict: Mapping from field_path to type distribution dictionary
        """
        # Initialize type distributions for each field
        type_distributions = {field_path: {} for field_path in field_paths}
        
        # Use iterable engine for sampling (handles nested structures correctly)
        # This is fast for small samples (10000 records) and maintains accuracy
        sample_limit = 10000
        iterableargs = {}
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        
        try:
            count = 0
            # Wrap with progress bar if requested
            iterable_wrapped = iterable
            if show_progress:
                iterable_wrapped = tqdm(iterable, total=sample_limit, 
                                       desc="Sampling for type detection", 
                                       unit="rows", leave=False)
            
            for item in iterable_wrapped:
                if count >= sample_limit:
                    break
                count += 1
                
                # Flatten the item using dict_generator (same as iterable engine)
                try:
                    dk = dict_generator(item)
                    for i in dk:
                        # Skip invalid paths (same logic as iterable engine)
                        if len(i) == 0:
                            continue
                        if i[0].isdigit():
                            continue
                        if len(i[0]) == 1:
                            continue
                        
                        # Build field path
                        k = '.'.join(i[:-1])
                        v = i[-1]
                        
                        # Only process fields we care about
                        if k not in field_paths:
                            continue
                        
                        # Detect type using guess_datatype (same as iterable engine)
                        thetype = guess_datatype(v, self.qd)['base']
                        
                        # Update type distribution
                        if k not in type_distributions:
                            type_distributions[k] = {}
                        type_distributions[k][thetype] = type_distributions[k].get(thetype, 0) + 1
                except Exception as e:
                    # If processing this record fails, skip it
                    logging.debug(f'Failed to process sample record for type detection: {e}')
                    continue
                    
        except Exception as e:
            # If sampling fails, log warning and continue with empty distributions
            logging.warning(f'Failed to sample records for type detection: {e}')
        finally:
            iterable.close()
        
        return type_distributions

    def _compute_duckdb_dictionaries(self, fromfile, filetype, fielddata, finfields, dictshare):
        """Compute value frequency dictionaries for low-cardinality fields using DuckDB GROUP BY.
        
        Args:
            fromfile: Path to input file
            filetype: File type ('csv', 'jsonl', 'json', etc.)
            fielddata: Dictionary of field statistics (used to identify low-cardinality fields)
            finfields: Dictionary mapping field paths to final types
            dictshare: Threshold percentage for dictionary construction (fields below this get dictionaries)
        
        Returns:
            dict: Mapping from field_path to dictionary structure {'items': {value: count}, 'count': n_uniq, 'type': field_type}
        """
        dictionaries = {}
        
        # Determine read function based on file type
        ignore_errors = ', ignore_errors=true'
        if filetype in ['csv', 'tsv']:
            read_func = f"read_csv('{fromfile}'{ignore_errors})"
        elif filetype in ['json', 'jsonl']:
            read_func = f"read_json('{fromfile}'{ignore_errors})"
        else:
            read_func = f"'{fromfile}'"
        
        # Identify fields that need dictionaries (uniqueness percentage below dictshare)
        for field_path, fd in fielddata.items():
            if fd['share_uniq'] >= dictshare:
                continue  # Skip high-cardinality fields
            
            # Skip None, empty, or invalid field paths
            if not field_path or not isinstance(field_path, str) or field_path == "None" or field_path.startswith('.') or (field_path and field_path[0].isdigit()):
                logging.debug(f'Skipping invalid field path for dictionary: {field_path}')
                # Create empty dictionary entry
                field_type = finfields.get(field_path, 'str')
                dictionaries[field_path] = {
                    'items': {},
                    'count': 0,
                    'type': field_type
                }
                continue
            
            try:
                # Construct field reference for SQL query
                field_parts = field_path.split('.')
                # Validate that no path part is None or "None"
                if any(not part or part == "None" or not isinstance(part, str) for part in field_parts):
                    logging.debug(f'Skipping dictionary field path with invalid parts: {field_path}')
                    # Create empty dictionary entry
                    field_type = finfields.get(field_path, 'str')
                    dictionaries[field_path] = {
                        'items': {},
                        'count': 0,
                        'type': field_type
                    }
                    continue
                
                if len(field_parts) == 1:
                    # Simple field path - just quote it
                    quoted_field = f'"{field_path}"'
                else:
                    # Nested field path - use dot notation with quoted parts
                    quoted_field = '.'.join([f'"{part}"' for part in field_parts])
                
                # Construct SQL query to get value frequencies
                # SELECT field_value, COUNT(*) as freq FROM ... GROUP BY field_value ORDER BY freq DESC
                query = f"""
                SELECT 
                    {quoted_field} as value,
                    COUNT(*) as freq
                FROM {read_func}
                WHERE {quoted_field} IS NOT NULL
                GROUP BY {quoted_field}
                ORDER BY freq DESC
                """
                
                # Execute query and fetch all results
                results = duckdb.sql(query).fetchall()
                
                # Build dictionary structure matching iterable engine format
                items_dict = {}
                for value, freq in results:
                    # Convert value to string for consistency with iterable engine
                    value_str = str(value) if value is not None else ''
                    items_dict[value_str] = int(freq)
                
                # Get field type and unique count
                field_type = finfields.get(field_path, 'str')
                n_uniq = len(items_dict)
                
                # Build dictionary structure
                dictionaries[field_path] = {
                    'items': items_dict,
                    'count': n_uniq,
                    'type': field_type
                }
                
            except Exception as e:
                # If dictionary construction fails for this field, log and skip
                logging.debug(f'Failed to build dictionary for field {field_path}: {e}')
                # Create empty dictionary entry
                field_type = finfields.get(field_path, 'str')
                dictionaries[field_path] = {
                    'items': {},
                    'count': 0,
                    'type': field_type
                }
        
        return dictionaries

    def _stats_duckdb(self, fromfile, options):
        """Compute statistics using DuckDB engine.
        
        This is the main entry point for DuckDB-based statistics computation.
        It orchestrates all the DuckDB operations and combines results.
        
        Args:
            fromfile: Path to input file
            options: Dictionary of options (same as stats method)
        """
        from rich import print
        from rich.table import Table
        
        # Get progress control option (default: show progress)
        show_progress = get_option(options, 'progress') is not False
        if 'no_progress' in options and options['no_progress']:
            show_progress = False
        
        # Detect file type if not provided
        filetype = get_option(options, 'format_in')
        if filetype is None:
            ftype = detect_file_type(fromfile)
            if ftype['success']:
                filetype = ftype['datatype'].id()
        
        if filetype is None:
            raise ValueError(f'Could not detect file type for {fromfile}')
        
        dictshare = get_option(options, 'dictshare')
        if dictshare and str(dictshare).isdigit():
            dictshare = int(dictshare)
        else:
            dictshare = DEFAULT_DICT_SHARE
        
        # Phase 0: Count rows for progress indication (fast COUNT query)
        total_count = 0
        if show_progress:
            try:
                ignore_errors = ', ignore_errors=true'
                if filetype in ['json', 'jsonl']:
                    query_str = f"SELECT COUNT(*) FROM read_json('{fromfile}'{ignore_errors})"
                elif filetype in ['csv', 'tsv']:
                    query_str = f"SELECT COUNT(*) FROM read_csv('{fromfile}'{ignore_errors})"
                else:
                    query_str = f"SELECT COUNT(*) FROM '{fromfile}'"
                with tqdm(desc="Counting rows", unit="rows", leave=False, total=None) as pbar:
                    total_count = duckdb.sql(query_str).fetchone()[0]
                    pbar.total = total_count
                    pbar.update(total_count)
            except Exception as e:
                logging.debug(f'Failed to count rows for progress: {e}')
        
        # Phase 1: Get basic statistics using duckdb_decompose
        if show_progress and total_count > 0:
            with tqdm(desc="Computing statistics", unit="rows", 
                     total=total_count, initial=total_count, leave=False) as pbar:
                fielddata, fieldtypes, computed_count = self._compute_duckdb_basic_stats(fromfile, filetype)
                # Use computed count if we didn't get it from Phase 0
                if total_count == 0:
                    total_count = computed_count
                    pbar.total = total_count
                # Mark statistics computation as complete
                pbar.update(0)  # Already at total, just refresh display
        else:
            fielddata, fieldtypes, computed_count = self._compute_duckdb_basic_stats(fromfile, filetype)
            if total_count == 0:
                total_count = computed_count
        
        # Check if we got any fields - if empty, fall back to iterable
        if not fielddata:
            logging.warning('DuckDB stats returned no fields from duckdb_decompose, falling back to iterable engine')
            raise ValueError('No fields extracted - DuckDB returned empty result')
        
        # Phase 2: Compute length statistics (minlen, maxlen, avglen)
        # Filter out None, empty, or invalid field paths before processing
        field_paths = [fp for fp in fielddata.keys() if fp and isinstance(fp, str) and fp != "None" and not fp.startswith('.') and not fp[0].isdigit()]
        length_stats = self._compute_duckdb_length_stats(fromfile, filetype, field_paths)
        
        # Merge length statistics into fielddata
        for field_path, stats in length_stats.items():
            if field_path in fielddata:
                fielddata[field_path]['minlen'] = stats['minlen']
                fielddata[field_path]['maxlen'] = stats['maxlen']
                fielddata[field_path]['avglen'] = stats['avglen']
                # Calculate totallen for consistency (avglen * total)
                if stats['avglen'] and stats['total_count']:
                    fielddata[field_path]['totallen'] = int(stats['avglen'] * stats['total_count'])
        
        # Phase 3: Type detection from samples (hybrid approach)
        type_distributions = self._detect_types_from_sample(fromfile, filetype, field_paths, show_progress)
        
        # Merge type distributions into fieldtypes
        for field_path, type_dist in type_distributions.items():
            if field_path in fieldtypes:
                # Update fieldtypes with sampled type distribution
                fieldtypes[field_path]['types'] = type_dist
            else:
                # Create new entry if not found
                fieldtypes[field_path] = {
                    'key': field_path,
                    'types': type_dist
                }
        
        # Initialize profile structure
        profile = {'version': 1.0}
        profile['count'] = total_count
        profile['num_fields'] = len(fielddata)
        
        # Determine final field types (matching iterable logic)
        finfields = {}
        for fd in fieldtypes.values():
            fdt = list(fd['types'].keys())
            if 'empty' in fdt:
                del fd['types']['empty']
            types_keys = list(fd['types'].keys())
            if len(types_keys) != 1:
                ftype = 'str'
            else:
                ftype = types_keys[0]
            finfields[fd['key']] = ftype
        
        profile['fieldtypes'] = finfields
        
        # Phase 4: Dictionary construction for low-cardinality fields
        dictionaries = self._compute_duckdb_dictionaries(fromfile, filetype, fielddata, finfields, dictshare)
        
        # Build dictkeys list and populate dicts
        dictkeys = []
        dicts = {}
        profile['fields'] = []
        for fd in fielddata.values():
            field = {'key': fd['key'], 'is_uniq': 0 if fd['share_uniq'] < 100 else 1}
            profile['fields'].append(field)
            if fd['share_uniq'] < dictshare:
                dictkeys.append(fd['key'])
                # Use dictionary from DuckDB computation
                if fd['key'] in dictionaries:
                    dicts[fd['key']] = dictionaries[fd['key']]
                else:
                    # Fallback if dictionary construction failed
                    field_type = finfields.get(fd['key'], 'str')
                    dicts[fd['key']] = {'items': {}, 'count': fd['n_uniq'], 'type': field_type}
        
        profile['dictkeys'] = dictkeys
        profile['dicts'] = dicts  # Store dictionaries in profile (though not displayed in table)
        
        # Store dictionaries in fielddata for compatibility with iterable engine output format
        # Note: The iterable engine stores uniq dictionaries in fielddata and then deletes them
        # We're building them separately but need to clean up the structure
        for k, v in fielddata.items():
            if 'uniq' in v:
                del v['uniq']
            fielddata[k] = v
        
        profile['debug'] = {'fieldtypes': fieldtypes.copy(), 'fielddata': fielddata, 'dicts': dicts}
        
        # Display statistics table
        self._display_statistics_table(fielddata, finfields, dictkeys)

    def _stats_iterable(self, fromfile, options):
        """Compute statistics using iterable engine (row-by-row processing).
        
        This is the original implementation, now refactored into a separate method.
        """
        from rich import print
        from rich.table import Table

        iterableargs = get_iterable_options(options)
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        dictshare = get_option(options, 'dictshare')

        if dictshare and dictshare.isdigit():
            dictshare = int(dictshare)
        else:
            dictshare = DEFAULT_DICT_SHARE

        profile = {'version': 1.0}
        fielddata = {}
        fieldtypes = {}

        #    data = json.load(open(profile['filename']))
        count = 0
        nfields = 0

        # Get progress control option (default: show progress)
        show_progress = get_option(options, 'progress') is not False
        if 'no_progress' in options and options['no_progress']:
            show_progress = False

        # process data items one by one
        logging.debug(f'Start processing {fromfile}')
        start_time = time.time()
        try:
            # Wrap iterable with tqdm if progress should be shown
            if show_progress:
                iterable_wrapped = tqdm(iterable, desc="Analyzing statistics", unit="rows")
            else:
                iterable_wrapped = iterable

            # Use context manager for tqdm to ensure proper cleanup
            if show_progress:
                with iterable_wrapped as pbar:
                    for item in pbar:
                        count += 1
                        dk = dict_generator(item)
                        if count % 1000 == 0:
                            logging.debug('Processing %d records of %s' % (count, fromfile))
                            # Update throughput in progress bar
                            elapsed = time.time() - start_time
                            if elapsed > 0:
                                throughput = count / elapsed
                                pbar.set_postfix({"throughput": f"{throughput:.0f} rows/s"})
                        for i in dk:
                            #            print(i)
                            k = '.'.join(i[:-1])
                            if len(i) == 0: continue
                            if i[0].isdigit(): continue
                            if len(i[0]) == 1: continue
                            v = i[-1]
                            if k not in fielddata:  # Use direct dict membership check instead of list()
                                fielddata[k] = {'key': k, 'uniq': {}, 'n_uniq': 0, 'total': 0, 'share_uniq': 0.0,
                                                'minlen': None, 'maxlen': 0, 'avglen': 0, 'totallen': 0}
                            fd = fielddata[k]
                            uniqval = fd['uniq'].get(v, 0)
                            fd['uniq'][v] = uniqval + 1
                            fd['total'] += 1
                            if uniqval == 0:
                                fd['n_uniq'] += 1
                                fd['share_uniq'] = (fd['n_uniq'] * 100.0) / fd['total']
                            fl = len(str(v))
                            if fd['minlen'] is None:
                                fd['minlen'] = fl
                            else:
                                fd['minlen'] = fl if fl < fd['minlen'] else fd['minlen']
                            fd['maxlen'] = fl if fl > fd['maxlen'] else fd['maxlen']
                            fd['totallen'] += fl
                            fielddata[k] = fd
                            if k not in fieldtypes:  # Use direct dict membership check instead of list()
                                fieldtypes[k] = {'key': k, 'types': {}}
                            fd = fieldtypes[k]
                            thetype = guess_datatype(v, self.qd)['base']
                            uniqval = fd['types'].get(thetype, 0)
                            fd['types'][thetype] = uniqval + 1
                            fieldtypes[k] = fd
            else:
                for item in iterable_wrapped:
                    count += 1
                    dk = dict_generator(item)
                    if count % 1000 == 0:
                        logging.debug('Processing %d records of %s' % (count, fromfile))
                    for i in dk:
                        #            print(i)
                        k = '.'.join(i[:-1])
                        if len(i) == 0: continue
                        if i[0].isdigit(): continue
                        if len(i[0]) == 1: continue
                        v = i[-1]
                        if k not in fielddata:  # Use direct dict membership check instead of list()
                            fielddata[k] = {'key': k, 'uniq': {}, 'n_uniq': 0, 'total': 0, 'share_uniq': 0.0,
                                            'minlen': None, 'maxlen': 0, 'avglen': 0, 'totallen': 0}
                        fd = fielddata[k]
                        uniqval = fd['uniq'].get(v, 0)
                        fd['uniq'][v] = uniqval + 1
                        fd['total'] += 1
                        if uniqval == 0:
                            fd['n_uniq'] += 1
                            fd['share_uniq'] = (fd['n_uniq'] * 100.0) / fd['total']
                        fl = len(str(v))
                        if fd['minlen'] is None:
                            fd['minlen'] = fl
                        else:
                            fd['minlen'] = fl if fl < fd['minlen'] else fd['minlen']
                        fd['maxlen'] = fl if fl > fd['maxlen'] else fd['maxlen']
                        fd['totallen'] += fl
                        fielddata[k] = fd
                        if k not in fieldtypes:  # Use direct dict membership check instead of list()
                            fieldtypes[k] = {'key': k, 'types': {}}
                        fd = fieldtypes[k]
                        thetype = guess_datatype(v, self.qd)['base']
                        uniqval = fd['types'].get(thetype, 0)
                        fd['types'][thetype] = uniqval + 1
                        fieldtypes[k] = fd
        finally:
            iterable.close()
        #        print count
        for k, v in fielddata.items():  # Use dict.items() directly, no list() conversion
            fielddata[k]['share_uniq'] = (v['n_uniq'] * 100.0) / v['total']
            fielddata[k]['avglen'] = v['totallen'] / v['total']
        profile['count'] = count
        profile['num_fields'] = nfields

        # Determine field types first so we can use them when building dicts
        finfields = {}
        for fd in fieldtypes.values():  # Use dict.values() directly, no list() conversion
            fdt = list(fd['types'].keys())  # Keep list() here as we need to check membership and modify
            if 'empty' in fdt:
                del fd['types']['empty']
            types_keys = list(fd['types'].keys())  # Need list for len() and indexing
            if len(types_keys) != 1:
                ftype = 'str'
            else:
                ftype = types_keys[0]
            finfields[fd['key']] = ftype

        profile['fieldtypes'] = finfields

        dictkeys = []
        dicts = {}
        #        print(profile)
        profile['fields'] = []
        for fd in fielddata.values():  # Use dict.values() directly, no list() conversion
            #            print(fd['key'])  # , fd['n_uniq'], fd['share_uniq'], fieldtypes[fd['key']]
            field = {'key': fd['key'], 'is_uniq': 0 if fd['share_uniq'] < 100 else 1}
            profile['fields'].append(field)
            if fd['share_uniq'] < dictshare:
                dictkeys.append(fd['key'])
                # Use determined field type instead of defaulting to 'str'
                field_type = finfields.get(fd['key'], 'str')
                dicts[fd['key']] = {'items': fd['uniq'], 'count': fd['n_uniq'],
                                    'type': field_type}
        #            for k, v in fd['uniq'].items():
        #                print fd['key'], k, v
        profile['dictkeys'] = dictkeys

        for k, v in fielddata.items():  # Use dict.items() directly, no list() conversion
            del v['uniq']
            fielddata[k] = v
        profile['debug'] = {'fieldtypes': fieldtypes.copy(), 'fielddata': fielddata}
        
        # Display statistics table
        self._display_statistics_table(fielddata, finfields, dictkeys)
    
    def _display_statistics_table(self, fielddata, finfields, dictkeys):
        """Display statistics table using Rich library.
        
        Args:
            fielddata: Dictionary of field statistics
            finfields: Dictionary mapping field paths to final types
            dictkeys: List of field paths that are dictionary keys
        """
        from rich import print
        from rich.table import Table
        
        table = []
        for fd in fielddata.values():  # Use dict.values() directly, no list() conversion
            field = [fd['key'], ]
            field.append(finfields.get(fd['key'], 'str'))
            field.append(True if fd['key'] in dictkeys else False)
            field.append(False if fd['share_uniq'] < 100 else True)
            field.append(fd['n_uniq'])
            field.append(fd['share_uniq'])
            field.append(fd['minlen'])
            field.append(fd['maxlen'])
            field.append(fd['avglen'])
            table.append(field)
        headers = ('key', 'ftype', 'is_dictkey', 'is_uniq', 'n_uniq', 'share_uniq', 'minlen', 'maxlen', 'avglen')
        reptable = Table(title="Statistics")
        reptable.add_column(headers[0], justify="left", style="magenta")
        for key in headers[1:-1]:
            reptable.add_column(key, justify="left", style="cyan", no_wrap=True)
        reptable.add_column(headers[-1], justify="right", style="cyan")
        for row in table:
            reptable.add_row(*map(str, row))
        print(reptable)

