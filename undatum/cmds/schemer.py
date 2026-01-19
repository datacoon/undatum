"""Schema generation and extraction module."""
import csv
import glob
import io
import json
import logging
import os
import sys
import tempfile
import zipfile
from typing import Optional

import duckdb
import orjson
import pandas as pd
import tqdm
import xxhash
import yaml
from pydantic import BaseModel
from pyzstd import ZstdFile
from qddate import DateParser

from ..ai import get_ai_service, get_description, get_fields_info
from ..common.schema_utils import duckdb_decompose
from ..common.scheme import generate_scheme_from_file
from ..utils import get_file_type, get_option

try:
    from iterable.helpers.detect import TEXT_DATA_TYPES, detect_file_type
    HAS_ITERABLE_DETECT = True
except ImportError:
    HAS_ITERABLE_DETECT = False


def get_schema_key(fields):
    """Generate hash key for schema based on field names."""
    return xxhash.xxh64('|'.join(sorted(fields))).hexdigest()


class FieldSchema(BaseModel):
    """Schema definition for a data field."""
    name: str
    ftype: str
    is_array: bool = False
    description: Optional[str] = None
    sem_type: str = None
    sem_url: str = None


class TableSchema(BaseModel):
    """Table schema definition."""
    key: Optional[str] = None
    num_cols: int = -1
    num_records: int = -1
    is_flat: bool = True
    id: Optional[str] = None
    fields: Optional[list[FieldSchema]] = []
    description: Optional[str] = None
    files: Optional[list[str]] = []
    success: bool = True
    error: Optional[str] = None


MAX_SAMPLE_SIZE = 200
DELIMITED_FILES = ['csv', 'tsv']

from ..constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES


def table_from_objects(objects:list, id:str, objects_limit:int, use_pandas:bool=False, filetype='csv', autodoc:bool=False, lang:str='English'):
    """Reconstructs table schema from list of objects"""
    table = TableSchema(id=id)
    table.num_records = len(objects)
    if autodoc:
       f = io.StringIO()
       writer = csv.writer(f)
       writer.writerows(objects[:MAX_SAMPLE_SIZE])
       table.description = get_description(f.getvalue(), language=lang)
    if use_pandas:
        df = pd.DataFrame(objects)
        columns_raw = duckdb_decompose(frame=df, path='*', limit=objects_limit)
    else:
        tfile = tempfile.NamedTemporaryFile(suffix='.' + filetype, mode='w', encoding='utf8', delete=False)
        tfile.close()
        tfile_real = ZstdFile(tfile.name, mode='w', level_or_option=9)
        wrapper = io.TextIOWrapper(tfile_real, encoding='utf8', write_through=True)
        if filetype == 'csv':
            writer = csv.writer(wrapper)
            writer.writerows(objects[:objects_limit])
        elif filetype == 'jsonl':
            for row in objects[:objects_limit]:
                wrapper.write(json.dumps(row) + '\n')
        tfile_real.close()
        # Getting structure
        columns_raw = duckdb_decompose(tfile.name, filetype=filetype, path='*', limit=objects_limit)
        os.remove(tfile.name)
    is_flat = True
    table.num_cols = len(columns_raw)

    for column in columns_raw:
        field = FieldSchema(name=column[0], ftype=column[1], is_array=column[2])
        table.fields.append(field)
        if field.ftype == 'STRUCT' or field.is_array:
            is_flat = False
        table.is_flat = is_flat
    table.num_records = len(objects)
    return table


def build_schema(filename:str, objects_limit:int=100000, engine:str='auto', filetype:str=None, compression:str=None):
    """Build schema from file by analyzing sample of objects.

    Args:
        filename: Path to input file
        objects_limit: Maximum number of objects to analyze
        engine: Processing engine ('auto', 'duckdb', or 'iterable')
        filetype: Override file type detection
        compression: Override compression detection

    Returns:
        TableSchema object with schema information
    """
    table = TableSchema(id=os.path.basename(filename))

    # Validate file exists and is readable
    if not os.path.exists(filename):
        table.success = False
        table.error = f"File not found: {filename}"
        return table
    if not os.access(filename, os.R_OK):
        table.success = False
        table.error = f"Cannot read file: {filename}"
        return table

    try:
        # Detect file type and compression
        if HAS_ITERABLE_DETECT and (filetype is None or compression is None):
            ftype = detect_file_type(filename)
            if ftype.get('success'):
                detected_filetype = ftype['datatype'].id() if hasattr(ftype['datatype'], 'id') else str(ftype['datatype'])
                detected_compression = ftype['codec'].id() if ftype.get('codec') and hasattr(ftype['codec'], 'id') else (str(ftype['codec']) if ftype.get('codec') else 'raw')
                if filetype is None:
                    filetype = detected_filetype
                if compression is None:
                    compression = detected_compression
            else:
                # Fallback to extension-based detection
                if filetype is None:
                    fileext = filename.rsplit('.', 1)[-1].lower()
                    filetype = fileext
                if compression is None:
                    compression = 'raw'
        else:
            # Fallback to extension-based detection
            if filetype is None:
                fileext = filename.rsplit('.', 1)[-1].lower()
                filetype = fileext
            if compression is None:
                compression = 'raw'

        # Determine engine
        if engine == 'auto':
            if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
                engine = 'duckdb'
            else:
                engine = 'iterable'

        # Count records (only for DuckDB engine)
        if engine == 'duckdb':
            try:
                text_ignore = ', ignore_errors=true'
                if filetype in ['json', 'jsonl']:
                    query_str = f"select count(*) from read_json('{filename}'{text_ignore})"
                elif filetype in ['csv', 'tsv']:
                    query_str = f"select count(*) from read_csv('{filename}'{text_ignore})"
                else:
                    query_str = f"select count(*) from '{filename}'"
                num_records = duckdb.sql(query_str).fetchall()[0][0]
                table.num_records = num_records
            except Exception as e:
                logging.warning(f"Could not count records: {e}")
                table.num_records = -1
        else:
            # For iterable engine, we can't easily count without reading all records
            table.num_records = -1

        # Getting structure
        if engine == 'duckdb':
            columns_raw = duckdb_decompose(filename, filetype=filetype, path='*', limit=objects_limit)
        else:
            # For iterable engine, we'd need to implement iterable-based schema extraction
            # For now, fall back to DuckDB if possible, otherwise raise error
            logging.warning(f"Schema extraction with iterable engine not yet fully implemented for {filetype} files. Falling back to DuckDB.")
            try:
                columns_raw = duckdb_decompose(filename, filetype=filetype, path='*', limit=objects_limit)
            except Exception as e:
                raise ValueError(f"Schema extraction failed for {filetype} files: {e}. Try using --engine duckdb or a different file format.")

        is_flat = True
        table.num_cols = len(columns_raw)
        fieldsnames = []
        for column in columns_raw:
            field = FieldSchema(name=column[0], ftype=column[1], is_array=column[2])
            fieldsnames.append(column[0])
            table.fields.append(field)
            if field.ftype == 'STRUCT' or field.is_array:
                is_flat = False
            table.is_flat = is_flat
        table.key = get_schema_key(fieldsnames)
        table.success = True
        return table
    except Exception as e:
        logging.error(f"Schema extraction failed: {e}")
        table.success = False
        table.error = str(e)
        return table



def _format_number(num):
    """Format number with commas for readability."""
    if num is None or num == -1:
        return "N/A"
    return f"{num:,}"


def _duckdb_to_cerberus_type(duckdb_type: str, is_array: bool) -> dict:
    """Convert DuckDB type to Cerberus schema format.

    Args:
        duckdb_type: DuckDB type string (e.g., 'VARCHAR', 'BIGINT', 'DOUBLE')
        is_array: Whether the field is an array

    Returns:
        Cerberus schema dictionary for the field
    """
    type_map = {
        'VARCHAR': 'string',
        'BIGINT': 'integer',
        'INTEGER': 'integer',
        'DOUBLE': 'float',
        'FLOAT': 'float',
        'BOOLEAN': 'boolean',
        'DATE': 'datetime',
        'TIMESTAMP': 'datetime',
        'STRUCT': 'dict',
        'JSON': 'string'
    }

    cerberus_type = type_map.get(duckdb_type, 'string')

    if is_array:
        return {
            'type': 'list',
            'schema': {'type': cerberus_type}
        }
    else:
        return {'type': cerberus_type}


def _duckdb_to_json_schema_type(duckdb_type: str, is_array: bool) -> dict:
    """Convert DuckDB type to JSON Schema type.

    Args:
        duckdb_type: DuckDB type string
        is_array: Whether the field is an array

    Returns:
        JSON Schema type string or dict
    """
    type_map = {
        'VARCHAR': 'string',
        'BIGINT': 'integer',
        'INTEGER': 'integer',
        'DOUBLE': 'number',
        'FLOAT': 'number',
        'BOOLEAN': 'boolean',
        'DATE': 'string',
        'TIMESTAMP': 'string',
        'STRUCT': 'object',
        'JSON': 'string'
    }

    json_type = type_map.get(duckdb_type, 'string')

    if is_array:
        return {'type': 'array', 'items': {'type': json_type}}
    else:
        return json_type


def _duckdb_to_avro_type(duckdb_type: str, is_array: bool) -> str | list:
    """Convert DuckDB type to Avro type.

    Args:
        duckdb_type: DuckDB type string
        is_array: Whether the field is an array

    Returns:
        Avro type string or array
    """
    type_map = {
        'VARCHAR': 'string',
        'BIGINT': 'long',
        'INTEGER': 'int',
        'DOUBLE': 'double',
        'FLOAT': 'float',
        'BOOLEAN': 'boolean',
        'DATE': 'string',
        'TIMESTAMP': 'string',
        'STRUCT': 'record',
        'JSON': 'string'
    }

    avro_type = type_map.get(duckdb_type, 'string')

    if is_array:
        return {'type': 'array', 'items': avro_type}
    else:
        return avro_type


def _to_cerberus(table: TableSchema) -> dict:
    """Convert TableSchema to Cerberus schema format.

    Args:
        table: TableSchema object

    Returns:
        Cerberus schema dictionary
    """
    schema = {}
    for field in table.fields:
        field_schema = _duckdb_to_cerberus_type(field.ftype, field.is_array)
        if field.description:
            field_schema['description'] = field.description
        schema[field.name] = field_schema
    return schema


def _to_json_schema(table: TableSchema) -> dict:
    """Convert TableSchema to JSON Schema format.

    Args:
        table: TableSchema object

    Returns:
        JSON Schema dictionary
    """
    properties = {}
    required = []

    for field in table.fields:
        field_type = _duckdb_to_json_schema_type(field.ftype, field.is_array)
        field_schema = {'type': field_type} if isinstance(field_type, str) else field_type

        if field.description:
            field_schema['description'] = field.description

        properties[field.name] = field_schema
        # In JSON Schema, we could mark fields as required, but for now we'll make all optional
        # required.append(field.name)

    json_schema = {
        '$schema': 'http://json-schema.org/draft-07/schema#',
        'type': 'object',
        'properties': properties
    }

    if required:
        json_schema['required'] = required

    if table.description:
        json_schema['description'] = table.description

    return json_schema


def _to_avro(table: TableSchema) -> dict:
    """Convert TableSchema to Avro schema format.

    Args:
        table: TableSchema object

    Returns:
        Avro schema dictionary
    """
    fields = []
    for field in table.fields:
        avro_type = _duckdb_to_avro_type(field.ftype, field.is_array)
        field_def = {
            'name': field.name,
            'type': avro_type
        }
        if field.description:
            field_def['doc'] = field.description
        fields.append(field_def)

    avro_schema = {
        'type': 'record',
        'name': table.id or 'Schema',
        'fields': fields
    }

    if table.description:
        avro_schema['doc'] = table.description

    return avro_schema


def _to_parquet(table: TableSchema) -> dict:
    """Convert TableSchema to Parquet schema format.

    Note: Parquet schema is typically defined using Apache Arrow or Parquet's native format.
    This returns a simplified representation that can be used to generate Parquet schemas.

    Args:
        table: TableSchema object

    Returns:
        Dictionary representation of Parquet schema metadata
    """
    fields = []
    for field in table.fields:
        # Parquet types map similarly to Avro
        parquet_type = _duckdb_to_avro_type(field.ftype, field.is_array)
        field_def = {
            'name': field.name,
            'type': parquet_type,
            'nullable': True  # Parquet fields are nullable by default
        }
        if field.description:
            field_def['description'] = field.description
        fields.append(field_def)

    return {
        'fields': fields,
        'metadata': {
            'description': table.description or '',
            'num_records': table.num_records,
            'num_cols': table.num_cols
        }
    }


def _convert_to_format(table: TableSchema, format_name: str) -> dict:
    """Convert TableSchema to specified format.

    Args:
        table: TableSchema object
        format_name: Target format ('cerberus', 'jsonschema', 'avro', 'parquet')

    Returns:
        Schema in the specified format (dictionary)

    Raises:
        ValueError: If format is not supported
    """
    format_map = {
        'cerberus': _to_cerberus,
        'jsonschema': _to_json_schema,
        'json-schema': _to_json_schema,  # Alias
        'avro': _to_avro,
        'parquet': _to_parquet
    }

    converter = format_map.get(format_name.lower())
    if not converter:
        raise ValueError(f"Unsupported format: {format_name}. Supported formats: {', '.join(format_map.keys())}")

    return converter(table)


def _write_schema_output(table, options, output_stream):
    """Write schema to output stream in specified format."""
    from tabulate import tabulate

    # Check if a specific schema format is requested (cerberus, jsonschema, avro, parquet)
    schema_format = options.get('format')
    if schema_format and schema_format.lower() in ['cerberus', 'jsonschema', 'json-schema', 'avro', 'parquet']:
        try:
            converted_schema = _convert_to_format(table, schema_format.lower())
            # All format outputs are JSON-serializable
            json_output = orjson.dumps(converted_schema, option=orjson.OPT_INDENT_2).decode('utf8')
            output_stream.write(json_output)
            output_stream.write('\n')
            return
        except Exception as e:
            logging.error(f"Format conversion failed: {e}")
            # Fall through to default output

    # Default output formats (yaml, json, text)
    if options.get('outtype') == 'json':
        json_output = json.dumps(table.model_dump(), indent=4, ensure_ascii=False)
        output_stream.write(json_output)
        output_stream.write('\n')
    elif options.get('outtype') == 'yaml':
        yaml_output = yaml.dump(table.model_dump(), Dumper=yaml.Dumper)
        output_stream.write(yaml_output)
    else:
        # Text output format
        print("=" * 70, file=output_stream)
        print("SCHEMA EXTRACTION", file=output_stream)
        print("=" * 70, file=output_stream)
        print(file=output_stream)

        # Check for errors
        if not table.success:
            print("ERROR", file=output_stream)
            print("-" * 70, file=output_stream)
            print(f"Failed to extract schema: {table.error}", file=output_stream)
            return

        # File information section
        print("File Information", file=output_stream)
        print("-" * 70, file=output_stream)
        headers = ['Attribute', 'Value']
        reptable = []
        reptable.append(['Filename', str(table.id)])
        reptable.append(['Total records', _format_number(table.num_records)])
        reptable.append(['Total columns', _format_number(table.num_cols)])
        reptable.append(['Structure', 'Flat' if table.is_flat else 'Nested'])
        if table.key:
            reptable.append(['Schema key', table.key])
        print(tabulate(reptable, headers=headers, tablefmt='grid'), file=output_stream)
        print(file=output_stream)

        # Fields section
        if table.fields:
            print("=" * 70, file=output_stream)
            print("FIELDS", file=output_stream)
            print("=" * 70, file=output_stream)
            print(file=output_stream)

            tabheaders = ['Field Name', 'Type', 'Is Array', 'Description']
            table_data = []
            for field in table.fields:
                desc = field.description if field.description else '-'
                table_data.append([
                    field.name,
                    field.ftype,
                    'Yes' if field.is_array else 'No',
                    desc
                ])
            print(tabulate(table_data, headers=tabheaders, tablefmt='grid'), file=output_stream)

            if table.description:
                print(file=output_stream)
                print("Summary:", file=output_stream)
                print("-" * 70, file=output_stream)
                desc_lines = table.description.split('\n')
                for line in desc_lines:
                    if line.strip():
                        print(f"  {line.strip()}", file=output_stream)


class Schemer:
    """Schema generation handler."""
    def __init__(self, nodates=True):
        if nodates:
            self.qd = None
        else:
            self.qd = DateParser(generate=True)
        pass

    def extract_schema(self, fromfile, options):
        """Extract schema from file and output in specified format."""
        engine = options.get('engine', 'auto')
        table = build_schema(fromfile, engine=engine)

        # Apply AI documentation if requested
        if options.get('autodoc'):
            try:
                ai_service = get_ai_service(
                    provider=options.get('ai_provider'),
                    config=options.get('ai_config')
                )
                fields = [f.name for f in table.fields]
                descriptions = get_fields_info(fields,
                                              language=options.get('lang', 'English'),
                                              ai_service=ai_service)
                for field in table.fields:
                    if field.name in descriptions:
                        field.description = descriptions[field.name]
            except Exception as e:
                logging.warning(f"Failed to generate AI documentation: {e}")
                # Continue without AI documentation

        # Determine output destination
        output_file = options.get('output')
        if output_file:
            with open(output_file, 'w', encoding='utf8') as output_stream:
                _write_schema_output(table, options, output_stream)
        else:
            _write_schema_output(table, options, sys.stdout)


    def extract_schema_bulk(self, fromdir, options):
        """Extracts schemes from all data files and writes schema structures"""
        files = []
        tables = {}
        supported_exts = ['csv', 'json', 'jsonl', 'parquet']

        # Support both directory paths and glob patterns
        if os.path.isdir(fromdir):
            # Directory: find all supported files
            for ext in supported_exts:
                pattern = os.path.join(fromdir, f'*.{ext}')
                files.extend(glob.glob(pattern))
                # Also handle compressed files
                for comp in ['gz', 'zstd', 'zst']:
                    pattern = os.path.join(fromdir, f'*.{ext}.{comp}')
                    files.extend(glob.glob(pattern))
        else:
            # Glob pattern: use as-is
            files.extend(glob.glob(fromdir))
            # Also try with common extensions if pattern doesn't match
            if not files:
                for ext in supported_exts:
                    pattern = f"{fromdir}.{ext}"
                    files.extend(glob.glob(pattern))
                    for comp in ['gz', 'zstd', 'zst']:
                        pattern = f"{fromdir}.{ext}.{comp}"
                        files.extend(glob.glob(pattern))
        mode = options.get('mode', 'distinct')
        print(f'Found {len(files)} files. Processing mode {mode}')

        # Initialize AI service if autodoc is enabled
        ai_service = None
        if options.get('autodoc'):
            try:
                ai_service = get_ai_service(
                    provider=options.get('ai_provider'),
                    config=options.get('ai_config')
                )
            except Exception as e:
                logging.warning(f"Failed to initialize AI service: {e}. Disabling autodoc.")
                options['autodoc'] = False

        engine = options.get('engine', 'auto')
        for filename in tqdm.tqdm(files):
            try:
                table = build_schema(filename, engine=engine)
                fbase = os.path.basename(filename)
                table.id = table.key

                # Apply AI documentation if requested
                if options.get('autodoc') and ai_service:
                    fields = [f.name for f in table.fields]
                    descriptions = get_fields_info(fields,
                                                  language=options.get('lang', 'English'),
                                                  ai_service=ai_service)
                    for field in table.fields:
                        if field.name in descriptions:
                            field.description = descriptions[field.name]

                if mode == 'distinct':
                    if table.key not in tables.keys():
                        tables[table.key] = table
                        tables[table.key].files.append(fbase)
                    else:
                        tables[table.key].files.append(fbase)
                elif mode == 'perfile':
                    table.files.append(fbase)
                    if 'output' in options and options['output']:
                        # Determine file extension based on format
                        schema_format = options.get('format')
                        if schema_format and schema_format.lower() in ['cerberus', 'jsonschema', 'json-schema', 'avro', 'parquet']:
                            ext = 'json'  # All format outputs are JSON
                        elif options.get('outtype') == 'json':
                            ext = 'json'
                        elif options.get('outtype') == 'yaml':
                            ext = 'yaml'
                        else:
                            ext = 'txt'
                        output_path = os.path.join(options['output'], fbase + '.' + ext)
                        with open(output_path, 'w', encoding='utf8') as f:
                            _write_schema_output(table, options, f)
            except Exception as e:
                logging.error(f"Failed to process {filename}: {e}")
                continue
        if mode == 'distinct':
            print(f'Total schemas {len(tables)}, files {len(files)}')
        elif mode == 'perfile':
            print(f'Total schemas {len(files)}, files {len(files)}')
        if 'output' in options and options['output']:
            if mode == 'distinct':
                print('Writing schemas')
                for table in tables.values():
                    # Determine file extension based on format
                    schema_format = options.get('format')
                    if schema_format and schema_format.lower() in ['cerberus', 'jsonschema', 'json-schema', 'avro', 'parquet']:
                        ext = 'json'  # All format outputs are JSON
                    elif options.get('outtype') == 'json':
                        ext = 'json'
                    elif options.get('outtype') == 'yaml':
                        ext = 'yaml'
                    else:
                        ext = 'txt'
                    output_path = os.path.join(options['output'],
                                             table.key + '.' + ext)
                    with open(output_path, 'w', encoding='utf8') as f:
                        _write_schema_output(table, options, f)


    def generate_scheme(self, fromfile, options):
        """Generates cerberus scheme from JSON lines or BSON file"""
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        if f_type not in ['jsonl', 'bson', 'csv']:
            print('Only JSON lines, CSV and BSON (.jsonl, .csv, .bson) files supported now')
            return
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            fnames[0]
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        else:
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                infile = open(fromfile, encoding=get_option(options, 'encoding'))

        logging.debug('Start identifying scheme for %s', fromfile)
        scheme = generate_scheme_from_file(fileobj=infile, filetype=f_type,
                                          delimiter=options['delimiter'],
                                          encoding=options['encoding'])
        if options['output']:
            with open(options['output'], 'w', encoding='utf8') as f:
                f.write(orjson.dumps(scheme,
                                    option=orjson.OPT_INDENT_2).decode('utf8'))
        if not options['zipfile']:
            infile.close()
        if options['zipfile']:
            z.close()
        else:
            print(str(orjson.dumps(scheme, option=orjson.OPT_INDENT_2).decode('utf8')))
