# -*- coding: utf8 -*-
from ..utils import get_file_type, get_option
import logging
import orjson
import zipfile
from qddate import DateParser
from ..common.scheme import generate_scheme_from_file
from pydantic import BaseModel
from typing import Optional
import duckdb
import pandas as pd
import yaml
import csv
import json
import tempfile
import io
import os
import xxhash
import tqdm
from pyzstd import ZstdFile
from ..ai.perplexity import get_fields_info, get_description



def column_type_parse(column_type):
    is_array = (column_type[-2:] == '[]') 
    if is_array:
        text = column_type[:-2]
    else:
        text = column_type
    if text[:6] == 'STRUCT':        
        atype = text[:6]
    elif text[:4] == 'JSON':
        atype = 'VARCHAR'
    else:
        atype = text
    return [atype, str(is_array)]


def get_schema_key(fields):
    return xxhash.xxh64('|'.join(sorted(fields))).hexdigest()


def duckdb_decompose(filename:str=None, frame:pd.DataFrame=None, filetype:str=None, path:str="*", limit:int=10000000, recursive:bool=True, root:str="", ignore_errors:bool=True):
    """Decomposes file or data frame structure"""  
    text_ignore = ', ignore_errors=true' if ignore_errors else ''
    if filetype in ['csv', 'tsv']:
        read_func = f"read_csv('{filename}'{text_ignore}, sample_size={limit})"
    elif filetype in ['json', 'jsonl']:
        read_func = f"read_json('{filename}'{text_ignore})"
    else:
        read_func = f"'{filename}'"
    if path == '*':
        if filename is not None:
            data = duckdb.sql(f"describe select {path} from {read_func} limit {limit}").fetchall()
        else:
            data = duckdb.sql(f"describe select {path} from frame limit {limit}").fetchall()
    else:
         path_parts = path.split('.')
         query = None
         if len(path_parts) == 1:
            if filename is not None:
                query = f"describe select unnest(\"{path}\", recursive:=true) from {read_func} limit {limit}"
            else:
                query = f"describe select unnest(\"{path}\", recursive:=true) from frame limit {limit}"
         elif len(path_parts) == 2:
            if filename is not None:
                query = f"describe select unnest(\"{path_parts[1]}\", recursive:=true) from (select unnest(\"{path_parts[0]}\", recursive:=true) from {read_func} limit {limit})"
            else:
                query = f"describe select unnest(\"{path_parts[1]}\", recursive:=true) from (select unnest(\"{path_parts[0]}\", recursive:=true) from frame limit {limit})"
         elif len(path_parts) == 3:
            if filename is not None:
                query = f"describe select unnest(\"{path_parts[2]}\", recursive:=true) from (select unnest(\"{path_parts[1]}\", recursive:=true) from (select unnest(\"{path_parts[0]}\", recursive:=true) from {read_func} limit {limit}))"
            else:
                query = f"describe select unnest(\"{path_parts[2]}\", recursive:=true) from (select unnest(\"{path_parts[1]}\", recursive:=true) from (select unnest(\"{path_parts[0]}\", recursive:=true) from frame limit {limit}))"                
         elif len(path_parts) == 4:
            if filename is not None:
                query = f"describe select unnest(\"{path_parts[2]}.{path_parts[3]}\", recursive:=true) from (select unnest(\"{path_parts[1]}\", recursive:=true) from (select unnest(\"{path_parts[0]}\", recursive:=true) from {read_func} limit {limit}))"
            else:
                query = f"describe select unnest(\"{path_parts[2]}.{path_parts[3]}\", recursive:=true) from (select unnest(\"{path_parts[1]}\", recursive:=true) from (select unnest(\"{path_parts[0]}\", recursive:=true) from frame limit {limit}))"
         data = duckdb.sql(query).fetchall()
    table = []
    for row in data:        
        item = [row[0] if len(root) ==0 else root+ '.' + row[0], ]
        item.extend(column_type_parse(row[1]))
        table.append(item)
        if recursive and item[1] == 'STRUCT':
            subtable = duckdb_decompose(filename, frame, filetype=filetype, path=row[0] if len(root) == 0 else item[0], limit=limit, recursive=recursive, root=item[0], ignore_errors=ignore_errors)
            for subitem in subtable:
                table.append(subitem)
    return table    


class FieldSchema(BaseModel):
    name: str
    ftype: str
    is_array:bool = False
    description: Optional[str] = None
    sem_type:str = None
    sem_url:str = None


class TableSchema(BaseModel):
    """Table schema"""    
    key: Optional[str] = None
    num_cols: int = -1
    is_flat:bool = True
    id:Optional[str] = None    
    fields:Optional[list[FieldSchema]] = []
    description:Optional[str] = None
    files:Optional[list[str]] = []


MAX_SAMPLE_SIZE = 200
DELIMITED_FILES = ['csv', 'tsv']
DUCKABLE_FILE_TYPES = ['csv', 'jsonl', 'json', 'parquet']  
DUCKABLE_CODECS  = ['zst', 'gzip', 'raw']


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
        if field.ftype == 'STRUCT' or field.is_array: is_flat = False                    
        table.is_flat = is_flat                            
    table.num_records = len(objects)    
    return table


def build_schema(filename:str, objects_limit:int=100000):
    fileext = filename.rsplit('.', 1)[-1].lower()
    filetype = fileext
    # Getting total count
    table = TableSchema(id=os.path.basename(filename))               
    # Getting structure
    columns_raw = duckdb_decompose(filename, filetype=filetype, path='*', limit=objects_limit)
    is_flat = True
    table.num_cols = len(columns_raw)
    fieldsnames = []
    for column in columns_raw:                          
        field = FieldSchema(name=column[0], ftype=column[1], is_array=column[2])
        fieldsnames.append(column[0])
        table.fields.append(field)
        if field.ftype == 'STRUCT' or field.is_array: is_flat = False                    
        table.is_flat = is_flat
    table.key = get_schema_key(fieldsnames)
    return table



class Schemer:
    def __init__(self, nodates=True):
        if nodates:
            self.qd = None
        else:
            self.qd = DateParser(generate=True)
        pass

    def extract_schema(self, fromfile, options):
        table = build_schema(fromfile)
        print(yaml.dump(table.model_dump(), Dumper=yaml.Dumper))


    def extract_schema_bulk(self, fromdir, options):
        """Extracts schemes from all data files and writes schema structures"""
        filenames = os.listdir(fromdir)
        files = []
        tables = {}
        for filename in filenames:
            if filename.rsplit('.', 1)[-1] in ['csv', 'json', 'jsonl', 'parquet', 'csv.gz', 'csv.zstd', 'jsonl.zstd']:
                files.append(os.path.join(fromdir, filename))
        mode = options['mode']
        print(f'Found {len(files)} files. Processing mode {mode}')                
        for filename in tqdm.tqdm(files):
            table = build_schema(filename)
            fbase = os.path.basename(filename)
            table.id = table.key
            if mode == 'distinct':
                if table.key not in tables.keys():
                    tables[table.key] = table
                    tables[table.key].files.append(fbase)                
                    print(options)
                    if 'autodoc' in options.keys() and options['autodoc'] and 'lang' in options.keys():                    
                        fields = []
                        for column in table.fields:
                            fields.append(column.name)
                        descriptions = get_fields_info(fields, language=options['lang'])
                        for column in table.fields:
                            if column.name in descriptions.keys(): column.description = descriptions[column.name]                
                else:
                    tables[table.key].files.append(fbase)
            elif mode == 'perfile':
                table.files.append(fbase)                
                if 'autodoc' in options.keys() and options['autodoc'] and 'lang' in options.keys():                    
                    fields = []
                    for column in table.fields:
                        fields.append(column.name)
                    descriptions = get_fields_info(fields, language=options['lang'])
                    for column in table.fields:
                        if column.name in descriptions.keys(): column.description = descriptions[column.name]                
                f = open(os.path.join(options['output'], fbase + '.yaml'), 'w', encoding='utf8')
                f.write(yaml.dump(table.model_dump(), Dumper=yaml.Dumper))
                f.close
        if mode == 'distinct':
            print(f'Total schemas {len(tables)}, files {len(files)}')
        elif mode == 'perfile':
            print(f'Total schemas {len(files)}, files {len(files)}')
        if 'output' in options.keys():
            if mode == 'distinct':
                print(f'Writing schemas')
                for table in tables.values():
                    f = open(os.path.join(options['output'], table.key + '.yaml'), 'w', encoding='utf8')
                    f.write(yaml.dump(table.model_dump(), Dumper=yaml.Dumper))
                    f.close
#            print(yaml.dump(table.model_dump(), Dumper=yaml.Dumper))


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
                infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))

        logging.debug('Start identifying scheme for %s' % (fromfile))
        scheme = generate_scheme_from_file(fileobj=infile, filetype=f_type, delimiter=options['delimiter'], encoding=options['encoding'])
        if options['output']:
            f = open(options['output'], 'w', encoding='utf8')
            f.write(orjson.dumps(scheme, option=orjson.OPT_INDENT_2).decode('utf8'))
            f.close()
        else:
            print(str(orjson.dumps(scheme, option=orjson.OPT_INDENT_2).decode('utf8')))
