#!/usr/bin/env python
# -*- coding: utf8 -*-
import typer
import logging

from .cmds.converter import Converter
from .cmds.selector import Selector
from .cmds.transformer import Transformer
from .cmds.analyzer import Analyzer
from .cmds.statistics import StatProcessor
from .cmds.textproc import TextProcessor
from .cmds.validator import Validator
from .cmds.schemer import Schemer
from .cmds.query import DataQuery
from .cmds.ingester import Ingester

import glob

DEFAULT_BATCH_SIZE = 1000

app = typer.Typer()

#logging.getLogger().addHandler(logging.StreamHandler())
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

def enableVerbose():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO)

@app.command()
def convert(input:str, output:str, delimiter:str=',', compression:str='brotli', encoding:str='utf8', verbose:bool=False, flatten:bool=False, prefix_strip:bool=True, fields:str=None, start_line:int=0, skip_end_rows:int=0, start_page:int=0, tagname:str=None, format_in:str=None, format_out:str=None, zipfile:bool=False):
    """Converts one file to another. Supports XML, CSV, JSON, BSON"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['compression'] = compression
    options['flatten'] = flatten
    options['encoding'] = encoding
    options['prefix_strip'] = prefix_strip
    options['start_line'] = start_line
    options['skip_end_rows'] = skip_end_rows
    options['start_page'] = start_page
    options['tagname'] = tagname
    options['fields'] = fields
    options['format_in'] = format_in
    options['format_out'] = format_out
    options['zipfile'] = zipfile
    acmd = Converter()
    acmd.convert(input, output, options)
    pass

@app.command()
def convertold(input:str, output:str, delimiter:str=',', compression:str='brotli', encoding:str='utf8', verbose:bool=False, flatten:bool=False, prefix_strip:bool=True, fields:str=None, start_line:int=0, skip_end_rows:int=0, start_page:int=0, tagname:str=None, format_in:str=None, format_out:str=None, zipfile:bool=False):
    """Converts one file to another. Supports XML, CSV, JSON, BSON (old implementation)"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['compression'] = compression
    options['flatten'] = flatten
    options['encoding'] = encoding
    options['prefix_strip'] = prefix_strip
    options['start_line'] = start_line
    options['skip_end_rows'] = skip_end_rows
    options['start_page'] = start_page
    options['tagname'] = tagname
    options['fields'] = fields
    options['format_in'] = format_in
    options['format_out'] = format_out
    options['zipfile'] = zipfile
    acmd = Converter()
    acmd.convert_old(input, output, options)
    pass

@app.command()
def uniq(input:str, output:str=None, fields:str=None, delimiter:str=',', encoding:str=None, verbose:bool=False, format_in:str=None, format_out:str=None, zipfile:bool=False, filter:str=None):
    """Returns all unique files of certain field(s)"""
    if verbose:
        enableVerbose()
    options = {}
    options['output'] = output
    options['fields'] = fields
    options['delimiter'] = delimiter
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['format_out'] = format_out
    options['zipfile'] = zipfile
    options['filter'] = filter
    acmd = Selector()
    acmd.uniq(input, options)
    pass


@app.command()
def headers(input:str, output:str=None, fields:str=None, delimiter:str=',', encoding:str=None, limit:int=10000, verbose:bool=False, format_in:str=None, format_out:str=None, zipfile:bool=False, filter:str=None):
    """Returns fieldnames of the file. Supports XML, CSV, JSON, BSON"""
    if verbose:
        enableVerbose()
    options = {}
    options['output'] = output
    options['delimiter'] = delimiter
    options['encoding'] = encoding
    options['limit'] = limit
    options['format_in'] = format_in
    options['format_out'] = format_out
    options['zipfile'] = zipfile
    acmd = Selector()
    acmd.headers(input, options)
    pass

@app.command()
def stats(input:str, output:str=None, dictshare:int=None, format_in:str=None, format_out:str=None, delimiter:str=None, verbose:bool=False, zipfile:bool=False, checkdates:bool=True, encoding:str=None):
    """Returns detailed stats on selected dataset"""
    if verbose:
        enableVerbose()
    options = {}
    options['output'] = output
    options['dictshare'] = dictshare
    options['zipfile'] = zipfile
    options['format_in'] = format_in
    options['format_out'] = format_out
    options['delimiter'] = delimiter
    options['checkdates'] = checkdates
    options['encoding'] = encoding
    options['verbose'] = verbose
    acmd = StatProcessor(nodates=not checkdates)
    acmd.stats(input, options)
    pass

def flatten(input:str, output:str=None, delimiter:str=',', encoding:str='utf8', format_in:str=None, filter:str=None, verbose:bool=False):
    """Flatten data records. Write them as one value per row"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['output'] = output
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['filter'] = filter
    acmd = TextProcessor()
    acmd.flatten(input, options)
    pass


@app.command()
def frequency(input:str, output:str=None, fields:str=None, delimiter:str=",", encoding:str=None, verbose:bool=False, format_in:str=None, format_out:str=None, zipfile:bool=False, filter:str=None):
    """Field value frequency calc"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['fields'] = fields
    options['output'] = output
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['format_out'] = format_out
    options['zipfile'] = zipfile
    options['filter'] = filter
    acmd = Selector()
    acmd.frequency(input, options)
    pass

@app.command()
def select(input:str, output:str=None, fields:str=None, delimiter:str=",", encoding:str=None, verbose:bool=False, format_in:str=None, format_out:str=None, zipfile:bool=False, filter:str=None):
    """Select or re-order columns from file. Supports CSV, JSONl, BSON"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['fields'] = fields
    options['output'] = output
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['format_out'] = format_out
    options['zipfile'] = zipfile
    options['filter'] = filter
    acmd = Selector()
    acmd.select(input, options)
    pass


@app.command()
def split(input:str, output:str=None, fields:str=None, delimiter:str=',', encoding:str="utf8", verbose:bool=False, format_in:str=None, zipfile:bool=False, gzipfile:str=None, chunksize:int=10000, filter:str=None, dirname:str=None):
    """Splits the given file with data into chunks."""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['fields'] = fields
    options['output'] = output
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['zipfile'] = zipfile
    options['gzipfile'] = gzipfile
    options['chunksize'] = chunksize
    options['filter'] = filter
    options['dirname'] = dirname
    acmd = Selector()
    acmd.split(input, options)
    pass

@app.command()
def validate(input:str, output:str=None, fields:str=None, delimiter:str=',', encoding:str='utf8', verbose:bool=False, format_in:str=None, zipfile:bool=False, rule:str=None, filter:str=None, mode:str="invalid"):
    """Validates selected field against validation rule"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['fields'] = fields
    options['output'] = output
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['zipfile'] = zipfile
    options['filter'] = filter
    options['rule'] = rule
    options['mode'] = mode
    acmd = Validator()
    acmd.validate(input, options)
    pass

@app.command()
def apply(input:str, output:str=None, fields:str=None, delimiter:str=",", encoding:str='utf8', verbose:bool=False, format_in:str=None, zipfile:bool=False, script:str=None, filter:str=None):
    """Runs script against each record of input file"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['fields'] = fields
    options['output'] = output
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['zipfile'] = zipfile
    options['filter'] = filter
    options['script'] = script
    acmd = Transformer()
    acmd.script(input, options)
    pass


@app.command()
def scheme(input:str, output:str=None, delimiter:str=',', encoding:str='utf8', verbose:bool=False, format_in:str=None, zipfile:bool=False, stype:str='cerberus'):
    """Generate data schema from file"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['output'] = output
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['zipfile'] = zipfile
    options['stype'] = stype
    acmd = Schemer()
    acmd.generate_scheme(input, options)
    pass



@app.command()
def analyze(input:str, verbose:bool=False, engine:str="auto", use_pandas:bool=False, outtype:str="text", output:str=None, autodoc:bool=False, lang:str="English"):
    """Analyzes given data file and returns human readable insights about it"""
    if verbose:
        enableVerbose()
    options = {}
    options['engine'] = engine
    options['use_pandas'] = use_pandas
    options['outtype'] = outtype
    options['output'] = output
    options['autodoc'] = autodoc
    options['lang'] = lang
    acmd = Analyzer()
    acmd.analyze(input, options)
    pass

@app.command()
def schema(input:str, verbose:bool=False, outtype:str="text", output:str=None, autodoc:bool=False, lang:str="English"):
    """Schema extraction"""
    if verbose:
        enableVerbose()
    options = {}
    options['outtype'] = outtype
    options['output'] = output
    options['autodoc'] = autodoc
    options['lang'] = lang
    acmd = Schemer()
    acmd.extract_schema(input, options)
    pass

@app.command()
def schema_bulk(input:str, verbose:bool=False, outtype:str="text", output:str=None, mode:str="distinct", autodoc:bool=False, lang:str="English"):
    """Schema extraction from many files. Default mode is 'distinct' that creates unique schema files per schema, alternative is 'perfile' that creates a schema per file with same names"""
    if verbose:
        enableVerbose()
    options = {}
    options['outtype'] = outtype
    options['output'] = output
    options['mode'] = mode
    options['autodoc'] = autodoc
    options['lang'] = lang
    acmd = Schemer()
    acmd.extract_schema_bulk(input, options)
    pass

@app.command()
def ingest(input:str, uri:str, db:str, table:str, verbose:bool=False, batch:int=DEFAULT_BATCH_SIZE, dbtype:str="mongodb", totals:bool=False, drop:bool=False, timeout:int=-30, skip:int=None, api_key:str=None, doc_id:str=None):
    """Data ingester"""
    if verbose:
        enableVerbose()
    options = {}
    options['dbtype'] = dbtype
    options['skip'] = skip
    options['drop'] = drop
    options['totals'] = totals
    options['doc_id'] =  doc_id
    options['api_key'] =  api_key
    options['timeout'] =  timeout
    acmd = Ingester(batch)
    files = glob.glob(input.strip("'"))    
    acmd.ingest(files, uri, db, table, options)
    pass



@app.command()
def query(input:str, output:str=None, fields:str=None, delimiter:str=',', encoding:str=None, verbose:bool=False, format_in:str=None, format_out:str=None, zipfile:bool=False, query:str=None):
    """Query data using mistql (experimental, require mistql). Use 'pip install mistql' to install"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['fields'] = fields
    options['output'] = output
    options['encoding'] = encoding
    options['format_in'] = format_in
    options['format_out'] = format_out
    options['zipfile'] = zipfile
    options['query'] = query
    acmd = DataQuery()
    acmd.query(input, options)
    pass




if __name__ == '__main__':
    app()

