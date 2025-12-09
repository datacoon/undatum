#!/usr/bin/env python
# -*- coding: utf8 -*-
"""Core module providing CLI commands for the undatum package."""
import glob
import logging

import typer

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

DEFAULT_BATCH_SIZE = 1000

app = typer.Typer()

#logging.getLogger().addHandler(logging.StreamHandler())
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)


def enable_verbose():
    """Enable verbose logging."""
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO)

@app.command()
def convert(input_file: str, output: str, delimiter: str = ',',
            compression: str = 'brotli', encoding: str = 'utf8',
            verbose: bool = False, flatten_data: bool = False,
            prefix_strip: bool = True, fields: str = None,
            start_line: int = 0, skip_end_rows: int = 0,
            start_page: int = 0, tagname: str = None,
            format_in: str = None, format_out: str = None,
            zipfile: bool = False):
    """Converts one file to another. Supports XML, CSV, JSON, BSON."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'compression': compression,
        'flatten': flatten_data,
        'encoding': encoding,
        'prefix_strip': prefix_strip,
        'start_line': start_line,
        'skip_end_rows': skip_end_rows,
        'start_page': start_page,
        'tagname': tagname,
        'fields': fields,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile
    }
    acmd = Converter()
    acmd.convert(input_file, output, options)

@app.command()
def convertold(input_file: str, output: str, delimiter: str = ',',
               compression: str = 'brotli', encoding: str = 'utf8',
               verbose: bool = False, flatten_data: bool = False,
               prefix_strip: bool = True, fields: str = None,
               start_line: int = 0, skip_end_rows: int = 0,
               start_page: int = 0, tagname: str = None,
               format_in: str = None, format_out: str = None,
               zipfile: bool = False):
    """Converts one file to another. Supports XML, CSV, JSON, BSON (old)."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'compression': compression,
        'flatten': flatten_data,
        'encoding': encoding,
        'prefix_strip': prefix_strip,
        'start_line': start_line,
        'skip_end_rows': skip_end_rows,
        'start_page': start_page,
        'tagname': tagname,
        'fields': fields,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile
    }
    acmd = Converter()
    acmd.convert_old(input_file, output, options)

@app.command()
def uniq(input_file: str, output: str = None, fields: str = None,
         delimiter: str = ',', encoding: str = None, verbose: bool = False,
         filetype: str = None, engine: str = "auto"):
    """Returns all unique files of certain field(s)."""
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'fields': fields,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Selector()
    acmd.uniq(input_file, options)


@app.command()
def headers(input_file: str, output: str = None, fields: str = None,
           delimiter: str = ',', encoding: str = None, limit: int = 10000,
           verbose: bool = False, format_in: str = None,
           format_out: str = None, zipfile: bool = False,
           filter_expr: str = None):  # pylint: disable=unused-argument
    """Returns fieldnames of the file. Supports XML, CSV, JSON, BSON."""
    if verbose:
        enable_verbose()
    # fields and filter_expr kept for API compatibility
    options = {
        'output': output,
        'delimiter': delimiter,
        'encoding': encoding,
        'limit': limit,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile
    }
    acmd = Selector()
    acmd.headers(input_file, options)

@app.command()
def stats(input_file: str, output: str = None, dictshare: int = None,
         format_in: str = None, format_out: str = None, delimiter: str = None,
         verbose: bool = False, zipfile: bool = False,
         checkdates: bool = True, encoding: str = None):
    """Returns detailed stats on selected dataset."""
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'dictshare': dictshare,
        'zipfile': zipfile,
        'format_in': format_in,
        'format_out': format_out,
        'delimiter': delimiter,
        'checkdates': checkdates,
        'encoding': encoding,
        'verbose': verbose
    }
    acmd = StatProcessor(nodates=not checkdates)
    acmd.stats(input_file, options)


def flatten(input_file: str, output: str = None, delimiter: str = ',',
           encoding: str = 'utf8', format_in: str = None,
           filter_expr: str = None, verbose: bool = False):
    """Flatten data records. Write them as one value per row."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'filter': filter_expr
    }
    acmd = TextProcessor()
    acmd.flatten(input_file, options)


@app.command()
def frequency(input_file: str, output: str = None, fields: str = None,
             delimiter: str = ",", encoding: str = None, verbose: bool = False,
             filetype: str = None, engine: str = "auto"):
    """Field value frequency calc."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Selector()
    acmd.frequency(input_file, options)


@app.command()
def select(input_file: str, output: str = None, fields: str = None,
          delimiter: str = ",", encoding: str = None, verbose: bool = False,
          format_in: str = None, format_out: str = None,
          zipfile: bool = False, filter_expr: str = None):
    """Select or re-order columns from file. Supports CSV, JSONl, BSON."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile,
        'filter': filter_expr
    }
    acmd = Selector()
    acmd.select(input_file, options)


@app.command()
def split(input_file: str, output: str = None, fields: str = None,
         delimiter: str = ',', encoding: str = "utf8", verbose: bool = False,
         format_in: str = None, zipfile: bool = False, gzipfile: str = None,
         chunksize: int = 10000, filter_expr: str = None,
         dirname: str = None):
    """Splits the given file with data into chunks."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'zipfile': zipfile,
        'gzipfile': gzipfile,
        'chunksize': chunksize,
        'filter': filter_expr,
        'dirname': dirname
    }
    acmd = Selector()
    acmd.split(input_file, options)


@app.command()
def validate(input_file: str, output: str = None, fields: str = None,
            delimiter: str = ',', encoding: str = 'utf8',
            verbose: bool = False, format_in: str = None,
            zipfile: bool = False, rule: str = None,
            filter_expr: str = None, mode: str = "invalid"):
    """Validates selected field against validation rule."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'zipfile': zipfile,
        'filter': filter_expr,
        'rule': rule,
        'mode': mode
    }
    acmd = Validator()
    acmd.validate(input_file, options)


@app.command()
def apply(input_file: str, output: str = None, fields: str = None,
         delimiter: str = ",", encoding: str = 'utf8', verbose: bool = False,
         format_in: str = None, zipfile: bool = False, script: str = None,
         filter_expr: str = None):
    """Runs script against each record of input file."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'zipfile': zipfile,
        'filter': filter_expr,
        'script': script
    }
    acmd = Transformer()
    acmd.script(input_file, options)


@app.command()
def scheme(input_file: str, output: str = None, delimiter: str = ',',
          encoding: str = 'utf8', verbose: bool = False,
          format_in: str = None, zipfile: bool = False,
          stype: str = 'cerberus'):
    """Generate data schema from file."""
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'zipfile': zipfile,
        'stype': stype
    }
    acmd = Schemer()
    acmd.generate_scheme(input_file, options)


@app.command()
def analyze(input_file: str, verbose: bool = False, engine: str = "auto",
           use_pandas: bool = False, outtype: str = "text",
           output: str = None, autodoc: bool = False,
           lang: str = "English"):
    """Analyzes given data file and returns human readable insights."""
    if verbose:
        enable_verbose()
    options = {
        'engine': engine,
        'use_pandas': use_pandas,
        'outtype': outtype,
        'output': output,
        'autodoc': autodoc,
        'lang': lang
    }
    acmd = Analyzer()
    acmd.analyze(input_file, options)


@app.command()
def schema(input_file: str, verbose: bool = False, outtype: str = "text",
          output: str = None, autodoc: bool = False, lang: str = "English"):
    """Schema extraction."""
    if verbose:
        enable_verbose()
    options = {
        'outtype': outtype,
        'output': output,
        'autodoc': autodoc,
        'lang': lang
    }
    acmd = Schemer()
    acmd.extract_schema(input_file, options)


@app.command()
def schema_bulk(input_file: str, verbose: bool = False,
               outtype: str = "text", output: str = None,
               mode: str = "distinct", autodoc: bool = False,
               lang: str = "English"):
    """Schema extraction from many files.

    Default mode is 'distinct' that creates unique schema files per schema,
    alternative is 'perfile' that creates a schema per file with same names.
    """
    if verbose:
        enable_verbose()
    options = {
        'outtype': outtype,
        'output': output,
        'mode': mode,
        'autodoc': autodoc,
        'lang': lang
    }
    acmd = Schemer()
    acmd.extract_schema_bulk(input_file, options)


@app.command()
def ingest(input_file: str, uri: str, db: str, table: str,
          verbose: bool = False, batch: int = DEFAULT_BATCH_SIZE,
          dbtype: str = "mongodb", totals: bool = False, drop: bool = False,
          timeout: int = -30, skip: int = None, api_key: str = None,
          doc_id: str = None):
    """Data ingester."""
    if verbose:
        enable_verbose()
    options = {
        'dbtype': dbtype,
        'skip': skip,
        'drop': drop,
        'totals': totals,
        'doc_id': doc_id,
        'api_key': api_key,
        'timeout': timeout
    }
    acmd = Ingester(batch)
    files = glob.glob(input_file.strip("'"))
    acmd.ingest(files, uri, db, table, options)


@app.command()
def query(input_file: str, output: str = None, fields: str = None,
         delimiter: str = ',', encoding: str = None, verbose: bool = False,
         format_in: str = None, format_out: str = None,
         zipfile: bool = False, query_expr: str = None):
    """Query data using mistql (experimental, require mistql).

    Use 'pip install mistql' to install.
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile,
        'query': query_expr
    }
    acmd = DataQuery()
    acmd.query(input_file, options)




if __name__ == '__main__':
    app()
