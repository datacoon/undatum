#!/usr/bin/env python
# -*- coding: utf8 -*-
"""Core module providing CLI commands for the undatum package.

This module defines the main CLI interface using Typer, including all
command handlers for data conversion, analysis, validation, and more.
"""
import glob
import logging
from typing import Annotated

import typer

from .cmds.analyzer import Analyzer
from .cmds.converter import Converter
from .cmds.ingester import Ingester
from .cmds.query import DataQuery
from .cmds.schemer import Schemer
from .cmds.selector import Selector
from .cmds.statistics import StatProcessor
from .cmds.textproc import TextProcessor
from .cmds.transformer import Transformer
from .cmds.validator import Validator

DEFAULT_BATCH_SIZE = 1000

app = typer.Typer()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)


def enable_verbose():
    """Enable verbose logging."""
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO)

@app.command()
def convert(
    input_file: Annotated[str, typer.Argument(help="Path to input file to convert.")],
    output: Annotated[str, typer.Argument(help="Path to output file.")],
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    compression: Annotated[str, typer.Option(help="Compression type (e.g., 'brotli', 'gzip', 'xz').")] = 'brotli',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    flatten_data: Annotated[bool, typer.Option(help="Flatten nested data structures into flat records.")] = False,
    prefix_strip: Annotated[bool, typer.Option(help="Strip XML namespace prefixes from element names.")] = True,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to include in output.")] = None,
    start_line: Annotated[int, typer.Option(help="Line number (0-based) to start reading from.")] = 0,
    skip_end_rows: Annotated[int, typer.Option(help="Number of rows to skip at the end of the file.")] = 0,
    start_page: Annotated[int, typer.Option(help="Page number (0-based) to start from for Excel files.")] = 0,
    tagname: Annotated[str, typer.Option(help="XML tag name that contains individual records.")] = None,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xml').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output file format (e.g., 'csv', 'jsonl', 'parquet').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False
):
    """Convert one file to another format.

    Supports conversion between XML, CSV, JSON, JSONL, BSON, Parquet, ORC, and AVRO formats.
    """
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
def convertold(
    input_file: Annotated[str, typer.Argument(help="Path to input file to convert.")],
    output: Annotated[str, typer.Argument(help="Path to output file.")],
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    compression: Annotated[str, typer.Option(help="Compression type (e.g., 'brotli', 'gzip', 'xz').")] = 'brotli',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    flatten_data: Annotated[bool, typer.Option(help="Flatten nested data structures into flat records.")] = False,
    prefix_strip: Annotated[bool, typer.Option(help="Strip XML namespace prefixes from element names.")] = True,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to include in output.")] = None,
    start_line: Annotated[int, typer.Option(help="Line number (0-based) to start reading from.")] = 0,
    skip_end_rows: Annotated[int, typer.Option(help="Number of rows to skip at the end of the file.")] = 0,
    start_page: Annotated[int, typer.Option(help="Page number (0-based) to start from for Excel files.")] = 0,
    tagname: Annotated[str, typer.Option(help="XML tag name that contains individual records.")] = None,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xml').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output file format (e.g., 'csv', 'jsonl', 'parquet').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False
):
    """Convert one file to another using legacy conversion method.

    .. deprecated:: 1.0.15
        This command uses the old conversion implementation. Use 'convert' instead.
    """
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
def uniq(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to extract unique values from.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Extract all unique values from specified field(s).

    Returns unique values or unique combinations if multiple fields are specified.
    """
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
def headers(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Field filter (kept for API compatibility, not currently used).")] = None,  # pylint: disable=unused-argument
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    limit: Annotated[int, typer.Option(help="Maximum number of records to scan for field detection.")] = 10000,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xml').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output format (e.g., 'csv', 'json').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    filter_expr: Annotated[str, typer.Option(help="Filter expression (kept for API compatibility, not currently used).")] = None  # pylint: disable=unused-argument
):
    """Returns fieldnames of the file. Supports XML, CSV, JSON, BSON.

    Scans the input file and returns all detected field/column names.
    """
    if verbose:
        enable_verbose()
    # fields and filter_expr kept for API compatibility but not currently used
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
def stats(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    dictshare: Annotated[int, typer.Option(help="Dictionary share threshold (0-100) for type detection.")] = None,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output format (e.g., 'json', 'yaml').")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    checkdates: Annotated[bool, typer.Option(help="Enable automatic date field detection.")] = True,
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None
):
    """Generate detailed statistics about a dataset.

    Provides field types, uniqueness counts, min/max/average lengths,
    and optional date field detection.
    """
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


@app.command()
def flatten(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'jsonl', 'xml').")] = None,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply before flattening.")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False
):
    """Flatten nested data records into one value per row.

    Converts nested structures (arrays, objects) into flat records.
    """
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
def frequency(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to calculate frequency for.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Calculate frequency distribution for specified fields.

    Counts occurrences of each unique value in the specified field(s).
    """
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
def select(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to select and reorder.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output format (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply (e.g., \"`status` == 'active'\").")] = None
):
    """Select or reorder columns from file.

    Supports CSV, JSONL, and BSON formats. Can also filter records.
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
        'filter': filter_expr
    }
    acmd = Selector()
    acmd.select(input_file, options)


@app.command()
def split(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path prefix. If not specified, uses input filename.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated field names to split by (creates one file per unique value combination).")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = "utf8",
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    gzipfile: Annotated[str, typer.Option(help="Gzip compression option for output files.")] = None,
    chunksize: Annotated[int, typer.Option(help="Number of records per chunk when splitting by size (default: 10000).")] = 10000,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply before splitting.")] = None,
    dirname: Annotated[str, typer.Option(help="Directory path to write output files to.")] = None
):
    """Split a data file into multiple chunks.

    Can split by chunk size or by unique field values.
    """
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
def validate(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to validate.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    rule: Annotated[str, typer.Option(help="Validation rule name (e.g., 'common.email', 'common.url', 'ru.org.inn', 'ru.org.ogrn').")] = None,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply before validation.")] = None,
    mode: Annotated[str, typer.Option(help="Output mode: 'invalid' (default, show invalid records), 'stats' (show statistics), or 'valid' (show valid records).")] = "invalid"
):
    """Validate fields against built-in or custom validation rules.

    Available rules: common.email, common.url, ru.org.inn, ru.org.ogrn
    """
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
def apply(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names (kept for compatibility).")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    script: Annotated[str, typer.Option(help="Path to Python script file containing transformation function.")] = None,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply before transformation.")] = None
):
    """Apply a transformation script to each record in the file.

    Executes a Python script that transforms each record.
    """
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
def scheme(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    stype: Annotated[str, typer.Option(help="Schema type: 'cerberus' (default) or other schema formats.")] = 'cerberus'
):
    """Generate data schema from file.

    Creates a schema definition based on the structure of the input data.
    """
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
def analyze(
    input_file: Annotated[str, typer.Argument(help="Path to input file to analyze.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto",
    use_pandas: Annotated[bool, typer.Option(help="Use pandas for data processing (may use more memory).")] = False,
    outtype: Annotated[str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")] = "text",
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered automatic field and dataset documentation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI-generated documentation (default: 'English').")] = "English",
    ai_provider: Annotated[str, typer.Option(help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'.")] = None,
    ai_model: Annotated[str, typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI).")] = None,
    ai_base_url: Annotated[str, typer.Option(help="Base URL for AI API (optional, uses provider-specific defaults if not specified).")] = None
):
    """Analyzes given data file and returns human readable insights.

    Provides detailed analysis of file structure, encoding, fields, data types,
    and optionally AI-generated field descriptions and dataset summaries.
    """
    if verbose:
        enable_verbose()

    # Build AI configuration
    ai_config = {}
    if ai_model:
        ai_config['model'] = ai_model
    if ai_base_url:
        ai_config['base_url'] = ai_base_url

    options = {
        'engine': engine,
        'use_pandas': use_pandas,
        'outtype': outtype,
        'output': output,
        'autodoc': autodoc,
        'lang': lang,
        'ai_provider': ai_provider,
        'ai_config': ai_config if ai_config else None
    }
    acmd = Analyzer()
    acmd.analyze(input_file, options)


@app.command()
def schema(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    outtype: Annotated[str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")] = "text",
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered automatic field documentation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI-generated documentation (default: 'English').")] = "English"
):
    """Extract schema from a data file.

    Generates a schema definition describing the structure and types of fields in the data.
    """
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
def schema_bulk(
    input_file: Annotated[str, typer.Argument(help="Glob pattern or directory path for input files (e.g., 'data/*.csv' or 'data/').")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    outtype: Annotated[str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")] = "text",
    output: Annotated[str, typer.Option(help="Output directory path for schema files.")] = None,
    mode: Annotated[str, typer.Option(help="Extraction mode: 'distinct' (extract unique schemas, default) or 'perfile' (one schema per file).")] = "distinct",
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered automatic field documentation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI-generated documentation (default: 'English').")] = "English"
):
    """Extract schemas from multiple files.

    Processes multiple files and extracts their schemas, either as distinct unique schemas
    or one schema per file.
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
def ingest(
    input_file: Annotated[str, typer.Argument(help="Path to input file or glob pattern (e.g., 'data/*.jsonl').")],
    uri: Annotated[str, typer.Argument(help="Database connection URI (e.g., 'mongodb://localhost:27017' or 'https://elasticsearch:9200').")],
    db: Annotated[str, typer.Argument(help="Database name.")],
    table: Annotated[str, typer.Argument(help="Collection or table name.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    batch: Annotated[int, typer.Option(help="Batch size for ingestion (number of records per batch, default: 1000).")] = DEFAULT_BATCH_SIZE,
    dbtype: Annotated[str, typer.Option(help="Database type: 'mongodb' (default) or 'elasticsearch'.")] = "mongodb",
    totals: Annotated[bool, typer.Option(help="Show total record counts during ingestion.")] = False,
    drop: Annotated[bool, typer.Option(help="Drop existing collection/table before ingestion.")] = False,
    timeout: Annotated[int, typer.Option(help="Connection timeout in seconds (default: -30).")] = -30,
    skip: Annotated[int, typer.Option(help="Number of records to skip at the beginning.")] = None,
    api_key: Annotated[str, typer.Option(help="API key for database authentication.")] = None,
    doc_id: Annotated[str, typer.Option(help="Field name to use as document ID (for MongoDB).")] = None
):
    """Ingest data into a database.

    Supports MongoDB and Elasticsearch databases. Reads data from files and inserts
    them into the specified database collection or table.
    """
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
def query(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names (kept for compatibility).")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output format (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    query_expr: Annotated[str, typer.Option(help="MistQL query expression to execute on the data.")] = None
):
    """Query data using MistQL query language.

    .. note:: Experimental feature. Requires 'mistql' package: pip install mistql

    Executes MistQL queries on the input data and returns the results.
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
