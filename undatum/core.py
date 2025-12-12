#!/usr/bin/env python
# -*- coding: utf8 -*-
"""Core module providing CLI commands for the undatum package.

This module defines the main CLI interface using Typer, including all
command handlers for data conversion, analysis, validation, and more.
"""
import glob
import logging

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
def convert(input_file: str, output: str, delimiter: str = ',',
            compression: str = 'brotli', encoding: str = 'utf8',
            verbose: bool = False, flatten_data: bool = False,
            prefix_strip: bool = True, fields: str = None,
            start_line: int = 0, skip_end_rows: int = 0,
            start_page: int = 0, tagname: str = None,
            format_in: str = None, format_out: str = None,
            zipfile: bool = False):
    """Convert one file to another format.

    Supports conversion between XML, CSV, JSON, JSONL, BSON, Parquet, ORC, and AVRO formats.

    Args:
        input_file: Path to input file.
        output: Path to output file.
        delimiter: CSV delimiter character (default: ',').
        compression: Compression type (default: 'brotli').
        encoding: File encoding (default: 'utf8').
        verbose: Enable verbose logging.
        flatten_data: Flatten nested structures.
        prefix_strip: Strip XML namespace prefixes.
        fields: Comma-separated list of fields to include.
        start_line: Line number to start reading from.
        skip_end_rows: Number of rows to skip at end.
        start_page: Page number to start from (for Excel files).
        tagname: XML tag name containing records.
        format_in: Input format override.
        format_out: Output format override.
        zipfile: Whether input is a zip file.
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
def convertold(input_file: str, output: str, delimiter: str = ',',
               compression: str = 'brotli', encoding: str = 'utf8',
               verbose: bool = False, flatten_data: bool = False,
               prefix_strip: bool = True, fields: str = None,
               start_line: int = 0, skip_end_rows: int = 0,
               start_page: int = 0, tagname: str = None,
               format_in: str = None, format_out: str = None,
               zipfile: bool = False):
    """Convert one file to another using legacy conversion method.

    .. deprecated:: 1.0.15
        This command uses the old conversion implementation. Use 'convert' instead.

    Args:
        input_file: Path to input file.
        output: Path to output file.
        delimiter: CSV delimiter character (default: ',').
        compression: Compression type (default: 'brotli').
        encoding: File encoding (default: 'utf8').
        verbose: Enable verbose logging.
        flatten_data: Flatten nested structures.
        prefix_strip: Strip XML namespace prefixes.
        fields: Comma-separated list of fields to include.
        start_line: Line number to start reading from.
        skip_end_rows: Number of rows to skip at end.
        start_page: Page number to start from (for Excel files).
        tagname: XML tag name containing records.
        format_in: Input format override.
        format_out: Output format override.
        zipfile: Whether input is a zip file.
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
def uniq(input_file: str, output: str = None, fields: str = None,
         delimiter: str = ',', encoding: str = None, verbose: bool = False,
         filetype: str = None, engine: str = "auto"):
    """Extract all unique values from specified field(s).

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        fields: Comma-separated list of field names.
        delimiter: CSV delimiter character.
        encoding: File encoding.
        verbose: Enable verbose logging.
        filetype: File type override.
        engine: Processing engine ('auto', 'duckdb', or 'iterable').
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
def headers(input_file: str, output: str = None, fields: str = None,  # pylint: disable=unused-argument
           delimiter: str = ',', encoding: str = None, limit: int = 10000,
           verbose: bool = False, format_in: str = None,
           format_out: str = None, zipfile: bool = False,
           filter_expr: str = None):  # pylint: disable=unused-argument
    """Returns fieldnames of the file. Supports XML, CSV, JSON, BSON.

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        fields: Field filter (kept for API compatibility, not currently used).
        delimiter: CSV delimiter character.
        encoding: File encoding.
        limit: Maximum number of records to scan.
        verbose: Enable verbose logging.
        format_in: Input format override.
        format_out: Output format override.
        zipfile: Whether input is a zip file.
        filter_expr: Filter expression (kept for API compatibility, not currently used).
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
def stats(input_file: str, output: str = None, dictshare: int = None,
         format_in: str = None, format_out: str = None, delimiter: str = None,
         verbose: bool = False, zipfile: bool = False,
         checkdates: bool = True, encoding: str = None):
    """Generate detailed statistics about a dataset.

    Provides field types, uniqueness counts, min/max/average lengths,
    and optional date field detection.

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        dictshare: Dictionary share threshold for type detection.
        format_in: Input format override.
        format_out: Output format override.
        delimiter: CSV delimiter character.
        verbose: Enable verbose logging.
        zipfile: Whether input is a zip file.
        checkdates: Enable automatic date field detection.
        encoding: File encoding.
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
def flatten(input_file: str, output: str = None, delimiter: str = ',',
           encoding: str = 'utf8', format_in: str = None,
           filter_expr: str = None, verbose: bool = False):
    """Flatten nested data records into one value per row.

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        delimiter: CSV delimiter character.
        encoding: File encoding.
        format_in: Input format override.
        filter_expr: Filter expression to apply.
        verbose: Enable verbose logging.
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
def frequency(input_file: str, output: str = None, fields: str = None,
             delimiter: str = ",", encoding: str = None, verbose: bool = False,
             filetype: str = None, engine: str = "auto"):
    """Calculate frequency distribution for specified fields.

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        fields: Comma-separated list of field names.
        delimiter: CSV delimiter character.
        encoding: File encoding.
        verbose: Enable verbose logging.
        filetype: File type override.
        engine: Processing engine ('auto', 'duckdb', or 'iterable').
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
def select(input_file: str, output: str = None, fields: str = None,
          delimiter: str = ",", encoding: str = None, verbose: bool = False,
          format_in: str = None, format_out: str = None,
          zipfile: bool = False, filter_expr: str = None):
    """Select or reorder columns from file.

    Supports CSV, JSONL, and BSON formats. Can also filter records.

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        fields: Comma-separated list of field names to select.
        delimiter: CSV delimiter character.
        encoding: File encoding.
        verbose: Enable verbose logging.
        format_in: Input format override.
        format_out: Output format override.
        zipfile: Whether input is a zip file.
        filter_expr: Filter expression to apply.
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
def split(input_file: str, output: str = None, fields: str = None,
         delimiter: str = ',', encoding: str = "utf8", verbose: bool = False,
         format_in: str = None, zipfile: bool = False, gzipfile: str = None,
         chunksize: int = 10000, filter_expr: str = None,
         dirname: str = None):
    """Split a data file into multiple chunks.

    Can split by chunk size or by unique field values.

    Args:
        input_file: Path to input file.
        output: Optional output file path prefix.
        fields: Comma-separated field names to split by (instead of chunk size).
        delimiter: CSV delimiter character.
        encoding: File encoding.
        verbose: Enable verbose logging.
        format_in: Input format override.
        zipfile: Whether input is a zip file.
        gzipfile: Gzip compression option.
        chunksize: Number of records per chunk (when not splitting by fields).
        filter_expr: Filter expression to apply.
        dirname: Directory to write output files.
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
def validate(input_file: str, output: str = None, fields: str = None,
            delimiter: str = ',', encoding: str = 'utf8',
            verbose: bool = False, format_in: str = None,
            zipfile: bool = False, rule: str = None,
            filter_expr: str = None, mode: str = "invalid"):
    """Validate fields against built-in or custom validation rules.

    Available rules: common.email, common.url, ru.org.inn, ru.org.ogrn

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        fields: Comma-separated list of field names to validate.
        delimiter: CSV delimiter character.
        encoding: File encoding.
        verbose: Enable verbose logging.
        format_in: Input format override.
        zipfile: Whether input is a zip file.
        rule: Validation rule name to apply.
        filter_expr: Filter expression to apply.
        mode: Output mode: 'invalid' (default), 'stats', or 'valid'.
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
def apply(input_file: str, output: str = None, fields: str = None,
         delimiter: str = ",", encoding: str = 'utf8', verbose: bool = False,
         format_in: str = None, zipfile: bool = False, script: str = None,
         filter_expr: str = None):
    """Apply a transformation script to each record in the file.

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        fields: Comma-separated list of field names (kept for compatibility).
        delimiter: CSV delimiter character.
        encoding: File encoding.
        verbose: Enable verbose logging.
        format_in: Input format override.
        zipfile: Whether input is a zip file.
        script: Path to Python script file to apply.
        filter_expr: Filter expression to apply.
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
def scheme(input_file: str, output: str = None, delimiter: str = ',',
          encoding: str = 'utf8', verbose: bool = False,
          format_in: str = None, zipfile: bool = False,
          stype: str = 'cerberus'):
    """Generate data schema from file.

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        delimiter: CSV delimiter character.
        encoding: File encoding.
        verbose: Enable verbose logging.
        format_in: Input format override.
        zipfile: Whether input is a zip file.
        stype: Schema type: 'cerberus' (default) or other formats.
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
def analyze(input_file: str, verbose: bool = False, engine: str = "auto",
           use_pandas: bool = False, outtype: str = "text",
           output: str = None, autodoc: bool = False,
           lang: str = "English", ai_provider: str = None,
           ai_model: str = None, ai_base_url: str = None):
    """Analyzes given data file and returns human readable insights.

    Args:
        ai_provider: AI provider to use (openai, openrouter, ollama, lmstudio, perplexity)
        ai_model: Model name to use (provider-specific)
        ai_base_url: Base URL for API (optional, provider-specific defaults)
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
def schema(input_file: str, verbose: bool = False, outtype: str = "text",
          output: str = None, autodoc: bool = False, lang: str = "English"):
    """Extract schema from a data file.

    Args:
        input_file: Path to input file.
        verbose: Enable verbose logging.
        outtype: Output type: 'text', 'json', 'yaml' (default: 'text').
        output: Optional output file path.
        autodoc: Enable AI-powered field documentation.
        lang: Language for AI documentation (default: 'English').
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
def schema_bulk(input_file: str, verbose: bool = False,
               outtype: str = "text", output: str = None,
               mode: str = "distinct", autodoc: bool = False,
               lang: str = "English"):
    """Extract schemas from multiple files.

    Args:
        input_file: Glob pattern or directory path for input files.
        verbose: Enable verbose logging.
        outtype: Output type: 'text', 'json', 'yaml' (default: 'text').
        output: Output directory path.
        mode: Extraction mode: 'distinct' (unique schemas) or 'perfile' (one per file).
        autodoc: Enable AI-powered field documentation.
        lang: Language for AI documentation (default: 'English').
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
    """Ingest data into a database.

    Supports MongoDB and Elasticsearch databases.

    Args:
        input_file: Path to input file or glob pattern.
        uri: Database connection URI.
        db: Database name.
        table: Collection/table name.
        verbose: Enable verbose logging.
        batch: Batch size for ingestion (default: 1000).
        dbtype: Database type: 'mongodb' (default) or 'elasticsearch'.
        totals: Show total counts.
        drop: Drop existing collection/table before ingestion.
        timeout: Connection timeout in seconds.
        skip: Number of records to skip.
        api_key: API key for database authentication.
        doc_id: Document ID field name.
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
def query(input_file: str, output: str = None, fields: str = None,
         delimiter: str = ',', encoding: str = None, verbose: bool = False,
         format_in: str = None, format_out: str = None,
         zipfile: bool = False, query_expr: str = None):
    """Query data using MistQL query language.

    .. note:: Experimental feature. Requires 'mistql' package: pip install mistql

    Args:
        input_file: Path to input file.
        output: Optional output file path.
        fields: Comma-separated list of field names (kept for compatibility).
        delimiter: CSV delimiter character.
        encoding: File encoding.
        verbose: Enable verbose logging.
        format_in: Input format override.
        format_out: Output format override.
        zipfile: Whether input is a zip file.
        query_expr: MistQL query expression.
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
