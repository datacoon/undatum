"""Top-level data processing commands for the undatum CLI.

Extracted from undatum.core to keep the assembly module thin. All commands
registered on ``data_app`` are merged into the main app by ``undatum.core``.
"""

import glob
import logging
import os
import sys
from typing import Annotated, Optional

import typer

from ..cmds.analyzer import Analyzer
from ..cmds.cat import Cat
from ..cmds.converter import Converter
from ..common.errors import ValidationError
from ..cmds.counter import Counter
from ..cmds.deduplicator import Deduplicator
from ..cmds.differ import Differ
from ..cmds.doc import Documenter
from ..cmds.enumerator import Enumerator
from ..cmds.excluder import Excluder
from ..cmds.exploder import Exploder
from ..cmds.extractor import Extractor
from ..cmds.filler import Filler
from ..cmds.fixlengths import FixLengths
from ..cmds.formatter import Formatter
from ..cmds.head import Head
from ..cmds.ingester import Ingester
from ..cmds.joiner import Joiner
from ..cmds.masker import Masker
from ..cmds.plotter import Plotter
from ..cmds.query import DataQuery
from ..cmds.renamer import Renamer
from ..cmds.replacer import Replacer
from ..cmds.reverser import Reverser
from ..cmds.sampler import Sampler
from ..cmds.schemer import Schemer
from ..cmds.searcher import Searcher
from ..cmds.selector import Selector
from ..cmds.slicer import Slicer
from ..cmds.sniffer import Sniffer
from ..cmds.sorter import Sorter
from ..cmds.sql import SqlExecutor
from ..cmds.statistics import StatProcessor
from ..cmds.table import TableFormatter
from ..cmds.tail import Tail
from ..cmds.textproc import TextProcessor
from ..cmds.transformer import Transformer
from ..cmds.transposer import Transposer
from ..cmds.validator import Validator
from .common import enable_verbose

DEFAULT_BATCH_SIZE = 1000

logger = logging.getLogger(__name__)

data_app = typer.Typer()


@data_app.command()
def convert(
    input_file: Annotated[str, typer.Argument(help="Path to input file to convert.")],
    output: Annotated[str, typer.Argument(help="Path to output file.")],
    delimiter: Annotated[
        Optional[str],
        typer.Option(help="CSV delimiter character (auto-detected when omitted)."),
    ] = None,
    compression: Annotated[
        Optional[str],
        typer.Option(help="Output compression codec (e.g. 'brotli', 'snappy', 'gzip')."),
    ] = None,
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = "utf8",
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    flatten_data: Annotated[
        bool, typer.Option(help="Flatten nested data structures into flat records.")
    ] = False,
    prefix_strip: Annotated[
        bool, typer.Option(help="Strip XML namespace prefixes from element names.")
    ] = True,
    start_line: Annotated[
        int, typer.Option(help="Line number (0-based) to start reading from.")
    ] = 0,
    start_page: Annotated[
        int, typer.Option(help="Page number (0-based) to start from for Excel files.")
    ] = 0,
    tagname: Annotated[
        str, typer.Option(help="XML tag name that contains individual records.")
    ] = None,
    format_in: Annotated[
        str,
        typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xml')."),
    ] = None,
    format_out: Annotated[
        str, typer.Option(help="Override output file format (e.g., 'csv', 'jsonl', 'parquet').")
    ] = None,
    batch_size: Annotated[
        int, typer.Option(help="Number of records per conversion batch.")
    ] = 50000,
    scan_limit: Annotated[
        int, typer.Option(help="Records to sample for output schema detection.")
    ] = 1000,
    atomic: Annotated[
        bool,
        typer.Option(help="Write to a temp file and rename on success (local output only)."),
    ] = False,
    threads: Annotated[
        int, typer.Option(help="Number of threads for parallel processing (default: CPU count).")
    ] = None,
    progress: Annotated[bool, typer.Option(help="Show progress bar.")] = True,
    low_memory: Annotated[
        bool,
        typer.Option(
            help="Prefer spill-to-disk / smaller batches for large-file conversion "
            "(DuckDB COPY when possible)."
        ),
    ] = False,
    engine: Annotated[
        str,
        typer.Option(help="Conversion engine: 'auto' (default), 'duckdb', or 'python'."),
    ] = "auto",
    recursive: Annotated[
        bool,
        typer.Option(
            help="Bulk-convert a directory or glob pattern; OUTPUT is treated as a directory."
        ),
    ] = False,
    to_ext: Annotated[
        str,
        typer.Option(
            help="Target extension for bulk conversion (e.g. 'parquet'). Defaults to --format-out."
        ),
    ] = None,
):
    """Convert one file (or a directory/glob with --recursive) to another format.

    Reading and writing are handled by the iterabledata engine, so any format it
    supports (100+ formats, including cloud URIs like s3://, gs://, az://) can be
    used as input or output. Use ``undatum formats list`` to see all formats.

    For multi-GB files, prefer ``--low-memory`` (uses DuckDB spill-to-disk when the
    format is duckable, and smaller Parquet write batches otherwise).

    Examples:
        # Single file
        undatum convert data.csv data.parquet

        # Large file, low memory
        undatum convert huge.jsonl.zst huge.parquet --low-memory

        # Bulk-convert a directory of CSVs to Parquet
        undatum convert ./raw ./processed --recursive --to-ext parquet

        # Bulk-convert with a glob pattern
        undatum convert "data/*.jsonl" ./out --recursive --to-ext csv
    """
    if verbose:
        enable_verbose()
    options = {
        "delimiter": delimiter,
        "compression": compression,
        "flatten": flatten_data,
        "encoding": encoding,
        "prefix_strip": prefix_strip,
        "start_line": start_line,
        "start_page": start_page,
        "tagname": tagname,
        "format_in": format_in,
        "format_out": format_out,
        "batch_size": batch_size,
        "scan_limit": scan_limit,
        "atomic": atomic,
        "threads": threads,
        "progress": progress,
        "low_memory": low_memory,
        "engine": engine,
    }
    acmd = Converter(batch_size=batch_size)
    is_glob = any(ch in input_file for ch in "*?[")
    is_dir = os.path.isdir(input_file)
    if (is_dir or is_glob) and not recursive:
        raise ValidationError(
            "Bulk input detected (directory or glob pattern). "
            "Use --recursive to bulk-convert.",
            field="input",
        )
    if recursive:
        acmd.bulk_convert(input_file, output, options, to_ext=to_ext)
    else:
        acmd.convert(input_file, output, options, limit=scan_limit)


@data_app.command()
def extract(
    input_files: Annotated[list[str], typer.Argument(help="Input file(s) to extract from.")],
    output_format: Annotated[
        str, typer.Option(help="Output format: csv, json, ndjson, parquet, datapackage.")
    ] = "csv",
    output: Annotated[
        Optional[str], typer.Option(help="Output file path (single table only).")
    ] = None,
    output_dir: Annotated[
        Optional[str], typer.Option(help="Output directory for multiple tables.")
    ] = None,
    method: Annotated[
        Optional[str], typer.Option(help="Extraction method: tables, text, ocr.")
    ] = None,
    pages: Annotated[
        Optional[str], typer.Option(help="PDF pages to extract (e.g., 1-3,7,10-12).")
    ] = None,
    ocr: Annotated[bool, typer.Option(help="Enable OCR for scanned PDFs.")] = False,
    flatten: Annotated[
        bool, typer.Option(help="Flatten multiple tables into one output table.")
    ] = False,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Extract tables or text from documents."""
    if verbose:
        enable_verbose()
    options = {
        "output_format": output_format,
        "output": output,
        "output_dir": output_dir,
        "method": method,
        "pages": pages,
        "ocr": ocr,
        "flatten": flatten,
    }
    Extractor().extract(input_files, options)


@data_app.command()
def uniq(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of field names to extract unique values from.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[
        str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl', 'xlsx').")
    ] = None,
    start_page: Annotated[
        int, typer.Option(help="Sheet index (0-based) for Excel files.")
    ] = 0,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")
    ] = "auto",
):
    """Extract all unique values from specified field(s).

    Returns unique values or unique combinations if multiple fields are specified.
    Supports CSV, JSONL, Excel (XLS/XLSX), and other iterable formats.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "fields": fields,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": filetype,
        "start_page": start_page,
        "engine": engine,
    }
    acmd = Selector()
    acmd.uniq(input_file, options)


@data_app.command()
def headers(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Field filter (kept for API compatibility, not currently used).")
    ] = None,  # pylint: disable=unused-argument
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    limit: Annotated[
        int, typer.Option(help="Maximum number of records to scan for field detection.")
    ] = 10000,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str,
        typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xml')."),
    ] = None,
    format_out: Annotated[
        str, typer.Option(help="Override output format (e.g., 'csv', 'json').")
    ] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    filter_expr: Annotated[
        str,
        typer.Option(help="Filter expression (kept for API compatibility, not currently used)."),
    ] = None,  # pylint: disable=unused-argument
):
    """Returns fieldnames of the file. Supports XML, CSV, JSON, BSON.

    Scans the input file and returns all detected field/column names.
    """
    if verbose:
        enable_verbose()
    # fields and filter_expr kept for API compatibility but not currently used
    options = {
        "output": output,
        "delimiter": delimiter,
        "encoding": encoding,
        "limit": limit,
        "format_in": format_in,
        "format_out": format_out,
        "zipfile": zipfile,
    }
    acmd = Selector()
    acmd.headers(input_file, options)


@data_app.command()
def stats(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    dictshare: Annotated[
        int, typer.Option(help="Dictionary share threshold (0-100) for type detection.")
    ] = None,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    format_out: Annotated[
        str, typer.Option(help="Override output format (e.g., 'json', 'yaml').")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    checkdates: Annotated[bool, typer.Option(help="Enable automatic date field detection.")] = True,
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    progress: Annotated[bool, typer.Option(help="Show progress bar (default: True).")] = True,
    no_progress: Annotated[
        bool, typer.Option(help="Disable progress bar (for non-interactive use).")
    ] = False,
    engine: Annotated[
        str,
        typer.Option(
            help="Engine to use for statistics computation: 'auto' (detect), 'duckdb' (DuckDB engine), or 'iterable' (row-by-row)."
        ),
    ] = "auto",
    threads: Annotated[
        int, typer.Option(help="Number of threads for parallel processing (default: CPU count).")
    ] = None,
):
    """Generate detailed statistics about a dataset.

    Provides field types, uniqueness counts, min/max/average lengths,
    and optional date field detection.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "dictshare": dictshare,
        "zipfile": zipfile,
        "format_in": format_in,
        "format_out": format_out,
        "delimiter": delimiter,
        "checkdates": checkdates,
        "encoding": encoding,
        "verbose": verbose,
        "progress": progress if not no_progress else False,
        "no_progress": no_progress,
        "engine": engine,
        "threads": threads,
    }
    acmd = StatProcessor(nodates=not checkdates)
    acmd.stats(input_file, options)


# Register `profile` as an alias of `stats`
data_app.command(name="profile")(stats)


@data_app.command()
def flatten(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = "utf8",
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'jsonl', 'xml').")
    ] = None,
    filter_expr: Annotated[
        str, typer.Option(help="Filter expression to apply before flattening.")
    ] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Flatten nested data records into one value per row.

    Converts nested structures (arrays, objects) into flat records.
    """
    if verbose:
        enable_verbose()
    options = {
        "delimiter": delimiter,
        "output": output,
        "encoding": encoding,
        "format_in": format_in,
        "filter": filter_expr,
    }
    acmd = TextProcessor()
    acmd.flatten(input_file, options)


@data_app.command()
def frequency(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of field names to calculate frequency for.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[
        str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl', 'xlsx').")
    ] = None,
    start_page: Annotated[
        int, typer.Option(help="Sheet index (0-based) for Excel files.")
    ] = 0,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'python'.")
    ] = "auto",
    duckdb_threads: Annotated[
        int, typer.Option(help="Number of threads for DuckDB engine.")
    ] = None,
    duckdb_memory: Annotated[
        str, typer.Option(help="Memory limit for DuckDB (e.g., '4GB', '512MB').")
    ] = None,
    duckdb_temp_dir: Annotated[str, typer.Option(help="Temporary directory for DuckDB.")] = None,
):
    """Calculate frequency distribution for specified fields.

    Counts occurrences of each unique value in the specified field(s).
    Supports CSV, JSONL, Excel (XLS/XLSX), and other iterable formats.
    """
    if verbose:
        enable_verbose()
    options = {
        "delimiter": delimiter,
        "fields": fields,
        "output": output,
        "encoding": encoding,
        "filetype": filetype,
        "start_page": start_page,
        "engine": engine,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
        "duckdb_temp_dir": duckdb_temp_dir,
    }
    acmd = Selector()
    acmd.frequency(input_file, options)


@data_app.command()
def select(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of field names to select and reorder.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xlsx').")
    ] = None,
    format_out: Annotated[
        str, typer.Option(help="Override output format (e.g., 'csv', 'jsonl').")
    ] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    filter_expr: Annotated[
        str, typer.Option(help="Filter expression to apply (e.g., \"`status` == 'active'\").")
    ] = None,
    start_page: Annotated[
        int, typer.Option(help="Sheet index (0-based) for Excel files.")
    ] = 0,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")
    ] = "auto",
    duckdb_threads: Annotated[
        int, typer.Option(help="Number of threads for DuckDB engine.")
    ] = None,
    duckdb_memory: Annotated[
        str, typer.Option(help="Memory limit for DuckDB (e.g., '4GB', '512MB').")
    ] = None,
    duckdb_temp_dir: Annotated[str, typer.Option(help="Temporary directory for DuckDB.")] = None,
):
    """Select or reorder columns from file.

    Supports CSV, JSONL, BSON, Excel (XLS/XLSX), and other iterable formats. Can also filter records.
    """
    if verbose:
        enable_verbose()
    options = {
        "delimiter": delimiter,
        "fields": fields,
        "output": output,
        "encoding": encoding,
        "format_in": format_in,
        "format_out": format_out,
        "zipfile": zipfile,
        "filter": filter_expr,
        "start_page": start_page,
        "engine": engine,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
        "duckdb_temp_dir": duckdb_temp_dir,
    }
    acmd = Selector()
    acmd.select(input_file, options)


@data_app.command()
def split(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str,
        typer.Option(
            help="Optional output file path prefix. If not specified, uses input filename."
        ),
    ] = None,
    fields: Annotated[
        str,
        typer.Option(
            help="Comma-separated field names to split by (creates one file per unique value combination)."
        ),
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = "utf8",
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    gzipfile: Annotated[str, typer.Option(help="Gzip compression option for output files.")] = None,
    chunksize: Annotated[
        int,
        typer.Option(help="Number of records per chunk when splitting by size (default: 10000)."),
    ] = 10000,
    filter_expr: Annotated[
        str, typer.Option(help="Filter expression to apply before splitting.")
    ] = None,
    dirname: Annotated[str, typer.Option(help="Directory path to write output files to.")] = None,
):
    """Split a data file into multiple chunks.

    Can split by chunk size or by unique field values.
    """
    if verbose:
        enable_verbose()
    options = {
        "delimiter": delimiter,
        "fields": fields,
        "output": output,
        "encoding": encoding,
        "format_in": format_in,
        "zipfile": zipfile,
        "gzipfile": gzipfile,
        "chunksize": chunksize,
        "filter": filter_expr,
        "dirname": dirname,
    }
    acmd = Selector()
    acmd.split(input_file, options)


@data_app.command()
def validate(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of field names to validate (legacy mode).")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = "utf8",
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    rule: Annotated[
        str,
        typer.Option(
            help="Validation rule name (legacy mode, e.g., 'common.email', 'common.url')."
        ),
    ] = None,
    filter_expr: Annotated[
        str, typer.Option(help="Filter expression to apply before validation.")
    ] = None,
    mode: Annotated[
        str,
        typer.Option(
            help="Output mode: 'invalid' (default, show invalid records), 'stats' (show statistics), or 'valid' (show valid records)."
        ),
    ] = "invalid",
    rules: Annotated[
        str, typer.Option(help="Path to YAML/JSON rule file for rich validation.")
    ] = None,
    severity: Annotated[
        str,
        typer.Option(
            help="Filter violations by severity: 'error', 'warning', 'info', or 'all' (default)."
        ),
    ] = "all",
    output_format: Annotated[
        str, typer.Option(help="Output format: 'text' (default) or 'json'.")
    ] = "text",
    violation_report: Annotated[
        str, typer.Option(help="Path to write detailed violation report (JSON format).")
    ] = None,
    fail_on_warnings: Annotated[
        bool, typer.Option(help="Treat warnings as errors (exit with non-zero code).")
    ] = False,
    max_violations: Annotated[
        int,
        typer.Option(
            help="Maximum number of violations to display (default: 10 for text, 100 for JSON)."
        ),
    ] = None,
    progress: Annotated[bool, typer.Option(help="Show progress bar.")] = False,
):
    """Validate data against validation rules.

    Two modes:
    1. Rule file mode: Use --rules option with YAML/JSON rule file for rich validation
    2. Legacy mode: Use --fields and --rule options for simple single-rule validation

    Examples:
        # Rich validation with rule file
        undatum validate data.csv --rules validation-rules.yml

        # Legacy mode (backward compatible)
        undatum validate data.csv --fields email --rule common.email
    """
    if verbose:
        enable_verbose()

    # Set default max_violations based on output format
    if max_violations is None:
        max_violations = 100 if output_format == "json" else 10

    options = {
        "delimiter": delimiter,
        "fields": fields,
        "output": output,
        "encoding": encoding,
        "format_in": format_in,
        "zipfile": zipfile,
        "filter": filter_expr,
        "rule": rule,
        "mode": mode,
        "rules": rules,
        "severity": severity,
        "output_format": output_format,
        "violation_report": violation_report,
        "fail_on_warnings": fail_on_warnings,
        "max_violations": max_violations,
        "progress": progress,
    }
    acmd = Validator()
    acmd.validate(input_file, options)


@data_app.command()
def apply(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of field names (kept for compatibility).")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = "utf8",
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    script: Annotated[
        str, typer.Option(help="Path to Python script file containing transformation function.")
    ] = None,
    filter_expr: Annotated[
        str, typer.Option(help="Filter expression to apply before transformation.")
    ] = None,
):
    """Apply a transformation script to each record in the file.

    Executes a Python script that transforms each record.
    """
    if verbose:
        enable_verbose()
    options = {
        "delimiter": delimiter,
        "fields": fields,
        "output": output,
        "encoding": encoding,
        "format_in": format_in,
        "zipfile": zipfile,
        "filter": filter_expr,
        "script": script,
    }
    acmd = Transformer()
    acmd.script(input_file, options)


@data_app.command()
def scheme(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = "utf8",
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    stype: Annotated[
        str, typer.Option(help="Schema type: 'cerberus' (default) or other schema formats.")
    ] = "cerberus",
):
    """[DEPRECATED] Generate data schema from file.

    ⚠️  This command is deprecated. Use 'undatum schema --format cerberus' instead.

    Creates a schema definition based on the structure of the input data.
    This command will be removed in a future version.
    """
    import warnings

    warnings.warn(
        "The 'scheme' command is deprecated. Use 'undatum schema --format cerberus' instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    if verbose:
        enable_verbose()

    # Redirect to schema command with cerberus format
    # Build AI configuration (not used for scheme, but needed for schema command)
    options = {
        "outtype": "json",  # Cerberus format outputs JSON
        "format": "cerberus",
        "output": output,
        "autodoc": False,
        "engine": "auto",
    }
    acmd = Schemer()
    acmd.extract_schema(input_file, options)


@data_app.command()
def analyze(
    input_file: Annotated[str, typer.Argument(help="Path to input file to analyze.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")
    ] = "auto",
    use_pandas: Annotated[
        bool, typer.Option(help="Use pandas for data processing (may use more memory).")
    ] = False,
    outtype: Annotated[
        str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")
    ] = "text",
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    autodoc: Annotated[
        bool, typer.Option(help="Enable AI-powered automatic field and dataset documentation.")
    ] = False,
    lang: Annotated[
        str, typer.Option(help="Language for AI-generated documentation (default: 'English').")
    ] = "English",
    ai_provider: Annotated[
        str,
        typer.Option(
            help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'."
        ),
    ] = None,
    ai_model: Annotated[
        str,
        typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI)."),
    ] = None,
    ai_base_url: Annotated[
        str,
        typer.Option(
            help="Base URL for AI API (optional, uses provider-specific defaults if not specified)."
        ),
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = None,
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    objects_limit: Annotated[
        int, typer.Option(help="Maximum number of records to scan for schema inference.")
    ] = 10000,
    ignore_errors: Annotated[
        bool, typer.Option(help="Ignore parse errors in CSV/JSON files (default: True).")
    ] = True,
    no_scan: Annotated[
        bool, typer.Option(help="Return file metadata only; skip structure scan.")
    ] = False,
    no_stats: Annotated[
        bool, typer.Option(help="Skip uniqueness statistics in field analysis.")
    ] = False,
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
        ai_config["model"] = ai_model
    if ai_base_url:
        ai_config["base_url"] = ai_base_url

    options = {
        "engine": engine,
        "use_pandas": use_pandas,
        "outtype": outtype,
        "output": output,
        "autodoc": autodoc,
        "lang": lang,
        "ai_provider": ai_provider,
        "ai_config": ai_config if ai_config else None,
        "delimiter": delimiter,
        "encoding": encoding,
        "objects_limit": objects_limit,
        "ignore_errors": ignore_errors,
        "scan": not no_scan,
        "stats": not no_stats,
    }
    acmd = Analyzer()
    acmd.analyze(input_file, options)


def _run_doc_command(
    input_file: str,
    format: str,
    output: Optional[str],
    sample_size: int,
    verbose: bool,
    engine: str,
    delimiter: str,
    encoding: Optional[str],
    tagname: Optional[str],
    start_line: int,
    start_page: int,
    format_in: Optional[str],
    autodoc: bool,
    lang: str,
    ai_provider: Optional[str],
    ai_model: Optional[str],
    ai_base_url: Optional[str],
    semantic_types: bool,
    pii_detect: bool,
    pii_mask_samples: bool,
):
    if verbose:
        enable_verbose()

    ai_config = {}
    if ai_model:
        ai_config["model"] = ai_model
    if ai_base_url:
        ai_config["base_url"] = ai_base_url

    options = {
        "format": format,
        "output": output,
        "sample_size": sample_size,
        "engine": engine,
        "delimiter": delimiter,
        "encoding": encoding,
        "tagname": tagname,
        "start_line": start_line,
        "start_page": start_page,
        "format_in": format_in,
        "autodoc": autodoc,
        "lang": lang,
        "ai_provider": ai_provider,
        "ai_config": ai_config if ai_config else None,
        "semantic_types": semantic_types,
        "pii_detect": pii_detect,
        "pii_mask_samples": pii_mask_samples,
    }
    acmd = Documenter()
    acmd.document(input_file, options)


@data_app.command()
def doc(
    input_file: Annotated[str, typer.Argument(help="Path to input file to document.")],
    format: Annotated[
        str, typer.Option(help="Output format: 'markdown' (default), 'json', 'yaml', or 'text'.")
    ] = "markdown",
    output: Annotated[
        Optional[str],
        typer.Option(help="Optional output file path. If not specified, prints to stdout."),
    ] = None,
    sample_size: Annotated[
        int, typer.Option(help="Number of sample records to include (default: 10).")
    ] = 10,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default) or 'duckdb'.")
    ] = "auto",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[
        Optional[str], typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")
    ] = None,
    tagname: Annotated[
        Optional[str], typer.Option(help="XML tag name that contains individual records.")
    ] = None,
    start_line: Annotated[
        int, typer.Option(help="Line number (0-based) to start reading from.")
    ] = 0,
    start_page: Annotated[
        int, typer.Option(help="Page number (0-based) to start from for Excel files.")
    ] = 0,
    format_in: Annotated[
        Optional[str],
        typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl')."),
    ] = None,
    autodoc: Annotated[
        bool, typer.Option(help="Enable AI-powered automatic field and dataset documentation.")
    ] = False,
    lang: Annotated[
        str, typer.Option(help="Language for AI-generated documentation (default: 'English').")
    ] = "English",
    ai_provider: Annotated[
        Optional[str],
        typer.Option(
            help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'."
        ),
    ] = None,
    ai_model: Annotated[
        Optional[str],
        typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI)."),
    ] = None,
    ai_base_url: Annotated[
        Optional[str],
        typer.Option(
            help="Base URL for AI API (optional, uses provider-specific defaults if not specified)."
        ),
    ] = None,
    semantic_types: Annotated[
        bool, typer.Option(help="Enable semantic type annotations using Metacrafter.")
    ] = False,
    pii_detect: Annotated[
        bool, typer.Option(help="Enable PII detection using Metacrafter.")
    ] = False,
    pii_mask_samples: Annotated[
        bool, typer.Option(help="Redact detected PII values in sample records.")
    ] = False,
):
    """Generate documentation for a dataset."""
    _run_doc_command(
        input_file=input_file,
        format=format,
        output=output,
        sample_size=sample_size,
        verbose=verbose,
        engine=engine,
        delimiter=delimiter,
        encoding=encoding,
        tagname=tagname,
        start_line=start_line,
        start_page=start_page,
        format_in=format_in,
        autodoc=autodoc,
        lang=lang,
        ai_provider=ai_provider,
        ai_model=ai_model,
        ai_base_url=ai_base_url,
        semantic_types=semantic_types,
        pii_detect=pii_detect,
        pii_mask_samples=pii_mask_samples,
    )


@data_app.command()
def document(
    input_file: Annotated[str, typer.Argument(help="Path to input file to document.")],
    format: Annotated[
        str, typer.Option(help="Output format: 'markdown' (default), 'json', 'yaml', or 'text'.")
    ] = "markdown",
    output: Annotated[
        Optional[str],
        typer.Option(help="Optional output file path. If not specified, prints to stdout."),
    ] = None,
    sample_size: Annotated[
        int, typer.Option(help="Number of sample records to include (default: 10).")
    ] = 10,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default) or 'duckdb'.")
    ] = "auto",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[
        Optional[str], typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")
    ] = None,
    tagname: Annotated[
        Optional[str], typer.Option(help="XML tag name that contains individual records.")
    ] = None,
    start_line: Annotated[
        int, typer.Option(help="Line number (0-based) to start reading from.")
    ] = 0,
    start_page: Annotated[
        int, typer.Option(help="Page number (0-based) to start from for Excel files.")
    ] = 0,
    format_in: Annotated[
        Optional[str],
        typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl')."),
    ] = None,
    autodoc: Annotated[
        bool, typer.Option(help="Enable AI-powered automatic field and dataset documentation.")
    ] = False,
    lang: Annotated[
        str, typer.Option(help="Language for AI-generated documentation (default: 'English').")
    ] = "English",
    ai_provider: Annotated[
        Optional[str],
        typer.Option(
            help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'."
        ),
    ] = None,
    ai_model: Annotated[
        Optional[str],
        typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI)."),
    ] = None,
    ai_base_url: Annotated[
        Optional[str],
        typer.Option(
            help="Base URL for AI API (optional, uses provider-specific defaults if not specified)."
        ),
    ] = None,
    semantic_types: Annotated[
        bool, typer.Option(help="Enable semantic type annotations using Metacrafter.")
    ] = False,
    pii_detect: Annotated[
        bool, typer.Option(help="Enable PII detection using Metacrafter.")
    ] = False,
    pii_mask_samples: Annotated[
        bool, typer.Option(help="Redact detected PII values in sample records.")
    ] = False,
):
    """Generate documentation for a dataset (alias for doc)."""
    _run_doc_command(
        input_file=input_file,
        format=format,
        output=output,
        sample_size=sample_size,
        verbose=verbose,
        engine=engine,
        delimiter=delimiter,
        encoding=encoding,
        tagname=tagname,
        start_line=start_line,
        start_page=start_page,
        format_in=format_in,
        autodoc=autodoc,
        lang=lang,
        ai_provider=ai_provider,
        ai_model=ai_model,
        ai_base_url=ai_base_url,
        semantic_types=semantic_types,
        pii_detect=pii_detect,
        pii_mask_samples=pii_mask_samples,
    )


@data_app.command()
def schema(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    outtype: Annotated[
        str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")
    ] = "text",
    format: Annotated[
        str,
        typer.Option(
            help="Schema format: 'yaml' (default), 'json', 'cerberus', 'jsonschema', 'avro', or 'parquet'. Overrides outtype when specified."
        ),
    ] = None,
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    autodoc: Annotated[
        bool, typer.Option(help="Enable AI-powered automatic field documentation.")
    ] = False,
    lang: Annotated[
        str, typer.Option(help="Language for AI-generated documentation (default: 'English').")
    ] = "English",
    ai_provider: Annotated[
        str,
        typer.Option(
            help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'."
        ),
    ] = None,
    ai_model: Annotated[
        str,
        typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI)."),
    ] = None,
    ai_base_url: Annotated[
        str,
        typer.Option(
            help="Base URL for AI API (optional, uses provider-specific defaults if not specified)."
        ),
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")
    ] = "auto",
):
    """Extract schema from a data file.

    Generates a schema definition describing the structure and types of fields in the data.
    Supports multiple output formats including YAML, JSON, Cerberus, JSON Schema, Avro, and Parquet.
    """
    if verbose:
        enable_verbose()

    # Build AI configuration
    ai_config = {}
    if ai_model:
        ai_config["model"] = ai_model
    if ai_base_url:
        ai_config["base_url"] = ai_base_url

    options = {
        "outtype": outtype,
        "format": format,
        "output": output,
        "autodoc": autodoc,
        "lang": lang,
        "ai_provider": ai_provider,
        "ai_config": ai_config if ai_config else None,
        "engine": engine,
    }
    acmd = Schemer()
    acmd.extract_schema(input_file, options)


@data_app.command()
def schema_bulk(
    input_file: Annotated[
        str,
        typer.Argument(
            help="Glob pattern or directory path for input files (e.g., 'data/*.csv' or 'data/')."
        ),
    ],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    outtype: Annotated[
        str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")
    ] = "text",
    format: Annotated[
        str,
        typer.Option(
            help="Schema format: 'yaml' (default), 'json', 'cerberus', 'jsonschema', 'avro', or 'parquet'. Overrides outtype when specified."
        ),
    ] = None,
    output: Annotated[str, typer.Option(help="Output directory path for schema files.")] = None,
    mode: Annotated[
        str,
        typer.Option(
            help="Extraction mode: 'distinct' (extract unique schemas, default) or 'perfile' (one schema per file)."
        ),
    ] = "distinct",
    autodoc: Annotated[
        bool, typer.Option(help="Enable AI-powered automatic field documentation.")
    ] = False,
    lang: Annotated[
        str, typer.Option(help="Language for AI-generated documentation (default: 'English').")
    ] = "English",
    ai_provider: Annotated[
        str,
        typer.Option(
            help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'."
        ),
    ] = None,
    ai_model: Annotated[
        str,
        typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI)."),
    ] = None,
    ai_base_url: Annotated[
        str,
        typer.Option(
            help="Base URL for AI API (optional, uses provider-specific defaults if not specified)."
        ),
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")
    ] = "auto",
):
    """Extract schemas from multiple files.

    Processes multiple files and extracts their schemas, either as distinct unique schemas
    or one schema per file.
    """
    if verbose:
        enable_verbose()

    # Build AI configuration
    ai_config = {}
    if ai_model:
        ai_config["model"] = ai_model
    if ai_base_url:
        ai_config["base_url"] = ai_base_url

    options = {
        "outtype": outtype,
        "format": format,
        "output": output,
        "mode": mode,
        "autodoc": autodoc,
        "lang": lang,
        "ai_provider": ai_provider,
        "ai_config": ai_config if ai_config else None,
        "engine": engine,
    }
    acmd = Schemer()
    acmd.extract_schema_bulk(input_file, options)


@data_app.command()
def ingest(
    input_file: Annotated[
        str, typer.Argument(help="Path to input file or glob pattern (e.g., 'data/*.jsonl').")
    ],
    uri: Annotated[
        str,
        typer.Argument(
            help="Database connection URI (e.g., 'mongodb://localhost:27017', 'postgresql://user:pass@host:5432/db', or 'https://elasticsearch:9200')."
        ),
    ],
    db: Annotated[str, typer.Argument(help="Database name.")],
    table: Annotated[str, typer.Argument(help="Collection or table name.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    batch: Annotated[
        int,
        typer.Option(help="Batch size for ingestion (number of records per batch, default: 1000)."),
    ] = DEFAULT_BATCH_SIZE,
    dbtype: Annotated[
        str,
        typer.Option(
            help="Database type: 'mongodb' (default), 'postgresql', 'duckdb', 'mysql', 'sqlite', 'elasticsearch', or 'elastic'."
        ),
    ] = "mongodb",
    totals: Annotated[
        bool, typer.Option(help="Show total record counts during ingestion.")
    ] = False,
    drop: Annotated[
        bool, typer.Option(help="Drop existing collection/table before ingestion.")
    ] = False,
    timeout: Annotated[
        int, typer.Option(help="Connection timeout in seconds (default: -30).")
    ] = -30,
    skip: Annotated[int, typer.Option(help="Number of records to skip at the beginning.")] = None,
    api_key: Annotated[
        str, typer.Option(help="API key for database authentication (Elasticsearch).")
    ] = None,
    doc_id: Annotated[
        str, typer.Option(help="Field name to use as document ID (Elasticsearch, default: 'id').")
    ] = None,
    mode: Annotated[
        str,
        typer.Option(
            help="Ingestion mode for PostgreSQL/DuckDB/MySQL/SQLite: 'append' (default), 'replace', or 'upsert'."
        ),
    ] = "append",
    create_table: Annotated[
        bool,
        typer.Option(help="Auto-create table from data schema (PostgreSQL/DuckDB/MySQL/SQLite)."),
    ] = False,
    upsert_key: Annotated[
        str,
        typer.Option(
            help="Field name(s) to use for conflict resolution in upsert mode (PostgreSQL/DuckDB/MySQL/SQLite, comma-separated for multiple keys)."
        ),
    ] = None,
    use_appender: Annotated[
        bool,
        typer.Option(help="Use Appender API for DuckDB (streaming insertion, default: False)."),
    ] = False,
):
    """Ingest data into a database.

    Supports MongoDB, PostgreSQL, DuckDB, MySQL, SQLite, and Elasticsearch databases.
    Reads data from files and inserts them into the specified database collection or table.

    For PostgreSQL:
    - Use COPY FROM for maximum performance (10-100x faster than INSERT)
    - Supports append, replace, and upsert modes
    - Can auto-create tables from data schema
    - Uses connection pooling for efficient connection management

    For DuckDB:
    - Fast bulk loading with optimized batch inserts
    - Supports append, replace, and upsert modes
    - Can auto-create tables from data schema
    - Appender API available for streaming insertion
    - Works with file-based or in-memory databases

    For MySQL:
    - Multi-row INSERT for efficient batch operations
    - Supports append, replace, and upsert modes
    - Can auto-create tables from data schema
    - Uses connection pooling for efficient connection management

    For SQLite:
    - Optimized batch inserts with PRAGMA optimizations
    - Supports append, replace, and upsert modes
    - Can auto-create tables from data schema
    - Works with file-based or in-memory databases
    """
    if verbose:
        enable_verbose()

    # Parse upsert_key if provided (can be comma-separated)
    upsert_key_parsed = None
    if upsert_key:
        upsert_key_parsed = [k.strip() for k in upsert_key.split(",")]
        if len(upsert_key_parsed) == 1:
            upsert_key_parsed = upsert_key_parsed[0]

    options = {
        "dbtype": dbtype,
        "skip": skip,
        "drop": drop,
        "totals": totals,
        "doc_id": doc_id,
        "api_key": api_key,
        "timeout": timeout,
        "mode": mode,
        "create_table": create_table,
        "upsert_key": upsert_key_parsed,
        "use_appender": use_appender,
    }
    acmd = Ingester(batch)
    files = glob.glob(input_file.strip("'"))
    acmd.ingest(files, uri, db, table, options)


@data_app.command()
def query(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of field names (kept for compatibility).")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    format_out: Annotated[
        str, typer.Option(help="Override output format (e.g., 'csv', 'jsonl').")
    ] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    query_expr: Annotated[
        str, typer.Option(help="MistQL query expression to execute on the data.")
    ] = None,
):
    """Query data using MistQL query language.

    .. note:: Experimental feature. Requires 'mistql' package: pip install mistql

    Executes MistQL queries on the input data and returns the results.
    """
    if verbose:
        enable_verbose()
    options = {
        "delimiter": delimiter,
        "fields": fields,
        "output": output,
        "encoding": encoding,
        "format_in": format_in,
        "format_out": format_out,
        "zipfile": zipfile,
        "query": query_expr,
    }
    acmd = DataQuery()
    acmd.query(input_file, options)


@data_app.command()
def count(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[
        str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")
    ] = "auto",
):
    """Count the number of rows in a data file.

    Returns the total number of data rows (excluding header if present).
    With DuckDB engine, counting is instant for supported formats.
    """
    if verbose:
        enable_verbose()
    options = {"delimiter": delimiter, "encoding": encoding, "filetype": filetype, "engine": engine}
    acmd = Counter()
    acmd.count(input_file, options)


@data_app.command()
def head(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    n: Annotated[int, typer.Option(help="Number of rows to extract (default: 10).")] = 10,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Extract the first N rows from a data file.

    Useful for quick data inspection.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "n": n,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Head()
    acmd.head(input_file, options)


@data_app.command()
def tail(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    n: Annotated[int, typer.Option(help="Number of rows to extract (default: 10).")] = 10,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Extract the last N rows from a data file.

    Uses efficient buffering for large files.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "n": n,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Tail()
    acmd.tail(input_file, options)


@data_app.command()
def mask(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of fields to mask (e.g., 'email,phone,ssn').")
    ] = None,
    method: Annotated[
        str, typer.Option(help="Masking method: 'redact' (default), 'hash', or 'randomize'.")
    ] = "redact",
    salt: Annotated[
        str, typer.Option(help="Optional salt for hash method (for additional security).")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    format_out: Annotated[
        str, typer.Option(help="Override output file format (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Mask sensitive fields in a data file for anonymization.

    Supports three masking methods:
    - redact: Replace with fixed token (e.g., '***')
    - hash: Deterministic one-way hash (preserves joins, hides identities)
    - randomize: Replace with random but type-compatible values

    Examples:
        # Redact email and phone fields
        undatum mask data.csv --fields email,phone --method redact --output masked.csv

        # Hash user IDs (deterministic, preserves joins)
        undatum mask data.jsonl --fields user_id --method hash --output masked.jsonl

        # Randomize age and email fields
        undatum mask data.csv --fields age,email --method randomize --output masked.csv
    """
    if verbose:
        enable_verbose()
    options = {
        "fields": fields,
        "method": method,
        "salt": salt,
        "delimiter": delimiter,
        "encoding": encoding,
        "format_in": format_in,
        "format_out": format_out,
    }
    acmd = Masker()
    acmd.mask(input_file, output, options)


@data_app.command()
def enum(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    field: Annotated[
        str, typer.Option(help="Field name for the generated values (default: 'row_id').")
    ] = "row_id",
    type: Annotated[
        str, typer.Option(help="Type of value: 'number' (default), 'uuid', or 'constant'.")
    ] = "number",
    start: Annotated[
        int, typer.Option(help="Starting number for numeric enumeration (default: 1).")
    ] = 1,
    value: Annotated[
        str, typer.Option(help="Constant value to use when type is 'constant'.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Add row numbers, UUIDs, or constant values to records.

    Useful for adding unique identifiers or sequential numbers to data.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "field": field,
        "type": type,
        "start": start,
        "value": value,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Enumerator()
    acmd.enum(input_file, options)


@data_app.command()
def reverse(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[
        str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")
    ] = "auto",
):
    """Reverse the order of rows in a data file.

    For large files, may require buffering. DuckDB engine provides optimization for supported formats.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": filetype,
        "engine": engine,
    }
    acmd = Reverser()
    acmd.reverse(input_file, options)


@data_app.command()
def table(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    limit: Annotated[
        int, typer.Option(help="Maximum number of rows to display (default: 20).")
    ] = 20,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of field names to display.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Display data in a formatted, aligned table for inspection.

    Uses the rich library to create a nicely formatted table output.
    """
    if verbose:
        enable_verbose()
    options = {
        "limit": limit,
        "fields": fields,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = TableFormatter()
    acmd.table(input_file, options)


@data_app.command()
def fixlengths(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    strategy: Annotated[str, typer.Option(help="Strategy: 'pad' (default) or 'truncate'.")] = "pad",
    value: Annotated[
        str, typer.Option(help="Value to use for padding (default: empty string).")
    ] = "",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Ensure all rows have the same number of fields.

    Pads shorter rows or truncates longer rows to normalize field counts.
    Useful for data cleaning workflows.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "strategy": strategy,
        "value": value,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = FixLengths()
    acmd.fixlengths(input_file, options)


@data_app.command()
def sort(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    by: Annotated[str, typer.Option(help="Comma-separated list of field names to sort by.")] = None,
    desc: Annotated[bool, typer.Option(help="Sort in descending order.")] = False,
    numeric: Annotated[
        str, typer.Option(help="Comma-separated list of field names to sort numerically.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[
        str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'python'.")
    ] = "auto",
    duckdb_threads: Annotated[
        int, typer.Option(help="Number of threads for DuckDB engine.")
    ] = None,
    duckdb_memory: Annotated[
        str, typer.Option(help="Memory limit for DuckDB (e.g., '4GB', '512MB').")
    ] = None,
    duckdb_temp_dir: Annotated[str, typer.Option(help="Temporary directory for DuckDB.")] = None,
    low_memory: Annotated[
        bool,
        typer.Option(help="Force external merge sort (spill sorted runs to disk)."),
    ] = False,
):
    """Sort rows by one or more columns.

    Supports multiple sort keys, ascending/descending order, and numeric sorting.
    Uses external merge sort for large files (auto above 100k rows, or with --low-memory).
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "by": by,
        "desc": desc,
        "numeric": numeric,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": filetype,
        "engine": engine,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
        "duckdb_temp_dir": duckdb_temp_dir,
        "low_memory": low_memory,
        "temp_dir": duckdb_temp_dir,
    }
    acmd = Sorter()
    acmd.sort(input_file, options)


@data_app.command()
def sample(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    n: Annotated[int, typer.Option(help="Number of rows to sample.")] = None,
    percent: Annotated[float, typer.Option(help="Percentage of rows to sample (0-100).")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'python'.")
    ] = "auto",
    duckdb_threads: Annotated[
        int, typer.Option(help="Number of threads for DuckDB engine.")
    ] = None,
    duckdb_memory: Annotated[
        str, typer.Option(help="Memory limit for DuckDB (e.g., '4GB', '512MB').")
    ] = None,
    duckdb_temp_dir: Annotated[str, typer.Option(help="Temporary directory for DuckDB.")] = None,
):
    """Randomly select rows from a data file.

    Uses reservoir sampling algorithm that doesn't require loading all data into memory.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "n": n,
        "percent": percent,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
        "format_in": format_in,  # Support both names
        "engine": engine,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
        "duckdb_temp_dir": duckdb_temp_dir,
    }
    acmd = Sampler()
    acmd.sample(input_file, options)


@data_app.command()
def search(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    pattern: Annotated[str, typer.Option(help="Regex pattern to search for.")] = None,
    fields: Annotated[
        str,
        typer.Option(
            help="Comma-separated list of field names to search in (default: all fields)."
        ),
    ] = None,
    ignore_case: Annotated[bool, typer.Option(help="Case-insensitive search.")] = False,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'python'.")
    ] = "auto",
    duckdb_threads: Annotated[
        int, typer.Option(help="Number of threads for DuckDB engine.")
    ] = None,
    duckdb_memory: Annotated[
        str, typer.Option(help="Memory limit for DuckDB (e.g., '4GB', '512MB').")
    ] = None,
    duckdb_temp_dir: Annotated[str, typer.Option(help="Temporary directory for DuckDB.")] = None,
):
    """Filter rows using regex patterns.

    Searches across specified fields or all fields, outputting rows that match the pattern.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "pattern": pattern,
        "fields": fields,
        "ignore_case": ignore_case,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
        "format_in": format_in,  # Support both names
        "engine": engine,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
        "duckdb_temp_dir": duckdb_temp_dir,
    }
    acmd = Searcher()
    acmd.search(input_file, options)


@data_app.command()
def dedup(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    key_fields: Annotated[
        str,
        typer.Option(
            help="Comma-separated list of field names to use for deduplication (default: all fields)."
        ),
    ] = None,
    keep: Annotated[
        str, typer.Option(help="Which duplicate to keep: 'first' (default) or 'last'.")
    ] = "first",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[
        str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'python'.")
    ] = "auto",
    duckdb_threads: Annotated[
        int, typer.Option(help="Number of threads for DuckDB engine.")
    ] = None,
    duckdb_memory: Annotated[
        str, typer.Option(help="Memory limit for DuckDB (e.g., '4GB', '512MB').")
    ] = None,
    duckdb_temp_dir: Annotated[str, typer.Option(help="Temporary directory for DuckDB.")] = None,
    low_memory: Annotated[
        bool,
        typer.Option(help="Force disk-backed exact deduplication (spill keys to SQLite)."),
    ] = False,
):
    """Remove duplicate rows.

    Can deduplicate by all fields or specified key fields. Supports keeping first or last occurrence.
    Large unique-key sets automatically spill to disk; use --low-memory to force that path.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "key_fields": key_fields,
        "keep": keep,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": filetype,
        "engine": engine,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
        "duckdb_temp_dir": duckdb_temp_dir,
        "low_memory": low_memory,
        "temp_dir": duckdb_temp_dir,
    }
    acmd = Deduplicator()
    acmd.dedup(input_file, options)


@data_app.command()
def fill(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    fields: Annotated[
        str, typer.Option(help="Comma-separated list of field names to fill (default: all fields).")
    ] = None,
    strategy: Annotated[
        str, typer.Option(help="Fill strategy: 'constant' (default), 'forward', or 'backward'.")
    ] = "constant",
    value: Annotated[
        str,
        typer.Option(help="Constant value to use for filling (required for 'constant' strategy)."),
    ] = "",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Fill empty or null values with specified values or strategies.

    Supports constant filling, forward-fill (use previous value), and backward-fill (use next value).
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "fields": fields,
        "strategy": strategy,
        "value": value,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Filler()
    acmd.fill(input_file, options)


@data_app.command()
def rename(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    map: Annotated[
        str, typer.Option(help="Field name mapping: 'old_name:new_name,old2:new2'.")
    ] = None,
    pattern: Annotated[
        str, typer.Option(help="Regex pattern to match field names (for regex-based renaming).")
    ] = None,
    replacement: Annotated[
        str, typer.Option(help="Replacement string for regex pattern (default: empty string).")
    ] = "",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Rename fields by exact mapping or regex patterns.

    Supports multiple field renames in one operation.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "map": map,
        "pattern": pattern,
        "replacement": replacement,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Renamer()
    acmd.rename(input_file, options)


@data_app.command()
def explode(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    field: Annotated[str, typer.Option(help="Field name to split by separator.")] = None,
    separator: Annotated[str, typer.Option(help="Separator character (default: comma).")] = ",",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Split a column by separator into multiple rows.

    Creates one row per value in the specified field, duplicating other fields.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "field": field,
        "separator": separator,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Exploder()
    acmd.explode(input_file, options)


@data_app.command()
def replace(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    field: Annotated[str, typer.Option(help="Field name to perform replacement in.")] = None,
    pattern: Annotated[str, typer.Option(help="Pattern to search for (string or regex).")] = None,
    replacement: Annotated[str, typer.Option(help="Replacement string.")] = "",
    regex: Annotated[bool, typer.Option(help="Treat pattern as regex.")] = False,
    global_replace: Annotated[
        bool, typer.Option(help="Replace all occurrences (default: replace first only).")
    ] = False,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Perform string replacement in specified fields.

    Supports simple string replacement and regex-based replacement.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "field": field,
        "pattern": pattern,
        "replacement": replacement,
        "regex": regex,
        "global": global_replace,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Replacer()
    acmd.replace(input_file, options)


@data_app.command()
def cat(
    input_files: Annotated[
        list[str], typer.Argument(help="Path(s) to input file(s). Multiple files can be specified.")
    ],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    mode: Annotated[
        str, typer.Option(help="Concatenation mode: 'rows' (default) or 'columns'.")
    ] = "rows",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Concatenate files by rows or columns.

    Row mode: appends files vertically. Column mode: combines files side-by-side.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "mode": mode,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Cat()
    acmd.cat(input_files, options)


@data_app.command()
def join(
    file1: Annotated[str, typer.Argument(help="Path to first input file.")],
    file2: Annotated[str, typer.Argument(help="Path to second input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    on: Annotated[
        str, typer.Option(help="Comma-separated list of key field names to join on.")
    ] = None,
    type: Annotated[
        str, typer.Option(help="Join type: 'inner' (default), 'left', 'right', or 'full'.")
    ] = "inner",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype1: Annotated[
        str, typer.Option(help="Override file type detection for first file.")
    ] = None,
    filetype2: Annotated[
        str, typer.Option(help="Override file type detection for second file.")
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'python'.")
    ] = "auto",
    duckdb_threads: Annotated[
        int, typer.Option(help="Number of threads for DuckDB engine.")
    ] = None,
    duckdb_memory: Annotated[
        str, typer.Option(help="Memory limit for DuckDB (e.g., '4GB', '512MB').")
    ] = None,
    duckdb_temp_dir: Annotated[str, typer.Option(help="Temporary directory for DuckDB.")] = None,
    progress: Annotated[bool, typer.Option(help="Show progress bar.")] = False,
):
    """Perform relational join between two files.

    Supports inner, left, right, and full outer joins. Uses hash-based join for streaming formats
    and DuckDB SQL join for supported formats.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "on": on,
        "type": type,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype1": filetype1,
        "filetype2": filetype2,
        "engine": engine,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
        "duckdb_temp_dir": duckdb_temp_dir,
        "progress": progress,
    }
    acmd = Joiner()
    acmd.join(file1, file2, options)


@data_app.command()
def diff(
    file1: Annotated[str, typer.Argument(help="Path to first input file.")],
    file2: Annotated[str, typer.Argument(help="Path to second input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    key: Annotated[
        str, typer.Option(help="Comma-separated list of key field names to compare on.")
    ] = None,
    output_format: Annotated[
        str, typer.Option(help="Detailed output format: json, csv, markdown, html, or unified.")
    ] = None,
    summary_only: Annotated[
        bool, typer.Option(help="Show summary only (suppress detailed output).")
    ] = False,
    ignore_order: Annotated[
        bool, typer.Option(help="Treat datasets as unordered sets when no key is provided.")
    ] = False,
    numeric_tolerance: Annotated[
        float, typer.Option(help="Numeric tolerance for float comparisons.")
    ] = None,
    ignore_case: Annotated[
        bool, typer.Option(help="Case-insensitive comparison for strings.")
    ] = False,
    max_added_rows: Annotated[
        int, typer.Option(help="Fail if added rows exceed this threshold.")
    ] = None,
    max_removed_rows: Annotated[
        int, typer.Option(help="Fail if removed rows exceed this threshold.")
    ] = None,
    max_changed_rows: Annotated[
        int, typer.Option(help="Fail if changed rows exceed this threshold.")
    ] = None,
    format: Annotated[
        str, typer.Option(help="(Deprecated) Output format: 'json' or 'unified'.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Compare two files and show differences.

    Outputs added, removed, and changed rows based on key fields.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "key": key,
        "output_format": output_format,
        "summary_only": summary_only,
        "ignore_order": ignore_order,
        "numeric_tolerance": numeric_tolerance,
        "ignore_case": ignore_case,
        "max_added_rows": max_added_rows,
        "max_removed_rows": max_removed_rows,
        "max_changed_rows": max_changed_rows,
        "format": format,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Differ()
    acmd.diff(file1, file2, options)


@data_app.command()
def exclude(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    exclude_file: Annotated[str, typer.Argument(help="Path to exclusion file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    on: Annotated[
        str, typer.Option(help="Comma-separated list of key field names to exclude on.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Remove rows from input file where keys match exclusion file.

    Uses hash-based lookup for performance.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "on": on,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Excluder()
    acmd.exclude(input_file, exclude_file, options)


@data_app.command()
def transpose(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Swap rows and columns.

    Transposes the data table, handling headers appropriately.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Transposer()
    acmd.transpose(input_file, options)


@data_app.command()
def sniff(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    format: Annotated[
        str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")
    ] = "text",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Detect file properties (delimiter, encoding, types, record count).

    Analyzes the file and reports detected properties.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "format": format,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Sniffer()
    acmd.sniff(input_file, options)


@data_app.command()
def slice(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    start: Annotated[int, typer.Option(help="Start index (inclusive).")] = None,
    end: Annotated[int, typer.Option(help="End index (inclusive).")] = None,
    indices: Annotated[
        str, typer.Option(help="Comma-separated list of specific indices to extract.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[
        str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")
    ] = None,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'python'.")
    ] = "auto",
    duckdb_threads: Annotated[
        int, typer.Option(help="Number of threads for DuckDB engine.")
    ] = None,
    duckdb_memory: Annotated[
        str, typer.Option(help="Memory limit for DuckDB (e.g., '4GB', '512MB').")
    ] = None,
    duckdb_temp_dir: Annotated[str, typer.Option(help="Temporary directory for DuckDB.")] = None,
):
    """Extract specific rows by range or index list.

    Supports range-based slicing (--start/--end) or index-based slicing (--indices).
    Uses DuckDB for efficient random access when supported.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "start": start,
        "end": end,
        "indices": indices,
        "delimiter": delimiter,
        "encoding": encoding,
        "filetype": filetype,
        "engine": engine,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
        "duckdb_temp_dir": duckdb_temp_dir,
    }
    acmd = Slicer()
    acmd.slice(input_file, options)


@data_app.command()
def plot(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    field: Annotated[
        str, typer.Option(help="Field name(s) to plot (comma-separated for multiple fields).")
    ],
    type: Annotated[
        str,
        typer.Option(
            help="Plot type: 'histogram', 'bar', 'scatter', 'line', or 'auto' (default: auto)."
        ),
    ] = "auto",
    output: Annotated[
        str, typer.Option(help="Output file path. If not specified, displays plot.")
    ] = None,
    format: Annotated[
        str,
        typer.Option(
            help="Output format: 'png', 'svg', or 'pdf' (default: auto-detect from output file)."
        ),
    ] = None,
    title: Annotated[str, typer.Option(help="Plot title.")] = None,
    xlabel: Annotated[str, typer.Option(help="X-axis label.")] = None,
    ylabel: Annotated[str, typer.Option(help="Y-axis label.")] = None,
    width: Annotated[float, typer.Option(help="Figure width in inches (default: 10).")] = 10,
    height: Annotated[float, typer.Option(help="Figure height in inches (default: 6).")] = 6,
    dpi: Annotated[int, typer.Option(help="Resolution for raster formats (default: 100).")] = 100,
    color: Annotated[str, typer.Option(help="Color scheme name (matplotlib colormap).")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Generate data visualizations from data files.

    Supports multiple plot types: histograms for numerical distributions, bar charts for
    categorical frequencies, scatter plots for relationships, and line plots for time series.

    Examples:
        # Generate histogram for numerical field
        undatum plot data.csv --field age --type histogram --output age_dist.png

        # Generate bar chart for categorical field
        undatum plot data.csv --field status --type bar

        # Generate scatter plot for two fields
        undatum plot data.csv --field x,y --type scatter --output scatter.png

        # Auto-detect plot type
        undatum plot data.csv --field age --output age_plot.png
    """
    if verbose:
        enable_verbose()

    try:
        plotter = Plotter()
        plotter.plot(
            fromfile=input_file,
            field=field,
            plot_type=type,
            output=output,
            output_format=format,
            title=title,
            xlabel=xlabel,
            ylabel=ylabel,
            width=width,
            height=height,
            dpi=dpi,
            color=color,
        )
    except ImportError as e:
        logger.error(f"Plotting requires matplotlib: {e}")
        logger.error("Install with: pip install matplotlib")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Plot generation failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


@data_app.command()
def fmt(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[
        str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")
    ] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character (default: comma).")] = ",",
    quote: Annotated[
        str,
        typer.Option(help="Quote style: 'minimal' (default), 'always', 'none', or 'nonnumeric'."),
    ] = "minimal",
    escape: Annotated[
        str, typer.Option(help="Escape character: 'double' (default), 'backslash', or 'none'.")
    ] = "double",
    line_ending: Annotated[
        str, typer.Option(help="Line ending: 'unix' (default), 'windows', 'crlf', or 'mac'.")
    ] = "unix",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[
        str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")
    ] = None,
):
    """Reformat CSV data with specific formatting options.

    Controls delimiter, quote style, escape character, and line endings.
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "delimiter": delimiter,
        "quote": quote,
        "escape": escape,
        "line_ending": line_ending,
        "encoding": encoding,
        "filetype": format_in,
    }
    acmd = Formatter()
    acmd.fmt(input_file, options)


@data_app.command()
def sql(
    query: Annotated[str, typer.Argument(help="DuckDB SQL query to execute.")],
    input_files: Annotated[
        list[str],
        typer.Argument(
            help="Input file(s). Each file is available as a view named after its file stem; "
            "a single file is also available as 'data'."
        ),
    ],
    output: Annotated[
        Optional[str], typer.Option(help="Output file path. Prints to stdout if omitted.")
    ] = None,
    format: Annotated[
        str, typer.Option(help="Output format: 'jsonl' (default), 'csv', or 'parquet'.")
    ] = "jsonl",
    duckdb_threads: Annotated[Optional[int], typer.Option(help="Number of DuckDB threads.")] = None,
    duckdb_memory: Annotated[
        Optional[str], typer.Option(help="DuckDB memory limit (e.g., '4GB', '512MB').")
    ] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Run an ad-hoc DuckDB SQL query over data files.

    Examples:
        # Query a single CSV (referenced as 'data')
        undatum sql "SELECT city, COUNT(*) AS n FROM data GROUP BY city" cities.csv

        # Join two files (views named after file stems)
        undatum sql "SELECT * FROM orders JOIN users USING (user_id)" orders.csv users.parquet

        # Save result as parquet
        undatum sql "SELECT * FROM data WHERE amount > 100" sales.jsonl --output big.parquet --format parquet
    """
    if verbose:
        enable_verbose()
    options = {
        "output": output,
        "format": format,
        "duckdb_threads": duckdb_threads,
        "duckdb_memory": duckdb_memory,
    }
    SqlExecutor().query(query, input_files, options)
