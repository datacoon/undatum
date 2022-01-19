#!/usr/bin/env python
# -*- coding: utf8 -*-
import click
import logging

from .cmds.converter import Converter
from .cmds.selector import Selector
from .cmds.transformer import Transformer
from .cmds.analyzer import Analyzer
from .cmds.textproc import TextProcessor
from .cmds.validator import Validator
from .cmds.schemer import Schemer

#logging.getLogger().addHandler(logging.StreamHandler())
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)

def enableVerbose():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.DEBUG)


@click.group()
def cli1():
    pass

@cli1.command()
@click.argument('input')
@click.argument('output')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV, default ','")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding, default is utf8")
@click.option('--verbose', '-v', count=False, help='Verbose output. Print additional info')
@click.option('--prefix-strip', default=True, is_flag=True, help="Strip prefix from XML files")
@click.option('--fields', '-f', default=None, help="Fieldnames from XLS conversion")
@click.option('--start-line', default=0, help="Start line, used for XLS and XLSX conversion")
@click.option('--skip-end-rows', default=0, help="Skip rows at the end of xls file")
@click.option('--start-page', default=0, help="Start page, used for XLS and XLSX conversion")
@click.option('--tagname',  default=None, help="Object tagname, needed for xml2json")
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('--format-out',  default=None, help="Format of output file, if set, replaces autodetect")
@click.option('-z', '--zipfile', 'zipfile', is_flag=False, help="Used to say input file is .zip file and that data file is inside")
def convert(input, output, delimiter, encoding, verbose, prefix_strip, fields, start_line, skip_end_rows, start_page, tagname, format_in, format_out, zipfile):
    """Converts one file to another. Supports XML, CSV, JSON, BSON"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
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


@click.group()
def cli2():
    pass

@cli2.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--fields', '-f', default=None, help="Fieldnames, delimiter by ','")
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('--format-out',  default=None, help="Format of output file, if set, replaces autodetect")
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
@click.option('--filter',  default=None, help="Filter input file with dict query")
def uniq(input, output, fields, delimiter, encoding, verbose, format_in, format_out, zipfile, filter):
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

@click.group()
def cli3():
    pass

@cli3.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--limit', '-l', default=1000, help="Limit of lines used to detect headers of JSON/BSON files only")
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('--format-out',  default=None, help="Format of output file, if set, replaces autodetect")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
def headers(input, output, delimiter, encoding, limit, format_in, format_out, verbose, zipfile):
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

@click.group()
def cli4():
    pass

@cli4.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--dictshare', '-s', 'dictshare', default=None, help="Uniqness level of certain field to detect that it's dict")
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('--format-out',  default=None, help="Format of output file, if set, replaces autodetect")
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
@click.option('--checkdates', is_flag=True, help="Significantly slow down process, buy identifies dates fields from text. Not used by default")
def stats(input, output, dictshare, format_in, format_out, delimiter, verbose, zipfile, checkdates):
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
    options['verbose'] = verbose
    acmd = Analyzer(nodates=not checkdates)
    acmd.analyze(input, options)
    pass


@click.group()
def cli5():
    pass

@cli5.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--filter',  default=None, help="Filter input file with dict query")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
def flatten(input, output, delimiter, encoding, filter, verbose):
    """Flatten data records. Write them as one value per row"""
    if verbose:
        enableVerbose()
    options = {}
    options['delimiter'] = delimiter
    options['output'] = output
    options['encoding'] = encoding
    options['filter'] = filter
    acmd = TextProcessor()
    acmd.flatten(input, options)
    pass


@click.group()
def cli6():
    pass

@cli6.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--fields', '-f', default=None, help="Fieldnames, delimiter by ','")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('--format-out',  default=None, help="Format of output file, if set, replaces autodetect")
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
@click.option('--filter',  default=None, help="Filter input file with dict query")
def frequency(input, output, fields, delimiter, encoding, verbose, format_in, format_out, zipfile, filter):
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


@click.group()
def cli7():
    pass

@cli7.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--fields', '-f', default=None, help="Fieldnames, delimiter by ','")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('--format-out',  default=None, help="Format of output file, if set, replaces autodetect")
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
@click.option('--filter',  default=None, help="Filter input file with dict query")
def select(input, output, fields, delimiter, encoding, verbose, format_in, format_out, zipfile, filter):
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


@click.group()
def cli8():
    pass

@cli8.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--fields', '-f', default=None, help="Fieldnames, delimiter by ','")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
@click.option('--gzip', '-g', 'gzipfile', is_flag=True, help="Used to say input file is gzipped file")
@click.option('-c', '--chunksize', default=10000, help='Default chunk size of file to split. Used if field value not given')
@click.option('--filter',  default=None, help="Filter input file with dict query")
def split(input, output, fields, delimiter, encoding, verbose, format_in, zipfile, gzipfile, chunksize, filter):
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
    acmd = Selector()
    acmd.split(input, options)
    pass

@click.group()
def cli9():
    pass


@cli9.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--fields', '-f', default=None, help="Fieldnames, delimiter by ','")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
@click.option('-r', '--rule',  default=None, required=True, help="Validation rule")
@click.option('--filter',  default=None, help="Filter input file with dict query")
@click.option('-m', '--mode',  default='invalid', help="Mode of validation output: invalid, all, or stats")
def validate(input, output, fields, delimiter, encoding, verbose, format_in, zipfile, rule, filter, mode):
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

@click.group()
def cli10():
    pass


@cli10.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--fields', '-f', default=None, help="Fieldnames, delimiter by ','")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
@click.option('-s', '--script',  default=None, required=True, help="Script to run")
@click.option('--filter',  default=None, help="Filter input file with dict query")
def apply(input, output, fields, delimiter, encoding, verbose, format_in, zipfile, script, filter):
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


@click.group()
def cli11():
    pass

@cli11.command()
@click.argument('input')
@click.option('--output', '-o', 'output', default=None, help='Output to this file')
@click.option('--delimiter', '-d', default=',', help="CSV delimiter if convert from CSV")
@click.option('--encoding', '-e', default='utf8', help="Input and output encoding")
@click.option('--verbose', '-v', count=True, help='Verbose output. Print additional info on command execution')
@click.option('--format-in',  default=None, help="Format of input file, if set, replaces autodetect")
@click.option('-z', '--zipfile', 'zipfile', is_flag=True, help="Used to say input file is .zip file and that data file is inside")
@click.option('--stype',  default='cerberus', help="Type of the schema: cerberus")
def scheme(input, output, delimiter, encoding, verbose, format_in, zipfile, stype):
    """Runs script against each record of input file"""
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


cli = click.CommandCollection(sources=[cli1, cli2, cli3, cli4, cli5, cli6, cli7, cli8, cli9, cli10, cli11])

#if __name__ == '__main__':
#    cli()

