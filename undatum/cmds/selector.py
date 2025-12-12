# -*- coding: utf8 -*-
"""Data selection and filtering module."""
import csv
import logging
import os
import sys
import zipfile

import bson
import dictquery as dq
import duckdb
import orjson
from iterable.helpers.detect import detect_file_type, open_iterable

from ..common.iterable import DataWriter, IterableData
from ..constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES
from ..utils import (detect_encoding, dict_generator, get_dict_value,
                     get_file_type, get_option, strip_dict_fields)

LINEEND = '\n'.encode('utf8')


ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


def _detect_engine(fromfile, engine, filetype):
    """Detect the appropriate engine for processing."""
    compression = 'raw'
    if filetype is None:
        ftype = detect_file_type(fromfile)
        if ftype['success']:
            filetype = ftype['datatype'].id()
            if ftype['codec'] is not None:
                compression = ftype['codec'].id()
    logging.info(f'File filetype {filetype} and compression {compression}')
    if engine == 'auto':
        if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
            return 'duckdb'
        return 'iterable'
    return engine


def get_iterable_fields_uniq(iterable, fields, dolog=False, dq_instance=None):  # pylint: disable=unused-argument
    """Returns all uniq values of the fields of iterable dictionary."""
    # dq_instance kept for API compatibility
    n = 0
    uniqval = []
    for row in iterable:
        n += 1
        if dolog and n % 1000 == 0:
            logging.debug('uniq: processing %d records', n)
        try:
            allvals = []
            for field in fields:
                allvals.append(get_dict_value(row, field.split('.')))

            for n1, _ in enumerate(allvals[0]):
                k = []
                for n2, _ in enumerate(allvals):
                    k.append(str(allvals[n2][n1]))
                if k not in uniqval:
                    uniqval.append(k)
        except KeyError:
            pass
    return uniqval


def get_duckdb_fields_uniq(filename, fields, dolog=False, dq_instance=None):  # pylint: disable=unused-argument
    """Returns all uniq values of the fields of the filename using DuckdDB."""
    # dq_instance kept for API compatibility
    uniqval = []
    fieldstext = ','.join(fields)
    query = (f"select unnest(grp) from (select distinct({fieldstext}) "
             f"as grp from '{filename}')")
    if dolog:
        logging.info(query)
    uniqval = duckdb.sql(query).fetchall()
    return uniqval



def get_iterable_fields_freq(iterable, fields, dolog=False, filter_expr=None, dq_instance=None):
    """Iterates and returns most frequent values."""
    n = 0
    valuedict = {}
    items = []
    for r in iterable:
        n += 1
        if dolog and n % 10000 == 0:
            logging.info('frequency: processing %d records', n)
        if filter_expr is not None:
            query_obj = dq_instance if dq_instance is not None else dq
            if not query_obj.match(r, filter_expr):
                continue
        try:
            allvals = []
            for field in fields:
                allvals.append(get_dict_value(r, field.split('.')))

            for n1, _ in enumerate(allvals[0]):
                k = []
                for n2, _ in enumerate(allvals):
                    k.append(str(allvals[n2][n1]))
                kx = '\t'.join(k)
                v = valuedict.get(kx, 0)
                valuedict[kx] = v + 1
        except KeyError:
            pass
    for k, v in valuedict.items():
        row = k.split('\t')
        row.append(v)
        items.append(row)
    items.sort(key=lambda x: x[-1], reverse=True)
    return items

def get_duckdb_fields_freq(filename, fields, dolog=False, dq_instance=None):  # pylint: disable=unused-argument
    """Returns frequencies for the fields of the filename using DuckdDB."""
    # dq_instance kept for API compatibility
    uniqval = []
    fieldstext = ','.join(fields)
    query = (f"select {fieldstext}, count(*) as c from '{filename}' "
             f"group by {fieldstext} order by c desc")
    if dolog:
        logging.info(query)
    uniqval = duckdb.sql(query).fetchall()
    return uniqval


class Selector:
    """Data selection and filtering handler."""
    def __init__(self):
        pass

    def uniq(self, fromfile, options=None):
        """Extracts unique values by field."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, 'filetype')
        to_file = get_option(options, 'output')
        engine = get_option(options, 'engine')
        if to_file:
            to_type = get_file_type(to_file)
            if not to_file:
                logging.debug('Output file type not supported')
                return
            out = open(to_file, 'w', encoding='utf8')
        else:
            to_type = 'csv'
            out = sys.stdout
        fields = options['fields'].split(',')
        detected_engine = _detect_engine(fromfile, engine, filetype)
        if detected_engine == 'duckdb':
            output_type = 'duckdb'
            uniqval = get_duckdb_fields_uniq(fromfile, fields, dolog=True)
        elif detected_engine == 'iterable':
            output_type = 'iterable'
            iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
            logging.info('uniq: looking for fields: %s' % (options['fields']))
            uniqval = get_iterable_fields_uniq(iterable, fields, dolog=True)
            iterable.close()
        else:
            logging.info('Engine not supported. Please choose duckdb or iterable')
            return
        logging.debug('%d unique values found' % (len(uniqval)))
        writer = DataWriter(out, filetype=to_type, output_type=output_type, fieldnames=fields)
        writer.write_items(uniqval)


    def headers(self, fromfile, options=None):
        """Extracts headers values."""
        if options is None:
            options = {}
        limit = get_option(options, 'limit')
        iterableargs = get_iterable_options(options)

        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        keys_set = set()  # Use set for O(1) lookup instead of O(n) list operations
        n = 0
        for item in iterable:
            if limit and n > limit:
                break
            n += 1
            dk = dict_generator(item)
            for i in dk:
                k = ".".join(i[:-1])
                keys_set.add(k)
        iterable.close()
        keys = list(keys_set)  # Convert to list for backward compatibility
        output = get_option(options, 'output')
        if output:
            with open(output, 'w', encoding=get_option(options, 'encoding')) as f:
                f.write('\n'.join(keys))
        else:
            for x in keys:
                print(x.encode('utf8').decode('utf8', 'ignore'))

    def frequency(self, fromfile, options=None):
        """Calculates frequency of the values in the file."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, 'filetype')
        to_file = get_option(options, 'output')
        engine = get_option(options, 'engine')
        if to_file:
            to_type = get_file_type(to_file)
            if not to_file:
                logging.debug('Output file type not supported')
                return
            out = open(to_file, 'w', encoding='utf8')
        else:
            to_type = 'csv'
            out = sys.stdout
        fields = options['fields'].split(',')
        detected_engine = _detect_engine(fromfile, engine, filetype)
        items = []
        output_type = 'iterable'
        if detected_engine == 'duckdb':
            items = get_duckdb_fields_freq(fromfile, fields=fields, dolog=True)
            output_type = 'duckdb'
        elif detected_engine == 'iterable':
            output_type = 'iterable'
            iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
            if iterable is not None:
                items = get_iterable_fields_freq(iterable, fields, dolog=True)
            else:
                logging.info('File type not supported')
                return
        else:
            logging.debug('Data processing engine is not set and not detected')
            return
        logging.debug('frequency: %d unique values found' % (len(items)))
        fields.append('count')
        writer = DataWriter(out, filetype=to_type, output_type=output_type, fieldnames=fields)
        writer.write_items(items)

    def select(self, fromfile, options=None):
        """Select or re-order columns from file."""
        if options is None:
            options = {}
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        iterable = IterableData(fromfile, options=options)
        to_file = get_option(options, 'output')

        if to_file:
            to_type = get_file_type(to_file)
            if not to_file:
                print('Output file type not supported')
                return
            if to_type == 'bson':
                out = open(to_file, 'wb')
            if to_type == 'jsonl':
                out = open(to_file, 'wb')
            else:
                out = open(to_file, 'w', encoding='utf8')
        else:
            to_type = f_type
            out = sys.stdout
        fields = options['fields'].split(',')
        writer = DataWriter(out, filetype=to_type, fieldnames=fields)
        if iterable:
            n = 0
            fields = [field.split('.') for field in fields]
            chunk = []
            for r in iterable.iter():
                n += 1
                if options['filter'] is not None:
                    res = dq.match(r, options['filter'])
                    #                    print(options['filter'], r)
                    if not res:
                        continue
                r_selected = strip_dict_fields(r, fields, 0)
                if n % 1000 == 0:
                    logging.info('select: processing %d records of %s' % (n, fromfile))
                    if len(chunk) > 0:
                        writer.write_items(chunk)
                        chunk = []
                else:
                    chunk.append(r_selected)
            if len(chunk) > 0:
                writer.write_items(chunk)
        else:
            logging.info('File type not supported')
            return
        logging.debug('select: %d records processed' % (n))
        out.close()


    def split_new(self, fromfile, options=None):
        """Splits the given file with data into chunks based on chunk size or field value."""
        if options is None:
            options = {}
        iterableargs = get_iterable_options(options)
        open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        get_option(options, 'output')


    def split(self, fromfile, options=None):
        """Splits the given file with data into chunks based on chunk size or field value."""
        if options is None:
            options = {}
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            finfilename = fnames[0]
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        elif options['gzipfile']:
            import gzip
            infile = gzip.open(fromfile, 'rb')
            finfilename = fromfile.split('.', 1)[0] + '.' + f_type
        else:
            finfilename = fromfile
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                if 'encoding' in options.keys():
                    infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
                else:
                    detected_enc = detect_encoding(fromfile, limit=100000)
                    if detected_enc:
                        infile = open(fromfile, 'r', encoding=detected_enc['encoding'])
                    else:
                        infile = open(fromfile, 'r', encoding='utf8')
        fields = options['fields'].split(',') if options['fields'] is not None else None
        valuedict = {}
        delimiter = get_option(options, 'delimiter')
        if f_type == 'csv':
            reader = csv.DictReader(infile, delimiter=delimiter)
            n = 0
            chunknum = 1
            if options['fields'] is None:
                splitname = finfilename.rsplit('.', 1)[0] + '_%d.csv' % (chunknum)
                out = open(splitname, 'w', encoding=get_option(options, 'encoding'))
                writer = csv.DictWriter(out, fieldnames=reader.fieldnames, delimiter=delimiter)
                writer.writeheader()
                for r in reader:
                    n += 1
                    if n % 10000 == 0:
                        logging.info('split: processing %d records of %s' % (n, fromfile))
                    if options['filter'] is not None:
                        if not dq.match(r, options['filter']):
                            continue
                    writer.writerow(r)
                    if n % options['chunksize'] == 0:
                        out.close()
                        chunknum += 1
                        splitname = finfilename.rsplit('.', 1)[0] + '_%d.csv' % (
                            chunknum)
                        out = open(splitname, 'w',
                                   encoding=get_option(options, 'encoding'))
                        writer = csv.DictWriter(out, fieldnames=reader.fieldnames,
                                                 delimiter=delimiter)
                        writer.writeheader()
        elif f_type == 'jsonl':
            n = 0
            chunknum = 1
            if options['fields'] is None:
                splitname = finfilename.rsplit('.', 1)[0] + '_%d.jsonl' % (chunknum)
                out = open(splitname, 'wb')  # , encoding=get_option(options, 'encoding'))

                for l in infile:
                    n += 1
                    if n % 10000 == 0:
                        logging.info('split: processing %d records of %s' % (n, fromfile))
                    r = orjson.loads(l)
                    if options['filter'] is not None:
                        if not dq.match(r, options['filter']):
                            continue
                    out.write(orjson.dumps(r, option=orjson.OPT_APPEND_NEWLINE))
                    if n % options['chunksize'] == 0:
                        out.close()
                        chunknum += 1
                        splitname = finfilename.rsplit('.', 1)[0] + '_%d.jsonl' % (chunknum)
                        logging.info('split: new chunk %s' % splitname)
                        out = open(splitname, 'wb') #, encoding=get_option(options, 'encoding'))
            else:
                for l in infile:
                    n += 1
                    if n % 10000 == 0:
                        logging.info('split: processing %d records of %s' % (n, fromfile))
                    r = orjson.loads(l)
                    if options['filter'] is not None:
                        if not dq.match(r, options['filter']):
                            continue
                    try:
                        kx = get_dict_value(r, fields[0].split('.'))[0]
                    except IndexError:
                        continue
                        kx = "None"
                    if kx is None:
                        continue
                    kx = (kx.replace('\\', '-').replace('/', '-')
                          .replace('?', '-').replace('<', '-')
                          .replace('>', '-').replace('\n', ''))
                    v = valuedict.get(kx, None)
                    if v is None:
                        # splitname = finfilename.rsplit('.', 1)[0] + '_%s.jsonl' % (kx)
                        splitname = '%s.jsonl' % (kx)
                        if options['dirname'] is not None:
                            splitname = os.path.join(options['dirname'],
                                                     splitname)
                        valuedict[kx] = open(splitname, 'w', encoding='utf8')
                    valuedict[kx].write(l)
                #                    valuedict[kx].write(l.decode('utf8'))#.decode('utf8')#)
                for opened in valuedict.values():
                    opened.close()
        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            for r in bson_iter:
                n += 1
                #                print(r)
                strip_dict_fields(r, fields, 0)
                #                out.write(json.dumps(r_selected)+'\n')
                if n % 10000 == 0:
                    logging.info('split: processing %d records of %s' % (n, fromfile))

        else:
            logging.info('File type not supported')
            return
        logging.debug('split: %d records processed' % (n))
