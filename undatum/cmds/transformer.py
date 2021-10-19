from xlrd import open_workbook
from operator import itemgetter, attrgetter
import csv
import zipfile
import sys
import orjson
import bson
import logging
#from xmlr import xmliter
import xml.etree.ElementTree as etree
from collections import defaultdict
from ..utils import get_file_type, get_option, write_items, get_dict_value, strip_dict_fields, dict_generator
import dictquery as dq
from runpy import run_path

class Transformer:
    def __init__(self):
        pass


    def script(self, fromfile, options={}):
        """Run certain script against selected file"""
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        else:
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
        to_file = get_option(options, 'output')
        if to_file:
            to_type = get_file_type(to_file)
            if not to_file:
                print('Output file type not supported')
                return
            if to_type == 'bson':
                out = open(to_file, 'wb')
            elif to_type == 'jsonl':
                out = open(to_file, 'wb')
            else:
                out = open(to_file, 'w', encoding='utf8')
        else:
            to_type = f_type
            out = sys.stdout
 #       fields = options['fields'].split(',') if options['fields'] else None
        script = run_path(options['script'])
        __process_func = script['process']
        delimiter = get_option(options, 'delimiter')
        if f_type == 'csv':
            reader = csv.DictReader(infile, delimiter=delimiter)
            if to_type == 'csv':
                writer = csv.DictWriter(out, fieldnames=fields, delimiter=delimiter)
                writer.writeheader()
            n = 0
            for r in reader:
                n += 1
                if n % 10000 == 0:
                    logging.info('apply script: processing %d records of %s' % (n, fromfile))
                item = __process_func(r)
                if to_type == 'csv':
                    writer.writerow(item)
                elif to_type == 'jsonl':
                    out.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE))
        elif f_type == 'jsonl':
            n = 0
            for l in infile:
                n += 1
                if n % 10000 == 0:
                    logging.info('apply script: processing %d records of %s' % (n, fromfile))
                r = orjson.loads(l)
                item = __process_func(r)
                out.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE))
        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            for r in bson_iter:
                n += 1
                if n % 10000 == 0:
                    logging.info('apply script: processing %d records of %s' % (n, fromfile))
                item = __process_func(r)
                out.write(str(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE)))
        else:
            logging.info('File type not supported')
            return
        logging.debug('select: %d records processed' % (n))
        out.close()


