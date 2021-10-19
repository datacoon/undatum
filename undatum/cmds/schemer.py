from ..utils import get_file_type, get_option
from ..constants import DATE_PATTERNS, DEFAULT_DICT_SHARE
from datetime import datetime
import logging
import orjson
import zipfile
from qddate import DateParser
from ..common.scheme import generate_scheme_from_file

class Schemer:
    def __init__(self, nodates=True):
        if nodates:
            self.qd = None
        else:
            self.qd = DateParser(generate=True)
        pass

    def generate_scheme(self, fromfile, options):
        """Generates cerberus scheme from JSON lines or BSON file"""
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        if f_type not in ['jsonl', 'bson', 'csv']:
            print('Only JSON lines, CSV and BSON (.jsonl, .csv, .bson) files supported now')
            return
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            finfilename = fnames[0]
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        else:
            finfilename = fromfile
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))

        logging.debug('Start identifying scheme for %s' % (fromfile))
        scheme = generate_scheme_from_file(fileobj=infile, filetype=f_type, delimiter=options['delimiter'], encoding=options['encoding'])
        if options['output']:
            f = open(options['output'], 'wb', encoding='utf8')
            f.write(orjson.dumps(scheme, option=orjson.OPT_INDENT_2))
            f.close()
        else:
            print(str(orjson.dumps(scheme, option=orjson.OPT_INDENT_2)))
