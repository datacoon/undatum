"""Data validation module."""
import csv
import logging
import sys
import zipfile

import bson
import orjson

from ..common.filter import match_filter
from ..utils import get_dict_value, get_file_type, get_option
from ..validate import VALIDATION_RULEMAP


class Validator:
    """Data validation handler."""
    def __init__(self):
        pass

    def validate(self, fromfile, options=None):
        """Validates selected field against validation rule."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        format_in = get_option(options, 'format_in')
        f_type = get_file_type(fromfile) if format_in is None else format_in
        zipfile_enabled = options.get('zipfile', False)
        if zipfile_enabled:
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
                infile = open(fromfile, encoding=get_option(options, 'encoding'))
        to_file = get_option(options, 'output')
        if to_file:
            get_file_type(to_file)
            if not to_file:
                logging.debug('Output file type not supported')
                return
            out = open(to_file, 'w', encoding='utf8')
        else:
            out = sys.stdout
        fields_value = get_option(options, 'fields')
        if not fields_value:
            raise ValueError("validate requires 'fields' option (comma-separated list of fields)")
        fields = fields_value.split(',')
        rule = get_option(options, 'rule')
        if not rule:
            raise ValueError("validate requires 'rule' option")
        val_func = VALIDATION_RULEMAP[rule]
        logging.info('uniq: looking for fields: %s', fields_value)
        validated = []
        stats = {'total': 0, 'invalid': 0, 'novalue' : 0}
        if f_type == 'csv':
            delimiter = get_option(options, 'delimiter')
            reader = csv.DictReader(infile, delimiter=delimiter)
            n = 0
            for r in reader:
                n += 1
                if n % 1000 == 0:
                    logging.info('uniq: processing %d records of %s', n, fromfile)
                filter_expr = options.get('filter')
                if filter_expr is not None:
                    if not match_filter(r, filter_expr):
                        continue
                res = val_func(r[fields[0]])
                stats['total'] += 1
                if not res:
                    stats['invalid'] += 1
                validated.append({fields[0] : r[fields[0]], fields[0] + '_valid' : res})

        elif f_type == 'jsonl':
            n = 0
            for l in infile:
                n += 1
                if n % 10000 == 0:
                    logging.info('uniq: processing %d records of %s', n, fromfile)
                r = orjson.loads(l)
                filter_expr = options.get('filter')
                if filter_expr is not None:
                    if not match_filter(r, filter_expr):
                        continue
                stats['total'] += 1
                values = get_dict_value(r, fields[0].split('.'))
                if len(values) > 0:
                    res = val_func(values[0])
                    if not res:
                        stats['invalid'] += 1
                    validated.append({fields[0] : values[0], fields[0] + '_valid' : res})
                else:
                    stats['novalue'] += 1

        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            for r in bson_iter:
                n += 1
                if n % 1000 == 0:
                    logging.info('uniq: processing %d records of %s', n, fromfile)
                filter_expr = options.get('filter')
                if filter_expr is not None:
                    if not match_filter(r, filter_expr):
                        continue
                stats['total'] += 1
                values = get_dict_value(r, fields[0].split('.'))
                if len(values) > 0:
                    res = val_func(values[0])
                    if not res:
                        stats['invalid'] += 1
                    validated.append({fields[0] : values[0], fields[0] + '_valid' : res})
                else:
                    stats['novalue'] += 1
        else:
            logging.error('Invalid filed format provided')
            if not zipfile_enabled:
                infile.close()
            return
        if not zipfile_enabled:
            infile.close()
        stats['share'] = 100.0 * stats['invalid'] / stats['total']
        novalue_share = 100.0 * stats['novalue'] / stats['total']
        logging.debug('validate: complete, %d records (%.2f%%) not valid and %d '
                     '(%.2f%%) not found of %d against %s',
                     stats['invalid'], stats['share'], stats['novalue'],
                     novalue_share, stats['total'], rule)
        if options['mode'] != 'stats':
            fieldnames = [fields[0], fields[0] + '_valid']
            writer = csv.DictWriter(out, fieldnames=fieldnames,
                                    delimiter=get_option(options, 'delimiter'))
            for row in validated:
                if options['mode'] == 'invalid':
                    if not row[fields[0] + '_valid']:
                        writer.writerow(row)
                elif options['mode'] == 'all':
                    writer.writerow(row)
        else:
            out.write(str(orjson.dumps(stats, option=orjson.OPT_INDENT_2)))
        if to_file:
            out.close()
        if options['zipfile']:
            z.close()
