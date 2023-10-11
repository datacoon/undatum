# -*- coding: utf8 -*-
# import json
import logging
import sys


# from xmlr import xmliter
from ..utils import get_file_type, get_option, strip_dict_fields
from ..common.iterable import IterableData
LINEEND = u'\n'.encode('utf8')

DEFAULT_CHUNK_SIZE = 50

class DataQuery:
    def __init__(self):
        pass


    def query(self, fromfile, options={}):
        """Use mistql to query data"""
        from mistql import query
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
        fields = options['fields'].split(',') if options['fields'] else None
#        writer = DataWriter(out, filetype=to_type, fieldnames=fields)
        if iterable:
            n = 0
            fields = [field.split('.') for field in fields] if fields else None
            for r in iterable.iter():
                n += 1
                if fields:
                    r_selected = strip_dict_fields(r, fields, 0)
                else:
                    r_selected = r
                if options['query'] is not None:
                    res = query(options['query'], r_selected)
                    #                    print(options['filter'], r)
                    if not res:
                        continue
                else:
                    res = r_selected
                print(res)
        else:
            logging.info('File type not supported')
            return
        logging.debug('query: %d records processed' % (n))
        out.close()
