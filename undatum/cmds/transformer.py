# -*- coding: utf8 -*-
import csv
import zipfile
import sys
import orjson
import bson
import logging
#from xmlr import xmliter
from ..utils import get_file_type, get_option, get_dict_value, strip_dict_fields, dict_generator, detect_encoding
from runpy import run_path
from iterable.helpers.detect import open_iterable

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out            

DEFAULT_HEADERS_DETECT_LIMIT = 1000



class Transformer:
    def __init__(self):
        pass


    def script(self, fromfile, options={}):
        """Run certain script against selected file"""

        script = run_path(options['script'])
        __process_func = script['process']

        iterableargs = get_iterable_options(options)
        read_iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)

        limit = DEFAULT_HEADERS_DETECT_LIMIT

        keys = []
        n = 0
        for item in read_iterable:
            if limit and n > limit:
                break
            item = __process_func(item)
            n += 1
            dk = dict_generator(item)
            for i in dk:
                k = ".".join(i[:-1])
                if k not in keys:
                    keys.append(k)

        read_iterable.close()
        read_iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)


        write_to_iterable = False
        to_file = get_option(options, 'output')
        if to_file:            
            write_to_iterable = True
            write_iterable = open_iterable(to_file, mode='w', iterableargs={'keys' : keys}) 
        n = 0
        for r in read_iterable:
            n += 1
            if n % 10000 == 0:                                                                                                                 
                logging.info('apply script: processing %d records of %s' % (n, fromfile))
            item = __process_func(r)
            if write_to_iterable:
                write_iterable.write(item)
            else:
                sys.stdout.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))
       
        logging.debug('select: %d records processed' % (n))
        read_iterable.close()
        if write_to_iterable:
            write_iterable.close()


