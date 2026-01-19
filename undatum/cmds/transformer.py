"""Data transformation module."""
import logging
import sys
from runpy import run_path

import orjson
from iterable.helpers.detect import open_iterable

#from xmlr import xmliter
from ..utils import dict_generator, get_option

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out

DEFAULT_HEADERS_DETECT_LIMIT = 1000



class Transformer:
    """Data transformation handler."""
    def __init__(self):
        pass


    def script(self, fromfile, options=None):
        """Run certain script against selected file"""

        if options is None:
            options = {}
        script = run_path(options['script'])
        __process_func = script['process']

        iterableargs = get_iterable_options(options)

        limit = DEFAULT_HEADERS_DETECT_LIMIT

        # First pass: extract schema
        keys_set = set()  # Use set for O(1) lookup instead of O(n) list operations
        read_iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        try:
            n = 0
            for item in read_iterable:
                if limit and n > limit:
                    break
                item = __process_func(item)
                n += 1
                dk = dict_generator(item)
                for i in dk:
                    k = ".".join(i[:-1])
                    keys_set.add(k)
        finally:
            read_iterable.close()
        keys = list(keys_set)  # Convert to list for backward compatibility

        # Second pass: process and write
        write_to_iterable = False
        to_file = get_option(options, 'output')
        if to_file:
            write_to_iterable = True
            write_iterable = open_iterable(to_file, mode='w', iterableargs={'keys': keys})
        else:
            write_iterable = None

        try:
            read_iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
            try:
                # Try to use reset() if available
                if hasattr(read_iterable, 'reset'):
                    read_iterable.reset()

                n = 0
                batch = []
                for r in read_iterable:
                    n += 1
                    if n % 10000 == 0:
                        logging.info('apply script: processing %d records of %s' % (n, fromfile))
                        if write_to_iterable and len(batch) > 0:
                            if hasattr(write_iterable, 'write_bulk'):
                                write_iterable.write_bulk(batch)
                            else:
                                for item in batch:
                                    write_iterable.write(item)
                            batch = []
                    item = __process_func(r)
                    if write_to_iterable:
                        batch.append(item)
                    else:
                        sys.stdout.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))

                # Flush remaining batch
                if write_to_iterable and len(batch) > 0:
                    if hasattr(write_iterable, 'write_bulk'):
                        write_iterable.write_bulk(batch)
                    else:
                        for item in batch:
                            write_iterable.write(item)

                logging.debug('apply script: %d records processed' % (n))
            finally:
                read_iterable.close()
        finally:
            if write_iterable:
                write_iterable.close()


