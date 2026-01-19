"""Text processing module."""
from iterable.helpers.detect import open_iterable

from ..utils import get_file_type, get_option

#STAT_READY_DATA_FORMATS = ['jsonl', 'bson', 'csv']

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


def get_keys(adict, prefix=None):
    """Extract all keys from nested dictionary."""
    keys = {}
    for k, v in adict.items():
        fullk = '.'.join([prefix, k]) if prefix else k
        keys[fullk] = 1
        if isinstance(v, dict):
            for ak in get_keys(v, fullk):
                keys[ak] = 1
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    for ak in get_keys(item, fullk):
                        keys[ak] = 1
        else:
            print(f'{fullk}\t{str(v)}')
    return keys


class TextProcessor:
    """Text processing handler."""
    def __init__(self):
        pass

    def flatten(self, filename, options):
        """Flatten the data. One field - one line"""
        get_file_type(filename) if options['format_in'] is None else options['format_in']
        iterableargs = get_iterable_options(options)
        iterable = open_iterable(filename, mode='r', iterableargs=iterableargs)
        try:
            get_option(options, 'output')
            i = 0
            for rec in iterable:
                allkeys = {}
                i += 1
                for k in get_keys(rec):
                    v = allkeys.get(k, 0)
                    allkeys[k] = v + 1
                for k, v in allkeys.items():
                    print('\t'.join([k, str(v)]))
        finally:
            iterable.close()

