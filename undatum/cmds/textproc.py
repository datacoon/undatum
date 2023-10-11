# -*- coding: utf8 -*-
from ..utils import get_file_type, get_option
from ..common.iterable import IterableData

def get_keys(adict, prefix=None):
    keys = {}
    for k, v in adict.items():
        fullk = '.'.join([prefix, k]) if prefix else k
        keys[fullk] = 1
        if type(v) == type({}):
            for ak in get_keys(v, fullk):
                keys[ak] = 1
        elif type(v) == type([]):
            for item in v:
                if type(item) == type({}):
                    for ak in get_keys(item, fullk):
                        keys[ak] = 1
        else:
            print((u'%s\t%s' % (fullk, str(v))))
    return keys


class TextProcessor:
    def __init__(self):
        pass

    def flatten(self, filename, options):
        """Flatten the data. One field - one line"""
        get_file_type(filename) if options['format_in'] is None else options['format_in']
        iterable = IterableData(filename, options=options)
        get_option(options, 'output')
        i = 0
        for rec in iterable.iter():
            allkeys = {}
            i += 1
            for k in get_keys(rec):
                v = allkeys.get(k, 0)
                allkeys[k] = v + 1
            for k, v in allkeys.items():
                print('\t'.join([k, str(v)]))

