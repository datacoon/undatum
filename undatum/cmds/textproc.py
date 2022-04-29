from ..utils import get_file_type, get_option
import orjson
import csv
import bson
from ..common.iterable import IterableData, DataWriter

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
        f_type = get_file_type(filename) if options['format_in'] is None else options['format_in']
        iterable = IterableData(filename, options=options)
        to_file = get_option(options, 'output')
        i = 0
        for rec in iterable.iter():
            allkeys = {}
            i += 1
            for k in get_keys(rec):
                v = allkeys.get(k, 0)
                allkeys[k] = v + 1
            for k, v in allkeys.items():
                print('\t'.join([k, str(v)]))



    def flatten_old(self, filename, options):
        if filename[-5:] == '.bson':
            f = open(filename, 'rb')
        else:
            f = open(filename, 'r', encoding=get_option(options, 'encoding'))
        i = 0
        if filename[-5:] == 'jsonl':
            for r in f:
                allkeys = {}
                i += 1
                rec = orjson.loads(r)
                for k in get_keys(rec):
                    v = allkeys.get(k, 0)
                    allkeys[k] = v + 1
                for k, v in allkeys.items():
                    print('\t'.join([k, str(v)]))
        elif filename[-5:] == '.bson':
            for rec in bson.decode_file_iter(f):
                allkeys = {}
                for k in get_keys(rec):
                    v = allkeys.get(k, 0)
                    allkeys[k] = v + 1
                for k, v in allkeys.items():
                    print('\t'.join([k, str(v)]))
        elif filename[-5:] == '.json':
            allkeys = {}
            rec = orjson.loads(f.read())
            for k in get_keys(rec):
                v = allkeys.get(k, 0)
                allkeys[k] = v + 1
            for k, v in allkeys.items():
                print('\t'.join([k, str(v)]))
        elif filename[-4:] == '.csv':
            dr = csv.DictReader(f, get_option(options, 'delimiter'))
            keys = dr.fieldnames
            for r in dr:
                for key in keys:
                    print((u'%s\t%s' % (key, r[key])))

