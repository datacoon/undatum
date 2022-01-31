from collections import OrderedDict
import csv
import chardet
import orjson
from .constants import SUPPORTED_FILE_TYPES
from .constants import DEFAULT_OPTIONS

def detect_encoding(filename, limit=1000000):
    f = open(filename, 'rb')
    chunk = f.read(limit)
    f.close()
    detected = chardet.detect(chunk)
    return detected

def detect_delimiter(filename, encoding='utf8'):
    f = open(filename, 'r', encoding=encoding)
    line = f.readline()
    f.close()
    dict1 = {',': line.count(','), ';': line.count(';'), '\t': line.count('\t'), '|' : line.count('|')}
    delimiter = max(dict1, key=dict1.get)
    return delimiter

def get_file_type(filename):
    ext = filename.rsplit('.', 1)[-1].lower()
    if ext in SUPPORTED_FILE_TYPES:
        return ext
    return None

def get_option(options, name):
    """Returns value of the option"""
    if name in options.keys():
        return options[name]
    elif name in DEFAULT_OPTIONS.keys():
        return DEFAULT_OPTIONS[name]
    return None

def write_items(fields, outdata, filetype, handle, delimiter=','):
    if len(outdata) == 0:
        return
    if filetype == 'csv':
        dw = csv.DictWriter(handle, delimiter=delimiter, fieldnames=fields)
        dw.writeheader()
        if type(outdata[0]) == type(''):
            for rawitem in outdata:
                item = {fields[0] : rawitem}
                dw.writerow(item)
        elif type(outdata[0]) == type([]):
            for rawitem in outdata:
                item = dict(zip(fields, rawitem))
                dw.writerow(item)
        else:
            dw.writerows(outdata)
    elif filetype == 'jsonl':
        # If our data is just array of strings, we just transform it to dict
        if type(outdata[0]) == type(''):
            for rawitem in outdata:
                item = {fields[0] : rawitem}
                handle.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))
        elif type(outdata[0]) == type([]):
            for rawitem in outdata:
                item = dict(zip(fields, rawitem))
                handle.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))
        else:
            for item in outdata:
                handle.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))


def get_dict_value(d, keys):
    out = []
    if d is None:
        return out
#    keys = key.split('.')
    if len(keys) == 1:
        if type(d) == type({}) or isinstance(d, OrderedDict):
            if keys[0] in d.keys():
                out.append(d[keys[0]])
        else:
            for r in d:
                if r and keys[0] in r.keys():
                    out.append(r[keys[0]])
#        return out
    else:
        if type(d) == type({}) or isinstance(d, OrderedDict):
            if keys[0] in d.keys():
                out.extend(get_dict_value(d[keys[0]], keys[1:]))
        else:
            for r in d:
                if keys[0] in r.keys():
                    out.extend(get_dict_value(r[keys[0]], keys[1:]))
    return out


def strip_dict_fields(record, fields, startkey=0):
    keys = record.keys()
    localf = []
    for field in fields:
        if len(field) > startkey:
            localf.append(field[startkey])
    for k in list(keys):
        if k not in localf:
            del record[k]

    if len(k) > 0:
        for k in record.keys():
            if type(record[k]) == type({}):
                record[k] = strip_dict_fields(record[k], fields, startkey + 1)
    return record


def dict_generator(indict, pre=None):
    """Processes python dictionary and return list of key values
    :param indict
    :param pre
    :return generator"""
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in list(indict.items()):
            if key == "_id":
                continue
            if isinstance(value, dict):
                #                print 'dgen', value, key, pre
                for d in dict_generator(value, pre + [key]):
                    yield d
            elif isinstance(value, list) or isinstance(value, tuple):
                for v in value:
                    if isinstance(v, dict):
                        #                print 'dgen', value, key, pre
                        for d in dict_generator(v, pre + [key]):
                            yield d
#                    for d in dict_generator(v, [key] + pre):
#                        yield d
            else:
                yield pre + [key, value]
    else:
        yield indict


def guess_int_size(i):
    if i < 255:
        return 'uint8'
    if i < 65535:
        return 'uint16'
    return 'uint32'

def guess_datatype(s, qd):
    """Guesses type of data by string provided
    :param s
    :param qd
    :return datatype"""
    attrs = {'base' : 'str'}
#    s = unicode(s)
    if s is None:
       return {'base' : 'empty'}
    if type(s) == type(1):
        return {'base' : 'int'}
    if type(s) == type(1.0):
        return {'base' : 'float'}
    elif type(s) != type(''):
#        print((type(s)))
        return {'base' : 'typed'}
#    s = s.decode('utf8', 'ignore')
    if s.isdigit():
        if s[0] == 0:
            attrs = {'base' : 'numstr'}
        else:
            attrs = {'base' : 'int', 'subtype' : guess_int_size(int(s))}
    else:
        try:
            i = float(s)
            attrs = {'base' : 'float'}
            return attrs
        except ValueError:
            pass
        if qd:
            is_date = False
            res = qd.match(s)
            if res:
                attrs = {'base': 'date', 'pat': res['pattern']}
                is_date = True
            if not is_date:
                if len(s.strip()) == 0:
                    attrs = {'base' : 'empty'}
    return attrs


def buf_count_newlines_gen(fname):
    def _make_gen(reader):
        while True:
            b = reader(2 ** 16)
            if not b: break
            yield b

    with open(fname, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
    return count


def get_dict_keys(iterable, limit=1000):
    n = 0
    keys = []
    for item in iterable:
        if limit and n > limit:
            break
        n += 1
        dk = dict_generator(item)
        for i in dk:
            k = ".".join(i[:-1])
            if k not in keys:
                keys.append(k)
    return keys


def _is_flat(item):
    """Measures if object is flat"""
    for k, v in item.items():
        if isinstance(v, tuple) or isinstance(v, list):
            return False
        elif isinstance(v, dict):
            if not _is_flat(v): return False
    return True
