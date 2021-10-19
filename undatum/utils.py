import orjson
import csv
from .constants import SUPPORTED_FILE_TYPES
from .constants import DEFAULT_OPTIONS

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
        if type(d) == type({}):
            if keys[0] in d.keys():
                out.append(d[keys[0]])
        else:
            for r in d:
                if r and keys[0] in r.keys():
                    out.append(r[keys[0]])
#        return out
    else:
        if type(d) == type({}):
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
