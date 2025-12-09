# -*- coding: utf8 -*-
"""Utility functions for file operations and data processing."""
from collections import OrderedDict
import chardet
from .constants import SUPPORTED_FILE_TYPES
from .constants import DEFAULT_OPTIONS


def detect_encoding(filename, limit=1000000):
    """Detect encoding of a file."""
    with open(filename, 'rb') as f:
        chunk = f.read(limit)
    detected = chardet.detect(chunk)
    return detected


def detect_delimiter(filename, encoding='utf8'):
    """Detect delimiter used in a CSV-like file."""
    with open(filename, 'r', encoding=encoding) as f:
        line = f.readline()
    dict1 = {',': line.count(','), ';': line.count(';'),
             '\t': line.count('\t'), '|': line.count('|')}
    delimiter = max(dict1, key=dict1.get)
    return delimiter


def get_file_type(filename):
    """Get file type based on extension."""
    ext = filename.rsplit('.', 1)[-1].lower()
    if ext in SUPPORTED_FILE_TYPES:
        return ext
    return None


def get_option(options, name):
    """Returns value of the option."""
    if name in options:
        return options[name]
    if name in DEFAULT_OPTIONS:
        return DEFAULT_OPTIONS[name]
    return None

def get_dict_value(d, keys):
    """Get dictionary value by nested keys."""
    out = []
    if d is None:
        return out
    if len(keys) == 1:
        if isinstance(d, (dict, OrderedDict)):
            if keys[0] in d:
                out.append(d[keys[0]])
        else:
            for r in d:
                if r and keys[0] in r:
                    out.append(r[keys[0]])
    else:
        if isinstance(d, (dict, OrderedDict)):
            if keys[0] in d:
                out.extend(get_dict_value(d[keys[0]], keys[1:]))
        else:
            for r in d:
                if keys[0] in r:
                    out.extend(get_dict_value(r[keys[0]], keys[1:]))
    return out


def strip_dict_fields(record, fields, startkey=0):
    """Strip dictionary fields based on field list."""
    keys = list(record.keys())
    localf = []
    for field in fields:
        if len(field) > startkey:
            localf.append(field[startkey])
    for k in keys:
        if k not in localf:
            del record[k]

    for k in record:
        if isinstance(record[k], dict):
            record[k] = strip_dict_fields(record[k], fields, startkey + 1)
    return record


def dict_generator(indict, pre=None):
    """Processes python dictionary and return list of key values.

    :param indict: Input dictionary
    :param pre: Prefix keys
    :return: Generator of key-value pairs
    """
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in list(indict.items()):
            if key == "_id":
                continue
            if isinstance(value, dict):
                yield from dict_generator(value, pre + [key])
            elif isinstance(value, (list, tuple)):
                for v in value:
                    if isinstance(v, dict):
                        yield from dict_generator(v, pre + [key])
            else:
                yield pre + [key, value]
    else:
        yield indict


def guess_int_size(i):
    """Guess integer size type."""
    if i < 255:
        return 'uint8'
    if i < 65535:
        return 'uint16'
    return 'uint32'


def guess_datatype(s, qd):
    """Guesses type of data by string provided.

    :param s: String to analyze
    :param qd: Query date matcher
    :return: Dictionary with datatype information
    """
    attrs = {'base': 'str'}
    if s is None:
        return {'base': 'empty'}
    if isinstance(s, int):
        return {'base': 'int'}
    if isinstance(s, float):
        return {'base': 'float'}
    if not isinstance(s, str):
        return {'base': 'typed'}
    if s.isdigit():
        if s[0] == '0':
            attrs = {'base': 'numstr'}
        else:
            attrs = {'base': 'int', 'subtype': guess_int_size(int(s))}
    else:
        try:
            float(s)
            attrs = {'base': 'float'}
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
                    attrs = {'base': 'empty'}
    return attrs


def buf_count_newlines_gen(fname):
    """Count newlines in a file using buffered reading."""
    def _make_gen(reader):
        while True:
            b = reader(2 ** 16)
            if not b:
                break
            yield b

    with open(fname, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
    return count


def get_dict_keys(iterable, limit=1000):
    """Get all dictionary keys from an iterable of dictionaries."""
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
    """Measures if object is flat."""
    for v in item.values():
        if isinstance(v, (tuple, list)):
            return False
        if isinstance(v, dict):
            if not _is_flat(v):
                return False
    return True
