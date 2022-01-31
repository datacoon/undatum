# -*- coding: utf8 -*-
# FIXME: A lot of unoptimized code here, it could be better, shorter and some functions could be improved
import os
from ..utils import get_file_type, get_option, dict_generator, guess_int_size, guess_datatype, detect_delimiter, detect_encoding, get_dict_value, get_dict_keys, _is_flat, buf_count_newlines_gen
from ..constants import SUPPORTED_FILE_TYPES
from collections import OrderedDict
import bson
import json
import jsonlines
import orjson
import csv
import zipfile
import xmltodict
OBJECTS_ANALYZE_LIMIT = 100



def analyze_csv(filename, objects_limit=OBJECTS_ANALYZE_LIMIT):
    """Analyzes CSV file"""
    report = []
    encoding_det = detect_encoding(filename, limit=100000)
    report.append(['Filename', filename])
    report.append(['File type', 'csv'])
    if encoding_det:
        encoding = encoding_det['encoding']
        report.append(['Encoding', encoding])
    else:
        encoding = 'utf8'
        report.append(['Encoding', 'Not detected'])
    delimiter = detect_delimiter(filename, encoding=encoding)
    report.append(['Delimiter', delimiter])
    report.append(['Filesize', str(os.path.getsize(filename))])
    report.append(['Number of lines', buf_count_newlines_gen(filename)])
    return report

def analyze_jsonl(filename, objects_limit=OBJECTS_ANALYZE_LIMIT):
    """Analyzes JSON lines file"""
    report = []
    encoding_det = detect_encoding(filename, limit=100000)
    report.append(['Filename', filename])
    report.append(['File type', 'jsonl'])
    if encoding_det:
        encoding = encoding_det['encoding']
        report.append(['Encoding', encoding])
    else:
        encoding = 'utf8'
        report.append(['Encoding', 'Not detected'])
    report.append(['Filesize', str(os.path.getsize(filename))])
    report.append(['Number of lines', buf_count_newlines_gen(filename)])
    f = open(filename, 'r', encoding=encoding)
    flat = True
    reader = jsonlines.Reader(f)
    objects = []
    n = 0
    for o in reader.iter():
        n += 1
        objects.append(o)
        if n > objects_limit:
            break
    for o in objects[:objects_limit]:
        if not _is_flat(o):
            flat = False
    report.append(['Is flat table?', str(flat)])
    report.append(['Fields', str('\n'.join(get_dict_keys(objects)))])
    return report


def analyze_bson(filename, objects_limit=OBJECTS_ANALYZE_LIMIT):
    """Analyzes BSON file"""
    report = []
    report.append(['Filename', filename])
    report.append(['File type', 'bson'])
    report.append(['Filesize', str(os.path.getsize(filename))])
    f = open(filename, 'rb')
    flat = True
    objects = []
    n = 0
    for o in bson.decode_file_iter(f):
        n += 1
        objects.append(o)
        if n > objects_limit:
            break
    f.close()
    for o in objects[:objects_limit]:
        if not _is_flat(o):
            flat = False
    report.append(['Is flat table?', str(flat)])
    report.append(['Fields', str('\n'.join(get_dict_keys(objects)))])
    return report


def analyze_json(filename, objects_limit=OBJECTS_ANALYZE_LIMIT, filesize_limit=500000000):
    """Analyzes JSON file"""
    report = []
    encoding_det = detect_encoding(filename, limit=100000)
    report.append(['Filename', filename])
    report.append(['File type', 'json'])
    if encoding_det:
        encoding = encoding_det['encoding']
        report.append(['Encoding', encoding])
    else:
        encoding = 'utf8'
        report.append(['Encoding', 'Not detected'])
    filesize = os.path.getsize(filename)
    report.append(['Filesize', str(filesize)])
    if filesize > filesize_limit:
        report.append(['Filesize overlimit', 'File size greater than %d. Not processed' % (filesize_limit)])
        return
    f = open(filename, 'r', encoding=encoding)
    data = json.load(f)
    f.close()
    objects = None
    if isinstance(data, list):
        objects = data
        n_count = len(objects)
        report.append(['JSON type', 'objects list'])
        report.append(['Objects count', str(n_count)])
    elif isinstance(data, dict):
        if len(data.keys()) == 1 and isinstance(data[list(data.keys())[0]], list):
            objects = data.values()[0]
            n_count = len(objects)
            report.append(['JSON type', 'objects list with key'])
            report.append(['JSON objects key', data.keys()[0]])
            report.append(['Objects count', str(n_count)])
            if objects:
                flat = True
                for o in objects[:objects_limit]:
                    if not _is_flat(o):
                        flat = False
                report.append(['Is flat table?', str(flat)])
                report.append(['Fields', str('\n'.join(get_dict_keys(objects)))])
        elif len(data.keys()) > 0:
            candidates = _seek_dict_lists(data, level=0)
            if len(candidates) > 0:
                for fullkey in list(candidates.keys()):
                    report.append(['Multiple tables exists', str(False)])
                    report.append(['Full data key', fullkey])
                    report.append(['Short data key', fullkey.rsplit('.', 1)[-1]])
                    report.append(['Objects count', str(list(candidates.values())[0]['num'])])
                    objects = get_dict_value(data, keys=fullkey.split('.'))[0]
                    flat = True
                    for o in objects[:objects_limit]:
                        if not _is_flat(o):
                            flat = False
                    report.append(['Is flat table?', str(flat)])
                    report.append(['Fields', str('\n'.join(get_dict_keys(objects)))])
            else:
                report.append(['JSON type', 'single object'])
                objects = [data,]
                flat = True
                for o in objects[:objects_limit]:
                    if not _is_flat(o):
                        flat = False
                report.append(['Is flat table?', str(flat)])
                report.append(['Fields', str('\n'.join(get_dict_keys(objects)))])
    return report


def _seek_dict_lists(data, level=0, path=None, candidates=OrderedDict()):
    for key, value in data.items():
        if isinstance(value, list):
            isobjectlist = False
            for listitem in value[:20]:
                if isinstance(listitem, dict) or isinstance(listitem, OrderedDict):
                    isobjectlist = True
                    break
            if not isobjectlist: continue
            key = path + '.%s' % (key) if path is not None else key
            if key not in candidates.keys():
                candidates[key] = {'key' : key, 'num' : len(value)}
        elif isinstance(value, OrderedDict) or isinstance(value, dict):
            res = _seek_xml_lists(value, level + 1, path + '.' + key if path else key, candidates)
            for k, v in res.items():
                if k not in candidates.keys():
                    candidates[k] = v
        else:
            continue
    return candidates



def _seek_xml_lists(data, level=0, path=None, candidates=OrderedDict()):
    for key, value in data.items():
        if isinstance(value, list):
            key = path + '.%s' % (key) if path is not None else key
            if key not in candidates.keys():
                candidates[key] = {'key' : key, 'num' : len(value)}
        elif isinstance(value, OrderedDict) or isinstance(value, dict):
            res = _seek_xml_lists(value, level + 1, path + '.' + key if path else key, candidates)
            for k, v in res.items():
                if k not in candidates.keys():
                    candidates[k] = v
        else:
            continue
    return candidates


def analyze_xml(filename, objects_limit=OBJECTS_ANALYZE_LIMIT, filesize_limit=100000000):
    """Analyzes XML file"""
    report = []
    encoding_det = detect_encoding(filename, limit=100000)
    report.append(['Filename', filename])
    report.append(['File type', 'xml'])
    if encoding_det:
        encoding = encoding_det['encoding']
        report.append(['Encoding', encoding])
    else:
        encoding = 'utf8'
        report.append(['Encoding', 'Not detected'])
    filesize = os.path.getsize(filename)
    report.append(['Filesize', str(filesize)])
#    report.append(['Number of lines', buf_count_newlines_gen(filename)])
    if filesize > filesize_limit:
        report.append(['Filesize overlimit', 'File size greater than %d. Not processed' % (filesize_limit)])
        return
    f = open(filename, 'rb')#, encoding=encoding)
    data = xmltodict.parse(f, process_namespaces=False)
    from pprint import pprint
#    pprint(data)
    candidates = _seek_xml_lists(data, level=0)
    if len(candidates.keys()) == 1:
        fullkey = str(list(candidates.keys())[0])
        report.append(['Miltiple tables exists', str(False)])
        report.append(['Full data key', fullkey])
        report.append(['Short data key', fullkey.rsplit('.', 1)[-1]])
        report.append(['Objects count', str(list(candidates.values())[0]['num'])])
        objects = get_dict_value(data, keys=fullkey.split('.'))[0]
        flat = True
        for o in objects[:objects_limit]:
            if not _is_flat(o):
                flat = False
        report.append(['Is flat table?', str(flat)])
        report.append(['Fields', str('\n'.join(get_dict_keys(objects)))])
    elif len(candidates) > 1:
        report.append(['Miltiple tables exists', str(True)])
        for fullkey in list(candidates.keys()):
            report.append(['Full data key', fullkey])
            report.append(['Short data key', fullkey.rsplit('.', 1)[-1]])
            report.append(['Objects count', str(candidates[fullkey]['num'])])
            objects = get_dict_value(data, keys=fullkey.split('.'))[0]
            flat = True
            for o in objects[:objects_limit]:
                if not _is_flat(o):
                    flat = False
            report.append(['Is flat table?', str(flat)])
            report.append(['Fields', str('\n'.join(get_dict_keys(objects)))])
    f.close()

    return report

class Analyzer:
    def __init__(self):
        pass

    def analyze(self, filename, options):
        """Analyzes given data file and returns it's parameters"""
        import prettytable as pt

        filetype = get_file_type(filename)
        if not filetype:
            print('Sorry, file type not supported')
            print('Supported file types are: %s' % (','.join(SUPPORTED_FILE_TYPES)))
            return
        table = None
        if filetype == 'csv':
            table = analyze_csv(filename)
        elif filetype == 'jsonl':
            table = analyze_jsonl(filename)
        elif filetype == 'bson':
            table = analyze_bson(filename)
        elif filetype == 'json':
            table = analyze_json(filename)
        elif filetype == 'xml':
            table = analyze_xml(filename)
        else:
            print('File type %s analyzer not ready yet' %(filetype))
        if table:
            print('This report intended to be human readable. For machine readable tasks please use other commands.')
            headers = ['Parameter', 'Value']
            outtable = pt.PrettyTable(headers)
            for row in table:
                outtable.add_row(row)
            print(f'{outtable}')
        pass
