# -*- coding: utf8 -*-
# FIXME: A lot of unoptimized code here, it could be better, shorter and some functions could be improved
import os
from ..utils import get_file_type, detect_delimiter, detect_encoding, get_dict_value, get_dict_keys, _is_flat, buf_count_newlines_gen
from ..constants import SUPPORTED_FILE_TYPES
from collections import OrderedDict
from bson import decode_file_iter
import json
import jsonlines
import xmltodict
OBJECTS_ANALYZE_LIMIT = 100



def analyze_csv(filename, objects_limit=OBJECTS_ANALYZE_LIMIT, encoding=None):
    """Analyzes CSV file"""
    report = []
    report.append(['Filename', filename])
    report.append(['File type', 'csv'])
    if not encoding:
        encoding_det = detect_encoding(filename, limit=100000)
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

def analyze_jsonl(filename, objects_limit=OBJECTS_ANALYZE_LIMIT, encoding=None):
    """Analyzes JSON lines file"""
    report = []
    if not encoding:
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
    with open(filename, 'r', encoding=encoding) as fileobj:
        flat = True
        reader = jsonlines.Reader(fileobj)
        objects = []
        n_count = 0
        for obj in reader.iter():
            n_count += 1
            objects.append(obj)
            if n_count > objects_limit:
                break
        for obj in objects[:objects_limit]:
            if not _is_flat(obj):
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
    with open(filename, 'rb') as fileobj:
        flat = True
        objects = []
        n_count = 0
        for obj in decode_file_iter(fileobj):
            n_count += 1
            objects.append(obj)
            if n_count > objects_limit:
                break
    for obj in objects[:objects_limit]:
        if not _is_flat(obj):
            flat = False
    report.append(['Is flat table?', str(flat)])
    report.append(['Fields', str('\n'.join(get_dict_keys(objects)))])
    return report


def analyze_json(filename, objects_limit=OBJECTS_ANALYZE_LIMIT, filesize_limit=500000000, encoding=None):
    """Analyzes JSON file"""
    report = []
    if not encoding:
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


def analyze_xml(filename, objects_limit=OBJECTS_ANALYZE_LIMIT, filesize_limit=100000000, encoding=None):
    """Analyzes XML file"""
    report = []
    if not encoding:
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
        from rich import print
        from rich.table import Table

        filetype = get_file_type(filename)
        if not filetype:
            print('Sorry, file type not supported')
            print('Supported file types are: %s' % (','.join(SUPPORTED_FILE_TYPES)))
            return
        table = None
        encoding = options['encoding'] if 'encoding' in options.keys() else None
        if filetype == 'csv':
            table = analyze_csv(filename, encoding=encoding)
        elif filetype == 'jsonl':
            table = analyze_jsonl(filename, encoding=encoding)
        elif filetype == 'bson':
            table = analyze_bson(filename)
        elif filetype == 'json':
            table = analyze_json(filename, encoding=encoding)
        elif filetype == 'xml':
            table = analyze_xml(filename, encoding=encoding)
        else:
            print('File type %s analyzer not ready yet' %(filetype))
        if table:
            print('This report intended to be human readable. For machine readable tasks please use other commands.')
            reptable = Table(title="Analysis report")
            reptable.add_column("Parameter", justify="right", style="cyan", no_wrap=True)
            reptable.add_column("Value", justify="right", style="magenta")
            for row in table:
                reptable.add_row(*map(str, row))
            print(reptable)
        pass
