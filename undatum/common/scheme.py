import datetime
import bson
import orjson
import csv
import logging
from copy import copy
from .functions import  get_dict_value_deep

OTYPES_MAP = [[type(""), 'string'],
              [type(u""), 'string'],
              [datetime.datetime, 'datetime'],
              [int, 'integer'],
              [bool, 'boolean'],
              [float, 'float'],
              [str, 'string'],
              [bson.int64.Int64, 'integer'],
              [bson.objectid.ObjectId, 'string'],
              [type([]), 'array']
              ]


def merge_schemes(alist, novalue=True):
    """Merges schemes of list of objects and generates final data schema"""
    if len(alist) == 0:
        return None
    obj = alist[0]
    okeys = obj.keys()
    for item in alist[1:]:
        for k in item.keys():
            #            print(obj[k]['type'])
            if k not in okeys:
                obj[k] = item[k]
            elif obj[k]['type'] in ['integer', 'float', 'string', 'datetime']:
                if not novalue:
                    obj[k]['value'] += item[k]['value']
            elif obj[k]['type'] == 'dict':
                if not novalue:
                    obj[k]['value'] += item[k]['value']
                if 'schema' in item[k].keys():
                    obj[k]['schema'] = merge_schemes([obj[k]['schema'], item[k]['schema']])
            elif obj[k]['type'] == 'array':
                #                if 'subtype' not in obj[k].keys():
                #                   logging.info(str(obj[k]))
                if 'subtype' in obj[k].keys() and obj[k]['subtype'] == 'dict':
                    if not novalue:
                        obj[k]['value'] += item[k]['value']
                    if 'schema' in item[k].keys():
                        obj[k]['schema'] = merge_schemes([obj[k]['schema'], item[k]['schema']])
                else:
                    if not novalue:
                        obj[k]['value'] += item['value']
    return obj


def get_schemes(alist):
    """Generates schemas for each object"""
    results = []
    for o in alist:
        results.append(get_schema(o))
    return results

def get_schema(obj, novalue=True):
    """Generates schema from object"""
    result = {}
    for k in obj.keys():
        tt = type(obj[k])
        if obj[k] is None:
            result[k] = {'type': 'string', 'value' : 1}
        elif tt == type("") or tt == type(u"") or isinstance(obj[k], str):
            result[k] = {'type': 'string', 'value' : 1}
        elif isinstance(obj[k], str):
            result[k] = {'type': 'string', 'value': 1}
        elif tt == datetime.datetime:
            result[k] = {'type': 'datetime', 'value' : 1}
        elif tt == bool:
            result[k] = {'type': 'boolean', 'value' : 1}
        elif tt == float:
            result[k] = {'type': 'float', 'value' : 1}
        elif tt == int:
            result[k] = {'type': 'integer', 'value' : 1}
        elif tt == bson.int64.Int64:
            result[k] = {'type': 'integer', 'value' : 1}
        elif tt == bson.objectid.ObjectId:
            result[k] = {'type': 'string', 'value' : 1}
        elif tt == type({}):
            result[k] = {'type': 'dict', 'value' : 1, 'schema' : get_schema(obj[k])}
        elif tt == type([]):
            result[k] = {'type': 'array', 'value' : 1}
            if len(obj[k]) == 0:
                result[k]['subtype'] = 'string'
            else:
                found = False
                for otype, oname in OTYPES_MAP:
                    if type(obj[k][0]) == otype:
                        result[k]['subtype'] = oname
                        found = True
                if not found:
                    if type(obj[k][0]) == type({}):
                        result[k]['subtype'] = 'dict'
                        result[k]['schema'] =  merge_schemes(get_schemes(obj[k]))
                    else:
                        logging.info("Unknown object %s type %s" % (k, str(type(obj[k][0]))))
        else:
            logging.info("Unknown object %s type %s" % (k, str(type(obj[k]))))
            result[k] = {'type': 'string', 'value' : 1}
        if novalue:
            del result[k]['value']
    return result

def extract_keys(obj, parent=None, text=None, level=1):
    """Extracts keys"""
    keys = []
    text = ''
    if not parent:
        text = "'schema': {\n"
    for k in obj.keys():
        if type(obj[k]) == type({}):
            text += "\t" * level + "'%s' : {'type' : 'dict', 'schema' : {\n" % (k)
            text += extract_keys(obj[k], k, text, level+1)
            text += "\t" * level + "}},\n"
        elif type(obj[k]) == type([]):
            text += "\t" * level + "'%s' : {'type' : 'list', 'schema' : { 'type' : 'dict', 'schema' : {\n" % (k)
            if len(obj[k]) > 0:
                item = obj[k][0]
                if type(item) == type({}):
                    text += extract_keys(item, k, text, level+1)
                else:
                    text += "\t" * level + "'%s' : {'type' : 'string'},\n" % (k)
            text += "\t" * level + "}}},\n"
        else:
            logging.info(str(type(obj[k])))
            text += "\t" * level + "'%s' : {'type' : 'string'},\n" % (k)
    if not parent:
        text += "}"
    return text

def __get_filetype_by_ext(filename):
    ext = filename.rsplit('.', 1)[-1].lower()
    if ext in ['bson', 'json', 'csv', 'jsonl']:
        return ext
    return filename


def generate_scheme_from_file(filename=None, fileobj=None, filetype='bson', alimit=1000, verbose=0, encoding='utf8', delimiter=",", quotechar='"'):
    """Generates schema of the data BSON file"""
    if not filetype and filename is not None:
        filetype = __get_filetype_by_ext(filename)
    datacache = []
    if filetype == 'bson':
        if filename:
            source = open(filename, 'rb')
        else:
            source = fileobj
        n = 0
        for r in bson.decode_file_iter(source):
            n += 1
            if n > alimit:
                break
            datacache.append(r)
        if filename:
            source.close()
    elif filetype == 'jsonl':
        if filename:
            source = open(filename, 'r', encoding=encoding)
        else:
            source = fileobj
        n = 0
        for r in source:
            n += 1
            if n > alimit:
                break
            datacache.append(orjson.loads(r))
        if filename:
            source.close()
    elif filetype == 'csv':
        if filename:
            source = open(filename, 'r', encoding=encoding)
        else:
            source = fileobj
        n = 0
        reader = csv.DictReader(source, quotechar=quotechar, delimiter=delimiter, quoting=csv.QUOTE_ALL)
        for r in reader:
            n += 1
            if n > alimit:
                break
            datacache.append(r)
        if filename:
            source.close()
    n = 0
    scheme = None
    for r in datacache:
        n += 1
        if scheme is None:
            scheme = get_schema(r)
        else:
            scheme = merge_schemes([scheme, get_schema(r)])
    return scheme

def schema2fieldslist(schema, prefix=None, predefined=None, sample=None):
    """Converts data schema to the fields list"""
    fieldslist = []
    for k in schema.keys():
        if prefix is None:
            name = k
        else:
            name = '.'.join(['.'.join(prefix.split('.')), k])
        try:
            sampledata = get_dict_value_deep(sample, name) if sample else ''
        except:
            sampledata = ''
        if 'schema' not in schema[k].keys():
            if schema[k]['type'] != 'array':
                field = {'name' : name, 'type': schema[k]['type'], 'description' : '', 'sample' : sampledata, 'class' : ""}
            else:
                field = {'name': name, 'type': 'list of [%s]' % schema[k]['type'], 'description' : '', 'sample' : sampledata, 'class' : ""}
            if predefined:
                if name in predefined.keys():
                    field['description'] = predefined[name]['text']
                    if predefined[name]['class']:
                        field['class'] = predefined[name]['class']
                elif k in predefined.keys():
                    field['description'] = predefined[k]['text']
                    if predefined[k]['class']:
                        field['class'] = predefined[k]['class']
            if field['type'] == 'datetime':
                field['class'] = 'datetime'
            fieldslist.append(field)
        else:
            if prefix is not None:
                subprefix = copy(prefix) + '.' + k
#                subprefix.append(k)
            else:
                subprefix = k
            if schema[k]['type'] == 'dict':
                field =  {'name' : name, 'type': schema[k]['type'], 'description' : '', 'sample' : '', 'class' : ''}
                if predefined:
                    if name in predefined.keys():
                        field['description'] = predefined[name]['text']
                        if predefined[name]['class']:
                            field['class'] = predefined[name]['class']
                    elif k in predefined.keys():
                        field['description'] = predefined[k]['text']
                        if predefined[k]['class']:
                            field['class'] = predefined[k]['class']
                fieldslist.append(field)
                fieldslist.extend(schema2fieldslist(schema[k]['schema'], prefix=subprefix, predefined=predefined, sample=sample))
            elif schema[k]['type'] == 'array':
                field = {'name': name, 'type': 'list of [%s]' % schema[k]['type'], 'description' : '', 'sample' : '', 'class' : ''}
                if predefined:
                    if name in predefined.keys():
                        field['description'] = predefined[name]['text']
                        if predefined[name]['class']:
                            field['class'] = predefined[name]['class']
                    elif k in predefined.keys():
                        field['description'] = predefined[k]['text']
                        if predefined[k]['class']:
                            field['class'] = predefined[k]['class']
                fieldslist.append(field)
                fieldslist.extend(schema2fieldslist(schema[k]['schema'], prefix=subprefix, sample=sample))
    return fieldslist
