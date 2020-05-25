from xlrd import open_workbook as load_xls
from openpyxl import load_workbook as load_xlsx
import logging
import csv
import json
import bson
#from xmlr import xmliter
import xml.etree.ElementTree as etree
from collections import defaultdict
PREFIX_STRIP = True
PREFIX = ""
from ..utils import get_file_type

def __copy_options(user_options, default_options):
    """If user provided option so we use it, if not, default option value should be used"""
    for k in default_options.keys():
        if k not in user_options.keys():
            user_options[k] = default_options[k]
    return user_options

def etree_to_dict(t, prefix_strip=True):
    tag = t.tag if not prefix_strip else t.tag.rsplit('}', 1)[-1]
    d = {tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
#            print(dir(dc))
            for k, v in dc.items():
                if prefix_strip:
                    k = k.rsplit('}', 1)[-1]
                dd[k].append(v)
        d = {tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[tag].update(('@' + k.rsplit('}', 1)[-1], v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            tag = tag.rsplit('}', 1)[-1]
            if text:
              d[tag]['#text'] = text
        else:
            d[tag] = text
    return d


def xml_to_jsonl(fromname, toname, options={}, default_options={ 'prefix_strip' : True}):
    options = __copy_options(options, default_options)
    ins = open(fromname, 'rb')#, encoding='utf-8')
    outf = open(toname, 'w')
    n = 0
    for event, elem in etree.iterparse(ins):
        shorttag = elem.tag.rsplit('}', 1)[-1]
        if shorttag == options['tagname']:
            n += 1
            if options['prefix_strip']:
                j = etree_to_dict(elem, prefix_strip=options['prefix_strip'])
            else:
                j = etree_to_dict(elem)
            outf.write(json.dumps(j))
            outf.write('\n')
        if n % 10000 == 0:
            logging.info('xml2jsonl: processed %d xml tags' % (n))
    ins.close()
    outf.close()

def xls_to_csv(fromname, toname, options={}, default_options={'start_line':0, 'skip_end_rows':0, 'delimiter' : ',', 'encoding' : 'utf8'}):
    options = __copy_options(options, default_options)
    b = load_xls(fromname)
    s = b.sheet_by_index(0)
    bc = open(toname ,'w', encoding=options['encoding'])
    bcw = csv.writer(bc, delimiter=options['delimiter'])
    n = 0
    for row in range(options['start_line'], s.nrows - options['skip_end_rows']):
        n += 1
        this_row = []
        for col in range(s.ncols):
            v = str(s.cell_value(row ,col))
            v = v.replace('\n', ' ').strip()
            #			v = v.encode('utf8') if type(v) == type(u'') else str(v)
            this_row.append(v)
        bcw.writerow(this_row)
        if n % 10000 == 0:
            logging.info('xls2csv: processed %d records' % (n))
    bc.close()

def csv_to_bson(fromname, toname, options={}, default_options={'encoding': 'utf8', 'delimiter' : ','}):
    options = __copy_options(options, default_options)
    source = open(fromname, 'r', encoding=options['encoding'])
    output = open(toname, 'wb')
    reader = csv.DictReader(source, delimiter=options['delimiter'])
    n = 0
    for j in reader:
        n += 1
        rec = bson.BSON.encode(j)
        output.write(rec)
        if n % 10000 == 0:
            logging.info('csv2bson: processed %d records' % (n))
    source.close
    output.close()

def csv_to_jsonl(fromname, toname, options={}, default_options={'encoding': 'utf8', 'delimiter' : ','}):
    options = __copy_options(options, default_options)
    source = open(fromname, 'r', encoding=options['encoding'])
    output = open(toname, 'wb')
    reader = csv.DictReader(source, delimiter=options['delimiter'])
    n = 0
    for j in reader:
        n += 1
        output.write(json.dumps(j).encode('utf8'))
        output.write(u'\n'.encode('utf8'))
        if n % 10000 == 0:
            logging.info('csv2jsonl: processed %d records' % (n))
    source.close
    output.close()

def xls_to_jsonl(fromname, toname, options={}, default_options={'start_page': 0, 'start_line' : 0, 'fields' : None}):
    options = __copy_options(options, default_options)
    source = load_xls(fromname)
    output = open(toname, 'w')
    sheet = source.sheet_by_index(options['start_page'])
    n = 0
    fields = options['fields'].split(',') if options['fields'] is not None else None
    for rownum in range(options['start_line'], sheet.nrows):
        n += 1
        tmp = list()
        for i in range(0, sheet.ncols):
            tmp.append(sheet.row_values(rownum)[i])
        if n == 1 and fields is None:
            fields = tmp
            continue
        l = json.dumps(dict(zip(fields, tmp)))
        output.write(l + '\n')
#        output.write(u'\n'.encode('utf8'))
        if n % 10000 == 0:
            logging.info('xls2jsonl: processed %d records' % (n))
    output.close()

def xlsx_to_jsonl(fromname, toname, options={}, default_options={'start_page': 0, 'start_line' : 0}):
    options = __copy_options(options, default_options)
    source = load_xlsx(fromname)
    output = open(toname, 'w')
    sheet = source.active
    n = 0
    for rownum in range(options['start_line'], sheet.nrows):
        n += 1
        tmp = list()
        for i in range(0, sheet.ncols):
            tmp.append(sheet.row_values(rownum)[i])
        l = json.dumps(dict(zip(options['fields'], tmp)))
        output.write(l.encode('utf8'))
        output.write(u'\n'.encode('utf8'))
        if n % 10000 == 0:
            logging.info('xls2jsonl: processed %d records' % (n))
    source.close
    output.close()

def xls_to_bson(fromname, toname, options={}, default_options={'start_page': 0, 'start_line' : 0}):
    options = __copy_options(options, default_options)
    source = load_xls(fromname)
    output = open(toname, 'wb')
    sheet = source.sheet_by_index(options['start_page'])
    n = 0
    for rownum in range(options['start_line'], sheet.nrows):
        n += 1
        tmp = list()
        for i in range(0, sheet.ncols):
            tmp.append(sheet.row_values(rownum)[i])
        output.write(bson.BSON.encode(dict(zip(options['fields'], tmp))))
        if n % 10000 == 0:
            logging.info('xls2bson: processed %d records' % (n))
    source.close
    output.close()


CONVERT_FUNC_MAP = {
    'xls2csv' : xls_to_csv,
    'xls2jsonl' : xls_to_jsonl,
    'xls2bson': xls_to_bson,
    'xlsx2jsonl': xlsx_to_jsonl,
    'csv2jsonl' : csv_to_jsonl,
    'csv2bson' : csv_to_bson,
    'xml2jsonl' : xml_to_jsonl
 }
class Converter:
    def __init__(self):
        pass

    def convert(self, fromfile, tofile,  options={}):
        fromtype = options['format_in'] if options['format_in'] is not None else get_file_type(fromfile)
        totype = options['format_out'] if options['format_out'] is not None else get_file_type(tofile)
        key = '%s2%s' % (fromtype, totype)
        func = CONVERT_FUNC_MAP.get(key, None)
        if func == None:
            logging.error('Conversion between %s and %s not supported' % (fromtype, totype))
        else:
            logging.info('Convert %s from %s to %s' % (key, fromfile, tofile))
            func(fromfile, tofile, options)

