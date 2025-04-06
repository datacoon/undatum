# -*- coding: utf8 -*-
import csv
import json
import orjson
import logging
import pandas
# from xmlr import xmliter
import xml.etree.ElementTree as etree
from collections import defaultdict

import bson
from bson import ObjectId
from xlrd import open_workbook as load_xls
from ..utils import get_file_type, get_option, dict_generator
from iterable.helpers.detect import open_iterable
from tqdm import tqdm

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']

DEFAULT_BATCH_SIZE = 50000

def get_iterable_options(options):
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out            



PREFIX_STRIP = True
PREFIX = ""

LINEEND = '\n'.encode('utf8')

def df_to_pyorc_schema(df):
    """Extracts column information from pandas dataframe and generate pyorc schema"""
    struct_schema = []
    for k, v in df.dtypes.to_dict().items():
        v = str(v)
        if v == 'float64':
            struct_schema.append('%s:float' % (k))
        elif v == 'float32':
            struct_schema.append('%s:float' % (k))
        elif v == 'datetime64[ns]':
            struct_schema.append('%s:timestamp' % (k))
        elif v == 'int32':
            struct_schema.append('%s:int' % (k))
        elif v == 'int64':
            struct_schema.append('%s:int' % (k))
        else:
            struct_schema.append('%s:string' %(k))
    return struct_schema


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
        d = {tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
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


def xml_to_jsonl(fromname, toname, options={}, default_options={'prefix_strip': True}):
    options = __copy_options(options, default_options)
    ins = open(fromname, 'rb')  # , encoding='utf-8')
    outf = open(toname, 'wb')
    n = 0
    for event, elem in etree.iterparse(ins):
        shorttag = elem.tag.rsplit('}', 1)[-1]
        if shorttag == options['tagname']:
            n += 1
            if options['prefix_strip']:
                j = etree_to_dict(elem, prefix_strip=options['prefix_strip'])
            else:
                j = etree_to_dict(elem)
            outf.write(orjson.dumps(j[shorttag]))
            outf.write(LINEEND)
        if n % 500 == 0:
            logging.info('xml2jsonl: processed %d xml tags' % (n))
    logging.info('xml2jsonl: processed %d xml tags finally' % (n))
    ins.close()
    outf.close()


def xls_to_csv(fromname, toname, options={},
               default_options={'start_line': 0, 'skip_end_rows': 0, 'delimiter': ',', 'encoding': 'utf8'}):
    options = __copy_options(options, default_options)
    b = load_xls(fromname)
    s = b.sheet_by_index(0)
    bc = open(toname, 'w', encoding=options['encoding'])
    bcw = csv.writer(bc, delimiter=options['delimiter'])
    n = 0
    for row in range(options['start_line'], s.nrows - options['skip_end_rows']):
        n += 1
        this_row = []
        for col in range(s.ncols):
            v = str(s.cell_value(row, col))
            v = v.replace('\n', ' ').strip()
            #			v = v.encode('utf8') if type(v) == type(u'') else str(v)
            this_row.append(v)
        bcw.writerow(this_row)
        if n % 10000 == 0:
            logging.info('xls2csv: processed %d records' % (n))
    bc.close()


def csv_to_bson(fromname, toname, options={}, default_options={'encoding': 'utf8', 'delimiter': ','}):
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
    source.close()
    output.close()


def csv_to_jsonl(fromname, toname, options={}, default_options={'encoding': 'utf8', 'delimiter': ','}):
    options = __copy_options(options, default_options)
    source = open(fromname, 'r', encoding=options['encoding'])
    output = open(toname, 'wb')
    reader = csv.DictReader(source, delimiter=options['delimiter'])
    n = 0
    for j in reader:
        n += 1
        output.write(json.dumps(j, ensure_ascii=False).encode('utf8'))
        #        output.write(orjson.dumps(j, ensure_ascii=False).encode('utf8', ))
        output.write(u'\n'.encode('utf8'))
        if n % 10000 == 0:
            logging.info('csv2jsonl: processed %d records' % (n))
    source.close()
    output.close()


def xls_to_jsonl(fromname, toname, options={}, default_options={'start_page': 0, 'start_line': 0, 'fields': None}):
    options = __copy_options(options, default_options)
    source = load_xls(fromname)
    output = open(toname, 'wb')
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
        line = orjson.dumps(dict(zip(fields, tmp)))
        output.write(line + LINEEND)
        #        output.write(u'\n'.encode('utf8'))
        if n % 10000 == 0:
            logging.info('xls2jsonl: processed %d records' % (n))
    output.close()




def xlsx_to_jsonl(fromname, toname, options={}, default_options={'start_page': 0, 'start_line': 0}):
    from openpyxl import load_workbook as load_xlsx
    options = __copy_options(options, default_options)
    source = load_xlsx(fromname)
    output = open(toname, 'wb')
    sheet = source.active       #FIXME! Use start_page instead
    n = 0
    fields = options['fields'].split(',') if options['fields'] is not None else None
    for row in sheet.iter_rows():
        n += 1
        if n < options['start_line']: 
            continue
        tmp = list()

        for cell in row:
            tmp.append(cell.value)
        if n == 1 and fields is None:
            fields = tmp
            continue
        line = orjson.dumps(dict(zip(fields, tmp)))
        output.write(line)
        output.write(LINEEND)
        if n % 10000 == 0:
            logging.debug('xlsx2jsonl: processed %d records' % (n))
    source.close
    output.close()

def xlsx_to_bson(fromname, toname, options={}, default_options={'start_page': 0, 'start_line': 0}):
    from openpyxl import load_workbook as load_xlsx
    options = __copy_options(options, default_options)
    source = load_xlsx(fromname)
    output = open(toname, 'wb')
    sheet = source.active       #FIXME! Use start_page instead
    n = 0
    fields = options['fields'].split(',') if options['fields'] is not None else None
    for row in sheet.iter_rows():
        n += 1
        if n < options['start_line']: 
            continue
        tmp = list()

        for cell in row:
            tmp.append(cell.value)
        if n == 1 and fields is None:
            fields = tmp
            continue
        output.write(bson.BSON.encode(dict(zip(fields, tmp))))

        if n % 10000 == 0:
            logging.debug('xlsx2bson: processed %d records' % (n))
    source.close
    output.close()

def xls_to_bson(fromname, toname, options={}, default_options={'start_page': 0, 'start_line': 0}):
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


def _is_flat(item):
    for k, v in item.items():
        if isinstance(v, dict) or isinstance(v, tuple) or isinstance(v, list):
            return False
    return True


def express_analyze_jsonl(filename, itemlimit=100):
    f = open(filename, 'r', encoding='utf8')
    isflat = True
    n = 0
    keys = set()
    for line in f:
        n += 1
        if n > itemlimit: 
            break
        record = orjson.loads(line)
        if isflat:
            if not _is_flat(record):
                isflat = False
        if len(keys) == 0:
            keys = set(record.keys())
        else:
            keys = keys.union(set(record.keys()))
    f.close()
    keys = list(keys)
    keys.sort()
    return {'isflat': isflat, 'keys': keys}


def jsonl_to_csv(fromname, toname, options={},
                 default_options={'force_flat': False, 'useitems': 100, 'delimiter': ','}):
    options = __copy_options(options, default_options)
    analysis = express_analyze_jsonl(fromname, itemlimit=options['useitems'])
    if not options['force_flat'] and not analysis['isflat']:
        logging.error("File %s is not flat and 'force_flat' flag not set. File not converted")
        return
    out = open(toname, 'w', encoding='utf8')
    writer = csv.writer(out, delimiter=options['delimiter'])
    writer.writerow(analysis['keys'])
    f = open(fromname, 'r', encoding='utf8')
    keys = analysis['keys']
    n = 0
    for line in f:
        n += 1
        record = orjson.loads(line)
        item = []
        for k in keys:
            if k in record.keys():
                item.append(record[k])
            else:
                item.append('')
        writer.writerow(item)
        if n % 10000 == 0:
            logging.info('jsonl2csv: processed %d records' % (n))
    out.close()
    f.close()
    pass


def default(obj):
    if isinstance(obj, ObjectId):
        return str(obj)

def bson_to_jsonl(fromname, toname, options={}, default_options={}):
    options = __copy_options(options, default_options)
    source = open(fromname, 'rb')
    output = open(toname, 'wb')
    n = 0
    for r in bson.decode_file_iter(source):
        n += 1
        output.write(orjson.dumps(r, default=default))
        output.write(LINEEND)
        if n % 10000 == 0:
            logging.info('bson2jsonl: processed %d records' % (n))
    source.close()
    output.close()


def json_to_jsonl(fromname, toname, options={}, default_options={}):
    """Simple implementation of JSON to JSON lines conversion. Assuming that JSON is an array or dict with 1-st level value with data"""
    options = __copy_options(options, default_options)    
    source = open(fromname, 'rb')
    source_data = json.load(source)    
    data = source_data
    if 'tagname' in options.keys():
        if isinstance(source_data, dict) and  options['tagname'] in source_data.keys():
            data = data[options['tagname']]    
    output = open(toname, 'wb')
    n = 0
    for r in data:
        n += 1
        output.write(orjson.dumps(r) + LINEEND)
        if n % 10000 == 0:
            logging.info('json2jsonl: processed %d records' % (n))
    source.close()
    output.close()


def csv_to_parquet(fromname, toname, options={}, default_options={'encoding': 'utf8', 'delimiter': ',', 'compression' : 'brotli'}):
    options = __copy_options(options, default_options)
    df = pandas.read_csv(fromname, delimiter=options['delimiter'], encoding=options['encoding'])
    df.to_parquet(toname, compression=options['compression'] if options['compression'] != 'None' else None)


def jsonl_to_parquet(fromname, toname, options={},
                 default_options={'force_flat': False, 'useitems': 100, 'compression' : 'brotli'}):                 
    options = __copy_options(options, default_options)
    df = pandas.read_json(fromname, lines=True, encoding=options['encoding'])
    df.to_parquet(toname, compression=options['compression'] if options['compression'] != 'None' else None)


PYORC_COMPRESSION_MAP = {'zstd': 5, 'snappy' : 2, 'zlib' : 1, 'lzo' : 3, 'lz4' : 4, 'None' : 0}

def csv_to_orc(fromname, toname, options={}, default_options={'encoding': 'utf8', 'delimiter': ',', 'compression' : 'zstd'}):
    """Converts CSV file to ORC file"""
    import pyorc    
    options = __copy_options(options, default_options)
    compression = PYORC_COMPRESSION_MAP[options['compression']] if options['compression'] in PYORC_COMPRESSION_MAP.keys() else 0
    source = open(fromname, 'r', encoding=options['encoding'])
    reader = csv.DictReader(source, delimiter=options['delimiter'])
    struct_schema = []
    for field in reader.fieldnames:
        struct_schema.append('%s:string' %(field))
    output = open(toname, 'wb')
    writer = pyorc.Writer(output, "struct<%s>" % (','.join(struct_schema)), struct_repr = pyorc.StructRepr.DICT, compression=compression, compression_strategy=1)
    n = 0
    for row in reader:
        n += 1
        try: 
            writer.write(row)
        except TypeError:
            print('Error processing row %d. Skip and continue' % (n))
    writer.close()
    output.close()

def jsonl_to_orc(fromname, toname, options={},
                 default_options={'force_flat': False, 'useitems': 100, 'compression' : 'zstd'}):
    """Converts JSON file to ORC file"""
    import pyorc
    options = __copy_options(options, default_options)
    compression = PYORC_COMPRESSION_MAP[options['compression']] if options['compression'] in PYORC_COMPRESSION_MAP.keys() else 0
    df = pandas.read_json(fromname, lines=True, encoding=options['encoding'])
    df.info()
    struct_schema = df_to_pyorc_schema(df)
    output = open(toname, 'wb')
    writer = pyorc.Writer(output, "struct<%s>" % (','.join(struct_schema)), struct_repr = pyorc.StructRepr.DICT, compression=compression, compression_strategy=1)
    writer.writerows(df.to_dict(orient="records"))
    writer.close()
    output.close()

def csv_to_avro(fromname, toname, options={}, default_options={'encoding': 'utf8', 'delimiter': ',', 'compression' : 'deflate'}):
    """Converts CSV file to AVRO file"""
    import avro.schema
    from avro.datafile import DataFileWriter
    from avro.io import DatumWriter
    
    options = __copy_options(options, default_options)    
    options['compression']
    source = open(fromname, 'r', encoding=options['encoding'])
    reader = csv.DictReader(source, delimiter=options['delimiter'])

    schema_dict = {"namespace": "data.avro", "type": "record", "name": "Record", "fields": []}

    for field in reader.fieldnames:
        schema_dict['fields'].append({'name' : field, 'type' : 'string'})
    schema = avro.schema.parse(json.dumps(schema_dict))
    output = open(toname, 'wb')
    writer = DataFileWriter(output, DatumWriter(), schema, codec=options['compression'])
    n = 0
    for row in reader:
        n += 1
        try: 
            writer.append(row)
        except TypeError:
            print('Error processing row %d. Skip and continue' % (n))
    writer.close()
    output.close()

CONVERT_FUNC_MAP = {
    'xls2csv': xls_to_csv,
    'xls2jsonl': xls_to_jsonl,
    'xls2bson': xls_to_bson,
    'xlsx2jsonl': xlsx_to_jsonl,
    'xlsx2bson': xlsx_to_bson,
    'csv2jsonl': csv_to_jsonl,
    'csv2bson': csv_to_bson,
    'xml2jsonl': xml_to_jsonl,
    'jsonl2csv': jsonl_to_csv,
    'bson2jsonl': bson_to_jsonl,
    'json2jsonl': json_to_jsonl,
    'csv2parquet' : csv_to_parquet,
    'jsonl2parquet': jsonl_to_parquet,
    'jsonl2orc' : jsonl_to_orc,
    'csv2orc' : csv_to_orc,
    'csv2avro' : csv_to_avro,
}


DEFAULT_HEADERS_DETECT_LIMIT = 1000

def make_flat(item):
    result = {}
    for k, v in item.items():
        if isinstance(v, tuple) or isinstance(v, list) or isinstance(v, dict):
            result[k] = str(v)
        result[k] = v
    return result

class Converter:
    def __init__(self, batch_size = DEFAULT_BATCH_SIZE):
        self.batch_size = batch_size
        pass

    def convert(self, fromfile, tofile, options={}, limit=DEFAULT_HEADERS_DETECT_LIMIT):
        iterableargs = get_iterable_options(options)
#        print(iterableargs)
        it_in = open_iterable(fromfile, mode='r', iterableargs=iterableargs)       
        is_flatten = get_option(options, 'flatten')
        keys = []
        n = 0
        logging.info('Extracting schema')
        for item in tqdm(it_in, total=limit):
#            print(item)
            if limit is not None and n > limit:
                break
            n += 1                
            if not is_flatten:
                dk = dict_generator(item)
                for i in dk:
                    k = ".".join(i[:-1])
                    if k not in keys:
                        keys.append(k)
            else:
                item = make_flat(item)
                for k in item.keys():
                    if k not in keys:
                        keys.append(k)

        it_in.reset()
        it_out = open_iterable(tofile, mode='w', iterableargs={'keys' : keys})

        logging.info('Converting data')
        n = 0
        batch = []
        for row in tqdm(it_in):
            n += 1 
            if is_flatten:
                for k in keys:
                    if k not in row.keys(): 
                        row[k] = None              
                batch.append(make_flat(row))
            else:
                batch.append(row)
            if n % self.batch_size == 0:
                it_out.write_bulk(batch)
                batch = []
        if len(batch) > 0: 
            it_out.write_bulk(batch)
        it_in.close()
        it_out.close()


    def convert_old(self, fromfile, tofile, options={}):
        fromtype = options['format_in'] if options['format_in'] is not None else get_file_type(fromfile)
        totype = options['format_out'] if options['format_out'] is not None else get_file_type(tofile)
        key = '%s2%s' % (fromtype, totype)
        func = CONVERT_FUNC_MAP.get(key, None)
        if func is None:
            logging.error('Conversion between %s and %s not supported' % (fromtype, totype))
        else:
            logging.info('Convert %s from %s to %s' % (key, fromfile, tofile))
            func(fromfile, tofile, options)
