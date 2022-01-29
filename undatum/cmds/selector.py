import csv
# import json
import logging
import sys
import zipfile

import bson
import dictquery as dq
import orjson

# from xmlr import xmliter
from ..utils import get_file_type, get_option, write_items, get_dict_value, strip_dict_fields, dict_generator, detect_encoding
from ..common.iterable import IterableData, DataWriter
LINEEND = u'\n'.encode('utf8')


def get_iterable_fields_uniq(iterable, fields, dolog=False, dq=None):
    """ Returns all uniq values of the fields of iterable dictionary"""
    n = 0
    uniqval = []
    for row in iterable:
#        print(row)
        n += 1
        if dolog and n % 1000 == 0:
            logging.debug('uniq: processing %d records' % (n))
        try:
            allvals = []
            for field in fields:
                allvals.append(get_dict_value(row, field.split('.')))

            for n1 in range(0, len(allvals[0]), 1):
                k = []
                for n2 in range(0, len(allvals)):
                    k.append(str(allvals[n2][n1]))
                if k not in uniqval:
                    uniqval.append(k)
        except KeyError:
            pass
    return uniqval

def get_iterable_fields_freq(iterable, fields, dolog=False, filter=None, dq=None):
    """Iterates and returns most frequent values"""
    n = 0
    valuedict = {}
    for r in iterable:
        n += 1
        if dolog and n % 10000 == 0:
            logging.info('frequency: processing %d records' % (n))
        if filter is not None:
            if not dq.match(r, filter):
                continue
        try:
            allvals = []
            for field in fields:
                allvals.append(get_dict_value(r, field.split('.')))

            for n1 in range(0, len(allvals[0]), 1):
                k = []
                for n2 in range(0, len(allvals)):
                    k.append(str(allvals[n2][n1]))
                kx = '\t'.join(k)
                v = valuedict.get(kx, 0)
                valuedict[kx] = v + 1
        except KeyError:
            pass
    return valuedict


class Selector:
    def __init__(self):
        pass

    def uniq(self, fromfile, options={}):
        logging.debug('Processing %s' % fromfile)
        iterable = IterableData(fromfile, options=options)
        to_file = get_option(options, 'output')
        if to_file:
            to_type = get_file_type(to_file)
            if not to_file:
                logging.debug('Output file type not supported')
                return
            out = open(to_file, 'w', encoding='utf8')
        else:
            to_type = 'csv'
            out = sys.stdout
        fields = options['fields'].split(',')
        logging.info('uniq: looking for fields: %s' % (options['fields']))
        n = 0
        uniqval = get_iterable_fields_uniq(iterable.iter(), fields, dolog=True)
        iterable.close()
        logging.debug('%d unique values found' % (len(uniqval)))
        write_items(fields, uniqval, filetype=to_type, handle=out)


    def headers(self, fromfile, options={}):
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        limit = get_option(options, 'limit')
        iterable = IterableData(fromfile, options=options)
        if f_type == 'csv':
            keys = iterable.iter().fieldnames
        else:
            n = 0
            keys = []
            for item in iterable.iter():
                if limit and n > limit:
                    break
                n += 1
                dk = dict_generator(item)
                for i in dk:
                    k = ".".join(i[:-1])
                    if k not in keys:
                        keys.append(k)
        iterable.close()
        output = get_option(options, 'output')
        if output:
            f = open(output, 'w', encoding=get_option(options, 'encoding'))
            f.write('\n'.join(keys))
            f.close()
        else:
            print('\n'.join(keys))

    def frequency(self, fromfile, options={}):
        """Calculates frequency of the values in the file"""
        iterable = IterableData(fromfile, options=options)

        to_file = get_option(options, 'output')
        if to_file:
            to_type = get_file_type(to_file)
            if not to_file:
                print('Output file type not supported')
                return
            out = open(to_file, 'w', encoding='utf8')
        else:
            to_type = 'csv'
            out = sys.stdout
        fields = options['fields'].split(',')
        valuedict = {}
        if iterable:
            valuedict = get_iterable_fields_freq(iterable.iter(), fields, dolog=True)
        else:
            logging.info('File type not supported')
            return
        logging.debug('frequency: %d unique values found' % (len(valuedict)))
        thedict = sorted(valuedict.items(), key=lambda item: item[1], reverse=False)
        output = get_option(options, 'output')
        strkeys = '\t'.join(fields) + '\tcount'
        if output:
            f = open(output, 'w', encoding=get_option(options, 'encoding'))
            f.write(strkeys + '\n')
            for k, v in thedict:
                f.write('%s\t%d\n' % (k, v))
            f.close()
        else:
            print(strkeys)
            for k, v in thedict:
                print('%s\t%d' % (k, v))

    def select(self, fromfile, options={}):
        """Select or re-order columns from file"""
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        iterable = IterableData(fromfile, options=options)
        to_file = get_option(options, 'output')

        if to_file:
            to_type = get_file_type(to_file)
            if not to_file:
                print('Output file type not supported')
                return
            if to_type == 'bson':
                out = open(to_file, 'wb')
            if to_type == 'jsonl':
                out = open(to_file, 'wb')
            else:
                out = open(to_file, 'w', encoding='utf8')
        else:
            to_type = f_type
            out = sys.stdout
        fields = options['fields'].split(',')
        writer = DataWriter(out, filetype=to_type, fieldnames=fields)
        if iterable:
            n = 0
            fields = [field.split('.') for field in fields]
            chunk = []
            for r in iterable.iter():
                n += 1
                if options['filter'] is not None:
                    res = dq.match(r, options['filter'])
                    #                    print(options['filter'], r)
                    if not res:
                        continue
                r_selected = strip_dict_fields(r, fields, 0)
                if n % 1000 == 0:
                    logging.info('select: processing %d records of %s' % (n, fromfile))
                    if len(chunk) > 0:
                        writer.write_items(chunk)
                        chunk = []
                else:
                    chunk.append(r_selected)
            if len(chunk) > 0:
                writer.write_items(chunk)
        else:
            logging.info('File type not supported')
            return
        logging.debug('select: %d records processed' % (n))
        out.close()

    def split(self, fromfile, options={}):
        """Splits the given file with data into chunks based on chunk size or field value"""
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            finfilename = fnames[0]
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        elif options['gzipfile']:
            import gzip
            infile = gzip.open(fromfile, 'rb')
            finfilename = fromfile.split('.', 1)[0] + '.' + f_type
        else:
            finfilename = fromfile
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                if 'encoding' in options.keys():
                    infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
                else:
                    detected_enc = detect_encoding(fromfile, limit=100000)
                    if detected_enc:
                        infile = open(fromfile, 'r', encoding=detected_enc['encoding'])
                    else:
                        infile = open(fromfile, 'r', encoding='utf8')
        fields = options['fields'].split(',') if options['fields'] is not None else None
        valuedict = {}
        delimiter = get_option(options, 'delimiter')
        if f_type == 'csv':
            reader = csv.DictReader(infile, delimiter=delimiter)
            n = 0
            chunknum = 1
            if options['fields'] is None:
                splitname = finfilename.rsplit('.', 1)[0] + '_%d.csv' % (chunknum)
                out = open(splitname, 'w', encoding=get_option(options, 'encoding'))
                writer = csv.DictWriter(out, fieldnames=reader.fieldnames, delimiter=delimiter)
                writer.writeheader()
                for r in reader:
                    n += 1
                    if n % 10000 == 0:
                        logging.info('split: processing %d records of %s' % (n, fromfile))
                    if options['filter'] is not None:
                        if not dq.match(r, options['filter']):
                            continue
                    writer.writerow(r)
                    if n % options['chunksize'] == 0:
                        out.close()
                        chunknum += 1
                        splitname = finfilename.rsplit('.', 1)[0] + '_%d.csv' % (chunknum)
                        out = open(splitname, 'w', encoding=get_option(options, 'encoding'))
                        writer = csv.DictWriter(out, fieldnames=reader.fieldnames, delimiter=delimiter)
                        writer.writeheader()
                out.close()
        elif f_type == 'jsonl':
            n = 0
            chunknum = 1
            if options['fields'] is None:
                splitname = finfilename.rsplit('.', 1)[0] + '_%d.jsonl' % (chunknum)
                out = open(splitname, 'wb')  # , encoding=get_option(options, 'encoding'))

                for l in infile:
                    n += 1
                    if n % 10000 == 0:
                        logging.info('split: processing %d records of %s' % (n, fromfile))
                    r = orjson.loads(l)
                    if options['filter'] is not None:
                        if not dq.match(r, options['filter']):
                            continue
                    out.write(orjson.dumps(r, option=orjson.OPT_APPEND_NEWLINE))
                    if n % options['chunksize'] == 0:
                        out.close()
                        chunknum += 1
                        splitname = finfilename.rsplit('.', 1)[0] + '_%d.jsonl' % (chunknum)
                        logging.info('split: new chunk %s' % splitname)
                        out = open(splitname, 'w', encoding=get_option(options, 'encoding'))
            else:
                for l in infile:
                    n += 1
                    if n % 10000 == 0:
                        logging.info('split: processing %d records of %s' % (n, fromfile))
                    r = orjson.loads(l)
                    if options['filter'] is not None:
                        if not dq.match(r, options['filter']):
                            continue
                    try:
                        kx = get_dict_value(r, fields[0].split('.'))[0]
                    except IndexError:
                        continue
                        kx = "None"
                    if kx is None: continue
                    kx = kx.replace('\\', '-').replace('/', '-').replace('?', '-').replace('<', '-').replace('>', '-')
                    v = valuedict.get(kx, None)
                    if v is None:
                        splitname = finfilename.rsplit('.', 1)[0] + '_%s.jsonl' % (kx)
                        valuedict[kx] = open(splitname, 'w', encoding='utf8')
                    valuedict[kx].write(l)
                #                    valuedict[kx].write(l.decode('utf8'))#.decode('utf8')#)
                for opened in valuedict.values():
                    opened.close()
        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            for r in bson_iter:
                n += 1
                #                print(r)
                r_selected = strip_dict_fields(r, fields, 0)
                #                out.write(json.dumps(r_selected)+'\n')
                if n % 10000 == 0:
                    logging.info('split: processing %d records of %s' % (n, fromfile))

        else:
            logging.info('File type not supported')
            return
        logging.debug('split: %d records processed' % (n))
