import csv
# import json
import logging
import sys
import zipfile

import bson
import dictquery as dq
import orjson

# from xmlr import xmliter
from ..utils import get_file_type, get_option, write_items, get_dict_value, strip_dict_fields, dict_generator

LINEEND = u'\n'.encode('utf8')


class Selector:
    def __init__(self):
        pass

    def uniq(self, fromfile, options={}):
        logging.debug('Processing %s' % fromfile)
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')

        else:
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
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
        if f_type == 'csv':
            delimiter = get_option(options, 'delimiter')
            uniqval = []
            reader = csv.DictReader(infile, delimiter=delimiter)
            n = 0
            for r in reader:
                n += 1
                if n % 1000 == 0:
                    logging.info('uniq: processing %d records of %s' % (n, fromfile))
                if options['filter'] is not None:
                    if not dq.match(r, options['filter']):
                        continue
                k = [r[x] for x in fields]
                if k not in uniqval:
                    uniqval.append(k)

        elif f_type == 'jsonl':
            uniqval = []
            n = 0
            for l in infile:
                n += 1
                if n % 10000 == 0:
                    logging.info('uniq: processing %d records of %s' % (n, fromfile))
                r = orjson.loads(l)
                if options['filter'] is not None:
                    if not dq.match(r, options['filter']):
                        continue
                try:
                    allvals = []
                    for field in fields:
                        allvals.append(get_dict_value(r, field.split('.')))

                    for n1 in range(0, len(allvals[0]), 1):
                        k = []
                        for n2 in range(0, len(allvals)):
                            k.append(str(allvals[n2][n1]))
                        if k not in uniqval:
                            uniqval.append(k)
                except KeyError:
                    pass
        elif f_type == 'bson':
            uniqval = []
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            for r in bson_iter:
                n += 1
                if n % 1000 == 0:
                    logging.info('uniq: processing %d records of %s' % (n, fromfile))
                if options['filter'] is not None:
                    if not dq.match(r, options['filter']):
                        continue
                try:
                    allvals = []
                    for field in fields:
                        allvals.append(get_dict_value(r, field.split('.')))

                    for n1 in range(0, len(allvals[0]), 1):
                        k = []
                        for n2 in range(0, len(allvals)):
                            k.append(str(allvals[n2][n1]))
                        if k not in uniqval:
                            uniqval.append(k)
                except KeyError:
                    pass
        else:
            logging.error('Invalid filed format provided')
            return
        infile.close()
        logging.debug('%d unique values found' % (len(uniqval)))
        write_items(fields, uniqval, filetype=to_type, handle=out)

    def headers(self, fromfile, options={}):
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        limit = get_option(options, 'limit')
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            if f_type == 'bson':
                f = z.open(fnames[0], 'rb')
            else:
                f = z.open(fnames[0], 'r')
        else:
            if f_type == 'bson':
                f = open(fromfile, 'rb')
            else:
                f = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
        if f_type == 'csv':
            delimiter = get_option(options, 'delimiter')
            dr = csv.DictReader(f, delimiter=delimiter)
            keys = dr.fieldnames
        elif f_type == 'jsonl':
            n = 0
            keys = []
            for l in f:
                n += 1
                if n > limit: break
                item = orjson.loads(l)
                dk = dict_generator(item)
                for i in dk:
                    k = ".".join(i[:-1])
                    if k not in keys:
                        keys.append(k)
        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(f)
            n = 0
            while n < limit:
                n += 1
                try:
                    item = next(bson_iter)
                except:
                    break
                dk = dict_generator(item)
                keys = []
                for i in dk:
                    k = ".".join(i[:-1])
                    if k not in keys:
                        keys.append(k)
        f.close()
        output = get_option(options, 'output')
        if output:
            f = open(output, 'w', encoding=get_option(options, 'encoding'))
            f.write('\n'.join(keys))
            f.close()
        else:
            print('\n'.join(keys))

    def frequency(self, fromfile, options={}):
        """Calculates frequency of the values in the file"""
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        else:
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
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
        if f_type == 'csv':
            delimiter = get_option(options, 'delimiter')
            reader = csv.DictReader(infile, delimiter=delimiter)
            n = 0
            for r in reader:
                n += 1
                if n % 10000 == 0:
                    logging.info('frequency: processing %d records of %s' % (n, fromfile))
                if options['filter'] is not None:
                    if not dq.match(r, options['filter']):
                        continue
                k = [r[x] for x in fields]
                kx = '\t'.join(k)
                v = valuedict.get(kx, 0)
                valuedict[kx] = v + 1
        elif f_type == 'jsonl':
            n = 0
            for l in infile:
                n += 1
                if n % 10000 == 0:
                    logging.info('frequency: processing %d records of %s' % (n, fromfile))
                r = orjson.loads(l)
                if options['filter'] is not None:
                    if not dq.match(r, options['filter']):
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
        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            for r in bson_iter:
                n += 1
                if n % 10000 == 0:
                    logging.info('frequency: processing %d records of %s' % (n, fromfile))
                if options['filter'] is not None:
                    if not dq.match(r, options['filter']):
                        continue

                #                print(r)
                allvals = []
                for field in fields:
                    allvals.append(get_dict_value(r, field.split('.')))

                for n1 in range(0, len(allvals[0]), 1):
                    k = []
                    for n2 in range(0, len(allvals)):
                        k.append(str(allvals[n2][n1]))
                    v = valuedict.get(k, 0)
                    valuedict[k] = v + 1
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
        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        else:
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
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
        valuedict = {}
        delimiter = get_option(options, 'delimiter')
        if f_type == 'csv':
            reader = csv.DictReader(infile, delimiter=delimiter)
            if to_type == 'csv':
                writer = csv.DictWriter(out, fieldnames=fields, delimiter=delimiter)
                writer.writeheader()
            n = 0
            for r in reader:
                n += 1
                if n % 10000 == 0:
                    logging.info('select: processing %d records of %s' % (n, fromfile))
                item = {}
                if options['filter'] is not None:
                    if not dq.match(r, options['filter']):
                        continue
                for x in fields:
                    item[x] = r[x]
                if to_type == 'csv':
                    writer.writerow(item)
                elif to_type == 'jsonl':
                    out.write(orjson.dumps(r_selected, option=orjson.OPT_APPEND_NEWLINE).encode('utf8'))
        elif f_type == 'jsonl':
            n = 0
            fields = [field.split('.') for field in fields]
            for l in infile:
                n += 1
                if n % 10000 == 0:
                    logging.info('select: processing %d records of %s' % (n, fromfile))
                r = orjson.loads(l)
                if options['filter'] is not None:
                    res = dq.match(r, options['filter'])
                    #                    print(options['filter'], r)
                    if not res:
                        continue
                r_selected = strip_dict_fields(r, fields, 0)
                out.write(orjson.dumps(r_selected, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))
        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            fields = [field.split('.') for field in fields]
            for r in bson_iter:
                n += 1
                if n % 10000 == 0:
                    logging.info('select: processing %d records of %s' % (n, fromfile))
                if options['filter'] is not None:
                    res = dq.match(r, options['filter'])
                    if not res:
                        continue
                r_selected = strip_dict_fields(r, fields, 0)
                out.write(orjson.dumps(r_selected, option=orjson.OPT_APPEND_NEWLINE).encode('utf8'))
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
                infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
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
