from ..utils import get_file_type, get_option, dict_generator, guess_int_size, guess_datatype
from ..constants import DATE_PATTERNS, DEFAULT_DICT_SHARE
from datetime import datetime
import logging
import bson
import orjson
import csv
import zipfile
from qddate import DateParser

STAT_READY_DATA_FORMATS = ['jsonl', 'bson', 'csv']

class StatProcessor:
    def __init__(self, nodates=True):
        if nodates:
            self.qd = None
        else:
            self.qd = DateParser(generate=True)
        pass

    def stats(self, fromfile, options):
        """Produces statistics and structure analysis of JSONlines, BSON or CSV file and produces stats"""
        from tabulate import tabulate
        f_type = get_file_type(fromfile) if options['format_in'] is None else options['format_in']
        if f_type not in STAT_READY_DATA_FORMATS:
            print('Only JSON lines (.jsonl), .csv and .bson files supported now')
            return

        if options['zipfile']:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            finfilename = fnames[0]
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        else:
            finfilename = fromfile
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                infile = open(fromfile, 'r', encoding=get_option(options, 'encoding'))
        dictshare = get_option(options, 'dictshare')
        if dictshare and dictshare.isdigit():
            dictshare = int(dictshare)
        else:
            dictshare = DEFAULT_DICT_SHARE

        profile = {'version': 1.0}
        fielddata = {}
        fieldtypes = {}

        #    data = json.load(open(profile['filename']))
        count = 0
        nfields = 0

        # Identify item list
        itemlist = []

        if f_type == 'jsonl':
            for l in infile:
                itemlist.append(orjson.loads(l))
        elif f_type == 'csv':
            delimiter = get_option(options, 'delimiter')
            reader = csv.DictReader(infile, delimiter=delimiter)
            for r in reader:
                itemlist.append(r)
        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(infile)
            for r in bson_iter:
                itemlist.append(r)

        # process data items one by one
        logging.debug('Start processing %s' % (fromfile))
        for item in itemlist:
            count += 1
            dk = dict_generator(item)
            if count % 1000 == 0: logging.debug('Processing %d records of %s' % (count, fromfile))
            for i in dk:
                #            print(i)
                k = '.'.join(i[:-1])
                if len(i) == 0: continue
                if i[0].isdigit(): continue
                if len(i[0]) == 1: continue
                v = i[-1]
                if k not in list(fielddata.keys()):
                    fielddata[k] = {'key': k, 'uniq': {}, 'n_uniq': 0, 'total': 0, 'share_uniq': 0.0, 'minlen' : None, 'maxlen' : 0, 'avglen' : 0, 'totallen' : 0}
                fd = fielddata[k]
                uniqval = fd['uniq'].get(v, 0)
                fd['uniq'][v] = uniqval + 1
                fd['total'] += 1
                if uniqval == 0:
                    fd['n_uniq'] += 1
                    fd['share_uniq'] = (fd['n_uniq'] * 100.0) / fd['total']
                fl = len(str(v))
                if fd['minlen'] is None:
                    fd['minlen'] = fl
                else:
                    fd['minlen'] = fl if fl < fd['minlen'] else fd['minlen']
                fd['maxlen'] = fl if fl > fd['maxlen'] else fd['maxlen']
                fd['totallen'] += fl
                fielddata[k] = fd
                if k not in list(fieldtypes.keys()):
                    fieldtypes[k] = {'key': k, 'types': {}}
                fd = fieldtypes[k]
                thetype = guess_datatype(v, self.qd)['base']
                uniqval = fd['types'].get(thetype, 0)
                fd['types'][thetype] = uniqval + 1
                fieldtypes[k] = fd
        #        print count
        for k, v in list(fielddata.items()):
            fielddata[k]['share_uniq'] = (v['n_uniq'] * 100.0) / v['total']
            fielddata[k]['avglen'] = v['totallen'] / v['total']
        profile['count'] = count
        profile['num_fields'] = nfields
        dictkeys = []
        dicts = {}
#        print(profile)
        profile['fields'] = []
        for fd in list(fielddata.values()):
#            print(fd['key'])  # , fd['n_uniq'], fd['share_uniq'], fieldtypes[fd['key']]
            field = {'key': fd['key'], 'is_uniq': 0 if fd['share_uniq'] < 100 else 1}
            profile['fields'].append(field)
            if fd['share_uniq'] < dictshare:
                dictkeys.append(fd['key'])
                dicts[fd['key']] = {'items': fd['uniq'], 'count': fd['n_uniq'],
                                    'type': 'str'}  # TODO: Shouldn't be "str" by default
        #            for k, v in fd['uniq'].items():
        #                print fd['key'], k, v
        profile['dictkeys'] = dictkeys

        finfields = {}
        for k, v in list(fielddata.items()):
            del v['uniq']
            fielddata[k] = v
        profile['debug'] = {'fieldtypes': fieldtypes.copy(), 'fielddata': fielddata}
        for fd in list(fieldtypes.values()):
            fdt = list(fd['types'].keys())
            if 'empty' in fdt:
                del fd['types']['empty']
            if len(list(fd['types'].keys())) != 1:
                ftype = 'str'
            else:
                ftype = list(fd['types'].keys())[0]
            finfields[fd['key']] = ftype

        profile['fieldtypes'] = finfields
        table = []
        for fd in list(fielddata.values()):
            field = [fd['key'], ]
            field.append(finfields[fd['key']])
            field.append(True if fd['key'] in dictkeys else False)
            field.append(False if fd['share_uniq'] < 100 else True)
            field.append(fd['n_uniq'])
            field.append(fd['share_uniq'])
            field.append(fd['minlen'])
            field.append(fd['maxlen'])
            field.append(fd['avglen'])
            table.append(field)
        headers = ['key', 'ftype', 'is_dictkey', 'is_uniq', 'n_uniq', 'share_uniq', 'minlen', 'maxlen', 'avglen']
        print(tabulate(table, headers=headers))



