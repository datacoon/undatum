# -*- coding: utf8 -*-
"""Statistical analysis module."""
from ..utils import get_option, dict_generator, guess_datatype
from ..constants import DEFAULT_DICT_SHARE
import logging
from qddate import DateParser
#from ..common.iterable import IterableData
from iterable.helpers.detect import open_iterable

#STAT_READY_DATA_FORMATS = ['jsonl', 'bson', 'csv']

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


class StatProcessor:
    """Statistical processing handler."""
    def __init__(self, nodates=True):
        if nodates:
            self.qd = None
        else:
            self.qd = DateParser(generate=True)
        pass

    def stats(self, fromfile, options):
        """Produces statistics and structure analysis of JSONlines, BSON or CSV file and produces stats"""
        from rich import print
        from rich.table import Table

        iterableargs = get_iterable_options(options)
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
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

        # process data items one by one
        logging.debug('Start processing %s' % (fromfile))
        for item in iterable:
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
                if k not in fielddata:  # Use direct dict membership check instead of list()
                    fielddata[k] = {'key': k, 'uniq': {}, 'n_uniq': 0, 'total': 0, 'share_uniq': 0.0,
                                    'minlen': None, 'maxlen': 0, 'avglen': 0, 'totallen': 0}
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
                if k not in fieldtypes:  # Use direct dict membership check instead of list()
                    fieldtypes[k] = {'key': k, 'types': {}}
                fd = fieldtypes[k]
                thetype = guess_datatype(v, self.qd)['base']
                uniqval = fd['types'].get(thetype, 0)
                fd['types'][thetype] = uniqval + 1
                fieldtypes[k] = fd
        #        print count
        for k, v in fielddata.items():  # Use dict.items() directly, no list() conversion
            fielddata[k]['share_uniq'] = (v['n_uniq'] * 100.0) / v['total']
            fielddata[k]['avglen'] = v['totallen'] / v['total']
        profile['count'] = count
        profile['num_fields'] = nfields

        # Determine field types first so we can use them when building dicts
        finfields = {}
        for fd in fieldtypes.values():  # Use dict.values() directly, no list() conversion
            fdt = list(fd['types'].keys())  # Keep list() here as we need to check membership and modify
            if 'empty' in fdt:
                del fd['types']['empty']
            types_keys = list(fd['types'].keys())  # Need list for len() and indexing
            if len(types_keys) != 1:
                ftype = 'str'
            else:
                ftype = types_keys[0]
            finfields[fd['key']] = ftype

        profile['fieldtypes'] = finfields

        dictkeys = []
        dicts = {}
        #        print(profile)
        profile['fields'] = []
        for fd in fielddata.values():  # Use dict.values() directly, no list() conversion
            #            print(fd['key'])  # , fd['n_uniq'], fd['share_uniq'], fieldtypes[fd['key']]
            field = {'key': fd['key'], 'is_uniq': 0 if fd['share_uniq'] < 100 else 1}
            profile['fields'].append(field)
            if fd['share_uniq'] < dictshare:
                dictkeys.append(fd['key'])
                # Use determined field type instead of defaulting to 'str'
                field_type = finfields.get(fd['key'], 'str')
                dicts[fd['key']] = {'items': fd['uniq'], 'count': fd['n_uniq'],
                                    'type': field_type}
        #            for k, v in fd['uniq'].items():
        #                print fd['key'], k, v
        profile['dictkeys'] = dictkeys

        for k, v in fielddata.items():  # Use dict.items() directly, no list() conversion
            del v['uniq']
            fielddata[k] = v
        profile['debug'] = {'fieldtypes': fieldtypes.copy(), 'fielddata': fielddata}
        table = []
        for fd in fielddata.values():  # Use dict.values() directly, no list() conversion
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
        headers = ('key', 'ftype', 'is_dictkey', 'is_uniq', 'n_uniq', 'share_uniq', 'minlen', 'maxlen', 'avglen')
        reptable = Table(title="Statistics")
        reptable.add_column(headers[0], justify="left", style="magenta")
        for key in headers[1:-1]:
            reptable.add_column(key, justify="left", style="cyan", no_wrap=True)
        reptable.add_column(headers[-1], justify="right", style="cyan")
        for row in table:
            reptable.add_row(*map(str, row))
        print(reptable)

