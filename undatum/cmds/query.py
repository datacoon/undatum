"""Data query module using mistql."""
import logging

from iterable.helpers.detect import open_iterable

from ..utils import get_file_type, get_option, strip_dict_fields

LINEEND = b'\n'

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


class DataQuery:
    """Data query handler using mistql."""
    def __init__(self):
        pass

    def query(self, fromfile, options=None):
        """Use mistql to query data."""
        if options is None:
            options = {}
        from mistql import query
        iterableargs = get_iterable_options(options)
        to_file = get_option(options, 'output')

        if to_file:
            get_file_type(to_file)
            if not to_file:
                logging.error('Output file type not supported')
                return
            out_iterable = open_iterable(to_file, mode='w', iterableargs={})
        else:
            out_iterable = None

        fields_value = get_option(options, 'fields')
        fields = fields_value.split(',') if fields_value else None
        fields_list = [field.split('.') for field in fields] if fields else None

        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        try:
            n = 0
            for r in iterable:
                n += 1
                if fields_list:
                    r_selected = strip_dict_fields(r, fields_list, 0)
                else:
                    r_selected = r
                if options.get('query') is not None:
                    res = query(options['query'], r_selected)
                    if not res:
                        continue
                else:
                    res = r_selected

                if out_iterable:
                    out_iterable.write(res)
                else:
                    print(res)
        finally:
            iterable.close()

        logging.debug('query: %d records processed', n)
        if out_iterable:
            out_iterable.close()
