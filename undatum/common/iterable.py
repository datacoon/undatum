"""Iterable data handling module.

.. deprecated:: 1.0.19
    This module contains deprecated classes `IterableData` and `DataWriter`.
    Use `open_iterable()` from `iterable.helpers.detect` instead.
"""
import csv
import io
import logging

import bson
import jsonlines

from ..constants import BINARY_FILE_TYPES
from ..utils import detect_delimiter, detect_encoding, get_file_type, get_option

SUPPORTED_COMPRESSION = {'gz': True, 'zip': True, 'xz': False, '7z': False,  'lz4': False, 'bz2' : True}
import gzip
from bz2 import BZ2File
from lzma import LZMAFile
from zipfile import ZipFile

try:
    import lz4
    SUPPORTED_COMPRESSION['lz4'] = True
except ImportError:
    pass

try:
    import py7zr
    SUPPORTED_COMPRESSION['7z'] = True
except ImportError:
    pass





DEFAULT_ENCODING = 'utf8'
DEFAULT_DELIMITER = ','

class IterableData:
    """Iterable data reader (CSV/JSON lines, BSON).

    .. deprecated:: 1.0.19
        This class is deprecated and will be removed in a future version.
        Use `open_iterable()` from `iterable.helpers.detect` instead.
        Example migration:

        Old::
            from undatum.common.iterable import IterableData
            idata = IterableData(filename, options={'format_in': 'jsonl'})
            for item in idata.iter():
                process(item)
            idata.close()

        New::
            from iterable.helpers.detect import open_iterable
            iterable = open_iterable(filename, mode='r', iterableargs={'format_in': 'jsonl'})
            try:
                for item in iterable:
                    process(item)
            finally:
                iterable.close()
    """
    def __init__(self, filename, options=None, autodetect=True, autodetect_limit=100000):
        """Creates iterable object from CSV, JSON lines and other iterable files.

        .. deprecated:: 1.0.19
            Use `open_iterable()` from `iterable.helpers.detect` instead.
        """
        import warnings
        if options is None:
            options = {}
        warnings.warn(
            "IterableData is deprecated. Use open_iterable() from iterable.helpers.detect instead.",
            DeprecationWarning,
            stacklevel=2
        )
        self.autodetect = autodetect
        self.autodetect_limit = autodetect_limit
        self.options = options
        self.archiveobj = None
        self.fileobj = None
        self.binary = False
        self.delimiter = get_option(options, 'delimiter')
        self.init(filename, options)
        pass


    def init(self, filename, options):
        f_type = get_file_type(filename) if options['format_in'] is None else options['format_in']
        self.encoding = get_option(options, 'encoding')
        self.filetype = f_type
        ext = filename.rsplit('.', 1)[-1].lower()
        self.ext = ext
        if ext in SUPPORTED_COMPRESSION.keys():
            self.binary = True
            self.mode = 'rb' if self.filetype in BINARY_FILE_TYPES else 'r'
            if ext == 'gz':
                self.fileobj = gzip.open(filename, self.mode)
            elif ext == 'bz2':
                self.fileobj = BZ2File(filename, self.mode)
            elif ext == 'xz':
                self.fileobj = LZMAFile(filename, self.mode)
            elif ext == 'zip':
                self.archiveobj = ZipFile(filename, mode='r')
                fnames = self.archiveobj.namelist()
                self.fileobj = self.archiveobj.open(fnames[0], self.mode)
            else:
                raise NotImplementedError
        else:
            if f_type in BINARY_FILE_TYPES:
                self.fileobj = open(filename, 'rb')
            else:
                if 'encoding' in options.keys() and options['encoding']:
                    encoding = get_option(options, 'encoding')
                else:
                    if self.autodetect:
                        detected_enc = detect_encoding(filename, limit=self.autodetect_limit)
                        encoding = detected_enc['encoding'] if detected_enc else DEFAULT_ENCODING
                        if f_type == 'csv':
                            detected_del = detect_delimiter(filename, encoding)
                            delimiter = detected_del if detected_del else DEFAULT_DELIMITER
                            self.delimiter = delimiter
                    else:
                        encoding = DEFAULT_ENCODING
                        if f_type == 'csv':
                            delimiter = DEFAULT_DELIMITER
                            self.delimiter = delimiter
                    logging.debug('Detected encoding {}'.format(detected_enc['encoding']))
                self.encoding = encoding
                self.fileobj = open(filename, encoding=encoding)

    def init_orig(self, filename, options):
        f_type = get_file_type(filename) if options['format_in'] is None else options['format_in']
        encoding = get_option(options, 'encoding')
        self.filetype = f_type
        if options['zipfile']:
            z = ZipFile(filename, mode='r')
            fnames = z.namelist()
            if f_type in BINARY_FILE_TYPES:
                self.fileobj = z.open(fnames[0], 'rb')
            else:
                self.fileobj = z.open(fnames[0], 'r')
        else:
            if f_type in BINARY_FILE_TYPES:
                self.fileobj = open(filename, 'rb')
            else:
                if 'encoding' in options.keys() and options['encoding']:
                    encoding = get_option(options, 'encoding')
                else:
                    if self.autodetect:
                        detected_enc = detect_encoding(filename, limit=self.autodetect_limit)
                        encoding = detected_enc['encoding'] if detected_enc else DEFAULT_ENCODING
                        if f_type == 'csv':
                            detected_del = detect_delimiter(filename, encoding)
                            delimiter = detected_del if detected_del else DEFAULT_DELIMITER
                            self.delimiter = delimiter
                    else:
                        encoding = DEFAULT_ENCODING
                        if f_type == 'csv':
                            delimiter = DEFAULT_DELIMITER
                    logging.debug('Detected encoding {}'.format(detected_enc['encoding']))
                self.encoding = encoding
                self.fileobj = open(filename, encoding=encoding)

    def iter(self):
        if self.filetype == 'csv':
            if self.binary:
                obj = io.TextIOWrapper(self.fileobj, encoding=self.encoding)
                reader = csv.DictReader(obj, delimiter=self.delimiter)
            else:
                reader = csv.DictReader(self.fileobj, delimiter=self.delimiter)
            return iter(reader)
        elif self.filetype == 'jsonl':
            return jsonlines.Reader(self.fileobj)
        elif self.filetype == 'bson':
            return bson.decode_file_iter(self.fileobj)

    def close(self):
        """Closes file object and archive file object if it exists"""
        if self.fileobj is not None:
            self.fileobj.close()
        if self.archiveobj is not None:
            self.archiveobj.close()



class BSONWriter:
    """BSON file writer."""
    def __init__(self, fileobj):
        self.fo = fileobj

    def write(self, item):
        rec = bson.BSON.encode(item)
        self.fo.write(rec)


class DataWriter:
    """Data writer (CSV/JSON lines, BSON).

    .. deprecated:: 1.0.19
        This class is deprecated and will be removed in a future version.
        Use `open_iterable()` with `mode='w'` from `iterable.helpers.detect` instead.
        Example migration:

        Old::
            from undatum.common.iterable import DataWriter
            writer = DataWriter(fileobj, filetype='jsonl', fieldnames=['a', 'b'])
            writer.write_items([{'a': 1, 'b': 2}])

        New::
            from iterable.helpers.detect import open_iterable
            iterable = open_iterable(filename, mode='w', iterableargs={'keys': ['a', 'b']})
            try:
                iterable.write({'a': 1, 'b': 2})
                # or for batches:
                iterable.write_bulk([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])
            finally:
                iterable.close()
    """
    def __init__(self, fileobj, filetype, output_type:str='iterable', delimiter:str=',', fieldnames:list=None):
        """Creates iterable object from CSV, JSON lines or BSON file.

        .. deprecated:: 1.0.19
            Use `open_iterable()` with `mode='w'` from `iterable.helpers.detect` instead.
        """
        import warnings
        warnings.warn(
            "DataWriter is deprecated. Use open_iterable() with mode='w' from iterable.helpers.detect instead.",
            DeprecationWarning,
            stacklevel=2
        )
        self.output_type = output_type
        self.filetype = filetype
        self.fieldnames = fieldnames
        self.fileobj = fileobj
        if self.filetype == 'csv':
            self.writer = csv.DictWriter(self.fileobj, delimiter=delimiter, fieldnames=fieldnames)
        elif self.filetype == 'jsonl':
            self.writer = jsonlines.Writer(self.fileobj)
        elif self.filetype == 'bson':
            self.writer = BSONWriter(self.fileobj)
        pass


#    def write_item(self, row):

    def write_items(self, outdata):
        if len(outdata) == 0:
            return
        if self.filetype == 'csv':
            self.writer.writeheader()
            if isinstance(outdata[0], str):
                for rawitem in outdata:
                    item = {self.fieldnames[0]: rawitem}
                    self.writer.writerow(item)
            elif isinstance(outdata[0], list) or isinstance(outdata[0], tuple):
                for rawitem in outdata:
                    item = dict(zip(self.fieldnames, rawitem))
                    self.writer.writerow(item)
            else:
                self.writer.writerows(outdata)
        elif self.filetype in ['jsonl', 'bson']:
            # If our data is just array of strings, we just transform it to dict
            if isinstance(outdata[0], str):
                for rawitem in outdata:
                    item = {self.fieldnames[0]: rawitem}
                    self.writer.write(item)
#                    handle.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))
            elif isinstance(outdata[0], list) or isinstance(outdata[0], tuple):
                for rawitem in outdata:
                    item = dict(zip(self.fieldnames, rawitem))
                    self.writer.write(item)
#                    handle.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))
            else:
                if self.output_type == 'iterable':
                    for item in outdata:
                        self.writer.write(item)
                elif self.output_type == 'duckdb':
                    item = dict(zip(self.fieldnames, rawitem))
                    self.writer.write(item)
#                    handle.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))


if __name__ == "__main__":
    f = open('outtest.jsonl', 'w')
    writer = DataWriter(f, filetype='jsonl', fieldnames=['name', 'value'])
    writer.write_items([{'name' : 'Cat', 'value' : 15}])
    pass
