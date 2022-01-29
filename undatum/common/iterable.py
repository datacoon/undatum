# -*- coding: utf-8 -*-
from ..utils import get_file_type, get_option, detect_encoding, detect_delimiter
import zipfile
import csv
import json
import orjson
import jsonlines
import bson
import logging

DEFAULT_ENCODING = 'utf8'
DEFAULT_DELIMITER = ','

class IterableData:
    """Iterable data reader (CSV/JSON lines, BSON"""
    def __init__(self, filename, options={}, autodetect=True, autodetect_limit=100000):
        """Creates iterable object from CSV, JSON lines or BSON file.
        """
        self.autodetect = autodetect
        self.autodetect_limit = autodetect_limit
        self.options = options
        self.init(filename, options)
        pass


    def init(self, filename, options):
        f_type = get_file_type(filename) if options['format_in'] is None else options['format_in']
        self.filetype = f_type
        if options['zipfile']:
            z = zipfile.ZipFile(filename, mode='r')
            fnames = z.namelist()
            if f_type == 'bson':
                self.fileobj = z.open(fnames[0], 'rb')
            else:
                # FIXME
                self.fileobj = z.open(fnames[0], 'r')
        else:
            if f_type == 'bson':
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
                    logging.debug('Detected encoding %s' % (detected_enc['encoding']))
                self.encoding = encoding
                self.fileobj = open(filename, 'r', encoding=encoding)

    def iter(self):
        if self.filetype == 'csv':
            reader = csv.DictReader(self.fileobj, delimiter=self.delimiter)
            return iter(reader)
        elif self.filetype == 'jsonl':
            return jsonlines.Reader(self.fileobj)
        elif self.filetype == 'bson':
            return bson.decode_file_iter(self.fileobj)

    def close(self):
        """Closes file object"""
        self.fileobj.close()



class BSONWriter:
    def __init__(self, fileobj):
        self.fo = fileobj

    def write(self, item):
        rec = bson.BSON.encode(j)
        self.fo.write(rec)


class DataWriter:
    """Data writer (CSV/JSON lines, BSON"""
    def __init__(self, fileobj, filetype, delimiter=',', fieldnames=None):
        """Creates iterable object from CSV, JSON lines or BSON file.
        """
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
                    item = {self.fieldsnames[0]: rawitem}
                    self.writer.writerow(item)
            elif type(outdata[0]) == type([]):
                for rawitem in outdata:
                    item = dict(zip(self.fieldsnames, rawitem))
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
            elif type(outdata[0]) == type([]):
                for rawitem in outdata:
                    item = dict(zip(self.fieldnames, rawitem))
                    self.writer.write(item)
#                    handle.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))
            else:
                for item in outdata:
                    self.writer.write(item)
#                    handle.write(orjson.dumps(item, option=orjson.OPT_APPEND_NEWLINE).decode('utf8'))


if __name__ == "__main__":
    f = open('outtest.jsonl', 'w')
    writer = DataWriter(f, filetype='jsonl', fieldnames=['name', 'value'])
    writer.write_items([{'name' : 'Cat', 'value' : 15}])
    pass
