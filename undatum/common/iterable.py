"""File-object based data writers (CSV/JSON lines, BSON).

Reading data files is handled by ``open_iterable()`` from
``iterable.helpers.detect`` (the ``iterabledata`` package). The writers in
this module complement it for the cases ``open_iterable`` cannot cover:
writing to an already-open file object such as ``sys.stdout``.

.. deprecated:: 1.0.19
    The legacy ``IterableData`` reader class was removed in 1.2.0.
    Use ``open_iterable()`` from ``iterable.helpers.detect`` instead.
"""

import csv

import bson
import jsonlines


class BSONWriter:
    """BSON file writer."""

    def __init__(self, fileobj):
        self.fo = fileobj

    def write(self, item):
        rec = bson.BSON.encode(item)
        self.fo.write(rec)


class DataWriter:
    """Record writer for CSV, JSON lines, and BSON over an open file object.

    Unlike ``open_iterable(..., mode='w')``, which requires a file path, this
    writer accepts any file-like object (including ``sys.stdout``), which is
    why command modules use it for console output.

    Example:
        >>> import sys
        >>> writer = DataWriter(sys.stdout, filetype='jsonl', fieldnames=['a', 'b'])
        >>> writer.write_items([{'a': 1, 'b': 2}])
    """

    def __init__(
        self,
        fileobj,
        filetype,
        output_type: str = "iterable",
        delimiter: str = ",",
        fieldnames: list = None,
    ):
        """Creates a writer over an open file object.

        Args:
            fileobj: Open file-like object to write to.
            filetype: Output format: 'csv', 'jsonl', or 'bson'.
            output_type: Source engine hint: 'iterable' (default) or 'duckdb'.
            delimiter: CSV delimiter character.
            fieldnames: Field names for CSV header or dict construction.
        """
        self.output_type = output_type
        self.filetype = filetype
        self.fieldnames = fieldnames
        self.fileobj = fileobj
        if self.filetype == "csv":
            self.writer = csv.DictWriter(self.fileobj, delimiter=delimiter, fieldnames=fieldnames)
        elif self.filetype == "jsonl":
            self.writer = jsonlines.Writer(self.fileobj)
        elif self.filetype == "bson":
            self.writer = BSONWriter(self.fileobj)

    def write_items(self, outdata):
        """Write a list of records, adapting strings/sequences to dicts."""
        if len(outdata) == 0:
            return
        if self.filetype == "csv":
            self.writer.writeheader()
            if isinstance(outdata[0], str):
                for rawitem in outdata:
                    item = {self.fieldnames[0]: rawitem}
                    self.writer.writerow(item)
            elif isinstance(outdata[0], (list, tuple)):
                for rawitem in outdata:
                    item = dict(zip(self.fieldnames, rawitem))
                    self.writer.writerow(item)
            else:
                self.writer.writerows(outdata)
        elif self.filetype in ["jsonl", "bson"]:
            # If our data is just array of strings, we just transform it to dict
            if isinstance(outdata[0], str):
                for rawitem in outdata:
                    item = {self.fieldnames[0]: rawitem}
                    self.writer.write(item)
            elif isinstance(outdata[0], (list, tuple)):
                for rawitem in outdata:
                    item = dict(zip(self.fieldnames, rawitem))
                    self.writer.write(item)
            else:
                for item in outdata:
                    self.writer.write(item)
