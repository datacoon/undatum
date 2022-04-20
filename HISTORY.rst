.. :changelog:

History
=======

1.0.13 (2022-04-20)
-------------------
* Fixed conversion xlsx-to-jsonl
* Added experimental command "query", not documented yet. Allows to use mistql query engine.

1.0.12 (2022-01-30)
-------------------                                                                     
* Added command "analyze" it provides human-readable information about data files: CSV, BSON, JSON lines, JSON, XML. Detects encoding, delimiters, type of files, fields with objects for JSON and XML files. Doesn't support Gzipped, ZIPped and other comressed files yet.

1.0.11 (2022-01-30)
-------------------
* Updated setup.py and requirements.txt to require certain versions of libs and Python 3.8

1.0.10 (2022-01-29)
-------------------
* Added encoding and delimiter detection for commands: uniq, select, frequency and headers. Completely rewrote these functions. If options for encoding and delimiter set, they override detected. If not set, detected delimiter and encoding used.
* Added support of .parquet files to convert to. It's done in a simpliest way using pandas "to_parquet" function.

1.0.9 (2022-01-18)
------------------
* Added support for CSV and BSON files for "stats" command

1.0.8 (2021-07-14)
------------------
* Replaced json with orjson for some operations. Keep looking on performance changes and going to replace or json lib calls to orjson

1.0.7 (2020-10-26)
------------------
* Added initial code to convert JSON lines files to CSV

1.0.6 (2020-04-20)
------------------
* First public release on PyPI and updated github code
