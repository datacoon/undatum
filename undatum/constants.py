# -*- coding: utf8 -*-
DATE_PATTERNS = ["%d.%m.%Y", "%Y-%m-%d", "%y-%m-%d", "%Y-%m-%dT%H:%M:%S",
                 "%Y-%m-%d %H:%M:%S",
                 "%d.%m.%Y %H:%M"]
DEFAULT_DICT_SHARE = 70



SUPPORTED_FILE_TYPES = ['xls', 'xlsx', 'csv', 'xml', 'json', 'jsonl', 'yaml', 'tsv', 'sql', 'bson', 'parquet', 'orc', 'avro']
COMPRESSED_FILE_TYPES = ['gz', 'xz', 'zip', 'lz4', '7z', 'bz2']
BINARY_FILE_TYPES = ['xls', 'xlsx', 'bson', 'parquet', 'irc'] + COMPRESSED_FILE_TYPES

DEFAULT_OPTIONS = {'encoding' : 'utf8',
                   'delimiter' : ',',
                   'limit' : 1000
                   }
