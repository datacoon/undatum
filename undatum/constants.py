"""Constants and configuration values for the undatum package."""
DATE_PATTERNS = ["%d.%m.%Y", "%Y-%m-%d", "%y-%m-%d", "%Y-%m-%dT%H:%M:%S",
                 "%Y-%m-%d %H:%M:%S",
                 "%d.%m.%Y %H:%M"]
DEFAULT_DICT_SHARE = 70


SUPPORTED_FILE_TYPES = ['xls', 'xlsx', 'csv', 'xml', 'json', 'jsonl', 'yaml',
                        'tsv', 'sql', 'bson', 'parquet', 'orc', 'avro']
COMPRESSED_FILE_TYPES = ['gz', 'xz', 'zip', 'lz4', '7z', 'bz2']
BINARY_FILE_TYPES = ['xls', 'xlsx', 'bson', 'parquet', 'irc'] + \
    COMPRESSED_FILE_TYPES

DEFAULT_OPTIONS = {'encoding': 'utf8',
                   'delimiter': ',',
                   'limit': 1000
                   }

DUCKABLE_FILE_TYPES = ['csv', 'jsonl', 'json', 'parquet']
DUCKABLE_CODECS = ['zst', 'gzip', 'raw']

EU_DATA_THEMES = [
    {"label": "AGRI", "uri": "http://publications.europa.eu/resource/authority/data-theme/AGRI"},
    {"label": "ECON", "uri": "http://publications.europa.eu/resource/authority/data-theme/ECON"},
    {"label": "EDUC", "uri": "http://publications.europa.eu/resource/authority/data-theme/EDUC"},
    {"label": "ENVI", "uri": "http://publications.europa.eu/resource/authority/data-theme/ENVI"},
    {"label": "ENER", "uri": "http://publications.europa.eu/resource/authority/data-theme/ENER"},
    {"label": "GOVE", "uri": "http://publications.europa.eu/resource/authority/data-theme/GOVE"},
    {"label": "HEAL", "uri": "http://publications.europa.eu/resource/authority/data-theme/HEAL"},
    {"label": "INTR", "uri": "http://publications.europa.eu/resource/authority/data-theme/INTR"},
    {"label": "JUST", "uri": "http://publications.europa.eu/resource/authority/data-theme/JUST"},
    {"label": "REGI", "uri": "http://publications.europa.eu/resource/authority/data-theme/REGI"},
    {"label": "SOCI", "uri": "http://publications.europa.eu/resource/authority/data-theme/SOCI"},
    {"label": "TECH", "uri": "http://publications.europa.eu/resource/authority/data-theme/TECH"},
    {"label": "TRAN", "uri": "http://publications.europa.eu/resource/authority/data-theme/TRAN"},
]
