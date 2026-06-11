"""Database ingestion package.

Split from the former monolithic ``undatum/cmds/ingester.py``.
"""

from .base import (  # noqa: F401
    DEFAULT_BATCH_SIZE,
    DUCKABLE_CODECS,
    DUCKABLE_FILE_TYPES,
    DUCKDB_DEFAULT_BATCH_SIZE,
    INITIAL_RETRY_DELAY,
    MAX_RETRIES,
    MYSQL_CONNECTION_POOL_SIZE,
    MYSQL_DEFAULT_BATCH_SIZE,
    POSTGRES_CONNECTION_POOL_SIZE,
    POSTGRES_DEFAULT_BATCH_SIZE,
    SQLITE_DEFAULT_BATCH_SIZE,
    BasicIngester,
)
from .core import Ingester  # noqa: F401
from .duckdb_backend import DuckDBIngester  # noqa: F401
from .elastic import ElasticIngester  # noqa: F401
from .mongo import MongoIngester  # noqa: F401
from .mysql_backend import PYMYSQL_AVAILABLE, MySQLIngester  # noqa: F401
from .postgres import PSYCOPG2_AVAILABLE, PostgresIngester  # noqa: F401
from .sqlite_backend import SQLiteIngester  # noqa: F401
