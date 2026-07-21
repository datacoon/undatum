"""Shared constants and base class for database ingesters."""

DUCKABLE_FILE_TYPES = ["parquet", "csv", "jsonl", "json", "jsonl.gz"]
# Keep in sync with undatum.constants.DUCKABLE_CODECS (gz + gzip aliases)
DUCKABLE_CODECS = ["gz", "gzip", "zst"]

DEFAULT_BATCH_SIZE = 1000
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1  # seconds

# PostgreSQL-specific defaults
POSTGRES_DEFAULT_BATCH_SIZE = 10000
POSTGRES_CONNECTION_POOL_SIZE = 5

# DuckDB-specific defaults
DUCKDB_DEFAULT_BATCH_SIZE = 50000

# MySQL-specific defaults
MYSQL_DEFAULT_BATCH_SIZE = 10000
MYSQL_CONNECTION_POOL_SIZE = 5

# SQLite-specific defaults
SQLITE_DEFAULT_BATCH_SIZE = 5000


class BasicIngester:
    """Base class for data ingestion.

    Provides the interface for database-specific ingester implementations.
    All ingester classes should inherit from this base class and implement
    the ingest() method.
    """

    def __init__(self):
        pass

    def ingest(self, batch):
        """Ingest a batch of records to the database.

        Args:
            batch: List of records (dictionaries) to ingest

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement ingest() method")
