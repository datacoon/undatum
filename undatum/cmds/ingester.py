"""Data ingestion module for databases."""

import csv
import io
import logging
import time

import duckdb
from elasticsearch import Elasticsearch
from iterable.helpers.detect import open_iterable
from pymongo import MongoClient
from tqdm import tqdm

# Optional PostgreSQL support
try:
    import psycopg2
    from psycopg2 import pool
    from psycopg2.extras import execute_values

    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    psycopg2 = None
    pool = None
    execute_values = None

# Optional MySQL support
try:
    import pymysql
    import pymysql.cursors

    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False
    pymysql = None

# SQLite is built-in
import sqlite3

ITERABLE_OPTIONS_KEYS = ["tagname", "delimiter", "encoding", "start_line", "page"]

DUCKABLE_FILE_TYPES = ["parquet", "csv", "jsonl", "json", "jsonl.gz"]
DUCKABLE_CODECS = ["gz", "zst"]


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


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


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


class ElasticIngester(BasicIngester):
    """Elasticsearch data ingester.

    Handles bulk ingestion of documents to Elasticsearch with retry logic
    and error handling. Uses connection pooling via Elasticsearch client.

    Args:
        uri: Elasticsearch connection URI
        api_key: API key for authentication
        search_index: Index name where documents will be indexed
        document_id: Field name in documents to use as document ID (default: "id")
        timeout: Connection timeout in seconds (default: 60)
    """

    def __init__(
        self, uri: str, api_key: str, search_index: str, document_id: str = "id", timeout: int = 60
    ):
        self.client = Elasticsearch(
            uri,
            api_key=api_key,
            verify_certs=False,
            ssl_show_warn=False,
            timeout=timeout,
            max_retries=10,
            retry_on_timeout=True,
        )
        self._index = search_index
        self._item_id = document_id

    def ingest(self, batch):
        """Ingest batch of documents to Elasticsearch with retry logic."""
        documents = []
        failed_docs = []

        # Build bulk operation documents, handling missing document IDs
        for doc in batch:
            if self._item_id not in doc:
                failed_docs.append(
                    {
                        "doc": doc,
                        "error": f"Missing required field '{self._item_id}' for document ID",
                    }
                )
                logging.warning(f"Document missing required field '{self._item_id}': {doc}")
                continue
            documents.append({"index": {"_index": self._index, "_id": doc[self._item_id]}})
            documents.append(doc)

        if not documents:
            if failed_docs:
                logging.error(
                    f"All {len(batch)} documents in batch failed validation (missing '{self._item_id}' field)"
                )
            return None

        # Retry logic with exponential backoff
        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.bulk(
                    operations=documents, pipeline="ent-search-generic-ingestion"
                )
                if result.get("errors"):
                    # Count and log individual errors from bulk response
                    error_items = [
                        r for r in result.get("items", []) if "error" in r.get("index", {})
                    ]
                    if error_items:
                        logging.warning(
                            f"Elasticsearch bulk operation had {len(error_items)} errors out of {len(batch)} documents"
                        )
                        for item in error_items[:5]:  # Log first 5 errors
                            error_info = item.get("index", {}).get("error", {})
                            logging.warning(f"  Error: {error_info}")
                return result
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    delay = INITIAL_RETRY_DELAY * (2**attempt)
                    logging.warning(
                        f"Elasticsearch bulk operation failed (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    logging.error(
                        f"Elasticsearch bulk operation failed after {MAX_RETRIES} attempts: {e}"
                    )
                    raise


class MongoIngester:
    """MongoDB data ingester.

    Handles bulk ingestion of documents to MongoDB with retry logic
    and error handling. Uses connection pooling via MongoClient.

    Args:
        uri: MongoDB connection URI
        db: Database name
        table: Collection name
        do_drop: If True, drop the collection before ingestion (default: False)
        timeout: Connection timeout in seconds (None uses default)
    """

    def __init__(self, uri, db, table, do_drop=False, timeout=None):
        # Use connection pooling (MongoClient manages pool automatically)
        if timeout and timeout > 0:
            self.client = MongoClient(uri, serverSelectionTimeoutMS=timeout * 1000)
        else:
            self.client = MongoClient(uri)
        self.db = self.client[db]
        if do_drop:
            self.db[table].drop()
        self.coll = self.db[table]

    def ingest(self, batch):
        """Ingest batch of documents to MongoDB with retry logic."""
        # Retry logic with exponential backoff
        last_exception = None
        for attempt in range(MAX_RETRIES):
            try:
                result = self.coll.insert_many(
                    batch, ordered=False
                )  # ordered=False for better error handling
                return result
            except Exception as e:
                last_exception = e
                if attempt < MAX_RETRIES - 1:
                    delay = INITIAL_RETRY_DELAY * (2**attempt)
                    logging.warning(
                        f"MongoDB insert_many failed (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    # On final failure, try to identify which documents failed
                    logging.error(f"MongoDB insert_many failed after {MAX_RETRIES} attempts: {e}")
                    # Try inserting one by one to identify problematic documents
                    failed_docs = []
                    for doc in batch:
                        try:
                            self.coll.insert_one(doc)
                        except Exception as doc_error:
                            failed_docs.append({"doc": doc, "error": str(doc_error)})
                            logging.error(f"Failed to insert document: {doc_error}")
                    if failed_docs:
                        logging.warning(
                            f"Failed to insert {len(failed_docs)} out of {len(batch)} documents"
                        )
                    raise last_exception from None


class PostgresIngester(BasicIngester):
    """PostgreSQL data ingester.

    Handles bulk ingestion of records to PostgreSQL with COPY FROM for maximum
    performance, upsert support, and schema management. Uses connection pooling
    for efficient connection management.

    Args:
        uri: PostgreSQL connection URI (postgresql://user:pass@host:port/db)
        db: Database name (overrides URI database if provided)
        table: Table name
        mode: Ingestion mode: 'append', 'replace', or 'upsert' (default: 'append')
        create_table: If True, auto-create table from data schema (default: False)
        upsert_key: Field name(s) to use for conflict resolution in upsert mode
        timeout: Connection timeout in seconds (None uses default)
        pool_size: Connection pool size (default: 5)
    """

    def __init__(
        self,
        uri,
        db,
        table,
        mode="append",
        create_table=False,
        upsert_key=None,
        timeout=None,
        pool_size=POSTGRES_CONNECTION_POOL_SIZE,
    ):
        if not PSYCOPG2_AVAILABLE:
            raise ImportError(
                "psycopg2 is required for PostgreSQL support. "
                "Install it with: pip install psycopg2-binary"
            )

        self.uri = uri
        self.db = db
        self.table = table
        self.mode = mode
        self.create_table = create_table
        self.upsert_key = upsert_key
        self.timeout = timeout
        self.pool_size = pool_size

        # Parse connection parameters from URI
        self.conn_params = self._parse_uri(uri)

        # Use db parameter if provided, otherwise use from URI
        database = db if db else self.conn_params.get("database")
        if not database:
            raise ValueError("Database name must be provided either in URI or as 'db' parameter")

        # Create connection pool
        try:
            self.pool = pool.ThreadedConnectionPool(
                1,
                pool_size,
                host=self.conn_params.get("host", "localhost"),
                port=self.conn_params.get("port", 5432),
                database=database,
                user=self.conn_params.get("user"),
                password=self.conn_params.get("password"),
                connect_timeout=timeout or 30,
            )
        except Exception as e:
            raise ConnectionError(f"Failed to create PostgreSQL connection pool: {e}") from e

        self._schema_created = False
        self._table_columns = None
        self._first_batch_processed = False

    def _parse_uri(self, uri):
        """Parse PostgreSQL connection URI into parameters."""
        # Simple URI parsing: postgresql://user:pass@host:port/database
        params = {}
        if uri.startswith("postgresql://") or uri.startswith("postgres://"):
            uri = uri.replace("postgresql://", "").replace("postgres://", "")
            if "@" in uri:
                auth, rest = uri.split("@", 1)
                if ":" in auth:
                    params["user"], params["password"] = auth.split(":", 1)
                else:
                    params["user"] = auth

            if "/" in rest:
                host_port, params["database"] = rest.rsplit("/", 1)
                if ":" in host_port:
                    params["host"], params["port"] = host_port.split(":")
                    params["port"] = int(params["port"])
                else:
                    params["host"] = host_port
            else:
                if ":" in rest:
                    params["host"], params["port"] = rest.split(":")
                    params["port"] = int(params["port"])
                else:
                    params["host"] = rest

        return params

    def _get_connection(self):
        """Get a connection from the pool."""
        return self.pool.getconn()

    def _put_connection(self, conn):
        """Return a connection to the pool."""
        self.pool.putconn(conn)

    def _infer_schema(self, batch):
        """Infer PostgreSQL schema from a sample batch of records.

        Analyzes multiple records to determine the best type for each column.
        """
        if not batch:
            return []

        # Analyze all records to determine types
        column_types = {}
        sample_size = min(len(batch), 100)  # Sample up to 100 records

        for record in batch[:sample_size]:
            for key, value in record.items():
                if key not in column_types:
                    column_types[key] = []
                column_types[key].append(value)

        schema = []
        for key, values in column_types.items():
            # Determine type from non-None values
            non_null_values = [v for v in values if v is not None]

            if not non_null_values:
                pg_type = "TEXT"  # Default if all nulls
            elif all(isinstance(v, bool) for v in non_null_values):
                pg_type = "BOOLEAN"
            elif all(isinstance(v, int) for v in non_null_values):
                pg_type = "BIGINT"
            elif all(isinstance(v, float) for v in non_null_values):
                pg_type = "DOUBLE PRECISION"
            elif all(isinstance(v, str) for v in non_null_values):
                # Try to detect date/timestamp patterns
                date_count = 0
                timestamp_count = 0
                for v in non_null_values[:10]:  # Check first 10
                    if len(v) == 10 and "-" in v and v.count("-") == 2:
                        date_count += 1
                    elif len(v) > 10 and ("T" in v or " " in v) and ("-" in v or ":" in v):
                        timestamp_count += 1

                if timestamp_count > date_count:
                    pg_type = "TIMESTAMP"
                elif date_count > 0:
                    pg_type = "DATE"
                else:
                    # Use VARCHAR with reasonable length estimate
                    max_len = max(len(str(v)) for v in non_null_values[:100])
                    if max_len > 255:
                        pg_type = "TEXT"
                    else:
                        pg_type = f"VARCHAR({min(max_len * 2, 1000)})"  # 2x buffer, max 1000
            else:
                # Mixed types, default to TEXT
                pg_type = "TEXT"

            schema.append((key, pg_type))

        return schema

    def _create_table(self, schema, conn):
        """Create table from inferred schema."""
        # Check if table exists
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
            """,
                (self.table,),
            )
            table_exists = cur.fetchone()[0]

        if table_exists:
            if self.mode == "replace":
                # Drop and recreate
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {self.table}")
                    conn.commit()
            else:
                # Table exists and not replacing, just validate schema matches
                logging.info(f"Table {self.table} already exists, skipping creation")
                self._schema_created = True
                self._table_columns = [col for col, _ in schema]
                return

        # Build CREATE TABLE statement
        columns = ", ".join([f'"{col}" {pg_type}' for col, pg_type in schema])
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.table} ({columns})"

        with conn.cursor() as cur:
            cur.execute(create_sql)
            conn.commit()

        self._schema_created = True
        self._table_columns = [col for col, _ in schema]
        logging.info(
            f"Created table {self.table} with schema: {', '.join([f'{col} {pg_type}' for col, pg_type in schema])}"
        )

    def _validate_schema(self, batch, conn):
        """Validate that batch schema matches table schema.

        Returns True if schema matches, False otherwise.
        Logs warnings for mismatches but allows continuation.
        """
        if not batch:
            return True

        # Get table columns
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """,
                (self.table,),
            )
            table_schema = {row[0]: row[1] for row in cur.fetchall()}

        if not table_schema:
            logging.warning(f"Table {self.table} does not exist or has no columns")
            return False

        # Check batch columns match
        batch_columns = set(batch[0].keys())
        table_columns = set(table_schema.keys())

        if batch_columns != table_columns:
            missing = batch_columns - table_columns
            extra = table_columns - batch_columns
            if missing:
                logging.warning(f"Batch columns not in table {self.table}: {missing}")
            if extra:
                logging.warning(f"Table columns not in batch: {extra}")
            # Allow continuation but warn
            return len(missing) == 0  # Only fail if batch has extra columns

        return True

    def _copy_from_csv(self, batch, conn):
        """Use COPY FROM for fast bulk loading."""
        if not batch:
            return

        # Get column names
        if self._table_columns:
            columns = self._table_columns
        else:
            columns = list(batch[0].keys())

        # Convert batch to CSV format in memory
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)

        for row in batch:
            # Handle None values as empty strings for CSV
            values = []
            for col in columns:
                val = row.get(col)
                if val is None:
                    values.append("")
                else:
                    values.append(str(val))
            writer.writerow(values)

        output.seek(0)

        # Use COPY FROM for maximum performance
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {self.table} ({', '.join([f'"{col}"' for col in columns])}) FROM STDIN WITH (FORMAT CSV)",
                output,
            )
            conn.commit()

    def _insert_with_upsert(self, batch, conn):
        """Use INSERT ... ON CONFLICT for upsert operations."""
        if not batch:
            return

        if not self.upsert_key:
            raise ValueError("upsert_key must be specified for upsert mode")

        columns = list(batch[0].keys())

        # Determine conflict keys
        if isinstance(self.upsert_key, str):
            conflict_keys = [self.upsert_key]
        else:
            conflict_keys = self.upsert_key

        # Build UPDATE clause for ON CONFLICT (update all columns except conflict keys)
        update_columns = [col for col in columns if col not in conflict_keys]
        if not update_columns:
            # If only conflict keys exist, use DO NOTHING
            update_clause = "DO NOTHING"
        else:
            update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])

        # Build conflict target
        conflict_target = ", ".join([f'"{key}"' for key in conflict_keys])

        # Check if table has unique constraint or primary key on conflict keys
        # For now, assume conflict keys form a unique constraint
        column_names = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        if update_clause == "DO NOTHING":
            sql = f"""
                INSERT INTO {self.table} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) DO NOTHING
            """
        else:
            sql = f"""
                INSERT INTO {self.table} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) DO UPDATE SET {update_clause}
            """

        values = [[row.get(col) for col in columns] for row in batch]

        with conn.cursor() as cur:
            execute_values(cur, sql, values, template=None, page_size=100)
            conn.commit()

    def _insert_batch(self, batch, conn):
        """Use multi-row INSERT for batch insertion."""
        if not batch:
            return

        columns = list(batch[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        column_names = ", ".join([f'"{col}"' for col in columns])

        sql = f"INSERT INTO {self.table} ({column_names}) VALUES ({placeholders})"
        values = [[row.get(col) for col in columns] for row in batch]

        with conn.cursor() as cur:
            execute_values(cur, sql, values, template=None, page_size=100)
            conn.commit()

    def ingest(self, batch):
        """Ingest batch of records to PostgreSQL with retry logic."""
        if not batch:
            return

        conn = None
        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                conn = self._get_connection()

                # Create table if needed
                if self.create_table and not self._schema_created:
                    schema = self._infer_schema(batch)
                    self._create_table(schema, conn)
                elif not self.create_table:
                    # Validate schema matches
                    if not self._validate_schema(batch, conn):
                        raise ValueError(f"Schema mismatch for table {self.table}")

                # Ingest based on mode
                if self.mode == "upsert":
                    self._insert_with_upsert(batch, conn)
                else:
                    # Use COPY FROM for best performance (append or replace)
                    # Truncate on first batch for replace mode
                    if self.mode == "replace" and not self._first_batch_processed:
                        with conn.cursor() as cur:
                            cur.execute(f"TRUNCATE TABLE {self.table}")
                            conn.commit()
                        self._first_batch_processed = True
                    self._copy_from_csv(batch, conn)

                self._put_connection(conn)
                return

            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                        self._put_connection(conn)
                    except Exception:
                        pass

                last_exception = e
                if attempt < MAX_RETRIES - 1:
                    delay = INITIAL_RETRY_DELAY * (2**attempt)
                    logging.warning(
                        f"PostgreSQL ingestion failed (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    logging.error(f"PostgreSQL ingestion failed after {MAX_RETRIES} attempts: {e}")
                    raise

        if last_exception:
            raise last_exception

    def close(self):
        """Close connection pool."""
        if hasattr(self, "pool") and self.pool:
            self.pool.closeall()


class DuckDBIngester(BasicIngester):
    """DuckDB data ingester.

    Handles bulk ingestion of records to DuckDB with COPY FROM for maximum
    performance, Appender API for streaming, and schema management. DuckDB is
    an embedded analytical database, so no separate server is required.

    Args:
        uri: DuckDB connection URI (duckdb:///path/to/db.db or duckdb:///:memory:)
        table: Table name
        mode: Ingestion mode: 'append', 'replace', or 'upsert' (default: 'append')
        create_table: If True, auto-create table from data schema (default: False)
        upsert_key: Field name(s) to use for conflict resolution in upsert mode
        use_appender: If True, use Appender API instead of COPY FROM (default: False)
    """

    def __init__(
        self, uri, table, mode="append", create_table=False, upsert_key=None, use_appender=False
    ):
        self.uri = uri
        self.table = table
        self.mode = mode
        self.create_table = create_table
        self.upsert_key = upsert_key
        self.use_appender = use_appender

        # Parse connection string
        db_path = self._parse_uri(uri)

        # Connect to DuckDB (file-based or in-memory)
        if db_path == ":memory:" or db_path is None:
            self.conn = duckdb.connect(":memory:")
            self.is_memory = True
        else:
            self.conn = duckdb.connect(db_path)
            self.is_memory = False

        self._schema_created = False
        self._table_columns = None
        self._first_batch_processed = False
        self._appender = None
        self._replace_per_call = True

    def _parse_uri(self, uri):
        """Parse DuckDB connection URI into database path.

        Supports:
        - duckdb:///path/to/db.db
        - duckdb:///:memory:
        - duckdb:///path/to/db.db (with absolute path)
        """
        if uri.startswith("duckdb:///"):
            path = uri.replace("duckdb:///", "")
            if path == ":memory:":
                return ":memory:"
            return path
        elif uri.startswith("duckdb://"):
            # Handle duckdb://:memory: format
            path = uri.replace("duckdb://", "")
            if path == ":memory:":
                return ":memory:"
            return path
        else:
            # Assume it's a direct path
            return uri

    def _infer_schema(self, batch):
        """Infer DuckDB schema from a sample batch of records.

        Analyzes multiple records to determine the best type for each column.
        DuckDB types: BOOLEAN, BIGINT, DOUBLE, VARCHAR, DATE, TIMESTAMP, etc.
        """
        if not batch:
            return []

        # Analyze all records to determine types
        column_types = {}
        sample_size = min(len(batch), 100)  # Sample up to 100 records

        for record in batch[:sample_size]:
            for key, value in record.items():
                if key not in column_types:
                    column_types[key] = []
                column_types[key].append(value)

        schema = []
        for key, values in column_types.items():
            # Determine type from non-None values
            non_null_values = [v for v in values if v is not None]

            if not non_null_values:
                duckdb_type = "VARCHAR"  # Default if all nulls
            elif all(isinstance(v, bool) for v in non_null_values):
                duckdb_type = "BOOLEAN"
            elif all(isinstance(v, int) for v in non_null_values):
                duckdb_type = "BIGINT"
            elif all(isinstance(v, float) for v in non_null_values):
                duckdb_type = "DOUBLE"
            elif all(isinstance(v, str) for v in non_null_values):
                # Try to detect date/timestamp patterns
                date_count = 0
                timestamp_count = 0
                for v in non_null_values[:10]:  # Check first 10
                    if len(v) == 10 and "-" in v and v.count("-") == 2:
                        date_count += 1
                    elif len(v) > 10 and ("T" in v or " " in v) and ("-" in v or ":" in v):
                        timestamp_count += 1

                if timestamp_count > date_count:
                    duckdb_type = "TIMESTAMP"
                elif date_count > 0:
                    duckdb_type = "DATE"
                else:
                    # Use VARCHAR
                    duckdb_type = "VARCHAR"
            else:
                # Mixed types, default to VARCHAR
                duckdb_type = "VARCHAR"

            schema.append((key, duckdb_type))

        return schema

    def _create_table(self, schema):
        """Create table from inferred schema."""
        # Check if table exists
        try:
            self.conn.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()
            table_exists = True
        except Exception:
            table_exists = False

        if table_exists:
            if self.mode == "replace":
                # Drop and recreate
                self.conn.execute(f"DROP TABLE IF EXISTS {self.table}")
            else:
                # Table exists and not replacing, just validate schema matches
                logging.info(f"Table {self.table} already exists, skipping creation")
                self._schema_created = True
                self._table_columns = [col for col, _ in schema]
                return

        # Build CREATE TABLE statement
        column_defs = [f'"{col}" {duckdb_type}' for col, duckdb_type in schema]
        constraints = []
        if self.mode == "upsert" and self.upsert_key:
            conflict_keys = [self.upsert_key] if isinstance(self.upsert_key, str) else self.upsert_key
            conflict_target = ", ".join([f'"{key}"' for key in conflict_keys])
            constraints.append(f"UNIQUE ({conflict_target})")
        columns = ", ".join(column_defs + constraints)
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.table} ({columns})"

        self.conn.execute(create_sql)

        self._schema_created = True
        self._table_columns = [col for col, _ in schema]
        logging.info(
            f"Created table {self.table} with schema: {', '.join([f'{col} {duckdb_type}' for col, duckdb_type in schema])}"
        )

    def _validate_schema(self, batch):
        """Validate that batch schema matches table schema.

        Returns True if schema matches, False otherwise.
        Logs warnings for mismatches but allows continuation.
        """
        if not batch:
            return True

        # Get table columns
        try:
            result = self.conn.execute(f"DESCRIBE {self.table}").fetchall()
            table_schema = {row[0]: row[1] for row in result}
        except Exception:
            logging.warning(f"Table {self.table} does not exist or cannot be described")
            return False

        if not table_schema:
            return False

        # Check batch columns match
        batch_columns = set(batch[0].keys())
        table_columns = set(table_schema.keys())

        if batch_columns != table_columns:
            missing = batch_columns - table_columns
            extra = table_columns - batch_columns
            if missing:
                logging.warning(f"Batch columns not in table {self.table}: {missing}")
            if extra:
                logging.warning(f"Table columns not in batch: {extra}")
            # Allow continuation but warn
            return len(missing) == 0  # Only fail if batch has extra columns

        return True

    def _copy_from_csv(self, batch):
        """Use batch INSERT for fast bulk loading (DuckDB optimized).

        Note: DuckDB's COPY FROM requires file paths, so for in-memory batches
        we use optimized executemany which is still very fast.
        """
        # For DuckDB, executemany with batch inserts is highly optimized
        # and performs similarly to COPY FROM for reasonable batch sizes
        self._insert_batch(batch)

    def _insert_batch(self, batch):
        """Use multi-row INSERT for batch insertion (DuckDB optimized)."""
        if not batch:
            return

        columns = list(batch[0].keys())
        column_names = ", ".join([f'"{col}"' for col in columns])

        # Build VALUES clause with placeholders
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT INTO {self.table} ({column_names}) VALUES ({placeholders})"

        # Prepare values
        values = [[row.get(col) for col in columns] for row in batch]

        # Use executemany for batch insertion
        self.conn.executemany(sql, values)

    def _insert_with_upsert(self, batch):
        """Use INSERT ... ON CONFLICT for upsert operations (DuckDB)."""
        if not batch:
            return

        if not self.upsert_key:
            raise ValueError("upsert_key must be specified for upsert mode")

        columns = list(batch[0].keys())

        # Determine conflict keys
        if isinstance(self.upsert_key, str):
            conflict_keys = [self.upsert_key]
        else:
            conflict_keys = self.upsert_key

        # Build UPDATE clause for ON CONFLICT
        update_columns = [col for col in columns if col not in conflict_keys]
        if not update_columns:
            # If only conflict keys exist, use DO NOTHING
            update_clause = "DO NOTHING"
        else:
            update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])

        # Build conflict target
        conflict_target = ", ".join([f'"{key}"' for key in conflict_keys])

        column_names = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["?" for _ in columns])

        if update_clause == "DO NOTHING":
            sql = f"""
                INSERT INTO {self.table} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) DO NOTHING
            """
        else:
            sql = f"""
                INSERT INTO {self.table} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) DO UPDATE SET {update_clause}
            """

        values = [[row.get(col) for col in columns] for row in batch]

        # Use executemany for batch upsert
        self.conn.executemany(sql, values)

    def _use_appender(self, batch):
        """Use DuckDB Appender API for streaming insertion."""
        if not batch:
            return

        # Get column names
        if self._table_columns:
            columns = self._table_columns
        else:
            columns = list(batch[0].keys())

        # Create appender if available
        if self._appender is None and hasattr(self.conn, "appender"):
            self._appender = self.conn.appender(self.table)

        if self._appender is not None:
            # Append each row
            for row in batch:
                values = [row.get(col) for col in columns]
                self._appender.append_row(values)
            return

        # Fallback: use DuckDB append(DataFrame) if available, otherwise batch insert
        try:
            import pandas as pd

            df = pd.DataFrame(batch, columns=columns)
            self.conn.append(self.table, df, by_name=True)
        except Exception:
            self._insert_batch(batch)

    def ingest(self, batch):
        """Ingest batch of records to DuckDB with retry logic."""
        if not batch:
            return

        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                # Create table if needed
                if self.create_table and not self._schema_created:
                    schema = self._infer_schema(batch)
                    self._create_table(schema)
                elif not self.create_table:
                    # Validate schema matches
                    if not self._validate_schema(batch):
                        raise ValueError(f"Schema mismatch for table {self.table}")

                # Ingest based on mode and method
                if self.use_appender:
                    self._use_appender(batch)
                elif self.mode == "upsert":
                    self._insert_with_upsert(batch)
                else:
                    # Use batch insert for best performance (append or replace)
                    # Truncate on first batch for replace mode
                    if self.mode == "replace":
                        if self._replace_per_call or not self._first_batch_processed:
                            self.conn.execute(f"DELETE FROM {self.table}")
                            if not self._replace_per_call:
                                self._first_batch_processed = True
                    self._insert_batch(batch)

                return

            except Exception as e:
                last_exception = e
                if attempt < MAX_RETRIES - 1:
                    delay = INITIAL_RETRY_DELAY * (2**attempt)
                    logging.warning(
                        f"DuckDB ingestion failed (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    logging.error(f"DuckDB ingestion failed after {MAX_RETRIES} attempts: {e}")
                    raise

        if last_exception:
            raise last_exception

    def close(self):
        """Close DuckDB connection and appender."""
        if self._appender:
            try:
                self._appender.close()
                self._appender = None
            except Exception:
                pass

        if hasattr(self, "conn") and self.conn:
            try:
                self.conn.close()
            except Exception:
                pass


class MySQLIngester(BasicIngester):
    """MySQL data ingester.

    Handles bulk ingestion of records to MySQL with LOAD DATA LOCAL INFILE for maximum
    performance, multi-row INSERT fallback, and upsert support. Uses connection pooling
    for efficient connection management.

    Args:
        uri: MySQL connection URI (mysql://user:pass@host:port/db)
        db: Database name
        table: Table name
        mode: Ingestion mode: 'append', 'replace', or 'upsert' (default: 'append')
        create_table: If True, auto-create table from data schema (default: False)
        upsert_key: Field name(s) to use for conflict resolution in upsert mode
        timeout: Connection timeout in seconds (None uses default)
        pool_size: Connection pool size (default: 5)
    """

    def __init__(
        self,
        uri,
        db,
        table,
        mode="append",
        create_table=False,
        upsert_key=None,
        timeout=None,
        pool_size=MYSQL_CONNECTION_POOL_SIZE,
    ):
        if not PYMYSQL_AVAILABLE:
            raise ImportError(
                "pymysql is required for MySQL support. Install it with: pip install pymysql"
            )

        self.uri = uri
        self.db = db
        self.table = table
        self.mode = mode
        self.create_table = create_table
        self.upsert_key = upsert_key
        self.timeout = timeout
        self.pool_size = pool_size

        # Parse connection parameters from URI
        self.conn_params = self._parse_uri(uri)

        # Store connection parameters for creating connections
        self.conn_kwargs = {
            "host": self.conn_params.get("host", "localhost"),
            "port": self.conn_params.get("port", 3306),
            "user": self.conn_params.get("user"),
            "password": self.conn_params.get("password"),
            "database": db,
            "connect_timeout": timeout or 30,
            "local_infile": True,  # Enable LOAD DATA LOCAL INFILE
        }

        # Simple connection pool (reuse connections)
        self.pool_size = pool_size
        self._connections = []

        self._schema_created = False
        self._table_columns = None
        self._first_batch_processed = False

    def _parse_uri(self, uri):
        """Parse MySQL connection URI into parameters."""
        params = {}
        if uri.startswith("mysql://") or uri.startswith("mysql+pymysql://"):
            uri = uri.replace("mysql://", "").replace("mysql+pymysql://", "")
            if "@" in uri:
                auth, rest = uri.split("@", 1)
                if ":" in auth:
                    params["user"], params["password"] = auth.split(":", 1)
                else:
                    params["user"] = auth

            if "/" in rest:
                host_port, params["database"] = rest.rsplit("/", 1)
                if ":" in host_port:
                    params["host"], params["port"] = host_port.split(":")
                    params["port"] = int(params["port"])
                else:
                    params["host"] = host_port
            else:
                if ":" in rest:
                    params["host"], params["port"] = rest.split(":")
                    params["port"] = int(params["port"])
                else:
                    params["host"] = rest

        return params

    def _get_connection(self):
        """Get a connection (create new, PyMySQL doesn't have built-in pool)."""
        try:
            return pymysql.connect(**self.conn_kwargs)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MySQL: {e}")

    def _infer_schema(self, batch):
        """Infer MySQL schema from a sample batch of records."""
        if not batch:
            return []

        column_types = {}
        sample_size = min(len(batch), 100)

        for record in batch[:sample_size]:
            for key, value in record.items():
                if key not in column_types:
                    column_types[key] = []
                column_types[key].append(value)

        schema = []
        for key, values in column_types.items():
            non_null_values = [v for v in values if v is not None]

            if not non_null_values:
                mysql_type = "TEXT"
            elif all(isinstance(v, bool) for v in non_null_values):
                mysql_type = "BOOLEAN"
            elif all(isinstance(v, int) for v in non_null_values):
                mysql_type = "BIGINT"
            elif all(isinstance(v, float) for v in non_null_values):
                mysql_type = "DOUBLE"
            elif all(isinstance(v, str) for v in non_null_values):
                max_len = max(len(str(v)) for v in non_null_values[:100])
                if max_len > 65535:
                    mysql_type = "LONGTEXT"
                elif max_len > 255:
                    mysql_type = "TEXT"
                else:
                    mysql_type = f"VARCHAR({min(max_len * 2, 255)})"
            else:
                mysql_type = "TEXT"

            schema.append((key, mysql_type))

        return schema

    def _create_table(self, schema, conn):
        """Create table from inferred schema."""
        with conn.cursor() as cur:
            cur.execute(f"SHOW TABLES LIKE '{self.table}'")
            table_exists = cur.fetchone() is not None

        if table_exists:
            if self.mode == "replace":
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {self.table}")
                    conn.commit()
            else:
                logging.info(f"Table {self.table} already exists, skipping creation")
                self._schema_created = True
                self._table_columns = [col for col, _ in schema]
                return

        columns = ", ".join([f"`{col}` {mysql_type}" for col, mysql_type in schema])
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.table} ({columns}) ENGINE=InnoDB"

        with conn.cursor() as cur:
            cur.execute(create_sql)
            conn.commit()

        self._schema_created = True
        self._table_columns = [col for col, _ in schema]
        logging.info(
            f"Created table {self.table} with schema: {', '.join([f'{col} {mysql_type}' for col, mysql_type in schema])}"
        )

    def _load_data_infile(self, batch, conn, temp_file):
        """Use LOAD DATA LOCAL INFILE for fast bulk loading."""
        if not batch:
            return

        columns = self._table_columns or list(batch[0].keys())
        column_names = ", ".join([f"`{col}`" for col in columns])

        load_sql = f"""
            LOAD DATA LOCAL INFILE '{temp_file}'
            INTO TABLE {self.table}
            FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\\\'
            LINES TERMINATED BY '\\n'
            ({column_names})
        """

        with conn.cursor() as cur:
            cur.execute(load_sql)
            conn.commit()

    def _insert_batch(self, batch, conn):
        """Use multi-row INSERT for batch insertion."""
        if not batch:
            return

        columns = list(batch[0].keys())
        column_names = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        sql = f"INSERT INTO {self.table} ({column_names}) VALUES ({placeholders})"
        values = [[row.get(col) for col in columns] for row in batch]

        with conn.cursor() as cur:
            cur.executemany(sql, values)
            conn.commit()

    def _insert_with_upsert(self, batch, conn):
        """Use INSERT ... ON DUPLICATE KEY UPDATE for upsert operations."""
        if not batch:
            return

        if not self.upsert_key:
            raise ValueError("upsert_key must be specified for upsert mode")

        columns = list(batch[0].keys())
        conflict_keys = [self.upsert_key] if isinstance(self.upsert_key, str) else self.upsert_key

        update_columns = [col for col in columns if col not in conflict_keys]
        if not update_columns:
            # Use INSERT IGNORE if no columns to update
            column_names = ", ".join([f"`{col}`" for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT IGNORE INTO {self.table} ({column_names}) VALUES ({placeholders})"
        else:
            update_clause = ", ".join([f"`{col}` = VALUES(`{col}`)" for col in update_columns])
            column_names = ", ".join([f"`{col}`" for col in columns])
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"""
                INSERT INTO {self.table} ({column_names})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """

        values = [[row.get(col) for col in columns] for row in batch]

        with conn.cursor() as cur:
            cur.executemany(sql, values)
            conn.commit()

    def ingest(self, batch):
        """Ingest batch of records to MySQL with retry logic."""
        if not batch:
            return

        conn = None
        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                conn = self._get_connection()

                # Create table if needed
                if self.create_table and not self._schema_created:
                    schema = self._infer_schema(batch)
                    self._create_table(schema, conn)

                # Ingest based on mode
                if self.mode == "upsert":
                    self._insert_with_upsert(batch, conn)
                else:
                    # Use multi-row INSERT (LOAD DATA requires file, which is complex)
                    if self.mode == "replace" and not self._first_batch_processed:
                        with conn.cursor() as cur:
                            cur.execute(f"TRUNCATE TABLE {self.table}")
                            conn.commit()
                        self._first_batch_processed = True
                    self._insert_batch(batch, conn)

                conn.close()
                return

            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                        conn.close()
                    except Exception:
                        pass

                last_exception = e
                if attempt < MAX_RETRIES - 1:
                    delay = INITIAL_RETRY_DELAY * (2**attempt)
                    logging.warning(
                        f"MySQL ingestion failed (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    logging.error(f"MySQL ingestion failed after {MAX_RETRIES} attempts: {e}")
                    raise

        if last_exception:
            raise last_exception

    def close(self):
        """Close any remaining connections."""
        # Connections are closed after each use, so nothing to do here
        pass


class SQLiteIngester(BasicIngester):
    """SQLite data ingester.

    Handles bulk ingestion of records to SQLite with PRAGMA optimizations for maximum
    performance, executemany for batch inserts, and upsert support. SQLite is built-in
    to Python, so no external dependencies are required.

    Args:
        uri: SQLite connection URI (sqlite:///path/to/db.db or sqlite:///:memory:)
        table: Table name
        mode: Ingestion mode: 'append', 'replace', or 'upsert' (default: 'append')
        create_table: If True, auto-create table from data schema (default: False)
        upsert_key: Field name(s) to use for conflict resolution in upsert mode
    """

    def __init__(self, uri, table, mode="append", create_table=False, upsert_key=None):
        self.uri = uri
        self.table = table
        self.mode = mode
        self.create_table = create_table
        self.upsert_key = upsert_key

        # Parse connection string
        db_path = self._parse_uri(uri)

        # Connect to SQLite
        if db_path == ":memory:" or db_path is None:
            self.conn = sqlite3.connect(":memory:", check_same_thread=False)
            self.is_memory = True
        else:
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.is_memory = False

        # Apply PRAGMA optimizations for bulk loading
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA cache_size = 10000")
        self.conn.execute("PRAGMA temp_store = MEMORY")

        self._schema_created = False
        self._table_columns = None
        self._first_batch_processed = False
        self._original_pragmas = {}
        self._replace_per_call = True

    def _parse_uri(self, uri):
        """Parse SQLite connection URI into database path."""
        if uri.startswith("sqlite:///"):
            path = uri.replace("sqlite:///", "")
            if path == ":memory:":
                return ":memory:"
            return path
        elif uri.startswith("sqlite://"):
            path = uri.replace("sqlite://", "")
            if path == ":memory:":
                return ":memory:"
            return path
        else:
            return uri

    def _infer_schema(self, batch):
        """Infer SQLite schema from a sample batch of records."""
        if not batch:
            return []

        column_types = {}
        sample_size = min(len(batch), 100)

        for record in batch[:sample_size]:
            for key, value in record.items():
                if key not in column_types:
                    column_types[key] = []
                column_types[key].append(value)

        schema = []
        for key, values in column_types.items():
            non_null_values = [v for v in values if v is not None]

            if not non_null_values:
                sqlite_type = "TEXT"
            elif all(isinstance(v, bool) for v in non_null_values):
                sqlite_type = "INTEGER"  # SQLite uses INTEGER for booleans
            elif all(isinstance(v, int) for v in non_null_values):
                sqlite_type = "INTEGER"
            elif all(isinstance(v, float) for v in non_null_values):
                sqlite_type = "REAL"
            elif all(isinstance(v, str) for v in non_null_values):
                sqlite_type = "TEXT"
            else:
                sqlite_type = "TEXT"

            schema.append((key, sqlite_type))

        return schema

    def _create_table(self, schema):
        """Create table from inferred schema."""
        # Check if table exists
        try:
            result = self.conn.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table}'"
            ).fetchone()
            table_exists = result is not None
        except Exception:
            table_exists = False

        if table_exists:
            if self.mode == "replace":
                self.conn.execute(f"DROP TABLE IF EXISTS {self.table}")
            else:
                logging.info(f"Table {self.table} already exists, skipping creation")
                self._schema_created = True
                self._table_columns = [col for col, _ in schema]
                return

        column_defs = [f'"{col}" {sqlite_type}' for col, sqlite_type in schema]
        constraints = []
        if self.mode == "upsert" and self.upsert_key:
            conflict_keys = [self.upsert_key] if isinstance(self.upsert_key, str) else self.upsert_key
            conflict_target = ", ".join([f'"{key}"' for key in conflict_keys])
            constraints.append(f"UNIQUE ({conflict_target})")
        columns = ", ".join(column_defs + constraints)
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.table} ({columns})"

        self.conn.execute(create_sql)
        self.conn.commit()

        self._schema_created = True
        self._table_columns = [col for col, _ in schema]
        logging.info(
            f"Created table {self.table} with schema: {', '.join([f'{col} {sqlite_type}' for col, sqlite_type in schema])}"
        )

    def _insert_batch(self, batch):
        """Use executemany for batch insertion."""
        if not batch:
            return

        columns = list(batch[0].keys())
        column_names = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["?" for _ in columns])

        sql = f"INSERT INTO {self.table} ({column_names}) VALUES ({placeholders})"
        values = [tuple(row.get(col) for col in columns) for row in batch]

        self.conn.executemany(sql, values)
        self.conn.commit()

    def _insert_with_upsert(self, batch):
        """Use INSERT ... ON CONFLICT for upsert operations (SQLite 3.24+)."""
        if not batch:
            return

        if not self.upsert_key:
            raise ValueError("upsert_key must be specified for upsert mode")

        columns = list(batch[0].keys())
        conflict_keys = [self.upsert_key] if isinstance(self.upsert_key, str) else self.upsert_key

        update_columns = [col for col in columns if col not in conflict_keys]
        if not update_columns:
            # Use DO NOTHING if no columns to update
            column_names = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join(["?" for _ in columns])
            conflict_target = ", ".join([f'"{key}"' for key in conflict_keys])
            sql = f"""
                INSERT INTO {self.table} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) DO NOTHING
            """
        else:
            update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
            column_names = ", ".join([f'"{col}"' for col in columns])
            placeholders = ", ".join(["?" for _ in columns])
            conflict_target = ", ".join([f'"{key}"' for key in conflict_keys])
            sql = f"""
                INSERT INTO {self.table} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_target}) DO UPDATE SET {update_clause}
            """

        values = [tuple(row.get(col) for col in columns) for row in batch]

        self.conn.executemany(sql, values)
        self.conn.commit()

    def ingest(self, batch):
        """Ingest batch of records to SQLite with retry logic."""
        if not batch:
            return

        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                # Create table if needed
                if self.create_table and not self._schema_created:
                    schema = self._infer_schema(batch)
                    self._create_table(schema)

                # Ingest based on mode
                if self.mode == "upsert":
                    self._insert_with_upsert(batch)
                else:
                    # Use batch insert
                    if self.mode == "replace":
                        if self._replace_per_call or not self._first_batch_processed:
                            self.conn.execute(f"DELETE FROM {self.table}")
                            self.conn.commit()
                            if not self._replace_per_call:
                                self._first_batch_processed = True
                    self._insert_batch(batch)

                return

            except Exception as e:
                last_exception = e
                if attempt < MAX_RETRIES - 1:
                    delay = INITIAL_RETRY_DELAY * (2**attempt)
                    logging.warning(
                        f"SQLite ingestion failed (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    logging.error(f"SQLite ingestion failed after {MAX_RETRIES} attempts: {e}")
                    raise

        if last_exception:
            raise last_exception

    def close(self):
        """Close SQLite connection."""
        if hasattr(self, "conn") and self.conn:
            try:
                self.conn.close()
            except Exception:
                pass


class Ingester:
    """Main data ingestion handler.

    Coordinates file reading, batching, and database-specific ingestion.
    Supports multiple files and provides progress tracking and error handling.

    Args:
        batch_size: Number of records per batch (default: 1000)
    """

    def __init__(self, batch_size=DEFAULT_BATCH_SIZE):
        self.batch_size = batch_size

    def ingest(self, fromfiles, uri, db, table, options=None):
        """Ingest data from multiple files to a database.

        Args:
            fromfiles: List of file paths or glob patterns
            uri: Database connection URI
            db: Database name
            table: Collection or table name
            options: Dictionary of ingestion options (dbtype, drop, timeout, etc.)
        """
        if options is None:
            options = {}
        for filename in fromfiles:
            self.ingest_single(filename, uri, db, table, options)

    def ingest_single(self, fromfile, uri, db, table, options=None):
        """Loads single file data contents to a database.

        Processes a single file, reading records in batches and ingesting them
        to the specified database. Provides progress tracking, error handling,
        and summary statistics.

        Args:
            fromfile: Path to input file
            uri: Database connection URI
            db: Database name
            table: Collection or table name
            options: Dictionary of ingestion options:
                - dbtype: Database type ('mongodb', 'elasticsearch', 'elastic', 'postgresql', 'postgres', 'duckdb', 'mysql', 'sqlite')
                - drop: Drop collection/table before ingestion (bool)
                - timeout: Connection timeout in seconds (int, positive values)
                - skip: Number of records to skip (int)
                - totals: Show total record counts (bool)
                - api_key: API key for authentication (str, Elasticsearch)
                - doc_id: Document ID field name (str, Elasticsearch, default: 'id')

        Raises:
            ValueError: If unsupported database type is specified
            ConnectionError: If database connection fails
        """
        if options is None:
            options = {}
        dbtype = options["dbtype"]
        processor = None
        totals = -1
        skip = options.get("skip")
        use_totals = options.get("totals", False)
        do_drop = options.get("drop", False)  # Fixed typo: was 'dro[]'
        timeout = options.get("timeout")

        # Convert timeout: negative values mean use default, positive values are seconds
        if timeout and timeout > 0:
            timeout_seconds = timeout
        else:
            timeout_seconds = None

        logging.info(f"Ingesting {fromfile} to {uri} with db {db} table {table}")

        # Calculate total records for progress bar
        if use_totals:
            parts = fromfile.rsplit(".", 2)
            if len(parts) == 2:
                if parts[-1].lower() in DUCKABLE_FILE_TYPES:
                    try:
                        totals = duckdb.sql(f"select count(*) from '{fromfile}'").fetchone()[0]
                    except Exception as e:
                        logging.warning(f"Could not count records in {fromfile}: {e}")
            elif len(parts) == 3:
                if (
                    parts[-2].lower() in DUCKABLE_FILE_TYPES
                    and parts[-1].lower() in DUCKABLE_CODECS
                ):
                    try:
                        totals = duckdb.sql(f"select count(*) from '{fromfile}'").fetchone()[0]
                    except Exception as e:
                        logging.warning(f"Could not count records in {fromfile}: {e}")

        # Initialize processor with timeout support
        if dbtype == "mongodb":
            processor = MongoIngester(uri, db, table, do_drop=do_drop, timeout=timeout_seconds)
        elif dbtype == "elastic" or dbtype == "elasticsearch":
            api_key = options.get("api_key")
            id_key = options.get("doc_id", "id")
            processor = ElasticIngester(
                uri=uri,
                api_key=api_key,
                search_index=table,
                document_id=id_key,
                timeout=timeout_seconds or 60,
            )
        elif dbtype == "postgresql" or dbtype == "postgres":
            mode = options.get("mode", "append")
            create_table = options.get("create_table", False)
            upsert_key = options.get("upsert_key")
            pool_size = options.get("pool_size", POSTGRES_CONNECTION_POOL_SIZE)
            # For replace mode, set drop equivalent
            if mode == "replace" and not create_table:
                do_drop = True  # Will be handled by truncate in first batch
            processor = PostgresIngester(
                uri=uri,
                db=db,
                table=table,
                mode=mode,
                create_table=create_table,
                upsert_key=upsert_key,
                timeout=timeout_seconds,
                pool_size=pool_size,
            )
        elif dbtype == "duckdb":
            mode = options.get("mode", "append")
            create_table = options.get("create_table", False)
            upsert_key = options.get("upsert_key")
            use_appender = options.get("use_appender", False)
            processor = DuckDBIngester(
                uri=uri,
                table=table,
                mode=mode,
                create_table=create_table,
                upsert_key=upsert_key,
                use_appender=use_appender,
            )
        elif dbtype == "mysql":
            mode = options.get("mode", "append")
            create_table = options.get("create_table", False)
            upsert_key = options.get("upsert_key")
            pool_size = options.get("pool_size", MYSQL_CONNECTION_POOL_SIZE)
            processor = MySQLIngester(
                uri=uri,
                db=db,
                table=table,
                mode=mode,
                create_table=create_table,
                upsert_key=upsert_key,
                timeout=timeout_seconds,
                pool_size=pool_size,
            )
        elif dbtype == "sqlite":
            mode = options.get("mode", "append")
            create_table = options.get("create_table", False)
            upsert_key = options.get("upsert_key")
            processor = SQLiteIngester(
                uri=uri, table=table, mode=mode, create_table=create_table, upsert_key=upsert_key
            )
        else:
            raise ValueError(f"Unsupported database type: {dbtype}")

        if hasattr(processor, "_replace_per_call"):
            processor._replace_per_call = False
            processor._first_batch_processed = False

        # Validate connection before starting
        try:
            if dbtype == "mongodb":
                # Test MongoDB connection
                processor.client.server_info()
            elif dbtype == "elastic" or dbtype == "elasticsearch":
                # Test Elasticsearch connection
                processor.client.info()
            elif dbtype == "postgresql" or dbtype == "postgres":
                # Test PostgreSQL connection
                conn = processor._get_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                finally:
                    processor._put_connection(conn)
            elif dbtype == "duckdb":
                # Test DuckDB connection (simple query)
                processor.conn.execute("SELECT 1").fetchone()
            elif dbtype == "mysql":
                # Test MySQL connection
                conn = processor._get_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                finally:
                    conn.close()
            elif dbtype == "sqlite":
                # Test SQLite connection (simple query)
                processor.conn.execute("SELECT 1").fetchone()
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

        iterableargs = get_iterable_options(options)
        it_in = open_iterable(fromfile, mode="r", iterableargs=iterableargs)

        # Statistics tracking
        start_time = time.time()
        total_rows = 0
        successful_rows = 0
        failed_rows = 0
        batch_count = 0
        errors = []

        try:
            logging.info(f"Ingesting data: filename {fromfile}, uri: {uri}, db {db}, table {table}")
            n = 0
            batch = []

            # Enhanced progress bar with throughput
            with tqdm(it_in, total=totals, desc=f"Ingesting to {dbtype}", unit="rows") as pbar:
                for row in pbar:
                    n += 1
                    if skip is not None and skip > 0:
                        if n < skip:
                            continue

                    batch.append(row)
                    total_rows += 1

                    if len(batch) >= self.batch_size:
                        batch_count += 1
                        try:
                            processor.ingest(batch)
                            successful_rows += len(batch)
                            # Update progress bar with throughput
                            elapsed = time.time() - start_time
                            if elapsed > 0:
                                throughput = successful_rows / elapsed
                                pbar.set_postfix({"throughput": f"{throughput:.0f} rows/s"})
                        except Exception as e:
                            failed_rows += len(batch)
                            error_msg = f"Batch {batch_count} failed: {e}"
                            errors.append(error_msg)
                            logging.error(error_msg)
                            # Continue with next batch
                        batch = []

                # Process remaining batch
                if len(batch) > 0:
                    batch_count += 1
                    try:
                        processor.ingest(batch)
                        successful_rows += len(batch)
                    except Exception as e:
                        failed_rows += len(batch)
                        error_msg = f"Final batch {batch_count} failed: {e}"
                        errors.append(error_msg)
                        logging.error(error_msg)

        finally:
            it_in.close()

            # Close database connections if needed
            if dbtype in ("postgresql", "postgres", "duckdb", "mysql", "sqlite") and hasattr(
                processor, "close"
            ):
                try:
                    processor.close()
                except Exception as e:
                    logging.warning(f"Error closing database connection: {e}")

            # Print summary statistics
            elapsed_time = time.time() - start_time
            print("\nIngestion Summary:")
            print(f"  Total rows processed: {total_rows}")
            print(f"  Successful rows: {successful_rows}")
            print(f"  Failed rows: {failed_rows}")
            print(f"  Batches processed: {batch_count}")
            print(f"  Time elapsed: {elapsed_time:.2f} seconds")
            if elapsed_time > 0:
                print(f"  Average throughput: {successful_rows / elapsed_time:.0f} rows/second")
            if errors:
                print(f"  Errors encountered: {len(errors)}")
                if logging.getLogger().level <= logging.DEBUG:
                    for error in errors:
                        print(f"    - {error}")
