"""Database connection utilities for query and load operations."""

import logging
import sqlite3
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Error connecting to database."""

    pass


def parse_db_uri(uri: str) -> tuple[str, dict[str, any]]:
    """Parse database connection URI into database type and connection parameters.

    Args:
        uri: Database connection URI (e.g., postgresql://user:pass@host:port/db)

    Returns:
        Tuple of (db_type, connection_params)
        - db_type: 'postgresql', 'mysql', or 'sqlite'
        - connection_params: Dictionary of connection parameters

    Raises:
        DatabaseConnectionError: If URI format is invalid
    """
    if not uri:
        raise DatabaseConnectionError("Database URI is required")

    # Handle SQLite (simple path or sqlite:///path)
    if uri.startswith("sqlite:///"):
        db_path = uri[10:]  # Remove 'sqlite:///' prefix
        return "sqlite", {"path": db_path}
    elif uri.startswith("sqlite://"):
        db_path = uri[9:]  # Remove 'sqlite://' prefix
        if db_path == ":memory:":
            return "sqlite", {"path": ":memory:"}
        else:
            return "sqlite", {"path": db_path}
    elif not uri.startswith(("postgresql://", "postgres://", "mysql://")):
        if "://" in uri:
            # Explicit but unsupported scheme - don't silently treat as SQLite path
            scheme = uri.split("://", 1)[0]
            raise DatabaseConnectionError(f"Unsupported database type: {scheme}")
        # Assume SQLite if no protocol
        return "sqlite", {"path": uri}

    # Parse URI
    try:
        parsed = urlparse(uri)
    except Exception as e:
        raise DatabaseConnectionError(f"Invalid database URI format: {e}") from e

    scheme = parsed.scheme.lower()

    # Determine database type
    if scheme in ("postgresql", "postgres"):
        db_type = "postgresql"
    elif scheme == "mysql":
        db_type = "mysql"
    else:
        raise DatabaseConnectionError(f"Unsupported database type: {scheme}")

    # Extract connection parameters
    params = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port,
        "user": parsed.username,
        "password": parsed.password,
        "database": parsed.path.lstrip("/") if parsed.path else None,
    }

    # Parse query parameters
    query_params = parse_qs(parsed.query)
    for key, value_list in query_params.items():
        if len(value_list) == 1:
            params[key] = value_list[0]
        else:
            params[key] = value_list

    # Set default ports
    if not params["port"]:
        if db_type == "postgresql":
            params["port"] = 5432
        elif db_type == "mysql":
            params["port"] = 3306

    return db_type, params


def get_db_connection(db_type: str, params: dict[str, any]):
    """Get database connection based on type and parameters.

    Args:
        db_type: Database type ('postgresql', 'mysql', 'sqlite')
        params: Connection parameters dictionary

    Returns:
        Database connection object

    Raises:
        DatabaseConnectionError: If connection fails
        ImportError: If required database driver is not installed
    """
    if db_type == "postgresql":
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError(
                "psycopg2 is required for PostgreSQL support. Install with: pip install psycopg2-binary"
            ) from e

        try:
            conn = psycopg2.connect(
                host=params["host"],
                port=params["port"],
                user=params["user"],
                password=params["password"],
                database=params["database"],
                connect_timeout=params.get("connect_timeout", 30),
            )
            return conn
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to connect to PostgreSQL: {e}") from e

    elif db_type == "mysql":
        try:
            import pymysql
        except ImportError as e:
            raise ImportError(
                "pymysql is required for MySQL support. Install with: pip install pymysql"
            ) from e

        try:
            conn = pymysql.connect(
                host=params["host"],
                port=params["port"],
                user=params["user"],
                password=params["password"],
                database=params["database"],
                connect_timeout=params.get("connect_timeout", 30),
                local_infile=True,
            )
            return conn
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to connect to MySQL: {e}") from e

    elif db_type == "sqlite":
        try:
            if params["path"] == ":memory:":
                conn = sqlite3.connect(":memory:")
            else:
                conn = sqlite3.connect(params["path"])
            return conn
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to connect to SQLite: {e}") from e

    else:
        raise DatabaseConnectionError(f"Unsupported database type: {db_type}")
