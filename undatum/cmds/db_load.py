"""Database load command for simplified data loading to databases."""

import logging
from typing import Optional

from ..cmds.ingester import Ingester
from ..common.errors import DatabaseError, ValidationError

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Simplified interface for loading data to databases."""

    def __init__(self):
        self.ingester = Ingester()

    def load(
        self,
        input_file: str,
        db_uri: str,
        table: str,
        mode: str = "append",
        create_table: bool = False,
        upsert_key: Optional[str] = None,
        **options,
    ):
        """Load data from file to database table.

        Args:
            input_file: Path to input file
            db_uri: Database connection URI
            table: Table name
            mode: Load mode ('append', 'replace', 'upsert')
            create_table: Auto-create table from schema
            upsert_key: Key field(s) for upsert mode (comma-separated)
            **options: Additional options passed to ingester
        """
        # Parse database URI to determine type
        from ..common.db_connection import DatabaseConnectionError, parse_db_uri

        try:
            db_type, params = parse_db_uri(db_uri)
        except DatabaseConnectionError as e:
            raise DatabaseError(f"Invalid database URI: {e}", connection_uri=db_uri) from e
        except Exception as e:
            raise DatabaseError(f"Failed to parse database URI: {e}", connection_uri=db_uri) from e

        # Extract database name from params
        db_name = params.get("database")
        if not db_name and db_type == "sqlite":
            # For SQLite, database name is not needed - use empty string or None
            # The ingester will handle SQLite URIs directly
            db_name = ""
        elif not db_name:
            raise ValidationError(
                f"Database name is required for {db_type}. Provide it in the URI (e.g., postgresql://user:pass@host/dbname)",
                field="db_uri",
            )

        # Prepare options for ingester
        ingest_options = {
            "dbtype": db_type,
            "mode": mode,
            "create_table": create_table,
            "upsert_key": upsert_key,
            **options,
        }

        # Call ingester
        logger.info(f"Loading {input_file} to {db_type} database table {table}")
        self.ingester.ingest_single(input_file, db_uri, db_name, table, ingest_options)
