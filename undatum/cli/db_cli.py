"""CLI commands for database query and load."""

import logging
import sys
from typing import Annotated

import typer

from ..cmds.db_load import DatabaseLoader
from ..cmds.db_query import DatabaseQueryExecutor
from .common import enable_verbose

logger = logging.getLogger(__name__)

db_app = typer.Typer(help="Database query and load commands.")


@db_app.command(name="query")
def db_query(
    query: Annotated[str, typer.Argument(help="SQL query to execute.")],
    db: Annotated[
        str,
        typer.Option(help="Database connection URI (e.g., postgresql://user:pass@host:port/db)."),
    ],
    output: Annotated[
        str, typer.Option(help="Output file path. If not specified, prints to stdout.")
    ] = None,
    output_format: Annotated[
        str, typer.Option(help="Output format: 'jsonl' (default), 'csv', or 'parquet'.")
    ] = "jsonl",
    query_file: Annotated[
        str, typer.Option(help="Path to SQL query file (alternative to query argument).")
    ] = None,
    batch_size: Annotated[
        int, typer.Option(help="Batch size for streaming results (default: 10000).")
    ] = 10000,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Execute SQL query against database and output results.

    Supports PostgreSQL, MySQL/MariaDB, and SQLite databases.
    Results are streamed for efficient memory usage with large result sets.

    Examples:
        # Query PostgreSQL
        undatum db query "SELECT * FROM users LIMIT 100" --db postgresql://user:pass@host/db

        # Query MySQL and save to file
        undatum db query "SELECT name, email FROM customers" --db mysql://user:pass@host:3306/mydb --output results.jsonl

        # Query SQLite and output CSV
        undatum db query "SELECT * FROM data" --db sqlite:///path/to/db.db --output-format csv

        # Query from file
        undatum db query --query-file query.sql --db postgresql://user:pass@host/db
    """
    if verbose:
        enable_verbose()

    # Read query from file if provided
    if query_file:
        try:
            with open(query_file, encoding="utf-8") as f:
                query = f.read()
        except Exception as e:
            logger.error(f"Failed to read query file: {e}")
            sys.exit(1)

    executor = DatabaseQueryExecutor()
    try:
        executor.query(query, db, output, output_format, batch_size)
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


@db_app.command()
def load(
    input_file: Annotated[str, typer.Argument(help="Path to input file to load.")],
    db: Annotated[
        str,
        typer.Option(help="Database connection URI (e.g., postgresql://user:pass@host:port/db)."),
    ],
    table: Annotated[str, typer.Option(help="Target table name.")],
    mode: Annotated[
        str, typer.Option(help="Load mode: 'append' (default), 'replace', or 'upsert'.")
    ] = "append",
    create_table: Annotated[
        bool, typer.Option(help="Auto-create table from data schema if it doesn't exist.")
    ] = False,
    upsert_key: Annotated[
        str, typer.Option(help="Key field(s) for upsert mode (comma-separated).")
    ] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Load data from file to database table.

    Simplified interface for loading data to databases. Supports PostgreSQL, MySQL/MariaDB, and SQLite.
    This is a convenience wrapper around the ingest command with a cleaner syntax.

    Examples:
        # Load data to PostgreSQL (append mode)
        undatum db load data.parquet --db postgresql://user:pass@host/db --table users

        # Load with replace mode
        undatum db load data.csv --db mysql://user:pass@host:3306/mydb --table customers --mode replace

        # Load with upsert
        undatum db load data.jsonl --db postgresql://user:pass@host/db --table orders --mode upsert --upsert-key id

        # Auto-create table
        undatum db load data.parquet --db sqlite:///db.db --table new_table --create-table
    """
    if verbose:
        enable_verbose()

    loader = DatabaseLoader()
    try:
        loader.load(input_file, db, table, mode, create_table, upsert_key)
    except Exception as e:
        logger.error(f"Load operation failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)
