"""Database query command for executing SQL queries and outputting results."""

import csv
import logging
import sys
from typing import Optional

import orjson

from ..common.db_connection import DatabaseConnectionError, get_db_connection, parse_db_uri
from ..common.errors import DatabaseError, ValidationError

logger = logging.getLogger(__name__)


class DatabaseQueryExecutor:
    """Execute SQL queries against databases and output results."""

    def __init__(self):
        pass

    def query(
        self,
        query: str,
        db_uri: str,
        output: Optional[str] = None,
        output_format: str = "jsonl",
        batch_size: int = 10000,
    ):
        """Execute SQL query and output results.

        Args:
            query: SQL query string
            db_uri: Database connection URI
            output: Output file path (None for stdout)
            output_format: Output format ('jsonl', 'csv', 'parquet')
            batch_size: Batch size for streaming results
        """
        # Parse database URI
        try:
            db_type, params = parse_db_uri(db_uri)
        except DatabaseConnectionError as e:
            raise DatabaseError(f"Invalid database URI: {e}", connection_uri=db_uri) from e

        # Get database connection
        try:
            conn = get_db_connection(db_type, params)
        except DatabaseConnectionError as e:
            raise DatabaseError(
                f"Failed to connect to database: {e}", db_type=db_type, connection_uri=db_uri
            ) from e
        except ImportError as e:
            from ..common.errors import DependencyError

            package_map = {"postgresql": "psycopg2-binary", "mysql": "pymysql"}
            package = package_map.get(db_type, db_type)
            raise DependencyError(
                package,
                feature=f"{db_type} database support",
                install_command=f"pip install {package}",
            ) from e

        try:
            # Execute query
            cursor = conn.cursor()

            # For PostgreSQL, use server-side cursor for streaming
            cursor = None
            if db_type == "postgresql":
                try:
                    import psycopg2.extras

                    # Use named cursor for server-side streaming
                    cursor = conn.cursor(
                        name="undatum_cursor", cursor_factory=psycopg2.extras.RealDictCursor
                    )
                except Exception:
                    # Fallback to regular cursor with DictCursor
                    try:
                        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    except Exception:
                        # Final fallback to regular cursor
                        cursor = conn.cursor()
            else:
                # For MySQL and SQLite, use regular cursor
                cursor = conn.cursor()

            logger.info(f"Executing query against {db_type} database...")
            try:
                cursor.execute(query)
            except Exception as e:
                raise DatabaseError(
                    f"Query execution failed: {e}", db_type=db_type, connection_uri=db_uri
                ) from e

            # Get column names
            columns = []
            if hasattr(cursor, "description") and cursor.description:
                columns = [desc[0] for desc in cursor.description]
            elif hasattr(cursor, "column_names"):
                columns = cursor.column_names
            elif db_type == "postgresql" and hasattr(cursor, "keys"):
                columns = cursor.keys()

            # If no columns from description, we'll infer from first row during output
            # For PostgreSQL named cursors, we need to handle this carefully

            # Open output file
            if output:
                out_file = open(output, "w", encoding="utf-8")
            else:
                out_file = sys.stdout

            try:
                # Output results based on format
                if output_format == "jsonl":
                    columns = self._output_jsonl(cursor, columns, out_file, batch_size)
                elif output_format == "csv":
                    columns = self._output_csv(cursor, columns, out_file, batch_size)
                elif output_format == "parquet":
                    columns = self._output_parquet(
                        cursor, columns, output or "output.parquet", batch_size
                    )
                else:
                    raise ValidationError(
                        f"Unsupported output format: '{output_format}'",
                        field="output_format",
                        suggestions=["jsonl", "csv", "parquet"],
                    )
            finally:
                if output:
                    out_file.close()

            cursor.close()

        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise
        finally:
            conn.close()

    def _output_jsonl(self, cursor, columns: list, out_file, batch_size: int) -> list:
        """Output results in JSONL format.

        Returns:
            List of column names (may be inferred from first row)
        """
        count = 0

        # Fetch results in batches
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                # Convert row to dictionary
                if isinstance(row, dict):
                    record = row
                    # Infer columns from first dict row if not available
                    if not columns:
                        columns = list(record.keys())
                else:
                    # Infer columns from first row if not available
                    if not columns:
                        columns = [f"column_{i}" for i in range(len(row))]

                    # Handle case where columns might not match row length
                    if len(columns) == len(row):
                        record = dict(zip(columns, row))
                    else:
                        # Use generic column names if mismatch
                        record = {f"column_{i}": val for i, val in enumerate(row)}

                # Write JSON line
                json_line = orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE).decode("utf-8")
                out_file.write(json_line)
                count += 1

        logger.info(f"Output {count} records in JSONL format")
        return columns

    def _output_csv(self, cursor, columns: list, out_file, batch_size: int) -> list:
        """Output results in CSV format.

        Returns:
            List of column names (may be inferred from first row)
        """
        count = 0
        header_written = False

        # Fetch results in batches
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                # Convert row to dictionary
                if isinstance(row, dict):
                    record = row
                    # Infer columns from first dict row if not available
                    if not columns:
                        columns = list(record.keys())
                else:
                    # Infer columns from first row if not available
                    if not columns:
                        columns = [f"column_{i}" for i in range(len(row))]

                    # Handle case where columns might not match row length
                    if len(columns) == len(row):
                        record = dict(zip(columns, row))
                    else:
                        # Use generic column names if mismatch
                        record = {f"column_{i}": val for i, val in enumerate(row)}

                # Write header on first row
                if not header_written:
                    writer = csv.DictWriter(out_file, fieldnames=columns)
                    writer.writeheader()
                    header_written = True

                writer.writerow(record)
                count += 1

        logger.info(f"Output {count} records in CSV format")
        return columns

    def _output_parquet(self, cursor, columns: list, output_path: str, batch_size: int) -> list:
        """Output results in Parquet format.

        Returns:
            List of column names (may be inferred from first row)
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for Parquet output. Install with: pip install pandas pyarrow"
            ) from e

        # Collect all rows (Parquet needs full dataset)
        all_rows = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                if isinstance(row, dict):
                    all_rows.append(row)
                    # Infer columns from first dict row if not available
                    if not columns:
                        columns = list(row.keys())
                else:
                    # Infer columns from first row if not available
                    if not columns:
                        columns = [f"column_{i}" for i in range(len(row))]
                    all_rows.append(
                        dict(
                            zip(columns, row)
                            if len(columns) == len(row)
                            else {f"column_{i}": val for i, val in enumerate(row)}
                        )
                    )

        # Convert to DataFrame and write Parquet
        df = pd.DataFrame(all_rows)
        df.to_parquet(output_path, index=False)

        logger.info(f"Output {len(all_rows)} records in Parquet format to {output_path}")
        return columns
