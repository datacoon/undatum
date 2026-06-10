# -*- coding: utf8 -*-
"""DuckDB connection configuration and factory."""
import logging
import os
import re
from typing import Optional

import duckdb


def parse_memory_size(memory_str: str) -> int:
    """Parse memory size string to bytes.

    Args:
        memory_str: Memory size string (e.g., '4GB', '512MB', '1024')

    Returns:
        Memory size in bytes

    Examples:
        >>> parse_memory_size('4GB')
        4294967296
        >>> parse_memory_size('512MB')
        536870912
        >>> parse_memory_size('1024')
        1024
    """
    if not memory_str:
        return 0

    # Remove whitespace and convert to lowercase
    memory_str = memory_str.strip().lower()

    # Match pattern: number followed by optional unit (GB, MB, KB, or bytes)
    match = re.match(r'^(\d+)(gb|mb|kb|b)?$', memory_str)
    if not match:
        raise ValueError(f"Invalid memory size format: {memory_str}")

    size = int(match.group(1))
    unit = match.group(2) or 'b'

    multipliers = {'gb': 1024 ** 3, 'mb': 1024 ** 2, 'kb': 1024, 'b': 1}
    return size * multipliers[unit]


def create_duckdb_connection(
    threads: Optional[int] = None,
    memory: Optional[str] = None,
    temp_dir: Optional[str] = None,
    database: Optional[str] = None,
) -> duckdb.DuckDBPyConnection:
    """Create a configured DuckDB connection.

    Args:
        threads: Number of threads (optional)
        memory: Memory limit as string (e.g., '4GB', '512MB') (optional)
        temp_dir: Temporary directory path (optional)
        database: Database path (default: ':memory:') (optional)

    Returns:
        Configured DuckDB connection
    """
    if database is None:
        database = ':memory:'

    conn = duckdb.connect(database=database)

    # Configure threads
    if threads is not None:
        if threads < 1:
            raise ValueError(f"Thread count must be >= 1, got {threads}")
        conn.execute(f"SET threads={threads}")
        logging.debug(f'DuckDB threads set to {threads}')

    # Configure memory limit
    if memory:
        try:
            memory_bytes = parse_memory_size(memory)
            if memory_bytes > 0:
                conn.execute(f"SET memory_limit='{memory_bytes}'")
                logging.debug(f'DuckDB memory limit set to {memory_bytes} bytes ({memory})')
        except ValueError as e:
            logging.warning(f'Invalid memory size format: {memory}, error: {e}')

    # Configure temp directory
    if temp_dir:
        if not os.path.exists(temp_dir):
            try:
                os.makedirs(temp_dir, exist_ok=True)
            except OSError as e:
                logging.warning(f'Could not create temp directory {temp_dir}: {e}')
        if os.path.exists(temp_dir) and os.path.isdir(temp_dir):
            # DuckDB uses temp_directory setting
            conn.execute(f"SET temp_directory='{temp_dir}'")
            logging.debug(f'DuckDB temp directory set to {temp_dir}')
        else:
            logging.warning(f'Temp directory does not exist or is not a directory: {temp_dir}')

    return conn


def get_duckdb_config_from_options(options: dict) -> dict:
    """Extract DuckDB configuration from options dictionary.

    Args:
        options: Options dictionary

    Returns:
        Dictionary with DuckDB configuration (threads, memory, temp_dir)
    """
    config = {}
    if 'duckdb_threads' in options:
        config['threads'] = options['duckdb_threads']
    if 'duckdb_memory' in options:
        config['memory'] = options['duckdb_memory']
    if 'duckdb_temp_dir' in options:
        config['temp_dir'] = options['duckdb_temp_dir']
    return config
