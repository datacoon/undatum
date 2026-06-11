"""Shared helpers for CLI modules."""

import logging

from rich.console import Console

console = Console()


def enable_verbose():
    """Enable verbose logging."""
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG
    )
