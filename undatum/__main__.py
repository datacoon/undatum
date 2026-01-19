#!/usr/bin/env python
"""The main entry point. Invoke as `undatum' or `python -m undatum`.

This module provides the CLI entry point for the undatum package.
"""
import sys

from .core import app


def main():
    """Main entry point for the application.

    Handles the CLI invocation and graceful shutdown on keyboard interrupt.
    """
    try:
        app()
    except KeyboardInterrupt:
        print("Ctrl-C pressed. Aborting")
    sys.exit(0)


if __name__ == '__main__':
    main()
