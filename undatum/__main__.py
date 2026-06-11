#!/usr/bin/env python
"""The main entry point. Invoke as `undatum' or `python -m undatum`.

This module provides the CLI entry point for the undatum package.
"""
import logging
import sys

from .common.errors import UndatumError, handle_command_error
from .core import app


def main():
    """Main entry point for the application.

    Handles the CLI invocation and graceful shutdown on keyboard interrupt.
    Also handles UndatumError exceptions for user-friendly error messages.
    """
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
    )
    try:
        app()
    except KeyboardInterrupt:
        print("Ctrl-C pressed. Aborting", file=sys.stderr)
        sys.exit(0)
    except UndatumError as e:
        exit_code = handle_command_error(e, verbose=False)
        sys.exit(exit_code)
    except Exception as e:
        # For other exceptions, try to format them nicely
        exit_code = handle_command_error(e, verbose=False)
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
