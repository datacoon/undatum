#!/usr/bin/env python
"""The main entry point. Invoke as `undatum' or `python -m undatum`.

This module provides the CLI entry point for the undatum package.
"""
import sys

from .core import app
from .common.errors import handle_command_error, UndatumError


def main():
    """Main entry point for the application.

    Handles the CLI invocation and graceful shutdown on keyboard interrupt.
    Also handles UndatumError exceptions for user-friendly error messages.
    """
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


if __name__ == '__main__':
    main()
