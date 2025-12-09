# -*- coding: utf8 -*-
#!/usr/bin/env python
"""The main entry point. Invoke as `undatum' or `python -m undatum`.

"""
import sys
from .core import app


def main():
    """Main entry point for the application."""
    try:
        app()
    except KeyboardInterrupt:
        print("Ctrl-C pressed. Aborting")
    sys.exit(0)


if __name__ == '__main__':
    main()
