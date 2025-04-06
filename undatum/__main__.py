# -*- coding: utf8 -*-
#!/usr/bin/env python
"""The main entry point. Invoke as `undatum' or `python -m undatum`.

"""
import sys


def main():
    try:
        from .core import app
        app()
    except KeyboardInterrupt:
        print("Ctrl-C pressed. Aborting")
    sys.exit(0)


if __name__ == '__main__':
    app()
