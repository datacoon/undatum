#!/usr/bin/env python
"""The main entry point. Invoke as `datus' or `python -m datus'.

"""
import sys


def main():
    try:
        from .core import cli
        exit_status = cli()
    except KeyboardInterrupt:
        print("Ctrl-C pressed. Aborting")
#        from httpie.status import ExitStatus
#        exit_status = ExitStatus.ERROR_CTRL_C

#    sys.exit(exit_status.value)
    sys.exit(0)


if __name__ == '__main__':
    main()
