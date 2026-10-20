"""PyInstaller entry point.

``undatum/__main__.py`` uses package-relative imports, so it cannot be the
frozen script itself; this wrapper imports it as part of the package.
"""

import sys

if __name__ == "__main__":
    if sys.argv[1:] == ["--_undatum-repack-worker"]:
        from undatum.common.repack_worker import main

        raise SystemExit(main())
    else:
        from undatum.__main__ import main

        main()
