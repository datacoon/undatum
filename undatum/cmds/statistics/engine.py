"""Engine detection for statistics processing."""

import logging

from iterable.helpers.detect import detect_file_type

from ...constants import DUCKABLE_CODECS, DUCKABLE_FILE_TYPES


def _detect_engine(fromfile, engine, filetype):
    """Detect the appropriate engine for statistics processing.

    Args:
        fromfile: Path to input file
        engine: Engine preference ('auto', 'duckdb', or 'iterable')
        filetype: Optional file type override (if None, will be detected)

    Returns:
        Detected engine name: 'duckdb' or 'iterable'
    """
    compression = "raw"
    if filetype is None:
        ftype = detect_file_type(fromfile)
        if ftype["success"]:
            filetype = ftype["datatype"].id()
            if ftype["codec"] is not None:
                compression = ftype["codec"].id()
    logging.info(f"Stats engine detection: filetype={filetype}, compression={compression}")
    if engine == "auto":
        if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
            return "duckdb"
        return "iterable"
    return engine
