"""Utility functions for file operations and data processing.

This module provides helper functions for encoding detection, delimiter detection,
file type identification, dictionary manipulation, and data type guessing.
"""
from collections import OrderedDict
from typing import Any, Optional, Union

import chardet

from .constants import DEFAULT_OPTIONS, SUPPORTED_FILE_TYPES


def detect_encoding(filename: str, limit: int = 1000000) -> dict[str, Any]:
    """Detect encoding of a file.

    Args:
        filename: Path to the file to analyze.
        limit: Maximum number of bytes to read for detection (default: 1000000).

    Returns:
        Dictionary with encoding detection results from chardet.
    """
    with open(filename, 'rb') as f:
        chunk = f.read(limit)
    detected = chardet.detect(chunk)
    return detected


def detect_delimiter(filename: str, encoding: str = 'utf8') -> str:
    """Detect delimiter used in a CSV-like file.

    Args:
        filename: Path to the CSV file to analyze.
        encoding: File encoding (default: 'utf8').

    Returns:
        Most likely delimiter character (',', ';', '\\t', or '|').
    """
    with open(filename, encoding=encoding) as f:
        line = f.readline()
    dict1 = {',': line.count(','), ';': line.count(';'),
             '\t': line.count('\t'), '|': line.count('|')}
    delimiter = max(dict1, key=dict1.get)
    return delimiter


def get_file_type(filename: str) -> Optional[str]:
    """Get file type based on extension.

    Args:
        filename: Path to the file.

    Returns:
        File extension if supported, None otherwise.
    """
    ext = filename.rsplit('.', 1)[-1].lower()
    if ext in SUPPORTED_FILE_TYPES:
        return ext
    return None


def get_option(options: dict[str, Any], name: str) -> Any:
    """Get option value from options dict or default options.

    Args:
        options: Dictionary of user-provided options.
        name: Option name to retrieve.

    Returns:
        Option value if found, None otherwise.
    """
    if name in options:
        return options[name]
    if name in DEFAULT_OPTIONS:
        return DEFAULT_OPTIONS[name]
    return None

def get_dict_value(d: Union[dict[str, Any], list[dict[str, Any]], None], keys: list[str]) -> list[Any]:
    """Get dictionary value by nested keys.

    Args:
        d: Dictionary or list of dictionaries to search.
        keys: List of nested keys to traverse.

    Returns:
        List of values found at the specified key path.
    """
    out = []
    if d is None:
        return out
    if len(keys) == 1:
        if isinstance(d, (dict, OrderedDict)):
            if keys[0] in d:
                out.append(d[keys[0]])
        else:
            for r in d:
                if r and keys[0] in r:
                    out.append(r[keys[0]])
    else:
        if isinstance(d, (dict, OrderedDict)):
            if keys[0] in d:
                out.extend(get_dict_value(d[keys[0]], keys[1:]))
        else:
            for r in d:
                if keys[0] in r:
                    out.extend(get_dict_value(r[keys[0]], keys[1:]))
    return out


def strip_dict_fields(record: dict[str, Any], fields: list[list[str]], startkey: int = 0) -> dict[str, Any]:
    """Strip dictionary fields based on field list.

    Args:
        record: Dictionary to process.
        fields: List of field paths (nested keys as lists).
        startkey: Starting index for field path (default: 0).

    Returns:
        Modified dictionary with only specified fields retained.
    """
    # Create set for O(1) lookup instead of O(n) list lookup
    localf = set()
    for field in fields:
        if len(field) > startkey:
            localf.add(field[startkey])
    # Iterate over copy of keys to avoid modification during iteration
    keys = list(record.keys())
    for k in keys:
        if k not in localf:
            del record[k]

    for k in record:
        if isinstance(record[k], dict):
            record[k] = strip_dict_fields(record[k], fields, startkey + 1)
    return record


def dict_generator(indict: Union[dict[str, Any], Any], pre: Optional[list[str]] = None):
    """Process dictionary and yield flattened key-value pairs.

    Recursively traverses nested dictionaries and lists, yielding
    key paths with their values. Skips '_id' keys.

    Args:
        indict: Input dictionary to process.
        pre: Prefix keys list for nested structures (default: None).

    Yields:
        Lists containing key path and value: [key1, key2, ..., value]
    """
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in indict.items():  # Use dict.items() directly, no list() conversion
            if key == "_id":
                continue
            if isinstance(value, dict):
                yield from dict_generator(value, pre + [key])
            elif isinstance(value, (list, tuple)):
                for v in value:
                    if isinstance(v, dict):
                        yield from dict_generator(v, pre + [key])
            else:
                yield pre + [key, value]
    else:
        yield indict


def guess_int_size(i: int) -> str:
    """Guess appropriate integer size type based on value.

    Args:
        i: Integer value to analyze.

    Returns:
        String indicating size type: 'uint8', 'uint16', or 'uint32'.
    """
    if i < 255:
        return 'uint8'
    if i < 65535:
        return 'uint16'
    return 'uint32'


def guess_datatype(s: Union[str, int, float, None], qd: Any) -> dict[str, Any]:
    """Guess data type of a string value.

    Analyzes a string to determine if it represents an integer, float,
    date, empty value, or remains a string.

    Args:
        s: Value to analyze (can be string, int, float, or None).
        qd: Query date matcher object for date detection.

    Returns:
        Dictionary with 'base' key indicating detected type and optional
        'subtype' or 'pat' keys for additional information.
    """
    attrs = {'base': 'str'}
    if s is None:
        return {'base': 'empty'}
    if isinstance(s, int):
        return {'base': 'int'}
    if isinstance(s, float):
        return {'base': 'float'}
    if not isinstance(s, str):
        return {'base': 'typed'}
    if s.isdigit():
        if s[0] == '0':
            attrs = {'base': 'numstr'}
        else:
            attrs = {'base': 'int', 'subtype': guess_int_size(int(s))}
    else:
        try:
            float(s)
            attrs = {'base': 'float'}
            return attrs
        except ValueError:
            pass
        if qd:
            is_date = False
            res = qd.match(s)
            if res:
                attrs = {'base': 'date', 'pat': res['pattern']}
                is_date = True
            if not is_date:
                if len(s.strip()) == 0:
                    attrs = {'base': 'empty'}
    return attrs


def buf_count_newlines_gen(fname: str) -> int:
    """Count newlines in a file using buffered reading.

    Efficiently counts newline characters in large files by reading
    in chunks rather than loading entire file into memory.

    Args:
        fname: Path to the file to analyze.

    Returns:
        Integer count of newline characters in the file.
    """
    def _make_gen(reader):
        while True:
            b = reader(2 ** 16)
            if not b:
                break
            yield b

    with open(fname, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
    return count


def get_dict_keys(iterable: Any, limit: int = 1000) -> list[str]:
    """Get all unique dictionary keys from an iterable of dictionaries.

    Extracts all nested keys from dictionaries, flattening them with dot notation.
    Uses set for O(1) lookup performance instead of O(n) list operations.

    Args:
        iterable: Iterable of dictionaries to process.
        limit: Maximum number of items to process (default: 1000).

    Returns:
        List of unique flattened key paths (e.g., ['field1', 'field2.subfield']).
    """
    n = 0
    keys_set = set()  # Use set for O(1) lookup instead of O(n) list operations
    for item in iterable:
        if limit and n > limit:
            break
        n += 1
        dk = dict_generator(item)
        for i in dk:
            k = ".".join(i[:-1])
            keys_set.add(k)
    return list(keys_set)  # Convert to list for backward compatibility


def _is_flat(item: dict[str, Any]) -> bool:
    """Check if dictionary contains only flat (non-nested) values.

    Args:
        item: Dictionary to check.

    Returns:
        True if dictionary contains no nested structures, False otherwise.
    """
    for v in item.values():
        if isinstance(v, (tuple, list)):
            return False
        if isinstance(v, dict):
            if not _is_flat(v):
                return False
    return True


def normalize_for_json(obj: Any) -> Any:
    """Convert non-JSON-serializable types to JSON-serializable ones.
    
    Recursively converts UUID objects and other non-serializable types to strings.
    This is needed when writing data from formats like Parquet (which preserve
    UUIDs as UUID objects) to JSON formats like JSONL.
    
    Args:
        obj: Object to normalize (can be dict, list, tuple, or primitive type)
        
    Returns:
        Normalized object with non-serializable types converted to strings
    """
    try:
        import uuid
        if isinstance(obj, uuid.UUID):
            return str(obj)
    except ImportError:
        pass  # uuid module not available
    
    if isinstance(obj, dict):
        return {key: normalize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [normalize_for_json(item) for item in obj]
    else:
        return obj
