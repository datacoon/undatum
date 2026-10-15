"""Disk-backed buffers for operations that need the whole input (reverse, backward fill)."""

from __future__ import annotations

import base64
import datetime as dt
import math
import os
import tempfile
import uuid
from collections.abc import Iterable, Iterator
from decimal import Decimal
from typing import Any

import orjson

# Records kept in memory per chunk before spilling to disk.
DEFAULT_CHUNK_ROWS = 50_000
_TAG = "__undatum_spill_type__"


def _tagged(kind: str, value: Any) -> dict[str, Any]:
    return {_TAG: kind, "value": value}


def _encode(value: Any) -> Any:
    """Encode typed values without allowing user dictionaries to imitate type tags."""
    if isinstance(value, dict):
        if _TAG in value or any(not isinstance(k, str) for k in value):
            return _tagged("dict", [[_encode(k), _encode(v)] for k, v in value.items()])
        return {k: _encode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_encode(v) for v in value]
    if isinstance(value, (tuple, set, frozenset)):
        return _tagged(type(value).__name__, [_encode(v) for v in value])
    if isinstance(value, dt.datetime):
        return _tagged("datetime", [value.isoformat(), value.fold])
    if isinstance(value, dt.date):
        return _tagged("date", value.isoformat())
    if isinstance(value, dt.time):
        return _tagged("time", [value.isoformat(), value.fold])
    if isinstance(value, dt.timedelta):
        return _tagged("timedelta", [value.days, value.seconds, value.microseconds])
    if isinstance(value, Decimal):
        return _tagged("decimal", str(value))
    if isinstance(value, uuid.UUID):
        return _tagged("uuid", str(value))
    if isinstance(value, bytes):
        return _tagged("bytes", base64.b64encode(value).decode("ascii"))
    if isinstance(value, float) and not math.isfinite(value):
        return _tagged("float", repr(value))
    if isinstance(value, int) and not -(2**63) <= value < 2**64:
        return _tagged("int", str(value))
    return value


def _decode(value: Any) -> Any:
    """Restore only the built-in types explicitly supported by the spill encoding."""
    if isinstance(value, list):
        return [_decode(v) for v in value]
    if not isinstance(value, dict):
        return value
    if _TAG not in value:
        return {k: _decode(v) for k, v in value.items()}
    kind, data = value[_TAG], value["value"]
    if kind == "dict":
        return {_decode(k): _decode(v) for k, v in data}
    if kind == "tuple":
        return tuple(_decode(v) for v in data)
    if kind == "set":
        return {_decode(v) for v in data}
    if kind == "frozenset":
        return frozenset(_decode(v) for v in data)
    if kind == "datetime":
        return dt.datetime.fromisoformat(data[0]).replace(fold=data[1])
    if kind == "date":
        return dt.date.fromisoformat(data)
    if kind == "time":
        return dt.time.fromisoformat(data[0]).replace(fold=data[1])
    if kind == "timedelta":
        return dt.timedelta(days=data[0], seconds=data[1], microseconds=data[2])
    if kind == "decimal":
        return Decimal(data)
    if kind == "uuid":
        return uuid.UUID(data)
    if kind == "bytes":
        return base64.b64decode(data)
    if kind == "float":
        return float(data)
    if kind == "int":
        return int(data)
    raise ValueError(f"Unknown spill type: {kind}")


def _dump(record: Any) -> bytes:
    return orjson.dumps(_encode(record))


def reversed_rows(rows: Iterable[Any], chunk_rows: int = DEFAULT_CHUNK_ROWS) -> Iterator[Any]:
    """Yield ``rows`` in reverse order with at most ``chunk_rows`` records in memory.

    Records are written to temporary JSON Lines chunks with explicit type tags;
    dates, decimals, binary and nested values retain their types when the chunks
    are read back. No executable objects are deserialized.

    Args:
        rows: Input records.
        chunk_rows: Records per chunk (the memory bound).

    Yields:
        The records, last first.
    """
    chunk: list[Any] = []
    spilled: list[str] = []
    try:
        for row in rows:
            chunk.append(row)
            if len(chunk) >= chunk_rows:
                spilled.append(_spill(chunk))
                chunk = []
        yield from reversed(chunk)
        chunk = []
        for path in reversed(spilled):
            with open(path, "rb") as handle:
                lines = handle.read().splitlines()
            for line in reversed(lines):
                yield _decode(orjson.loads(line))
            os.remove(path)
    finally:
        for path in spilled:
            if os.path.exists(path):
                os.remove(path)


def _spill(records: list[Any]) -> str:
    fd, path = tempfile.mkstemp(prefix="undatum-spill-", suffix=".jsonl")
    try:
        with os.fdopen(fd, "wb") as handle:
            for record in records:
                handle.write(_dump(record))
                handle.write(b"\n")
    except BaseException:
        os.remove(path)
        raise
    return path
