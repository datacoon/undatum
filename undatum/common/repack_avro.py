"""Avro OCF block recompression without record inference or union coercion."""

from __future__ import annotations

import bz2
import hashlib
import importlib
import struct
import time
import zlib
from collections.abc import Iterator
from typing import Any, BinaryIO

from .errors import DependencyError, ValidationError
from .file_snapshot import FileSnapshot
from .repack_types import RepackBudget, RepackError, RepackOptions, RepackPlan

MAX_BLOCK_BYTES = 64 * 1024 * 1024
AVRO_CODECS = frozenset({"null", "deflate", "bzip2", "zstandard", "snappy"})


def _dependency(module: str) -> Any:
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        raise DependencyError(
            exc.name or module,
            feature="Avro repack",
            install_command='pip install "undatum[compression]"',
        ) from exc


def _long(source: BinaryIO, *, eof: bool = False) -> int | None:
    value = 0
    for index in range(10):
        raw = source.read(1)
        if not raw:
            if index == 0 and eof:
                return None
            raise RepackError("Truncated Avro integer.")
        byte = raw[0]
        if index == 9 and byte > 1:
            raise RepackError("Invalid Avro integer.")
        value |= (byte & 127) << (7 * index)
        if not byte & 128:
            return (value >> 1) ^ -(value & 1)
    raise RepackError("Invalid Avro integer.")


def _encode_long(value: int) -> bytes:
    number = (value << 1) ^ (value >> 63)
    result = bytearray()
    while number > 127:
        result.append((number & 127) | 128)
        number >>= 7
    result.append(number)
    return bytes(result)


def _bytes(source: BinaryIO, maximum: int) -> bytes:
    size = _long(source)
    if size is None or size < 0 or size > maximum:
        raise ValidationError("Avro field/block exceeds supported bounded size.", field="input")
    data = source.read(size)
    if len(data) != size:
        raise RepackError("Truncated Avro field/block.")
    return data


def read_header(source: BinaryIO) -> tuple[dict[bytes, bytes], bytes]:
    """Read original metadata bytes and sync identity with bounded header allocation."""
    if source.read(4) != b"Obj\x01":
        raise RepackError("Invalid Avro object container magic.")
    metadata: dict[bytes, bytes] = {}
    while True:
        count = _long(source)
        if count == 0:
            break
        if count is None or abs(count) > 10000:
            raise RepackError("Invalid Avro metadata count.")
        declared = _long(source) if count < 0 else None
        start = source.tell()
        for _ in range(abs(count)):
            key, value = _bytes(source, 1024 * 1024), _bytes(source, 1024 * 1024)
            key.decode("utf8")
            if key in metadata:
                raise RepackError("Duplicate Avro metadata key.")
            metadata[key] = value
            if source.tell() > 1024 * 1024:
                raise RepackError("Avro metadata header exceeds one MiB.")
        if declared is not None and source.tell() - start != declared:
            raise RepackError("Invalid Avro metadata block length.")
    sync = source.read(16)
    if len(sync) != 16 or b"avro.schema" not in metadata:
        raise RepackError("Missing Avro schema or sync marker.")
    return metadata, sync


def prepare_avro(path: str, target_codec: str) -> None:
    """Validate schemas and codec dependencies before any candidate is written."""
    schema_module = _dependency("avro.schema")
    _dependency("avro.datafile")
    with open(path, "rb") as source:
        metadata, _ = read_header(source)
    schema_module.parse(metadata[b"avro.schema"].decode("utf8"))
    original = metadata.get(b"avro.codec", b"null").decode("ascii")
    for codec in (original, target_codec):
        if codec not in AVRO_CODECS:
            raise ValidationError(f"Unsupported Avro codec '{codec}'.", field="compression")
        if codec == "zstandard":
            _dependency("zstandard")
        elif codec == "snappy":
            _dependency("snappy")


def _decode(data: bytes, codec: str) -> bytes:
    maximum = MAX_BLOCK_BYTES
    if codec == "null":
        raw = data
    elif codec == "deflate":
        decoder = zlib.decompressobj(-15)
        raw = decoder.decompress(data, maximum + 1)
        if len(raw) > maximum:
            raise RepackError("Avro decoded block exceeds 64 MiB.")
        # Apache avro 1.x writes zlib.compress(data)[2:-1], retaining three Adler
        # bytes. Accept only that verified legacy trailer, never arbitrary junk.
        legacy = decoder.unused_data == struct.pack(">I", zlib.adler32(raw) & 0xFFFFFFFF)[:3]
        if not decoder.eof or (decoder.unused_data and not legacy) or decoder.unconsumed_tail:
            raise RepackError("Incomplete or trailing Avro deflate block.")
    elif codec == "bzip2":
        decompressor = bz2.BZ2Decompressor()
        raw = decompressor.decompress(data, max_length=maximum + 1)
        if not decompressor.eof or decompressor.unused_data:
            raise RepackError("Incomplete or oversized Avro compressed block.")
    elif codec == "zstandard":
        zstd = _dependency("zstandard")
        # Validate complete framing before using a bounded reader that tolerates truncation.
        parameters = zstd.get_frame_parameters(data)
        if (
            parameters.content_size not in (zstd.CONTENTSIZE_UNKNOWN, zstd.CONTENTSIZE_ERROR)
            and parameters.content_size > maximum
        ):
            raise RepackError("Avro decoded block exceeds 64 MiB.")
        try:
            raw = zstd.ZstdDecompressor().decompress(
                data, max_output_size=maximum, allow_extra_data=False
            )
        except zstd.ZstdError as exc:
            raise RepackError(f"Invalid Avro zstandard block: {exc}") from exc
    elif codec == "snappy":
        if len(data) < 4:
            raise RepackError("Missing Avro snappy checksum.")
        # Raw Snappy starts with the decoded length (unsigned base-128 varint).
        # Check it before the native decoder can allocate the entire block.
        length = 0
        for index, byte in enumerate(data[:5]):
            length |= (byte & 127) << (7 * index)
            if not byte & 128:
                break
        else:
            raise RepackError("Invalid Avro snappy decoded length.")
        if length > maximum:
            raise RepackError("Avro decoded block exceeds 64 MiB.")
        raw = _dependency("snappy").decompress(data[:-4])
        if struct.pack(">I", zlib.crc32(raw) & 0xFFFFFFFF) != data[-4:]:
            raise RepackError("Invalid Avro snappy checksum.")
    else:
        raise ValidationError(f"Unsupported Avro codec '{codec}'.", field="compression")
    if len(raw) > maximum:
        raise RepackError("Avro decoded block exceeds 64 MiB.")
    return bytes(raw)


def _encode(data: bytes, codec: str, level: int | None, threads: int) -> bytes:
    if codec == "null":
        return data
    if codec == "deflate":
        compressor = zlib.compressobj(level if level is not None else 9, zlib.DEFLATED, -15)
        return compressor.compress(data) + compressor.flush()
    if codec == "bzip2":
        return bz2.compress(data, compresslevel=level or 9)
    if codec == "zstandard":
        return bytes(
            _dependency("zstandard").ZstdCompressor(level=level, threads=threads).compress(data)
        )
    if codec == "snappy":
        return bytes(_dependency("snappy").compress(data)) + struct.pack(
            ">I", zlib.crc32(data) & 0xFFFFFFFF
        )
    raise ValidationError(f"Unsupported Avro codec '{codec}'.", field="compression")


def _blocks(
    source: BinaryIO, codec: str, sync: bytes, budget: RepackBudget
) -> Iterator[tuple[int, bytes]]:
    while True:
        budget.check()
        count = _long(source, eof=True)
        if count is None:
            return
        if count <= 0:
            raise RepackError("Invalid Avro record count.")
        data = _bytes(source, MAX_BLOCK_BYTES)
        if source.read(16) != sync:
            raise RepackError("Invalid Avro block sync marker.")
        raw = _decode(data, codec)
        budget.check(len(raw))
        yield count, raw


def _native_validate(path: str, budget: RepackBudget) -> None:
    from avro.datafile import DataFileReader
    from avro.io import DatumReader

    with open(path, "rb") as raw, DataFileReader(raw, DatumReader()) as reader:
        for _ in reader:
            budget.check()


def repack_avro(
    plan: RepackPlan, target: str, options: RepackOptions, budget: RepackBudget
) -> dict[str, Any]:
    """Recompress encoded records with unchanged schema bytes, metadata and union branches."""
    started = time.monotonic()
    expected, rows, decoded = hashlib.sha256(), 0, 0
    with open(plan.source, "rb") as source, open(target, "wb") as output:
        metadata, sync = read_header(source)
        codec = metadata.get(b"avro.codec", b"null").decode("ascii")
        out_metadata = {**metadata, b"avro.codec": plan.target_codec.encode("ascii")}
        output.write(b"Obj\x01" + _encode_long(len(out_metadata)))
        for key, value in out_metadata.items():
            output.write(_encode_long(len(key)) + key + _encode_long(len(value)) + value)
        output.write(b"\0" + sync)
        for count, raw in _blocks(source, codec, sync, budget):
            expected.update(struct.pack("<QQ", count, len(raw)))
            expected.update(raw)
            rows += count
            decoded += len(raw)
            encoded = _encode(raw, plan.target_codec, plan.level, options.threads)
            output.write(_encode_long(count) + _encode_long(len(encoded)) + encoded + sync)
            budget.check()
    budget.check()
    encode_seconds = time.monotonic() - started
    verified = time.monotonic()
    snapshot = FileSnapshot.capture(target, budget.check)
    actual, actual_rows = hashlib.sha256(), 0
    with open(target, "rb") as candidate:
        new_metadata, new_sync = read_header(candidate)
        if new_metadata != out_metadata or new_sync != sync:
            raise RepackError("Candidate changed Avro schema, metadata or sync marker.")
        for count, raw in _blocks(candidate, plan.target_codec, sync, budget):
            actual.update(struct.pack("<QQ", count, len(raw)))
            actual.update(raw)
            actual_rows += count
    if actual_rows != rows or expected.digest() != actual.digest():
        raise RepackError("Candidate changed Avro encoded records, union branches or order.")
    # Native validation decodes the same blocks again; reserve that known work
    # before entering its reader, in addition to the independent block hashes.
    budget.check(decoded)
    _native_validate(plan.source, budget)
    budget.check(decoded)
    _native_validate(target, budget)
    snapshot.require_unchanged(check=budget.check)
    return {
        "rows": rows,
        "decoded_bytes": decoded,
        "verification": {
            "method": "avro-encoded-block-sha256",
            "candidate_sha256": snapshot.digest,
            "passed": True,
            "sha256": actual.hexdigest(),
        },
        "timings": {
            "encode_seconds": encode_seconds,
            "verify_seconds": time.monotonic() - verified,
        },
    }
