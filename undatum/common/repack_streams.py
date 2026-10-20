"""Streaming compression with independent payload and complete-frame verification."""

from __future__ import annotations

import hashlib
import importlib
import logging
import lzma
import os
import struct
import time
import zlib
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import Any, BinaryIO

from .errors import DependencyError, ValidationError
from .file_snapshot import FileSnapshot
from .repack_types import RepackBudget, RepackError, RepackOptions, RepackPlan

CHUNK_SIZE = 1024 * 1024
STREAM_CODECS = frozenset({"gz", "bz2", "xz", "lzma", "zst", "lz4", "br"})
LEVEL_RANGES = {
    "gz": (0, 9),
    "bz2": (1, 9),
    "xz": (0, 9),
    "lzma": (0, 9),
    "zst": (-131072, 22),
    "lz4": (0, 16),
    "br": (0, 11),
}
logger = logging.getLogger(__name__)


def load_codec(codec: str) -> Any:
    """Resolve iterabledata's codec class and surface actual missing dependencies."""
    from iterable.helpers.detect import CODEC_REGISTRY

    try:
        module_name, symbol = CODEC_REGISTRY[codec]
        return getattr(importlib.import_module(module_name), symbol)
    except ImportError as exc:
        raise DependencyError(
            exc.name or codec,
            feature=f"{codec} compression",
            install_command='pip install "undatum[compression]"',
        ) from exc


def resolve_level(codec: str, options: RepackOptions) -> int:
    """Apply iterabledata profiles with explicit codec-specific level validation."""
    from iterable.codecs.profiles import PROFILE_LEVELS

    profile_key = {"gz": "gzip", "zst": "zstd", "br": "brotli", "lzma": "xz"}.get(codec, codec)
    level = options.level
    if level is None:
        level = int(PROFILE_LEVELS[profile_key][options.profile])
    low, high = LEVEL_RANGES[codec]
    if not low <= level <= high:
        raise ValidationError(f"{codec} supports levels {low} through {high}.", field="level")
    if options.threads != 1 and codec != "zst":
        raise ValidationError(
            "Codec threads are currently supported only for zstd streams.", field="threads"
        )
    return level


def gzip_header(path: str | None) -> bytes:
    """Preserve the first gzip member's name, comment and mtime; refuse format extras."""
    if path is None:
        return b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff"
    with open(path, "rb") as source:
        header = bytearray(source.read(10))
        if len(header) != 10 or header[:3] != b"\x1f\x8b\x08":
            raise RepackError("Invalid gzip header.")
        flags = header[3]
        if flags & 0xE0 or flags & 4:
            raise ValidationError(
                "Gzip extra/reserved headers require a preserving adapter "
                "(for example BGZF); repack refused.",
                field="input",
            )
        for flag in (8, 16):
            if flags & flag:
                for _ in range(65536):
                    byte = source.read(1)
                    if not byte:
                        raise RepackError("Truncated gzip header.")
                    header.extend(byte)
                    if byte == b"\0":
                        break
                else:
                    raise RepackError("Gzip header exceeds supported bounds.")
        if flags & 2:
            checksum = source.read(2)
            if checksum != struct.pack("<H", zlib.crc32(header) & 0xFFFF):
                raise RepackError("Invalid gzip header checksum.")
            header.extend(checksum)
        return bytes(header)


class _GzipWriter:
    def __init__(self, stream: BinaryIO, header: bytes, level: int):
        self.stream = stream
        self.compressor = zlib.compressobj(level, zlib.DEFLATED, -15)
        self.checksum = 0
        self.size = 0
        stream.write(header)

    def write(self, block: bytes) -> int:
        self.stream.write(self.compressor.compress(block))
        self.checksum = zlib.crc32(block, self.checksum)
        self.size += len(block)
        return len(block)

    def finish(self) -> None:
        self.stream.write(self.compressor.flush())
        self.stream.write(struct.pack("<II", self.checksum & 0xFFFFFFFF, self.size & 0xFFFFFFFF))


class _BrotliReader:
    """Bound output and require a complete stream, unlike brotli-file's reader."""

    def __init__(self, raw: BinaryIO, budget: RepackBudget):
        import brotli

        self.raw = raw
        self.budget = budget
        self.decoder = brotli.Decompressor()
        self.bounded = hasattr(self.decoder, "can_accept_more_data")
        self.pending = bytearray()
        self.finished = False

    def read(self, size: int) -> bytes:
        import brotli

        while len(self.pending) < size and not self.finished:
            self.budget.check()
            if self.decoder.is_finished():
                if self.raw.read(1):
                    raise RepackError("Trailing Brotli stream data.")
                self.finished = True
                break
            wants_input = not self.bounded or self.decoder.can_accept_more_data()
            # Old bindings cannot cap decoded output. Feeding one byte bounds each
            # operation to a single format-bounded metablock rather than many blocks.
            data = self.raw.read(65536 if self.bounded else 1) if wants_input else b""
            if wants_input and not data:
                raise RepackError("Truncated Brotli stream.")
            try:
                block = (
                    self.decoder.process(data, output_buffer_limit=size - len(self.pending))
                    if self.bounded
                    else self.decoder.process(data)
                )
            except brotli.error as exc:
                raise RepackError(f"Invalid Brotli stream: {exc}") from exc
            self.pending.extend(block)
        result = bytes(self.pending[:size])
        del self.pending[:size]
        return result


@contextmanager
def _registry_stream(path: str, codec: str, write: bool, level: int | None) -> Iterator[Any]:
    instance = load_codec(codec)(
        filename=path,
        mode="wb" if write else "rb",
        options={"compression_level": level} if write else {},
    )
    try:
        instance.open()
        yield instance.fileobj()
    except BaseException:
        try:
            instance.close()
        except Exception:
            logger.debug("Codec cleanup failed", exc_info=True)
        raise
    else:
        instance.close()


def check_zstd_frames(path: str, budget: RepackBudget) -> None:
    """Check complete Zstandard frame boundaries, including trailing/skippable frames."""
    import zstandard as zstd

    size = os.path.getsize(path)
    with open(path, "rb") as source:
        while source.tell() < size:
            budget.check()
            magic = source.read(4)
            if len(magic) != 4:
                raise RepackError("Truncated Zstandard frame.")
            number = int.from_bytes(magic, "little")
            if 0x184D2A50 <= number <= 0x184D2A5F:
                skipped_length = source.read(4)
                if len(skipped_length) != 4:
                    raise RepackError("Truncated Zstandard skippable frame.")
                source.seek(int.from_bytes(skipped_length, "little"), 1)
            elif magic == b"\x28\xb5\x2f\xfd":
                start = source.tell() - 4
                prefix = magic + source.read(14)
                header_size = zstd.frame_header_size(prefix)
                source.seek(start + header_size)
                last = False
                while not last:
                    budget.check()
                    block = source.read(3)
                    if len(block) != 3:
                        raise RepackError("Truncated Zstandard block header.")
                    value = int.from_bytes(block, "little")
                    last, kind, length = bool(value & 1), (value >> 1) & 3, value >> 3
                    if kind == 3 or length > 131072:
                        raise RepackError("Invalid Zstandard block.")
                    source.seek(1 if kind == 1 else length, 1)
                if prefix[4] & 4:
                    source.seek(4, 1)
            else:
                raise RepackError("Invalid or trailing Zstandard frame data.")
            if source.tell() > size:
                raise RepackError("Truncated Zstandard frame body/checksum.")


def open_reader(stack: ExitStack, path: str, codec: str | None, budget: RepackBudget) -> Any:
    """Open all stream frames, checking framing where a backend tolerates truncation."""
    if codec is None:
        return stack.enter_context(open(path, "rb"))
    if codec == "lzma":
        return stack.enter_context(lzma.open(path, "rb", format=lzma.FORMAT_ALONE))
    if codec == "br":
        return _BrotliReader(stack.enter_context(open(path, "rb")), budget)
    if codec == "zst":
        import zstandard as zstd

        check_zstd_frames(path, budget)
        raw = stack.enter_context(open(path, "rb"))
        return stack.enter_context(
            zstd.ZstdDecompressor().stream_reader(
                raw,
                read_across_frames=True,
                closefd=False,
            )
        )
    return stack.enter_context(_registry_stream(path, codec, False, None))


def _fingerprint(path: str, codec: str | None, budget: RepackBudget) -> tuple[str, int]:
    digest, count = hashlib.sha256(), 0
    with ExitStack() as stack:
        reader = open_reader(stack, path, codec, budget)
        while block := reader.read(CHUNK_SIZE):
            budget.check(len(block))
            digest.update(block)
            count += len(block)
    return digest.hexdigest(), count


def repack_stream(
    plan: RepackPlan, target: str, options: RepackOptions, budget: RepackBudget
) -> dict[str, Any]:
    """Encode a byte stream and independently verify the complete decoded candidate."""
    from .progress import progress_bar, update_progress

    started = time.monotonic()
    assert plan.level is not None
    digest, count = hashlib.sha256(), 0
    with ExitStack() as stack:
        source = open_reader(stack, plan.source, plan.source_codec, budget)
        finish = None
        dest: Any
        if plan.target_codec == "gz":
            raw = stack.enter_context(open(target, "wb"))
            writer = _GzipWriter(
                raw,
                gzip_header(plan.source if plan.source_codec == "gz" else None),
                int(plan.level or 0),
            )
            dest = writer
            finish = writer.finish
        elif plan.target_codec == "zst" and options.threads != 1:
            import zstandard as zstd

            raw = stack.enter_context(open(target, "wb"))
            dest = stack.enter_context(
                zstd.ZstdCompressor(
                    level=plan.level,
                    threads=options.threads,
                ).stream_writer(raw, closefd=False)
            )
        elif plan.target_codec == "lzma":
            dest = stack.enter_context(
                lzma.open(target, "wb", format=lzma.FORMAT_ALONE, preset=plan.level)
            )
        else:
            dest = stack.enter_context(
                _registry_stream(target, plan.target_codec, True, plan.level)
            )
        with progress_bar(
            total=None, desc="Repacking", unit="B", show_progress=options.progress
        ) as bar:
            while block := source.read(CHUNK_SIZE):
                budget.check(len(block))
                digest.update(block)
                count += len(block)
                dest.write(block)
                update_progress(bar, n=len(block))
        if finish:
            finish()
    budget.check()
    encode_seconds = time.monotonic() - started
    verified = time.monotonic()
    snapshot = FileSnapshot.capture(target, budget.check)
    expected = (digest.hexdigest(), count)
    if _fingerprint(target, plan.target_codec, budget) != expected:
        raise RepackError("Candidate decoded content differs from the source.")
    snapshot.require_unchanged(check=budget.check)
    return {
        "decoded_bytes": count,
        "rows": None,
        "verification": {
            "method": "decoded-sha256",
            "sha256": expected[0],
            "decoded_bytes": count,
            "passed": True,
            "candidate_sha256": snapshot.digest,
        },
        "timings": {
            "encode_seconds": encode_seconds,
            "verify_seconds": time.monotonic() - verified,
        },
    }
