"""Verified, atomic file recompression with codec-specific performance profiles."""

from __future__ import annotations

import gzip
import logging
import os
import stat
import time
import zlib
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..common.errors import DependencyError, FileNotFoundError, UndatumError, ValidationError
from ..common.file_snapshot import (
    FileConflictError,
    FileSnapshot,
    file_digest,
    require_regular,
    same_file,
)
from ..common.format_names import format_from_name
from ..common.path_utils import is_uri
from ..common.repack_streams import (
    STREAM_CODECS,
    gzip_header,
    load_codec,
    repack_stream,
    resolve_level,
)
from ..common.repack_types import (
    RepackBudget,
    RepackError,
    RepackOptions,
    RepackPlan,
    RepackResult,
    native_sidecar_paths,
)
from ..common.writer import AtomicFileTarget

logger = logging.getLogger(__name__)
BUILT_IN_COMPRESSION_FORMATS = frozenset({"parquet", "orc", "avro"})
DATABASE_SUFFIXES = frozenset({"duckdb", "ddb"})
SQLITE_SUFFIXES = frozenset({"sqlite", "sqlite3", "gpkg", "mbtiles", "sqlitedb", "vscdb"})
OTHER_DATA_SUFFIXES = frozenset(
    {
        "xml",
        "bson",
        "arrow",
        "ipc",
        "feather",
        "xlsx",
        "xls",
        "ods",
        "h5",
        "hdf5",
        "nc",
        "nc4",
        "npy",
        "npz",
        "fits",
        "nrrd",
    }
)


def _named_format(path: str) -> str | None:
    named = format_from_name(path)
    suffix = Path(path).suffix.lower().lstrip(".")
    if suffix in DATABASE_SUFFIXES:
        return "duckdb"
    if suffix in SQLITE_SUFFIXES:
        return "sqlite"
    if suffix in BUILT_IN_COMPRESSION_FORMATS:
        return suffix
    if suffix in OTHER_DATA_SUFFIXES:
        return suffix
    return named[0] if named else None


CODEC_ALIASES = {
    "gzip": "gz",
    "gz": "gz",
    "zstandard": "zst",
    "zstd": "zst",
    "zst": "zst",
    "brotli": "br",
    "br": "br",
    "bzip2": "bz2",
    "bz2": "bz2",
    "xz": "xz",
    "lzma": "lzma",
    "lz4": "lz4",
    "lzo": "lzo",
    "lzop": "lzo",
    "snappy": "sz",
    "sz": "sz",
    "zip": "zip",
    "7z": "7z",
}
_MAGIC = {
    b"\x1f\x8b": "gz",
    b"BZh": "bz2",
    b"\xfd7zXZ\x00": "xz",
    b"\x28\xb5\x2f\xfd": "zst",
    b"\x04\x22\x4d\x18": "lz4",
}
_ARCHIVE_MAGIC = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08", b"7z\xbc\xaf\x27\x1c")


def _input_kind(path: str) -> tuple[str | None, str | None]:
    with open(path, "rb") as source:
        prefix = source.read(18)
    suffix = Repacker._codec_from_path(path)
    if suffix in {"zip", "7z"} or prefix.startswith(_ARCHIVE_MAGIC):
        raise ValidationError(
            "ZIP/7z archive repack is unsupported; use a preserving archive tool.", field="input"
        )
    codec = next((name for magic, name in _MAGIC.items() if prefix.startswith(magic)), None)
    if len(prefix) >= 4 and 0x184D2A50 <= int.from_bytes(prefix[:4], "little") <= 0x184D2A5F:
        codec = "zst"
    if codec and suffix and codec != suffix:
        raise ValidationError("Compression magic and filename suffix disagree.", field="input")
    if suffix in _MAGIC.values() and codec is None:
        raise RepackError(f"Input does not have a valid {suffix} compression header.")
    codec = codec or suffix
    named_path = path.rsplit(".", 1)[0] if suffix else path
    datatype = _named_format(named_path)
    if not codec and prefix.startswith(b"SQLite format 3\0"):
        if datatype not in (None, "sqlite"):
            raise ValidationError("SQLite header and filename format disagree.", field="input")
        return None, "sqlite"
    if not codec and datatype == "sqlite":
        raise RepackError("Input has an invalid SQLite header.")
    if not codec and prefix[8:12] == b"DUCK":
        if datatype not in (None, "duckdb"):
            raise ValidationError("DuckDB header and filename format disagree.", field="input")
        return None, "duckdb"
    if not codec and datatype == "duckdb":
        raise RepackError("Input has an invalid DuckDB header.")
    if not codec:
        native = next(
            (
                name
                for magic, name in (
                    (b"PAR1", "parquet"),
                    (b"PARE", "parquet"),
                    (b"ORC", "orc"),
                    (b"Obj\x01", "avro"),
                )
                if prefix.startswith(magic)
            ),
            None,
        )
        if native and datatype in BUILT_IN_COMPRESSION_FORMATS and datatype != native:
            raise ValidationError("Native data magic and filename suffix disagree.", field="input")
        if datatype in BUILT_IN_COMPRESSION_FORMATS and native != datatype:
            raise RepackError(f"Input has an invalid {datatype} header.")
        # A text header beginning PAR1/ORC is not proof of a native binary file.
        if datatype is None or datatype in BUILT_IN_COMPRESSION_FORMATS:
            datatype = native or datatype
    return codec, datatype


def _native_settings(datatype: str, options: RepackOptions) -> tuple[str, int | None]:
    compression = options.compression
    if compression is not None and not isinstance(compression, str):
        raise ValidationError("Expected a codec name.", field="compression")
    key = (compression or ("zstandard" if datatype == "avro" else "zstd")).lower()
    if datatype == "orc":
        key = {"zstandard": "zstd", "zst": "zstd", "raw": "none", "null": "none"}.get(key, key)
        if key == "lzo":
            raise ValidationError(
                "The supported pyorc backend cannot encode ORC LZO; choose zstd or zlib.",
                field="compression",
            )
        if key not in {"none", "zlib", "snappy", "lz4", "zstd"}:
            raise ValidationError(f"Unsupported ORC codec '{key}'.", field="compression")
        if options.level is not None:
            raise ValidationError(
                "ORC supports compression strategies, not an explicit level.", field="level"
            )
        if options.threads != 1:
            raise ValidationError("ORC repack currently uses one codec thread.", field="threads")
        return key, None
    if datatype == "avro":
        key = {
            "zst": "zstandard",
            "zstd": "zstandard",
            "gz": "deflate",
            "gzip": "deflate",
            "bz2": "bzip2",
            "none": "null",
            "raw": "null",
        }.get(key, key)
        supported = {"null", "deflate", "snappy", "bzip2", "zstandard"}
        aliases = {"deflate": "gz", "bzip2": "bz2", "zstandard": "zst"}
    else:
        key = {
            "gz": "gzip",
            "zst": "zstd",
            "zstandard": "zstd",
            "br": "brotli",
            "raw": "none",
            "null": "none",
        }.get(key, key)
        supported = {"none", "snappy", "gzip", "brotli", "zstd", "lz4", "lz4_raw"}
        aliases = {"gzip": "gz", "brotli": "br", "zstd": "zst", "lz4": "lz4", "lz4_raw": "lz4"}
        if options.threads != 1:
            raise ValidationError(
                "Parquet repack currently uses one codec thread.", field="threads"
            )
    if key not in supported:
        raise ValidationError(f"Unsupported {datatype} codec '{key}'.", field="compression")
    if key not in aliases:
        if options.level is not None:
            raise ValidationError(f"{key} does not support an explicit level.", field="level")
        if options.threads != 1:
            raise ValidationError(f"{key} does not support codec threads.", field="threads")
        return key, None
    return key, resolve_level(aliases[key], options)


def prepare_plan(
    source: str, destination: str, inplace: bool, options: RepackOptions
) -> RepackPlan:
    """Resolve format, codec, levels and dependencies without creating any output."""
    codec, datatype = _input_kind(source)
    out_codec = Repacker._codec_from_path(destination)
    out_name = destination.rsplit(".", 1)[0] if out_codec else destination
    out_kind = _named_format(out_name)
    if out_kind and datatype and out_kind != datatype:
        raise ValidationError(
            "Repack preserves the data format; use convert to change it.", field="output"
        )
    if datatype == "sqlite" and codec is None:
        if out_codec:
            raise ValidationError("Native SQLite output cannot add an outer codec.", field="output")
        from ..common.repack_sqlite import prepare_sqlite

        prepare_sqlite(source, destination, inplace, options)
        return RepackPlan(source, "builtin", "sqlite", None, "none", None, options.profile, False)
    if datatype == "duckdb" and codec is None:
        if out_codec:
            raise ValidationError("Native DuckDB output cannot add an outer codec.", field="output")
        from ..common.repack_duckdb import prepare_duckdb, require_offline

        require_offline(source, destination)
        prepare_duckdb(source, options)
        return RepackPlan(source, "builtin", "duckdb", None, "auto", None, options.profile, False)
    if datatype in BUILT_IN_COMPRESSION_FORMATS and codec is None:
        if out_codec:
            raise ValidationError(
                "Native repack does not add an outer container; choose a native output.",
                field="output",
            )
        compression, level = _native_settings(datatype, options)
        try:
            if datatype == "parquet":
                import pyarrow as pa

                if compression != "none" and not pa.Codec.is_available(compression):
                    raise ValidationError(
                        f"PyArrow codec '{compression}' is unavailable.", field="compression"
                    )
                if level is not None:
                    low = pa.Codec.minimum_compression_level(compression)
                    high = pa.Codec.maximum_compression_level(compression)
                    if options.level is None:
                        level = max(low, min(level, high))
                    if not low <= level <= high:
                        raise ValidationError(
                            f"{compression} supports native levels {low} through {high}.",
                            field="level",
                        )
                    pa.Codec(compression, compression_level=level)
            elif datatype == "orc":
                import pyorc  # noqa: F401 - validate before allocating output
            else:
                from ..common.repack_avro import prepare_avro

                prepare_avro(source, compression)
        except ImportError as exc:
            raise DependencyError(
                exc.name or datatype,
                feature=f"{datatype} repack",
                install_command='pip install "undatum[repack]"',
            ) from exc
        return RepackPlan(
            source, "builtin", datatype, None, compression, level, options.profile, False
        )
    if options.compression is not None and not isinstance(options.compression, str):
        raise ValidationError("Expected a codec name.", field="compression")
    target = (
        Repacker._normalize_codec(options.compression)
        if options.compression
        else out_codec or codec
    )
    if target not in STREAM_CODECS or (codec is not None and codec not in STREAM_CODECS):
        raise ValidationError(
            "Input has no container compression or requested stream codec is unsupported.",
            field="compression",
            suggestions=sorted(STREAM_CODECS),
        )
    assert target is not None
    if out_codec != target and not (
        inplace and codec == target and out_codec is None and out_kind is None
    ):
        raise ValidationError(
            f"Output must have a suffix matching the {target} codec.", field="output"
        )
    migration = codec != target
    if inplace and migration:
        raise ValidationError(
            "Changing an outer codec requires a distinct compatible output.", field="output"
        )
    level = resolve_level(target, options)
    for required in {codec, target} - {None, "lzma"}:
        load_codec(str(required))
    if codec == "gz":
        gzip_header(source)
    return RepackPlan(
        source, "container", datatype, codec, target, level, options.profile, migration
    )


def perform_repack(plan: RepackPlan, target: str, options: RepackOptions) -> dict[str, Any]:
    """Encode and verify a staged candidate without publishing any destination."""
    budget = RepackBudget(
        options,
        target,
        scratch_paths=native_sidecar_paths(plan, target),
    )
    if plan.mode == "container":
        report = repack_stream(plan, target, options, budget)
    elif plan.datatype == "sqlite":
        from ..common.repack_sqlite import repack_sqlite

        report = repack_sqlite(plan, target, options, budget)
    elif plan.datatype == "duckdb":
        from ..common.repack_duckdb import repack_duckdb

        report = repack_duckdb(plan, target, options, budget)
    elif plan.datatype == "parquet":
        from ..common.repack_columnar import repack_parquet

        report = repack_parquet(plan, target, options, budget)
    elif plan.datatype == "orc":
        from ..common.repack_columnar import repack_orc

        report = repack_orc(plan, target, options, budget)
    else:
        from ..common.repack_avro import repack_avro

        report = repack_avro(plan, target, options, budget)
    budget.check()
    report["resources"] = {
        "decoded_bytes_processed": budget.decoded,
        "scratch_peak_bytes": budget.scratch_peak,
        "memory_enforcement": "none",
        "peak_rss_bytes": None,
    }
    return report


class Repacker:
    """Recompress a local file and publish only independently verified content."""

    def repack(
        self, fromfile: str, tofile: str | None = None, options: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Repack streams or native Parquet/ORC/Avro/DuckDB/SQLite with atomic publication.

        Args:
            fromfile: Local input path.
            tofile: Optional output, defaulting to an in-place optimization.
            options: Compression, acceptance, progress and resource options.

        Returns:
            A compatible dict containing sizes, settings, verification and publication outcome.

        Raises:
            UndatumError: If configuration, decoding, preservation or publication fails.
        """
        started = time.monotonic()
        settings = RepackOptions.from_dict(options)
        output = tofile or fromfile
        if fromfile == "-" or output == "-":
            raise ValidationError("Repack requires local files; stdin/stdout are unsupported.")
        if is_uri(fromfile) or is_uri(output):
            raise ValidationError("Remote URIs are not supported by repack; use local files.")
        if not os.path.exists(fromfile):
            raise FileNotFoundError(fromfile)
        try:
            return self._run(fromfile, output, settings, started)
        except UndatumError:
            raise
        except ImportError as exc:
            raise DependencyError(
                exc.name or "compression",
                feature="repack",
                install_command='pip install "undatum[compression]"',
            ) from exc
        except (EOFError, ValueError, gzip.BadGzipFile, zlib.error) as exc:
            raise RepackError(f"Repack failed: {exc}") from exc
        except OSError as exc:
            raise UndatumError(f"Repack file operation failed: {exc}", exit_code=3) from exc
        except Exception as exc:
            if type(exc).__module__.startswith(
                ("avro", "pyarrow", "pyorc", "zstandard", "lz4", "_lzma")
            ):
                raise RepackError(f"Repack failed: {exc}") from exc
            raise

    def _run(
        self, source: str, output: str, options: RepackOptions, started: float
    ) -> dict[str, Any]:
        """Apply source guards, acceptance policy and publication to a verified candidate."""
        inplace = same_file(source, output)
        require_regular(source, inplace=inplace)
        exists = os.path.lexists(output)
        if exists and (os.path.islink(output) or not stat.S_ISREG(os.lstat(output).st_mode)):
            raise ValidationError(
                "Output must be a regular file, not a link or special file.", field="output"
            )
        preflight_budget = RepackBudget(options, "", started=started)
        original = FileSnapshot.capture(source, preflight_budget.check)
        prior = (
            FileSnapshot.capture(output, preflight_budget.check) if exists and not inplace else None
        )
        plan = prepare_plan(source, output, inplace, options)
        original.require_unchanged(check=preflight_budget.check)
        database = plan.mode == "builtin" and plan.datatype in {"duckdb", "sqlite"}

        def guard_database(*paths: str) -> None:
            if database and plan.datatype == "duckdb":
                from ..common.repack_duckdb import require_offline

                require_offline(source, output, *paths)
            elif database:
                from ..common.repack_sqlite import require_offline as require_sqlite_offline

                require_sqlite_offline(source, output, *paths)

        guard_database()
        if options.memory_limit is not None or options.timeout is not None:
            from ..common.repack_worker import require_monitor

            require_monitor(options)
        with AtomicFileTarget(output) as stage, ExitStack() as cleanup:
            if database and plan.datatype == "duckdb":
                from ..common.repack_duckdb import remove_candidate_sidecars

                cleanup.callback(remove_candidate_sidecars, stage.target)
            elif database:
                from ..common.repack_sqlite import (
                    remove_candidate_sidecars as remove_sqlite_sidecars,
                )

                cleanup.callback(remove_sqlite_sidecars, stage.target)
            budget = RepackBudget(
                options,
                stage.target,
                started=started,
                scratch_paths=native_sidecar_paths(plan, stage.target),
            )
            guard_database(stage.target)
            original.require_unchanged(check=budget.check)
            if options.memory_limit is not None or options.timeout is not None:
                from ..common.repack_worker import supervised_repack

                report = supervised_repack(plan, stage.target, options, started)
            else:
                report = perform_repack(plan, stage.target, options)
            guard_database(stage.target)
            budget.check()
            candidate = FileSnapshot.capture(stage.target, budget.check)
            proof = report.get("verification", {})
            if not proof.get("passed") or candidate.digest != proof.get("candidate_sha256"):
                raise RepackError("Candidate no longer matches its preservation proof.")
            if candidate.state[2] == 0:
                raise RepackError("Encoder produced an empty candidate file.")
            original.require_unchanged(check=budget.check)
            if prior:
                prior.require_unchanged(check=budget.check)
            candidate.require_unchanged(check=budget.check)
            insize, candidate_size = original.state[2], candidate.state[2]
            percent = 100 * (insize - candidate_size) / insize if insize else None
            no_benefit = not plan.migration and not options.allow_grow and candidate_size >= insize
            below = options.min_savings is not None and (
                percent is None or percent < options.min_savings
            )
            accepted = not no_benefit and not below
            reason = "below_min_savings" if below else "no_benefit" if no_benefit else "accepted"
            published = False
            final_size: int | None = candidate_size if accepted else insize
            status = "replaced" if accepted else "skipped" if plan.migration else "unchanged"
            if options.dry_run:
                status = "dry_run"
            elif accepted or (not inplace and not plan.migration):
                if not accepted:
                    with open(source, "rb") as reader, open(stage.target, "wb") as writer:
                        while block := reader.read(1024 * 1024):
                            budget.check()
                            writer.write(block)
                    if file_digest(stage.target, budget.check) != original.digest:
                        raise RepackError("Original-copy fallback changed source bytes.")
                original.apply_metadata(stage.target)
                if file_digest(stage.target, budget.check) != (
                    candidate.digest if accepted else original.digest
                ):
                    raise RepackError("Metadata application changed candidate bytes.")
                publication = FileSnapshot.capture(stage.target, budget.check)

                def guard() -> None:
                    budget.check()
                    guard_database(stage.target)
                    publication.require_unchanged(check=budget.check)
                    original.require_unchanged(check=budget.check)
                    if prior:
                        prior.require_unchanged(check=budget.check)
                    elif not exists and os.path.lexists(output):
                        raise FileConflictError(output)

                stage.before_commit = guard
                stage.commit()
                published = True
            if not published:
                final_size = os.path.getsize(output) if exists else None
            savings = (
                insize - (final_size if final_size is not None else insize) if published else 0
            )
            result = RepackResult(
                input=source,
                output=output,
                mode=plan.mode,
                input_size=insize,
                candidate_size=candidate_size,
                output_size=final_size,
                status=status,
                reason_code=reason,
                published=published,
                changed=accepted and published,
                savings_bytes=savings,
                savings_percent=100 * savings / insize if insize else None,
                candidate_savings_bytes=insize - candidate_size,
                candidate_savings_percent=percent,
                would_publish=accepted or (not inplace and not plan.migration),
                elapsed_seconds=time.monotonic() - started,
                effective_settings={
                    "codec": plan.target_codec,
                    "profile": plan.profile,
                    "level": plan.level,
                    "threads": options.threads,
                    "applied": accepted and published,
                },
                verification=report["verification"],
                resources=report["resources"],
                rows=report.get("rows"),
                decoded_bytes=report["decoded_bytes"],
                timings=report["timings"],
            )
            result.resources["scratch_peak_bytes"] = max(
                result.resources["scratch_peak_bytes"], budget.scratch_peak
            )
            if plan.datatype == "orc" and plan.mode == "builtin":
                result.effective_settings["strategy"] = (
                    "speed" if options.profile == "fast" else "compression"
                )
            if database and plan.datatype == "duckdb":
                result.effective_settings["strategy"] = "copy_database"
            elif database:
                result.effective_settings["strategy"] = "vacuum_into"
            payload = asdict(result)
            payload.update(
                bytes_read=result.decoded_bytes,
                bytes_written=result.output_size,
                level=plan.level,
                profile=plan.profile,
                **(
                    {"codec": plan.target_codec}
                    if plan.mode == "container"
                    else {"compression": plan.target_codec}
                ),
            )
            logger.info("Repack %s: %s → %s bytes (%s)", source, insize, final_size, status)
            return payload

    @staticmethod
    def _normalize_codec(codec: str) -> str | None:
        """Canonicalize outer codec names without conflating XZ and LZMA-alone."""
        return CODEC_ALIASES.get(str(codec).lower().lstrip("."))

    @classmethod
    def _codec_from_path(cls, path: str) -> str | None:
        """Recognize compression suffixes independently of the inner datatype."""
        return cls._normalize_codec(Path(path).suffix.lstrip("."))
