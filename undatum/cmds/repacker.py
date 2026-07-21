"""Repack command — recompress files at maximum compression by default."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from ..common.errors import (
    FileNotFoundError,
    FormatError,
    PermissionError,
    ValidationError,
    find_similar_files,
)
from ..common.path_utils import is_uri, validate_file_path
from ..common.progress import progress_bar, update_progress
from ..constants import COMPRESSED_FILE_TYPES
from ..utils import get_option

logger = logging.getLogger(__name__)

BUILT_IN_COMPRESSION_FORMATS = frozenset({"parquet", "orc", "avro"})
CHUNK_SIZE = 1024 * 1024

# Map detected / CLI codec ids onto iterabledata profile keys and registry keys.
CODEC_ALIASES = {
    "gzip": "gz",
    "gz": "gz",
    "zstd": "zst",
    "zst": "zst",
    "brotli": "br",
    "br": "br",
    "bz2": "bz2",
    "bzip2": "bz2",
    "xz": "xz",
    "lzma": "xz",
    "lz4": "lz4",
    "lzo": "lzo",
    "lzop": "lzo",
    "snappy": "sz",
    "sz": "sz",
    "zip": "zip",
    "7z": "7z",
}

DEFAULT_PARQUET_COMPRESSION = "zstd"
DEFAULT_AVRO_COMPRESSION = "zstandard"
DEFAULT_ORC_COMPRESSION = 5  # pyorc.CompressionKind.ZSTD

ORC_COMPRESSION_NAMES = {
    "none": 0,
    "zlib": 1,
    "snappy": 2,
    "lzo": 3,
    "lz4": 4,
    "zstd": 5,
}


class Repacker:
    """Recompress files using container codecs or format-native compression."""

    def repack(self, fromfile: str, tofile: str | None = None, options: dict | None = None):
        """Repack a file at maximum compression by default.

        Args:
            fromfile: Path to the input file.
            tofile: Optional output path. When omitted, rewrites ``fromfile`` atomically.
            options: Optional dict with keys ``level``, ``progress``, ``compression``.

        Returns:
            Dict with ``input``, ``output``, ``mode``, and size metrics when available.
        """
        if options is None:
            options = {}

        if is_uri(fromfile):
            raise ValidationError(
                "Remote URIs are not supported for in-place-capable repack; "
                "download locally or use convert.",
                field="input",
            )

        try:
            validate_file_path(fromfile, check_read=True)
        except FileNotFoundError as e:
            raise FileNotFoundError(fromfile, find_similar_files(fromfile)) from e
        except PermissionError as e:
            raise PermissionError(fromfile, operation="read") from e

        inplace = tofile is None
        if inplace:
            if is_uri(fromfile):
                raise ValidationError(
                    "In-place repack requires a local file path.",
                    field="input",
                )
            tofile = fromfile

        assert tofile is not None

        if is_uri(tofile):
            raise ValidationError(
                "Remote output URIs are not supported by repack; use a local path.",
                field="output",
            )

        detection = self._detect(fromfile)
        out_codec = self._codec_from_path(tofile)
        in_codec_id = self._codec_id(detection.get("codec"))
        datatype_id = self._datatype_id(detection.get("datatype"))

        compression_override = get_option(options, "compression")
        if compression_override:
            compression_override = str(compression_override).lower()

        # Decide strategy.
        if datatype_id in BUILT_IN_COMPRESSION_FORMATS and not in_codec_id:
            mode = "builtin"
            target_codec = None
        elif in_codec_id or out_codec or compression_override:
            mode = "container"
            if compression_override:
                target_codec = self._normalize_codec(compression_override)
            elif out_codec:
                target_codec = out_codec
            else:
                target_codec = in_codec_id
            if not target_codec:
                raise ValidationError(
                    "Could not determine target compression codec.",
                    field="compression",
                    suggestions=sorted(CODEC_ALIASES.keys()),
                )
        else:
            raise ValidationError(
                f"'{fromfile}' has no container compression and is not a "
                f"built-in-compression format (parquet/orc/avro).",
                field="input",
                suggestions=[
                    f"undatum convert {fromfile} {fromfile}.gz",
                    f"undatum repack {fromfile} {fromfile}.zst",
                    "undatum convert data.csv data.parquet  # then undatum repack data.parquet",
                ],
            )

        level = get_option(options, "level")
        show_progress = get_option(options, "progress")
        if show_progress is None:
            show_progress = True

        write_path = tofile
        temp_path = None
        if inplace:
            temp_path = self._temp_path_for(fromfile)
            write_path = temp_path

        try:
            if mode == "container":
                result = self._repack_container(
                    fromfile,
                    write_path,
                    source_codec_id=in_codec_id,
                    target_codec_id=target_codec,
                    level=level,
                    show_progress=show_progress,
                )
            else:
                result = self._repack_builtin(
                    fromfile,
                    write_path,
                    datatype_id=datatype_id,
                    compression=compression_override,
                    level=level,
                    show_progress=show_progress,
                )

            if temp_path is not None:
                os.replace(temp_path, fromfile)
                temp_path = None
                result["output"] = fromfile
            else:
                result["output"] = tofile

            result["input"] = fromfile
            result["mode"] = mode
            self._log_summary(result)
            return result
        finally:
            if temp_path is not None and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError as exc:
                    logger.warning("Failed to remove temp file %s: %s", temp_path, exc)

    def _repack_container(
        self,
        fromfile: str,
        tofile: str,
        *,
        source_codec_id: str | None,
        target_codec_id: str,
        level: int | None,
        show_progress: bool,
    ) -> dict[str, Any]:
        """Byte-stream decompress/recompress using iterabledata codecs."""
        codec_args = self._codec_write_options(target_codec_id, level)
        reader = None
        raw_src = None
        writer = None
        bytes_in = 0
        try:
            if source_codec_id:
                reader_cls = self._load_codec_class(source_codec_id)
                reader = reader_cls(filename=fromfile, mode="rb", options={})
                reader.open()
                src = reader.fileobj()
                # Decompressed size is unknown; avoid a misleading compressed-size total.
                total = None
            else:
                raw_src = open(fromfile, "rb")
                src = raw_src
                total = os.path.getsize(fromfile) if os.path.exists(fromfile) else None

            writer_cls = self._load_codec_class(target_codec_id)
            writer = writer_cls(filename=tofile, mode="wb", options=codec_args)
            writer.open()
            dest = writer.fileobj()

            with progress_bar(
                total=total,
                desc="Repacking",
                unit="B",
                show_progress=show_progress,
            ) as pbar:
                while True:
                    chunk = src.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    dest.write(chunk)
                    bytes_in += len(chunk)
                    update_progress(pbar, n=len(chunk))
        finally:
            if writer is not None:
                writer.close()
            if reader is not None:
                reader.close()
            if raw_src is not None:
                raw_src.close()

        return {
            "bytes_read": bytes_in,
            "bytes_written": os.path.getsize(tofile) if os.path.exists(tofile) else None,
            "codec": target_codec_id,
            "level": codec_args.get("compression_level"),
            "profile": codec_args.get("profile"),
        }

    def _repack_builtin(
        self,
        fromfile: str,
        tofile: str,
        *,
        datatype_id: str,
        compression: str | None,
        level: int | None,
        show_progress: bool,
    ) -> dict[str, Any]:
        """Rewrite formats that embed compression (parquet/orc/avro)."""
        if datatype_id == "parquet":
            return self._repack_parquet(
                fromfile,
                tofile,
                compression=compression or DEFAULT_PARQUET_COMPRESSION,
                level=level,
                show_progress=show_progress,
            )
        if datatype_id == "orc":
            return self._repack_orc(
                fromfile,
                tofile,
                compression=compression,
                show_progress=show_progress,
            )
        if datatype_id == "avro":
            return self._repack_via_convert(
                fromfile,
                tofile,
                compression=compression or DEFAULT_AVRO_COMPRESSION,
                show_progress=show_progress,
            )
        raise FormatError(fromfile, datatype_id, list(BUILT_IN_COMPRESSION_FORMATS))

    def _repack_parquet(
        self,
        fromfile: str,
        tofile: str,
        *,
        compression: str,
        level: int | None,
        show_progress: bool,
    ) -> dict[str, Any]:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            from ..common.errors import DependencyError

            raise DependencyError(
                "pyarrow",
                feature="Parquet repack",
                install_command="pip install pyarrow",
            ) from exc

        # Default "max" for zstd/gzip/brotli when level omitted.
        write_kwargs: dict[str, Any] = {"compression": compression}
        if level is not None:
            write_kwargs["compression_level"] = int(level)
        elif compression.lower() in {"zstd", "gzip", "brotli"}:
            try:
                from iterable.codecs.profiles import PROFILE_LEVELS

                profile_key = "gzip" if compression.lower() == "gzip" else compression.lower()
                max_level = PROFILE_LEVELS.get(profile_key, {}).get("max")
                if max_level is not None:
                    write_kwargs["compression_level"] = max_level
            except Exception:  # noqa: BLE001 - profiles optional
                logger.debug("Could not resolve max parquet compression level", exc_info=True)

        pf = pq.ParquetFile(fromfile)
        writer = None
        rows = 0
        try:
            total = pf.metadata.num_rows if pf.metadata is not None else None
            with progress_bar(
                total=total,
                desc="Repacking Parquet",
                unit="rows",
                show_progress=show_progress,
            ) as pbar:
                for batch in pf.iter_batches():
                    if writer is None:
                        writer = pq.ParquetWriter(tofile, batch.schema, **write_kwargs)
                    writer.write_batch(batch)
                    rows += batch.num_rows
                    update_progress(pbar, n=batch.num_rows)
        finally:
            if writer is not None:
                writer.close()

        return {
            "rows": rows,
            "bytes_written": os.path.getsize(tofile) if os.path.exists(tofile) else None,
            "compression": compression,
            "level": write_kwargs.get("compression_level"),
        }

    def _repack_orc(
        self,
        fromfile: str,
        tofile: str,
        *,
        compression: str | None,
        show_progress: bool,
    ) -> dict[str, Any]:
        orc_compression = DEFAULT_ORC_COMPRESSION
        if compression:
            key = compression.lower()
            if key in ORC_COMPRESSION_NAMES:
                orc_compression = ORC_COMPRESSION_NAMES[key]
            elif key.isdigit():
                orc_compression = int(key)
            else:
                raise ValidationError(
                    f"Unsupported ORC compression '{compression}'.",
                    field="compression",
                    suggestions=list(ORC_COMPRESSION_NAMES.keys()),
                )
        return self._repack_via_convert(
            fromfile,
            tofile,
            compression=orc_compression,
            show_progress=show_progress,
        )

    def _repack_via_convert(
        self,
        fromfile: str,
        tofile: str,
        *,
        compression: Any,
        show_progress: bool,
    ) -> dict[str, Any]:
        from iterable.convert import convert as iterable_convert

        result = iterable_convert(
            fromfile,
            tofile,
            toiterableargs={"compression": compression},
            silent=not show_progress,
            show_progress=show_progress,
            atomic=False,
        )
        return {
            "rows": getattr(result, "rows_out", None),
            "bytes_written": getattr(result, "bytes_written", None)
            or (os.path.getsize(tofile) if os.path.exists(tofile) else None),
            "compression": compression,
        }

    @staticmethod
    def _codec_write_options(codec_id: str, level: int | None) -> dict[str, Any]:
        """Build codecargs for max profile or an explicit level."""
        if level is not None:
            return {"compression_level": int(level)}
        return {"profile": "max"}

    @staticmethod
    def _load_codec_class(codec_id: str):
        """Resolve an iterabledata codec class from its registry id."""
        import importlib

        from iterable.helpers.detect import CODEC_REGISTRY

        if codec_id not in CODEC_REGISTRY:
            raise ValidationError(
                f"Unsupported compression codec '{codec_id}'.",
                field="compression",
                suggestions=sorted(CODEC_REGISTRY.keys()),
            )
        module_path, symbol = CODEC_REGISTRY[codec_id]
        try:
            module = importlib.import_module(module_path)
            return getattr(module, symbol)
        except ImportError as exc:
            from ..common.errors import DependencyError

            raise DependencyError(
                symbol,
                feature=f"{codec_id} compression",
                install_command=f"pip install iterabledata  # codec dependency for {codec_id}",
            ) from exc

    @staticmethod
    def _detect(path: str) -> dict[str, Any]:
        try:
            from iterable.helpers.detect import detect_file_type

            result = detect_file_type(path)
            if result.get("success"):
                return result
        except Exception as exc:  # noqa: BLE001 - fall back to extension heuristics
            logger.debug("detect_file_type failed for %s: %s", path, exc)
        return {"success": False, "codec": None, "datatype": None}

    @staticmethod
    def _codec_id(codec_cls: Any) -> str | None:
        if codec_cls is None:
            return None
        try:
            raw = codec_cls.id()
        except Exception:  # noqa: BLE001
            return None
        return Repacker._normalize_codec(raw)

    @staticmethod
    def _datatype_id(datatype_cls: Any) -> str | None:
        if datatype_cls is None:
            return None
        try:
            return str(datatype_cls.id()).lower()
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _normalize_codec(codec: str) -> str | None:
        key = str(codec).lower().lstrip(".")
        if key in ("raw", "none", ""):
            return None
        return CODEC_ALIASES.get(key, key if key in COMPRESSED_FILE_TYPES else None)

    @classmethod
    def _codec_from_path(cls, path: str) -> str | None:
        name = Path(path).name.lower()
        # Prefer longest known suffix (e.g. .tar.gz handled as .gz).
        parts = name.split(".")
        if len(parts) < 2:
            return None
        ext = parts[-1]
        return cls._normalize_codec(ext)

    @staticmethod
    def _temp_path_for(path: str) -> str:
        directory = os.path.dirname(os.path.abspath(path)) or "."
        fd, temp_path = tempfile.mkstemp(
            prefix=".undatum_repack_",
            suffix=Path(path).suffix or ".tmp",
            dir=directory,
        )
        os.close(fd)
        return temp_path

    @staticmethod
    def _log_summary(result: dict[str, Any]) -> None:
        parts = [f"Repacked {result.get('input')} → {result.get('output')}"]
        if result.get("bytes_written") is not None:
            parts.append(f"({result['bytes_written']:,} bytes)")
        if result.get("codec"):
            parts.append(f"codec={result['codec']}")
        if result.get("compression"):
            parts.append(f"compression={result['compression']}")
        if result.get("level") is not None:
            parts.append(f"level={result['level']}")
        logger.info(" ".join(parts))
