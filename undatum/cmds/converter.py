"""File format conversion module."""

import logging
import xml.etree.ElementTree as etree
from collections import defaultdict
from typing import Any

import pandas

from ..common.command_utils import (
    ITERABLE_OPTIONS_KEYS,
    get_iterable_options,
    resolve_csv_delimiter,
)
from ..common.engine_selector import detect_engine, is_format_supported_by_duckdb
from ..common.errors import (
    FileNotFoundError,
    FormatError,
    PermissionError,
    ValidationError,
    find_similar_files,
)
from ..common.path_utils import validate_file_path
from ..constants import COMPRESSED_FILE_TYPES, DUCKABLE_FILE_TYPES, SUPPORTED_FILE_TYPES
from ..utils import get_file_type, get_option

# Preferred order for suggesting writable conversion targets. The list is
# filtered against iterabledata's actual write capabilities at runtime so the
# suggestions can never contradict the read-only check below.
_PREFERRED_WRITABLE_FORMATS = ["csv", "jsonl", "json", "parquet", "orc", "avro", "bson"]


def _writable_format_suggestions() -> list[str]:
    """Return the preferred writable formats that iterabledata can actually write.

    Falls back to the full preferred list if capability reporting is unavailable.
    """
    try:
        from iterable.helpers.capabilities import supports_write
    except Exception:  # noqa: BLE001 - capabilities unavailable: use static list
        return list(_PREFERRED_WRITABLE_FORMATS)
    writable = []
    for fmt in _PREFERRED_WRITABLE_FORMATS:
        try:
            if supports_write(fmt) is not False:
                writable.append(fmt)
        except Exception:  # noqa: BLE001 - unknown format: skip it
            continue
    return writable or list(_PREFERRED_WRITABLE_FORMATS)


# Formats commonly suggested as writable conversion targets (capability-filtered).
WRITABLE_FORMAT_SUGGESTIONS = _writable_format_suggestions()

# Formats that report as writable but require an externally-supplied schema or
# message class to serialize (e.g. a compiled protobuf message). They cannot be
# used as generic conversion targets, so we fail fast with an actionable error
# instead of surfacing the engine's "requires '<x>' parameter" error. Keys are
# resolved output format ids/extensions; values name the required input.
_SCHEMA_REQUIRED_FORMATS: dict[str, str] = {
    "pb": "a compiled protobuf message class (message_class)",
    "protobuf": "a compiled protobuf message class (message_class)",
    "capnp": "a Cap'n Proto schema (schema_file and schema_name)",
    "thrift": "a generated Thrift struct class (struct_class)",
}

DEFAULT_BATCH_SIZE = 50000
LOW_MEMORY_BATCH_SIZE = 5000
DEFAULT_HEADERS_DETECT_LIMIT = 1000

_DEPRECATED_CONVERT_OPTIONS: dict[str, Any] = {
    "skip_end_rows": 0,
    "fields": None,
    "zipfile": False,
}


def df_to_pyorc_schema(df):
    """Extracts column information from pandas dataframe and generate pyorc schema"""
    struct_schema = []
    for k, v in df.dtypes.to_dict().items():
        v = str(v)
        if v == "float64":
            struct_schema.append(f"{k}:float")
        elif v == "float32":
            struct_schema.append(f"{k}:float")
        elif v == "datetime64[ns]":
            struct_schema.append(f"{k}:timestamp")
        elif v == "int32":
            struct_schema.append(f"{k}:int")
        elif v == "int64":
            struct_schema.append(f"{k}:int")
        else:
            struct_schema.append(f"{k}:string")
    return struct_schema


def etree_to_dict(t, prefix_strip=True):
    """Convert XML element tree to dictionary."""
    tag = t.tag if not prefix_strip else t.tag.rsplit("}", 1)[-1]
    d = {tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                if prefix_strip:
                    # Remove XML namespace prefix (e.g., '{http://...}tagname' -> 'tagname')
                    k = k.rsplit("}", 1)[-1]
                dd[k].append(v)
        d = {tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[tag].update(("@" + k.rsplit("}", 1)[-1], v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            tag = tag.rsplit("}", 1)[-1]
            if text:
                d[tag]["#text"] = text
        else:
            d[tag] = text
    return d


def _is_flat(item):
    """Check if dictionary item is flat (no nested structures)."""
    for _k, v in item.items():
        if isinstance(v, (dict, tuple, list)):
            return False
    return True


def express_analyze_jsonl(filename, itemlimit=100):
    """Quickly analyze JSONL file structure."""
    import orjson

    isflat = True
    n = 0
    keys = set()
    with open(filename, encoding="utf8") as f:
        for line in f:
            n += 1
            if n > itemlimit:
                break
            record = orjson.loads(line)
            if isflat:
                if not _is_flat(record):
                    isflat = False
            if len(keys) == 0:
                keys = set(record.keys())
            else:
                keys = keys.union(set(record.keys()))
    keys = list(keys)
    keys.sort()
    return {"isflat": isflat, "keys": keys}


def make_flat(item):
    """Flatten nested structures in dictionary by converting to strings."""
    result = {}
    for k, v in item.items():
        if isinstance(v, (tuple, list, dict)):
            result[k] = str(v)
        else:
            result[k] = v
    return result


def _warn_deprecated_convert_options(options: dict) -> None:
    """Warn when legacy convert flags are set but not supported by the engine."""
    for key, default in _DEPRECATED_CONVERT_OPTIONS.items():
        value = options.get(key)
        if value not in (None, default):
            logging.warning(
                "Option '%s' is deprecated for convert and has no effect with the "
                "iterabledata engine",
                key,
            )


def _format_bytes(num_bytes: int | None) -> str:
    if num_bytes is None:
        return ""
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    return f"{num_bytes / (1024 * 1024):.1f} MB"


def format_conversion_summary(result) -> str:
    """Format a one-line summary from iterabledata's ConversionResult."""
    parts = [f"Converted {result.rows_out:,} rows in {result.elapsed_seconds:.1f}s"]
    if result.bytes_written:
        parts.append(f"({_format_bytes(result.bytes_written)} written)")
    if result.errors:
        parts.append(f"({len(result.errors)} errors)")
    return " ".join(parts)


def format_bulk_conversion_summary(result) -> str:
    """Format a one-line summary from iterabledata's BulkConversionResult."""
    parts = [
        f"Bulk converted {result.successful_files}/{result.total_files} files",
        f"({result.total_rows_out:,} rows in {result.total_elapsed_seconds:.1f}s)",
    ]
    if result.failed_files:
        parts.append(f"({result.failed_files} failed)")
    return " ".join(parts)


class Converter:
    """File format converter handler."""

    def __init__(self, batch_size=DEFAULT_BATCH_SIZE):
        self.batch_size = batch_size

    @staticmethod
    def _resolve_output_format(tofile, options: dict) -> str | None:
        """Determine the output data format id from the target path or options.

        Strips a trailing compression-codec extension (e.g. ``.gz``) so that
        ``out.csv.gz`` resolves to ``csv``.
        """
        fmt = options.get("format_out")
        if fmt:
            return fmt.lower()
        basename = str(tofile).rsplit("/", 1)[-1]
        parts = basename.split(".")
        if len(parts) >= 2 and parts[-1].lower() in COMPRESSED_FILE_TYPES:
            parts = parts[:-1]
        if len(parts) >= 2:
            return parts[-1].lower()
        return None

    def _check_output_schema_required(self, tofile, options: dict) -> None:
        """Raise a clear error when the output format needs an external schema.

        Some formats (protobuf, Cap'n Proto, Thrift) report as writable but
        cannot serialize plain records without a compiled schema/message class,
        so they are not valid generic conversion targets.
        """
        out_fmt = self._resolve_output_format(tofile, options)
        if not out_fmt:
            return
        requirement = _SCHEMA_REQUIRED_FORMATS.get(out_fmt)
        if requirement is not None:
            raise ValidationError(
                f"Output format '{out_fmt}' requires {requirement} and cannot be used as a "
                f"generic conversion target. Convert to a self-describing format instead.",
                field="output",
                suggestions=[
                    f for f in WRITABLE_FORMAT_SUGGESTIONS if f not in _SCHEMA_REQUIRED_FORMATS
                ],
            )

    def _check_output_writable(self, tofile, options: dict) -> None:
        """Raise a clear error when the output format is read-only.

        Uses iterabledata's capability reporting so undatum fails fast with an
        actionable message instead of a cryptic engine error.

        Parquet is treated as writable when ``pyarrow`` is importable, even if
        iterabledata's capability probe reports False (common when pyarrow was
        missing at probe time). DuckDB can also write Parquet via COPY.
        """
        out_fmt = self._resolve_output_format(tofile, options)
        if not out_fmt:
            return
        if out_fmt == "parquet":
            try:
                import pyarrow  # noqa: F401

                return
            except ImportError:
                from ..common.errors import DependencyError

                raise DependencyError(
                    "pyarrow",
                    feature="Parquet write",
                    install_command="pip install pyarrow",
                ) from None
        try:
            from iterable.helpers.capabilities import supports_write

            writable = supports_write(out_fmt)
        except Exception:  # noqa: BLE001 - unknown format: let the engine decide
            return
        if writable is False:
            raise ValidationError(
                f"Output format '{out_fmt}' is read-only; undatum can read it but cannot "
                f"write to it.",
                field="output",
                suggestions=WRITABLE_FORMAT_SUGGESTIONS,
            )

    def _build_convert_kwargs(
        self, options: dict, limit, fromfile: str | None = None
    ) -> dict:
        """Translate undatum convert options into iterabledata convert kwargs."""
        iterableargs = get_iterable_options(options)
        toiterableargs: dict = {}

        format_in = options.get("format_in")
        format_out = options.get("format_out")
        if format_in:
            iterableargs["format"] = str(format_in).lower()
        if format_out:
            toiterableargs["format"] = str(format_out).lower()

        compression = options.get("compression")
        if compression:
            toiterableargs["compression"] = compression

        filetype = iterableargs.get("format") or (
            get_file_type(fromfile) if fromfile else None
        )
        delimiter = resolve_csv_delimiter(iterableargs, filename=fromfile, filetype=filetype)
        if delimiter:
            iterableargs["delimiter"] = delimiter
            toiterableargs["delimiter"] = delimiter

        is_flatten = bool(get_option(options, "flatten"))
        show_progress = get_option(options, "progress")
        if show_progress is None:
            show_progress = True

        scan_limit = options.get("scan_limit")
        if scan_limit is None:
            scan_limit = limit if limit is not None else DEFAULT_HEADERS_DETECT_LIMIT
        batch_size = options.get("batch_size", self.batch_size)
        if options.get("low_memory") and (
            options.get("batch_size") is None or batch_size == self.batch_size == DEFAULT_BATCH_SIZE
        ):
            # Prefer smaller batches unless the user explicitly set batch_size.
            if options.get("batch_size") is None:
                batch_size = LOW_MEMORY_BATCH_SIZE

        return {
            "iterableargs": iterableargs,
            "toiterableargs": toiterableargs,
            "scan_limit": scan_limit,
            "batch_size": batch_size,
            "is_flatten": is_flatten,
            "silent": not show_progress,
            "show_progress": show_progress,
            "atomic": bool(options.get("atomic", False)),
        }

    def _try_duckdb_convert(self, fromfile, tofile, options: dict) -> bool:
        """Attempt DuckDB spill-to-disk conversion for duckable → Parquet/CSV/JSONL.

        Returns:
            True if conversion completed via DuckDB, False if caller should fall back.
        """
        out_fmt = self._resolve_output_format(tofile, options)
        if out_fmt not in ("parquet", "csv", "json", "jsonl"):
            return False

        engine = options.get("engine") or "auto"
        low_memory = bool(options.get("low_memory"))
        # Prefer DuckDB for low-memory parquet/csv/jsonl or when engine requests it.
        if engine == "python":
            return False
        if engine == "auto" and not low_memory and out_fmt != "parquet":
            # Keep existing iterable path as default for non-parquet unless low-memory.
            return False

        detected = detect_engine(fromfile, engine if engine != "auto" else "auto", operation="convert")
        if detected != "duckdb" and not (low_memory and engine == "auto"):
            # Still try duckdb if format looks duckable under low-memory.
            ftype = get_file_type(fromfile)
            if not is_format_supported_by_duckdb(ftype, "raw") and ftype not in DUCKABLE_FILE_TYPES:
                return False

        try:
            from iterable.helpers.detect import detect_file_type

            from ..common.duckdb_config import create_duckdb_connection, get_duckdb_config_from_options

            ftype_info = detect_file_type(fromfile)
            filetype = options.get("format_in")
            compression = "raw"
            if not filetype and ftype_info.get("success"):
                filetype = ftype_info["datatype"].id()
                if ftype_info.get("codec") is not None:
                    compression = ftype_info["codec"].id()
            filetype = (filetype or get_file_type(fromfile) or "").lower()
            if not is_format_supported_by_duckdb(filetype, compression):
                return False

            if filetype == "csv":
                read_expr = f"read_csv_auto('{fromfile}', all_varchar=true)"
            elif filetype in ("json", "jsonl"):
                read_expr = f"read_json_auto('{fromfile}')"
            elif filetype == "parquet":
                read_expr = f"read_parquet('{fromfile}')"
            else:
                return False

            duckdb_config = get_duckdb_config_from_options(options)
            if low_memory and not duckdb_config.get("memory"):
                duckdb_config["memory"] = "1GB"
            conn = create_duckdb_connection(**duckdb_config)
            try:
                query = f"SELECT * FROM {read_expr}"
                if out_fmt == "parquet":
                    conn.execute(f"COPY ({query}) TO '{tofile}' (FORMAT PARQUET)")
                elif out_fmt == "csv":
                    conn.execute(f"COPY ({query}) TO '{tofile}' (FORMAT CSV, HEADER)")
                else:
                    conn.execute(f"COPY ({query}) TO '{tofile}' (FORMAT JSON)")
            finally:
                conn.close()
            logging.info("convert: completed via DuckDB spill path (%s → %s)", filetype, out_fmt)
            return True
        except Exception as exc:  # noqa: BLE001 - fall back to iterable path
            logging.warning("DuckDB convert path failed, falling back to iterable: %s", exc)
            return False

    def convert(self, fromfile, tofile, options=None, limit=DEFAULT_HEADERS_DETECT_LIMIT):
        """Convert a file (or cloud/DB source) to another format.

        Delegates to iterabledata's ``iterable.convert.convert``, which performs
        schema scanning, batched streaming writes, optional flattening, progress
        reporting, atomic local writes, and native cloud (s3/gs/az) handling.
        Friendly errors for missing files and unsupported formats are preserved.

        When ``low_memory`` is set (or converting duckable formats to Parquet),
        prefers DuckDB ``COPY`` spill-to-disk when possible.

        Args:
            fromfile: Path or URI of the input source.
            tofile: Path or URI of the output file.
            options: Dictionary of conversion options (encoding, delimiter, etc.).
            limit: Maximum records to sample for schema detection.

        Returns:
            iterabledata ``ConversionResult`` with row/byte metrics, or ``None``
            when DuckDB completed the conversion.

        Raises:
            FileNotFoundError: If a local input file does not exist.
            PermissionError: If a local file cannot be read.
            FormatError: If the input format is not supported.
        """
        if options is None:
            options = {}

        _warn_deprecated_convert_options(options)

        # Fail fast with an actionable message if the output format is read-only
        # or requires an external schema (protobuf/capnp/thrift).
        self._check_output_writable(tofile, options)
        self._check_output_schema_required(tofile, options)

        # Validate local input files; cloud/DB URIs are validated by the engine.
        if "://" not in fromfile:
            try:
                validate_file_path(fromfile, check_read=True)
            except FileNotFoundError as e:
                suggestions = find_similar_files(fromfile)
                raise FileNotFoundError(fromfile, suggestions) from e
            except PermissionError as e:
                raise PermissionError(fromfile, operation="read") from e

        if "://" not in fromfile and self._try_duckdb_convert(fromfile, tofile, options):
            return None

        from iterable.convert import convert as iterable_convert

        convert_kwargs = self._build_convert_kwargs(options, limit, fromfile=fromfile)

        try:
            result = iterable_convert(fromfile, tofile, **convert_kwargs)
        except (FileNotFoundError, PermissionError, FormatError):
            raise
        except Exception as e:
            # Surface a helpful error when the input format is not supported.
            file_type = get_file_type(fromfile)
            if file_type is None or file_type not in SUPPORTED_FILE_TYPES:
                raise FormatError(fromfile, file_type, SUPPORTED_FILE_TYPES) from e
            # Actionable hint when Parquet support is missing
            if "pyarrow" in str(e).lower() or "parquet" in str(e).lower():
                try:
                    import pyarrow  # noqa: F401
                except ImportError as import_err:
                    from ..common.errors import DependencyError

                    raise DependencyError(
                        "pyarrow",
                        feature="Parquet read/write",
                        install_command="pip install pyarrow",
                    ) from import_err
            raise

        if options.get("summary", True):
            logging.info(format_conversion_summary(result))
        return result

    def bulk_convert(self, source, dest, options=None, to_ext=None, parallel=None):
        """Convert many files (directory or glob) via iterabledata's bulk_convert.

        Args:
            source: Directory path or glob pattern of input files.
            dest: Output directory.
            options: Conversion options (same as :meth:`convert`).
            to_ext: Target extension (e.g. ``"parquet"``); falls back to the
                ``format_out`` option.
            parallel: Force parallel execution; defaults to True when a thread
                count is supplied via options.

        Returns:
            iterabledata ``BulkConversionResult``.

        Raises:
            ValidationError: If no target extension can be determined.
        """
        if options is None:
            options = {}

        _warn_deprecated_convert_options(options)

        target_ext = to_ext or options.get("format_out")
        if not target_ext:
            raise ValidationError(
                "Bulk conversion requires a target extension (use --to-ext or --format-out).",
                field="output",
                suggestions=WRITABLE_FORMAT_SUGGESTIONS,
            )

        # Fail fast if the requested target format is read-only or schema-required.
        self._check_output_writable(f"x.{target_ext}", {})
        self._check_output_schema_required(f"x.{target_ext}", {})

        from iterable.convert import bulk_convert as iterable_bulk_convert

        convert_kwargs = self._build_convert_kwargs(
            options, DEFAULT_HEADERS_DETECT_LIMIT, fromfile=source
        )
        threads = options.get("threads")
        use_parallel = parallel if parallel is not None else bool(threads)

        logging.info("Bulk mode: converting %s -> %s (target: .%s)", source, dest, target_ext)
        result = iterable_bulk_convert(
            source,
            dest,
            to_ext=target_ext,
            parallel=use_parallel,
            workers=threads if threads else None,
            **convert_kwargs,
        )
        if options.get("summary", True):
            logging.info(format_bulk_conversion_summary(result))
        return result
