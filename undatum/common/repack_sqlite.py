"""Offline SQLite VACUUM INTO with schema, row identity and typed value verification."""

from __future__ import annotations

import hashlib
import json
import os
import re
import struct
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ConfigurationError, DependencyError, UndatumError, ValidationError
from .file_snapshot import FileSnapshot
from .repack_types import RepackBudget, RepackError, RepackOptions, RepackPlan

SIDECARS = ("-wal", "-shm", "-journal")
MAX_CATALOG_BYTES = 4 * 1024 * 1024
MAX_COLUMNS = 999
MAX_BATCH_ROWS = 1024
_SETTINGS = ("application_id", "user_version", "encoding", "page_size", "auto_vacuum")


@dataclass(frozen=True)
class SQLiteTable:
    """Original column definitions and a deterministic accessible record identity."""

    name: str
    columns: tuple[tuple[Any, ...], ...]
    rowid: str | None
    primary_key: tuple[str, ...]

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(str(column[1]) for column in self.columns)


@dataclass(frozen=True)
class SQLiteManifest:
    """Significant catalog and application settings; physical page locations may change."""

    catalog: tuple[tuple[Any, ...], ...]
    tables: tuple[SQLiteTable, ...]
    settings: tuple[tuple[str, Any], ...]
    journal_mode: str


@dataclass(frozen=True)
class SQLiteProof:
    """Framed typed values and row identities from one complete table scan."""

    rows: int
    decoded_bytes: int
    digest: str


def _runtime() -> Any:
    try:
        import sqlite3
    except ImportError as exc:
        raise DependencyError(
            "sqlite3",
            feature="SQLite repack",
            install_command="Install a Python distribution with SQLite support.",
        ) from exc
    if sqlite3.sqlite_version_info < (3, 31, 0):
        raise ConfigurationError(
            "SQLite repack requires SQLite >=3.31; upgrade Python's SQLite runtime."
        )
    return sqlite3


def require_offline(*paths: str) -> None:
    """Refuse all live/recovery sidecars, including those beside resolved aliases."""
    for path in paths:
        for resolved in {os.path.abspath(path), os.path.realpath(path)}:
            for suffix in SIDECARS:
                if os.path.lexists(resolved + suffix):
                    raise ValidationError(
                        f"SQLite sidecar '{path}{suffix}' exists; close database users and "
                        "recover/checkpoint the database before repacking offline.",
                        field="input/output",
                    )


def remove_candidate_sidecars(target: str) -> None:
    """Remove only the WAL, SHM and rollback journal owned by the private candidate."""
    for suffix in SIDECARS:
        try:
            os.unlink(target + suffix)
        except FileNotFoundError:
            pass


@contextmanager
def _connection(
    path: str, budget: RepackBudget | None = None, *, writable: bool = False
) -> Iterator[Any]:
    sqlite3 = _runtime()
    require_offline(path)
    failure: list[BaseException] = []

    def progress() -> int:
        try:
            if budget is not None:
                budget.check()
        except BaseException as exc:
            failure.append(exc)
            return 1
        return 0

    uri = Path(path).resolve().as_uri() + ("?mode=rw" if writable else "?mode=ro&immutable=1")
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=0.1, isolation_level=None)
        try:
            connection.set_progress_handler(progress, 1000)
            disable_extensions = getattr(connection, "enable_load_extension", None)
            if disable_extensions is not None:
                disable_extensions(False)
            connection.execute("PRAGMA trusted_schema=OFF")
            if connection.execute("PRAGMA trusted_schema").fetchone() != (0,):
                raise ConfigurationError("SQLite runtime cannot disable trusted_schema.")
            connection.execute("PRAGMA temp_store=MEMORY")
            connection.execute("PRAGMA cache_size=-8192")
            yield connection
        finally:
            connection.set_progress_handler(None, 0)
            connection.close()
    except sqlite3.Error as exc:
        if failure:
            raise failure[0] from exc
        if "no such collation" in str(exc).lower() or "no such function" in str(exc).lower():
            raise ValidationError(f"Unsupported SQLite schema: {exc}", field="input") from exc
        raise UndatumError(f"SQLite repack failed: {exc}", exit_code=3) from exc


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _journal_mode(path: str) -> str:
    with open(path, "rb") as source:
        header = source.read(21)
    if (
        len(header) < 21
        or not header.startswith(b"SQLite format 3\0")
        or header[18:20] not in (b"\1\1", b"\2\2")
    ):
        raise RepackError("Invalid or unsupported SQLite database header.")
    if header[20]:
        raise ValidationError("SQLite reserved page bytes require a preserving extension adapter.")
    return "wal" if header[18:20] == b"\2\2" else "delete"


def _table(connection: Any, name: str) -> SQLiteTable:
    columns = tuple(connection.execute("PRAGMA table_xinfo(" + _quote(name) + ")"))
    if not columns or len(columns) > MAX_COLUMNS:
        raise ValidationError("SQLite tables must have 1 through 999 supported columns.")
    if any(column[6] not in (0, 2, 3) for column in columns):
        raise ValidationError("Unsupported hidden SQLite columns.", field="input")
    primary_key = tuple(str(c[1]) for c in sorted(columns, key=lambda c: c[5]) if c[5])
    # A rowid table's primary-key index has an auxiliary cid=-1 rowid column;
    # WITHOUT ROWID primary indexes instead cover their actual table columns.
    without_rowid = False
    for index in connection.execute("PRAGMA index_list(" + _quote(name) + ")"):
        if index[3] == "pk":
            entries = tuple(connection.execute("PRAGMA index_xinfo(" + _quote(index[1]) + ")"))
            without_rowid = bool(entries) and all(entry[1] != -1 for entry in entries)
            break
    rowid = None
    if not without_rowid:
        names = {str(column[1]).lower() for column in columns}
        rowid = next((alias for alias in ("rowid", "_rowid_", "oid") if alias not in names), None)
        if rowid is None:
            raise ValidationError("All SQLite hidden rowid aliases are shadowed.", field="input")
    elif not primary_key:
        raise RepackError("SQLite WITHOUT ROWID table has no accessible primary key.")
    return SQLiteTable(name, columns, rowid, primary_key)


def _query(table: SQLiteTable) -> str:
    fields = [table.rowid] if table.rowid else []
    for name in table.names:
        column = _quote(name)
        fields.extend(
            (
                f"typeof({column})",
                f"CASE typeof({column}) WHEN 'text' THEN CAST({column} AS BLOB) ELSE {column} END",
            )
        )
    order = table.rowid or ", ".join(_quote(name) + " COLLATE BINARY" for name in table.primary_key)
    return "SELECT " + ", ".join(fields) + " FROM " + _quote(table.name) + " ORDER BY " + order


def _manifest(connection: Any, path: str) -> SQLiteManifest:
    catalog: list[tuple[Any, ...]] = []
    tables: list[SQLiteTable] = []
    size = 0
    for row in connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
    ):
        size += len(json.dumps(row, ensure_ascii=True).encode("ascii"))
        if size > MAX_CATALOG_BYTES:
            raise ValidationError("SQLite catalog exceeds the supported 4 MiB limit.")
        catalog.append(row)
    for kind, name, _owner, sql in catalog:
        if kind != "table":
            continue
        ddl = re.sub(r"/\*.*?\*/|--[^\n]*", " ", sql or "", flags=re.DOTALL)
        if re.match(r"\s*CREATE\s+VIRTUAL\s+TABLE\b", ddl, re.IGNORECASE):
            raise ValidationError("Unsupported SQLite virtual table.", field="input")
        table = _table(connection, name)
        size += len(json.dumps(table.columns, ensure_ascii=True).encode("ascii"))
        if size > MAX_CATALOG_BYTES:
            raise ValidationError("SQLite catalog exceeds the supported 4 MiB limit.")
        # Prepare only: validate declared collations and our projection without
        # executing views, triggers or a table-sized sort during preflight.
        connection.execute("EXPLAIN " + _query(table) + " LIMIT 0").close()
        connection.execute(
            "EXPLAIN SELECT * FROM "
            + _quote(name)
            + " ORDER BY "
            + ", ".join(_quote(column) for column in table.names)
            + " LIMIT 0"
        ).close()
        tables.append(table)
    settings = tuple((key, connection.execute("PRAGMA " + key).fetchone()[0]) for key in _SETTINGS)
    page_size = dict(settings)["page_size"]
    page_count = connection.execute("PRAGMA page_count").fetchone()[0]
    if os.path.getsize(path) != page_size * page_count:
        raise ValidationError("Unsupported SQLite trailing data or incomplete physical pages.")
    return SQLiteManifest(tuple(catalog), tuple(tables), settings, _journal_mode(path))


def prepare_sqlite(source: str, destination: str, inplace: bool, options: RepackOptions) -> None:
    """Validate the offline replacement assertion and supported native profile before staging."""
    if options.compression is not None and (
        not isinstance(options.compression, str)
        or options.compression.lower() not in {"none", "auto"}
    ):
        raise ValidationError("SQLite compaction supports none/auto only.", field="compression")
    if options.level is not None:
        raise ValidationError("SQLite compaction has no compression level.", field="level")
    if options.threads != 1:
        raise ValidationError("SQLite compaction uses one engine thread.", field="threads")
    if (
        not options.dry_run
        and (inplace or os.path.lexists(destination))
        and not options.sqlite_offline
    ):
        raise ValidationError(
            "Replacing an existing SQLite path requires --sqlite-offline / sqlite_offline=True "
            "after closing all database users; alternatively choose a new OUTPUT or --dry-run.",
            field="sqlite_offline",
        )
    require_offline(source, destination)
    _journal_mode(source)
    with _connection(source) as connection:
        _manifest(connection, source)


def _integrity(connection: Any) -> None:
    if connection.execute("PRAGMA integrity_check(1)").fetchall() != [("ok",)]:
        raise RepackError("SQLite integrity_check failed; publication refused.")


def _payload(kind: str, value: Any) -> tuple[bytes, bytes]:
    if kind == "null" and value is None:
        return b"N", b""
    if kind == "integer" and type(value) is int:
        return b"I" + struct.pack("<q", value), b""
    if kind == "real" and type(value) is float:
        return b"R" + struct.pack("<d", value), b""
    if kind in {"text", "blob"} and isinstance(value, bytes):
        return (b"T" if kind == "text" else b"B") + struct.pack("<Q", len(value)), value
    raise RepackError(f"Unsupported SQLite verification value: {kind} / {type(value).__name__}")


def _scan(
    connection: Any, table: SQLiteTable, options: RepackOptions, budget: RepackBudget
) -> SQLiteProof:
    digest = hashlib.sha256(b"sqlite-repack-values/1")
    rows, decoded = 0, 0
    cursor = connection.execute(_query(table))
    try:
        while batch := cursor.fetchmany(min(options.batch_size, MAX_BATCH_ROWS)):
            batch_bytes = 0
            for row in batch:
                digest.update(b"row")
                offset = 1 if table.rowid else 0
                if table.rowid:
                    header, payload = _payload("integer", row[0])
                    digest.update(header)
                    digest.update(payload)
                    batch_bytes += len(header) + len(payload)
                for index in range(offset, len(row), 2):
                    header, payload = _payload(row[index], row[index + 1])
                    digest.update(header)
                    digest.update(payload)
                    batch_bytes += len(header) + len(payload)
                rows += 1
            decoded += batch_bytes
            budget.check(batch_bytes)
    finally:
        cursor.close()
    digest.update(struct.pack("<Q", rows))
    return SQLiteProof(rows, decoded, digest.hexdigest())


def _rewrite(source: str, target: str, mode: str, budget: RepackBudget) -> None:
    with _connection(source, budget) as connection:
        connection.execute("VACUUM INTO ?", (os.path.abspath(target),))
        budget.check()
    if mode == "wal":
        with _connection(target, budget, writable=True) as connection:
            if connection.execute("PRAGMA journal_mode=WAL").fetchone() != ("wal",):
                raise RepackError("SQLite candidate could not retain WAL mode.")
            budget.check()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            budget.check()


def repack_sqlite(
    plan: RepackPlan, target: str, options: RepackOptions, budget: RepackBudget
) -> dict[str, Any]:
    """Compact an offline database and independently prove its complete stored content."""
    started = time.monotonic()
    with _connection(plan.source, budget) as connection:
        manifest = _manifest(connection, plan.source)
        _integrity(connection)
        expected = tuple(_scan(connection, table, options, budget) for table in manifest.tables)
    _rewrite(plan.source, target, manifest.journal_mode, budget)
    encode_seconds = time.monotonic() - started
    verified = time.monotonic()
    require_offline(plan.source, target)
    snapshot = FileSnapshot.capture(target, budget.check)
    with _connection(plan.source, budget) as source, _connection(target, budget) as candidate:
        if _manifest(source, plan.source) != manifest or _manifest(candidate, target) != manifest:
            raise RepackError("Candidate changed SQLite schema, settings or journal mode.")
        _integrity(source)
        _integrity(candidate)
        for table, proof in zip(manifest.tables, expected, strict=True):
            if (
                _scan(source, table, options, budget) != proof
                or _scan(candidate, table, options, budget) != proof
            ):
                raise RepackError("Candidate changed SQLite rowids, storage classes or values.")
    require_offline(plan.source, target)
    snapshot.require_unchanged(check=budget.check)
    return {
        "rows": sum(proof.rows for proof in expected),
        "decoded_bytes": sum(proof.decoded_bytes for proof in expected),
        "verification": {
            "method": "sqlite-schema-settings-rowids-storage-classes-values",
            "passed": True,
            "candidate_sha256": snapshot.digest,
            "sqlite_version": _runtime().sqlite_version,
            "journal_mode": manifest.journal_mode,
        },
        "timings": {
            "encode_seconds": encode_seconds,
            "verify_seconds": time.monotonic() - verified,
        },
    }
