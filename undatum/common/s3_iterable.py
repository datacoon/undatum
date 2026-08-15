"""S3 integration for iterable data processing."""

import logging
import os
import tempfile
from contextlib import contextmanager
from typing import Any, Optional

from iterable.helpers.detect import open_iterable

from ..common.command_utils import apply_iterable_csv_delimiter, apply_table_selection
from ..common.path_utils import (
    is_native_cloud_uri,
    is_s3_uri,
    looks_like_missing_cloud_dep,
    missing_cloud_extra_error,
)
from ..formats.s3 import get_s3_client, parse_s3_uri


def _configure_iterable(iterable, path: str, iterableargs: dict) -> None:
    """Apply undatum-specific iterable configuration after open."""
    apply_iterable_csv_delimiter(iterable, path, iterableargs)


def _find_plugin_connector(path: str):
    """Return a ConnectorPlugin that can handle the path, if any.

    Only consulted for URI-style paths (scheme://...) that are not s3://,
    so local file handling stays on the fast path.
    """
    if "://" not in path or is_s3_uri(path):
        return None
    try:
        from ..cli.plugins_cli import plugin_manager

        return plugin_manager.get_registry().find_connector(path)
    except Exception:
        return None


def _download_via_connector(connector, path: str) -> str:
    """Download a connector-handled URI to a temp file and return its path."""
    suffix = os.path.splitext(path.split("?")[0])[1] or ".tmp"
    temp_fd, temp_file = tempfile.mkstemp(suffix=suffix)
    try:
        logging.info(f"Fetching {path} via connector plugin '{connector.name}'")
        with os.fdopen(temp_fd, "wb") as out, connector.open(path, mode="rb") as src:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                out.write(chunk)
    except Exception:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise
    return temp_file


@contextmanager
def open_iterable_with_s3(
    path: str,
    mode: str = "r",
    iterableargs: Optional[dict[str, Any]] = None,
    region: Optional[str] = None,
    profile: Optional[str] = None,
):
    """Open iterable data file, supporting local paths and cloud URIs.

    Supports local paths, ``s3://`` (via boto3, honoring ``region``/``profile``),
    and GCS/Azure/``s3a://`` URIs (delegated to iterabledata's native fsspec
    cloud support for both reading and writing).

    Args:
        path: File path or cloud URI (s3://, gs://, az://, abfs://, ...)
        mode: File mode ('r' for read, 'w' for write)
        iterableargs: Arguments for iterable processing
        region: AWS region (for s3:// URIs)
        profile: AWS profile name (for s3:// URIs)

    Yields:
        Iterable object (context manager)

    Raises:
        ImportError: If boto3 is not installed and an s3:// URI is provided
        ValueError: If S3 URI is invalid or credentials are missing
    """
    if iterableargs is None:
        iterableargs = {}
    if mode in ("r", "rb"):
        iterableargs = apply_table_selection(path, iterableargs)

    # Database connection URIs (postgres, mysql, mssql, clickhouse, mongo, es)
    # are read via iterabledata's DB drivers. Only for read modes.
    if mode in ("r", "rb"):
        from ..common.db_source import is_db_uri, open_db_source

        if is_db_uri(path):
            db_iterable = open_db_source(path, iterableargs=iterableargs)
            try:
                _configure_iterable(db_iterable, path, iterableargs)
                yield db_iterable
            finally:
                if hasattr(db_iterable, "close"):
                    db_iterable.close()
            return

    # GCS/Azure/s3a are opened directly by iterabledata (read and write).
    if is_native_cloud_uri(path):
        try:
            with open_iterable(path, mode=mode, iterableargs=iterableargs) as iterable:
                _configure_iterable(iterable, path, iterableargs)
                yield iterable
        except Exception as exc:
            if looks_like_missing_cloud_dep(path, exc):
                raise missing_cloud_extra_error(path, exc) from exc
            raise
        return

    if not is_s3_uri(path):
        connector = _find_plugin_connector(path)
        if connector is not None:
            if mode == "w":
                raise NotImplementedError(
                    "Write mode not supported for connector plugin URIs via iterable wrapper"
                )
            temp_file = _download_via_connector(connector, path)
            try:
                with open_iterable(temp_file, mode=mode, iterableargs=iterableargs) as iterable:
                    _configure_iterable(iterable, temp_file, iterableargs)
                    yield iterable
            finally:
                if temp_file and os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except OSError:
                        logging.warning(f"Failed to remove temporary file: {temp_file}")
            return

    if is_s3_uri(path):
        if mode == "w":
            # Delegate S3 writes to iterabledata's native fsspec cloud support
            # instead of failing; reads still use the boto3 temp-file path below.
            with open_iterable(path, mode=mode, iterableargs=iterableargs) as iterable:
                _configure_iterable(iterable, path, iterableargs)
                yield iterable
            return

        # Handle S3 read: download to temp file and use that
        temp_file = None
        try:
            # Download S3 object to temporary file
            bucket, key = parse_s3_uri(path)
            client = get_s3_client(region=region, profile=profile)

            # Create temporary file
            suffix = os.path.splitext(key)[1] or ".tmp"
            temp_fd, temp_file = tempfile.mkstemp(suffix=suffix)
            os.close(temp_fd)

            # Download from S3
            logging.info(f"Downloading s3://{bucket}/{key} to temporary file")
            client.download_file(bucket, key, temp_file)

            # Use temporary file with open_iterable
            with open_iterable(temp_file, mode=mode, iterableargs=iterableargs) as iterable:
                _configure_iterable(iterable, temp_file, iterableargs)
                yield iterable
        finally:
            # Clean up temporary file
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except OSError:
                    logging.warning(f"Failed to remove temporary file: {temp_file}")
    else:
        # Local file: use open_iterable directly
        with open_iterable(path, mode=mode, iterableargs=iterableargs) as iterable:
            _configure_iterable(iterable, path, iterableargs)
            yield iterable


class _TempFileCleanupIterable:
    """Wraps an iterable so close() also removes the backing temp file."""

    def __init__(self, inner, temp_file: str):
        self._inner = inner
        self._temp_file = temp_file

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def __iter__(self):
        return iter(self._inner)

    def close(self):
        try:
            self._inner.close()
        finally:
            if self._temp_file and os.path.exists(self._temp_file):
                try:
                    os.remove(self._temp_file)
                except OSError:
                    logging.warning(f"Failed to remove temporary file: {self._temp_file}")


def open_path(
    path: str,
    mode: str = "r",
    iterableargs: Optional[dict[str, Any]] = None,
    region: Optional[str] = None,
    profile: Optional[str] = None,
):
    """Open a local path or cloud URI as an iterable (non-context-manager variant).

    Drop-in replacement for ``open_iterable`` that adds support for cloud URIs.
    GCS/Azure/``s3a://`` and S3 writes are delegated to iterabledata's native
    fsspec cloud support; ``s3://`` reads are downloaded to a temporary file
    (removed when ``close()`` is called) so AWS region/profile keep working.

    Args:
        path: File path or cloud URI (s3://, gs://, az://, abfs://, ...)
        mode: File mode ('r' for read, 'w' for write)
        iterableargs: Arguments for iterable processing
        region: AWS region (for S3 URIs)
        profile: AWS profile name (for S3 URIs)

    Returns:
        Iterable object with a ``close()`` method.
    """
    if iterableargs is None:
        iterableargs = {}
    if mode in ("r", "rb"):
        iterableargs = apply_table_selection(path, iterableargs)

    # Database connection URIs are read via iterabledata's DB drivers.
    if mode in ("r", "rb"):
        from ..common.db_source import is_db_uri, open_db_source

        if is_db_uri(path):
            db_iterable = open_db_source(path, iterableargs=iterableargs)
            _configure_iterable(db_iterable, path, iterableargs)
            return db_iterable

    # GCS/Azure/s3a are opened directly by iterabledata (read and write).
    if is_native_cloud_uri(path):
        try:
            iterable = open_iterable(path, mode=mode, iterableargs=iterableargs)
        except Exception as exc:
            if looks_like_missing_cloud_dep(path, exc):
                raise missing_cloud_extra_error(path, exc) from exc
            raise
        _configure_iterable(iterable, path, iterableargs)
        return iterable

    if not is_s3_uri(path):
        connector = _find_plugin_connector(path)
        if connector is not None:
            if mode == "w":
                raise NotImplementedError(
                    "Write mode not supported for connector plugin URIs via iterable wrapper"
                )
            temp_file = _download_via_connector(connector, path)
            try:
                inner = open_iterable(temp_file, mode=mode, iterableargs=iterableargs)
            except Exception:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                raise
            _configure_iterable(inner, temp_file, iterableargs)
            return _TempFileCleanupIterable(inner, temp_file)
        inner = open_iterable(path, mode=mode, iterableargs=iterableargs)
        _configure_iterable(inner, path, iterableargs)
        return inner

    if mode == "w":
        # Delegate S3 writes to iterabledata's native fsspec cloud support.
        return open_iterable(path, mode=mode, iterableargs=iterableargs)

    bucket, key = parse_s3_uri(path)
    client = get_s3_client(region=region, profile=profile)

    suffix = os.path.splitext(key)[1] or ".tmp"
    temp_fd, temp_file = tempfile.mkstemp(suffix=suffix)
    os.close(temp_fd)
    try:
        logging.info(f"Downloading s3://{bucket}/{key} to temporary file")
        client.download_file(bucket, key, temp_file)
        inner = open_iterable(temp_file, mode=mode, iterableargs=iterableargs)
    except Exception:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise
    _configure_iterable(inner, temp_file, iterableargs)
    return _TempFileCleanupIterable(inner, temp_file)
