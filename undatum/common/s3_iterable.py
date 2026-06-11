"""S3 integration for iterable data processing."""

import logging
import os
import tempfile
from contextlib import contextmanager
from typing import Any, Optional

from iterable.helpers.detect import open_iterable

from ..common.path_utils import is_s3_uri
from ..formats.s3 import get_s3_client, parse_s3_uri


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
    """Open iterable data file, supporting both local paths and S3 URIs.

    Args:
        path: File path or S3 URI (s3://bucket/key)
        mode: File mode ('r' for read, 'w' for write)
        iterableargs: Arguments for iterable processing
        region: AWS region (for S3 URIs)
        profile: AWS profile name (for S3 URIs)

    Yields:
        Iterable object (context manager)

    Raises:
        ImportError: If boto3 is not installed and S3 URI is provided
        ValueError: If S3 URI is invalid or credentials are missing
    """
    if iterableargs is None:
        iterableargs = {}

    if is_s3_uri(path):
        # Handle S3 URI: download to temp file and use that
        if mode == "w":
            raise NotImplementedError("S3 write mode not yet implemented via iterable wrapper")

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
    """Open a local path or S3 URI as an iterable (non-context-manager variant).

    Drop-in replacement for ``open_iterable`` that adds read support for
    ``s3://`` URIs. For S3 inputs the object is downloaded to a temporary
    file which is removed when ``close()`` is called on the returned object.

    Args:
        path: File path or S3 URI (s3://bucket/key)
        mode: File mode ('r' for read, 'w' for write)
        iterableargs: Arguments for iterable processing
        region: AWS region (for S3 URIs)
        profile: AWS profile name (for S3 URIs)

    Returns:
        Iterable object with a ``close()`` method.
    """
    if iterableargs is None:
        iterableargs = {}

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
            return _TempFileCleanupIterable(inner, temp_file)
        return open_iterable(path, mode=mode, iterableargs=iterableargs)

    if mode == "w":
        raise NotImplementedError("S3 write mode not yet implemented via iterable wrapper")

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
    return _TempFileCleanupIterable(inner, temp_file)
