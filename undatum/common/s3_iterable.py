# -*- coding: utf8 -*-
"""S3 integration for iterable data processing."""
import logging
import os
import tempfile
from contextlib import contextmanager
from typing import Optional, Any

from iterable.helpers.detect import open_iterable

from ..common.path_utils import is_s3_uri
from ..formats.s3 import get_s3_client, parse_s3_uri


@contextmanager
def open_iterable_with_s3(
    path: str,
    mode: str = 'r',
    iterableargs: Optional[dict[str, Any]] = None,
    region: Optional[str] = None,
    profile: Optional[str] = None
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
        if mode == 'w':
            raise NotImplementedError("S3 write mode not yet implemented via iterable wrapper")

        temp_file = None
        try:
            # Download S3 object to temporary file
            bucket, key = parse_s3_uri(path)
            client = get_s3_client(region=region, profile=profile)

            # Create temporary file
            suffix = os.path.splitext(key)[1] or '.tmp'
            temp_fd, temp_file = tempfile.mkstemp(suffix=suffix)
            os.close(temp_fd)

            # Download from S3
            logging.info(f'Downloading s3://{bucket}/{key} to temporary file')
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
                    logging.warning(f'Failed to remove temporary file: {temp_file}')
    else:
        # Local file: use open_iterable directly
        with open_iterable(path, mode=mode, iterableargs=iterableargs) as iterable:
            yield iterable
