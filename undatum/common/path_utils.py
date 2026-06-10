# -*- coding: utf8 -*-
"""Path and URI utilities for local and remote file handling."""
import os
import urllib.parse
from pathlib import Path
from typing import Optional, Tuple


def is_uri(path: str) -> bool:
    """Check if a path is a URI (s3://, http://, https://, etc.).

    Args:
        path: Path string to check

    Returns:
        True if path is a URI, False if local path
    """
    parsed = urllib.parse.urlparse(path)
    return bool(parsed.scheme)


def is_s3_uri(path: str) -> bool:
    """Check if a path is an S3 URI.

    Args:
        path: Path string to check

    Returns:
        True if path is an S3 URI (s3://...)
    """
    parsed = urllib.parse.urlparse(path)
    return parsed.scheme == 's3'


def is_http_uri(path: str) -> bool:
    """Check if a path is an HTTP/HTTPS URI.

    Args:
        path: Path string to check

    Returns:
        True if path is HTTP/HTTPS URI
    """
    parsed = urllib.parse.urlparse(path)
    return parsed.scheme in ('http', 'https')


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """Parse S3 URI into bucket and key.

    Args:
        s3_uri: S3 URI in format s3://bucket/path/to/file

    Returns:
        Tuple of (bucket, key)

    Raises:
        ValueError: If URI format is invalid
    """
    parsed = urllib.parse.urlparse(s3_uri)
    if parsed.scheme != 's3':
        raise ValueError(f"Invalid S3 URI scheme: {s3_uri}")

    bucket = parsed.netloc
    if not bucket:
        raise ValueError(f"Missing bucket in S3 URI: {s3_uri}")

    # Remove leading slash from path
    key = parsed.path.lstrip('/')
    if not key:
        raise ValueError(f"Missing key/path in S3 URI: {s3_uri}")

    return bucket, key


def normalize_path(path: str) -> str:
    """Normalize a path (local or URI).

    Args:
        path: Path to normalize

    Returns:
        Normalized path
    """
    if is_uri(path):
        # For URIs, just return as-is (no normalization needed)
        return path
    # For local paths, normalize
    return os.path.normpath(path)


def resolve_path(path: str) -> str:
    """Resolve a path to absolute path (for local paths only).

    Args:
        path: Path to resolve

    Returns:
        Resolved absolute path (or original URI if URI)
    """
    if is_uri(path):
        # URIs don't need resolution
        return path
    return os.path.abspath(path)


def validate_file_path(file_path: str, check_read: bool = True, check_write: bool = False) -> None:
    """Validate that a file path exists and has required permissions.
    
    Args:
        file_path: Path to validate
        check_read: If True, check that file is readable
        check_write: If True, check that file is writable
        
    Raises:
        FileNotFoundError: If file does not exist
        PermissionError: If file doesn't have required permissions
    """
    from .errors import FileNotFoundError, PermissionError, find_similar_files
    
    if is_uri(file_path):
        # For URIs, we can't validate existence locally
        return
    
    path = Path(file_path)
    
    if not path.exists():
        suggestions = find_similar_files(file_path)
        raise FileNotFoundError(file_path, suggestions)
    
    if check_read and not os.access(file_path, os.R_OK):
        raise PermissionError(file_path, operation="read")
    
    if check_write and not os.access(file_path, os.W_OK):
        raise PermissionError(file_path, operation="write")


def validate_directory_path(dir_path: str, check_write: bool = False) -> None:
    """Validate that a directory path exists and has required permissions.
    
    Args:
        dir_path: Path to directory to validate
        check_write: If True, check that directory is writable
        
    Raises:
        FileNotFoundError: If directory does not exist
        PermissionError: If directory doesn't have required permissions
    """
    from .errors import FileNotFoundError, PermissionError
    
    if is_uri(dir_path):
        # For URIs, we can't validate existence locally
        return
    
    path = Path(dir_path)
    
    if not path.exists():
        raise FileNotFoundError(dir_path)
    
    if not path.is_dir():
        raise ValueError(f"Path is not a directory: {dir_path}")
    
    if check_write and not os.access(dir_path, os.W_OK):
        raise PermissionError(dir_path, operation="write")
