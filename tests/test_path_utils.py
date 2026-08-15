"""Tests for path and URI utilities."""

import os

import pytest

from undatum.common.path_utils import (
    is_azure_uri,
    is_cloud_uri,
    is_gcs_uri,
    is_http_uri,
    is_native_cloud_uri,
    is_s3_uri,
    is_uri,
    missing_cloud_extra_error,
    normalize_path,
    parse_cloud_uri,
    parse_s3_uri,
    resolve_path,
)


class TestIsUri:
    """Test is_uri function."""

    def test_is_uri_s3(self):
        """Test S3 URI detection."""
        assert is_uri("s3://bucket/path") is True

    def test_is_uri_http(self):
        """Test HTTP URI detection."""
        assert is_uri("http://example.com/path") is True

    def test_is_uri_https(self):
        """Test HTTPS URI detection."""
        assert is_uri("https://example.com/path") is True

    def test_is_uri_local_path(self):
        """Test local path detection."""
        assert is_uri("/local/path") is False
        assert is_uri("local/path") is False
        assert is_uri("./relative/path") is False

    def test_is_uri_file_scheme(self):
        """Test file:// URI detection."""
        assert is_uri("file:///local/path") is True


class TestIsS3Uri:
    """Test is_s3_uri function."""

    def test_is_s3_uri_valid(self):
        """Test valid S3 URI."""
        assert is_s3_uri("s3://bucket/path/to/file") is True
        assert is_s3_uri("s3://my-bucket/data.csv") is True

    def test_is_s3_uri_invalid(self):
        """Test invalid S3 URI."""
        assert is_s3_uri("http://example.com/path") is False
        assert is_s3_uri("/local/path") is False
        assert is_s3_uri("file:///local/path") is False


class TestIsHttpUri:
    """Test is_http_uri function."""

    def test_is_http_uri_http(self):
        """Test HTTP URI detection."""
        assert is_http_uri("http://example.com/path") is True

    def test_is_http_uri_https(self):
        """Test HTTPS URI detection."""
        assert is_http_uri("https://example.com/path") is True

    def test_is_http_uri_invalid(self):
        """Test invalid HTTP URI."""
        assert is_http_uri("s3://bucket/path") is False
        assert is_http_uri("/local/path") is False
        assert is_http_uri("ftp://example.com/path") is False


class TestParseS3Uri:
    """Test parse_s3_uri function."""

    def test_parse_s3_uri_valid(self):
        """Test parsing valid S3 URI."""
        bucket, key = parse_s3_uri("s3://bucket/path/to/file")
        assert bucket == "bucket"
        assert key == "path/to/file"

    def test_parse_s3_uri_root(self):
        """Test parsing S3 URI with root path."""
        bucket, key = parse_s3_uri("s3://bucket/file.csv")
        assert bucket == "bucket"
        assert key == "file.csv"

    def test_parse_s3_uri_nested(self):
        """Test parsing nested S3 URI."""
        bucket, key = parse_s3_uri("s3://my-bucket/data/2024/file.json")
        assert bucket == "my-bucket"
        assert key == "data/2024/file.json"

    def test_parse_s3_uri_invalid_scheme(self):
        """Test parsing invalid scheme."""
        with pytest.raises(ValueError, match="Invalid S3 URI scheme"):
            parse_s3_uri("http://bucket/path")

    def test_parse_s3_uri_missing_bucket(self):
        """Test parsing S3 URI with missing bucket."""
        with pytest.raises(ValueError, match="Missing bucket"):
            parse_s3_uri("s3:///path/to/file")

    def test_parse_s3_uri_missing_key(self):
        """Test parsing S3 URI with missing key."""
        with pytest.raises(ValueError, match="Missing key"):
            parse_s3_uri("s3://bucket/")


class TestNormalizePath:
    """Test normalize_path function."""

    def test_normalize_path_local(self):
        """Test normalizing local path."""
        # os.path.normpath resolves .. correctly
        assert normalize_path("./test/../file") == "file"
        assert normalize_path("/a/b/../c") == "/a/c"

    def test_normalize_path_uri(self):
        """Test normalizing URI (should remain unchanged)."""
        uri = "s3://bucket/path/to/file"
        assert normalize_path(uri) == uri

    def test_normalize_path_http_uri(self):
        """Test normalizing HTTP URI."""
        uri = "https://example.com/path/to/file"
        assert normalize_path(uri) == uri


class TestResolvePath:
    """Test resolve_path function."""

    def test_resolve_path_local(self):
        """Test resolving local path."""
        path = "test_file.txt"
        resolved = resolve_path(path)
        assert os.path.isabs(resolved)
        assert path in resolved

    def test_resolve_path_uri(self):
        """Test resolving URI (should remain unchanged)."""
        uri = "s3://bucket/path/to/file"
        assert resolve_path(uri) == uri

    def test_resolve_path_http_uri(self):
        """Test resolving HTTP URI."""
        uri = "https://example.com/path"
        assert resolve_path(uri) == uri


class TestCloudUriHelpers:
    """GCS/Azure/cloud URI detection and parsing."""

    def test_is_gcs_uri(self):
        assert is_gcs_uri("gs://bucket/data.csv") is True
        assert is_gcs_uri("gcs://bucket/data.csv") is True
        assert is_gcs_uri("s3://bucket/data.csv") is False
        assert is_gcs_uri("/local/path") is False

    def test_is_azure_uri(self):
        assert is_azure_uri("az://container/data.csv") is True
        assert is_azure_uri("abfs://container/data.csv") is True
        assert is_azure_uri("abfss://container@account.dfs.core.windows.net/data.csv") is True
        assert is_azure_uri("gs://bucket/data.csv") is False

    def test_is_native_cloud_uri(self):
        assert is_native_cloud_uri("gs://bucket/data.csv") is True
        assert is_native_cloud_uri("az://container/data.csv") is True
        assert is_native_cloud_uri("s3a://bucket/data.csv") is True
        assert is_native_cloud_uri("s3://bucket/data.csv") is False

    def test_is_cloud_uri(self):
        assert is_cloud_uri("s3://bucket/data.csv") is True
        assert is_cloud_uri("gs://bucket/data.csv") is True
        assert is_cloud_uri("az://container/data.csv") is True
        assert is_cloud_uri("https://example.com/data.csv") is False
        assert is_cloud_uri("/local/file.csv") is False

    def test_parse_cloud_uri_gcs(self):
        scheme, bucket, key = parse_cloud_uri("gs://my-bucket/path/to/file.csv")
        assert scheme == "gs"
        assert bucket == "my-bucket"
        assert key == "path/to/file.csv"

    def test_parse_cloud_uri_azure(self):
        scheme, bucket, key = parse_cloud_uri("az://my-container/data.jsonl")
        assert scheme == "az"
        assert bucket == "my-container"
        assert key == "data.jsonl"

    def test_parse_cloud_uri_invalid_scheme(self):
        with pytest.raises(ValueError, match="Unsupported cloud URI scheme"):
            parse_cloud_uri("https://example.com/file.csv")

    def test_parse_cloud_uri_missing_bucket(self):
        with pytest.raises(ValueError, match="Missing bucket or container"):
            parse_cloud_uri("gs:///path/to/file")

    def test_parse_cloud_uri_missing_key(self):
        with pytest.raises(ValueError, match="Missing key"):
            parse_cloud_uri("az://container/")

    def test_missing_cloud_extra_error_gcs(self):
        from undatum.common.errors import DependencyError

        error = missing_cloud_extra_error("gs://bucket/data.csv")
        assert isinstance(error, DependencyError)
        assert error.exit_code == 2
        assert "gcsfs" in str(error)
        assert "undatum[gcs]" in str(error)
        assert "undatum[cloud]" in str(error)

    def test_missing_cloud_extra_error_azure(self):
        error = missing_cloud_extra_error("az://container/data.csv")
        assert "adlfs" in str(error)
        assert "undatum[azure]" in str(error)
        assert "undatum[cloud]" in str(error)
