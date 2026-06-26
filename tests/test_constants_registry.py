"""Tests that undatum's format/codec constants are driven by iterabledata."""

from undatum import constants
from undatum.utils import get_file_type


class TestSupportedFileTypes:
    """SUPPORTED_FILE_TYPES should reflect iterabledata's registry, not a whitelist."""

    def test_many_formats_exposed(self):
        # Far more than the legacy 13-format whitelist.
        assert len(constants.SUPPORTED_FILE_TYPES) > 50

    def test_core_formats_present(self):
        for fmt in ["csv", "json", "jsonl", "parquet", "orc", "avro", "xml", "bson"]:
            assert fmt in constants.SUPPORTED_FILE_TYPES

    def test_extended_formats_present(self):
        # Formats from iterabledata that the old whitelist never exposed.
        assert "geojson" in constants.SUPPORTED_FILE_TYPES

    def test_extract_extras_present(self):
        for fmt in constants.EXTRACT_FILE_TYPES:
            assert fmt in constants.SUPPORTED_FILE_TYPES


class TestCodecs:
    """COMPRESSED_FILE_TYPES should include iterabledata's full codec set."""

    def test_extended_codecs(self):
        for codec in ["gz", "bz2", "xz", "zip", "zstd"]:
            assert codec in constants.COMPRESSED_FILE_TYPES

    def test_raw_not_included(self):
        assert "raw" not in constants.COMPRESSED_FILE_TYPES


class TestGetFileType:
    """get_file_type now recognizes any registry-backed format."""

    def test_recognizes_extended_format(self):
        assert get_file_type("data.geojson") == "geojson"

    def test_recognizes_core_format(self):
        assert get_file_type("data.parquet") == "parquet"

    def test_unknown_returns_none(self):
        assert get_file_type("data.unknownext") is None
