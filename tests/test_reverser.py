"""Tests for reverser command."""

import os
import tempfile
from unittest.mock import MagicMock, patch

from undatum.cmds.reverser import Reverser, _detect_engine, get_iterable_options


class TestGetIterableOptions:
    """Test get_iterable_options function."""

    def test_get_iterable_options_all(self):
        """Test extracting all iterable options."""
        options = {
            "tagname": "item",
            "delimiter": ",",
            "encoding": "utf-8",
            "start_line": 1,
            "page": 1,
            "other": "value",
        }
        result = get_iterable_options(options)
        assert result == {
            "tagname": "item",
            "delimiter": ",",
            "encoding": "utf-8",
            "start_line": 1,
            "page": 1,
        }


class TestDetectEngine:
    """Test _detect_engine function."""

    @patch("undatum.cmds.reverser.detect_file_type")
    def test_detect_engine_auto_duckdb(self, mock_detect):
        """Test auto-detection selecting DuckDB."""
        mock_detect.return_value = {
            "success": True,
            "datatype": MagicMock(id=lambda: "csv"),
            "codec": MagicMock(id=lambda: "raw"),
        }

        result = _detect_engine("test.csv", "auto", None)
        assert result == "duckdb"

    @patch("undatum.cmds.reverser.detect_file_type")
    def test_detect_engine_auto_iterable(self, mock_detect):
        """Test auto-detection selecting iterable engine."""
        mock_detect.return_value = {
            "success": True,
            "datatype": MagicMock(id=lambda: "xml"),
            "codec": MagicMock(id=lambda: "raw"),
        }

        result = _detect_engine("test.xml", "auto", None)
        assert result == "iterable"

    def test_detect_engine_explicit(self):
        """Test explicit engine selection."""
        result = _detect_engine("test.csv", "iterable", "csv")
        assert result == "iterable"


class TestReverser:
    """Test Reverser class."""

    def test_init(self):
        """Test Reverser initialization."""
        reverser = Reverser()
        assert reverser is not None

    @patch("undatum.cmds.reverser.open_iterable")
    @patch("undatum.cmds.reverser.DataWriter")
    def test_reverse_basic(self, mock_writer_class, mock_open_iterable):
        """Test basic reverse operation."""
        reverser = Reverser()

        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([{"id": 1}, {"id": 2}, {"id": 3}]))
        mock_open_iterable.return_value = mock_iterable

        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"id": 1}\n{"id": 2}\n{"id": 3}\n')
            temp_path = f.name

        try:
            options = {}
            reverser.reverse(temp_path, options)
            mock_writer.write_items.assert_called_once()
            # Check that items were reversed
            items = mock_writer.write_items.call_args[0][0]
            assert items[0]["id"] == 3  # Last item should be first
            assert items[-1]["id"] == 1  # First item should be last
        finally:
            os.unlink(temp_path)
