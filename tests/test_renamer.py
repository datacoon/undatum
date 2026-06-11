"""Tests for renamer command."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from undatum.cmds.renamer import Renamer, get_iterable_options


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


class TestRenamer:
    """Test Renamer class."""

    def test_init(self):
        """Test Renamer initialization."""
        renamer = Renamer()
        assert renamer is not None

    @patch("undatum.cmds.renamer.open_iterable")
    @patch("undatum.cmds.renamer.DataWriter")
    def test_rename_with_mapping(self, mock_writer_class, mock_open_iterable):
        """Test rename with field mapping."""
        renamer = Renamer()

        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([{"old_name": "Alice", "age": 30}]))
        mock_open_iterable.return_value = mock_iterable

        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"old_name": "Alice", "age": 30}\n')
            temp_path = f.name

        try:
            options = {"map": "old_name:new_name"}
            renamer.rename(temp_path, options)
            mock_writer.write_items.assert_called_once()
            # Check that items were renamed
            items = mock_writer.write_items.call_args[0][0]
            assert "new_name" in items[0]
            assert "old_name" not in items[0]
        finally:
            os.unlink(temp_path)

    @patch("undatum.cmds.renamer.open_iterable")
    @patch("undatum.cmds.renamer.DataWriter")
    def test_rename_with_regex_pattern(self, mock_writer_class, mock_open_iterable):
        """Test rename with regex pattern."""
        renamer = Renamer()

        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([{"old_field": "value"}]))
        mock_open_iterable.return_value = mock_iterable

        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"old_field": "value"}\n')
            temp_path = f.name

        try:
            options = {"pattern": "old_", "replacement": "new_"}
            renamer.rename(temp_path, options)
            mock_writer.write_items.assert_called_once()
        finally:
            os.unlink(temp_path)

    @patch("undatum.cmds.renamer.open_iterable")
    def test_rename_invalid_regex(self, mock_open_iterable):
        """Test rename with invalid regex pattern."""
        renamer = Renamer()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            f.write('{"field": "value"}\n')
            temp_path = f.name

        try:
            options = {"pattern": "[invalid", "replacement": ""}
            from undatum.common.errors import ValidationError

            with pytest.raises(ValidationError, match="Invalid regex pattern"):
                renamer.rename(temp_path, options)
        finally:
            os.unlink(temp_path)
