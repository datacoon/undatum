"""Tests for enumerator command."""
import tempfile
import os
from unittest.mock import patch, MagicMock

import pytest

from undatum.cmds.enumerator import Enumerator, get_iterable_options


class TestGetIterableOptions:
    """Test get_iterable_options function."""

    def test_get_iterable_options_all(self):
        """Test extracting all iterable options."""
        options = {
            'tagname': 'item',
            'delimiter': ',',
            'encoding': 'utf-8',
            'start_line': 1,
            'page': 1,
            'other': 'value'
        }
        result = get_iterable_options(options)
        assert result == {
            'tagname': 'item',
            'delimiter': ',',
            'encoding': 'utf-8',
            'start_line': 1,
            'page': 1
        }


class TestEnumerator:
    """Test Enumerator class."""

    def test_init(self):
        """Test Enumerator initialization."""
        enumerator = Enumerator()
        assert enumerator is not None

    @patch('undatum.cmds.enumerator.open_iterable')
    @patch('undatum.cmds.enumerator.DataWriter')
    def test_enum_number_type(self, mock_writer_class, mock_open_iterable):
        """Test enum with number type."""
        enumerator = Enumerator()
        
        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([
            {'name': 'Alice'},
            {'name': 'Bob'}
        ]))
        mock_open_iterable.return_value = mock_iterable
        
        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"name": "Alice"}\n{"name": "Bob"}\n')
            temp_path = f.name

        try:
            options = {'type': 'number', 'field': 'row_id', 'start': 1}
            enumerator.enum(temp_path, options)
            # Should add row_id field
            mock_writer.write_items.assert_called_once()
        finally:
            os.unlink(temp_path)

    @patch('undatum.cmds.enumerator.open_iterable')
    @patch('undatum.cmds.enumerator.DataWriter')
    def test_enum_uuid_type(self, mock_writer_class, mock_open_iterable):
        """Test enum with UUID type."""
        enumerator = Enumerator()
        
        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([
            {'name': 'Alice'}
        ]))
        mock_open_iterable.return_value = mock_iterable
        
        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"name": "Alice"}\n')
            temp_path = f.name

        try:
            options = {'type': 'uuid', 'field': 'id'}
            enumerator.enum(temp_path, options)
            mock_writer.write_items.assert_called_once()
        finally:
            os.unlink(temp_path)

    @patch('undatum.cmds.enumerator.open_iterable')
    @patch('undatum.cmds.enumerator.DataWriter')
    def test_enum_constant_type(self, mock_writer_class, mock_open_iterable):
        """Test enum with constant type."""
        enumerator = Enumerator()
        
        mock_iterable = MagicMock()
        mock_iterable.__iter__ = MagicMock(return_value=iter([
            {'name': 'Alice'}
        ]))
        mock_open_iterable.return_value = mock_iterable
        
        mock_writer = MagicMock()
        mock_writer_class.return_value = mock_writer
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"name": "Alice"}\n')
            temp_path = f.name

        try:
            options = {'type': 'constant', 'field': 'status', 'value': 'active'}
            enumerator.enum(temp_path, options)
            mock_writer.write_items.assert_called_once()
        finally:
            os.unlink(temp_path)
