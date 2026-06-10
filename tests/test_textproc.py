"""Tests for textproc command."""
from unittest.mock import patch, MagicMock

import pytest

from undatum.cmds.textproc import TextProcessor, get_iterable_options, get_keys


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


class TestGetKeys:
    """Test get_keys function."""

    def test_get_keys_simple(self):
        """Test getting keys from simple dictionary."""
        d = {'key1': 'value1', 'key2': 'value2'}
        keys = get_keys(d)
        assert 'key1' in keys
        assert 'key2' in keys

    def test_get_keys_nested(self):
        """Test getting keys from nested dictionary."""
        d = {'level1': {'level2': 'value'}}
        keys = get_keys(d)
        assert 'level1' in keys
        assert 'level1.level2' in keys

    def test_get_keys_with_prefix(self):
        """Test getting keys with prefix."""
        d = {'key': 'value'}
        keys = get_keys(d, prefix='prefix')
        assert 'prefix.key' in keys


class TestTextProcessor:
    """Test TextProcessor class."""

    def test_init(self):
        """Test text processor initialization."""
        processor = TextProcessor()
        assert processor is not None
