"""Tests for transformer command."""

from undatum.cmds.transformer import Transformer, get_iterable_options


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

    def test_get_iterable_options_empty(self):
        """Test extracting from empty options."""
        options = {}
        result = get_iterable_options(options)
        assert result == {}


class TestTransformer:
    """Test Transformer class."""

    def test_init(self):
        """Test transformer initialization."""
        transformer = Transformer()
        assert transformer is not None
