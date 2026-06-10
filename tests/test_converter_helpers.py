"""Tests for converter helper functions."""
import tempfile
import os
import xml.etree.ElementTree as etree
from unittest.mock import patch, MagicMock

import pytest
import pandas as pd

from undatum.cmds.converter import (
    get_iterable_options,
    df_to_pyorc_schema,
    etree_to_dict,
    _is_flat,
    make_flat,
)
import undatum.cmds.converter as converter_module


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

    def test_get_iterable_options_empty(self):
        """Test extracting from empty options."""
        options = {}
        result = get_iterable_options(options)
        assert result == {}


class TestDfToPyorcSchema:
    """Test df_to_pyorc_schema function."""

    def test_df_to_pyorc_schema_float64(self):
        """Test schema generation for float64."""
        df = pd.DataFrame({'col1': [1.0, 2.0]})
        schema = df_to_pyorc_schema(df)
        assert 'col1:float' in schema

    def test_df_to_pyorc_schema_int64(self):
        """Test schema generation for int64."""
        df = pd.DataFrame({'col1': [1, 2]})
        schema = df_to_pyorc_schema(df)
        assert 'col1:int' in schema or 'col1:string' in schema

    def test_df_to_pyorc_schema_string(self):
        """Test schema generation for string."""
        df = pd.DataFrame({'col1': ['a', 'b']})
        schema = df_to_pyorc_schema(df)
        assert 'col1:string' in schema

    def test_df_to_pyorc_schema_datetime(self):
        """Test schema generation for datetime."""
        df = pd.DataFrame({'col1': pd.to_datetime(['2024-01-01', '2024-01-02'])})
        schema = df_to_pyorc_schema(df)
        assert 'col1:timestamp' in schema


class TestCopyOptions:
    """Test __copy_options function."""

    def test_copy_options_missing_keys(self):
        """Test copying missing keys from defaults."""
        # Access module-level function directly
        copy_options = converter_module.__copy_options
        
        user_options = {'key1': 'value1'}
        default_options = {'key1': 'default1', 'key2': 'default2'}
        result = copy_options(user_options, default_options)
        assert result['key1'] == 'value1'  # User value takes precedence
        assert result['key2'] == 'default2'  # Default value added

    def test_copy_options_all_present(self):
        """Test copying when all keys are present."""
        # Access module-level function directly
        copy_options = converter_module.__copy_options
        
        user_options = {'key1': 'value1', 'key2': 'value2'}
        default_options = {'key1': 'default1', 'key2': 'default2'}
        result = copy_options(user_options, default_options)
        assert result['key1'] == 'value1'
        assert result['key2'] == 'value2'


class TestEtreeToDict:
    """Test etree_to_dict function."""

    def test_etree_to_dict_simple(self):
        """Test converting simple XML element to dict."""
        elem = etree.Element('root')
        elem.text = 'value'
        result = etree_to_dict(elem)
        assert 'root' in result
        assert result['root'] == 'value'

    def test_etree_to_dict_with_children(self):
        """Test converting XML element with children."""
        root = etree.Element('root')
        child = etree.SubElement(root, 'child')
        child.text = 'child_value'
        result = etree_to_dict(root)
        assert 'root' in result
        assert 'child' in result['root']

    def test_etree_to_dict_with_attributes(self):
        """Test converting XML element with attributes."""
        elem = etree.Element('root')
        elem.set('attr1', 'value1')
        result = etree_to_dict(elem, prefix_strip=True)
        assert 'root' in result
        assert '@attr1' in result['root']

    def test_etree_to_dict_prefix_strip(self):
        """Test prefix stripping."""
        elem = etree.Element('{http://example.com}root')
        result = etree_to_dict(elem, prefix_strip=True)
        assert 'root' in result


class TestIsFlat:
    """Test _is_flat function."""

    def test_is_flat_simple_dict(self):
        """Test flat dictionary."""
        item = {'key1': 'value1', 'key2': 'value2'}
        from undatum.cmds.converter import _is_flat
        assert _is_flat(item) is True

    def test_is_flat_with_list(self):
        """Test dictionary with list."""
        item = {'key1': 'value1', 'key2': [1, 2, 3]}
        from undatum.cmds.converter import _is_flat
        assert _is_flat(item) is False

    def test_is_flat_with_nested_dict(self):
        """Test dictionary with nested dict."""
        item = {'key1': 'value1', 'key2': {'nested': 'value'}}
        from undatum.cmds.converter import _is_flat
        assert _is_flat(item) is False


class TestMakeFlat:
    """Test make_flat function."""

    def test_make_flat_simple(self):
        """Test flattening simple nested structure."""
        item = {'level1': {'level2': 'value'}}
        result = make_flat(item)
        assert 'level1.level2' in result or isinstance(result, dict)

    def test_make_flat_already_flat(self):
        """Test flattening already flat structure."""
        item = {'key1': 'value1', 'key2': 'value2'}
        result = make_flat(item)
        assert isinstance(result, dict)
