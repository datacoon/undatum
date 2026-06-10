"""Tests for engine selector."""
from unittest.mock import patch, MagicMock

import pytest

from undatum.common.engine_selector import detect_engine, _is_sql_expressible


class TestDetectEngine:
    """Test detect_engine function."""

    @patch('undatum.common.engine_selector.detect_file_type')
    def test_detect_engine_auto_duckdb(self, mock_detect):
        """Test auto-detection selecting DuckDB."""
        mock_detect.return_value = {
            'success': True,
            'datatype': MagicMock(id=lambda: 'csv'),
            'codec': MagicMock(id=lambda: 'raw')
        }
        
        result = detect_engine('test.csv')
        assert result == 'duckdb'

    @patch('undatum.common.engine_selector.detect_file_type')
    def test_detect_engine_auto_iterable(self, mock_detect):
        """Test auto-detection selecting iterable engine."""
        mock_detect.return_value = {
            'success': True,
            'datatype': MagicMock(id=lambda: 'xml'),
            'codec': MagicMock(id=lambda: 'raw')
        }
        
        result = detect_engine('test.xml')
        assert result == 'iterable'

    def test_detect_engine_explicit_duckdb(self):
        """Test explicit DuckDB selection."""
        result = detect_engine('test.csv', engine='duckdb')
        assert result == 'duckdb'

    def test_detect_engine_explicit_python(self):
        """Test explicit Python/iterable selection."""
        result = detect_engine('test.csv', engine='python')
        assert result == 'iterable'

    def test_detect_engine_with_filetype(self):
        """Test engine detection with known filetype."""
        result = detect_engine('test.csv', filetype='csv')
        assert result == 'duckdb'

    @patch('undatum.common.engine_selector.detect_file_type')
    def test_detect_engine_compressed(self, mock_detect):
        """Test engine detection with compressed file."""
        mock_detect.return_value = {
            'success': True,
            'datatype': MagicMock(id=lambda: 'csv'),
            'codec': MagicMock(id=lambda: 'gzip')
        }
        result = detect_engine('test.csv.gz')
        # Compressed files may use iterable engine depending on codec
        assert result in ('duckdb', 'iterable')

    @patch('undatum.common.engine_selector.detect_file_type')
    def test_detect_engine_unsupported_compression(self, mock_detect):
        """Test engine detection with unsupported compression."""
        mock_detect.return_value = {
            'success': True,
            'datatype': MagicMock(id=lambda: 'csv'),
            'codec': MagicMock(id=lambda: 'bz2')
        }
        
        result = detect_engine('test.csv.bz2')
        assert result == 'iterable'


class TestIsSqlExpressible:
    """Test _is_sql_expressible function."""

    def test_is_sql_expressible_sort(self):
        """Test SQL-expressible operation."""
        result = _is_sql_expressible('sort')
        assert result is True

    def test_is_sql_expressible_filter(self):
        """Test SQL-expressible filter operation."""
        result = _is_sql_expressible('filter')
        assert result is True

    def test_is_sql_expressible_unknown(self):
        """Test unknown operation."""
        result = _is_sql_expressible('unknown_op')
        assert result is False

    def test_is_sql_expressible_case_insensitive(self):
        """Test case insensitive operation matching."""
        result = _is_sql_expressible('SORT')
        assert result is True


class TestIsFormatSupportedByDuckdb:
    """Test is_format_supported_by_duckdb function."""

    def test_is_format_supported_by_duckdb_csv(self):
        """Test CSV format support."""
        from undatum.common.engine_selector import is_format_supported_by_duckdb
        assert is_format_supported_by_duckdb('csv') is True

    def test_is_format_supported_by_duckdb_jsonl(self):
        """Test JSONL format support."""
        from undatum.common.engine_selector import is_format_supported_by_duckdb
        assert is_format_supported_by_duckdb('jsonl') is True

    def test_is_format_supported_by_duckdb_xml(self):
        """Test XML format (not supported)."""
        from undatum.common.engine_selector import is_format_supported_by_duckdb
        assert is_format_supported_by_duckdb('xml') is False

    def test_is_format_supported_by_duckdb_with_compression(self):
        """Test format with compression."""
        from undatum.common.engine_selector import is_format_supported_by_duckdb
        assert is_format_supported_by_duckdb('csv', 'gzip') is True
        assert is_format_supported_by_duckdb('csv', 'bz2') is False

    def test_is_format_supported_by_duckdb_default_compression(self):
        """Test format with default compression."""
        from undatum.common.engine_selector import is_format_supported_by_duckdb
        assert is_format_supported_by_duckdb('csv', None) is True
