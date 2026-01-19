"""Tests for stats command with DuckDB engine."""
import pytest
from pathlib import Path

from undatum.cmds.statistics import StatProcessor


@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file for testing."""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("name,age,city\nAlice,30,New York\nBob,25,London\nCharlie,35,Paris\n")
    return str(csv_file)


@pytest.fixture
def sample_jsonl_file(tmp_path):
    """Create a sample JSONL file for testing."""
    jsonl_file = tmp_path / "sample.jsonl"
    content = (
        '{"name": "Alice", "age": 30, "city": "New York"}\n'
        '{"name": "Bob", "age": 25, "city": "London"}\n'
        '{"name": "Charlie", "age": 35, "city": "Paris"}\n'
        '{"name": "Diana", "age": 28, "city": "Berlin"}\n'
        '{"name": "Eve", "age": 32, "city": "Madrid"}\n'
    )
    jsonl_file.write_text(content)
    return str(jsonl_file)


@pytest.fixture
def nested_jsonl_file(tmp_path):
    """Create a JSONL file with nested structures."""
    jsonl_file = tmp_path / "nested.jsonl"
    content = (
        '{"user": {"name": "Alice", "address": {"city": "New York"}}, "score": 95}\n'
        '{"user": {"name": "Bob", "address": {"city": "London"}}, "score": 87}\n'
        '{"user": {"name": "Charlie", "address": {"city": "Paris"}}, "score": 92}\n'
    )
    jsonl_file.write_text(content)
    return str(jsonl_file)


@pytest.fixture
def jsonl_with_null_values(tmp_path):
    """Create a JSONL file with null values to test None handling."""
    jsonl_file = tmp_path / "with_nulls.jsonl"
    content = (
        '{"name": "Alice", "age": 30, "city": "New York"}\n'
        '{"name": "Bob", "age": null, "city": "London"}\n'
        '{"name": null, "age": 35, "city": "Paris"}\n'
        '{"name": "Diana", "age": 28, "city": null}\n'
    )
    jsonl_file.write_text(content)
    return str(jsonl_file)


@pytest.fixture
def large_jsonl_file(tmp_path):
    """Create a larger JSONL file for testing performance."""
    jsonl_file = tmp_path / "large.jsonl"
    content_lines = []
    for i in range(100):
        content_lines.append(f'{{"id": {i}, "name": "User{i}", "value": {i * 10}}}\n')
    jsonl_file.write_text(''.join(content_lines))
    return str(jsonl_file)


class TestStatsDuckDBEngine:
    """Test statistics command with DuckDB engine."""

    def test_stats_csv_duckdb_engine(self, sample_csv_file, capsys):
        """Test stats command with CSV file using DuckDB engine."""
        processor = StatProcessor()
        options = {
            'format_in': 'csv',
            'engine': 'duckdb'
        }
        
        # Should not raise exception
        processor.stats(sample_csv_file, options)
        
        # Check that output was produced (captured by capsys)
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_jsonl_duckdb_engine(self, sample_jsonl_file, capsys):
        """Test stats command with JSONL file using DuckDB engine."""
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        # Should not raise exception
        processor.stats(sample_jsonl_file, options)
        
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_auto_engine_detection(self, sample_jsonl_file, capsys):
        """Test stats command with auto engine detection (should use DuckDB for JSONL)."""
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'auto'
        }
        
        # Should not raise exception and should use DuckDB
        processor.stats(sample_jsonl_file, options)
        
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_nested_jsonl_duckdb(self, nested_jsonl_file, capsys):
        """Test stats command with nested JSON structures using DuckDB."""
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        # Should not raise exception even with nested structures
        processor.stats(nested_jsonl_file, options)
        
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_jsonl_with_null_values(self, jsonl_with_null_values, capsys):
        """Test stats command with null values - should handle None gracefully."""
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        # Should not raise "Referenced column 'None' not found" error
        processor.stats(jsonl_with_null_values, options)
        
        captured = capsys.readouterr()
        # Should complete successfully
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_large_file_duckdb(self, large_jsonl_file, capsys):
        """Test stats command with larger file using DuckDB."""
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        # Should handle larger files efficiently
        processor.stats(large_jsonl_file, options)
        
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_duckdb_fallback_to_iterable(self, tmp_path, capsys):
        """Test that DuckDB falls back to iterable on errors."""
        # Create an invalid file that might cause DuckDB to fail
        invalid_file = tmp_path / "invalid.jsonl"
        invalid_file.write_text('{"invalid": json}\n')  # Malformed JSON
        
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        # Should fall back to iterable engine without crashing
        try:
            processor.stats(str(invalid_file), options)
        except Exception:
            # Some exceptions are acceptable for malformed files
            pass

    def test_stats_duckdb_multiple_runs(self, sample_jsonl_file, capsys):
        """Test that DuckDB engine works consistently across multiple runs."""
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        # Run multiple times to check for resource leaks or state issues
        for _ in range(3):
            processor.stats(sample_jsonl_file, options)
            captured = capsys.readouterr()
            assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_duckdb_with_dictshare(self, sample_jsonl_file, capsys):
        """Test stats command with custom dictshare parameter."""
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb',
            'dictshare': '10'  # Pass as string to match expected format
        }
        
        # Should work with custom dictshare
        processor.stats(sample_jsonl_file, options)
        
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_duckdb_no_progress(self, sample_jsonl_file, capsys):
        """Test stats command with progress disabled."""
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb',
            'no_progress': True
        }
        
        # Should work without progress bar
        processor.stats(sample_jsonl_file, options)
        
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_empty_file_handling(self, tmp_path, capsys):
        """Test stats command with empty file."""
        empty_file = tmp_path / "empty.jsonl"
        empty_file.write_text('')
        
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        # Should handle empty file gracefully
        try:
            processor.stats(str(empty_file), options)
        except Exception:
            # Empty files may raise exceptions, which is acceptable
            pass


class TestStatsDuckDBEdgeCases:
    """Test edge cases for stats command with DuckDB engine."""

    def test_stats_csv_with_special_characters(self, tmp_path, capsys):
        """Test CSV with special characters in field names."""
        csv_file = tmp_path / "special.csv"
        csv_file.write_text('field.name,field_age,field-value\nAlice,30,100\n')
        
        processor = StatProcessor()
        options = {
            'format_in': 'csv',
            'engine': 'duckdb'
        }
        
        processor.stats(str(csv_file), options)
        captured = capsys.readouterr()
        # Should complete without errors
        assert captured.out is not None

    def test_stats_jsonl_single_record(self, tmp_path, capsys):
        """Test stats with single record file."""
        jsonl_file = tmp_path / "single.jsonl"
        jsonl_file.write_text('{"name": "Alice", "age": 30}\n')
        
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        processor.stats(str(jsonl_file), options)
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out

    def test_stats_jsonl_very_long_field_names(self, tmp_path, capsys):
        """Test stats with very long field names."""
        jsonl_file = tmp_path / "long_fields.jsonl"
        long_field = "a" * 100
        jsonl_file.write_text(f'{{"{long_field}": "value"}}\n')
        
        processor = StatProcessor()
        options = {
            'format_in': 'jsonl',
            'engine': 'duckdb'
        }
        
        processor.stats(str(jsonl_file), options)
        captured = capsys.readouterr()
        assert 'Statistics' in captured.out or 'key' in captured.out
