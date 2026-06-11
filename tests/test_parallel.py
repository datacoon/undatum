"""Tests for parallel processing utilities."""

from unittest.mock import patch

import pytest

from undatum.common.parallel import (
    get_cpu_count,
    is_cpu_bound_operation,
    parallel_map,
    parallel_process_chunks,
)


class TestGetCpuCount:
    """Test get_cpu_count function."""

    @patch("os.cpu_count")
    def test_get_cpu_count_normal(self, mock_cpu_count):
        """Test get_cpu_count with normal CPU count."""
        mock_cpu_count.return_value = 4
        assert get_cpu_count() == 4

    @patch("os.cpu_count")
    def test_get_cpu_count_none(self, mock_cpu_count):
        """Test get_cpu_count when cpu_count returns None."""
        mock_cpu_count.return_value = None
        assert get_cpu_count() == 1

    @patch("os.cpu_count")
    def test_get_cpu_count_exception(self, mock_cpu_count):
        """Test get_cpu_count when cpu_count raises exception."""
        mock_cpu_count.side_effect = Exception("Test error")
        assert get_cpu_count() == 1


class TestIsCpuBoundOperation:
    """Test is_cpu_bound_operation function."""

    def test_cpu_bound_operations(self):
        """Test CPU-bound operations."""
        cpu_bound_ops = [
            "convert",
            "stats",
            "frequency",
            "dedup",
            "search",
            "fill",
            "transform",
            "apply",
        ]
        for op in cpu_bound_ops:
            assert is_cpu_bound_operation(op) is True
            assert is_cpu_bound_operation(op.upper()) is True

    def test_io_bound_operations(self):
        """Test I/O-bound operations."""
        io_bound_ops = ["read", "write", "load", "save", "copy", "move"]
        for op in io_bound_ops:
            assert is_cpu_bound_operation(op) is False

    def test_unknown_operation(self):
        """Test unknown operation."""
        assert is_cpu_bound_operation("unknown") is False


class TestParallelMap:
    """Test parallel_map function."""

    def test_parallel_map_single_thread(self):
        """Test parallel_map with single thread (sequential)."""

        def square(x):
            return x * x

        items = [1, 2, 3, 4, 5]
        results = list(parallel_map(square, items, num_threads=1))
        assert results == [1, 4, 9, 16, 25]

    def test_parallel_map_zero_threads(self):
        """Test parallel_map with zero threads (sequential)."""

        def square(x):
            return x * x

        items = [1, 2, 3]
        results = list(parallel_map(square, items, num_threads=0))
        assert results == [1, 4, 9]

    def test_parallel_map_empty(self):
        """Test parallel_map with empty iterable."""

        def square(x):
            return x * x

        items = []
        results = list(parallel_map(square, items))
        assert results == []

    def test_parallel_map_with_chunk_size(self):
        """Test parallel_map with chunk size."""

        def square_list(chunk):
            return [x * x for x in chunk]

        items = [1, 2, 3, 4, 5]
        results = list(parallel_map(square_list, items, num_threads=2, chunk_size=2))
        # Results may be out of order, so check that all expected values are present
        assert len(results) == 5
        assert set(results) == {1, 4, 9, 16, 25}

    def test_parallel_map_with_list_result(self):
        """Test parallel_map when function returns a list."""

        def chunk_square(chunk):
            return [x * x for x in chunk]

        items = [1, 2, 3]
        results = list(parallel_map(chunk_square, items, num_threads=2, chunk_size=1))
        # Results may be out of order
        assert len(results) == 3
        assert set(results) == {1, 4, 9}

    def test_parallel_map_with_exception(self):
        """Test parallel_map when function raises exception."""

        def failing_func(chunk):
            # When chunk_size > 1, function receives chunks
            if isinstance(chunk, list):
                if 3 in chunk:
                    raise ValueError("Test error")
                return [x * x for x in chunk]
            else:
                if chunk == 3:
                    raise ValueError("Test error")
                return chunk * chunk

        items = [1, 2, 3, 4]
        with pytest.raises(ValueError, match="Test error"):
            list(parallel_map(failing_func, items, num_threads=2, chunk_size=1))


class TestParallelProcessChunks:
    """Test parallel_process_chunks function."""

    def test_parallel_process_chunks_single_thread(self):
        """Test parallel_process_chunks with single thread."""

        def square_chunk(chunk):
            return [x * x for x in chunk]

        chunks = [[1, 2], [3, 4], [5]]
        results = list(parallel_process_chunks(square_chunk, iter(chunks), num_threads=1))
        assert results == [[1, 4], [9, 16], [25]]

    def test_parallel_process_chunks_empty(self):
        """Test parallel_process_chunks with empty chunks."""

        def square_chunk(chunk):
            return [x * x for x in chunk]

        chunks = []
        results = list(parallel_process_chunks(square_chunk, iter(chunks)))
        assert results == []

    def test_parallel_process_chunks_zero_threads(self):
        """Test parallel_process_chunks with zero threads."""

        def square_chunk(chunk):
            return [x * x for x in chunk]

        chunks = [[1, 2], [3]]
        results = list(parallel_process_chunks(square_chunk, iter(chunks), num_threads=0))
        assert results == [[1, 4], [9]]

    def test_parallel_process_chunks_multithreaded(self):
        """Test parallel_process_chunks with multiple threads."""

        def square_chunk(chunk):
            return [x * x for x in chunk]

        chunks = [[1, 2], [3, 4], [5, 6]]
        results = list(parallel_process_chunks(square_chunk, iter(chunks), num_threads=2))
        # Results may be out of order, so check contents
        assert len(results) == 3
        result_sets = [set(r) for r in results]
        assert {1, 4} in result_sets
        assert {9, 16} in result_sets
        assert {25, 36} in result_sets

    def test_parallel_process_chunks_with_exception(self):
        """Test parallel_process_chunks when processor raises exception."""

        def failing_processor(chunk):
            if 3 in chunk:
                raise ValueError("Test error")
            return [x * x for x in chunk]

        chunks = [[1, 2], [3, 4]]
        with pytest.raises(ValueError, match="Test error"):
            list(parallel_process_chunks(failing_processor, iter(chunks), num_threads=2))
