"""Tests for chunked I/O utilities."""

from undatum.common.chunked_io import (
    chunked_reader,
    chunked_writer,
    process_chunked,
)


class TestChunkedReader:
    """Test chunked_reader function."""

    def test_chunked_reader_basic(self):
        """Test basic chunked reading."""
        items = [1, 2, 3, 4, 5]
        chunks = list(chunked_reader(iter(items), chunk_size=2))
        assert chunks == [[1, 2], [3, 4], [5]]

    def test_chunked_reader_exact_chunk_size(self):
        """Test chunked reading with exact chunk size."""
        items = [1, 2, 3, 4, 5, 6]
        chunks = list(chunked_reader(iter(items), chunk_size=3))
        assert chunks == [[1, 2, 3], [4, 5, 6]]

    def test_chunked_reader_single_chunk(self):
        """Test chunked reading with single chunk."""
        items = [1, 2, 3]
        chunks = list(chunked_reader(iter(items), chunk_size=10))
        assert chunks == [[1, 2, 3]]

    def test_chunked_reader_empty(self):
        """Test chunked reading with empty iterable."""
        items = []
        chunks = list(chunked_reader(iter(items), chunk_size=2))
        assert chunks == []

    def test_chunked_reader_custom_chunk_size(self):
        """Test chunked reading with custom chunk size."""
        items = list(range(10))
        chunks = list(chunked_reader(iter(items), chunk_size=4))
        assert chunks == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]


class TestChunkedWriter:
    """Test chunked_writer function."""

    def test_chunked_writer_basic(self):
        """Test basic chunked writing."""
        written_chunks = []

        def writer_func(chunk):
            written_chunks.append(chunk)

        items = [1, 2, 3, 4, 5]
        chunked_writer(items, writer_func, chunk_size=2)
        assert written_chunks == [[1, 2], [3, 4], [5]]

    def test_chunked_writer_exact_chunk_size(self):
        """Test chunked writing with exact chunk size."""
        written_chunks = []

        def writer_func(chunk):
            written_chunks.append(chunk)

        items = [1, 2, 3, 4, 5, 6]
        chunked_writer(items, writer_func, chunk_size=3)
        assert written_chunks == [[1, 2, 3], [4, 5, 6]]

    def test_chunked_writer_single_chunk(self):
        """Test chunked writing with single chunk."""
        written_chunks = []

        def writer_func(chunk):
            written_chunks.append(chunk)

        items = [1, 2, 3]
        chunked_writer(items, writer_func, chunk_size=10)
        assert written_chunks == [[1, 2, 3]]

    def test_chunked_writer_empty(self):
        """Test chunked writing with empty list."""
        written_chunks = []

        def writer_func(chunk):
            written_chunks.append(chunk)

        items = []
        chunked_writer(items, writer_func, chunk_size=2)
        assert written_chunks == []


class TestProcessChunked:
    """Test process_chunked function."""

    def test_process_chunked_basic(self):
        """Test basic chunked processing."""

        def reader():
            yield [1, 2]
            yield [3, 4]
            yield [5]

        def processor(chunk):
            return [x * 2 for x in chunk]

        written_chunks = []

        def writer(chunk):
            written_chunks.append(chunk)

        total = process_chunked(reader(), processor, writer, chunk_size=2)
        assert total == 5
        assert written_chunks == [[2, 4], [6, 8], [10]]

    def test_process_chunked_empty(self):
        """Test chunked processing with empty reader."""

        def reader():
            return
            yield  # Make it a generator

        def processor(chunk):
            return chunk

        written_chunks = []

        def writer(chunk):
            written_chunks.append(chunk)

        total = process_chunked(reader(), processor, writer, chunk_size=2)
        assert total == 0
        assert written_chunks == []

    def test_process_chunked_identity(self):
        """Test chunked processing with identity processor."""

        def reader():
            yield [1, 2, 3]
            yield [4, 5]

        def processor(chunk):
            return chunk

        written_chunks = []

        def writer(chunk):
            written_chunks.append(chunk)

        total = process_chunked(reader(), processor, writer, chunk_size=2)
        assert total == 5
        assert written_chunks == [[1, 2, 3], [4, 5]]
