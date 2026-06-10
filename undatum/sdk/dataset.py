# -*- coding: utf8 -*-
"""Dataset class for programmatic data processing."""
import logging
import tempfile
from typing import Optional, Any, Iterator, Union
from pathlib import Path

from iterable.helpers.detect import open_iterable

from ..common.path_utils import is_s3_uri
from ..common.s3_iterable import open_iterable_with_s3
from ..cmds.converter import Converter
from ..cmds.deduplicator import Deduplicator
from ..cmds.filler import Filler
from ..cmds.sorter import Sorter
from ..cmds.searcher import Searcher
from ..cmds.selector import Selector
from ..cmds.joiner import Joiner
from ..cmds.sampler import Sampler
from ..cmds.statistics import StatProcessor
from ..cmds.counter import Counter
from ..cmds.head import Head
from ..cmds.tail import Tail
from ..cmds.masker import Masker


logger = logging.getLogger(__name__)


class Dataset:
    """Dataset class for programmatic data processing with method chaining.
    
    Provides a fluent API for reading, transforming, and writing data that
    mirrors the CLI commands but allows programmatic composition.
    
    Example:
        >>> from undatum import Dataset
        >>> ds = Dataset.read("data.jsonl")
        >>> ds = ds.fill("age", value=0).dedup(keys=["user_id"])
        >>> stats = ds.stats()
        >>> ds.write("output.parquet")
    """
    
    def __init__(self, source: Optional[str] = None, data: Optional[Iterator[dict]] = None):
        """Initialize Dataset.
        
        Args:
            source: Path to source file (for lazy loading)
            data: In-memory data iterator (for chained operations)
        """
        self._source = source
        self._data = data
        self._temp_file = None
    
    @classmethod
    def read(cls, path: str, **options) -> 'Dataset':
        """Read data from a file or S3 URI.
        
        Args:
            path: File path or S3 URI (s3://bucket/path)
            **options: Additional options (encoding, delimiter, format_in, etc.)
        
        Returns:
            Dataset instance
        
        Example:
            >>> ds = Dataset.read("data.csv")
            >>> ds = Dataset.read("s3://bucket/data.jsonl", encoding="utf8")
        """
        return cls(source=path, data=None)
    
    def write(self, path: str, **options) -> None:
        """Write dataset to a file or S3 URI.
        
        Args:
            path: Output file path or S3 URI
            **options: Additional options (format_out, delimiter, etc.)
        
        Example:
            >>> ds.write("output.jsonl")
            >>> ds.write("s3://bucket/output.parquet", format_out="parquet")
        """
        # Get data source
        if self._data is not None:
            # Data is in memory from chained operations
            # Write to temp file first, then process
            import os
            import tempfile
            temp_fd, temp_input = tempfile.mkstemp(suffix='.jsonl')
            os.close(temp_fd)
            
            try:
                # Write data to temp file
                with open_iterable(temp_input, mode='w', iterableargs={}) as it_out:
                    for record in self._data:
                        it_out.write(record)
                
                # Convert temp file to output
                converter = Converter()
                converter.convert(temp_input, path, options)
            finally:
                if os.path.exists(temp_input):
                    os.remove(temp_input)
        elif self._source:
            # Convert from source file
            converter = Converter()
            converter.convert(self._source, path, options)
        else:
            raise ValueError("No data source available for writing")
    
    def fill(self, fields: Union[str, list[str]], value: Optional[Any] = None, 
             strategy: Optional[str] = None, **options) -> 'Dataset':
        """Fill empty or null values in specified fields.
        
        Args:
            fields: Field name(s) to fill
            value: Constant value to use (if strategy is None)
            strategy: Fill strategy ('forward' or 'backward')
            **options: Additional options
        
        Returns:
            New Dataset instance with filled data
        
        Example:
            >>> ds = ds.fill("age", value=0)
            >>> ds = ds.fill(["name", "email"], value="N/A")
            >>> ds = ds.fill("status", strategy="forward")
        """
        if isinstance(fields, str):
            fields = [fields]
        
        filler = Filler()
        output_path = self._get_temp_output()
        
        filler_opts = {
            'fields': ','.join(fields),
            'value': value,
            'strategy': strategy,
            **options
        }
        
        filler.fill(self._get_input_path(), output_path, filler_opts)
        return Dataset(source=output_path)
    
    def dedup(self, keys: Optional[list[str]] = None, keep: str = 'first', **options) -> 'Dataset':
        """Remove duplicate rows.
        
        Args:
            keys: List of key fields (None for all fields)
            keep: Which duplicate to keep ('first' or 'last')
            **options: Additional options
        
        Returns:
            New Dataset instance with deduplicated data
        
        Example:
            >>> ds = ds.dedup()  # Deduplicate by all fields
            >>> ds = ds.dedup(keys=["user_id", "email"])
            >>> ds = ds.dedup(keys=["id"], keep="last")
        """
        deduplicator = Deduplicator()
        output_path = self._get_temp_output()
        
        dedup_opts = {
            'key_fields': ','.join(keys) if keys else None,
            'keep': keep,
            **options
        }
        
        deduplicator.dedup(self._get_input_path(), output_path, dedup_opts)
        return Dataset(source=output_path)
    
    def sort(self, by: Union[str, list[str]], desc: bool = False, 
             numeric: bool = False, **options) -> 'Dataset':
        """Sort rows by specified field(s).
        
        Args:
            by: Field name(s) to sort by
            desc: Sort in descending order
            numeric: Use numeric sorting
            **options: Additional options
        
        Returns:
            New Dataset instance with sorted data
        
        Example:
            >>> ds = ds.sort("name")
            >>> ds = ds.sort(["date", "price"], desc=True)
            >>> ds = ds.sort("age", numeric=True)
        """
        if isinstance(by, str):
            by = [by]
        
        sorter = Sorter()
        output_path = self._get_temp_output()
        
        sort_opts = {
            'by': ','.join(by),
            'desc': desc,
            'numeric': numeric,
            **options
        }
        
        sorter.sort(self._get_input_path(), output_path, sort_opts)
        return Dataset(source=output_path)
    
    def filter(self, pattern: Optional[str] = None, fields: Optional[list[str]] = None,
               query: Optional[str] = None, **options) -> 'Dataset':
        """Filter rows using regex pattern or query expression.
        
        Args:
            pattern: Regex pattern to search for
            fields: Fields to search in (None for all fields)
            query: Query expression (alternative to pattern)
            **options: Additional options
        
        Returns:
            New Dataset instance with filtered data
        
        Example:
            >>> ds = ds.filter(pattern="error|warning")
            >>> ds = ds.filter(pattern="active", fields=["status"])
            >>> ds = ds.filter(query="`price` > 100")
        """
        searcher = Searcher()
        output_path = self._get_temp_output()
        
        search_opts = {
            'pattern': pattern,
            'fields': ','.join(fields) if fields else None,
            'query': query,
            **options
        }
        
        searcher.search(self._get_input_path(), output_path, search_opts)
        return Dataset(source=output_path)
    
    def select(self, fields: Union[str, list[str]], filter_expr: Optional[str] = None,
               **options) -> 'Dataset':
        """Select specific fields from dataset.
        
        Args:
            fields: Field name(s) to select
            filter_expr: Optional filter expression
            **options: Additional options
        
        Returns:
            New Dataset instance with selected fields
        
        Example:
            >>> ds = ds.select(["name", "email"])
            >>> ds = ds.select("user_id", filter_expr="`status` == 'active'")
        """
        if isinstance(fields, str):
            fields = [fields]
        
        selector = Selector()
        output_path = self._get_temp_output()
        
        select_opts = {
            'fields': ','.join(fields),
            'filter': filter_expr,
            **options
        }
        
        selector.select(self._get_input_path(), output_path, select_opts)
        return Dataset(source=output_path)
    
    def join(self, other: Union['Dataset', str], keys: Union[str, list[str]],
             join_type: str = 'inner', **options) -> 'Dataset':
        """Join with another dataset.
        
        Args:
            other: Other Dataset instance or file path
            keys: Join key field(s)
            join_type: Join type ('inner', 'left', 'right', 'full')
            **options: Additional options
        
        Returns:
            New Dataset instance with joined data
        
        Example:
            >>> ds1 = Dataset.read("users.jsonl")
            >>> ds2 = Dataset.read("orders.jsonl")
            >>> ds = ds1.join(ds2, keys=["user_id"], join_type="left")
        """
        if isinstance(keys, str):
            keys = [keys]
        
        joiner = Joiner()
        output_path = self._get_temp_output()
        
        other_path = other._get_input_path() if isinstance(other, Dataset) else other
        
        join_opts = {
            'keys': ','.join(keys),
            'join_type': join_type,
            **options
        }
        
        joiner.join(self._get_input_path(), other_path, output_path, join_opts)
        return Dataset(source=output_path)
    
    def sample(self, n: Optional[int] = None, percent: Optional[float] = None, **options) -> 'Dataset':
        """Sample rows from dataset.
        
        Args:
            n: Number of rows to sample
            percent: Percentage of rows to sample (0-100)
            **options: Additional options
        
        Returns:
            New Dataset instance with sampled data
        
        Example:
            >>> ds = ds.sample(n=1000)
            >>> ds = ds.sample(percent=10.0)
        """
        sampler = Sampler()
        output_path = self._get_temp_output()
        
        sample_opts = {
            'n': n,
            'percent': percent,
            **options
        }
        
        sampler.sample(self._get_input_path(), output_path, sample_opts)
        return Dataset(source=output_path)
    
    def mask(self, fields: Union[str, list[str]], method: str = 'redact',
             salt: Optional[str] = None, **options) -> 'Dataset':
        """Mask sensitive fields for anonymization.
        
        Args:
            fields: Field name(s) to mask
            method: Masking method ('redact', 'hash', 'randomize')
            salt: Optional salt for hash method
            **options: Additional options
        
        Returns:
            New Dataset instance with masked data
        
        Example:
            >>> ds = ds.mask(["email", "phone"], method="redact")
            >>> ds = ds.mask("user_id", method="hash", salt="my-salt")
        """
        if isinstance(fields, str):
            fields = [fields]
        
        masker = Masker()
        output_path = self._get_temp_output()
        
        mask_opts = {
            'fields': ','.join(fields),
            'method': method,
            'salt': salt,
            **options
        }
        
        masker.mask(self._get_input_path(), output_path, mask_opts)
        return Dataset(source=output_path)
    
    def stats(self, **options) -> dict[str, Any]:
        """Compute statistics for the dataset.
        
        Args:
            **options: Additional options (checkdates, engine, etc.)
        
        Returns:
            Dictionary with statistics
        
        Example:
            >>> stats = ds.stats()
            >>> stats = ds.stats(checkdates=True, engine="duckdb")
        """
        processor = StatProcessor()
        processor.stats(self._get_input_path(), options)
        # Note: stats() prints to stdout, returns None
        # In future, could return structured stats object
        return {}
    
    def count(self, **options) -> int:
        """Count the number of rows in the dataset.
        
        Args:
            **options: Additional options
        
        Returns:
            Number of rows
        
        Example:
            >>> n = ds.count()
        """
        counter = Counter()
        # Note: count() prints to stdout, returns None
        # Would need to modify Counter to return value
        counter.count(self._get_input_path(), options)
        return 0  # Placeholder
    
    def head(self, n: int = 10, **options) -> list[dict]:
        """Get first N rows from dataset.
        
        Args:
            n: Number of rows to return
            **options: Additional options
        
        Returns:
            List of records
        
        Example:
            >>> rows = ds.head(20)
        """
        head_cmd = Head()
        output_path = self._get_temp_output()
        
        head_opts = {
            'n': n,
            **options
        }
        
        head_cmd.head(self._get_input_path(), head_opts)
        # Note: head() writes to file/stdout, doesn't return
        # Would need to modify Head to return records
        return []
    
    def tail(self, n: int = 10, **options) -> list[dict]:
        """Get last N rows from dataset.
        
        Args:
            n: Number of rows to return
            **options: Additional options
        
        Returns:
            List of records
        
        Example:
            >>> rows = ds.tail(20)
        """
        tail_cmd = Tail()
        output_path = self._get_temp_output()
        
        tail_opts = {
            'n': n,
            **options
        }
        
        tail_cmd.tail(self._get_input_path(), tail_opts)
        # Note: tail() writes to file/stdout, doesn't return
        # Would need to modify Tail to return records
        return []
    
    def _get_input_path(self) -> str:
        """Get input path for processing."""
        if self._source:
            return self._source
        elif self._data:
            # Write data to temp file
            import os
            import tempfile
            temp_fd, temp_path = tempfile.mkstemp(suffix='.jsonl')
            os.close(temp_fd)
            
            with open_iterable(temp_path, mode='w', iterableargs={}) as it_out:
                for record in self._data:
                    it_out.write(record)
            
            self._temp_file = temp_path
            return temp_path
        else:
            raise ValueError("No data source available")
    
    def _get_temp_output(self) -> str:
        """Get temporary output path for chained operations."""
        import tempfile
        import os
        temp_fd, temp_path = tempfile.mkstemp(suffix='.jsonl')
        os.close(temp_fd)
        return temp_path
    
    def __iter__(self) -> Iterator[dict]:
        """Iterate over dataset records."""
        if self._data:
            yield from self._data
        elif self._source:
            if is_s3_uri(self._source):
                with open_iterable_with_s3(self._source, mode='r', iterableargs={}) as it:
                    yield from it
            else:
                with open_iterable(self._source, mode='r', iterableargs={}) as it:
                    yield from it
        else:
            raise ValueError("No data source available")
