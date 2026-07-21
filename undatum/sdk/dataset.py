"""Dataset class for programmatic data processing."""

import logging
import tempfile
from collections.abc import Iterator
from typing import Any, Optional, Union

from iterable.helpers.detect import open_iterable

from ..cmds.converter import Converter
from ..cmds.deduplicator import Deduplicator
from ..cmds.filler import Filler
from ..cmds.joiner import Joiner
from ..cmds.masker import Masker
from ..cmds.sampler import Sampler
from ..cmds.searcher import Searcher
from ..cmds.selector import Selector
from ..cmds.sorter import Sorter
from ..cmds.statistics import StatProcessor
from ..common.path_utils import is_s3_uri
from ..common.s3_iterable import open_iterable_with_s3

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

    # Options understood by iterabledata's open_iterable
    _ITERABLE_OPTIONS_KEYS = ("tagname", "delimiter", "encoding", "start_line", "page")

    def __init__(
        self,
        source: Optional[str] = None,
        data: Optional[Iterator[dict]] = None,
        options: Optional[dict] = None,
    ):
        """Initialize Dataset.

        Args:
            source: Path to source file (for lazy loading)
            data: In-memory data iterator (for chained operations)
            options: Read options (encoding, delimiter, etc.) applied when iterating
        """
        self._source = source
        self._data = data
        self._options = options or {}
        self._temp_file = None

    @classmethod
    def read(cls, path: str, **options) -> "Dataset":
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
        return cls(source=path, data=None, options=options)

    def _iterable_args(self) -> dict:
        """Extract iterable-specific options from stored read options."""
        return {
            k: v
            for k, v in self._options.items()
            if k in self._ITERABLE_OPTIONS_KEYS and v is not None
        }

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

            temp_fd, temp_input = tempfile.mkstemp(suffix=".jsonl")
            os.close(temp_fd)

            try:
                # Write data to temp file
                with open_iterable(temp_input, mode="w", iterableargs={}) as it_out:
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

    def fill(
        self,
        fields: Union[str, list[str]],
        value: Optional[Any] = None,
        strategy: Optional[str] = None,
        **options,
    ) -> "Dataset":
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

        filler_opts = {"fields": ",".join(fields), "value": value, "strategy": strategy, **options}

        filler.fill(self._get_input_path(), output_path, filler_opts)
        return Dataset(source=output_path)

    def dedup(self, keys: Optional[list[str]] = None, keep: str = "first", **options) -> "Dataset":
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

        dedup_opts = {"key_fields": ",".join(keys) if keys else None, "keep": keep, **options}

        deduplicator.dedup(self._get_input_path(), output_path, dedup_opts)
        return Dataset(source=output_path)

    def sort(
        self, by: Union[str, list[str]], desc: bool = False, numeric: bool = False, **options
    ) -> "Dataset":
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

        sort_opts = {"by": ",".join(by), "desc": desc, "numeric": numeric, **options}

        sorter.sort(self._get_input_path(), output_path, sort_opts)
        return Dataset(source=output_path)

    def filter(
        self,
        pattern: Optional[str] = None,
        fields: Optional[list[str]] = None,
        query: Optional[str] = None,
        **options,
    ) -> "Dataset":
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
            "pattern": pattern,
            "fields": ",".join(fields) if fields else None,
            "query": query,
            **options,
        }

        searcher.search(self._get_input_path(), output_path, search_opts)
        return Dataset(source=output_path)

    def select(
        self, fields: Union[str, list[str]], filter_expr: Optional[str] = None, **options
    ) -> "Dataset":
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
        output_path = options.get("output") or self._get_temp_output()

        select_opts = {
            "fields": ",".join(fields),
            "filter": filter_expr,
            "output": output_path,
            **options,
        }

        selector.select(self._get_input_path(), select_opts)
        return Dataset(source=output_path)

    def join(
        self,
        other: Union["Dataset", str],
        keys: Union[str, list[str]],
        join_type: str = "inner",
        **options,
    ) -> "Dataset":
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

        join_opts = {"keys": ",".join(keys), "join_type": join_type, **options}

        joiner.join(self._get_input_path(), other_path, output_path, join_opts)
        return Dataset(source=output_path)

    def sample(
        self, n: Optional[int] = None, percent: Optional[float] = None, **options
    ) -> "Dataset":
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

        sample_opts = {"n": n, "percent": percent, **options}

        sampler.sample(self._get_input_path(), output_path, sample_opts)
        return Dataset(source=output_path)

    def mask(
        self,
        fields: Union[str, list[str]],
        method: str = "redact",
        salt: Optional[str] = None,
        **options,
    ) -> "Dataset":
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

        mask_opts = {"fields": ",".join(fields), "method": method, "salt": salt, **options}

        masker.mask(self._get_input_path(), output_path, mask_opts)
        return Dataset(source=output_path)

    def stats(self, **options) -> dict[str, Any]:
        """Compute statistics for the dataset.

        Args:
            **options: Additional options (checkdates, engine, etc.)

        Returns:
            Dictionary with the dataset profile: 'count', 'num_fields',
            'fieldtypes', 'fields', 'dictkeys' and detailed per-field data
            under 'debug'.

        Example:
            >>> stats = ds.stats()
            >>> stats['count']
            1000
        """
        processor = StatProcessor()
        stats_opts = {**self._options, "quiet": True, "progress": False, **options}
        profile = processor.stats(self._get_input_path(), stats_opts)
        return profile or {}

    def package(
        self,
        output: Optional[str] = None,
        package_dir: Optional[str] = None,
        **options,
    ) -> dict[str, Any]:
        """Generate a Frictionless Data Package descriptor for this dataset.

        Args:
            output: Output ``datapackage.json`` path.
            package_dir: Optional directory to materialize the package.
            **options: Additional packaging options (autodoc, metadata, etc.).

        Returns:
            Dictionary with ``package``, ``output_file``, and optional ``archive_path``.

        Example:
            >>> result = Dataset.read("data.csv").package(output="datapackage.json")
            >>> result["package"]["name"]
            'data'
        """
        from ..cmds.packager import Packager

        pack_opts = {**self._options, "quiet": True, **options}
        if output:
            pack_opts["output"] = output
        if package_dir:
            pack_opts["package_dir"] = package_dir
        return Packager().create([self._get_input_path()], pack_opts)

    def count(self, **options) -> int:
        """Count the number of rows in the dataset.

        Args:
            **options: Additional options

        Returns:
            Number of rows

        Example:
            >>> n = ds.count()
        """
        return sum(1 for _ in self)

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
        from itertools import islice

        return list(islice(iter(self), n))

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
        from collections import deque

        return list(deque(iter(self), maxlen=n))

    def to_pandas(self, chunksize: Optional[int] = None) -> Any:
        """Convert the dataset to a pandas DataFrame.

        Args:
            chunksize: If given, return an iterator of DataFrames of this many
                rows each instead of a single DataFrame.

        Returns:
            A ``pandas.DataFrame`` (or an iterator of them when ``chunksize`` is set).

        Example:
            >>> df = Dataset.read("data.jsonl").to_pandas()
        """
        from iterable.dataframe_adapters import iterable_to_pandas

        return iterable_to_pandas(iter(self), chunksize=chunksize)

    def to_polars(self, chunksize: Optional[int] = None) -> Any:
        """Convert the dataset to a Polars DataFrame.

        Requires ``polars`` (``pip install undatum[polars]``).

        Args:
            chunksize: If given, return an iterator of DataFrames of this many
                rows each instead of a single DataFrame.

        Returns:
            A ``polars.DataFrame`` (or an iterator of them when ``chunksize`` is set).

        Example:
            >>> df = Dataset.read("data.parquet").to_polars()
        """
        from iterable.dataframe_adapters import iterable_to_polars

        return iterable_to_polars(iter(self), chunksize=chunksize)

    def to_dask(self, chunksize: int = 1000000) -> Any:
        """Convert the dataset to a Dask DataFrame.

        Requires ``dask[dataframe]`` (``pip install undatum[dask]``).

        Args:
            chunksize: Approximate number of rows per Dask partition.

        Returns:
            A ``dask.dataframe.DataFrame``.

        Example:
            >>> ddf = Dataset.read("big.jsonl").to_dask()
        """
        from iterable.dataframe_adapters import iterable_to_dask

        return iterable_to_dask(iter(self), chunksize=chunksize)

    def as_dataclasses(self, dataclass_type: type, skip_empty: bool = True) -> Iterator[Any]:
        """Iterate rows as instances of a dataclass.

        Args:
            dataclass_type: The dataclass type to convert rows into.
            skip_empty: Skip empty rows.

        Yields:
            Instances of ``dataclass_type``.

        Example:
            >>> from dataclasses import dataclass
            >>> @dataclass
            ... class Person:
            ...     name: str
            ...     age: int
            >>> for p in Dataset.read("people.csv").as_dataclasses(Person):
            ...     print(p.name)
        """
        from iterable.helpers.typed import as_dataclasses

        return as_dataclasses(self, dataclass_type, skip_empty=skip_empty)

    def as_pydantic(
        self, model_type: type, skip_empty: bool = True, validate: bool = True
    ) -> Iterator[Any]:
        """Iterate rows as instances of a Pydantic model.

        Requires ``pydantic`` (already a core undatum dependency).

        Args:
            model_type: The Pydantic ``BaseModel`` subclass to convert rows into.
            skip_empty: Skip empty rows.
            validate: Validate rows against the model schema.

        Yields:
            Instances of ``model_type``.

        Example:
            >>> from pydantic import BaseModel
            >>> class Person(BaseModel):
            ...     name: str
            ...     age: int
            >>> for p in Dataset.read("people.csv").as_pydantic(Person):
            ...     print(p.name)
        """
        from iterable.helpers.typed import as_pydantic

        return as_pydantic(self, model_type, skip_empty=skip_empty, validate=validate)

    def _get_input_path(self) -> str:
        """Get input path for processing."""
        if self._source:
            return self._source
        elif self._data:
            # Write data to temp file
            import os
            import tempfile

            temp_fd, temp_path = tempfile.mkstemp(suffix=".jsonl")
            os.close(temp_fd)

            with open_iterable(temp_path, mode="w", iterableargs={}) as it_out:
                for record in self._data:
                    it_out.write(record)

            self._temp_file = temp_path
            return temp_path
        else:
            raise ValueError("No data source available")

    def _get_temp_output(self) -> str:
        """Get temporary output path for chained operations."""
        import os

        temp_fd, temp_path = tempfile.mkstemp(suffix=".jsonl")
        os.close(temp_fd)
        return temp_path

    def __iter__(self) -> Iterator[dict]:
        """Iterate over dataset records."""
        if self._data:
            yield from self._data
        elif self._source:
            iterableargs = self._iterable_args()
            if is_s3_uri(self._source):
                with open_iterable_with_s3(self._source, mode="r", iterableargs=iterableargs) as it:
                    yield from it
            else:
                with open_iterable(self._source, mode="r", iterableargs=iterableargs) as it:
                    yield from it
        else:
            raise ValueError("No data source available")
