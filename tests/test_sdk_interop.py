"""Tests for Dataset DataFrame and typed-row interop (SDK additions)."""

from dataclasses import dataclass

import pytest

from undatum import Dataset

CSV = "tests/fixtures/2cols6rows.csv"


class TestDataFrameInterop:
    def test_to_pandas(self):
        df = Dataset.read(CSV).to_pandas()
        assert df.shape[0] == 6
        assert "id" in df.columns and "name" in df.columns

    def test_to_pandas_chunked(self):
        chunks = list(Dataset.read(CSV).to_pandas(chunksize=2))
        assert len(chunks) == 3
        assert all(len(c) == 2 for c in chunks)

    def test_to_polars(self):
        pytest.importorskip("polars")
        df = Dataset.read(CSV).to_polars()
        assert df.shape[0] == 6

    def test_to_dask(self):
        pytest.importorskip("dask.dataframe")
        ddf = Dataset.read(CSV).to_dask()
        assert ddf.compute().shape[0] == 6


class TestTypedRows:
    def test_as_dataclasses(self):
        @dataclass
        class Row:
            id: str
            name: str

        rows = list(Dataset.read(CSV).as_dataclasses(Row))
        assert len(rows) == 6
        assert rows[0].name == "John"

    def test_as_pydantic(self):
        pydantic = pytest.importorskip("pydantic")

        class Row(pydantic.BaseModel):
            id: str
            name: str

        rows = list(Dataset.read(CSV).as_pydantic(Row))
        assert len(rows) == 6
        assert rows[0].id == "1"
