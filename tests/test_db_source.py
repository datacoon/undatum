"""Tests for the unified iterabledata-backed database source helper."""

from unittest.mock import patch

import pytest

from undatum.common.db_source import (
    ITERABLE_ONLY_DB_ENGINES,
    _split_driver_kwargs,
    detect_db_engine,
    is_db_uri,
    open_db_source,
)


class TestDetectDbEngine:
    def test_postgres_aliases(self):
        assert detect_db_engine("postgresql://u:p@h/db") == "postgres"
        assert detect_db_engine("postgres://u:p@h/db") == "postgres"

    def test_mysql_aliases(self):
        assert detect_db_engine("mysql://u:p@h/db") == "mysql"
        assert detect_db_engine("mariadb://u:p@h/db") == "mysql"

    def test_extended_engines(self):
        assert detect_db_engine("mssql://h/db") == "mssql"
        assert detect_db_engine("sqlserver://h/db") == "mssql"
        assert detect_db_engine("clickhouse://h:9000/db") == "clickhouse"
        assert detect_db_engine("mongodb://h:27017/db") == "mongo"
        assert detect_db_engine("elasticsearch://h:9200") == "elasticsearch"
        assert detect_db_engine("opensearch://h:9200") == "elasticsearch"

    def test_non_db(self):
        assert detect_db_engine("/tmp/data.csv") is None
        assert detect_db_engine("https://example.com/x.json") is None
        assert detect_db_engine("s3://bucket/key.jsonl") is None

    def test_is_db_uri(self):
        assert is_db_uri("clickhouse://h/db") is True
        assert is_db_uri("file.csv") is False

    def test_iterable_only_engines_constant(self):
        assert ITERABLE_ONLY_DB_ENGINES == {"mssql", "clickhouse", "mongo", "elasticsearch"}


class TestSplitDriverKwargs:
    def test_extracts_known_params(self):
        uri = "mongodb://h:27017/db?collection=users&limit=100&authSource=admin"
        clean, kwargs = _split_driver_kwargs(uri)
        assert kwargs == {"collection": "users", "limit": 100}
        # Non-consumed params remain on the connection URI.
        assert "authSource=admin" in clean
        assert "collection" not in clean

    def test_columns_split_into_list(self):
        uri = "clickhouse://h/db?columns=a,b,c"
        _clean, kwargs = _split_driver_kwargs(uri)
        assert kwargs["columns"] == ["a", "b", "c"]

    def test_no_query_string(self):
        uri = "clickhouse://h:9000/db"
        clean, kwargs = _split_driver_kwargs(uri)
        assert clean == uri
        assert kwargs == {}


class TestOpenDbSource:
    @patch("undatum.common.db_source.open_iterable")
    def test_routes_to_engine(self, mock_open_iterable):
        open_db_source("clickhouse://h:9000/db", query="SELECT 1")
        mock_open_iterable.assert_called_once()
        args, kwargs = mock_open_iterable.call_args
        assert args[0] == "clickhouse://h:9000/db"
        assert kwargs["engine"] == "clickhouse"
        assert kwargs["iterableargs"]["query"] == "SELECT 1"

    @patch("undatum.common.db_source.open_iterable")
    def test_merges_uri_and_explicit_kwargs(self, mock_open_iterable):
        open_db_source(
            "mongodb://h:27017/db?collection=users",
            iterableargs={"batch_size": 500},
        )
        _args, kwargs = mock_open_iterable.call_args
        assert kwargs["engine"] == "mongo"
        ia = kwargs["iterableargs"]
        assert ia["collection"] == "users"
        assert ia["batch_size"] == 500

    def test_rejects_non_db_uri(self):
        with pytest.raises(ValueError):
            open_db_source("/tmp/data.csv")
