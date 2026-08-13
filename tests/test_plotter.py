"""Tests for plot command filtering and aggregation."""

import pytest

pytest.importorskip("matplotlib")

from undatum.cmds.plotter import Plotter  # noqa: E402


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "people.csv"
    path.write_text(
        "name,age,city,amount,status\n"
        "Alice,30,NY,10,active\n"
        "Bob,25,London,20,inactive\n"
        "Charlie,35,NY,30,active\n"
        "Diana,28,Paris,5,active\n"
    )
    return str(path)


class TestPlotFilterAggregate:
    def test_filter_data(self):
        plotter = Plotter()
        data = [{"city": "NY", "age": 30}, {"city": "London", "age": 20}]
        out = plotter._filter_data(data, "age >= 25")
        assert out == [{"city": "NY", "age": 30}]

    def test_aggregate_skipped_for_histogram(self):
        plotter = Plotter()
        data = [{"age": 10}, {"age": 20}]
        out = plotter._aggregate_data(data, ["age"], {"aggregate": "count"}, "histogram")
        assert out == data

    def test_aggregate_sum(self):
        plotter = Plotter()
        data = [
            {"city": "A", "n": 10},
            {"city": "A", "n": 20},
            {"city": "B", "n": 5},
        ]
        out = plotter._aggregate_data(
            data, ["city"], {"aggregate": "sum", "value_field": "n"}, "bar"
        )
        by_city = {row["city"]: row["_count"] for row in out}
        assert by_city["A"] == 30
        assert by_city["B"] == 5

    def test_aggregate_mean_and_top_n(self):
        plotter = Plotter()
        data = [
            {"city": "A", "n": 10},
            {"city": "A", "n": 20},
            {"city": "B", "n": 100},
            {"city": "C", "n": 1},
        ]
        out = plotter._aggregate_data(
            data,
            ["city"],
            {"aggregate": "mean", "value_field": "n", "top_n": 2},
            "bar",
        )
        assert len(out) == 2
        assert out[0]["city"] == "B"
        assert out[0]["_count"] == 100
        assert out[1]["city"] == "A"
        assert out[1]["_count"] == 15

    def test_aggregate_sum_requires_value_field(self):
        plotter = Plotter()
        with pytest.raises(ValueError, match="value-field"):
            plotter._aggregate_data([{"city": "A"}], ["city"], {"aggregate": "sum"}, "bar")

    def test_bar_png_with_filter(self, sample_csv, tmp_path):
        out = tmp_path / "cities.png"
        Plotter().plot(
            sample_csv,
            field="city",
            plot_type="bar",
            output=str(out),
            output_format="png",
            filter="age >= 30",
        )
        assert out.exists() and out.stat().st_size > 0

    def test_histogram_png(self, sample_csv, tmp_path):
        out = tmp_path / "ages.png"
        Plotter().plot(
            sample_csv,
            field="age",
            plot_type="histogram",
            output=str(out),
        )
        assert out.exists() and out.stat().st_size > 0

    def test_bar_sum_png(self, sample_csv, tmp_path):
        out = tmp_path / "amounts.png"
        Plotter().plot(
            sample_csv,
            field="city",
            plot_type="bar",
            output=str(out),
            aggregate="sum",
            value_field="amount",
        )
        assert out.exists() and out.stat().st_size > 0

    def test_read_data_flatten_nested(self, tmp_path):
        src = tmp_path / "nested.jsonl"
        src.write_text(
            '{"name": "TJK", "capital_city": {"lat": 38.56, "lon": 68.77}}\n',
            encoding="utf8",
        )
        rows = Plotter()._read_data(str(src), ["capital_city.lat"], {"flatten_nested": True})
        assert rows[0]["capital_city.lat"] == 38.56
