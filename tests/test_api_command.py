import json
from unittest.mock import patch

import pytest

from undatum.cmds.api import (
    DataApi,
    _apply_sort_alias,
    _build_api_app,
    _unique_resource_name,
    dump_api_config,
    load_api_config,
    require_api_dependencies,
)


def _skip_without_api():
    try:
        import fastapi  # noqa: F401
        from fastapi.testclient import TestClient  # noqa: F401
    except ImportError:
        pytest.skip("fastapi not installed (pip install 'undatum[api]')")


def _sample_config(csv_path):
    return {
        "resources": [
            {
                "name": "data",
                "path": str(csv_path),
                "format": "csv",
                "primary_key": "id",
                "fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "name", "type": "varchar"},
                    {"name": "amount", "type": "integer"},
                ],
                "pagination": {"default_limit": 5, "max_limit": 50},
                "query": {
                    "allowed_ops": ["eq", "ne", "lt", "gt", "le", "ge", "like"],
                    "allowed_order_by": ["id", "name", "amount"],
                },
            }
        ]
    }


@pytest.fixture
def sample_csv(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "id,name,amount\n1,Alice,100\n2,Bob,200\n3,Carol,150\n",
        encoding="utf8",
    )
    return csv_path


def test_api_discover_writes_config(tmp_path):
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text("id,amount\n1,100\n2,200\n", encoding="utf8")
    output_path = tmp_path / "api.yml"

    DataApi().discover([str(csv_path)], {"output": str(output_path), "format_in": "csv"})

    config = load_api_config(str(output_path))
    assert "resources" in config
    assert config["resources"][0]["name"] == "sales"
    assert config["resources"][0]["format"] == "csv"
    assert config["resources"][0]["path"] == str(csv_path.resolve())


def test_api_discover_resource_name_collision(tmp_path, capsys):
    first = tmp_path / "sales.csv"
    second = tmp_path / "sales.parquet"
    first.write_text("id,v\n1,a\n", encoding="utf8")
    second.write_text("not parquet", encoding="utf8")

    with patch("undatum.cmds.api._detect_format", side_effect=["csv", "csv"]):
        with patch(
            "undatum.cmds.api._infer_fields",
            return_value=[{"name": "id", "type": "integer"}, {"name": "v", "type": "varchar"}],
        ):
            with patch("undatum.cmds.api._infer_primary_key_candidates", return_value=[]):
                config = DataApi().discover(
                    [str(first), str(second)],
                    {"emit": False, "format_in": "csv"},
                )

    names = [resource["name"] for resource in config["resources"]]
    assert names == ["sales", "sales_2"]
    assert "collision" in capsys.readouterr().err.lower()


def test_api_config_json_roundtrip(tmp_path):
    config = {
        "resources": [
            {
                "name": "items",
                "path": "data/items.csv",
                "format": "csv",
                "fields": [{"name": "id", "type": "integer"}],
                "pagination": {"default_limit": 10, "max_limit": 100},
                "query": {"allowed_ops": ["eq"], "allowed_order_by": ["id"]},
            }
        ]
    }
    output_path = tmp_path / "api.json"
    output_path.write_text(dump_api_config(config, output=str(output_path), config_format="json"))

    loaded = load_api_config(str(output_path))
    assert loaded == json.loads(json.dumps(config))


def test_unique_resource_name():
    used: set[str] = set()
    assert _unique_resource_name("sales", used) == "sales"
    assert _unique_resource_name("sales", used) == "sales_2"
    assert _unique_resource_name("sales", used) == "sales_3"


def test_apply_sort_alias():
    assert _apply_sort_alias({"sort": "-amount"}) == {
        "order_by": "amount",
        "order_dir": "desc",
    }
    assert _apply_sort_alias({"sort": "name"}) == {
        "order_by": "name",
        "order_dir": "asc",
    }


def test_require_api_dependencies_raises():
    with patch.dict("sys.modules", {"fastapi": None, "uvicorn": None}):
        with pytest.raises(Exception) as exc_info:
            require_api_dependencies()
        assert "undatum[api]" in str(exc_info.value)


def test_api_build_app(sample_csv):
    _skip_without_api()
    app = _build_api_app(_sample_config(sample_csv))
    assert app is not None


def test_api_list_envelope_and_filter(sample_csv):
    _skip_without_api()
    from fastapi.testclient import TestClient

    client = TestClient(_build_api_app(_sample_config(sample_csv)))
    response = client.get("/data?amount__gt=100")
    assert response.status_code == 200
    payload = response.json()
    assert "data" in payload
    assert "pagination" in payload
    assert payload["pagination"]["count"] == 2
    assert all(row["amount"] > 100 for row in payload["data"])


def test_api_sort_alias(sample_csv):
    _skip_without_api()
    from fastapi.testclient import TestClient

    client = TestClient(_build_api_app(_sample_config(sample_csv)))
    response = client.get("/data?sort=-amount")
    assert response.status_code == 200
    amounts = [row["amount"] for row in response.json()["data"]]
    assert amounts == sorted(amounts, reverse=True)


def test_api_include_total(sample_csv):
    _skip_without_api()
    from fastapi.testclient import TestClient

    client = TestClient(_build_api_app(_sample_config(sample_csv)))
    response = client.get("/data?include_total=true&amount__gt=100")
    assert response.status_code == 200
    assert response.json()["pagination"]["total"] == 2


def test_api_detail_endpoint(sample_csv):
    _skip_without_api()
    from fastapi.testclient import TestClient

    client = TestClient(_build_api_app(_sample_config(sample_csv)))
    response = client.get("/data/2")
    assert response.status_code == 200
    assert response.json()["name"] == "Bob"


def test_api_root_and_docs(sample_csv):
    _skip_without_api()
    from fastapi.testclient import TestClient

    client = TestClient(_build_api_app(_sample_config(sample_csv)))
    root = client.get("/")
    assert root.status_code == 200
    assert root.json()["docs"] == "/docs"
    assert root.json()["resources"][0]["name"] == "data"

    docs = client.get("/docs")
    assert docs.status_code == 200

    openapi = client.get("/openapi.json")
    assert openapi.status_code == 200
    schema = openapi.json()
    assert "/data" in schema["paths"]
    list_params = {param["name"] for param in schema["paths"]["/data"]["get"]["parameters"]}
    assert {"limit", "offset", "order_by", "sort"}.issubset(list_params)


def test_api_composite_primary_key_no_detail_route(sample_csv):
    _skip_without_api()
    from fastapi.testclient import TestClient

    config = _sample_config(sample_csv)
    config["resources"][0]["primary_key"] = ["id", "name"]
    client = TestClient(_build_api_app(config))
    assert client.get("/data/1").status_code == 404
    openapi_paths = client.get("/openapi.json").json()["paths"]
    assert "/data/{pk}" not in openapi_paths


def test_api_export_openapi(sample_csv, tmp_path):
    _skip_without_api()

    config_path = tmp_path / "api.json"
    config_path.write_text(
        dump_api_config(_sample_config(sample_csv), config_format="json"),
        encoding="utf8",
    )
    output_path = tmp_path / "openapi.json"
    schema = DataApi().export_openapi(str(config_path), {"output": str(output_path)})

    assert output_path.exists()
    assert schema["info"]["title"] == "undatum Data API"
    assert "/data" in schema["paths"]
    loaded = json.loads(output_path.read_text(encoding="utf8"))
    assert loaded["paths"]["/data"]["get"]["responses"]["200"]
