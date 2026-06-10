import json

import pytest

from undatum.cmds.api import DataApi, _build_api_app, dump_api_config, load_api_config


def test_api_discover_writes_config(tmp_path):
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text("id,amount\n1,100\n2,200\n", encoding="utf8")
    output_path = tmp_path / "api.yml"

    DataApi().discover([str(csv_path)], {"output": str(output_path), "format_in": "csv"})

    config = load_api_config(str(output_path))
    assert "resources" in config
    assert config["resources"][0]["name"] == "sales"
    assert config["resources"][0]["format"] == "csv"


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


def test_api_build_app(tmp_path):
    try:
        import fastapi  # noqa: F401
    except Exception:
        pytest.skip("fastapi not installed")

    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf8")
    config = {
        "resources": [
            {
                "name": "data",
                "path": str(csv_path),
                "format": "csv",
                "primary_key": "id",
                "fields": [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}],
                "pagination": {"default_limit": 5, "max_limit": 50},
                "query": {"allowed_ops": ["eq"], "allowed_order_by": ["id", "name"]},
            }
        ]
    }
    app = _build_api_app(config)
    assert app is not None
