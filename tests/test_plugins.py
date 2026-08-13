"""Tests for plugin registry, validation, and transform application."""

from undatum.cli.plugins_cli import _validate_plugin
from undatum.plugins.base import ConnectorPlugin, TransformPlugin
from undatum.plugins.registry import PluginRegistry


class UpperTransform(TransformPlugin):
    def __init__(self):
        super().__init__(name="upper", version="1.0.0", description="uppercase strings")

    def register_transforms(self, registry):
        registry.register(self)

    def transform(self, record, **kwargs):
        return {k: v.upper() if isinstance(v, str) else v for k, v in record.items()}


class DemoConnector(ConnectorPlugin):
    def __init__(self):
        super().__init__(name="demo", version="1.0.0", description="demo connector")

    def register_connectors(self, registry):
        registry.register(self)

    def can_handle(self, uri: str) -> bool:
        return uri.startswith("demo://")

    def open(self, uri: str, mode: str = "r", **kwargs):
        return open(uri[len("demo://") :], mode)


def test_registry_find_connector_and_transform():
    registry = PluginRegistry()
    transform = UpperTransform()
    connector = DemoConnector()
    registry.register(transform)
    registry.register(connector)

    assert registry.find_transform("upper") is transform
    assert registry.find_connector("demo://file.csv") is connector
    assert registry.find_connector("/local/file.csv") is None


def test_apply_named_transform():
    registry = PluginRegistry()
    registry.register(UpperTransform())
    result = registry.apply_transforms({"name": "alice", "age": 3}, names=["upper"])
    assert result == {"name": "ALICE", "age": 3}


def test_validate_transform_plugin():
    assert _validate_plugin(UpperTransform()) == []
    assert _validate_plugin(DemoConnector()) == []


def test_plugins_info_lists_transform_and_connector(monkeypatch):
    from typer.testing import CliRunner

    from undatum.cli import plugins_cli
    from undatum.core import app

    registry = PluginRegistry()
    registry.register(UpperTransform())
    registry.register(DemoConnector())
    monkeypatch.setattr(plugins_cli.plugin_manager, "get_registry", lambda: registry)

    runner = CliRunner()
    transform = runner.invoke(app, ["plugins", "info", "upper"])
    assert transform.exit_code == 0, transform.stdout
    assert "Transform" in transform.stdout
    assert "upper" in transform.stdout

    connector = runner.invoke(app, ["plugins", "info", "demo"])
    assert connector.exit_code == 0, connector.stdout
    assert "Connector" in connector.stdout
    assert "demo" in connector.stdout
