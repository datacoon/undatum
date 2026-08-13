# -*- coding: utf8 -*-
"""Example IO connector plugin for undatum.

Registers a ``demo://`` URI scheme that reads a local file after the prefix.
"""
from __future__ import annotations

import os
from typing import Any

from undatum.plugins.base import ConnectorPlugin, Plugin


def register(undatum_app=None) -> Plugin:
    """Register plugin with undatum."""
    return ExampleConnectorPlugin()


class ExampleConnectorPlugin(ConnectorPlugin):
    """Demo connector that maps ``demo://path`` to a local file path."""

    def __init__(self):
        super().__init__(
            name="example-connector",
            version="1.0.0",
            description="Example connector plugin demonstrating the demo:// URI scheme",
        )

    def register_connectors(self, registry: Any) -> None:
        registry.register(self)

    def can_handle(self, uri: str) -> bool:
        return isinstance(uri, str) and uri.startswith("demo://")

    def open(self, uri: str, mode: str = "r", **kwargs) -> Any:
        local_path = uri[len("demo://") :]
        if not os.path.exists(local_path):
            raise FileNotFoundError(local_path)
        binary = "b" in mode
        return open(local_path, "rb" if binary or mode.startswith("r") else mode)
