# -*- coding: utf8 -*-
"""Example transform plugin for undatum.

Uppercases string values in each record. Use with:

    undatum apply data.jsonl --plugin example-transform --output out.jsonl
"""
from __future__ import annotations

from typing import Any

from undatum.plugins.base import Plugin, TransformPlugin


def register(undatum_app=None) -> Plugin:
    """Register plugin with undatum."""
    return ExampleTransformPlugin()


class ExampleTransformPlugin(TransformPlugin):
    """Demo transform that uppercases string field values."""

    def __init__(self):
        super().__init__(
            name="example-transform",
            version="1.0.0",
            description="Example transform plugin that uppercases string values",
        )

    def register_transforms(self, registry: Any) -> None:
        registry.register(self)

    def transform(self, record: dict[str, Any], **kwargs) -> dict[str, Any]:
        result = {}
        for key, value in record.items():
            if isinstance(value, str):
                result[key] = value.upper()
            else:
                result[key] = value
        return result
