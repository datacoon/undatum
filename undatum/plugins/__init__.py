# -*- coding: utf8 -*-
"""Plugin system for extending undatum functionality."""
from .manager import PluginManager
from .registry import PluginRegistry
from .base import Plugin, CommandPlugin, ConnectorPlugin, TransformPlugin

__all__ = [
    'PluginManager',
    'PluginRegistry',
    'Plugin',
    'CommandPlugin',
    'ConnectorPlugin',
    'TransformPlugin',
]
