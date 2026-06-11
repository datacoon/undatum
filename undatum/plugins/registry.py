"""Plugin registry for managing plugins."""

import logging
from typing import Optional

from .base import CommandPlugin, ConnectorPlugin, Plugin, TransformPlugin

logger = logging.getLogger(__name__)


class PluginRegistry:
    """Registry for managing undatum plugins."""

    def __init__(self):
        self._plugins: dict[str, Plugin] = {}
        self._command_plugins: list[CommandPlugin] = []
        self._connector_plugins: list[ConnectorPlugin] = []
        self._transform_plugins: list[TransformPlugin] = []

    def register(self, plugin: Plugin) -> None:
        """Register a plugin.

        Args:
            plugin: Plugin instance
        """
        if plugin.name in self._plugins:
            logger.warning(f"Plugin '{plugin.name}' is already registered, overwriting")

        self._plugins[plugin.name] = plugin

        if isinstance(plugin, CommandPlugin):
            self._command_plugins.append(plugin)
        if isinstance(plugin, ConnectorPlugin):
            self._connector_plugins.append(plugin)
        if isinstance(plugin, TransformPlugin):
            self._transform_plugins.append(plugin)

        logger.info(f"Registered plugin: {plugin.name} v{plugin.version}")

    def get_plugin(self, name: str) -> Optional[Plugin]:
        """Get plugin by name.

        Args:
            name: Plugin name

        Returns:
            Plugin instance or None
        """
        return self._plugins.get(name)

    def list_plugins(self) -> list[Plugin]:
        """List all registered plugins.

        Returns:
            List of plugin instances
        """
        return list(self._plugins.values())

    def get_command_plugins(self) -> list[CommandPlugin]:
        """Get all command plugins.

        Returns:
            List of command plugin instances
        """
        return self._command_plugins.copy()

    def get_connector_plugins(self) -> list[ConnectorPlugin]:
        """Get all connector plugins.

        Returns:
            List of connector plugin instances
        """
        return self._connector_plugins.copy()

    def get_transform_plugins(self) -> list[TransformPlugin]:
        """Get all transform plugins.

        Returns:
            List of transform plugin instances
        """
        return self._transform_plugins.copy()

    def find_connector(self, uri: str) -> Optional[ConnectorPlugin]:
        """Find connector plugin that can handle URI.

        Args:
            uri: URI to find connector for

        Returns:
            Connector plugin or None
        """
        for connector in self._connector_plugins:
            try:
                if connector.can_handle(uri):
                    return connector
            except Exception as e:
                logger.warning(f"Error checking connector {connector.name} for URI {uri}: {e}")
                continue
        return None
