"""Base classes for undatum plugins."""

import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Base class for all undatum plugins."""

    def __init__(self, name: str, version: str = "1.0.0", description: str = ""):
        """Initialize plugin.

        Args:
            name: Plugin name
            version: Plugin version
            description: Plugin description
        """
        self.name = name
        self.version = version
        self.description = description

    def initialize(self, undatum_app: Any) -> None:
        """Initialize plugin with undatum app instance.

        Args:
            undatum_app: Undatum application instance
        """
        pass

    def cleanup(self) -> None:
        """Cleanup plugin resources."""
        pass


class CommandPlugin(Plugin):
    """Plugin that adds custom commands."""

    @abstractmethod
    def register_commands(self, app: Any) -> None:
        """Register commands with undatum CLI.

        Args:
            app: Typer app instance
        """
        pass


class ConnectorPlugin(Plugin):
    """Plugin that adds custom IO connectors."""

    @abstractmethod
    def register_connectors(self, registry: Any) -> None:
        """Register IO connectors.

        Args:
            registry: Connector registry instance
        """
        pass

    @abstractmethod
    def can_handle(self, uri: str) -> bool:
        """Check if connector can handle URI.

        Args:
            uri: URI to check

        Returns:
            True if connector can handle URI
        """
        pass

    @abstractmethod
    def open(self, uri: str, mode: str = "r", **kwargs) -> Any:
        """Open URI for reading or writing.

        Args:
            uri: URI to open
            mode: Open mode ('r' or 'w')
            **kwargs: Additional options

        Returns:
            File-like object
        """
        pass


class TransformPlugin(Plugin):
    """Plugin that adds custom transforms."""

    @abstractmethod
    def register_transforms(self, registry: Any) -> None:
        """Register transforms.

        Args:
            registry: Transform registry instance
        """
        pass

    @abstractmethod
    def transform(self, record: dict[str, Any], **kwargs) -> dict[str, Any]:
        """Transform a record.

        Args:
            record: Input record
            **kwargs: Transform options

        Returns:
            Transformed record
        """
        pass
