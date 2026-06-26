#!/usr/bin/env python
"""Main CLI assembly for the undatum package.

Composes the Typer application from per-domain CLI modules in
``undatum.cli`` and loads plugins. Individual command implementations
live in ``undatum.cli.data_commands`` and the other ``undatum.cli``
modules.
"""

import logging
from typing import Annotated, Optional

import typer

from . import __version__
from .cli.ai_cli import ai_app
from .cli.api_cli import api_app
from .cli.common import enable_verbose  # noqa: F401 - re-exported for backward compatibility
from .cli.data_commands import data_app
from .cli.db_cli import db_app
from .cli.examples_cli import examples_app
from .cli.formats_cli import formats_app
from .cli.mcp_cli import mcp_app
from .cli.package_cli import package_app
from .cli.pipeline_cli import pipeline_app, templates_app  # noqa: F401
from .cli.plugins_cli import plugin_manager, plugins_app

logger = logging.getLogger(__name__)

app = typer.Typer()


def _version_callback(value: bool):
    if value:
        typer.echo(f"undatum {__version__}")
        raise typer.Exit()


@app.callback()
def _main_callback(
    version: Annotated[
        Optional[bool],
        typer.Option("--version", callback=_version_callback, is_eager=True, help="Show version."),
    ] = None,
):
    """undatum: a command-line tool for data processing and analysis."""


# Merge top-level data commands into the main app
app.registered_commands.extend(data_app.registered_commands)

app.add_typer(ai_app, name="ai")
app.add_typer(package_app, name="package")
app.add_typer(api_app, name="api")
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(db_app, name="db")
app.add_typer(examples_app, name="examples")
app.add_typer(formats_app, name="formats")
app.add_typer(mcp_app, name="mcp")
app.add_typer(plugins_app, name="plugins")

# Load and register plugins after app is fully initialized
try:
    plugin_manager.load_all_plugins(app)
    registry = plugin_manager.get_registry()
    for command_plugin in registry.get_command_plugins():
        try:
            command_plugin.register_commands(app)
        except Exception as e:
            logger.warning(f"Failed to register commands from plugin '{command_plugin.name}': {e}")
except Exception as e:
    logger.warning(f"Failed to load/register plugins: {e}")

# Sort commands alphabetically for help output
app.registered_commands.sort(
    key=lambda cmd: (cmd.name or (cmd.callback.__name__ if cmd.callback else "")).lower()
)


if __name__ == "__main__":
    app()
