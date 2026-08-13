"""Inspect resolved undatum configuration."""

from typing import Any

import typer
import yaml

from ..common.app_config import describe_cli_config

config_app = typer.Typer(help="Show resolved CLI defaults from config files and environment.")


def _dump(data: dict[str, Any]) -> str:
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


@config_app.callback(invoke_without_command=True)
def config_root(ctx: typer.Context) -> None:
    """Show resolved CLI defaults when no subcommand is given."""
    if ctx.invoked_subcommand is None:
        _print_config()


@config_app.command("show")
def config_show() -> None:
    """Print resolved CLI defaults and which config files were loaded."""
    _print_config()


def _print_config() -> None:
    typer.echo(_dump(describe_cli_config()).rstrip())
