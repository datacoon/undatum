# -*- coding: utf8 -*-
"""Example command plugin for undatum."""
from typing import Annotated, Optional

import typer

from undatum.plugins.base import CommandPlugin, Plugin


def register(undatum_app) -> Plugin:
    """Register plugin with undatum.
    
    This function is called by undatum's plugin system.
    It should return a Plugin instance.
    
    Args:
        undatum_app: Undatum Typer app instance
        
    Returns:
        Plugin instance
    """
    return ExampleCommandPlugin(undatum_app)


class ExampleCommandPlugin(CommandPlugin):
    """Example plugin that adds a custom command."""
    
    def __init__(self, app):
        super().__init__(
            name="example-command",
            version="1.0.0",
            description="Example command plugin demonstrating plugin API"
        )
        self.app = app
    
    def register_commands(self, app) -> None:
        """Register commands with undatum CLI."""
        
        @app.command()
        def example(
            input_file: Annotated[str, typer.Argument(help="Input file path.")],
            output: Annotated[Optional[str], typer.Option(help="Output file path.")] = None,
            verbose: Annotated[bool, typer.Option(help="Enable verbose output.")] = False
        ):
            """Example command added by plugin.
            
            This command demonstrates how plugins can add custom commands to undatum.
            """
            print(f"Example plugin command called with input: {input_file}")
            if output:
                print(f"Output: {output}")
            if verbose:
                print("Verbose mode enabled")
