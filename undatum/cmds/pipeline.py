"""Pipeline execution module for running declarative workflows."""

import logging
import os
import tempfile
from typing import Any, Optional

from ..common.errors import FileNotFoundError, PermissionError, ValidationError
from ..common.pipeline_parser import (
    PipelineSpec,
    validate_pipeline,
)

logger = logging.getLogger(__name__)


class PipelineRunner:
    """Pipeline execution engine."""

    def __init__(self, dry_run: bool = False):
        """Initialize pipeline runner.

        Args:
            dry_run: If True, validate only without executing
        """
        self.dry_run = dry_run
        self.working_dir = os.getcwd()
        self.temp_files = []
        self.step_outputs = {}  # Track outputs from each step

    def run(self, spec: PipelineSpec, variables: Optional[dict[str, str]] = None) -> bool:
        """Execute pipeline specification.

        Args:
            spec: PipelineSpec to execute
            variables: Optional variable overrides

        Returns:
            True if all steps succeeded, False otherwise
        """
        # Resolve variables
        resolved_spec = spec.resolve_variables(overrides=variables)

        # Validate pipeline
        validation_errors = validate_pipeline(resolved_spec)
        if validation_errors:
            logger.error("Pipeline validation failed:")
            for error in validation_errors:
                logger.error(f"  - {error}")
            return False

        if self.dry_run:
            logger.info("Dry run mode: validation passed, skipping execution")
            return True

        # Execute steps
        try:
            for i, step in enumerate(resolved_spec.steps):
                step_name = step.get("name", f"step_{i+1}")
                logger.info(f"Executing step {i+1}/{len(resolved_spec.steps)}: {step_name}")

                success = self._execute_step(step, step_name)
                if not success:
                    logger.error(f"Step '{step_name}' failed")
                    return False

                logger.info(f"Step '{step_name}' completed successfully")

            logger.info("Pipeline execution completed successfully")
            return True

        finally:
            # Cleanup temporary files
            self._cleanup()

    def _execute_step(self, step: dict[str, Any], step_name: str) -> bool:
        """Execute a single pipeline step.

        Args:
            step: Step definition
            step_name: Step name for logging

        Returns:
            True if step succeeded, False otherwise
        """
        command = step.get("command")
        args = step.get("args", {})

        # Resolve input/output paths
        resolved_args = self._resolve_paths(args, step_name)

        # Map pipeline args to CLI command invocation
        try:
            # Import command processors dynamically
            from ..core import app

            # Get command function from typer app
            command_func = None
            for cmd_name, _cmd_info in app.registered_commands.items():
                if cmd_name == command:
                    # Find the command callback
                    for group in app.registered_groups.values():
                        if command in group.commands:
                            command_func = group.commands[command].callback
                            break
                    if not command_func:
                        # Try direct command
                        if hasattr(app, "commands") and command in app.commands:
                            command_func = app.commands[command].callback
                    break

            if not command_func:
                # Fallback: use subprocess to call CLI
                return self._execute_via_cli(command, resolved_args)

            # Convert args to function parameters
            # This is a simplified approach - in practice, we'd need to map
            # pipeline args to CLI option names
            try:
                # For now, use subprocess approach which is more reliable
                return self._execute_via_cli(command, resolved_args)
            except Exception as e:
                logger.error(f"Error executing step '{step_name}': {e}")
                return False

        except FileNotFoundError as e:
            logger.error(f"Step '{step_name}' failed: {e}")
            return False
        except PermissionError as e:
            logger.error(f"Step '{step_name}' failed: {e}")
            return False
        except ValidationError as e:
            logger.error(f"Step '{step_name}' failed: {e}")
            return False
        except Exception as e:
            from ..common.errors import format_error_message

            error_msg = format_error_message(e, verbose=False)
            logger.error(f"Step '{step_name}' failed: {error_msg}")
            return False

    def _execute_via_cli(self, command: str, args: dict[str, Any]) -> bool:
        """Execute command via CLI subprocess.

        Args:
            command: Command name
            args: Command arguments

        Returns:
            True if command succeeded, False otherwise
        """
        import subprocess

        # Build command line
        cmd = ["undatum", command]

        # Convert args to CLI options
        for key, value in args.items():
            if value is None:
                continue

            # Convert snake_case to kebab-case
            option = key.replace("_", "-")

            if isinstance(value, bool):
                if value:
                    cmd.append(f"--{option}")
            elif isinstance(value, (list, tuple)):
                # Handle list values (e.g., fields)
                if key == "fields" or key.endswith("_fields"):
                    cmd.extend([f"--{option}", ",".join(str(v) for v in value)])
                else:
                    for v in value:
                        cmd.extend([f"--{option}", str(v)])
            else:
                cmd.extend([f"--{option}", str(value)])

        # Execute command
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode != 0:
                logger.error(f"Command failed: {' '.join(cmd)}")
                if result.stderr:
                    logger.error(f"Error output: {result.stderr}")
                return False

            if result.stdout:
                logger.debug(f"Command output: {result.stdout}")

            return True

        except Exception as e:
            logger.error(f"Error executing command: {e}")
            return False

    def _resolve_paths(self, args: dict[str, Any], step_name: str) -> dict[str, Any]:
        """Resolve input/output paths, handling step dependencies.

        Args:
            args: Step arguments
            step_name: Step name

        Returns:
            Resolved arguments with paths updated
        """
        resolved = dict(args)

        # Check for input path
        for input_key in ("input", "input_file", "fromfile", "from"):
            if input_key in resolved:
                input_path = resolved[input_key]
                # Check if this references a previous step's output
                if input_path.startswith("$") and input_path[1:] in self.step_outputs:
                    resolved[input_key] = self.step_outputs[input_path[1:]]
                break

        # Check for output path
        output_path = None
        for output_key in ("output", "output_file", "tofile", "to"):
            if output_key in resolved:
                output_path = resolved[output_key]
                break

        # If no explicit output, create temp file
        if not output_path and "output" not in resolved:
            output_path = self._create_temp_file(step_name)
            resolved["output"] = output_path

        # Store output for next steps
        if output_path:
            self.step_outputs[step_name] = output_path

        return resolved

    def _create_temp_file(self, step_name: str) -> str:
        """Create temporary file for step output.

        Args:
            step_name: Step name (for file naming)

        Returns:
            Temporary file path
        """
        temp_fd, temp_path = tempfile.mkstemp(suffix=".jsonl", prefix=f"{step_name}_")
        os.close(temp_fd)
        self.temp_files.append(temp_path)
        return temp_path

    def _cleanup(self):
        """Clean up temporary files."""
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except OSError as e:
                logger.warning(f"Failed to remove temporary file {temp_file}: {e}")
