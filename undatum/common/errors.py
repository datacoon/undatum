# -*- coding: utf8 -*-
"""Error handling utilities and custom exception classes for undatum commands.

This module provides custom exception classes and error handling utilities
to ensure consistent, user-friendly error messages across all commands.
"""
import os
import sys
from difflib import get_close_matches
from pathlib import Path
from typing import List, Optional


class UndatumError(Exception):
    """Base exception class for all undatum errors.
    
    All custom exceptions should inherit from this class to ensure
    consistent error handling across the application.
    """
    
    def __init__(self, message: str, context: Optional[dict] = None, exit_code: int = 1):
        """Initialize error with message and optional context.
        
        Args:
            message: User-friendly error message
            context: Optional dictionary with additional context (file path, field name, etc.)
            exit_code: Exit code for the error (1=user error, 2=config, 3=system, 4=internal)
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.exit_code = exit_code
    
    def __str__(self) -> str:
        return self.message


class FileNotFoundError(UndatumError):
    """Raised when a file is not found.
    
    Provides suggestions for similar file names to help with typos.
    """
    
    def __init__(self, file_path: str, suggestions: Optional[List[str]] = None):
        """Initialize file not found error.
        
        Args:
            file_path: Path to the file that was not found
            suggestions: Optional list of suggested file paths
        """
        message = f"File not found: '{file_path}'"
        if suggestions:
            if len(suggestions) == 1:
                message += f"\nDid you mean: '{suggestions[0]}'?"
            else:
                message += "\nDid you mean one of these?"
                for suggestion in suggestions[:5]:  # Limit to 5 suggestions
                    message += f"\n  - {suggestion}"
        else:
            message += "\nCheck that the file path is correct and the file exists."
        
        super().__init__(message, context={'file_path': file_path}, exit_code=1)


class PermissionError(UndatumError):
    """Raised when file permission is denied.
    
    Provides actionable guidance for fixing permission issues.
    """
    
    def __init__(self, file_path: str, operation: str = "read"):
        """Initialize permission error.
        
        Args:
            file_path: Path to the file with permission issues
            operation: Operation that was attempted (read, write, execute)
        """
        message = f"Permission denied: Cannot {operation} '{file_path}'"
        
        if operation == "read":
            message += "\nCheck file permissions. You may need to run:"
            message += f"\n  chmod +r '{file_path}'"
        elif operation == "write":
            message += "\nCheck file permissions. You may need to run:"
            message += f"\n  chmod +w '{file_path}'"
            message += "\nOr check if the directory is writable."
        
        super().__init__(message, context={'file_path': file_path, 'operation': operation}, exit_code=3)


class ValidationError(UndatumError):
    """Raised when input validation fails.
    
    Provides clear explanation of what was wrong and suggestions for fixing.
    """
    
    def __init__(self, message: str, field: Optional[str] = None, suggestions: Optional[List[str]] = None):
        """Initialize validation error.
        
        Args:
            message: Error message explaining what was wrong
            field: Optional field name that caused the error
            suggestions: Optional list of suggested valid values
        """
        error_msg = f"Invalid input: {message}"
        
        if field:
            error_msg = f"Invalid parameter '{field}': {message}"
        
        if suggestions:
            if len(suggestions) == 1:
                error_msg += f"\nDid you mean: '{suggestions[0]}'?"
            else:
                error_msg += "\nValid options:"
                for suggestion in suggestions[:10]:  # Limit to 10 suggestions
                    error_msg += f"\n  - {suggestion}"
        
        super().__init__(error_msg, context={'field': field}, exit_code=1)


class FormatError(UndatumError):
    """Raised when a file format is not supported.
    
    Lists supported formats and suggests conversion if applicable.
    """
    
    def __init__(self, file_path: str, format_name: str, supported_formats: Optional[List[str]] = None):
        """Initialize format error.
        
        Args:
            file_path: Path to the file with unsupported format
            format_name: The unsupported format name/extension
            supported_formats: Optional list of supported formats
        """
        message = f"Unsupported file format: '{format_name}'"
        
        if supported_formats:
            message += f"\nSupported formats: {', '.join(supported_formats)}"
        
        message += f"\nConvert the file with: undatum convert '{file_path}' <output>"
        
        super().__init__(message, context={'file_path': file_path, 'format': format_name}, exit_code=1)


class ConfigurationError(UndatumError):
    """Raised when there's a configuration issue.
    
    Provides guidance for fixing configuration problems.
    """
    
    def __init__(self, message: str, config_key: Optional[str] = None, fix_hint: Optional[str] = None):
        """Initialize configuration error.
        
        Args:
            message: Error message explaining the configuration issue
            config_key: Optional configuration key that caused the error
            fix_hint: Optional hint for fixing the issue
        """
        error_msg = f"Configuration error: {message}"
        
        if config_key:
            error_msg = f"Configuration error in '{config_key}': {message}"
        
        if fix_hint:
            error_msg += f"\n{fix_hint}"
        
        super().__init__(error_msg, context={'config_key': config_key}, exit_code=2)


class DependencyError(UndatumError):
    """Raised when a required dependency is missing.
    
    Provides installation instructions.
    """
    
    def __init__(self, package_name: str, feature: Optional[str] = None, install_command: Optional[str] = None):
        """Initialize dependency error.
        
        Args:
            package_name: Name of the missing package
            feature: Optional feature name that requires this dependency
            install_command: Optional custom install command (defaults to pip install)
        """
        message = f"Missing dependency: '{package_name}'"
        
        if feature:
            message += f"\nThis feature requires '{package_name}'"
        
        if install_command:
            message += f"\nInstall with: {install_command}"
        else:
            message += f"\nInstall with: pip install {package_name}"
        
        super().__init__(message, context={'package': package_name, 'feature': feature}, exit_code=2)


class DatabaseError(UndatumError):
    """Raised when database operations fail.
    
    Provides connection and query error guidance.
    """
    
    def __init__(self, message: str, db_type: Optional[str] = None, connection_uri: Optional[str] = None):
        """Initialize database error.
        
        Args:
            message: Error message explaining the database issue
            db_type: Optional database type (postgresql, mysql, etc.)
            connection_uri: Optional connection URI (may be masked for security)
        """
        error_msg = f"Database error: {message}"
        
        if db_type:
            error_msg = f"{db_type} error: {message}"
        
        if connection_uri:
            # Mask password in connection URI for security
            masked_uri = _mask_connection_uri(connection_uri)
            error_msg += f"\nConnection: {masked_uri}"
        
        error_msg += "\nCheck that the database is running and accessible."
        
        super().__init__(error_msg, context={'db_type': db_type}, exit_code=3)


def _mask_connection_uri(uri: str) -> str:
    """Mask password in connection URI for security.
    
    Args:
        uri: Connection URI string
        
    Returns:
        URI with password masked
    """
    if '@' in uri:
        parts = uri.split('@')
        if '://' in parts[0]:
            protocol, auth = parts[0].split('://', 1)
            if ':' in auth:
                user, _ = auth.split(':', 1)
                return f"{protocol}://{user}:***@{parts[1]}"
    return uri


def find_similar_files(file_path: str, max_suggestions: int = 5) -> List[str]:
    """Find similar file names in the same directory to help with typos.
    
    Args:
        file_path: Path to the file that was not found
        max_suggestions: Maximum number of suggestions to return
        
    Returns:
        List of similar file paths
    """
    try:
        path = Path(file_path)
        directory = path.parent
        filename = path.name
        
        if not directory.exists() or not directory.is_dir():
            return []
        
        # Get all files in the directory
        files = [f.name for f in directory.iterdir() if f.is_file()]
        
        if not files:
            return []
        
        # Find close matches
        matches = get_close_matches(filename, files, n=max_suggestions, cutoff=0.6)
        
        # Return full paths
        return [str(directory / match) for match in matches]
    except Exception:
        return []


def find_similar_field_names(field_name: str, available_fields: List[str], max_suggestions: int = 5) -> List[str]:
    """Find similar field names to help with typos.
    
    Args:
        field_name: Field name that was not found
        available_fields: List of available field names
        max_suggestions: Maximum number of suggestions to return
        
    Returns:
        List of similar field names
    """
    if not available_fields:
        return []
    
    matches = get_close_matches(field_name, available_fields, n=max_suggestions, cutoff=0.6)
    return matches


def format_error_message(error: Exception, verbose: bool = False) -> str:
    """Format an exception into a user-friendly error message.
    
    Args:
        error: The exception to format
        verbose: If True, include full traceback
        
    Returns:
        Formatted error message
    """
    if isinstance(error, UndatumError):
        return str(error)
    
    # For non-UndatumError exceptions, provide user-friendly message
    error_type = type(error).__name__
    error_msg = str(error)
    
    # Map common Python exceptions to user-friendly messages
    if isinstance(error, FileNotFoundError):
        return f"File not found: {error_msg}\nCheck that the file path is correct and the file exists."
    elif isinstance(error, PermissionError):
        return f"Permission denied: {error_msg}\nCheck file permissions."
    elif isinstance(error, ValueError):
        return f"Invalid value: {error_msg}"
    elif isinstance(error, KeyError):
        return f"Missing key: {error_msg}"
    elif isinstance(error, TypeError):
        return f"Type error: {error_msg}"
    else:
        if verbose:
            import traceback
            return f"{error_type}: {error_msg}\n\n{traceback.format_exc()}"
        else:
            return f"Error: {error_msg}\n\nRun with --verbose for detailed error information."


def handle_command_error(error: Exception, verbose: bool = False) -> int:
    """Handle an error from a command and return appropriate exit code.
    
    Args:
        error: The exception that occurred
        verbose: If True, show full traceback
        
    Returns:
        Exit code for the command
    """
    if isinstance(error, UndatumError):
        print(f"Error: {error.message}", file=sys.stderr)
        if verbose and error.__cause__:
            import traceback
            print("\nDetailed error information:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        return error.exit_code
    
    # Format non-UndatumError exceptions
    message = format_error_message(error, verbose=verbose)
    print(f"Error: {message}", file=sys.stderr)
    
    # Default exit codes based on exception type
    if isinstance(error, (FileNotFoundError, ValueError, KeyError)):
        return 1  # User error
    elif isinstance(error, PermissionError):
        return 3  # System error
    else:
        return 4  # Internal error
