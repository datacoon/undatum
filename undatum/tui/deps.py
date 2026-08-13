"""Optional Textual dependency gate for the TUI extra."""


def require_tui_dependencies() -> None:
    """Raise DependencyError when the optional TUI extra is not installed."""
    try:
        import textual  # noqa: F401
    except ImportError as exc:
        from ..common.errors import DependencyError

        raise DependencyError(
            "textual",
            feature="TUI",
            install_command='pip install "undatum[tui]"',
        ) from exc
