"""Optional FastAPI/Jinja2 dependency gate for the web extra."""


def require_web_dependencies() -> None:
    """Raise DependencyError when the optional web extra is not installed."""
    try:
        import fastapi  # noqa: F401
        import jinja2  # noqa: F401
        import multipart  # noqa: F401
        import uvicorn  # noqa: F401
    except ImportError as exc:
        from ..common.errors import DependencyError

        raise DependencyError(
            "fastapi",
            feature="web UI",
            install_command='pip install "undatum[web]"',
        ) from exc
