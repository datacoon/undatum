"""
undatum: a command-line tool for data processing. Brings CSV simplicity to JSON lines and BSON

"""

__version__ = "1.4.0"
__author__ = "Ivan Begtin"
__licence__ = "MIT"

__all__ = ["Dataset"]


def __getattr__(name):
    # Lazy import so `import undatum` stays lightweight for CLI startup
    if name == "Dataset":
        from .sdk.dataset import Dataset

        return Dataset
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
