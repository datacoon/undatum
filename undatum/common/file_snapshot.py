"""Snapshots and metadata guards for destructive local file operations."""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable
from dataclasses import dataclass

from .errors import UndatumError, ValidationError


def file_digest(path: str, check: Callable[[], None] | None = None) -> str:
    """Hash a file without retaining its contents in memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            if check:
                check()
            digest.update(block)
    return digest.hexdigest()


def _generation(state: os.stat_result) -> tuple[int, ...]:
    return (
        state.st_dev,
        state.st_ino,
        state.st_size,
        state.st_mtime_ns,
        state.st_ctime_ns,
        state.st_mode,
        state.st_nlink,
        state.st_uid,
        state.st_gid,
    )


def same_file(left: str, right: str) -> bool:
    """Recognize existing inode aliases and equivalent nonexistent paths."""
    try:
        return os.path.samefile(left, right)
    except (FileNotFoundError, OSError):
        return os.path.normcase(os.path.abspath(left)) == os.path.normcase(os.path.abspath(right))


def extended_attributes(path: str) -> dict[str, bytes]:
    """Read every extended attribute supported by the platform."""
    if not hasattr(os, "listxattr"):
        return {}
    get_attribute = vars(os)["getxattr"]
    return {name: get_attribute(path, name) for name in os.listxattr(path)}


def require_regular(path: str, *, inplace: bool = False) -> None:
    """Refuse special files and ambiguous in-place link replacement."""
    entry = os.lstat(path)
    state = os.stat(path)
    if not stat.S_ISREG(state.st_mode):
        raise ValidationError("Repack requires a regular file.", field="input")
    if inplace and (stat.S_ISLNK(entry.st_mode) or state.st_nlink > 1):
        raise ValidationError(
            "In-place repack of symbolic links or multiple hard links is unsupported; "
            "choose a distinct output.",
            field="output",
        )


@dataclass(frozen=True)
class FileSnapshot:
    """A stable file generation, content digest and supported filesystem metadata."""

    path: str
    entry: tuple[int, ...]
    state: tuple[int, ...]
    digest: str
    mode: int
    mtime_ns: int
    atime_ns: int
    xattrs: dict[str, bytes]

    @classmethod
    def capture(cls, path: str, check: Callable[[], None] | None = None) -> FileSnapshot:
        """Capture metadata and reject a file changed while hashing."""
        path = os.path.abspath(path)
        entry, state = os.lstat(path), os.stat(path)
        snapshot = cls(
            path,
            _generation(entry),
            _generation(state),
            file_digest(path, check),
            stat.S_IMODE(state.st_mode),
            state.st_mtime_ns,
            state.st_atime_ns,
            extended_attributes(path),
        )
        snapshot.require_unchanged(check_contents=False)
        return snapshot

    def require_unchanged(
        self,
        *,
        check_contents: bool = True,
        check: Callable[[], None] | None = None,
    ) -> None:
        """Refuse a changed source, candidate or existing destination."""
        try:
            unchanged = (
                _generation(os.lstat(self.path)) == self.entry
                and _generation(os.stat(self.path)) == self.state
                and (not check_contents or file_digest(self.path, check) == self.digest)
                and extended_attributes(self.path) == self.xattrs
            )
        except OSError as exc:
            raise FileConflictError(self.path) from exc
        if not unchanged:
            raise FileConflictError(self.path)

    def apply_metadata(self, target: str) -> None:
        """Restore and verify mode, mtime and supported extended attributes."""
        if hasattr(os, "listxattr"):
            current = extended_attributes(target)
            for name in current.keys() - self.xattrs.keys():
                # macOS can attach a protected creation-time provenance attribute.
                if name != "com.apple.provenance":
                    vars(os)["removexattr"](target, name)
            for name, value in self.xattrs.items():
                vars(os)["setxattr"](target, name, value)
        os.chmod(target, self.mode)
        os.utime(target, ns=(self.atime_ns, self.mtime_ns))
        state = os.stat(target)
        actual = extended_attributes(target)
        if "com.apple.provenance" not in self.xattrs:
            actual.pop("com.apple.provenance", None)
        if (
            stat.S_IMODE(state.st_mode) != self.mode
            or state.st_mtime_ns != self.mtime_ns
            or actual != self.xattrs
        ):
            raise OSError("Filesystem metadata could not be preserved; publication refused")


class FileConflictError(UndatumError):
    """A source, candidate or destination changed during an operation."""

    code = "file_conflict"

    def __init__(self, path: str):
        super().__init__(f"File changed before publication: '{path}'", {"file": path}, exit_code=3)
