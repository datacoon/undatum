"""Regression tests for release packaging files."""

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = REPO_ROOT / "packaging" / "undatum.spec"


def test_pyinstaller_spec_exists():
    assert SPEC_PATH.is_file()
    text = SPEC_PATH.read_text(encoding="utf-8")
    assert "undatum" in text
    assert "__main__.py" in text


def test_pyinstaller_spec_is_not_gitignored():
    tracked = subprocess.run(
        ["git", "ls-files", "--", "packaging/undatum.spec"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    if tracked.stdout.strip():
        return

    ignored = subprocess.run(
        [
            "git",
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--",
            "packaging/undatum.spec",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert not ignored.stdout.strip(), (
        "packaging/undatum.spec is gitignored; "
        "the release workflow cannot find it on tagged builds"
    )
