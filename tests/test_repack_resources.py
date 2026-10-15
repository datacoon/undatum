"""Worker supervision proves abort, cleanup and actionable monitoring guarantees."""

import gzip
import json
import subprocess
import sys
import textwrap

import pytest

from undatum.cmds.repacker import Repacker
from undatum.common.repack_types import ResourceLimitError


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux RSS accounting")
def test_worker_peak_does_not_inherit_large_parent_rss():
    code = textwrap.dedent(
        """
        import json
        import resource
        import subprocess
        import sys

        allocation = bytearray(128 * 1024 * 1024)
        worker = subprocess.check_output([
            sys.executable, '-c',
            'from undatum.common.repack_worker import _peak_rss; print(_peak_rss())',
        ], text=True)
        print(json.dumps([resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024, int(worker)]))
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, timeout=30
    )
    parent_peak, worker_peak = json.loads(result.stdout)
    assert parent_peak >= 128 * 1024 * 1024
    assert 0 < worker_peak < 128 * 1024 * 1024


def test_hard_rss_limit_terminates_worker_and_keeps_source(tmp_path):
    pytest.importorskip("psutil")
    source = tmp_path / "data.csv.gz"
    before = gzip.compress(b"payload" * 1000, mtime=0)
    source.write_bytes(before)
    with pytest.raises(ResourceLimitError, match="RSS"):
        Repacker().repack(str(source), options={"memory_limit": 1, "progress": False})
    assert source.read_bytes() == before
    assert not list(tmp_path.glob(".undatum-tmp-*"))


def test_supervised_success_and_timeout(tmp_path):
    pytest.importorskip("psutil")
    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(gzip.compress(b"payload" * 1000, compresslevel=1, mtime=0))
    result = Repacker().repack(
        str(source),
        str(output),
        {
            "memory_limit": 512 * 1024 * 1024,
            "timeout": 10,
            "progress": False,
        },
    )
    assert result["verification"]["passed"]
    assert result["resources"]["memory_enforcement"] == "process-tree-rss"
    assert result["resources"]["peak_rss_bytes"] > 0
    assert gzip.decompress(output.read_bytes()) == b"payload" * 1000
    before = source.read_bytes()
    with pytest.raises(ResourceLimitError, match="timeout"):
        Repacker().repack(str(source), options={"timeout": 0.000001, "progress": False})
    assert source.read_bytes() == before
    assert not list(tmp_path.glob(".undatum-tmp-*"))


def test_worker_is_killed_on_parent_interrupt(tmp_path, monkeypatch):
    import undatum.common.repack_worker as worker

    source = tmp_path / "data.csv.gz"
    source.write_bytes(gzip.compress(b"payload" * 1000, mtime=0))
    before = source.read_bytes()
    processes = []
    real = worker.subprocess.Popen

    def capture(*args, **kwargs):
        process = real(*args, **kwargs)
        processes.append(process)
        return process

    def interrupt(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr(worker.subprocess, "Popen", capture)
    monkeypatch.setattr(worker.time, "sleep", interrupt)
    with pytest.raises(KeyboardInterrupt):
        Repacker().repack(str(source), options={"timeout": 10, "progress": False})
    assert processes and processes[0].poll() is not None
    assert source.read_bytes() == before
    assert not list(tmp_path.glob(".undatum-tmp-*"))


def test_cli_json_error_and_exit_code(tmp_path):
    source = tmp_path / "data.csv.gz"
    source.write_bytes(gzip.compress(b"payload" * 1000, mtime=0))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "undatum",
            "repack",
            str(source),
            "--json",
            "--max-decoded-bytes",
            "1",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert not result.stdout
    document = json.loads(result.stderr)
    assert document["error"]["code"] == "resource_limit"


def test_worker_decode_error_is_a_user_error(tmp_path):
    source, output = tmp_path / "data.csv.gz", tmp_path / "out.csv.gz"
    source.write_bytes(gzip.compress(b"data" * 1000, mtime=0)[:-1])
    before = source.read_bytes()
    output.write_bytes(b"prior")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "undatum",
            "repack",
            str(source),
            str(output),
            "--timeout",
            "10",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1 and not result.stdout
    assert json.loads(result.stderr)["error"]["code"] == "repack_failed"
    assert source.read_bytes() == before and output.read_bytes() == b"prior"


def test_cli_refuses_stdin_and_stdout(tmp_path):
    source = tmp_path / "data.csv.gz"
    source.write_bytes(gzip.compress(b"data", mtime=0))
    for paths in (["-"], [str(source), "-"]):
        result = subprocess.run(
            [sys.executable, "-m", "undatum", "repack", *paths, "--json"],
            input=b"",
            check=False,
            capture_output=True,
            cwd=tmp_path,
        )
        assert result.returncode == 1 and not result.stdout
        assert json.loads(result.stderr)["error"]["code"] == "validation_error"
    assert not (tmp_path / "-").exists()
