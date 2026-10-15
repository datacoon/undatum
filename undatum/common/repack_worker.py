"""Isolated native encoding with process-tree RSS and wall-time supervision."""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
import tempfile
import time
import zlib
from dataclasses import asdict
from typing import Any

from .errors import DependencyError, UndatumError
from .repack_types import (
    RepackBudget,
    RepackError,
    RepackOptions,
    RepackPlan,
    ResourceLimitError,
    native_sidecar_paths,
)


def require_monitor(options: RepackOptions) -> None:
    """Require optional monitoring only when a hard native RSS limit was requested."""
    if options.memory_limit is not None:
        try:
            import psutil  # noqa: F401
        except ImportError as exc:
            raise DependencyError(
                "psutil",
                feature="repack native RSS limit",
                install_command='pip install "undatum[repack]"',
            ) from exc


def _peak_rss() -> int | None:
    """Read the OS high-water RSS to catch allocations between monitor samples."""
    if sys.platform.startswith("linux"):
        # ru_maxrss can retain the parent's pre-exec peak. VmHWM belongs to
        # this executable's address space, so a large caller cannot cause a
        # small isolated worker to fail its own memory budget.
        try:
            with open("/proc/self/status", encoding="ascii") as status:
                for line in status:
                    if line.startswith("VmHWM:"):
                        return int(line.split()[1]) * 1024
        except OSError:
            pass
        return None
    if sys.platform == "win32":
        try:
            import psutil

            return int(psutil.Process().memory_info().peak_wset)
        except ImportError:
            return None
    import resource

    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(usage if sys.platform == "darwin" else usage * 1024)


def supervised_repack(
    plan: RepackPlan, target: str, options: RepackOptions, started: float
) -> dict[str, Any]:
    """Run only encode/verify in a worker; the parent alone can publish output."""
    peak = 0
    budget = RepackBudget(
        options,
        target,
        started=started,
        scratch_paths=native_sidecar_paths(plan, target),
    )
    with tempfile.TemporaryFile(mode="w+b") as response:
        process = subprocess.Popen(
            [sys.executable, "-m", "undatum.common.repack_worker"],
            stdin=subprocess.PIPE,
            stdout=response,
            stderr=None,
            cwd=os.getcwd(),
        )
        try:
            assert process.stdin is not None
            request = {
                "plan": asdict(plan),
                "target": target,
                "options": {**asdict(options), "progress": False},
            }
            process.stdin.write(json.dumps(request).encode("utf8"))
            process.stdin.close()
            while process.poll() is None:
                budget.check()
                if options.memory_limit is not None:
                    import psutil

                    try:
                        parent = psutil.Process(process.pid)
                        rss = sum(
                            child.memory_info().rss
                            for child in [parent, *parent.children(recursive=True)]
                        )
                        peak = max(peak, rss)
                    except psutil.NoSuchProcess:
                        rss = 0
                    if rss > options.memory_limit:
                        raise ResourceLimitError(
                            "Native process RSS budget exceeded (worker terminated)."
                        )
                time.sleep(0.02)
            response.seek(0)
            raw = response.read(1024 * 1024)
            if not raw:
                raise UndatumError(
                    f"Repack worker exited without a result ({process.returncode}).", exit_code=3
                )
            document = json.loads(raw)
            if "error" in document:
                error = document["error"]
                exception = UndatumError(error["message"], error.get("context"), error["exit_code"])
                exception.code = error["code"]
                raise exception
            if process.returncode:
                raise UndatumError(f"Repack worker failed ({process.returncode}).", exit_code=3)
            report: dict[str, Any] = document["result"]
            report["resources"]["scratch_peak_bytes"] = max(
                report["resources"]["scratch_peak_bytes"], budget.scratch_peak
            )
            peak = max(peak, report["resources"].get("worker_peak_rss_bytes") or 0)
            if options.memory_limit is not None and peak > options.memory_limit:
                raise ResourceLimitError("Native process peak RSS exceeded the budget.")
            report["resources"].update(
                memory_enforcement="process-tree-rss"
                if options.memory_limit is not None
                else "none",
                peak_rss_bytes=peak if options.memory_limit is not None else None,
                timeout_enforcement="worker" if options.timeout is not None else "none",
            )
            return report
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            if process.stdin and not process.stdin.closed:
                process.stdin.close()


def main() -> int:
    """Consume a bounded internal JSON request and return result/error without publication."""
    from ..cmds.repacker import perform_repack
    from .errors import exit_code_for

    try:
        request = json.loads(sys.stdin.buffer.read(1024 * 1024))
        plan = RepackPlan(**request["plan"])
        options = RepackOptions.from_dict(request["options"])
        result = perform_repack(plan, request["target"], options)
        result["resources"]["worker_peak_rss_bytes"] = _peak_rss()
        print(json.dumps({"result": result}))
        return 0
    except Exception as exc:
        if not isinstance(exc, UndatumError):
            if isinstance(exc, ImportError):
                exc = DependencyError(
                    exc.name or "compression",
                    feature="repack",
                    install_command='pip install "undatum[compression]"',
                )
            elif isinstance(exc, (EOFError, ValueError, gzip.BadGzipFile, zlib.error)) or type(
                exc
            ).__module__.startswith(("avro", "pyarrow", "pyorc", "zstandard", "lz4", "_lzma")):
                exc = RepackError(f"Repack failed: {exc}")
            elif isinstance(exc, OSError):
                exc = UndatumError(f"Repack file operation failed: {exc}", exit_code=3)
        print(
            json.dumps(
                {
                    "error": {
                        "message": str(exc),
                        "code": getattr(exc, "code", "repack_failed"),
                        "exit_code": exit_code_for(exc),
                        "context": getattr(exc, "context", {}),
                    }
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
