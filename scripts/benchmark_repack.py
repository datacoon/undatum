"""Benchmark verified repack size/time/RSS/scratch and read-back against optional baselines."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.metadata
import json
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from itertools import zip_longest
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@dataclass
class Case:
    source: Path
    kind: str
    logical_size: int


def generate(directory: Path, size_mib: int) -> list[Case]:
    """Create bounded-memory low/high-cardinality CSV/JSONL and typed native data."""
    import avro.schema
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyorc
    import zstandard as zstd
    from avro.datafile import DataFileWriter
    from avro.io import DatumWriter

    directory.mkdir(parents=True, exist_ok=True)
    cases = []
    wanted = size_mib * 1024 * 1024
    for name in ("csv-low", "csv-high", "jsonl"):
        randomizer = random.Random(1729)
        raw = directory / (name + (".jsonl" if name == "jsonl" else ".csv"))
        with raw.open("wb") as sink:
            if name != "jsonl":
                sink.write(b"n,word,amount,flag\n")
            index = 0
            while sink.tell() < wanted:
                word = (
                    randomizer.randbytes(32).hex() if name == "csv-high" else "repeated value " * 5
                )
                record = {
                    "n": index,
                    "word": word,
                    "amount": (index % 1000) / 100,
                    "flag": bool(index % 2),
                }
                line = (
                    json.dumps(record)
                    if name == "jsonl"
                    else f"{index},{word},{record['amount']},{record['flag']}"
                )
                sink.write((line + "\n").encode())
                index += 1
        source = raw.with_name(raw.name + ".gz")
        with (
            source.open("wb") as output,
            gzip.GzipFile(
                filename="", fileobj=output, mode="wb", compresslevel=1, mtime=0
            ) as encoder,
        ):
            with raw.open("rb") as reader:
                shutil.copyfileobj(reader, encoder, 1024 * 1024)
        cases.append(Case(source, "gzip", raw.stat().st_size))
        if name == "csv-high":
            zsource = raw.with_name("zstd-high.csv.zst")
            with zsource.open("wb") as output, raw.open("rb") as reader:
                with zstd.ZstdCompressor(level=1).stream_writer(output) as encoder:
                    shutil.copyfileobj(reader, encoder, 1024 * 1024)
            cases.append(Case(zsource, "zstd", raw.stat().st_size))
    for name, payload in (("tiny", b"a,b\n1,2\n"), ("empty", b"")):
        source = directory / f"{name}.csv.gz"
        source.write_bytes(gzip.compress(payload, mtime=0))
        cases.append(Case(source, "gzip", len(payload)))

    schema = pa.schema(
        [
            pa.field("n", pa.int64()),
            pa.field("word", pa.string()),
            pa.field("amount", pa.float64()),
            pa.field("flag", pa.bool_()),
        ],
        metadata={b"bench": b"typed values"},
    )
    avro_schema = avro.schema.parse(
        json.dumps(
            {
                "type": "record",
                "name": "Bench",
                "fields": [
                    {"name": "n", "type": "long"},
                    {"name": "word", "type": "string"},
                    {"name": "amount", "type": "double"},
                    {"name": "flag", "type": "boolean"},
                ],
            }
        )
    )
    parquet, orc, avro = (directory / f"typed.{kind}" for kind in ("parquet", "orc", "avro"))
    rows = max(1, wanted // 128)
    with (
        pq.ParquetWriter(parquet, schema, compression="snappy") as pq_writer,
        orc.open("wb") as orc_output,
        pyorc.Writer(
            orc_output, "struct<n:bigint,word:string,amount:double,flag:boolean>"
        ) as orc_writer,
        DataFileWriter(avro.open("wb"), DatumWriter(), avro_schema, codec="null") as avro_writer,
    ):
        avro_writer.set_meta("bench", b"typed values")
        for start in range(0, rows, 65536):
            records = [
                {
                    "n": i,
                    "word": f"category {i % 10}" * 5,
                    "amount": (i % 1000) / 100,
                    "flag": bool(i % 2),
                }
                for i in range(start, min(rows, start + 65536))
            ]
            pq_writer.write_table(pa.Table.from_pylist(records, schema=schema))
            orc_writer.writerows(tuple(record.values()) for record in records)
            for record in records:
                avro_writer.append(record)
    cases.extend(
        Case(path, kind, wanted)
        for path, kind in ((parquet, "parquet"), (orc, "orc"), (avro, "avro"))
    )
    return cases


def _measure(argv: list[str], cwd: Path) -> tuple[dict[str, Any], float, int]:
    import psutil

    peak, started = 0, time.monotonic()
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(argv, cwd=cwd, stdout=output, stderr=errors)
        try:
            while process.poll() is None:
                try:
                    parent = psutil.Process(process.pid)
                    peak = max(
                        peak,
                        sum(
                            child.memory_info().rss
                            for child in [parent, *parent.children(recursive=True)]
                        ),
                    )
                except psutil.NoSuchProcess:
                    pass
                time.sleep(0.01)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
        elapsed = time.monotonic() - started
        output.seek(0)
        text = output.read().decode("utf8")
        errors.seek(0)
        if process.returncode:
            return (
                {"error": errors.read().decode("utf8")[-3000:], "exit_code": process.returncode},
                elapsed,
                peak,
            )
        return json.loads(text), elapsed, peak


def verify(case: Case, output: Path) -> bool:
    """Verify comparable decoded preservation even for a baseline without a verifier."""
    from undatum.common.repack_types import RepackBudget, RepackOptions

    if not output.exists():
        return False
    if case.kind in {"gzip", "zstd"}:

        def checksum(path):
            from contextlib import ExitStack

            from undatum.common.repack_streams import open_reader

            digest = hashlib.sha256()
            with ExitStack() as stack:
                source = open_reader(
                    stack,
                    str(path),
                    "gz" if case.kind == "gzip" else "zst",
                    RepackBudget(RepackOptions(), ""),
                )
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(block)
            return digest.digest()

        return checksum(case.source) == checksum(output)
    if case.kind == "parquet":
        import pyarrow.parquet as pq

        from undatum.common.repack_columnar import _verify_parquet

        with pq.ParquetFile(case.source) as source, pq.ParquetFile(output) as candidate:
            try:
                _verify_parquet(
                    source, candidate, RepackOptions(), RepackBudget(RepackOptions(), "")
                )
            except Exception:
                return False
        return True
    if case.kind == "orc":
        import pyorc

        with case.source.open("rb") as a, output.open("rb") as b:
            left, right = pyorc.Reader(a), pyorc.Reader(b)
            return (
                str(left.schema) == str(right.schema)
                and left.user_metadata == right.user_metadata
                and all(x == y for x, y in zip_longest(left, right, fillvalue=object()))
            )
    from undatum.common.repack_avro import _blocks, read_header

    with case.source.open("rb") as a, output.open("rb") as b:
        left, sync_a = read_header(a)
        right, sync_b = read_header(b)
        codec_a, codec_b = (
            left.pop(b"avro.codec", b"null").decode(),
            right.pop(b"avro.codec", b"null").decode(),
        )
        if sync_a != sync_b or left != right:
            return False
        left_blocks = _blocks(a, codec_a, sync_a, RepackBudget(RepackOptions(), ""))
        right_blocks = _blocks(b, codec_b, sync_b, RepackBudget(RepackOptions(), ""))
        return all(x == y for x, y in zip_longest(left_blocks, right_blocks, fillvalue=object()))


def read_back(case: Case, output: Path) -> dict[str, Any]:
    """Measure candidate decode and downstream Parquet scan independently of encoding."""
    started = time.monotonic()
    if case.kind == "gzip":
        with gzip.open(output, "rb") as source:
            while source.read(1024 * 1024):
                pass
    elif case.kind == "zstd":
        import zstandard as zstd

        with output.open("rb") as raw, zstd.ZstdDecompressor().stream_reader(raw) as reader:
            while reader.read(1024 * 1024):
                pass
    elif case.kind == "parquet":
        import duckdb

        with duckdb.connect() as connection:
            connection.execute(
                "SELECT count(*), sum(n) FROM read_parquet(?)", [str(output)]
            ).fetchone()
    elif case.kind == "orc":
        import pyorc

        with output.open("rb") as source:
            sum(row[0] for row in pyorc.Reader(source))
    elif case.kind == "avro":
        from avro.datafile import DataFileReader
        from avro.io import DatumReader

        with DataFileReader(output.open("rb"), DatumReader()) as reader:
            sum(row["n"] for row in reader)
    else:
        return {}
    elapsed = time.monotonic() - started
    return {
        "decode_or_scan_seconds": elapsed,
        "read_back_mib_per_second": case.logical_size / 1024**2 / elapsed if elapsed else None,
    }


_BASELINE = """
import importlib.util,json,sys
spec=importlib.util.spec_from_file_location('undatum.cmds._repack_baseline',sys.argv[1])
module=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=module
spec.loader.exec_module(module)
result=module.Repacker().repack(sys.argv[2],sys.argv[3],{'progress':False})
print(json.dumps(result))
"""
_FILEREPACK = """
import json,sys
from filerepack import FileRepacker,RepackOptions
options=RepackOptions(keep_if_larger=False,compression_level=9,tool_threads=1,
 ultra=False,
 avro_codec='zstandard',format_max_decoded_bytes=32*1024**3,
 format_max_scratch_bytes=8*1024**3,format_max_memory_bytes=1024**3,
 format_max_nodes=10**9,format_timeout=120)
summary=FileRepacker().repack(sys.argv[1],outfile=sys.argv[2],options=options)
print(json.dumps(summary.outcome.to_dict()))
"""


def run(args: argparse.Namespace) -> dict[str, Any]:
    """Run cold subprocess trials with provenance and strictly separate baseline fidelity."""
    records: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="undatum-repack-bench-") as scratch:
        directory = Path(scratch)
        cases = generate(directory, args.size_mib)
        if args.only:
            selected = set(args.only.split(","))
            cases = [case for case in cases if case.source.name in selected]
            if not cases:
                raise ValueError("No benchmark cases match --only.")
        baseline = directory / "repacker_baseline.py"
        if args.baseline_ref:
            source = subprocess.check_output(
                ["git", "show", f"{args.baseline_ref}:undatum/cmds/repacker.py"], cwd=ROOT
            )
            baseline.write_bytes(source)
        for case in cases:
            implementations = [("undatum", profile) for profile in args.profiles]
            if args.baseline_ref:
                implementations.append(("baseline", "max"))
            if args.filerepack_python and args.filerepack_root:
                implementations.append(("filerepack", "max"))
            samples: dict[tuple[str, str], list[dict[str, Any]]] = {
                key: [] for key in implementations
            }
            for iteration in range(args.repeat):
                offset = iteration % len(implementations)
                ordered = implementations[offset:] + implementations[:offset]
                for implementation, profile in ordered:
                    trials = samples[(implementation, profile)]
                    output = directory / (f"output-{implementation}-{profile}-" + case.source.name)
                    if output.exists():
                        output.unlink()
                    if implementation == "undatum":
                        argv = [
                            args.python,
                            "-m",
                            "undatum",
                            "repack",
                            str(case.source),
                            str(output),
                            "--profile",
                            profile,
                            "--allow-grow",
                            "--batch-size",
                            str(args.batch_size),
                            "--row-group-size",
                            str(args.row_group_size),
                            "--json",
                        ]
                        cwd = ROOT
                    elif implementation == "baseline":
                        argv = [
                            args.python,
                            "-c",
                            _BASELINE,
                            str(baseline),
                            str(case.source),
                            str(output),
                        ]
                        cwd = ROOT
                    else:
                        argv = [
                            args.filerepack_python,
                            "-c",
                            _FILEREPACK,
                            str(case.source),
                            str(output),
                        ]
                        cwd = Path(args.filerepack_root)
                    result, elapsed, rss = _measure(argv, cwd)
                    try:
                        preserved = "error" not in result and verify(case, output)
                    except Exception as exc:
                        preserved = False
                        result["verification_error"] = str(exc)
                    trial: dict[str, Any] = {
                        "seconds": elapsed,
                        "peak_rss_bytes": rss,
                        "preservation_passed": preserved,
                        "output_size": output.stat().st_size if output.exists() else None,
                        "timings": result.get("timings", {}),
                        "resources": result.get(
                            "resources", result.get("details", {}).get("resources", {})
                        ),
                        "effective_settings": result.get("effective_settings", {}),
                        "status": result.get("status", "legacy"),
                        "reason": result.get("reason"),
                        "reason_code": result.get("reason_code"),
                    }
                    if "error" in result:
                        trial["error"] = result["error"]
                    if "verification_error" in result:
                        trial["verification_error"] = result["verification_error"]
                    if preserved:
                        trial.update(read_back(case, output))
                    trials.append(trial)
            for implementation, profile in implementations:
                trials = samples[(implementation, profile)]
                record = {
                    "case": case.source.name,
                    "kind": case.kind,
                    "implementation": implementation,
                    "profile": profile,
                    "input_size": case.source.stat().st_size,
                    "logical_size": case.logical_size,
                    "trials": trials,
                    "median_seconds": statistics.median(t["seconds"] for t in trials),
                    "peak_rss_bytes": max(t["peak_rss_bytes"] for t in trials),
                    "preservation_passed": all(t["preservation_passed"] for t in trials),
                }
                records.append(record)
                print(
                    f"{implementation} {profile} {case.source.name}: {record['median_seconds']:.3f}s, "
                    f"preservation={record['preservation_passed']}",
                    file=sys.stderr,
                )
    versions = {
        name: importlib.metadata.version(name)
        for name in ("iterabledata", "pyarrow", "duckdb", "pyorc", "avro")
    }
    filerepack = None
    if args.filerepack_python and args.filerepack_root:
        root = Path(args.filerepack_root)
        filerepack = {
            "commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=root, text=True
            ).strip(),
            "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root)),
            "source_sha256": {
                str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted((root / "filerepack").glob("*.py"))
            },
            "runtime": json.loads(
                subprocess.check_output(
                    [
                        args.filerepack_python,
                        "-c",
                        "import json,sys,importlib.metadata as m; "
                        "print(json.dumps({'python':sys.version,'versions':"
                        "{k:m.version(k) for k in ('pyarrow','avro','zstandard')}}))",
                    ],
                    cwd=root,
                    text=True,
                )
            ),
            "settings": "gzip level 9, zstd level 19 (ultra=False); one codec thread; "
            "growth allowed; native scratch/decoded/RSS limits relaxed",
        }
    return {
        "schema": "undatum.repack-benchmark/1",
        "environment": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "python": sys.version,
            "versions": versions,
            "commit": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "repacker_sha256": hashlib.sha256(
                (ROOT / "undatum/cmds/repacker.py").read_bytes()
            ).hexdigest(),
            "baseline_ref": args.baseline_ref,
            "baseline_commit": subprocess.check_output(
                ["git", "rev-parse", args.baseline_ref], cwd=ROOT, text=True
            ).strip()
            if args.baseline_ref
            else None,
            "filerepack_root": args.filerepack_root,
            "filerepack": filerepack,
            "source_sha256": {
                str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in [
                    Path(__file__).resolve(),
                    ROOT / "undatum/cmds/repacker.py",
                    *sorted((ROOT / "undatum/common").glob("repack*.py")),
                    ROOT / "undatum/common/file_snapshot.py",
                    ROOT / "undatum/common/writer.py",
                ]
            },
            "mode": "cold subprocess; rotating implementations per repeat; sizes include headers; all undatum candidates verified",
        },
        "size_mib": args.size_mib,
        "repeat": args.repeat,
        "layout": {"batch_size": args.batch_size, "row_group_size": args.row_group_size},
        "records": records,
    }


def check(results: dict[str, Any], budgets: dict[str, Any]) -> list[str]:
    """Gate measured undatum preservation and calibrated per-case performance budgets."""
    failures = []
    for record in results["records"]:
        if record["implementation"] != "undatum":
            continue
        key = f"{record['case']}:{record['profile']}"
        if not record["preservation_passed"]:
            failures.append(f"{key}: preservation failed")
        budget = budgets.get(key)
        if budget and (
            record["median_seconds"] > budget["seconds"]
            or record["peak_rss_bytes"] > budget["rss_bytes"]
        ):
            failures.append(f"{key}: performance budget exceeded")
        if budget:
            for trial in record["trials"]:
                size = trial.get("output_size")
                if size is not None and size > budget.get("output_bytes", float("inf")):
                    failures.append(f"{key}: output size budget exceeded")
                if trial.get("resources", {}).get("scratch_peak_bytes", 0) > budget.get(
                    "scratch_bytes", float("inf")
                ):
                    failures.append(f"{key}: scratch budget exceeded")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mib", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=65536)
    parser.add_argument("--row-group-size", type=int, default=65536)
    parser.add_argument("--only", help="Comma-separated case filenames, for layout experiments.")
    parser.add_argument(
        "--profiles",
        nargs="+",
        choices=("fast", "balanced", "max"),
        default=["fast", "balanced", "max"],
    )
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--baseline-ref")
    parser.add_argument("--filerepack-python")
    parser.add_argument("--filerepack-root")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--budgets", type=Path)
    parser.add_argument("--calibrate", type=Path)
    args = parser.parse_args(argv)
    if min(args.size_mib, args.repeat, args.batch_size, args.row_group_size) < 1:
        parser.error("size-mib, repeat and layout sizes must be positive")
    results = run(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n")
    if args.calibrate:
        budgets = {
            f"{r['case']}:{r['profile']}": {
                "seconds": round(max(3 * r["median_seconds"], r["median_seconds"] + 1), 2),
                "rss_bytes": int(1.3 * r["peak_rss_bytes"] + 20 * 1024**2),
                "output_bytes": int(1.03 * max(t["output_size"] for t in r["trials"]) + 16),
                "scratch_bytes": int(
                    1.2 * max(t["resources"]["scratch_peak_bytes"] for t in r["trials"]) + 4096
                ),
            }
            for r in results["records"]
            if r["implementation"] == "undatum" and r["preservation_passed"]
        }
        args.calibrate.write_text(json.dumps(budgets, indent=2) + "\n")
    failures = check(results, json.loads(args.budgets.read_text()) if args.budgets else {})
    for failure in failures:
        print(failure, file=sys.stderr)
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
