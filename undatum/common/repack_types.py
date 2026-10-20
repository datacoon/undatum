"""Typed repack configuration, effective plans and cooperative resource budgets."""

from __future__ import annotations

import math
import os
import time
from dataclasses import dataclass, field, fields
from typing import Any

from .errors import UndatumError, ValidationError


class RepackError(UndatumError):
    """An invalid or non-preserving data candidate."""

    code = "repack_failed"


class ResourceLimitError(UndatumError):
    """A repack operation exceeded an explicitly requested resource budget."""

    code = "resource_limit"


@dataclass(frozen=True)
class RepackOptions:
    """Validated options; levels are resolved separately per codec."""

    compression: str | None = None
    level: int | None = None
    profile: str = "max"
    progress: bool = True
    dry_run: bool = False
    allow_grow: bool = False
    min_savings: float | None = None
    max_decoded_bytes: int | None = None
    temp_limit: int | None = None
    memory_limit: int | None = None
    timeout: float | None = None
    batch_size: int = 65536
    row_group_size: int = 65536
    threads: int = 1
    sqlite_offline: bool = False

    @classmethod
    def from_dict(cls, values: dict[str, Any] | None) -> RepackOptions:
        """Validate supplied options before opening any output."""
        values = values or {}
        accepted = {f.name for f in fields(cls)}
        supplied = {k: v for k, v in values.items() if k in accepted and v is not None}
        result = cls(**supplied)
        if result.profile not in {"fast", "balanced", "max"}:
            raise ValidationError("Choose fast, balanced or max.", field="profile")
        for name in ("progress", "dry_run", "allow_grow", "sqlite_offline"):
            if type(getattr(result, name)) is not bool:
                raise ValidationError("Expected a boolean.", field=name)
        if result.level is not None and type(result.level) is not int:
            raise ValidationError("Expected an integer.", field="level")
        for name in (
            "max_decoded_bytes",
            "temp_limit",
            "memory_limit",
            "batch_size",
            "row_group_size",
            "threads",
        ):
            value = getattr(result, name)
            if value is not None and (type(value) is not int or value < 1):
                raise ValidationError("Expected a positive integer.", field=name)
        for name in ("min_savings", "timeout"):
            value = getattr(result, name)
            if value is not None and (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value < 0
                or (name == "timeout" and value == 0)
                or (name == "min_savings" and value > 100)
            ):
                raise ValidationError("Expected a finite valid number.", field=name)
        return result


@dataclass(frozen=True)
class RepackPlan:
    """A resolved, format-preserving encode operation."""

    source: str
    mode: str
    datatype: str | None
    source_codec: str | None
    target_codec: str
    level: int | None
    profile: str
    migration: bool


def native_sidecar_paths(plan: RepackPlan, target: str) -> tuple[str, ...]:
    """Paths owned by a database candidate, also monitored by its parent worker."""
    if plan.mode != "builtin":
        return ()
    suffixes = {"duckdb": (".wal",), "sqlite": ("-wal", "-shm", "-journal")}.get(
        plan.datatype or "", ()
    )
    return tuple(target + suffix for suffix in suffixes)


@dataclass
class RepackResult:
    """Publication outcome; candidate and final sizes are deliberately distinct."""

    input: str
    output: str
    mode: str
    input_size: int
    candidate_size: int
    output_size: int | None
    status: str
    reason_code: str
    published: bool
    changed: bool
    savings_bytes: int
    savings_percent: float | None
    candidate_savings_bytes: int
    candidate_savings_percent: float | None
    would_publish: bool
    elapsed_seconds: float
    effective_settings: dict[str, Any]
    verification: dict[str, Any]
    resources: dict[str, Any]
    rows: int | None = None
    decoded_bytes: int = 0
    timings: dict[str, float] = field(default_factory=dict)


@dataclass
class RepackBudget:
    """Cooperative cumulative decoded-byte, stage-size and elapsed-time accounting."""

    options: RepackOptions
    target: str
    started: float = field(default_factory=time.monotonic)
    decoded: int = 0
    scratch_peak: int = 0
    scratch_paths: tuple[str, ...] = ()

    def check(self, decoded: int = 0) -> None:
        """Check operation budgets at a bounded transfer or batch boundary."""
        self.decoded += decoded
        if (
            self.options.max_decoded_bytes is not None
            and self.decoded > self.options.max_decoded_bytes
        ):
            raise ResourceLimitError("Decoded-byte budget exceeded.")
        scratch = 0
        for path in (self.target, *self.scratch_paths):
            try:
                scratch += os.path.getsize(path)
            except FileNotFoundError:
                pass  # A native checkpoint can remove its WAL between samples.
        self.scratch_peak = max(self.scratch_peak, scratch)
        if self.options.temp_limit is not None and self.scratch_peak > self.options.temp_limit:
            raise ResourceLimitError("Temporary-file budget exceeded.")
        if (
            self.options.timeout is not None
            and time.monotonic() - self.started > self.options.timeout
        ):
            raise ResourceLimitError("Repack timeout exceeded.")
