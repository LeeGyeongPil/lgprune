"""Retention policy: which threads to trim (keep_latest) or delete.

TOML example (examples/policy.toml):

    trim_after = "1d"          # idle 1 day or more  -> keep only the latest checkpoint
    delete_after = "30d"       # idle 30 days or more -> delete the whole thread
    keep_recent = 20           # never touch the 20 most recently active threads
    exclude_interrupted = true # never touch threads waiting for a human (interrupt)
    include = ["*"]            # thread_id globs to consider
    exclude = ["prod-*"]       # thread_id globs to leave alone
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

from .errors import LgpruneError

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhdw])\s*$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(text: str | int | float | None) -> timedelta | None:
    """'90s' '30m' '12h' '1d' '2w' -> timedelta. None/'' -> None."""
    if text is None or text == "":
        return None
    if isinstance(text, int | float):
        return timedelta(days=float(text))
    m = _DURATION_RE.match(str(text))
    if not m:
        raise LgpruneError(f"bad duration {text!r} (use e.g. 12h, 1d, 30d)", key="err.duration", value=str(text))
    return timedelta(seconds=float(m.group(1)) * _UNITS[m.group(2)])


def format_duration(td: timedelta | None) -> str | None:
    if td is None:
        return None
    secs = int(td.total_seconds())
    if secs == 0:
        return "0d"
    for unit, size in (("w", 604800), ("d", 86400), ("h", 3600), ("m", 60)):
        if secs % size == 0 and secs >= size:
            return f"{secs // size}{unit}"
    return f"{secs}s"


@dataclass
class Policy:
    trim_after: timedelta | None = None
    delete_after: timedelta | None = None
    keep_recent: int = 10
    exclude_interrupted: bool = True
    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    threads: list[str] | None = None

    def __post_init__(self) -> None:
        if self.keep_recent < 0:
            raise LgpruneError("keep_recent must be >= 0", key="err.keep_recent")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Policy:
        known = {"trim_after", "delete_after", "keep_recent", "exclude_interrupted", "include", "exclude", "threads"}
        unknown = set(data) - known
        if unknown:
            raise LgpruneError(
                f"unknown policy keys: {', '.join(sorted(unknown))}", key="err.policy_keys", keys=", ".join(sorted(unknown))
            )
        return cls(
            trim_after=parse_duration(data.get("trim_after")),
            delete_after=parse_duration(data.get("delete_after")),
            keep_recent=int(data.get("keep_recent", 10)),
            exclude_interrupted=bool(data.get("exclude_interrupted", True)),
            include=[str(x) for x in data.get("include") or []],
            exclude=[str(x) for x in data.get("exclude") or []],
            threads=[str(x) for x in data["threads"]] if data.get("threads") else None,
        )

    @classmethod
    def from_toml(cls, path: str | Path) -> Policy:
        try:
            data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as e:
            raise LgpruneError(f"cannot read policy {path}: {e}", key="err.policy_read", path=str(path), detail=str(e)) from e
        return cls.from_dict(data.get("lgprune", data))

    def to_json(self) -> dict[str, Any]:
        return {
            "trim_after": format_duration(self.trim_after),
            "delete_after": format_duration(self.delete_after),
            "keep_recent": self.keep_recent,
            "exclude_interrupted": self.exclude_interrupted,
            "include": list(self.include),
            "exclude": list(self.exclude),
            "threads": None if self.threads is None else list(self.threads),
        }
