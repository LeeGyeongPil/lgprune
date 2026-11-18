"""Data classes shared by the CLI and the Python API, with JSON conversion.

JSON keys and reason codes are fixed English identifiers: they do not change
with the screen language, so scripts can rely on them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from .errors import PlanFormatError

PLAN_FORMAT = "lgprune-plan"
PLAN_VERSION = 1

# 스레드에 붙는 표지(flags). 보고서·계획 모두 이 이름을 그대로 쓴다.
FLAG_INTERRUPTED = "interrupted"  # head 에 __interrupt__ 쓰기가 있음(사람 승인 대기)
FLAG_DELTA = "delta"  # DeltaChannel 사용 — keep_latest 하면 값이 조용히 사라진다
FLAG_SUBGRAPHS = "subgraphs"  # checkpoint_ns 가 2개 이상
FLAG_LEGACY = "legacy"  # head 체크포인트 형식 v < 4
FLAG_UNDECODABLE = "undecodable"  # 직렬화 해제 실패(암호화 serde 등)

ACTIONS = ("keep_latest", "delete", "skip")


@dataclass
class ThreadStats:
    thread_id: str
    namespaces: int
    checkpoints: int
    writes: int
    blobs: int
    bytes: int
    latest_bytes: int
    history_bytes: int
    top_channel: str | None
    channel_bytes: dict[str, int]
    last_activity: datetime | None
    idle_days: float | None
    flags: frozenset[str]
    heads: dict[str, str]
    table_bytes: dict[str, int]
    trim_rows: dict[str, int]
    trim_bytes: int

    def to_json(self) -> dict[str, Any]:
        return {
            "thread_id": self.thread_id,
            "namespaces": self.namespaces,
            "checkpoints": self.checkpoints,
            "writes": self.writes,
            "blobs": self.blobs,
            "bytes": self.bytes,
            "latest_bytes": self.latest_bytes,
            "history_bytes": self.history_bytes,
            "top_channel": self.top_channel,
            "channel_bytes": dict(sorted(self.channel_bytes.items(), key=lambda kv: -kv[1])),
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "idle_days": None if self.idle_days is None else round(self.idle_days, 3),
            "flags": sorted(self.flags),
            "heads": dict(self.heads),
            "table_bytes": dict(self.table_bytes),
        }


@dataclass
class Report:
    backend: str
    db: str
    generated_at: datetime
    threads: list[ThreadStats]
    file_bytes: int | None = None  # SQLite: main file + WAL
    official_prune: bool = False

    def thread(self, thread_id: str) -> ThreadStats:
        for t in self.threads:
            if t.thread_id == thread_id:
                return t
        raise KeyError(thread_id)

    def to_json(self) -> dict[str, Any]:
        totals = {
            "threads": len(self.threads),
            "checkpoints": sum(t.checkpoints for t in self.threads),
            "writes": sum(t.writes for t in self.threads),
            "blobs": sum(t.blobs for t in self.threads),
            "bytes": sum(t.bytes for t in self.threads),
            "history_bytes": sum(t.history_bytes for t in self.threads),
        }
        return {
            "backend": self.backend,
            "db": self.db,
            "generated_at": self.generated_at.isoformat(),
            "file_bytes": self.file_bytes,
            "official_prune": self.official_prune,
            "totals": totals,
            "threads": [t.to_json() for t in self.threads],
        }


@dataclass
class PlanItem:
    thread_id: str
    action: str  # keep_latest | delete | skip
    reason: str  # 이유 코드(기계용): idle / explicit / interrupted / delta_channel / ...
    rows: dict[str, int] = field(default_factory=dict)
    bytes: int = 0
    heads: dict[str, str] = field(default_factory=dict)
    fingerprint: str | None = None
    ns_fingerprints: dict[str, str] = field(default_factory=dict)
    flags: list[str] = field(default_factory=list)
    idle_days: float | None = None

    @property
    def total_rows(self) -> int:
        return sum(self.rows.values())

    def to_json(self) -> dict[str, Any]:
        return {
            "thread_id": self.thread_id,
            "action": self.action,
            "reason": self.reason,
            "rows": dict(self.rows),
            "bytes": self.bytes,
            "heads": dict(self.heads),
            "fingerprint": self.fingerprint,
            "ns_fingerprints": dict(self.ns_fingerprints),
            "flags": list(self.flags),
            "idle_days": None if self.idle_days is None else round(self.idle_days, 3),
        }

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> PlanItem:
        try:
            action = d["action"]
            if action not in ACTIONS:
                raise PlanFormatError(f"unknown action {action!r}", key="err.plan_format", detail=f"action {action!r}")
            return cls(
                thread_id=str(d["thread_id"]),
                action=action,
                reason=str(d.get("reason", "")),
                rows={str(k): int(v) for k, v in (d.get("rows") or {}).items()},
                bytes=int(d.get("bytes") or 0),
                heads={str(k): str(v) for k, v in (d.get("heads") or {}).items()},
                fingerprint=d.get("fingerprint"),
                ns_fingerprints={str(k): str(v) for k, v in (d.get("ns_fingerprints") or {}).items()},
                flags=[str(f) for f in d.get("flags") or []],
                idle_days=d.get("idle_days"),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise PlanFormatError(f"bad plan item: {e!r}", key="err.plan_format", detail=repr(e)) from e


@dataclass
class Plan:
    backend: str
    db: str
    created_at: datetime
    now: datetime
    policy: dict[str, Any]
    items: list[PlanItem]

    def item(self, thread_id: str) -> PlanItem:
        for i in self.items:
            if i.thread_id == thread_id:
                return i
        raise KeyError(thread_id)

    @property
    def actionable(self) -> list[PlanItem]:
        return [i for i in self.items if i.action != "skip"]

    def totals(self) -> dict[str, int]:
        out = {"threads": len(self.actionable), "checkpoints": 0, "writes": 0, "blobs": 0, "bytes": 0}
        for i in self.actionable:
            for k, v in i.rows.items():
                out[k] = out.get(k, 0) + v
            out["bytes"] += i.bytes
        return out

    def to_json(self) -> dict[str, Any]:
        return {
            "format": PLAN_FORMAT,
            "version": PLAN_VERSION,
            "backend": self.backend,
            "db": self.db,
            "created_at": self.created_at.isoformat(),
            "now": self.now.isoformat(),
            "policy": self.policy,
            "totals": self.totals(),
            "items": [i.to_json() for i in self.items],
        }

    def dump(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_json(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    @classmethod
    def from_json(cls, d: Any) -> Plan:
        if not isinstance(d, dict) or d.get("format") != PLAN_FORMAT:
            raise PlanFormatError("not an lgprune plan file", key="err.plan_format", detail="format")
        if d.get("version") != PLAN_VERSION:
            raise PlanFormatError(
                f"unsupported plan version {d.get('version')!r}", key="err.plan_format", detail="version"
            )
        try:
            return cls(
                backend=str(d["backend"]),
                db=str(d["db"]),
                created_at=datetime.fromisoformat(d["created_at"]),
                now=datetime.fromisoformat(d["now"]),
                policy=dict(d.get("policy") or {}),
                items=[PlanItem.from_json(i) for i in d["items"]],
            )
        except (KeyError, TypeError, ValueError) as e:
            raise PlanFormatError(f"bad plan file: {e!r}", key="err.plan_format", detail=repr(e)) from e

    @classmethod
    def load(cls, path: str | Path) -> Plan:
        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as e:
            raise PlanFormatError(f"cannot read plan {path}: {e}", key="err.plan_read", path=str(path)) from e
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise PlanFormatError(f"plan {path} is not JSON: {e}", key="err.plan_format", detail=str(e)) from e
        return cls.from_json(data)


@dataclass
class ApplyResult:
    applied: list[str]
    skipped: dict[str, str]
    rows_deleted: int
    rows_by_table: dict[str, int]
    verified: bool
    backup: str | None = None
    engine: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "applied": list(self.applied),
            "skipped": dict(self.skipped),
            "rows_deleted": self.rows_deleted,
            "rows_by_table": dict(self.rows_by_table),
            "verified": self.verified,
            "backup": self.backup,
            "engine": self.engine,
        }


@dataclass
class VerifyResult:
    ok: bool
    checked: list[str]
    mismatched: dict[str, str]

    def to_json(self) -> dict[str, Any]:
        return {"ok": self.ok, "checked": list(self.checked), "mismatched": dict(self.mismatched)}


@dataclass
class ReclaimResult:
    backend: str
    executed: bool
    before_bytes: int | None
    after_bytes: int | None
    reclaimable_bytes: int | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "executed": self.executed,
            "before_bytes": self.before_bytes,
            "after_bytes": self.after_bytes,
            "reclaimable_bytes": self.reclaimable_bytes,
            "detail": dict(self.detail),
        }
