"""Safety checks: which threads can be trimmed without losing state.

Everything here is fail-closed: when we cannot tell, the thread is skipped.

- DeltaChannel: the channel stores only a sentinel and rebuilds its value from
  ancestor checkpoints' writes. Deleting those ancestors (keep_latest) makes the
  value come back EMPTY, with no error. We detect it two ways:
  metadata markers on ANY checkpoint of the thread, or a `_DeltaSnapshot` value.
- interrupted: the head checkpoint has a pending `__interrupt__` write
  (a human approval is still waiting).
- legacy: head checkpoint format v < 4 reads pending sends from its parent.
- undecodable: we could not deserialize a checkpoint (custom/encrypted serde).
- stale: the thread got new checkpoints after the plan was made.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .models import FLAG_DELTA, FLAG_INTERRUPTED, FLAG_LEGACY, FLAG_UNDECODABLE

try:  # 베타 기능이라 위치가 바뀌면 값 타입 판별만 빠지고 메타데이터 판별은 남는다
    from langgraph.checkpoint.serde.types import _DeltaSnapshot
except ImportError:  # pragma: no cover - depends on upstream layout
    _DeltaSnapshot = None  # type: ignore[assignment,misc]

INTERRUPT_CHANNEL = "__interrupt__"
TASKS_CHANNEL = "__pregel_tasks"

# DeltaChannel 이 체크포인트 메타데이터에 남기는 표지(Python / LangGraph.js)
DELTA_METADATA_KEYS = ("counters_since_delta_snapshot", "delta_writes_versioned")

# 이유 코드(기계용, JSON 에 그대로 남는다)
REASON_DELTA = "delta_channel"
REASON_INTERRUPTED = "interrupted"
REASON_LEGACY = "legacy"
REASON_UNDECODABLE = "undecodable"
REASON_STALE = "stale"
REASON_MISSING = "missing"

# UUIDv6 의 시각은 1582-10-15 부터 100ns 단위
_GREGORIAN_OFFSET_100NS = 0x01B21DD213814000


def has_official_prune(saver_cls: type) -> bool:
    """True when the installed checkpointer overrides `BaseCheckpointSaver.prune`."""
    from langgraph.checkpoint.base import BaseCheckpointSaver

    return getattr(saver_cls, "prune", None) is not BaseCheckpointSaver.prune


def metadata_has_delta(metadata: Mapping[str, Any] | None) -> bool:
    if not isinstance(metadata, Mapping):
        return False
    return any(k in metadata for k in DELTA_METADATA_KEYS)


def value_is_delta_snapshot(value: Any) -> bool:
    return _DeltaSnapshot is not None and isinstance(value, _DeltaSnapshot)


def keep_latest_blocker(flags: frozenset[str] | set[str]) -> str | None:
    """Reason code that forbids keep_latest for a thread, or None if it is safe."""
    if FLAG_UNDECODABLE in flags:
        return REASON_UNDECODABLE
    if FLAG_DELTA in flags:
        return REASON_DELTA
    if FLAG_LEGACY in flags:
        return REASON_LEGACY
    return None


def is_interrupted(flags: frozenset[str] | set[str]) -> bool:
    return FLAG_INTERRUPTED in flags


def uuid6_time(checkpoint_id: str) -> datetime | None:
    """Creation time encoded in a LangGraph checkpoint ID (UUIDv6), or None."""
    try:
        h = checkpoint_id.replace("-", "")
        if len(h) != 32 or h[12] != "6":
            return None
        ticks = int(h[0:12] + h[13:16], 16)
        seconds = (ticks - _GREGORIAN_OFFSET_100NS) / 10_000_000
        return datetime.fromtimestamp(seconds, tz=UTC)
    except (ValueError, OverflowError, OSError):
        return None


def parse_ts(value: Any) -> datetime | None:
    """Checkpoint `ts` (ISO 8601) -> aware datetime."""
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
