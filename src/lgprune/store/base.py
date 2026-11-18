"""Pieces shared by the SQLite and Postgres stores.

`ThreadAccumulator` collects one thread's rows while a store streams them, then
turns them into a `ThreadStats`. Both backends feed it the same way:

    add_checkpoint(...)  in (namespace, checkpoint_id) ascending order
    add_write(...)       after all checkpoints of the thread
    add_blob(...)        Postgres only
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..models import (
    FLAG_INTERRUPTED,
    FLAG_LEGACY,
    FLAG_SUBGRAPHS,
    ThreadStats,
)
from ..safety import INTERRUPT_CHANNEL, uuid6_time


@dataclass
class _Namespace:
    head: str = ""
    head_bytes: int = 0
    head_v: int | None = None
    head_ts: datetime | None = None
    head_refs: dict[str, str] = field(default_factory=dict)  # Postgres: channel -> blob version
    ids: set[str] = field(default_factory=set)
    other_refs: set[tuple[str, str]] = field(default_factory=set)  # 이력 체크포인트가 가리키는 블롭


class ThreadAccumulator:
    def __init__(self, thread_id: str) -> None:
        self.thread_id = thread_id
        self.ns: dict[str, _Namespace] = {}
        self.checkpoints = 0
        self.checkpoint_bytes = 0
        self.writes = 0
        self.write_bytes = 0
        self.blobs = 0
        self.blob_bytes = 0
        self.head_write_bytes = 0
        self.head_blob_bytes = 0
        self.trim_writes = 0
        self.trim_blobs = 0
        self.channel_bytes: Counter[str] = Counter()
        self.flags: set[str] = set()

    # --- 체크포인트 ---------------------------------------------------------
    def add_checkpoint(
        self,
        ns: str,
        checkpoint_id: str,
        nbytes: int,
        *,
        v: int | None,
        ts: datetime | None,
        refs: dict[str, str] | None = None,
    ) -> None:
        n = self.ns.setdefault(ns, _Namespace())
        if n.head and refs is not None:
            # 이전 head 는 이제 이력이 된다 — 그 블롭 참조를 이력 쪽으로 옮긴다
            n.other_refs.update(n.head_refs.items())
        n.ids.add(checkpoint_id)
        # 체크포인트 ID 는 시간순 UUIDv6 이라 오름차순으로 읽으면 마지막 것이 head
        n.head = checkpoint_id
        n.head_bytes = nbytes
        n.head_v = v
        n.head_ts = ts
        n.head_refs = dict(refs or {})
        self.checkpoints += 1
        self.checkpoint_bytes += nbytes

    # --- 쓰기(pending writes) ------------------------------------------------
    def add_write(self, ns: str, checkpoint_id: str, channel: str, nbytes: int) -> None:
        self.writes += 1
        self.write_bytes += nbytes
        self.channel_bytes[channel] += nbytes
        n = self.ns.get(ns)
        if n is None:
            return  # 체크포인트가 아직 없는 쓰기(백그라운드 저장 중) — 지우지 않는다
        if checkpoint_id == n.head:
            self.head_write_bytes += nbytes
            if channel == INTERRUPT_CHANNEL:
                self.flags.add(FLAG_INTERRUPTED)
        elif checkpoint_id in n.ids:
            # keep_latest 는 "지워지는 체크포인트의 쓰기"만 지운다(공식 Postgres 와 같은 의미)
            self.trim_writes += 1

    # --- 블롭(Postgres) -------------------------------------------------------
    def add_blob(self, ns: str, channel: str, version: str, nbytes: int) -> None:
        self.blobs += 1
        self.blob_bytes += nbytes
        self.channel_bytes[channel] += nbytes
        n = self.ns.get(ns)
        if n is None:
            return
        if n.head_refs.get(channel) == version:
            self.head_blob_bytes += nbytes
        elif (channel, version) in n.other_refs:
            self.trim_blobs += 1

    def add_channel_bytes(self, channel: str, nbytes: int) -> None:
        self.channel_bytes[channel] += nbytes

    # --- 마무리 ---------------------------------------------------------------
    def finish(self, now: datetime) -> ThreadStats:
        flags = set(self.flags)
        if len(self.ns) > 1:
            flags.add(FLAG_SUBGRAPHS)
        if any(n.head_v is not None and n.head_v < 4 for n in self.ns.values()):
            flags.add(FLAG_LEGACY)

        last: datetime | None = None
        for n in self.ns.values():
            t = n.head_ts or uuid6_time(n.head)
            if t and (last is None or t > last):
                last = t
        idle = None if last is None else max(0.0, (now - last).total_seconds() / 86400)

        total = self.checkpoint_bytes + self.write_bytes + self.blob_bytes
        latest = sum(n.head_bytes for n in self.ns.values()) + self.head_write_bytes + self.head_blob_bytes
        trim_rows = {
            "checkpoints": self.checkpoints - len(self.ns),
            "writes": self.trim_writes,
            "blobs": self.trim_blobs,
        }
        top = None
        if self.channel_bytes:
            top = max(self.channel_bytes.items(), key=lambda kv: (kv[1], kv[0]))[0]
        return ThreadStats(
            thread_id=self.thread_id,
            namespaces=len(self.ns),
            checkpoints=self.checkpoints,
            writes=self.writes,
            blobs=self.blobs,
            bytes=total,
            latest_bytes=latest,
            history_bytes=total - latest,
            top_channel=top,
            channel_bytes=dict(self.channel_bytes),
            last_activity=last,
            idle_days=idle,
            flags=frozenset(flags),
            heads={ns: n.head for ns, n in sorted(self.ns.items())},
            table_bytes={
                "checkpoints": self.checkpoint_bytes,
                "writes": self.write_bytes,
                "blobs": self.blob_bytes,
            },
            trim_rows=trim_rows,
            trim_bytes=total - latest,
        )


def as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
