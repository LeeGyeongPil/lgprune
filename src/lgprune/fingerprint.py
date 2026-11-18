"""Head state fingerprint.

For each namespace we hash what `get_state` would load from the head:
the head checkpoint's channel values and its pending writes. Values are
re-serialized with the checkpointer's serde so SQLite (values inside the
checkpoint) and Postgres (values in checkpoint_blobs) hash the same way.

Limit: the fingerprint is computed from stored values. A DeltaChannel value
that is rebuilt from ancestors is not covered - that is why DeltaChannel
threads are refused before any delete (see safety.py).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from typing import Any


def ns_fingerprint(
    serde: Any,
    channel_values: Mapping[str, Any],
    writes: Iterable[tuple[str, int, str, Any]],
) -> str:
    """sha256 over sorted channel values and sorted (task_id, idx, channel, value) writes."""
    h = hashlib.sha256()
    for channel in sorted(channel_values):
        typ, data = serde.dumps_typed(channel_values[channel])
        _feed(h, b"v", channel.encode(), typ.encode(), data)
    for task_id, idx, channel, value in sorted(writes, key=lambda w: (w[0], w[1])):
        typ, data = serde.dumps_typed(value)
        _feed(h, b"w", task_id.encode(), str(idx).encode(), channel.encode(), typ.encode(), data)
    return h.hexdigest()


def thread_fingerprint(ns_fingerprints: Mapping[str, str]) -> str:
    """One hash for the whole thread (all namespaces)."""
    h = hashlib.sha256()
    for ns in sorted(ns_fingerprints):
        _feed(h, ns.encode(), ns_fingerprints[ns].encode())
    return h.hexdigest()


def _feed(h: Any, *parts: bytes) -> None:
    # 길이를 앞에 붙여 경계가 모호해지지 않게 한다("ab"+"c" 와 "a"+"bc" 구분)
    for p in parts:
        h.update(len(p).to_bytes(8, "big"))
        h.update(p)
