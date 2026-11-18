"""SQLite store (`langgraph-checkpoint-sqlite` schema: `checkpoints` + `writes`).

SQLite has no blob table: channel values live inside each checkpoint's msgpack
row. There is no official `prune` for SqliteSaver (3.1.3 raises
NotImplementedError), so the keep_latest engine here follows the official
Postgres semantics:

    per namespace keep the head (max checkpoint_id) and its pending writes,
    delete the writes of the deleted checkpoints, delete the other checkpoints,
    set the head's parent to NULL when the parent was deleted.

Reads use a read-only connection (`mode=ro`), so `report` and `plan` never
change the file. Writes happen on a separate connection inside
`BEGIN IMMEDIATE`, which takes the write lock up front.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from ..errors import LgpruneError, UnsupportedBackendError
from ..fingerprint import ns_fingerprint
from ..models import FLAG_DELTA, FLAG_UNDECODABLE, Report, ThreadStats
from ..safety import has_official_prune, metadata_has_delta, parse_ts, value_is_delta_snapshot
from .base import ThreadAccumulator, as_int

T = TypeVar("T")

REQUIRED_COLUMNS = {
    "checkpoints": {"thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "type", "checkpoint", "metadata"},
    "writes": {"thread_id", "checkpoint_ns", "checkpoint_id", "task_id", "idx", "channel", "type", "value"},
}


class SqliteStore:
    backend = "sqlite"

    def __init__(self, path: str | os.PathLike[str], *, busy_timeout: float = 5.0) -> None:
        self.path = Path(path).expanduser().resolve()
        self.busy_timeout = busy_timeout
        self.serde = JsonPlusSerializer()
        self._ro: sqlite3.Connection | None = None
        self._rw: sqlite3.Connection | None = None
        if not self.path.is_file():
            raise LgpruneError(f"database file not found: {self.path}", key="err.no_file", path=str(self.path))
        try:
            self._check_schema(self._read_conn())
        except BaseException:
            self.close()  # 실패해도 연결을 남기지 않는다
            raise

    # --- 연결 ------------------------------------------------------------------
    @property
    def db(self) -> str:
        """Identity written into plans."""
        return str(self.path)

    @property
    def official_prune(self) -> bool:
        from langgraph.checkpoint.sqlite import SqliteSaver

        return has_official_prune(SqliteSaver)

    def reader(self) -> sqlite3.Connection:
        """Connection for read-only queries (report, plan, verify)."""
        return self._read_conn()

    def _read_conn(self) -> sqlite3.Connection:
        if self._ro is None:
            try:
                # 읽기 전용으로 열어 report·plan 이 파일·WAL 을 절대 바꾸지 않게 한다
                self._ro = sqlite3.connect(
                    f"{self.path.as_uri()}?mode=ro", uri=True, timeout=self.busy_timeout, check_same_thread=False
                )
            except sqlite3.Error as e:
                raise LgpruneError(f"cannot open {self.path}: {e}", key="err.open", path=str(self.path), detail=str(e)) from e
        return self._ro

    def _write_conn(self) -> sqlite3.Connection:
        if self._rw is None:
            try:
                self._rw = sqlite3.connect(
                    str(self.path), timeout=self.busy_timeout, isolation_level=None, check_same_thread=False
                )
            except sqlite3.Error as e:
                raise LgpruneError(f"cannot open {self.path}: {e}", key="err.open", path=str(self.path), detail=str(e)) from e
        return self._rw

    def write_conn(self) -> sqlite3.Connection:
        """Read-write connection in autocommit mode (we issue BEGIN/COMMIT ourselves)."""
        return self._write_conn()

    def close_reader(self) -> None:
        if self._ro is not None:
            self._ro.close()
            self._ro = None

    def close(self) -> None:
        for conn in (self._ro, self._rw):
            if conn is not None:
                conn.close()
        self._ro = self._rw = None

    def __enter__(self) -> SqliteStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _check_schema(self, conn: sqlite3.Connection) -> None:
        try:
            for table, cols in REQUIRED_COLUMNS.items():
                have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
                if not cols <= have:
                    raise UnsupportedBackendError(
                        f"{self.path} has no LangGraph '{table}' table with the expected columns",
                        key="err.schema",
                        path=str(self.path),
                        table=table,
                    )
        except sqlite3.DatabaseError as e:  # SQLite 파일이 아님
            raise UnsupportedBackendError(
                f"{self.path} is not a SQLite database: {e}", key="err.not_sqlite", path=str(self.path)
            ) from e

    def file_bytes(self) -> int:
        wal = Path(str(self.path) + "-wal")
        return self.path.stat().st_size + (wal.stat().st_size if wal.exists() else 0)

    # --- 읽기: 보고서 --------------------------------------------------------------
    def report(self, *, now: datetime | None = None) -> Report:
        now = now or datetime.now(UTC)
        threads = self.scan(self._read_conn(), now=now)
        threads.sort(key=lambda t: -t.bytes)
        return Report(
            backend=self.backend,
            db=self.db,
            generated_at=now,
            threads=threads,
            file_bytes=self.file_bytes(),
            official_prune=self.official_prune,
        )

    def scan(self, conn: Any, *, now: datetime, thread_id: str | None = None) -> list[ThreadStats]:
        """Stream every checkpoint and write once and aggregate per thread."""
        where, args = ("WHERE thread_id = ?", (thread_id,)) if thread_id is not None else ("", ())
        accs: dict[str, ThreadAccumulator] = {}
        rows = conn.execute(
            "SELECT thread_id, checkpoint_ns, checkpoint_id, type, checkpoint, metadata "
            f"FROM checkpoints {where} ORDER BY thread_id, checkpoint_ns, checkpoint_id",
            args,
        )
        for tid, ns, cid, typ, blob, meta in rows:
            acc = accs.get(tid) or accs.setdefault(tid, ThreadAccumulator(tid))
            nbytes = len(blob or b"") + len(meta or b"")
            v = ts = None
            try:
                # 메타데이터 표지: 어느 체크포인트에라도 있으면 DeltaChannel 스레드
                if metadata_has_delta(_load_metadata(meta)):
                    acc.flags.add(FLAG_DELTA)
            except (ValueError, UnicodeDecodeError):
                acc.flags.add(FLAG_UNDECODABLE)  # 표지를 확인할 수 없으면 판별 불가로 본다
            try:
                ck = self.serde.loads_typed((typ, blob))
                v, ts = as_int(ck.get("v")), parse_ts(ck.get("ts"))
                for ch, value in (ck.get("channel_values") or {}).items():
                    if value_is_delta_snapshot(value):
                        acc.flags.add(FLAG_DELTA)
                    acc.add_channel_bytes(ch, len(self.serde.dumps_typed(value)[1]))
            except Exception:  # noqa: BLE001 - 어떤 이유든 못 풀면 fail-closed
                acc.flags.add(FLAG_UNDECODABLE)
            acc.add_checkpoint(ns, cid, nbytes, v=v, ts=ts)

        wrows = conn.execute(
            f"SELECT thread_id, checkpoint_ns, checkpoint_id, channel, length(value) FROM writes {where}", args
        )
        for tid, ns, cid, channel, nbytes in wrows:
            acc = accs.get(tid)
            if acc is None:
                acc = accs.setdefault(tid, ThreadAccumulator(tid))
            acc.add_write(ns, cid, channel, nbytes or 0)
        return [a.finish(now) for a in accs.values()]

    def thread_state(self, conn: Any, thread_id: str, *, now: datetime) -> ThreadStats | None:
        found = self.scan(conn, now=now, thread_id=thread_id)
        return found[0] if found else None

    def thread_ids(self, conn: Any = None) -> list[str]:
        conn = conn or self._read_conn()
        return [r[0] for r in conn.execute("SELECT DISTINCT thread_id FROM checkpoints ORDER BY thread_id")]

    # --- 지문 ---------------------------------------------------------------------
    def fingerprint(self, conn: Any, thread_id: str) -> dict[str, str]:
        """{namespace: fingerprint} of the head state. Raises if a head cannot be decoded.

        Uses plain SELECTs on the given connection (never SqliteSaver.cursor(),
        which commits) so it can run inside the apply transaction.
        """
        out: dict[str, str] = {}
        heads = conn.execute(
            "SELECT checkpoint_ns, max(checkpoint_id) FROM checkpoints WHERE thread_id = ? GROUP BY checkpoint_ns",
            (thread_id,),
        ).fetchall()
        for ns, head in heads:
            typ, blob = conn.execute(
                "SELECT type, checkpoint FROM checkpoints WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?",
                (thread_id, ns, head),
            ).fetchone()
            ck = self.serde.loads_typed((typ, blob))
            writes = [
                (task_id, idx, channel, self.serde.loads_typed((wtyp, value)))
                for task_id, idx, channel, wtyp, value in conn.execute(
                    "SELECT task_id, idx, channel, type, value FROM writes "
                    "WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?",
                    (thread_id, ns, head),
                )
            ]
            out[ns] = ns_fingerprint(self.serde, ck.get("channel_values") or {}, writes)
        return out

    def counts(self, conn: Any, thread_ids: list[str] | None = None) -> dict[str, int]:
        """Row counts per table (optionally for some threads)."""
        out = {}
        for table, key in (("checkpoints", "checkpoints"), ("writes", "writes")):
            if thread_ids is None:
                out[key] = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            else:
                marks = ",".join("?" * len(thread_ids))
                out[key] = conn.execute(
                    f"SELECT count(*) FROM {table} WHERE thread_id IN ({marks})", thread_ids
                ).fetchone()[0] if thread_ids else 0
        out["blobs"] = 0
        return out

    # --- 쓰기: 트랜잭션·엔진 ------------------------------------------------------
    def run_in_transaction(self, fn: Callable[[Any], T]) -> T:
        """Run fn(conn) inside BEGIN IMMEDIATE; commit on success, roll back on any error."""
        conn = self._write_conn()
        try:
            # IMMEDIATE: 시작하자마자 쓰기 잠금을 잡아, 읽고 판단한 뒤 다른 런이 끼어들지 못하게 한다
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as e:
            raise LgpruneError(f"database is busy: {e}", key="err.locked", detail=str(e)) from e
        try:
            result = fn(conn)
            conn.execute("COMMIT")
        except BaseException as e:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass  # SQLite 가 이미 되돌린 경우 — 원래 오류를 가리지 않는다
            if isinstance(e, sqlite3.OperationalError):
                raise LgpruneError(f"database error, rolled back: {e}", key="err.locked", detail=str(e)) from e
            raise
        return result

    def keep_latest(self, conn: Any, thread_ids: list[str]) -> dict[str, int]:
        """Keep each namespace's head checkpoint and its writes; delete the rest."""
        deleted = {"checkpoints": 0, "writes": 0, "blobs": 0}
        for tid in thread_ids:
            heads = conn.execute(
                "SELECT checkpoint_ns, max(checkpoint_id) FROM checkpoints WHERE thread_id = ? GROUP BY checkpoint_ns",
                (tid,),
            ).fetchall()
            for ns, head in heads:
                # 1) 지워질 체크포인트에 딸린 쓰기만 지운다(아직 체크포인트가 없는 쓰기는 남긴다)
                cur = conn.execute(
                    "DELETE FROM writes WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id IN ("
                    " SELECT checkpoint_id FROM checkpoints"
                    " WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id <> ?)",
                    (tid, ns, tid, ns, head),
                )
                deleted["writes"] += cur.rowcount
                # 2) head 가 아닌 체크포인트를 지운다
                cur = conn.execute(
                    "DELETE FROM checkpoints WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id <> ?",
                    (tid, ns, head),
                )
                deleted["checkpoints"] += cur.rowcount
                # 3) 끊긴 부모 포인터는 NULL — 남은 사슬이 없는 행을 가리키지 않게 한다
                conn.execute(
                    "UPDATE checkpoints SET parent_checkpoint_id = NULL "
                    "WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ? AND parent_checkpoint_id IS NOT NULL",
                    (tid, ns, head),
                )
        return deleted

    def delete_threads(self, conn: Any, thread_ids: list[str]) -> dict[str, int]:
        deleted = {"checkpoints": 0, "writes": 0, "blobs": 0}
        for tid in thread_ids:
            deleted["writes"] += conn.execute("DELETE FROM writes WHERE thread_id = ?", (tid,)).rowcount
            deleted["checkpoints"] += conn.execute("DELETE FROM checkpoints WHERE thread_id = ?", (tid,)).rowcount
        return deleted

    def ensure_engine(self, *, keep_latest: bool) -> str:
        return "lgprune-sqlite"

    # --- 백업·회수 -----------------------------------------------------------------
    def backup(self, target: str | os.PathLike[str]) -> str:
        dest = Path(target).expanduser().resolve()
        if dest.exists():
            raise LgpruneError(f"backup target already exists: {dest}", key="err.backup_exists", path=str(dest))
        if dest == self.path:
            raise LgpruneError("backup target is the database itself", key="err.backup_exists", path=str(dest))
        out = sqlite3.connect(str(dest))
        try:
            # SQLite 온라인 백업 API: 쓰는 중이어도 일관된 사본을 만든다
            self._write_conn().backup(out)
        finally:
            out.close()
        return str(dest)


def _load_metadata(meta: Any) -> dict[str, Any] | None:
    """Checkpoint metadata JSON -> dict. Raises ValueError when it is not readable."""
    if meta is None:
        return None
    data = json.loads(meta.decode() if isinstance(meta, bytes | bytearray) else meta)
    if not isinstance(data, dict):
        raise ValueError("metadata is not an object")
    return data
