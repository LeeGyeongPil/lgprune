"""Postgres store (`langgraph-checkpoint-postgres` schema).

Tables: `checkpoints` (JSONB, small values inline), `checkpoint_blobs` (channel
values, referenced by `channel_versions` inside the checkpoint JSON - not by a
foreign key) and `checkpoint_writes`.

Deleting is delegated to the official `PostgresSaver.prune` (3.2.0+), which
knows the blob reference rules. lgprune adds target selection, the report,
the plan, the re-check inside the transaction and the fingerprint compare.

One connection is shared by our reader and `PostgresSaver(conn)`. Apply runs
in one outer REPEATABLE READ transaction; the official prune's own
`transaction()` becomes a savepoint inside it (its `SET TRANSACTION ISOLATION
LEVEL REPEATABLE READ` is accepted because the level is the same), so a
fingerprint mismatch can still roll everything back before commit.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar
from urllib.parse import urlsplit, urlunsplit

from ..errors import LgpruneError, UnsafePruneError, UnsupportedBackendError
from ..fingerprint import ns_fingerprint
from ..models import FLAG_DELTA, FLAG_UNDECODABLE, Report, ThreadStats
from ..safety import DELTA_METADATA_KEYS, has_official_prune, parse_ts
from .base import ThreadAccumulator, as_int

T = TypeVar("T")

TABLES = ("checkpoints", "checkpoint_blobs", "checkpoint_writes")
RETRIES = 3

# DeltaChannel 판별(공식 prune 과 같은 두 갈래): 메타데이터 표지, 또는
# channel_values 에 인라인 true 표지가 있고 그 뒤에 블롭이 있는 스냅샷
_DELTA_SNAPSHOT_SQL = """
SELECT DISTINCT c.thread_id
FROM checkpoints c
CROSS JOIN LATERAL jsonb_each(c.checkpoint -> 'channel_values') AS v
WHERE {where} v.value = 'true'::jsonb AND EXISTS (
    SELECT 1 FROM checkpoint_blobs b
    WHERE b.thread_id = c.thread_id AND b.checkpoint_ns = c.checkpoint_ns
    AND b.channel = v.key AND b.version = c.checkpoint -> 'channel_versions' ->> v.key
)
"""


_AUTH_MARKERS = ("password authentication failed", "no password supplied", "fe_sendauth")
_AUTH_SQLSTATES = ("28P01", "28000")


def is_auth_failure(error: BaseException) -> bool:
    """True when a psycopg error means the server wanted a (different) password."""
    if getattr(error, "sqlstate", None) in _AUTH_SQLSTATES:
        return True
    text = str(error).lower()
    return any(marker in text for marker in _AUTH_MARKERS)


def redact_url(url: str) -> str:
    """Drop the password so plans never contain secrets (libpq reads PGPASSWORD / .pgpass)."""
    parts = urlsplit(url)
    if parts.password is None:
        return url
    netloc = parts.hostname or ""
    if parts.username:
        netloc = f"{parts.username}@{netloc}"
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


class PostgresStore:
    backend = "postgres"

    def __init__(self, url: str, *, busy_timeout: float = 5.0) -> None:
        try:
            import psycopg
            from langgraph.checkpoint.postgres import PostgresSaver
            from psycopg.rows import dict_row
        except ImportError as e:
            raise UnsupportedBackendError(
                "Postgres support needs the extra: pip install 'lgprune[postgres]'", key="err.pg_extra"
            ) from e
        self._psycopg = psycopg
        self.url = url
        try:
            # PostgresSaver 가 요구하는 연결 설정(autocommit, dict_row).
            # prepare_threshold=None: 서버 쪽 prepared statement 를 만들지 않는다 — PgBouncer 트랜잭션 풀링과
            # PGlite(클라이언트 연결이 바뀌어도 세션 하나를 공유)에서 이름 충돌이 나지 않게 한다
            self.conn = psycopg.connect(
                url, autocommit=True, prepare_threshold=None, row_factory=dict_row, connect_timeout=15
            )
        except psycopg.Error as e:
            detail = str(e).strip().splitlines()[0]
            if (
                urlsplit(url).password is None
                and not os.environ.get("PGPASSWORD")
                and is_auth_failure(e)
            ):
                # 계획 파일은 비밀번호를 지운 URL 을 담는다 — 비밀번호를 어디서 받는지 알려 준다
                raise LgpruneError(
                    f"cannot connect to Postgres: {e} (the URL has no password: set PGPASSWORD, "
                    "use ~/.pgpass or pass --db URL)",
                    key="err.pg_connect_nopw",
                    detail=detail,
                ) from e
            raise LgpruneError(f"cannot connect to Postgres: {e}", key="err.pg_connect", detail=detail) from e
        try:
            # 잠금을 오래 기다리지 않게 — 기다리다 실패하면 아무것도 바꾸지 않고 종료 코드 1
            ms = max(1, int(busy_timeout * 1000))
            self.conn.execute(f"SET lock_timeout = {ms}")
            self.saver = PostgresSaver(self.conn)
            self.serde = self.saver.serde
            self._check_schema()
        except BaseException:
            self.conn.close()  # 실패해도 연결을 남기지 않는다(PGlite 처럼 연결이 1개뿐인 서버도 있다)
            raise

    @property
    def db(self) -> str:
        return redact_url(self.url)

    @property
    def official_prune(self) -> bool:
        return has_official_prune(type(self.saver))

    def reader(self) -> Any:
        """The one shared connection (PGlite and small servers allow a single connection)."""
        return self.conn

    def close(self) -> None:
        if not self.conn.closed:
            self.conn.close()

    def __enter__(self) -> PostgresStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _check_schema(self) -> None:
        rows = self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = ANY(%s)",
            (list(TABLES),),
        ).fetchall()
        missing = set(TABLES) - {r["table_name"] for r in rows}
        if missing:
            raise UnsupportedBackendError(
                f"no LangGraph checkpoint tables here (missing: {', '.join(sorted(missing))})",
                key="err.pg_schema",
                missing=", ".join(sorted(missing)),
            )

    # --- 읽기 ---------------------------------------------------------------------
    def report(self, *, now: datetime | None = None) -> Report:
        now = now or datetime.now(UTC)
        threads = self.scan(self.conn, now=now)
        threads.sort(key=lambda t: -t.bytes)
        return Report(self.backend, self.db, now, threads, None, self.official_prune)

    def scan(self, conn: Any, *, now: datetime, thread_id: str | None = None) -> list[ThreadStats]:
        where, args = ("WHERE thread_id = %s", (thread_id,)) if thread_id is not None else ("", ())
        accs: dict[str, ThreadAccumulator] = {}
        marker = " OR ".join(f"metadata ? '{k}'" for k in DELTA_METADATA_KEYS)
        cur = conn.execute(
            "SELECT thread_id, checkpoint_ns, checkpoint_id, checkpoint ->> 'v' AS v, checkpoint ->> 'ts' AS ts, "
            "pg_column_size(checkpoint) + coalesce(pg_column_size(metadata), 0) AS nbytes, "
            f"({marker}) AS delta, checkpoint -> 'channel_versions' AS refs "
            f"FROM checkpoints {where} ORDER BY thread_id, checkpoint_ns, checkpoint_id",
            args,
        )
        for r in cur:
            acc = accs.get(r["thread_id"]) or accs.setdefault(r["thread_id"], ThreadAccumulator(r["thread_id"]))
            if r["delta"]:
                acc.flags.add(FLAG_DELTA)
            refs = r["refs"] if isinstance(r["refs"], dict) else {}
            acc.add_checkpoint(
                r["checkpoint_ns"],
                r["checkpoint_id"],
                r["nbytes"] or 0,
                v=as_int(r["v"]),
                ts=parse_ts(r["ts"]),
                refs={str(k): str(v) for k, v in refs.items()},
            )
        snap_where = "c.thread_id = %s AND" if thread_id is not None else ""
        for r in conn.execute(_DELTA_SNAPSHOT_SQL.format(where=snap_where), args):
            if r["thread_id"] in accs:
                accs[r["thread_id"]].flags.add(FLAG_DELTA)
        for r in conn.execute(
            f"SELECT thread_id, checkpoint_ns, checkpoint_id, channel, coalesce(length(blob), 0) AS n "
            f"FROM checkpoint_writes {where}",
            args,
        ):
            acc = accs.get(r["thread_id"]) or accs.setdefault(r["thread_id"], ThreadAccumulator(r["thread_id"]))
            acc.add_write(r["checkpoint_ns"], r["checkpoint_id"], r["channel"], r["n"])
        for r in conn.execute(
            f"SELECT thread_id, checkpoint_ns, channel, version, coalesce(length(blob), 0) AS n "
            f"FROM checkpoint_blobs {where}",
            args,
        ):
            acc = accs.get(r["thread_id"]) or accs.setdefault(r["thread_id"], ThreadAccumulator(r["thread_id"]))
            acc.add_blob(r["checkpoint_ns"], r["channel"], r["version"], r["n"])
        return [a.finish(now) for a in accs.values()]

    def thread_state(self, conn: Any, thread_id: str, *, now: datetime) -> ThreadStats | None:
        found = self.scan(conn, now=now, thread_id=thread_id)
        if not found:
            return None
        st = found[0]
        try:
            self.fingerprint(conn, thread_id)
        except Exception:  # noqa: BLE001 - 못 풀면 판별 불가(fail-closed)
            st.flags = st.flags | {FLAG_UNDECODABLE}
        return st

    def thread_ids(self, conn: Any = None) -> list[str]:
        conn = conn or self.conn
        return [r["thread_id"] for r in conn.execute("SELECT DISTINCT thread_id FROM checkpoints ORDER BY 1")]

    def fingerprint(self, conn: Any, thread_id: str) -> dict[str, str]:
        out: dict[str, str] = {}
        heads = conn.execute(
            "SELECT checkpoint_ns, max(checkpoint_id) AS head FROM checkpoints WHERE thread_id = %s GROUP BY 1",
            (thread_id,),
        ).fetchall()
        for h in heads:
            ns, head = h["checkpoint_ns"], h["head"]
            # 채널 값은 공식 get_tuple 로 읽는다(인라인 값 + 블롭 합치기 규칙을 그대로 따름)
            tup = self.saver.get_tuple(
                {"configurable": {"thread_id": thread_id, "checkpoint_ns": ns, "checkpoint_id": head}}
            )
            values = tup.checkpoint.get("channel_values", {}) if tup else {}
            writes = [
                (w["task_id"], w["idx"], w["channel"], self.serde.loads_typed((w["type"], w["blob"])))
                for w in conn.execute(
                    "SELECT task_id, idx, channel, type, blob FROM checkpoint_writes "
                    "WHERE thread_id = %s AND checkpoint_ns = %s AND checkpoint_id = %s",
                    (thread_id, ns, head),
                )
            ]
            out[ns] = ns_fingerprint(self.serde, values, writes)
        return out

    def counts(self, conn: Any, thread_ids: list[str] | None = None) -> dict[str, int]:
        out = {}
        for table, key in (("checkpoints", "checkpoints"), ("checkpoint_writes", "writes"), ("checkpoint_blobs", "blobs")):
            if thread_ids is None:
                row = conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()
            else:
                row = conn.execute(f"SELECT count(*) AS n FROM {table} WHERE thread_id = ANY(%s)", (thread_ids,)).fetchone()
            out[key] = row["n"]
        return out

    # --- 쓰기 ---------------------------------------------------------------------
    def ensure_engine(self, *, keep_latest: bool) -> str:
        if keep_latest and not self.official_prune:
            raise UnsupportedBackendError(
                "the installed langgraph-checkpoint-postgres has no prune(); upgrade to 3.2.0 or newer",
                key="err.pg_no_prune",
            )
        return "official-postgres-prune"

    def run_in_transaction(self, fn: Callable[[Any], T]) -> T:
        """One REPEATABLE READ transaction; retried on serialization failure / deadlock."""
        errors = self._psycopg.errors
        last: Exception | None = None
        for _ in range(RETRIES):
            self.conn.isolation_level = self._psycopg.IsolationLevel.REPEATABLE_READ
            try:
                with self.conn.transaction():
                    return fn(self.conn)
            except (errors.SerializationFailure, errors.DeadlockDetected) as e:
                last = e  # 다른 트랜잭션과 겹침 — 바뀐 것 없이 롤백됐으니 다시 시도해도 안전
            except errors.LockNotAvailable as e:
                raise LgpruneError(f"database is busy: {e}", key="err.locked", detail=str(e).strip()) from e
            finally:
                self.conn.isolation_level = None
        raise LgpruneError(f"gave up after {RETRIES} retries: {last}", key="err.retries", detail=str(last))

    def keep_latest(self, conn: Any, thread_ids: list[str]) -> dict[str, int]:
        if not thread_ids:
            return {"checkpoints": 0, "writes": 0, "blobs": 0}
        self.ensure_engine(keep_latest=True)
        before = self.counts(conn, thread_ids)
        try:
            # 공식 구현에 위임: 블롭 참조 회수·부모 NULL·호출형 서브그래프 ns 규칙까지 포함
            self.saver.prune(list(thread_ids), strategy="keep_latest")
        except NotImplementedError as e:
            raise UnsafePruneError(str(e), key="err.delta_refused", threads=", ".join(thread_ids)) from e
        after = self.counts(conn, thread_ids)
        return {k: before[k] - after[k] for k in before}

    def delete_threads(self, conn: Any, thread_ids: list[str]) -> dict[str, int]:
        if not thread_ids:
            return {"checkpoints": 0, "writes": 0, "blobs": 0}
        before = self.counts(conn, thread_ids)
        for tid in thread_ids:
            self.saver.delete_thread(tid)
        after = self.counts(conn, thread_ids)
        return {k: before[k] - after[k] for k in before}

    def backup(self, target: str) -> str:
        raise LgpruneError("--backup is for SQLite; use pg_dump for Postgres", key="err.pg_backup")

    # --- 회수 ---------------------------------------------------------------------
    def relation_bytes(self) -> dict[str, int]:
        out = {}
        for t in TABLES:
            out[t] = self.conn.execute("SELECT pg_total_relation_size(%s::regclass) AS n", (t,)).fetchone()["n"]
        return out


_URL_RE = re.compile(r"^postgres(ql)?(\+\w+)?://", re.IGNORECASE)


def is_postgres_url(value: str) -> bool:
    return bool(_URL_RE.match(value))
