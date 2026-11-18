"""Give the freed space back.

SQLite: deleting rows only puts pages on the freelist; the file keeps its size.
`VACUUM` rewrites the file, but in WAL mode the rewrite lands in the `-wal`
file first - so we also run `PRAGMA wal_checkpoint(TRUNCATE)`. VACUUM needs
free disk space about the size of the database and an exclusive lock, which is
why it is a separate command that requires `--yes`.

Postgres: deleted rows become reusable space for autovacuum; the files do not
shrink. `VACUUM FULL` / pg_repack would shrink them but lock the tables, so we
only print guidance and, with `--analyze --yes`, run `VACUUM (ANALYZE)`.
"""

from __future__ import annotations

import shutil
import sqlite3
from typing import Any

from .errors import LgpruneError, UnsafePruneError
from .models import ReclaimResult
from .store.postgres import TABLES


def reclaim(store: Any, *, yes: bool = False, analyze: bool = False) -> ReclaimResult:
    if store.backend == "sqlite":
        return _reclaim_sqlite(store, yes=yes)
    return _reclaim_postgres(store, yes=yes, analyze=analyze)


def _reclaim_sqlite(store: Any, *, yes: bool) -> ReclaimResult:
    conn: sqlite3.Connection = store.write_conn()
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
    before = store.file_bytes()
    detail: dict[str, Any] = {"freelist_pages": freelist, "page_size": page_size}
    if not yes:
        # 미리보기: 아무것도 실행하지 않고 되찾을 수 있는 양만 알려 준다
        return ReclaimResult("sqlite", False, before, None, reclaimable_bytes=freelist * page_size, detail=detail)

    db_size = store.path.stat().st_size
    free = shutil.disk_usage(store.path.parent).free
    if free < db_size:
        raise UnsafePruneError(
            f"not enough free disk space for VACUUM ({free} < {db_size} bytes)",
            key="err.disk_space",
            free=free,
            need=db_size,
        )
    store.close_reader()  # 우리 쪽 읽기 연결이 열려 있으면 WAL 을 끝까지 비우지 못한다
    try:
        conn.execute("VACUUM")
        # WAL 모드: VACUUM 결과가 WAL 에 쌓인다 — TRUNCATE 체크포인트로 본 파일에 옮기고 WAL 을 0 바이트로
        busy, log, done = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    except sqlite3.OperationalError as e:
        raise LgpruneError(f"VACUUM failed: {e}", key="err.locked", detail=str(e)) from e
    detail.update({"wal_checkpoint": {"busy": busy, "log": log, "checkpointed": done}, "integrity": integrity})
    return ReclaimResult("sqlite", True, before, store.file_bytes(), reclaimable_bytes=freelist * page_size, detail=detail)


def _reclaim_postgres(store: Any, *, yes: bool, analyze: bool) -> ReclaimResult:
    before = store.relation_bytes()
    detail: dict[str, Any] = {"relations_before": before, "analyzed": False}
    if yes and analyze:
        # VACUUM 은 트랜잭션 밖(autocommit 연결)에서만 돈다. 파일은 줄지 않고 재사용 공간으로 표시된다
        store.conn.execute("VACUUM (ANALYZE) " + ", ".join(TABLES))
        detail["analyzed"] = True
    after = store.relation_bytes()
    detail["relations_after"] = after
    return ReclaimResult("postgres", bool(yes and analyze), sum(before.values()), sum(after.values()), detail=detail)
