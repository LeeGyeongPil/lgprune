"""Shared fixtures.

- `db`: a fresh copy of the sample SQLite database for every test.
- `pg_url`: a Postgres URL with the sample threads loaded. Uses
  LGPRUNE_TEST_PG_URL when set (CI: postgres service container); otherwise it
  starts PGlite (tests/pglite, after `npm ci --prefix tests/pglite`) in the
  background and stops it at the end. Without either, pg tests are skipped.
"""

from __future__ import annotations

import hashlib
import io
import os
import shutil
import socket
import sqlite3
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from lgprune.cli import main
from lgprune.sample import create_sample

HERE = Path(__file__).parent
PGLITE_DIR = HERE / "pglite"


@pytest.fixture(autouse=True)
def _english_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # 개발자 컴퓨터의 언어 설정이 테스트 결과를 바꾸지 않게 한다
    for name in ("LGPRUNE_LANG", "LC_ALL", "LC_MESSAGES", "LANG", "LGPRUNE_DB"):
        monkeypatch.delenv(name, raising=False)


# --- SQLite -----------------------------------------------------------------------------
@pytest.fixture(scope="session")
def sample_master(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return create_sample(tmp_path_factory.mktemp("master") / "sample.sqlite")


@pytest.fixture
def db(sample_master: Path, tmp_path: Path) -> Path:
    target = tmp_path / "agent.sqlite"
    shutil.copyfile(sample_master, target)
    return target


def table_counts(path: Path) -> dict[tuple[str, str], int]:
    """{(table, thread_id): rows} for every thread - to prove nothing changed."""
    conn = sqlite3.connect(str(path))
    try:
        out = {}
        for table in ("checkpoints", "writes"):
            for tid, n in conn.execute(f"SELECT thread_id, count(*) FROM {table} GROUP BY thread_id"):
                out[(table, tid)] = n
        return out
    finally:
        conn.close()


def files_sha(path: Path) -> str:
    """sha256 of the main file + the WAL content.

    A missing WAL and an empty WAL are the same to SQLite. Any read-only open of
    a WAL-mode database makes SQLite create an empty -wal/-shm pair, so we
    compare content (missing == empty), not the directory listing.
    """
    h = hashlib.sha256()
    h.update(hashlib.sha256(path.read_bytes()).digest())
    wal = Path(str(path) + "-wal")
    h.update(hashlib.sha256(wal.read_bytes() if wal.exists() else b"").digest())
    return h.hexdigest()


def run_cli(*argv: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = main([str(a) for a in argv], out=out, err=err)
    return code, out.getvalue(), err.getvalue()


# --- Postgres ---------------------------------------------------------------------------
def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_pg(url: str, proc: subprocess.Popen[bytes] | None, timeout: float = 60) -> None:
    import psycopg

    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"PGlite server exited with {proc.returncode}")
        try:
            psycopg.connect(url, connect_timeout=5).close()
            return
        except psycopg.Error as e:
            last = e
            time.sleep(0.2)
    raise RuntimeError(f"Postgres did not come up: {last}")


@pytest.fixture(scope="session")
def pg_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    pytest.importorskip("psycopg")
    url = os.environ.get("LGPRUNE_TEST_PG_URL")
    if url:
        _wait_for_pg(url, None)
        yield url
        return
    node = shutil.which("node")
    server = PGLITE_DIR / "node_modules" / "@electric-sql" / "pglite-socket" / "dist" / "scripts" / "server.js"
    if not node or not server.exists():
        pytest.skip("no Postgres: set LGPRUNE_TEST_PG_URL or run `npm ci --prefix tests/pglite`")
    port = _free_port()
    log = (tmp_path_factory.mktemp("pglite") / "server.log").open("wb")
    # PGlite 는 백엔드 하나다. 테스트는 연결을 하나씩 열고 닫지만, 앞 연결이 닫히기 전에 다음 연결이
    # 오면 기본값(1)에서는 거절되므로 몇 개까지 받아 두게 한다(동시에 쓰지는 않는다)
    proc = subprocess.Popen(
        [node, str(server), f"--port={port}", "--host=127.0.0.1", "--db=memory://", "--max-connections=4"],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    url = f"postgresql://postgres:postgres@127.0.0.1:{port}/postgres?sslmode=disable"
    try:
        _wait_for_pg(url, proc)
        yield url
    finally:
        # 서버를 직접 끝낸다(--run 방식은 자식이 끝나도 서버가 남는다)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        log.close()


@pytest.fixture
def pg_url(pg_server: str) -> str:
    """Empty the checkpoint tables and load the four sample threads."""
    import psycopg
    from langgraph.checkpoint.postgres import PostgresSaver
    from psycopg.rows import dict_row

    from lgprune.sample import fill

    with psycopg.connect(pg_server, autocommit=True, prepare_threshold=None, row_factory=dict_row) as conn:
        saver = PostgresSaver(conn)
        saver.setup()
        for table in ("checkpoint_writes", "checkpoint_blobs", "checkpoints"):
            conn.execute(f"DELETE FROM {table}")
        fill(saver)
    return pg_server
