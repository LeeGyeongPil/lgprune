"""AC-7: reclaim shrinks the file (VACUUM + wal_checkpoint(TRUNCATE)), only with --yes."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from .conftest import files_sha, run_cli


def _size(db: Path) -> int:
    wal = Path(str(db) + "-wal")
    return db.stat().st_size + (wal.stat().st_size if wal.exists() else 0)


def test_vacuum_and_wal_truncate_shrink_file(db: Path, tmp_path: Path) -> None:
    # B 는 통째 삭제, A 는 최신만 유지
    for args in (("--threads", "B", "--delete-after", "0d"), ("--threads", "A", "--trim-after", "0d")):
        plan_path = tmp_path / "plan.json"
        assert run_cli("plan", db, *args, "-o", plan_path)[0] == 0
        assert run_cli("apply", plan_path, "--yes")[0] == 0

    # --yes 없으면 아무것도 하지 않는다
    sha, size = files_sha(db), _size(db)
    code, out, _ = run_cli("reclaim", db)
    assert code == 0 and "--yes" in out
    assert files_sha(db) == sha and _size(db) == size

    code, out, _ = run_cli("reclaim", db, "--yes")
    assert code == 0
    m = re.search(r"\((?:saved .*?; )?(\d+) -> (\d+) bytes\)", out)
    assert m, out
    before, after = int(m.group(1)), int(m.group(2))
    assert before == size
    assert after < before / 2
    assert after == _size(db)
    assert "integrity_check: ok" in out

    conn = sqlite3.connect(str(db))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT count(*) FROM checkpoints WHERE thread_id = 'B'").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM checkpoints WHERE thread_id = 'A'").fetchone()[0] == 1
    finally:
        conn.close()
    wal = Path(str(db) + "-wal")
    assert not wal.exists() or wal.stat().st_size == 0
