"""AC-1: report shows where the space goes, per thread."""

from __future__ import annotations

import json
from pathlib import Path

from .conftest import files_sha, run_cli

JSON_KEYS = {"thread_id", "checkpoints", "writes", "bytes", "latest_bytes", "history_bytes", "top_channel", "idle_days", "flags"}


def test_sample_report_flags_and_sizes(db: Path) -> None:
    sha = files_sha(db)
    code, out, _ = run_cli("report", db, "--json")
    assert code == 0
    rep = json.loads(out)
    threads = {t["thread_id"]: t for t in rep["threads"]}
    assert set(threads) == {"A", "B", "C", "D"}
    for t in threads.values():
        assert JSON_KEYS <= set(t)

    a = threads["A"]
    assert a["checkpoints"] == 60 and a["writes"] == 60
    assert a["top_channel"] == "messages"
    assert a["history_bytes"] > a["latest_bytes"] > 0
    assert a["bytes"] == a["history_bytes"] + a["latest_bytes"]
    assert a["flags"] == []
    assert "interrupted" in threads["C"]["flags"]
    assert "delta" in threads["D"]["flags"]
    assert threads["B"]["checkpoints"] == 15
    assert rep["official_prune"] is False  # SqliteSaver 3.1.3 에는 공식 prune 이 없다
    assert files_sha(db) == sha  # report 는 읽기 전용

    code, out, _ = run_cli("report", db)
    assert code == 0
    lines = {line.split()[0]: line for line in out.splitlines() if line[:2] in ("A ", "B ", "C ", "D ")}
    assert set(lines) == {"A", "B", "C", "D"}
    assert "messages" in lines["A"] and "%" in lines["A"]
    assert "interrupted" in lines["C"]
    assert "delta" in lines["D"]


def test_report_top_and_env_default(db: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LGPRUNE_DB", str(db))
    code, out, _ = run_cli("report", "--top", "1")
    assert code == 0
    rows = [line for line in out.splitlines() if line[:2] in ("A ", "B ", "C ", "D ")]
    assert len(rows) == 1 and rows[0].startswith("A ")  # 가장 큰 스레드만


def test_report_errors(tmp_path: Path) -> None:
    code, _, err = run_cli("report", tmp_path / "missing.sqlite")
    assert code == 1 and "not found" in err
    other = tmp_path / "other.sqlite"
    import sqlite3

    conn = sqlite3.connect(other)
    conn.execute("CREATE TABLE notes (id INTEGER)")
    conn.commit()
    conn.close()
    code, _, err = run_cli("report", other)
    assert code == 2 and "checkpoints" in err  # LangGraph 저장소가 아니면 안전 거부
    junk = tmp_path / "junk.sqlite"
    junk.write_bytes(b"not a database at all" * 100)
    code, _, err = run_cli("report", junk)
    assert code == 2
    code, _, err = run_cli("report")
    assert code == 1 and "LGPRUNE_DB" in err
