"""AC-3, AC-5, AC-6: SQLite apply keeps state, skips stale/interrupted threads, rolls back on mismatch."""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest
from langgraph.checkpoint.sqlite import SqliteSaver

import lgprune
from lgprune.sample import config, get_state_values, graph_for

from .conftest import run_cli, table_counts


def _one_more_turn(db: Path, thread_id: str) -> int:
    conn = sqlite3.connect(str(db), check_same_thread=False)
    try:
        app = graph_for(thread_id, SqliteSaver(conn))
        app.invoke({"messages": ["one more"]}, config(thread_id))
        return len(app.get_state(config(thread_id)).values["messages"])
    finally:
        conn.close()


def _per_ns(db: Path, thread_id: str) -> dict[str, int]:
    conn = sqlite3.connect(str(db))
    try:
        return dict(conn.execute(
            "SELECT checkpoint_ns, count(*) FROM checkpoints WHERE thread_id = ? GROUP BY checkpoint_ns", (thread_id,)
        ).fetchall())
    finally:
        conn.close()


def test_keep_latest_preserves_state_and_resume(db: Path, tmp_path: Path) -> None:
    before = get_state_values(db, "A")
    counts_before = table_counts(db)
    plan_path, backup = tmp_path / "plan.json", tmp_path / "agent.bak.sqlite"
    assert run_cli("plan", db, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0

    code, out, err = run_cli("apply", plan_path, "--yes", "--backup", backup)
    assert code == 0, err
    assert "applied keep_latest A" in out and "state verified" in out

    assert _per_ns(db, "A") == {"": 1}  # ns 마다 체크포인트 1개
    assert get_state_values(db, "A") == before  # 실제 그래프로 읽은 상태가 같다
    assert _one_more_turn(db, "A") == 42  # 정상 재개

    # 백업은 온전하고 정리 전 행 수를 그대로 가진다
    conn = sqlite3.connect(str(backup))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        conn.close()
    assert table_counts(backup) == counts_before

    # 같은 백업 경로에 다시 쓰지 않는다
    assert run_cli("plan", db, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0
    code, _, err = run_cli("apply", plan_path, "--yes", "--backup", backup)
    assert code == 1 and "already exists" in err


def test_stale_and_interrupted_threads_untouched(db: Path, tmp_path: Path) -> None:
    with lgprune.open_store(str(db)) as store:
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(0), keep_recent=0))
    # 계획을 만든 뒤 A 에 새 턴이 생긴다(실행 중인 런)
    _one_more_turn(db, "A")
    a_rows = {k: v for k, v in table_counts(db).items() if k[1] == "A"}
    with lgprune.open_store(str(db)) as store:
        result = lgprune.apply_plan(store, plan)
    assert result.skipped["A"] == "stale"
    assert result.applied == ["B"]
    assert {k: v for k, v in table_counts(db).items() if k[1] == "A"} == a_rows

    # 통째 삭제 정책에서도 승인 대기 C 는 남는다
    plan_path = tmp_path / "plan.json"
    assert run_cli("plan", db, "--delete-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0
    code, out, _ = run_cli("apply", plan_path, "--yes")
    assert code == 0
    assert "skipped C" in out
    remaining = {tid for (_, tid) in table_counts(db)}
    assert remaining == {"C"}
    conn = sqlite3.connect(str(db), check_same_thread=False)
    try:
        assert graph_for("C", SqliteSaver(conn)).get_state(config("C")).next == ("approve",)
    finally:
        conn.close()


def test_fingerprint_mismatch_rolls_back(db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = lgprune.SqliteStore.keep_latest

    def damaging(self, conn, thread_ids):  # type: ignore[no-untyped-def]
        rows = original(self, conn, thread_ids)
        # 오류 주입: 삭제 단계에서 A 의 head 행을 B 의 체크포인트 내용으로 덮어쓴다
        conn.execute(
            "UPDATE checkpoints SET checkpoint = (SELECT checkpoint FROM checkpoints WHERE thread_id = 'B' "
            "ORDER BY checkpoint_id DESC LIMIT 1) WHERE thread_id = 'A'"
        )
        return rows

    monkeypatch.setattr(lgprune.SqliteStore, "keep_latest", damaging)
    before = table_counts(db)
    a_state = get_state_values(db, "A")
    plan_path = tmp_path / "plan.json"
    assert run_cli("plan", db, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0

    with lgprune.open_store(str(db)) as store, pytest.raises(lgprune.VerifyMismatchError) as info:
        lgprune.apply_plan(store, lgprune.Plan.load(plan_path))
    assert "A" in info.value.threads
    assert table_counts(db) == before  # 다른 스레드(B)까지 전부 롤백
    assert get_state_values(db, "A") == a_state

    code, _, err = run_cli("apply", plan_path, "--yes")
    assert code == 3
    assert "Rolled back" in err
    assert table_counts(db) == before


def test_verify_after_apply(db: Path, tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.json"
    assert run_cli("plan", db, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0
    code, out, _ = run_cli("verify", plan_path)
    assert code == 0  # 정리 전에도 head 상태는 같다(keep_latest 는 head 를 바꾸지 않음)
    assert run_cli("apply", plan_path, "--yes")[0] == 0
    code, out, _ = run_cli("verify", plan_path)
    assert code == 0 and "state verified" in out
    # 그 뒤 A 가 새 턴을 진행하면 계획과 달라진 것을 알려 준다(종료 코드 3)
    _one_more_turn(db, "A")
    code, out, _ = run_cli("verify", plan_path, "--json")
    assert code == 3 and '"A"' in out


def test_busy_database_changes_nothing(db: Path, tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.json"
    assert run_cli("plan", db, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0
    before = table_counts(db)
    holder = sqlite3.connect(str(db), isolation_level=None)
    try:
        holder.execute("BEGIN IMMEDIATE")  # 다른 쓰기가 잠금을 잡고 있다
        code, _, err = run_cli("apply", plan_path, "--yes", "--busy-timeout", "1s")
        assert code == 1 and "busy" in err
        holder.execute("ROLLBACK")
    finally:
        holder.close()
    assert table_counts(db) == before


def test_writes_without_checkpoint_are_kept(db: Path) -> None:
    """A run saves writes in the background; they can land before their checkpoint does."""
    conn = sqlite3.connect(str(db))
    with conn:
        for tid, cid in (("E", "1f1e0000-0000-6000-8000-000000000000"), ("A", "1fff0000-0000-6000-8000-000000000000")):
            conn.execute(
                "INSERT INTO writes (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value) "
                "VALUES (?, '', ?, 't', 0, 'messages', 'msgpack', x'90')",
                (tid, cid),
            )
    conn.close()
    with lgprune.open_store(str(db)) as store:
        plan = lgprune.make_plan(store, lgprune.Policy(delete_after=timedelta(0), keep_recent=0))
        assert plan.item("E").reason == "no_checkpoints"  # 시각을 모르면 건드리지 않는다
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(0), keep_recent=0))
        assert plan.item("A").rows["writes"] == 60  # 아직 체크포인트가 없는 쓰기는 세지 않는다
        lgprune.apply_plan(store, plan)
    counts = table_counts(db)
    assert counts[("writes", "E")] == 1
    assert counts[("writes", "A")] == 1  # 미래 체크포인트의 쓰기는 남는다


def test_plan_for_other_database_is_refused(db: Path, tmp_path: Path, sample_master: Path) -> None:
    plan_path = tmp_path / "plan.json"
    assert run_cli("plan", db, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0
    other = tmp_path / "other.sqlite"
    other.write_bytes(sample_master.read_bytes())
    with lgprune.open_store(str(other)) as store, pytest.raises(lgprune.PlanFormatError):
        lgprune.apply_plan(store, lgprune.Plan.load(plan_path))
    # --db 로 명시하면 쓸 수 있다(head 와 지문 검사는 그대로)
    code, out, _ = run_cli("apply", plan_path, "--yes", "--db", other)
    assert code == 0 and "applied keep_latest A" in out
