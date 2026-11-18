"""AC-8: Postgres (PGlite locally, a real Postgres service in CI).

PGlite allows one connection at a time, so every test opens the store, does
its work through `store.saver` (the same connection) and closes it.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

import lgprune
from lgprune.sample import config, graph_for

from .conftest import run_cli

pytestmark = pytest.mark.pg


def _rows(store: lgprune.PostgresStore, thread_id: str) -> dict[str, int]:
    return store.counts(store.conn, [thread_id])


def _values(store: lgprune.PostgresStore, thread_id: str) -> dict:
    return dict(graph_for(thread_id, store.saver).get_state(config(thread_id)).values)


def test_pg_report_counts_three_tables(pg_url: str) -> None:
    code, out, _ = run_cli("report", pg_url, "--json")
    assert code == 0
    rep = json.loads(out)
    assert rep["backend"] == "postgres"
    assert "password" not in rep["db"] and ":postgres@" not in rep["db"]  # 비밀번호는 지운다
    threads = {t["thread_id"]: t for t in rep["threads"]}
    assert set(threads) == {"A", "B", "C", "D"}
    a = threads["A"]
    assert (a["checkpoints"], a["writes"], a["blobs"]) == (60, 60, 60)
    assert a["table_bytes"]["checkpoints"] > 0
    assert a["table_bytes"]["writes"] > 0
    assert a["table_bytes"]["blobs"] > 100_000  # messages 가 블롭 쪽에 쌓인다
    assert a["top_channel"] == "messages"
    assert a["history_bytes"] > a["latest_bytes"]
    assert "interrupted" in threads["C"]["flags"]
    assert "delta" in threads["D"]["flags"]
    assert rep["official_prune"] is True

    code, out, _ = run_cli("report", pg_url)
    assert code == 0 and "blobs" in out and "messages" in out


def test_pg_keep_latest_delegates_to_official_prune(pg_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from langgraph.checkpoint.postgres import PostgresSaver

    calls: list[tuple[list[str], str]] = []
    original = PostgresSaver.prune

    def spy(self, thread_ids, *, strategy="keep_latest"):  # type: ignore[no-untyped-def]
        calls.append((list(thread_ids), strategy))
        return original(self, thread_ids, strategy=strategy)

    monkeypatch.setattr(PostgresSaver, "prune", spy)

    with lgprune.open_store(pg_url) as store:
        a_before = _values(store, "A")
        d_before = _values(store, "D")
        d_rows = _rows(store, "D")
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(0), keep_recent=0))
        assert plan.item("D").reason == "delta_channel"
        assert plan.item("A").rows == {"checkpoints": 59, "writes": 60, "blobs": 59}
        result = lgprune.apply_plan(store, plan)
        assert result.verified and result.engine == "official-postgres-prune"
        # D 를 뺀 목록으로 공식 prune 을 한 번 부른다
        assert calls == [(["A", "B"], "keep_latest")]
        assert _rows(store, "A") == {"checkpoints": 1, "writes": 0, "blobs": 1}
        assert _values(store, "A") == a_before
        assert _rows(store, "D") == d_rows
        assert _values(store, "D") == d_before and len(d_before["messages"]) == 24
        assert lgprune.verify_plan(store, plan).ok
        # 정리 뒤에도 이어서 실행된다
        graph_for("A", store.saver).invoke({"messages": ["after"]}, config("A"))
        assert len(_values(store, "A")["messages"]) == 42


def test_pg_tampered_plan_rolls_back_everything(pg_url: str, tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.json"
    code, _, _ = run_cli("plan", pg_url, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)
    assert code == 0
    data = json.loads(plan_path.read_text())
    for item in data["items"]:
        if item["thread_id"] == "D":
            item["action"], item["reason"] = "keep_latest", "idle"
    plan_path.write_text(json.dumps(data))
    with lgprune.open_store(pg_url) as store:
        before = {t: _rows(store, t) for t in "ABCD"}
    code, _, err = run_cli("apply", plan_path, "--yes")
    assert code == 2
    assert "DeltaChannel" in err
    with lgprune.open_store(pg_url) as store:
        assert {t: _rows(store, t) for t in "ABCD"} == before


def test_pg_without_official_prune_exits_2(pg_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from langgraph.checkpoint.base import BaseCheckpointSaver
    from langgraph.checkpoint.postgres import PostgresSaver

    plan_path = tmp_path / "plan.json"
    assert run_cli("plan", pg_url, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0
    # 3.2.0 미만 설치본처럼: prune 이 기본 구현(NotImplementedError)으로 돌아간 상태
    monkeypatch.setattr(PostgresSaver, "prune", BaseCheckpointSaver.prune)
    code, out, err = run_cli("report", pg_url)
    assert code == 0 and "official prune() in the installed checkpointer: no" in out
    with lgprune.open_store(pg_url) as store:
        before = {t: _rows(store, t) for t in "ABCD"}
    code, _, err = run_cli("apply", plan_path, "--yes")
    assert code == 2
    assert "3.2.0" in err and "Upgrade" in err
    with lgprune.open_store(pg_url) as store:
        assert {t: _rows(store, t) for t in "ABCD"} == before


def test_pg_official_prune_is_rolled_back_on_mismatch(pg_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """The official prune runs inside our outer transaction, so a mismatch undoes it."""
    original = lgprune.PostgresStore.keep_latest

    def damaging(self, conn, thread_ids):  # type: ignore[no-untyped-def]
        rows = original(self, conn, thread_ids)
        # 오류 주입: 정리 직후 A 의 head 가 가리키는 messages 블롭을 망가뜨린다
        conn.execute(
            "UPDATE checkpoint_blobs SET blob = %s WHERE thread_id = 'A' AND channel = 'messages'",
            (self.serde.dumps_typed(["broken"])[1],),
        )
        return rows

    monkeypatch.setattr(lgprune.PostgresStore, "keep_latest", damaging)
    with lgprune.open_store(pg_url) as store:
        before = {t: _rows(store, t) for t in "ABCD"}
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(0), keep_recent=0))
        with pytest.raises(lgprune.VerifyMismatchError):
            lgprune.apply_plan(store, plan)
        assert {t: _rows(store, t) for t in "ABCD"} == before  # A 는 여전히 체크포인트 60개
        assert len(_values(store, "A")["messages"]) == 40


def test_pg_delete_keeps_interrupted_and_reclaim_notes(pg_url: str, tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.json"
    assert run_cli("plan", pg_url, "--delete-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0
    code, out, _ = run_cli("apply", plan_path, "--yes")
    assert code == 0, out
    with lgprune.open_store(pg_url) as store:
        assert lgprune.Plan.load(plan_path).item("C").reason == "interrupted"
        assert all(v == 0 for v in _rows(store, "A").values())
        assert all(v == 0 for v in _rows(store, "D").values())  # 통째 삭제는 DeltaChannel 이어도 허용
        assert graph_for("C", store.saver).get_state(config("C")).next == ("approve",)
    code, out, _ = run_cli("verify", plan_path)
    assert code == 0 and "state verified" in out
    code, out, _ = run_cli("reclaim", pg_url)
    assert code == 0 and "autovacuum" in out and "--analyze --yes" in out
    code, out, _ = run_cli("reclaim", pg_url, "--analyze", "--yes")
    assert code == 0 and "VACUUM (ANALYZE) done" in out
