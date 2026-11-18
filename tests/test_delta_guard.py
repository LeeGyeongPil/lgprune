"""AC-4: DeltaChannel threads are never trimmed (plain keep_latest would empty them silently)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import lgprune
from lgprune.sample import get_state_values
from lgprune.tour import naive_keep_latest

from .conftest import run_cli, table_counts


def test_delta_thread_never_trimmed(db: Path, tmp_path: Path) -> None:
    # 대조군: 단순 SQL 은 오류 없이 24 -> 0
    copy = tmp_path / "naive.sqlite"
    copy.write_bytes(db.read_bytes())
    assert len(get_state_values(copy, "D")["messages"]) == 24
    naive_keep_latest(copy, "D")
    assert get_state_values(copy, "D").get("messages", []) == []

    d_values = get_state_values(db, "D")
    later = datetime.now(UTC) + timedelta(days=400)
    policies = [
        lgprune.Policy(trim_after=timedelta(0), keep_recent=0),
        lgprune.Policy(trim_after=timedelta(0), keep_recent=0, include=["D"]),
        lgprune.Policy(trim_after=timedelta(0), keep_recent=0, exclude_interrupted=False),
        lgprune.Policy(threads=["D"]),
        lgprune.Policy(trim_after=timedelta(days=1), keep_recent=0),
    ]
    with lgprune.open_store(str(db)) as store:
        for policy in policies:
            for now in (None, later):
                item = lgprune.make_plan(store, policy, now=now).item("D")
                assert item.action != "keep_latest"
        plan = lgprune.make_plan(store, policies[0])
        lgprune.apply_plan(store, plan)
    assert get_state_values(db, "D") == d_values
    assert len(get_state_values(db, "D")["messages"]) == 24


def test_tampered_plan_rejected_and_rolled_back(db: Path, tmp_path: Path) -> None:
    plan_path = tmp_path / "plan.json"
    assert run_cli("plan", db, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)[0] == 0
    data = json.loads(plan_path.read_text())
    for item in data["items"]:
        if item["thread_id"] == "D":  # 손으로 고쳐 D 를 keep_latest 로 넣는다
            item["action"], item["reason"] = "keep_latest", "idle"
    plan_path.write_text(json.dumps(data))
    before = table_counts(db)

    code, _, err = run_cli("apply", plan_path, "--yes")
    assert code == 2
    assert "DeltaChannel" in err
    assert table_counts(db) == before  # A·B 를 포함해 아무것도 바뀌지 않음

    with lgprune.open_store(str(db)) as store, pytest.raises(lgprune.UnsafePruneError):
        lgprune.apply_plan(store, lgprune.Plan.load(plan_path))
    assert table_counts(db) == before
    assert len(get_state_values(db, "D")["messages"]) == 24

    code, _, err = run_cli("--lang", "ko", "apply", plan_path, "--yes")
    assert code == 2 and "DeltaChannel" in err and "롤백" in err


def test_explicit_thread_still_skipped(db: Path) -> None:
    code, out, _ = run_cli("plan", db, "--threads", "D", "--json")
    assert code == 2  # 지정한 스레드가 전부 거부됨
    item = next(i for i in json.loads(out)["items"] if i["thread_id"] == "D")
    assert (item["action"], item["reason"]) == ("skip", "delta_channel")

    code, out, _ = run_cli("plan", db, "--threads", "D")
    assert code == 2 and "DeltaChannel" in out
    code, out, _ = run_cli("plan", db, "--threads", "D", "--include-interrupted", "--json")
    assert code == 2
