"""AC-2: plan and apply without --yes never change the database; policy selection."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import lgprune
from lgprune.policy import parse_duration

from .conftest import files_sha, run_cli, table_counts


def test_plan_and_dry_apply_are_read_only(db: Path, tmp_path: Path) -> None:
    sha = files_sha(db)
    counts = table_counts(db)
    plan_path = tmp_path / "plan.json"

    code, out, _ = run_cli("plan", db, "--trim-after", "0d", "--keep-recent", "0", "-o", plan_path)
    assert code == 0
    assert "database was not changed" in out
    assert files_sha(db) == sha

    plan = json.loads(plan_path.read_text())
    items = {i["thread_id"]: i for i in plan["items"]}
    assert items["A"]["action"] == "keep_latest"
    assert items["A"]["rows"]["checkpoints"] == 59
    assert items["A"]["rows"]["writes"] == 60
    assert items["A"]["fingerprint"] and items["A"]["heads"]
    assert items["B"]["action"] == "keep_latest"
    assert (items["C"]["action"], items["C"]["reason"]) == ("skip", "interrupted")
    assert (items["D"]["action"], items["D"]["reason"]) == ("skip", "delta_channel")
    assert plan["totals"]["checkpoints"] == 59 + 14

    # --yes 없는 apply 는 미리보기만 — DB 를 열지도 않는다
    code, out, _ = run_cli("apply", plan_path)
    assert code == 0
    assert "Preview only" in out and "keep_latest" in out
    assert files_sha(db) == sha
    assert table_counts(db) == counts


def test_policy_selection_rules(db: Path) -> None:
    later = datetime.now(UTC) + timedelta(days=40)
    with lgprune.open_store(str(db)) as store:
        # 최근 N개 보호: 가장 최근에 활동한 D 와 C 는 recent
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(0), keep_recent=2))
        assert {i.thread_id for i in plan.items if i.reason == "recent"} == {"C", "D"}
        # 유휴 기간이 안 지났으면 active
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(days=1), keep_recent=0))
        assert {i.reason for i in plan.items} == {"active"}
        # 40일 뒤라고 가정: delete_after 가 trim_after 보다 우선, 승인 대기 C 는 제외
        plan = lgprune.make_plan(
            store, lgprune.Policy(trim_after=timedelta(days=1), delete_after=timedelta(days=30), keep_recent=0), now=later
        )
        acts = {i.thread_id: (i.action, i.reason) for i in plan.items}
        assert acts["A"] == ("delete", "idle") and acts["B"] == ("delete", "idle")
        assert acts["D"] == ("delete", "idle")  # 통째 삭제는 DeltaChannel 이어도 손실이 아니다
        assert acts["C"] == ("skip", "interrupted")
        assert plan.item("A").rows == {"checkpoints": 60, "writes": 60, "blobs": 0}
        # include / exclude 글롭
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(0), keep_recent=0, exclude=["A*"]))
        assert plan.item("A").reason == "excluded"
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(0), keep_recent=0, include=["B"]))
        assert [i.thread_id for i in plan.actionable] == ["B"]
        # 승인 대기도 포함하겠다고 하면 C 도 keep_latest(최신과 그 쓰기는 남는다)
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(0), keep_recent=0, exclude_interrupted=False))
        assert plan.item("C").action == "keep_latest"
        # 없는 스레드를 직접 지정하면 missing
        plan = lgprune.make_plan(store, lgprune.Policy(threads=["A", "nope"]))
        assert plan.item("A").action == "keep_latest" and plan.item("nope").reason == "missing"


def test_policy_file_and_flags(db: Path, tmp_path: Path) -> None:
    policy = tmp_path / "policy.toml"
    policy.write_text('trim_after = "0d"\nkeep_recent = 0\nexclude = ["B"]\n')
    code, out, _ = run_cli("plan", db, "--policy", policy, "--json")
    assert code == 0
    items = {i["thread_id"]: i for i in json.loads(out)["items"]}
    assert items["A"]["action"] == "keep_latest" and items["B"]["reason"] == "excluded"
    # 플래그가 파일 값을 덮어쓴다
    code, out, _ = run_cli("plan", db, "--policy", policy, "--exclude", "A", "--json")
    items = {i["thread_id"]: i for i in json.loads(out)["items"]}
    assert items["A"]["reason"] == "excluded" and items["B"]["action"] == "keep_latest"
    bad = tmp_path / "bad.toml"
    bad.write_text('trim_afterr = "1d"\n')
    code, _, err = run_cli("plan", db, "--policy", bad)
    assert code == 1 and "trim_afterr" in err
    code, _, err = run_cli("plan", db, "--trim-after", "soon")
    assert code == 1 and "soon" in err


def test_example_policy_file_parses(db: Path) -> None:
    policy = lgprune.Policy.from_toml(Path(__file__).parent.parent / "examples" / "policy.toml")
    assert policy.trim_after == timedelta(days=1) and policy.delete_after == timedelta(days=30)
    assert policy.keep_recent == 20 and policy.exclude == ["prod-*", "pinned-*"]
    with lgprune.open_store(str(db)) as store:
        assert {i.reason for i in lgprune.make_plan(store, policy).items} == {"recent"}


def test_parse_duration() -> None:
    assert parse_duration("90s") == timedelta(seconds=90)
    assert parse_duration("12h") == timedelta(hours=12)
    assert parse_duration("1d") == timedelta(days=1)
    assert parse_duration("2w") == timedelta(weeks=2)
    assert parse_duration("0d") == timedelta(0)
    assert parse_duration(None) is None
    with pytest.raises(lgprune.LgpruneError):
        parse_duration("1 year")


def test_plan_file_errors(tmp_path: Path) -> None:
    bad = tmp_path / "x.json"
    bad.write_text("{}")
    code, _, err = run_cli("apply", bad, "--yes")
    assert code == 1 and "plan" in err
    code, _, err = run_cli("apply", tmp_path / "none.json")
    assert code == 1
