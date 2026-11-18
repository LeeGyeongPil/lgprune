"""The three steps: make a plan (read-only), apply it (one transaction), verify it.

    make_plan   scan threads -> policy -> per thread: keep_latest / delete / skip(reason)
    apply_plan  BEGIN -> re-check every target (stale? unsafe?) -> delete
                -> compare head fingerprints BEFORE commit -> COMMIT or ROLLBACK
    verify_plan compare the current head state with the fingerprints in the plan
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from fnmatch import fnmatchcase
from typing import Any

from .errors import PlanFormatError, UnsafePruneError, VerifyMismatchError
from .fingerprint import thread_fingerprint
from .models import ApplyResult, Plan, PlanItem, ThreadStats, VerifyResult
from .policy import Policy
from .safety import (
    REASON_INTERRUPTED,
    REASON_MISSING,
    REASON_STALE,
    REASON_UNDECODABLE,
    is_interrupted,
    keep_latest_blocker,
)

# 계획에서 쓰는 이유 코드(기계용)
REASON_IDLE = "idle"  # 유휴 기간이 정책을 넘김
REASON_EXPLICIT = "explicit"  # --threads 로 직접 지정
REASON_NOT_SELECTED = "not_selected"
REASON_EXCLUDED = "excluded"
REASON_RECENT = "recent"  # keep_recent 로 보호
REASON_ACTIVE = "active"  # 아직 유휴 기간이 안 됨
REASON_NO_RULE = "no_rule"  # trim_after·delete_after 둘 다 없음
REASON_NOTHING = "nothing_to_trim"  # 이미 ns 마다 체크포인트 1개
REASON_NO_CHECKPOINTS = "no_checkpoints"  # 쓰기만 있고 체크포인트가 아직 없음


def _zero_rows() -> dict[str, int]:
    return {"checkpoints": 0, "writes": 0, "blobs": 0}


# --- make_plan ---------------------------------------------------------------------
def make_plan(store: Any, policy: Policy, *, now: datetime | None = None) -> Plan:
    """Decide what to do with every thread. Never writes to the database."""
    now = now or datetime.now(UTC)
    reader = store.reader()
    stats = store.scan(reader, now=now)
    stats.sort(key=lambda s: -s.bytes)

    # 최근 활동 순서로 keep_recent 개를 보호한다
    by_recent = sorted(stats, key=lambda s: s.last_activity or datetime.min.replace(tzinfo=UTC), reverse=True)
    recent = {s.thread_id for s in by_recent[: policy.keep_recent]}
    explicit = set(policy.threads) if policy.threads else None

    trim_after, delete_after = policy.trim_after, policy.delete_after
    if explicit is not None and trim_after is None and delete_after is None:
        trim_after = timedelta(0)  # 스레드를 직접 지정했으면 "지금 정리"로 본다

    items = [
        _decide(store, reader, s, policy, recent, explicit, trim_after, delete_after)
        for s in stats
    ]
    # 지정했지만 DB 에 없는 스레드도 이유와 함께 보여 준다
    known = {s.thread_id for s in stats}
    for tid in sorted((explicit or set()) - known):
        items.append(PlanItem(thread_id=tid, action="skip", reason=REASON_MISSING, rows=_zero_rows()))

    return Plan(
        backend=store.backend,
        db=store.db,
        created_at=datetime.now(UTC),
        now=now,
        policy=policy.to_json(),
        items=items,
    )


def _decide(
    store: Any,
    reader: Any,
    s: ThreadStats,
    policy: Policy,
    recent: set[str],
    explicit: set[str] | None,
    trim_after: timedelta | None,
    delete_after: timedelta | None,
) -> PlanItem:
    def skip(reason: str) -> PlanItem:
        return PlanItem(
            thread_id=s.thread_id, action="skip", reason=reason, rows=_zero_rows(),
            heads=dict(s.heads), flags=sorted(s.flags), idle_days=s.idle_days,
        )

    tid = s.thread_id
    # 1) 대상 선정: 직접 지정 / include / exclude / 최근 N개
    if explicit is not None and tid not in explicit:
        return skip(REASON_NOT_SELECTED)
    if policy.include and not any(fnmatchcase(tid, p) for p in policy.include):
        return skip(REASON_NOT_SELECTED)
    if any(fnmatchcase(tid, p) for p in policy.exclude):
        return skip(REASON_EXCLUDED)
    if explicit is None and tid in recent:
        return skip(REASON_RECENT)

    # 2) 유휴 기간으로 동작 결정(삭제가 정리보다 우선)
    if trim_after is None and delete_after is None:
        return skip(REASON_NO_RULE)
    if s.idle_days is None:
        # 체크포인트 없이 쓰기만 있음 = 첫 저장이 진행 중일 수 있다 — 시각을 모르면 건드리지 않는다
        return skip(REASON_NO_CHECKPOINTS)
    idle = timedelta(days=s.idle_days)
    if delete_after is not None and idle >= delete_after:
        action = "delete"
    elif trim_after is not None and idle >= trim_after:
        action = "keep_latest"
    else:
        return skip(REASON_ACTIVE)
    reason = REASON_EXPLICIT if explicit is not None else REASON_IDLE

    # 3) 안전장치(fail-closed)
    if action == "keep_latest":
        blocker = keep_latest_blocker(s.flags)  # DeltaChannel·legacy·undecodable 은 재정의 불가
        if blocker:
            return skip(blocker)
    if policy.exclude_interrupted and is_interrupted(s.flags):
        return skip(REASON_INTERRUPTED)  # 사람 승인 대기 중인 스레드는 건드리지 않는다
    if action == "keep_latest" and sum(s.trim_rows.values()) == 0:
        return skip(REASON_NOTHING)

    # 4) head 상태 지문: apply 가 커밋 전에 이 값과 비교한다
    try:
        ns_fps = store.fingerprint(reader, tid)
    except Exception:  # noqa: BLE001
        if action == "keep_latest":
            return skip(REASON_UNDECODABLE)
        ns_fps = {}

    if action == "delete":
        rows = {"checkpoints": s.checkpoints, "writes": s.writes, "blobs": s.blobs}
        nbytes = s.bytes
    else:
        rows, nbytes = dict(s.trim_rows), s.trim_bytes
    return PlanItem(
        thread_id=tid,
        action=action,
        reason=reason,
        rows=rows,
        bytes=nbytes,
        heads=dict(s.heads),
        fingerprint=thread_fingerprint(ns_fps) if ns_fps else None,
        ns_fingerprints=ns_fps,
        flags=sorted(s.flags),
        idle_days=s.idle_days,
    )


# --- apply_plan --------------------------------------------------------------------
def apply_plan(store: Any, plan: Plan, *, backup: str | None = None, check_db: bool = True) -> ApplyResult:
    """Apply a plan in one transaction. Raises UnsafePruneError / VerifyMismatchError after rolling back."""
    if plan.backend != store.backend:
        raise PlanFormatError(
            f"plan is for {plan.backend}, store is {store.backend}", key="err.plan_db", plan_db=plan.db, db=store.db
        )
    if check_db and plan.db != store.db:
        raise PlanFormatError(
            f"plan was made for {plan.db}, not {store.db}", key="err.plan_db", plan_db=plan.db, db=store.db
        )
    targets = plan.actionable
    engine = store.ensure_engine(keep_latest=any(i.action == "keep_latest" for i in targets))
    backup_path = store.backup(backup) if backup else None
    exclude_interrupted = bool(plan.policy.get("exclude_interrupted", True))
    now = datetime.now(UTC)

    def attempt(conn: Any) -> ApplyResult:
        skipped = {i.thread_id: i.reason for i in plan.items if i.action == "skip"}
        trim: list[str] = []
        delete: list[str] = []
        pre: dict[str, dict[str, str]] = {}
        unsafe: dict[str, str] = {}

        # 1단계: 트랜잭션 안에서 대상마다 다시 확인 — 계획 뒤에 바뀐 스레드는 건너뛴다
        for item in targets:
            tid = item.thread_id
            st = store.thread_state(conn, tid, now=now)
            if st is None:
                skipped[tid] = REASON_MISSING
                continue
            if st.heads != item.heads:
                skipped[tid] = REASON_STALE  # 계획 뒤에 새 체크포인트가 생김
                continue
            if item.action == "keep_latest":
                blocker = keep_latest_blocker(st.flags)
                if blocker:
                    unsafe[tid] = blocker  # 계획 파일을 고쳐 넣어도 여기서 막힌다
                    continue
            if exclude_interrupted and is_interrupted(st.flags):
                skipped[tid] = REASON_INTERRUPTED
                continue
            try:
                fps = store.fingerprint(conn, tid)
            except Exception:  # noqa: BLE001
                if item.action == "keep_latest":
                    unsafe[tid] = REASON_UNDECODABLE
                    continue
                fps = {}
            if item.fingerprint and thread_fingerprint(fps) != item.fingerprint:
                skipped[tid] = REASON_STALE  # head 는 같지만 대기 쓰기가 바뀜
                continue
            pre[tid] = fps
            (trim if item.action == "keep_latest" else delete).append(tid)

        if unsafe:
            names = ", ".join(f"{t} ({r})" for t, r in sorted(unsafe.items()))
            raise UnsafePruneError(
                f"refused keep_latest for {names}: it would lose state "
                "(a DeltaChannel value is rebuilt from the checkpoints it would delete). Nothing was changed.",
                key="err.unsafe",
                threads=names,
            )

        # 2단계: 삭제
        rows = _zero_rows()
        for part in (store.keep_latest(conn, trim), store.delete_threads(conn, delete)):
            for k, v in part.items():
                rows[k] = rows.get(k, 0) + v

        # 3단계: 커밋 전에 head 상태 비교 — 다르면 예외로 롤백
        mismatched: dict[str, str] = {}
        for tid in trim:
            problem = compare_heads(pre[tid], store.fingerprint(conn, tid), store.backend)
            if problem:
                mismatched[tid] = problem
        for tid in delete:
            if any(store.counts(conn, [tid]).values()):
                mismatched[tid] = "still_present"
        if mismatched:
            names = ", ".join(f"{t} ({r})" for t, r in sorted(mismatched.items()))
            raise VerifyMismatchError(
                f"head state changed while pruning {names}; rolled back, nothing was changed",
                key="err.mismatch",
                threads=sorted(mismatched),
                names=names,
            )
        return ApplyResult(
            applied=trim + delete,
            skipped=skipped,
            rows_deleted=sum(rows.values()),
            rows_by_table=rows,
            verified=True,
            backup=backup_path,
            engine=engine,
        )

    return store.run_in_transaction(attempt)


def compare_heads(before: dict[str, str], after: dict[str, str], backend: str) -> str | None:
    """None when every namespace head is unchanged; otherwise a short reason code."""
    for ns, fp in before.items():
        if ns not in after:
            # 공식 Postgres prune 은 끝난 서브그래프 호출 ns("name:task_id")를 함께 지운다
            if backend == "postgres" and ":" in ns:
                continue
            return f"head_missing:{ns or 'root'}"
        if after[ns] != fp:
            return f"state_changed:{ns or 'root'}"
    extra = set(after) - set(before)
    if extra:
        return f"unexpected_ns:{sorted(extra)[0]}"
    return None


# --- verify_plan -------------------------------------------------------------------
def verify_plan(store: Any, plan: Plan) -> VerifyResult:
    """Check that kept heads still match the plan and deleted threads are gone."""
    reader = store.reader()
    checked: list[str] = []
    mismatched: dict[str, str] = {}
    present = set(store.thread_ids(reader))
    for item in plan.actionable:
        tid = item.thread_id
        checked.append(tid)
        if item.action == "delete":
            if tid in present:
                mismatched[tid] = "still_present"
            continue
        if tid not in present:
            mismatched[tid] = REASON_MISSING
            continue
        try:
            now_fps = store.fingerprint(reader, tid)
        except Exception:  # noqa: BLE001
            mismatched[tid] = REASON_UNDECODABLE
            continue
        problem = compare_heads(item.ns_fingerprints, now_fps, store.backend)
        if problem:
            mismatched[tid] = problem
    return VerifyResult(ok=not mismatched, checked=checked, mismatched=mismatched)
