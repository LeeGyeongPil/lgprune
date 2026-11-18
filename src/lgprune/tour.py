"""`lgprune tour` - the whole flow on a synthetic sample database, in about 5 seconds.

1. create the sample (real LangGraph graphs, no LLM)
2. on a throw-away copy, run the naive "keep only the latest checkpoint" SQL
   on DeltaChannel thread D and show its messages go 24 -> 0 without any error
3. report  4. plan (read-only)  5. apply (D is skipped) + verify  6. reclaim
"""

from __future__ import annotations

import hashlib
import shutil
import sqlite3
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import TextIO

from .errors import LgpruneError
from .i18n import Translator
from .plan import apply_plan, make_plan, verify_plan
from .policy import Policy
from .reclaim import reclaim
from .sample import config, create_sample, get_state_values, graph_for
from .store import open_store


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    for p in (path, Path(str(path) + "-wal")):
        if p.exists():
            h.update(p.read_bytes())
    return h.hexdigest()


def naive_keep_latest(db: Path, thread_id: str) -> None:
    """The hand-written SQL people try first (DO NOT use on real data)."""
    conn = sqlite3.connect(str(db), isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        head = conn.execute("SELECT max(checkpoint_id) FROM checkpoints WHERE thread_id = ?", (thread_id,)).fetchone()[0]
        conn.execute("DELETE FROM writes WHERE thread_id = ? AND checkpoint_id <> ?", (thread_id, head))
        conn.execute("DELETE FROM checkpoints WHERE thread_id = ? AND checkpoint_id <> ?", (thread_id, head))
        conn.execute("UPDATE checkpoints SET parent_checkpoint_id = NULL WHERE thread_id = ?", (thread_id,))
        conn.execute("COMMIT")
    finally:
        conn.close()


def run_tour(t: Translator, out: TextIO, *, keep: str | None = None) -> int:
    from .cli import render_apply, render_plan, render_reclaim, render_report, render_verify

    def say(text: str = "") -> None:
        out.write(text + "\n")
        out.flush()

    started = time.monotonic()
    if keep:
        work = Path(keep).expanduser().resolve()
        if work.exists() and any(work.iterdir()):
            raise LgpruneError(f"{work} is not empty", key="err.keep_not_empty", path=str(work))
        work.mkdir(parents=True, exist_ok=True)
    else:
        work = Path(tempfile.mkdtemp(prefix="lgprune-tour-"))
    try:
        db = work / "sample.sqlite"
        say(t("tour.title"))
        say(t("tour.workdir", path=str(work)))
        say()

        # 1) 예제 DB
        say(t("tour.step1"))
        create_sample(db)
        say(t("tour.sample_made", path=str(db)))
        say()

        # 2) 단순 SQL 의 조용한 손실을 임시 복사본에서 재현
        say(t("tour.step2"))
        naive = work / "naive-copy.sqlite"
        shutil.copyfile(db, naive)
        before = len(get_state_values(naive, "D")["messages"])
        naive_keep_latest(naive, "D")
        after = len(get_state_values(naive, "D").get("messages", []))
        say(t("tour.naive_result", before=before, after=after))
        say(t("tour.naive_why"))
        say()

        # 3) 보고서
        say(t("tour.step3"))
        with open_store(str(db)) as store:
            say(render_report(store.report(), t))
        say()

        # 4) 계획(읽기 전용)
        say(t("tour.step4"))
        sha_before = _sha(db)
        with open_store(str(db)) as store:
            plan = make_plan(store, Policy(trim_after=timedelta(0), keep_recent=0))
        plan_path = work / "plan.json"
        plan.dump(plan_path)
        say(render_plan(plan, t))
        say(t("tour.unchanged") if _sha(db) == sha_before else t("tour.changed"))
        say()

        # 5) 적용 + 검증
        say(t("tour.step5"))
        a_before = get_state_values(db, "A")
        with open_store(str(db)) as store:
            result = apply_plan(store, plan, backup=str(work / "sample.bak.sqlite"))
            say(render_apply(result, plan, t))
            say(render_verify(verify_plan(store, plan), t))
        d_after = len(get_state_values(db, "D")["messages"])
        say(t("tour.d_kept", before=before, after=d_after))
        say(t("tour.a_same", same=t("yes") if get_state_values(db, "A") == a_before else t("no")))
        # 정리한 뒤에도 대화를 이어 갈 수 있는지 한 턴 더 실행해 본다
        conn = sqlite3.connect(str(db), check_same_thread=False)
        try:
            from langgraph.checkpoint.sqlite import SqliteSaver

            app = graph_for("A", SqliteSaver(conn))
            app.invoke({"messages": ["one more turn"]}, config("A"))
            resumed = len(app.get_state(config("A")).values["messages"])
        finally:
            conn.close()
        say(t("tour.a_resumed", n=resumed))
        say()

        # 6) 공간 회수
        say(t("tour.step6"))
        with open_store(str(db)) as store:
            say(render_reclaim(reclaim(store, yes=True), t))
        say()
        say(t("tour.done", seconds=f"{time.monotonic() - started:.1f}"))
        say(t("tour.kept", path=str(work)) if keep else t("tour.cleaned"))
        return 0
    finally:
        if not keep:
            shutil.rmtree(work, ignore_errors=True)  # 임시 폴더는 항상 지운다
