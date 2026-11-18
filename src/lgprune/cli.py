"""Command line: report -> plan -> apply -> verify -> reclaim (+ sample, tour).

Exit codes: 0 ok, 1 usage/connection error, 2 safety refusal, 3 verify mismatch.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import unicodedata
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, NoReturn, TextIO

from . import __version__
from .errors import LgpruneError
from .i18n import Translator, resolve_lang
from .models import ApplyResult, Plan, ReclaimResult, Report, VerifyResult
from .plan import apply_plan, make_plan, verify_plan
from .policy import Policy, parse_duration
from .reclaim import reclaim
from .store import open_store

EXIT_OK, EXIT_USAGE, EXIT_REFUSED, EXIT_MISMATCH = 0, 1, 2, 3


# --- 표 출력 ---------------------------------------------------------------------------
def _width(text: str) -> int:
    # 한글 등 전각 문자는 두 칸으로 센다(표 정렬용)
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)


def _pad(text: str, width: int, right: bool) -> str:
    gap = " " * max(0, width - _width(text))
    return gap + text if right else text + gap


def table(headers: list[str], rows: list[list[str]], right: set[int] | None = None) -> str:
    right = right or set()
    widths = [max([_width(h)] + [_width(r[i]) for r in rows]) for i, h in enumerate(headers)]
    lines = ["  ".join(_pad(h, widths[i], i in right) for i, h in enumerate(headers)).rstrip()]
    lines.append("  ".join("-" * w for w in widths))
    for r in rows:
        lines.append("  ".join(_pad(c, widths[i], i in right) for i, c in enumerate(r)).rstrip())
    return "\n".join(lines)


def human(n: int | None) -> str:
    if n is None:
        return "-"
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} B"  # pragma: no cover


def render_report(report: Report, t: Translator, top: int | None = None) -> str:
    pg = report.backend == "postgres"
    headers = [t("col.thread"), t("col.checkpoints"), t("col.writes")]
    if pg:
        headers.append(t("col.blobs"))
    headers += [t("col.size"), t("col.latest"), t("col.history"), t("col.top_channel"), t("col.idle"), t("col.flags")]
    rows = []
    for s in report.threads[: top or None]:
        share = f"{human(s.history_bytes)} ({100 * s.history_bytes / s.bytes:.0f}%)" if s.bytes else "-"
        row = [s.thread_id, str(s.checkpoints), str(s.writes)]
        if pg:
            row.append(str(s.blobs))
        row += [
            human(s.bytes),
            human(s.latest_bytes),
            share,
            s.top_channel or "-",
            "-" if s.idle_days is None else f"{s.idle_days:.1f}",
            ", ".join(t(f"flag.{f}") for f in sorted(s.flags)) or "-",
        ]
        rows.append(row)
    right = {1, 2, 3, 4, 5, 6} if pg else {1, 2, 3, 4, 5}
    out = [t("report.title", backend=report.backend, db=report.db), "", table(headers, rows, right), ""]
    total = sum(s.bytes for s in report.threads)
    hist = sum(s.history_bytes for s in report.threads)
    out.append(t("report.totals_one" if len(report.threads) == 1 else "report.totals_other", threads=len(report.threads), total=human(total), history=human(hist)))
    if report.file_bytes is not None:
        out.append(t("report.file", size=human(report.file_bytes)))
    out.append(t("report.official_prune", value=t("yes") if report.official_prune else t("no")))
    out.append(t("report.hint"))
    return "\n".join(out)


def render_plan(plan: Plan, t: Translator) -> str:
    pg = plan.backend == "postgres"
    headers = [t("col.thread"), t("col.action"), t("col.reason"), t("col.checkpoints"), t("col.writes")]
    if pg:
        headers.append(t("col.blobs"))
    headers.append(t("col.frees"))
    rows = []
    for i in plan.items:
        row = [
            i.thread_id,
            t.action(i.action),
            t.reason(i.reason),
            str(i.rows.get("checkpoints", 0)),
            str(i.rows.get("writes", 0)),
        ]
        if pg:
            row.append(str(i.rows.get("blobs", 0)))
        row.append(human(i.bytes) if i.action != "skip" else "-")
        rows.append(row)
    right = {3, 4, 5, 6} if pg else {3, 4, 5}
    tot = plan.totals()
    lines = [t("plan.title", db=plan.db), t("plan.policy", policy=_policy_text(plan.policy)), "", table(headers, rows, right), ""]
    lines.append(
        t("plan.totals_one" if tot["threads"] == 1 else "plan.totals_other", threads=tot["threads"], checkpoints=tot["checkpoints"], writes=tot["writes"],
          blobs=tot["blobs"], size=human(tot["bytes"]))
    )
    return "\n".join(lines)


def _policy_text(p: dict[str, Any]) -> str:
    parts = []
    for k, v in p.items():
        if v in (None, [], ""):
            continue
        parts.append(f"{k}={','.join(v) if isinstance(v, list) else str(v).lower() if isinstance(v, bool) else v}")
    return " ".join(parts)


def render_apply(result: ApplyResult, plan: Plan, t: Translator) -> str:
    lines = []
    if result.backup:
        lines.append(t("apply.backup", path=result.backup))
    for tid in result.applied:
        item = plan.item(tid)
        lines.append(t("apply.applied", action=t.action(item.action), thread=tid))
    for tid, reason in result.skipped.items():
        if reason in ("not_selected", "recent", "active", "no_rule", "nothing_to_trim", "excluded", "no_checkpoints"):
            continue  # 정책상 대상이 아니었던 스레드는 계획 표에서 이미 보였다
        lines.append(t("apply.skipped", thread=tid, reason=t.reason(reason)))
    lines.append(t("apply.rows", rows=result.rows_deleted, checkpoints=result.rows_by_table.get("checkpoints", 0),
                   writes=result.rows_by_table.get("writes", 0), blobs=result.rows_by_table.get("blobs", 0)))
    lines.append(t("apply.verified", n=len(result.applied)))
    return "\n".join(lines)


def render_verify(result: VerifyResult, t: Translator) -> str:
    if result.ok:
        return t("verify.ok", n=len(result.checked))
    lines = [t("verify.bad", n=len(result.mismatched))]
    for tid, why in result.mismatched.items():
        lines.append(f"  {tid}: {why}")
    return "\n".join(lines)


def render_reclaim(result: ReclaimResult, t: Translator) -> str:
    if result.backend == "postgres":
        lines = [t("reclaim.pg_note")]
        for name, size in result.detail.get("relations_before", {}).items():
            after = result.detail.get("relations_after", {}).get(name, size)
            lines.append(f"  {name}: {human(size)} -> {human(after)}")
        lines.append(t("reclaim.pg_analyzed") if result.executed else t("reclaim.pg_hint"))
        return "\n".join(lines)
    if not result.executed:
        return t("reclaim.preview", size=human(result.before_bytes), free=human(result.reclaimable_bytes),
                 pages=result.detail.get("freelist_pages", 0))
    saved = (result.before_bytes or 0) - (result.after_bytes or 0)
    return "\n".join([
        t("reclaim.done", before=human(result.before_bytes), after=human(result.after_bytes), saved=human(saved),
          before_b=result.before_bytes, after_b=result.after_bytes),
        t("reclaim.integrity", value=result.detail.get("integrity", "?")),
    ])


# --- argparse (도움말도 고른 언어로) ------------------------------------------------------
class _Parser(argparse.ArgumentParser):
    """Usage errors exit with 1 (2 is reserved for safety refusals)."""

    def error(self, message: str) -> NoReturn:
        self.print_usage(sys.stderr)
        self.exit(EXIT_USAGE, f"{self.prog}: error: {message}\n")


def _formatter(t: Translator) -> type[argparse.HelpFormatter]:
    class Fmt(argparse.RawDescriptionHelpFormatter):
        def add_usage(self, usage: Any, actions: Any, groups: Any, prefix: str | None = None) -> None:
            super().add_usage(usage, actions, groups, prefix=t("help.usage_prefix") if prefix is None else prefix)

    return Fmt


def _parser(t: Translator, **kwargs: Any) -> _Parser:
    p = _Parser(add_help=False, formatter_class=_formatter(t), **kwargs)
    p._optionals.title = t("help.options")
    p._positionals.title = t("help.positionals")
    p.add_argument("-h", "--help", action="help", help=t("help.help"))
    p.add_argument("--lang", choices=["en", "ko"], help=t("help.lang"))
    return p


def build_parser(t: Translator) -> _Parser:
    root = _parser(t, prog="lgprune", description=t("help.description"), epilog=t("help.epilog"))
    root.add_argument("--version", action="version", version=f"lgprune {__version__}", help=t("help.version"))
    subs = root.add_subparsers(dest="command", title=t("help.commands"), metavar="COMMAND", parser_class=_Parser)

    def sub(name: str) -> _Parser:
        p = subs.add_parser(name, help=t(f"help.{name}"), description=t(f"help.{name}.desc"), add_help=False,
                            formatter_class=_formatter(t))
        p._optionals.title = t("help.options")
        p._positionals.title = t("help.positionals")
        p.add_argument("-h", "--help", action="help", help=t("help.help"))
        p.add_argument("--lang", choices=["en", "ko"], help=t("help.lang"))
        return p

    db_help = t("help.db")
    p = sub("report")
    p.add_argument("db", nargs="?", help=db_help)
    p.add_argument("--json", action="store_true", help=t("help.json"))
    p.add_argument("--top", type=int, metavar="N", help=t("help.top"))
    p.add_argument("--now", metavar="ISO", help=t("help.now"))

    p = sub("plan")
    p.add_argument("db", nargs="?", help=db_help)
    p.add_argument("--policy", metavar="FILE", help=t("help.policy"))
    p.add_argument("--trim-after", metavar="DUR", help=t("help.trim_after"))
    p.add_argument("--delete-after", metavar="DUR", help=t("help.delete_after"))
    p.add_argument("--keep-recent", type=int, metavar="N", help=t("help.keep_recent"))
    p.add_argument("--include", action="append", metavar="GLOB", help=t("help.include"))
    p.add_argument("--exclude", action="append", metavar="GLOB", help=t("help.exclude"))
    p.add_argument("--threads", metavar="A,B", help=t("help.threads"))
    p.add_argument("--include-interrupted", action="store_true", help=t("help.include_interrupted"))
    p.add_argument("--now", metavar="ISO", help=t("help.now"))
    p.add_argument("-o", "--output", metavar="FILE", help=t("help.output"))
    p.add_argument("--json", action="store_true", help=t("help.json"))

    p = sub("apply")
    p.add_argument("plan_file", metavar="PLAN", help=t("help.plan_file"))
    p.add_argument("--yes", action="store_true", help=t("help.yes_apply"))
    p.add_argument("--backup", metavar="PATH", help=t("help.backup"))
    p.add_argument("--busy-timeout", default="5s", metavar="DUR", help=t("help.busy_timeout"))
    p.add_argument("--db", help=t("help.db_override"))
    p.add_argument("--json", action="store_true", help=t("help.json"))

    p = sub("verify")
    p.add_argument("plan_file", metavar="PLAN", help=t("help.plan_file"))
    p.add_argument("--db", help=t("help.db_override"))
    p.add_argument("--json", action="store_true", help=t("help.json"))

    p = sub("reclaim")
    p.add_argument("db", nargs="?", help=db_help)
    p.add_argument("--yes", action="store_true", help=t("help.yes_reclaim"))
    p.add_argument("--analyze", action="store_true", help=t("help.analyze"))
    p.add_argument("--json", action="store_true", help=t("help.json"))

    p = sub("sample")
    p.add_argument("out", metavar="OUT.sqlite", help=t("help.sample_out"))

    p = sub("tour")
    p.add_argument("--keep", metavar="DIR", help=t("help.keep"))
    return root


def _pre_lang(argv: Sequence[str]) -> str | None:
    """Find --lang before argparse runs, so help text is already in that language."""
    for i, a in enumerate(argv):
        if a == "--lang" and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith("--lang="):
            return a.split("=", 1)[1]
    return None


def _parse_now(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as e:
        raise LgpruneError(f"bad --now {value!r}", key="err.now", value=value) from e
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _db_arg(value: str | None) -> str:
    db = value or os.environ.get("LGPRUNE_DB")
    if not db:
        raise LgpruneError("no database given (argument or LGPRUNE_DB)", key="err.no_db")
    return db


def _policy_from_args(a: argparse.Namespace) -> Policy:
    policy = Policy.from_toml(a.policy) if a.policy else Policy()
    # 플래그가 정책 파일 값을 덮어쓴다
    if a.trim_after is not None:
        policy.trim_after = parse_duration(a.trim_after)
    if a.delete_after is not None:
        policy.delete_after = parse_duration(a.delete_after)
    if a.keep_recent is not None:
        policy.keep_recent = a.keep_recent
        Policy.__post_init__(policy)
    if a.include:
        policy.include = list(a.include)
    if a.exclude:
        policy.exclude = list(a.exclude)
    if a.threads:
        policy.threads = [x.strip() for x in a.threads.split(",") if x.strip()]
    if a.include_interrupted:
        policy.exclude_interrupted = False
    return policy


def _print_json(data: Any, out: TextIO) -> None:
    out.write(json.dumps(data, indent=2, ensure_ascii=False) + "\n")


# --- 명령 -------------------------------------------------------------------------------
def cmd_report(a: argparse.Namespace, t: Translator, out: TextIO) -> int:
    with open_store(_db_arg(a.db)) as store:
        rep = store.report(now=_parse_now(a.now))
    if a.json:
        _print_json(rep.to_json(), out)
    else:
        out.write(render_report(rep, t, a.top) + "\n")
    return EXIT_OK


def cmd_plan(a: argparse.Namespace, t: Translator, out: TextIO) -> int:
    policy = _policy_from_args(a)
    with open_store(_db_arg(a.db)) as store:
        plan = make_plan(store, policy, now=_parse_now(a.now))
    if a.output:
        plan.dump(a.output)
    if a.json:
        _print_json(plan.to_json(), out)
    else:
        out.write(render_plan(plan, t) + "\n")
        out.write(t("plan.readonly") + "\n")
        if a.output:
            out.write(t("plan.saved", path=a.output) + "\n")
    # 직접 지정한 스레드가 전부 안전장치로 거부되면 종료 코드 2
    if policy.threads and not plan.actionable:
        if not a.json:
            out.write(t("plan.all_refused") + "\n")
        return EXIT_REFUSED
    return EXIT_OK


def cmd_apply(a: argparse.Namespace, t: Translator, out: TextIO) -> int:
    plan = Plan.load(a.plan_file)
    if not a.yes:
        # 미리보기: DB 를 열지 않는다
        out.write(render_plan(plan, t) + "\n" + t("apply.preview") + "\n")
        return EXIT_OK
    timeout = parse_duration(a.busy_timeout)
    with open_store(a.db or plan.db, busy_timeout=timeout.total_seconds() if timeout else 5.0) as store:
        result = apply_plan(store, plan, backup=a.backup, check_db=a.db is None)
    if a.json:
        _print_json(result.to_json(), out)
    else:
        out.write(render_apply(result, plan, t) + "\n")
    return EXIT_OK


def cmd_verify(a: argparse.Namespace, t: Translator, out: TextIO) -> int:
    plan = Plan.load(a.plan_file)
    with open_store(a.db or plan.db) as store:
        result = verify_plan(store, plan)
    if a.json:
        _print_json(result.to_json(), out)
    else:
        out.write(render_verify(result, t) + "\n")
    return EXIT_OK if result.ok else EXIT_MISMATCH


def cmd_reclaim(a: argparse.Namespace, t: Translator, out: TextIO) -> int:
    with open_store(_db_arg(a.db)) as store:
        result = reclaim(store, yes=a.yes, analyze=a.analyze)
    if a.json:
        _print_json(result.to_json(), out)
    else:
        out.write(render_reclaim(result, t) + "\n")
    return EXIT_OK


def cmd_sample(a: argparse.Namespace, t: Translator, out: TextIO) -> int:
    from .sample import create_sample

    path = create_sample(a.out)
    out.write(t("sample.done", path=str(path)) + "\n")
    return EXIT_OK


def cmd_tour(a: argparse.Namespace, t: Translator, out: TextIO) -> int:
    from .tour import run_tour

    return run_tour(t, out, keep=a.keep)


COMMANDS = {
    "report": cmd_report,
    "plan": cmd_plan,
    "apply": cmd_apply,
    "verify": cmd_verify,
    "reclaim": cmd_reclaim,
    "sample": cmd_sample,
    "tour": cmd_tour,
}


def main(argv: Sequence[str] | None = None, *, out: TextIO | None = None, err: TextIO | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    out = out or sys.stdout
    err = err or sys.stderr
    t = Translator(resolve_lang(_pre_lang(argv)))
    parser = build_parser(t)
    try:
        a = parser.parse_args(argv)
    except SystemExit as e:  # --help, --version, 사용법 오류
        return int(e.code or 0)
    if not a.command:
        parser.print_help(out)
        return EXIT_USAGE
    try:
        return COMMANDS[a.command](a, t, out)
    except LgpruneError as e:
        msg = t(e.key, **e.args_) if e.key and e.key in t._table else str(e)
        err.write(t("error.prefix") + msg + "\n")
        return e.exit_code
    except KeyboardInterrupt:
        err.write(t("error.interrupted") + "\n")
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
