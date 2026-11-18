"""AC-9 (part 2) and AC-10: help/tables in both languages, usage errors, the tour."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from .conftest import run_cli


@pytest.mark.parametrize("lang,prefix", [("en", "usage:"), ("ko", "사용법:")])
@pytest.mark.parametrize("cmd", ["report", "plan", "apply", "verify", "reclaim", "sample", "tour"])
def test_subcommand_usage_prefix_once(
    cmd: str, lang: str, prefix: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run_cli("--lang", lang, cmd, "--help")[0] == 0
    first = capsys.readouterr().out.splitlines()[0]
    assert first.startswith(f"{prefix} lgprune {cmd}")
    assert first.count(prefix) == 1


def test_subcommand_arg_error_has_clean_prog(capsys: pytest.CaptureFixture[str]) -> None:
    code, _, err = run_cli("--lang", "en", "plan", "--keep-recent", "x")
    assert code != 0
    text = err + capsys.readouterr().err
    assert "lgprune plan: error:" in text
    assert "usage: usage:" not in text and "usage: lgprune plan: error" not in text


def test_help_in_both_languages(db: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    # 도움말(argparse 가 stdout 에 직접 쓴다)
    assert run_cli("--lang", "ko", "--help")[0] == 0
    ko_help = capsys.readouterr().out
    assert "사용법:" in ko_help and "명령" in ko_help and "보존 정리 도구" in ko_help
    assert run_cli("--lang", "en", "--help")[0] == 0
    en_help = capsys.readouterr().out
    assert "usage:" in en_help and "commands" in en_help and "Retention for LangGraph" in en_help
    assert "사용법" not in en_help
    assert run_cli("plan", "--help", "--lang", "ko")[0] == 0
    assert "이만큼 쉬고 있는 스레드" in capsys.readouterr().out

    # 표 머리글과 건너뛴 이유
    _, ko_report, _ = run_cli("--lang", "ko", "report", db)
    assert "스레드" in ko_report and "가장 큰 채널" in ko_report and "승인 대기" in ko_report
    _, en_report, _ = run_cli("--lang", "en", "report", db)
    assert "top channel" in en_report and "스레드" not in en_report
    _, ko_plan, _ = run_cli("--lang", "ko", "plan", db, "--trim-after", "0d", "--keep-recent", "0")
    assert "DeltaChannel - 값 손실 위험" in ko_plan and "최신만 유지" in ko_plan
    _, en_plan, _ = run_cli("--lang", "en", "plan", db, "--trim-after", "0d", "--keep-recent", "0")
    assert "interrupted - waiting for a human" in en_plan

    # 환경 변수 우선순위
    monkeypatch.setenv("LANG", "ko_KR.UTF-8")
    assert "가장 큰 채널" in run_cli("report", db)[1]
    monkeypatch.setenv("LGPRUNE_LANG", "en")
    assert "top channel" in run_cli("report", db)[1]
    assert "가장 큰 채널" in run_cli("--lang", "ko", "report", db)[1]

    # 오류 문구도 언어를 따른다
    assert "데이터베이스 파일이 없습니다" in run_cli("--lang", "ko", "report", "/nonexistent/x.sqlite")[2]


def test_usage_errors_exit_1(capsys: pytest.CaptureFixture[str]) -> None:
    assert run_cli("frobnicate")[0] == 1
    assert run_cli("plan", "x.sqlite", "--keep-recent", "many")[0] == 1
    assert run_cli()[0] == 1
    assert run_cli("--version")[0] == 0
    capsys.readouterr()


def test_pg_url_without_password_explains_where_it_comes_from(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plan files drop the password; a failed connect must say how to supply it (no server needed)."""
    pytest.importorskip("psycopg")
    import socket

    with socket.socket() as s:  # 아무도 듣지 않는 포트 -> 바로 연결 실패
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    url = f"postgresql://postgres@127.0.0.1:{port}/postgres?connect_timeout=3"
    code, _, err = run_cli("report", url)
    assert code == 1
    assert "PGPASSWORD" in err and "--db" in err
    assert "PGPASSWORD" in run_cli("--lang", "ko", "report", url)[2]
    # 비밀번호가 주어졌으면(PGPASSWORD) 그 안내는 하지 않는다 — 원인이 다르다
    monkeypatch.setenv("PGPASSWORD", "x")
    code, _, err = run_cli("report", url)
    assert code == 1 and "cannot connect to Postgres" in err and "PGPASSWORD" not in err


def test_sample_command(tmp_path: Path) -> None:
    out_db = tmp_path / "s.sqlite"
    code, out, _ = run_cli("sample", out_db)
    assert code == 0 and out_db.exists() and "DeltaChannel" in out
    code, _, err = run_cli("sample", out_db)
    assert code == 1 and "already exists" in err


def test_tour_runs_end_to_end(tmp_path: Path) -> None:
    started = time.monotonic()
    code, out, err = run_cli("tour", "--lang", "en", "--keep", tmp_path / "tour")
    assert code == 0, err
    assert time.monotonic() - started < 60
    assert "24 -> 0" in out
    assert "skipped D (DeltaChannel)" in out
    assert "state verified" in out
    assert "D messages after lgprune: 24 -> 24" in out
    assert "one more turn -> 42 messages" in out
    assert "integrity_check: ok" in out
    assert (tmp_path / "tour" / "sample.bak.sqlite").exists()
