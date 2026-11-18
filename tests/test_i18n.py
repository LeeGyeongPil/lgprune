"""AC-9 (part 1): language choice order and message table parity."""

from __future__ import annotations

import json
import re
from pathlib import Path

from lgprune.i18n import Translator, messages, resolve_lang

from .conftest import run_cli


def test_lang_precedence_and_key_parity(db: Path) -> None:
    # --lang > LGPRUNE_LANG > LC_ALL > LC_MESSAGES > LANG, 한국어가 아니면 영어
    env = {"LGPRUNE_LANG": "en", "LC_ALL": "ko_KR.UTF-8", "LC_MESSAGES": "ko_KR.UTF-8", "LANG": "ko_KR.UTF-8"}
    assert resolve_lang("ko", env) == "ko"
    assert resolve_lang(None, env) == "en"
    assert resolve_lang(None, {**env, "LGPRUNE_LANG": ""}) == "ko"
    assert resolve_lang(None, {"LC_ALL": "C", "LANG": "ko_KR.UTF-8"}) == "en"
    assert resolve_lang(None, {"LC_MESSAGES": "ko_KR", "LANG": "en_US"}) == "ko"
    assert resolve_lang(None, {"LANG": "ko_KR.UTF-8"}) == "ko"
    assert resolve_lang(None, {"LANG": "fr_FR.UTF-8"}) == "en"
    assert resolve_lang(None, {}) == "en"
    assert resolve_lang("de", {"LANG": "ko_KR.UTF-8"}) == "en"

    en, ko = messages("en"), messages("ko")
    assert set(en) == set(ko)
    placeholder = re.compile(r"\{(\w+)")
    for key in en:
        assert set(placeholder.findall(en[key])) == set(placeholder.findall(ko[key])), key
        assert ko[key].strip(), key

    # 언어를 바꾸면 문구가 실제로 바뀐다
    assert Translator("en")("col.thread") != Translator("ko")("col.thread")
    assert Translator("ko").reason("delta_channel") != Translator("en").reason("delta_channel")

    # JSON 출력의 키와 이유 코드는 언어와 무관
    now = "2026-12-01T00:00:00+00:00"
    outs = [run_cli("--lang", lang, "report", db, "--json", "--now", now)[1] for lang in ("en", "ko")]
    assert json.loads(outs[0]) == json.loads(outs[1])
    plans = [
        json.loads(run_cli("--lang", lang, "plan", db, "--trim-after", "0d", "--keep-recent", "0", "--json", "--now", now)[1])
        for lang in ("en", "ko")
    ]
    reasons = [{i["thread_id"]: i["reason"] for i in p["items"]} for p in plans]
    assert reasons[0] == reasons[1]
    assert reasons[0]["D"] == "delta_channel"
