"""Screen language: English or Korean.

Order: `--lang en|ko` > `LGPRUNE_LANG` > `LC_ALL` > `LC_MESSAGES` > `LANG`.
Anything that is not Korean falls back to English.
All user-facing strings live in `locales/<lang>.json` (one table per language).
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from functools import lru_cache
from importlib import resources
from typing import Any

LANGS = ("en", "ko")


def _normalize(value: str | None) -> str | None:
    """'ko_KR.UTF-8' -> 'ko', 'en_US' -> 'en', 'C' -> 'en', '' -> None."""
    if not value:
        return None
    v = value.strip().lower()
    if not v:
        return None
    # 한국어로 시작하면 ko, 그 밖의 값은 전부 영어
    return "ko" if v.startswith("ko") else "en"


def resolve_lang(cli_value: str | None = None, env: Mapping[str, str] | None = None) -> str:
    env = os.environ if env is None else env
    for candidate in (
        cli_value,
        env.get("LGPRUNE_LANG"),
        env.get("LC_ALL"),
        env.get("LC_MESSAGES"),
        env.get("LANG"),
    ):
        lang = _normalize(candidate)
        if lang:
            return lang
    return "en"


@lru_cache(maxsize=len(LANGS))
def messages(lang: str) -> dict[str, str]:
    text = resources.files("lgprune").joinpath("locales", f"{lang}.json").read_text(encoding="utf-8")
    return json.loads(text)


class Translator:
    """`t("key", name=value)` -> formatted string in one language."""

    def __init__(self, lang: str = "en") -> None:
        self.lang = lang if lang in LANGS else "en"
        self._table = messages(self.lang)
        self._fallback = messages("en")

    def __call__(self, key: str, **kwargs: Any) -> str:
        template = self._table.get(key) or self._fallback.get(key) or key
        return template.format(**kwargs) if kwargs else template

    def reason(self, code: str) -> str:
        """Localized label for a machine reason code (JSON keeps the code)."""
        return self(f"reason.{code}") if f"reason.{code}" in self._table else code

    def action(self, code: str) -> str:
        return self(f"action.{code}") if f"action.{code}" in self._table else code
