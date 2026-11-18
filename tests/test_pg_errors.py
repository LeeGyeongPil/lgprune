"""F-1: the password hint is only for authentication failures (no server needed)."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from lgprune.store.postgres import is_auth_failure  # noqa: E402


class _Err(Exception):
    def __init__(self, msg: str, sqlstate: str | None = None) -> None:
        super().__init__(msg)
        self.sqlstate = sqlstate


@pytest.mark.parametrize(
    "msg,state",
    [
        ('FATAL: password authentication failed for user "x"', None),
        ("connection failed: fe_sendauth: no password supplied", None),
        ("anything", "28P01"),
        ("anything", "28000"),
    ],
)
def test_auth_errors_detected(msg: str, state: str | None) -> None:
    assert is_auth_failure(_Err(msg, state))


def test_password_hint_only_on_auth_failure() -> None:
    assert not is_auth_failure(_Err("connection refused", None))
    assert not is_auth_failure(_Err("timeout expired", "08006"))
