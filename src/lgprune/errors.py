"""Exception hierarchy.

The CLI maps these to exit codes:
    LgpruneError (connection, usage, plan file) -> 1
    UnsupportedBackendError / UnsafePruneError   -> 2 (safety refusal)
    VerifyMismatchError                          -> 3 (rolled back / verify failed)

Each error carries a message key (`key`) and its arguments (`args_`) so the CLI
can print it in the chosen language. `str(err)` is always English, for logs and
for library users.
"""

from __future__ import annotations

from typing import Any


class LgpruneError(Exception):
    exit_code = 1

    def __init__(self, message: str, *, key: str | None = None, **kwargs: Any) -> None:
        super().__init__(message)
        self.key = key
        self.args_ = kwargs


class PlanFormatError(LgpruneError):
    """The plan file is missing, not JSON, or not an lgprune plan."""


class UnsupportedBackendError(LgpruneError):
    """The database is not a LangGraph checkpoint store we know how to prune."""

    exit_code = 2


class UnsafePruneError(LgpruneError):
    """A requested action would lose state (DeltaChannel, legacy format, ...)."""

    exit_code = 2


class VerifyMismatchError(LgpruneError):
    """Head state after pruning differs from the plan. The transaction was rolled back."""

    exit_code = 3

    def __init__(self, message: str, *, threads: list[str] | None = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.threads = threads or []
