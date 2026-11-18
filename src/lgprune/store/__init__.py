"""`open_store(url)` picks the backend from the URL.

    ./agent.sqlite or /path/to/file   -> SqliteStore
    postgresql://user@host/db         -> PostgresStore (needs lgprune[postgres])
"""

from __future__ import annotations

from .postgres import PostgresStore, is_postgres_url, redact_url
from .sqlite import SqliteStore

Store = SqliteStore | PostgresStore

__all__ = ["PostgresStore", "SqliteStore", "Store", "open_store", "redact_url"]


def open_store(url: str, *, busy_timeout: float = 5.0) -> Store:
    """Open a checkpoint store. Use it as a context manager so the connection is closed."""
    if is_postgres_url(url):
        return PostgresStore(url, busy_timeout=busy_timeout)
    path = url[len("sqlite:///") :] if url.startswith("sqlite:///") else url
    return SqliteStore(path, busy_timeout=busy_timeout)
