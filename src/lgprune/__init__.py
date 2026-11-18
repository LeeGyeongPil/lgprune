"""lgprune - retention for LangGraph checkpoint databases.

    with lgprune.open_store("agent.sqlite") as store:
        report = store.report()
        plan = lgprune.make_plan(store, lgprune.Policy(trim_after=timedelta(days=1)))
        result = lgprune.apply_plan(store, plan)

Independent open-source tool; not affiliated with LangChain.
"""

from __future__ import annotations

from .errors import (
    LgpruneError,
    PlanFormatError,
    UnsafePruneError,
    UnsupportedBackendError,
    VerifyMismatchError,
)
from .models import ApplyResult, Plan, PlanItem, ReclaimResult, Report, ThreadStats, VerifyResult
from .plan import apply_plan, make_plan, verify_plan
from .policy import Policy, parse_duration
from .reclaim import reclaim
from .store import PostgresStore, SqliteStore, Store, open_store

__version__ = "0.1.0"

__all__ = [
    "ApplyResult",
    "LgpruneError",
    "Plan",
    "PlanFormatError",
    "PlanItem",
    "Policy",
    "PostgresStore",
    "ReclaimResult",
    "Report",
    "SqliteStore",
    "Store",
    "ThreadStats",
    "UnsafePruneError",
    "UnsupportedBackendError",
    "VerifyMismatchError",
    "VerifyResult",
    "__version__",
    "apply_plan",
    "make_plan",
    "open_store",
    "parse_duration",
    "reclaim",
    "verify_plan",
]
