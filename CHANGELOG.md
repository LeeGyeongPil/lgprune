# Changelog

## 0.1.1 - 2026-11-18

### Fixed

- When the Postgres URL has no password, authentication errors explain PGPASSWORD / ~/.pgpass / --db (other connection failures no longer get that hint).
- Singular wording for one thread (`1 thread`) in English report and plan totals.

## 0.1.0 - 2026-11-18

First release.

- `report`, `plan`, `apply`, `verify`, `reclaim`, `sample` and `tour` commands, plus a Python API (`open_store`, `make_plan`, `apply_plan`, `verify_plan`, `reclaim`).
- SQLite engine with `keep_latest` and `delete`, run in one `BEGIN IMMEDIATE` transaction with a pre-commit head-state comparison and rollback.
- Postgres support: report, and apply through the official `PostgresSaver.prune` (langgraph-checkpoint-postgres 3.2.0 or newer).
- Safety checks: DeltaChannel threads are never trimmed, interrupted threads are skipped by default, legacy, undecodable and stale threads are skipped.
- English and Korean screens (`--lang en|ko`, `LGPRUNE_LANG`).
