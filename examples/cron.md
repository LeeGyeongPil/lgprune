# Running lgprune on a schedule

lgprune has no built-in scheduler. Run `plan` then `apply` from cron or a systemd timer.
`apply` re-checks every thread inside one transaction, so a run that keeps writing
between `plan` and `apply` is skipped as `stale` instead of being cut.

## cron (SQLite)

```cron
# every night at 03:30: trim idle threads, delete old ones, then shrink the file on Sundays
30 3 * * *  cd /srv/agent && lgprune plan agent.sqlite --policy policy.toml -o /tmp/lgprune-plan.json --json > /dev/null && lgprune apply /tmp/lgprune-plan.json --yes --json >> /var/log/lgprune.jsonl
45 3 * * 0  cd /srv/agent && lgprune reclaim agent.sqlite --yes --json >> /var/log/lgprune.jsonl
```

Exit codes: `0` ok, `1` usage/connection error (for example the database stayed locked),
`2` safety refusal, `3` state mismatch (rolled back). Alert on anything but `0`.

`reclaim` runs `VACUUM`, which needs free disk space about the size of the database and an
exclusive lock. Schedule it when the agent is idle, or less often than `apply`.

## systemd timer (Postgres)

`/etc/systemd/system/lgprune.service`

```ini
[Unit]
Description=lgprune checkpoint retention

[Service]
Type=oneshot
Environment=LGPRUNE_DB=postgresql://agent@db.internal/agent
# the password comes from PGPASSWORD or ~/.pgpass; plan files never contain it
EnvironmentFile=/etc/lgprune.env
ExecStart=/usr/local/bin/lgprune plan --policy /etc/lgprune/policy.toml -o /var/lib/lgprune/plan.json --json
ExecStart=/usr/local/bin/lgprune apply /var/lib/lgprune/plan.json --yes --json
```

`/etc/systemd/system/lgprune.timer`

```ini
[Unit]
Description=Run lgprune nightly

[Timer]
OnCalendar=*-*-* 03:30
Persistent=true

[Install]
WantedBy=timers.target
```

On Postgres, deleted rows become reusable space for autovacuum; the table files do not shrink.
`lgprune reclaim --analyze --yes` runs `VACUUM (ANALYZE)`. `VACUUM FULL` or `pg_repack`
shrink the files but lock the tables, so run them yourself in a maintenance window.
