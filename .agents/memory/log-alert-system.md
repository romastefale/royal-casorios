---
name: Log dump + smart owner alerts
description: How the bot decides what reaches the owner's DM vs. what stays on disk, and the cyclic-free wiring of the alert classifier.
---

# Log dump to disk + smart owner alerts

The owner used to get the whole log dumped to their DM every 5min. That was removed. Now:

- **Periodic log dump goes to files**, not DM: `log_dump_job` / `/royallog` write ring-buffer
  snapshots to `backup/logs/` with rotation (`LOG_BACKUP_KEEP`). Gist (`GH_TOKEN`) is optional.
- **Smart alerting** lives in `royal/alerts.py` (leaf module, deps only `config`; graph stays acyclic
  `config ← alerts ← core`). A root-logger `OwnerAlertHandler` classifies every `ERROR`/`CRITICAL`
  via `ERROR_CATALOG` and only DMs the owner for *relevant* cases (real code bug / DB / import / boot).
  Transient Telegram errors (RetryAfter/flood, "query is too old", edit no-op, blocked bot, network)
  are silenced from DM. `is_benign_telegram_error()` is used in handler `except` blocks (e.g. `hub_cb`)
  so benign Telegram errors are not even logged as ERROR.

**Rule — `OWNER_ALERTS_ENABLED` gates only the DM, never the file.**
**Why:** observability must survive turning DMs off; a relevant error must always leave a report in
`backup/alerts/` even when the owner muted alerts. A code review caught an early version that early-
returned before writing the file when the flag was off.
**How to apply:** in `OwnerAlertHandler.emit`, classify + `_write_file` first; check
`OWNER_ALERTS_ENABLED` only right before scheduling the DM.

**Catalog mirror:** `ERROR_CATALOG` (code) and `backup/ERROR_CATALOG.md` (human-readable) must be
kept in sync when adding/editing a case.

**Recursion safety:** the DM sender (`_send_owner_alert_dm` in core) swallows all exceptions and never
logs on failure — otherwise a failed alert-DM would log an ERROR that re-triggers the handler.
