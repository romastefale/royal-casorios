---
name: Supergroup migration self-heal
description: Proactive sends to a group that migrated to a supergroup while the bot was offline raise TelegramMigrateToChat; handle by migrating data + retrying, and catalog it as expected.
---

# Supergroup migration self-heal

When a Telegram group is promoted to a supergroup **while the bot is offline**
(redeploy, downtime), the migration service message is never delivered, so
`on_chat_migration` never fires and the DB keeps the **old** chat_id. The next
**proactive** send (boot greeting, scheduled job) to that stale id raises
`aiogram.exceptions.TelegramMigrateToChat`, whose instance attribute
`exc.migrate_to_chat_id` carries the new supergroup id.

**Rule:** the self-heal lives **centrally in `safe_send` (`royal/core.py`)** —
the helper almost every group post uses. It catches `TelegramMigrateToChat`,
calls `migrate_chat_data(old, new)` (transactional + idempotent — safe to
re-run), then retries the send to the new id. So the FIRST proactive post after
an offline migration heals the data and every later post targets the new id.
Any proactive send path that bypasses `safe_send` (e.g. `announce_update` uses
`bot.send_photo` directly) needs the same handling inline. Reactive handlers
don't — `on_chat_migration` covers the live migration message.

**Why:** without it the group's XP/saldo/casórios stay orphaned under the old id
and the player progress looks lost; and the raw exception fires a false
"uncatalogued error" owner DM.

**How to apply:**
- `TelegramMigrateToChat` is in `ERROR_CATALOG` (`royal/alerts.py`) as
  `chat_migrated`, `relevant=False` → no owner DM (it's expected, not a bug).
  Mirror entry in `backup/ERROR_CATALOG.md`.
- Dedupe targets in such loops: in a mixed-state boot the chat list can contain
  **both** old and new ids → would double-greet the supergroup (violates the
  owner's no-flood rule). Track a `sent_targets` set; migrate data but skip the
  re-send if the new id was already greeted this run.
- Don't force a full greeting retry on migrate-DB-failure — that re-floods every
  group. The `logger.exception` on migrate failure is a DB error → already
  owner-alerted, which is the right safety net.
