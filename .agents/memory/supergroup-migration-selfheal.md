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

**Rule:** any proactive send loop that can hit a stale chat_id should catch
`TelegramMigrateToChat`, call `migrate_chat_data(old, new)` (transactional +
idempotent — safe to re-run) to self-heal the orphaned progress, then retry the
send to the new id. Reactive handlers don't need this — `on_chat_migration`
already covers the live migration message.

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
