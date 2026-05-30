---
name: DM commands must target the DM active chat, not an arbitrary group
description: Privacy/config commands in DM should resolve the user's active group, not LIMIT 1
---

# DM commands and multi-group users

A user can have a profile in several groups. DM commands that act on "their"
group must resolve the **active** group (the one chosen via /royalgrupo), not the
first arbitrary row.

**Rule:** prefer `get_dm_active_chat(uid)` (validated with `get_player`), falling
back to a `SELECT ... LIMIT 1` only when there is no active chat / no profile
there. Most DM commands already do this via `resolve_dm_chat`.

**Why:** the privacy command used a bare `SELECT chat_id FROM players WHERE
user_id=? LIMIT 1`, so multi-group users could only ever toggle privacy for one
arbitrary group and never their active one.

**How to apply:** when adding a DM command that reads/writes a per-group value,
resolve the active chat the same way (`_privacy_chat_id` in privacidade.py is the
local helper for the privacy flow).
