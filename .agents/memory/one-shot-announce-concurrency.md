---
name: One-shot announce concurrency
description: One-shot group announcements that gain a manual re-trigger need an in-process running guard, not just the flag-at-end pattern.
---

# One-shot announce concurrency guard

The boot announce functions (`announce_typewriter_feature`, `announce_no_bots_feature`)
use a "check `bot_meta` flag → sleep → broadcast → set flag at the end" pattern. That is
retry-safe across boots but is **NOT** concurrency-safe: it only works because each is
spawned exactly once at startup.

**Rule:** the moment a one-shot announce also gets a manual re-trigger path (an owner
command that resets the flag and re-spawns the task), add an **in-process running guard**
(a module-level bool set to True *before any await*, reset in `finally`). Under asyncio
single-thread, setting it before the first `await` guarantees a second concurrent run sees
True and bails — preventing double-post / double-pin in the group.

**Why:** owner rule = never flood the group. Two triggers (boot task + owner command, or
repeated command spam) would otherwise both pass the flag check during the sleep window and
broadcast twice.

**How to apply:** split the body into a guarded entrypoint (flag check + running check + set
+ try/finally) and a plain `_impl()` that does the sleep/query/send. The owner command
should also read the running flag and reply "já em andamento" instead of spawning a dup.
