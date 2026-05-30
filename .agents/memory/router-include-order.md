---
name: Router include order (royal/ package)
description: Ordering constraints for aiogram Router includes after the main.py → royal/ modular split.
---

# Router include order constraints (royal/handlers/*)

After the monolith `main.py` was split into the `royal/` package, each feature is an aiogram
`Router` included in `main.py`. In aiogram the FIRST router (in include order) whose handler
matches WINS and stops propagation. Most handlers are disjoint (unique commands / button texts /
callback prefixes), so include order is irrelevant for them — EXCEPT two constraints that MUST hold:

1. **`system` must be included LAST.** It owns the catch-all `track` (`@router.message` with
   `~(F.text & startswith("/"))`) plus migration / dice / music-bot-post handlers. `track` must only
   win after every specific handler had a chance.

2. **`hub` must be included BEFORE `cfg` and `missoes`.**
   **Why:** `hub_cb`'s filter is `F.data.startswith("r:") & ~F.data.startswith(_HUB_DEDICATED_PREFIXES)`
   and `_HUB_DEDICATED_PREFIXES = ("r:inv:", "r:priv:", "r:dados:")` — it does NOT exclude `"r:cfg:"`
   or `"r:quest:"`. So `hub_cb` actually MATCHES `r:cfg:` / `r:quest:` callbacks. In the original
   monolith `hub_cb` was registered before `cfg_cb` / `quest_claim_cb`, so `hub_cb` intercepts those
   prefixes. The split preserves this exact precedence by keeping `hub` before `cfg`/`missoes`.
   **How to apply:** if you ever add a new `r:` callback prefix with a dedicated handler in another
   module, either add the prefix to `_HUB_DEDICATED_PREFIXES` (so hub_cb stops swallowing it) OR keep
   that module's router included before/after hub consistently with the intended precedence. Do not
   reorder `hub` after `cfg`/`missoes`.

The split was purely structural (verbatim code, zero behavior/DB change). Behavior equivalence was
checked via: 401/401 top-level symbol parity vs the original, ruff `E9,F63,F7,F82` clean,
`import main` OK with 20 routers, full test suite green.
