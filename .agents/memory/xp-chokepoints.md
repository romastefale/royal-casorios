---
name: XP award chokepoints
description: Where all XP/scoring funnels in the Royal bot — gate participation here, not per call-site.
---

All XP grants funnel through exactly two functions in `royal/core.py`:
`award_xp_immediate` (event XP: couple, palavra, chest, boss, vote_like, quest,
music_mention, lucky, tomo, reaction) and `award_xp_message` (per-message/reply XP).

**Rule:** to globally pause a player's scoring (e.g. the "left the game"
`players.left_game` flag), add the guard inside these two functions, NOT at each
call site. There are ~12 call sites; gating per-site is error-prone and a new
call site silently re-opens the leak.

**Why:** during Etapa 4 (leave/return), per-site gating missed lucky/music/quest/
tomo/vote_like/chest/boss XP. Centralizing in the two award functions closed all
of them in one place and is future-proof.

**How to apply:** non-XP side effects that ride alongside an XP grant still need
their own guard (florins/gold award in quest claim, item consume in tomo use,
lucky jackpot public mention, rejoin welcome mention). The central guard only
stops the XP, not the sibling side effect.
