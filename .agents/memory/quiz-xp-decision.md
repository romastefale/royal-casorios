---
name: Quiz /rquiz awards XP (batched)
description: The owner reversed the earlier "quiz gives no XP" decision — quiz now awards XP, but batched per-player to respect the no-flood rule.
---

# Quiz `/rquiz` awards XP

Quiz **does** award XP. Each correct answer is worth `QUIZ_XP_PER_POINT` XP, granted
**once per player at the podium** (`_send_results`) via `award_xp_immediate(pts × QUIZ_XP_PER_POINT,
reason="quiz")`, iterating all scorers — not per-answer.

**Why:** This reverses an earlier explicit decision ("quiz NÃO concede XP, para evitar flood de
level-up"). The owner asked to make the quiz award XP too. The original no-XP rule existed only to
avoid level-up flooding — the owner's no-flood rule (§ User preferences) still stands.

**How to apply:** Award XP in **one batch per player** (not on each `poll_answer`), so each player
triggers at most one level-up announcement. Do not move the grant into `rq_poll_answer`. If you ever
need to change the amount, edit `QUIZ_XP_PER_POINT` in `royal/config.py`. Class/couple/event
multipliers are applied inside the central chokepoint `award_xp_immediate`, so the displayed
"+N XP por acerto" footer is nominal (bonuses add silently on top).
