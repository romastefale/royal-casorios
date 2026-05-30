---
name: Inventory consume must be atomically guarded
description: Why using a consumable must decrement-with-guard BEFORE granting the effect
---

# Inventory "usar" button exploit class

The inventory card does NOT refresh or remove its inline keyboard in-place after a
consumable is used — the "usar" button stays clickable on the same message.

**Rule:** consuming a consumable must go through an atomic guarded decrement
(`UPDATE inventory SET qty=qty-1 WHERE ... AND qty>0`) and only grant the effect
(XP / heal / etc.) when `rowcount>0`. Never grant the effect first and decrement
after.

**Why:** granting first + decrementing after (no guard) let a player spam the
persistent button after qty hit 0 for unlimited +XP/heal. This is the same
anti-duplication family as the PALAVRA/BAÚ atomic claims.

**How to apply:** route every consumable effect in the inventory `use` handler
through `consume_consumable()` (royal/core.py). Any NEW consumable effect must do
the same — consume-then-grant, not grant-then-consume. Locked by
`test_consume_consumable_atomico_evita_uso_duplicado`.
