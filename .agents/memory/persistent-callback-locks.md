---
name: Persistent callback owner-locks
description: How to owner-lock inline-keyboard callbacks that must outlive the _msg_owners TTL
---

# Lock durável p/ menus persistentes (callbacks que não somem)

**Regra:** qualquer menu inline com `auto_delete_secs=0` (persiste até o Fechar) deve
embutir o `uid` do dono **dentro do callback_data** (ex.: `r:tut:{idx}:{uid}`,
`r:av:{n}:{uid}`, `r:close:{uid}`) e validar `cb.from_user.id == uid` no handler.
Ao editar in-place, rebuildar o teclado com o mesmo uid para manter o lock entre edições.

**Why:** `assert_owner`/`register_owner` guardam o dono em `_msg_owners` = `TTLCache(ttl=900)`
(15 min). Para menus efêmeros isso basta, mas para menus que **persistem** o lock expira e,
passado o TTL, `assert_owner` não acha dono e **libera para qualquer um** operar o menu alheio
(broken access control — pego em code review). `assert_owner` só serve como reforço/retrocompat
quando o uid NÃO está no callback.

**How to apply:** ao criar um novo menu owner-locked persistente, embuta o uid no callback de
TODO botão de navegação/ação (não só no Fechar) e cheque-o no handler antes de agir. Trava por
teste de formato do teclado (ver `tests/test_pure.py::test_*_kb_lock_duravel_uid_embutido`).
