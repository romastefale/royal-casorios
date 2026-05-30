---
name: Grupos do Royal + ponte @Mira (chat IDs, papéis, captura)
description: Os três grupos Telegram do bot e como a ponte com a IA @Mira captura respostas (modelo TR3 reply-match) — incluindo o blocker de código que a quebrava.
---

# Grupos do Royal

Três grupos Telegram com papéis distintos (informados pelo dono):

- **Grupo REAL / produção:** `-1002556760909` — ÚNICO grupo onde o jogo roda e PONTUA. Cuidado
  redobrado com estado/migração pra não perder progresso.
- **Grupo de TESTE:** `-1004225775299` — poucas pessoas caçam erro; NADA pontua (fora do scoring,
  tipo `TEST_CHAT_IDS`).
- **Grupo PONTE IA (@Mira):** `-1003624946383` (supergrupo, prefixo -100; era `-5204321141` antes de
  virar supergrupo). Só o dono, o bot do jogo e a IA "@Mira". O bot do JOGO é **ADMIN** desse grupo.
  O bridge NÃO guarda dados de jogo → trocar o id é só re-rota de `IA_BRIDGE_CHAT_ID` (sem
  `migrate_chat_data`); `on_chat_migration`/`safe_send` auto-curam envio pro id velho.

# Ponte @Mira — captura da resposta

**Modelo TR3 (decisão do dono, é o que está em produção):** a captura correlaciona pela **resposta
ser um `reply_to` ao NOSSO pedido** (`reply_to_message.message_id == request_mid`), **SEM checar quem
enviou** (id/username/`is_bot`). 1 pedido por vez por grupo-ponte → determinístico. Fail-closed: sem
`request_mid` não captura. `MIRA_USER_ID` virou só diagnóstico/log.
**Por quê:** o dono confirma que esse modelo funciona no repo dele `romastefale/TR3` (auditado: só
aiogram Bot API via webhook, ZERO Telethon/MTProto; captura qualquer reply ao relay sem checar
identidade). Replicamos o mesmo contrato.

**⚠️ BLOCKER REAL ERA CÓDIGO, não config do Telegram.** A resposta da @Mira chega `is_bot=True` e a
outer-middleware `_require_user_for_commands` (`royal/core.py`) dropa TODO `is_bot` ANTES de qualquer
router → a resposta morria antes do capture (sintoma: ZERO `[MIRA] bridge msg` nos logs). **Fix:** o
grupo-ponte está na **allowlist** dessa middleware (depois da allowlist de migração, antes do drop
`is_bot`). O catch-all `track` (system.py) também dropa bot → o router `inteligencia` entra ANTES de
`system`.

**⚠️ `ask_mira` DEVE relayar com `parse_mode=None`.** O default do bot é HTML e os prompts contêm
`<...>` (ex. template do quiz) → senão `TelegramBadRequest: can't parse entities`.

**⚠️ Risco aceito (sender-agnostic):** qualquer conta no grupo-ponte que dê reply ao pedido satisfaz
a captura → **manter o grupo-ponte restrito** (dono + @Mira).

**Custo zero:** a IA roda do LADO da @Mira (conta externa que o dono mantém); o jogo só SOLICITA e
INGERE → nenhuma IA paga dentro do jogo.

## Se DEPOIS deste fix os logs ainda mostrarem ZERO `[MIRA] bridge msg`
A allowlist da middleware já não é mais o gargalo → o próximo suspeito é a regra do Telegram de que um
**bot não recebe msgs de outro bot** via getUpdates ("regardless of mode" — privacy OFF/admin não
ajudam; a exceção reply-à-própria-msg é só p/ USUÁRIOS). Se a @Mira responde como BOT, a entrega ao
bot do jogo seria impossível por qualquer código/config, e a saída real é a @Mira responder de uma
**conta de USUÁRIO** (userbot do lado dela). Isso só se confirma com deploy + olhar os logs; o dono
sustenta que a mesma @Mira funciona no TR3, então a verificação é empírica, não teórica.
