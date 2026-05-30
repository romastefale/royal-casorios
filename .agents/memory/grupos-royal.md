---
name: Grupos do Royal (chat IDs e papéis)
description: Os três grupos Telegram do bot e o papel de cada um — qual pontua, qual é teste, e o grupo-ponte da IA @Mira.
---

# Grupos do Royal

Três grupos Telegram com papéis distintos (informados pelo dono):

- **Grupo REAL / produção do jogo:** `-1002556760909` — ÚNICO grupo onde o jogo roda e
  PONTUA. Cuidado redobrado com estado/migração pra não perder progresso.
- **Grupo de TESTE:** `-1004225775299` — poucas pessoas testam só pra caçar erro; NADA
  pontua aqui (deve ficar fora do scoring, tipo `TEST_CHAT_IDS`).
- **Grupo PONTE IA (@Mira):** `-5204321141` — só o dono, o bot do jogo e a interface de
  IA "@Mira". Base do módulo novo **"inteligência royal"**.

**@Mira / inteligência royal:** o jogo manda `@Mira <pedido>` no grupo-ponte; a @Mira
responde e o jogo captura/ingere a resposta. Implementado em `royal/mira.py` (ponte) +
`royal/handlers/inteligencia.py` (captura) + `mira_palavras_job` (Objetivo 1: palavras do
dia → banco dinâmico `palavra_pool`). Config: `MIRA_USERNAME`/`IA_BRIDGE_CHAT_ID`.

✅ **Bot-to-bot FUNCIONA** (a antiga nota "bot não lê bot" estava errada): com o
**"Bot-to-Bot Communication Mode" LIGADO no @BotFather** (ação do dono) o jogo RECEBE as
mensagens da @Mira. Sem esse modo, o Telegram não entrega → ponte fica muda. O catch-all
`track` descarta `is_bot`, então a captura precisa de router ANTES de `system` (já feito).

✅ **Regra do dono (custo zero):** a IA roda do LADO da @Mira (bot externo que o dono
mantém); o jogo só SOLICITA e INGERE → nenhuma IA paga dentro do jogo.

⚠️ `-5204321141` não tem o prefixo `-100` dos supergrupos — provável grupo básico (legado).
Se virar supergrupo, o chat_id muda (migração já tratada por `on_chat_migration`).
