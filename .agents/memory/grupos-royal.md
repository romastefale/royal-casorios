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

📌 **NOTA DO DONO (nunca esquecer):** o bot do JOGO é **ADMIN** do grupo-ponte onde está com a @Mira.

⚠️ **Ser admin NÃO resolve bot-to-bot.** Regra OFICIAL do Telegram (Bots FAQ): um bot
**não recebe** mensagens de OUTRO bot num grupo — *mesmo sendo admin e com privacy mode
OFF*. Admin/privacy é irrelevante pra isso. O ÚNICO mecanismo é o **"Bot-to-Bot
Communication Mode"** (Bot API **10.0**, lançado 7-mai-2026) ligado no **@BotFather
(MiniApp)**.

✅ **Bot-to-bot FUNCIONA em GRUPO** com o modo ligado: pra o jogo RECEBER as msgs da @Mira
no grupo-ponte, o bot do JOGO (o receptor) precisa ter o **Bot-to-Bot Communication Mode
LIGADO**. Sem isso, o Telegram não entrega → ponte muda → `/rquiz` dá timeout ("A geração
falhou"). O catch-all `track` descarta `is_bot`, então a captura precisa de router ANTES de
`system` (já feito — order `inteligencia, quiz, system`).

🔴 **Cadeia de falha do `/rquiz` (diagnóstico real via logs de produção):**
1. `MIRA_USERNAME` **não setado** no Railway → `MIRA_ENABLED=False` → `/rquiz` responde
   **"quiz indisponível"** (log: `[MIRA] ponte desligada (sem MIRA_USERNAME/IA_BRIDGE_CHAT_ID)`).
2. Mesmo com `MIRA_USERNAME` setado: se o **Bot-to-Bot Mode** estiver OFF no bot do jogo,
   o pedido sai mas a resposta da @Mira nunca chega → **"A geração falhou"**.
   → Fix é **config externa** (Railway env + @BotFather), NÃO código. O código está correto.

✅ **Regra do dono (custo zero):** a IA roda do LADO da @Mira (bot externo que o dono
mantém); o jogo só SOLICITA e INGERE → nenhuma IA paga dentro do jogo.

⚠️ `-5204321141` não tem o prefixo `-100` dos supergrupos — provável grupo básico (legado).
Se virar supergrupo, o chat_id muda (migração já tratada por `on_chat_migration`).
