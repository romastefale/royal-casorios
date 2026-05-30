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
- **Grupo PONTE IA (@Mira):** `-1003624946383` (supergrupo, prefixo -100; era `-5204321141`
  grupo básico antes de virar supergrupo) — só o dono, o bot do jogo e a interface de
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
1. `MIRA_USERNAME` agora tem **default `"Mira"`** no código (ponte LIGADA por padrão) → não
   precisa mais setar no Railway. Setar `MIRA_USERNAME=""` (vazio explícito) desliga.
2. Único passo externo restante: **Bot-to-Bot Mode** LIGADO no @BotFather pro bot do jogo —
   se estiver OFF o pedido sai mas a resposta da @Mira nunca chega → **"A geração falhou"**.
   → Fix é **config externa** (@BotFather), NÃO código. O código está correto.

🟢 **TR3 (repo `romastefale/TR3`) é a PROVA, não a contradição:** o dono apontou o TR3
("ele faz") como exemplo de bot que lê a Mira. Li `app/bot/tigraoresponde.py` +
`app/main.py`: o TR3 só faz relay (`Mira, <pergunta>`) + captura o **reply** no chat-alvo,
**sem checar `is_bot`** — não há mágica de código. O dono confirmou que a Mira do TR3 é um
**BOT do @BotFather**. Logo, a ÚNICA razão de o TR3 funcionar é o **Bot-to-Bot Mode LIGADO
no bot receptor do TR3**. Mesmo código, config diferente → o bot do Royal precisa do mesmo
toggle. (Webhook vs polling do TR3 é irrelevante: o filtro bot↔bot é server-side, vale pros
dois.) NÃO tentar "copiar o código do TR3" pra resolver — o que falta é config (@BotFather +
`MIRA_USERNAME`).

✅ **Regra do dono (custo zero):** a IA roda do LADO da @Mira (bot externo que o dono
mantém); o jogo só SOLICITA e INGERE → nenhuma IA paga dentro do jogo.

⚠️ O grupo-ponte VIROU supergrupo: id atual `-1003624946383` (prefixo -100); o antigo
`-5204321141` (grupo básico) ficou órfão. O bridge NÃO guarda dados de jogo, então a troca
é só re-rota do `IA_BRIDGE_CHAT_ID` (sem `migrate_chat_data`); `on_chat_migration`/`safe_send`
auto-curam se sobrar envio pro id velho.
