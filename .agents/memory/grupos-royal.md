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

⚠️ **O relay pra @Mira (`ask_mira`) DEVE ir com `parse_mode=None`.** O default do bot é HTML e
os prompts máquina→máquina contêm `<...>` (ex. template do quiz `<pergunta>`/`<alternativa>`) →
sem isso o Telegram rejeita com `TelegramBadRequest: can't parse entities: Unsupported start tag`.

🟢 **CORREÇÃO DEFINITIVA (o dono confirmou): a @Mira é uma conta de USUÁRIO (userbot),
NÃO um bot do @BotFather.** Isso vira o diagnóstico de cabeça: passamos sessões focados em
"Bot-to-Bot Communication Mode" — que é o lever ERRADO. Bot-to-Bot Mode só importa quando os
DOIS lados são bots. Como a @Mira é USER, o que gateia a entrega das msgs dela ao bot do jogo
é o **PRIVACY MODE** (regra oficial Telegram, core.telegram.org Bots FAQ/Features).

⚠️ **Regra oficial Telegram (verificada por pesquisa, mai/2026):**
- Bot com **privacy mode ON** (default) só recebe no grupo: comandos a ele, replies às
  PRÓPRIAS msgs dele, service msgs, e msgs de chat privado. **NÃO recebe** msgs normais de
  usuários.
- Bot **admin** OU bot com **privacy mode OFF** recebe TODAS as msgs de usuários (menos msgs
  de OUTROS bots — bot↔bot é proibido salvo Bot-to-Bot Mode).
- ⚠️ **Mudar privacy/admin SÓ vale depois de REMOVER + RE-ADICIONAR o bot ao grupo existente**
  (o grupo cacheia o estado antigo). Provável causa de continuar mudo mesmo "sendo admin": o
  grupo virou supergrupo e o estado efetivo não atualizou → re-adicionar resolve.

✅ **FIX (ação do dono, garantido pq a @Mira é user):** @BotFather → `/setprivacy` →
@RoyalRPGbot → **Disable**; depois **remover e re-adicionar** o bot ao grupo-ponte (e manter
como admin). Aí o bot passa a RECEBER as msgs da @Mira → captura funciona. Alternativa que
dribla privacy sem mexer em nada: fazer a @Mira **REPLICAR (reply_to)** a msg do relay do bot
(reply à própria msg do bot SEMPRE é entregue, mesmo com privacy ON) — é o que o TR3 faz.

🔴 **Prova por log (produção, deploy do diagnóstico):** durante os 180s de espera do `/rquiz`,
**ZERO** linhas `[MIRA] bridge msg ...` aparecem → o bot não recebe NADA do grupo-ponte →
problema é ENTREGA (privacy), não match. `MIRA_USERNAME` default `"Mira"` no código (ponte
ligada); `MIRA_USERNAME=""` desliga.

🟢 **TR3 (repo `romastefale/TR3`) reinterpretado:** o TR3 faz relay + captura o **reply** no
chat-alvo. Como replies à própria msg do bot são entregues MESMO com privacy ON, o TR3
funciona pq a Mira dele **responde com reply_to** — não por nenhum modo especial. Caminho
análogo p/ o Royal: privacy OFF + re-add, OU garantir reply_to da @Mira.

✅ **Regra do dono (custo zero):** a IA roda do LADO da @Mira (bot externo que o dono
mantém); o jogo só SOLICITA e INGERE → nenhuma IA paga dentro do jogo.

⚠️ O grupo-ponte VIROU supergrupo: id atual `-1003624946383` (prefixo -100); o antigo
`-5204321141` (grupo básico) ficou órfão. O bridge NÃO guarda dados de jogo, então a troca
é só re-rota do `IA_BRIDGE_CHAT_ID` (sem `migrate_chat_data`); `on_chat_migration`/`safe_send`
auto-curam se sobrar envio pro id velho.

🔬 **Diagnóstico de captura (relay já funciona mas `/rquiz` dá timeout):** se a @Mira RESPONDE
visível no grupo-ponte mas o jogo loga `[MIRA] timeout`, o jogo não capturou. Desambiguar pelos
logs `[MIRA] bridge msg ...` durante a espera: (A) **ZERO linhas** = o bot não RECEBE → é
ENTREGA (privacy mode — ver fix acima: privacy OFF + re-add; a @Mira é USER); (B) linha COM
`id=` divergente de `MIRA_USER_ID` = match rejeita → ajustar env `MIRA_USER_ID`. **Regra
durável:** o discriminador da @Mira é id/username, NUNCA `is_bot` (ela vem `is_bot=False`); a
captura não filtra `is_bot`. A captura do grupo-ponte só CONSOME (para propagação) quando casa
a @Mira; senão deixa passar (SkipHandler) p/ não engolir comandos do dono no grupo-ponte.
