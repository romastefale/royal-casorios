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

🔴 **CAUSA-RAIZ DEFINITIVA (provada por export do cliente Telegram, ambos os lados): a @Mira é
um BOT, não um userbot.** O export das duas msgs do grupo-ponte mostra `botInfo` no autor TANTO
do relay (`RoyalRPGbot` id `8846613568`) QUANTO da resposta (`Mira` id `8377231659` =
`MIRA_USER_ID`, username `mira`, `subscriberCount` ~1.05M → é um BOT público de IA). Ou seja:
**bot↔bot.** Regra ABSOLUTA do Telegram (core.telegram.org Bots FAQ): *"bots will not be able
to see messages from other bots regardless of mode"* — um bot NUNCA recebe msg de outro bot via
getUpdates. **Não existe** "Bot-to-Bot Communication Mode" público; aquilo era teoria errada.

⛔ **Por isso privacy mode / admin / re-add NÃO resolvem.** Esses levers só afetam msgs de
USUÁRIOS. Como a @Mira é bot, o `RoyalRPGbot` (bot) jamais receberá a resposta dela, com
qualquer config. O relay É enviado (msg 29 ok) e a @Mira responde certo (msg 30, reply à 29,
formato perfeito, id casa) — só que a entrega ao bot do jogo é impossível.

🔴 **Prova por log (deploy do diagnóstico):** durante os 180s de espera do `/rquiz`, **ZERO**
linhas `[MIRA] bridge msg ...` → o bot não recebe NADA → confirma bot↔bot, não é match nem
config. `MIRA_USERNAME` default `"Mira"` (ponte ligada); `""` desliga.

✅ **ÚNICAS soluções reais (bridge bot↔bot é impossível):**
1. **Userbot relay (fix correto, custo zero):** uma CONTA DE USUÁRIO (Telethon/Pyrogram) no
   grupo-ponte — usuário PODE ler msg de bot. Ela manda o prompt à @Mira, lê a resposta e
   alimenta o jogo (mesmo processo Railway → chama os ingestores direto, ou via SQLite/HTTP).
   Precisa: `API_ID`+`API_HASH` (my.telegram.org, grátis) + session de login por telefone.
2. **Só fallback:** `/rquiz`/palavras usam as listas fixas; sem IA dinâmica, zero infra nova.
3. **Trocar a @Mira por uma IA rodando em conta de USUÁRIO** (aí o bot leria com privacy
   OFF+admin+re-add) — mas a @Mira atual é bot público, não dá.

⚠️ **TR3 (repo `romastefale/TR3`) — reinterpretar:** se o TR3 "funciona", é porque o lado que
CAPTURA lá é um USERBOT (user lê bot), OU os dois lados não são ambos bots. Bot↔bot puro não
funciona em lugar nenhum. Não copiar do TR3 a ideia de "bot lê bot".

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
