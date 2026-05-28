# Royal Casorios → RPG - Royal para Geeks

> **Nome oficial do jogo do bot:** **RPG - Royal para Geeks** (usar este nome em UI/comunicação pública daqui pra frente).


Telegram bot construído com aiogram (Python 3.12). Roda como worker (sem frontend), persistindo dados em SQLite local.

**Deploy:** Railway (config em `railway.json`). No Replit fazemos apenas o código — não é necessário rodar/configurar workflow aqui.

### Push para o repositório (Railway escuta a branch `royalRPG`)
**Use `GITHUB_TOKEN`** (auto-provisionado pelo Replit nesta workspace, sempre disponível). **NÃO** use `GH_TOKEN` — não existe como secret aqui.

```bash
git push "https://x-access-token:${GITHUB_TOKEN}@github.com/romastefale/royal-casorios.git" main:royalRPG
```

A branch `main` LOCAL nunca é alterada no remoto — sempre fazemos `main:royalRPG` para o Railway pegar o deploy.

## Estrutura
- `main.py` — código principal do bot
- `royal_render.py` — gerador de cards 1080×1080 (Pillow puro, sem Chromium)
- `royal_words.py` — palavras e charadas pro mini-game
- `requirements.txt` — dependências (`aiogram`, `Pillow`)
- Requer secret `BOT_TOKEN`

## 💾 Persistência de dados — CRÍTICO (Railway)

**Container do Railway é efêmero.** A cada `git push` que dispara
redeploy, o filesystem é recriado a partir do snapshot do repo. Se o
SQLite mora em `./data/royal_casorios.sqlite3` (path RELATIVO), o
arquivo é restaurado pro estado commitado e **TODOS os dados gravados
em runtime são perdidos** (perfis, royal_id, XP, casórios, inventário,
saldo, palavras, configs por usuário, etc).

**Solução obrigatória — Railway Volumes:**

1. No dashboard do Railway → serviço do bot → aba **Volumes** → **+ New
   Volume**.
2. Mount path: `/data`. Size: 1 GB (sobra muito; SQLite cresce devagar).
3. Aba **Variables** → adicionar:
   ```
   DATABASE_PATH=/data/royal_casorios.sqlite3
   ```
4. Redeploy. Na 1ª boot, `_ensure_db_persistence()` em `main.py`
   detecta `/data/royal_casorios.sqlite3` vazio e **copia o seed**
   do repo (`./data/royal_casorios.sqlite3` baked no build) pra
   dentro do volume — uma única vez. A partir daí o volume persiste
   independente de redeploys.

**Como confirmar que tá funcionando:**
- Logs da 1ª boot pós-volume: `[DB] BOOTSTRAP: copiei seed do repo ...`
- Boots seguintes: `[DB] usando /data/royal_casorios.sqlite3 (XXX KB)`
- Se aparecer `[DB] !! ATENCAO: DATABASE_PATH eh relativo ...` →
  **volume não está montado / env var não foi setada** → corrigir antes
  de qualquer outro deploy.

> ⚠️ Enquanto não configurar o volume, **não faça commits novos do
> `data/royal_casorios.sqlite3`** — cada commit do DB sobrescreve o
> snapshot que é "restaurado" em redeploy. O `.gitignore` já ignora
> `data/` e `*.sqlite3` daqui pra frente; o arquivo atualmente trackado
> precisa ser removido com `git rm --cached data/royal_casorios.sqlite3`
> (ação destrutiva — pedir ao usuário ou rodar via project task).

## 🔇 `/royalmudo` — master switch via DM do owner

- **Em GRUPO** (owner-only): toggle do chat atual (ON/OFF).
- **Em DM do owner**: **broadcast** — se qualquer grupo estiver ON-AIR, silencia TODOS os grupos do `chats_rpg`. Se todos já estiverem mudos, religa TODOS.
- Útil pra silenciar tudo de uma vez antes de deploy/manutenção sem precisar entrar em cada grupo.
- Log: `[MUDO] BROADCAST actor=<uid> new_state=ON|OFF total=N changed=M`.

## 📜 Log dump — `/royallog` + auto 5min

- **Ring buffer in-memory:** `_LogRingBuffer` (maxlen 5000) anexado ao root logger em `main.py:107` — captura logs do bot + aiogram.
- **`/royallog`** (owner-only, off-menu, qualquer chat): manda snapshot agora pro DM do owner como `.log` file + atualiza gist (se `GH_TOKEN` setado). Ack auto-deletado em 12s no grupo.
- **Job automático `log_dump_job()`** roda a cada `LOG_DUMP_INTERVAL_SEC` (default 300s = 5min):
  - Envia file pro DM do `OWNER_USER_ID` (silent, sem notificação)
  - Faz PATCH no secret gist (cria 1x na 1ª chamada, salva `gist_id` em `bot_meta`)
- **Gist é secret** (`public: false`) — só com URL acessível, não indexável.
- Cap de 500KB no conteúdo enviado pro gist (limite seguro abaixo dos 1MB).
- Failsafe: DM falha (bot bloqueado / sem DM iniciada) → loga + segue. Gist sem token → no-op silencioso.

**Como ativar gist no Railway:** Variables → adicionar `GH_TOKEN` com PAT do GitHub (scope: `gist`). Sem isso, só DM funciona.

**Como desligar tudo:** `LOG_DUMP_ENABLED=0` no Railway.

## 💎 M01 — Telegram Stars (Premium)

**Pagamento via Telegram Stars (XTR)** — sem gateway externo, sem provider_token. Telegram processa diretamente.

**Acesso:** `/royalloja` → botão **💎 Premium**.

**Itens premium (`PREMIUM_ITEMS` em `main.py`):**
| Item | Stars | Perk armazenado em `players` |
|---|---|---|
| ⚡ Boost +20% XP (24h) | 50⭐ | `xp_boost_until` (ISO timestamp) |
| 💡 Dica da Palavra | 1⭐ | `prm_hints` (contador) |
| 🔱 Ressurreicao no Boss | 10⭐ | `prm_ressurrects` (contador) |
| 🥇 Skin Dourada permanente | 100⭐ | `prm_skin_gold` (0/1) |

**Fluxo técnico:**
1. Callback `r:xtr:{iid}` chama `bot.send_invoice(currency="XTR", prices=[LabeledPrice])`.
2. Handler `pre_checkout_handler` aceita queries com prefixo `prm|`.
3. Handler `successful_payment_handler` valida payload, grava em `stars_purchases` (idempotente por `charge_id`), concede perk via `_grant_premium_perk()`.
4. Ack com efeito 🎉 (DM) ou texto (grupo).

**Migration v10:** adiciona colunas em `players` + tabela `stars_purchases` (ledger auditável).

**Aplicação dos perks (TODO sprints futuros):**
- `xp_boost_until` — multiplicar em hot paths de XP (palavra/boss/casorio/chat) checando `now < xp_boost_until` → `*1.2`.
- `prm_hints` — consumir em `/royalpalavra` revelando 1 letra.
- `prm_ressurrects` — consumir ao morrer no boss.
- `prm_skin_gold` — flag passada pro `render_profile_card` (badge dourado).

> ⚠️ M01 entrega o pipeline de cobrança + grant + ledger. As **aplicações dos perks** nos hot paths serão wired no Sprint 4/5.

## 🌟 M04 — Royal Plus (assinatura Stars recorrente)

**Assinatura mensal via Telegram Stars** — `sendInvoice(subscription_period=2592000)` (30 dias, único valor aceito em XTR). Renovação automática gerenciada pelo Telegram.

**Item (`SUBSCRIPTIONS` em `main.py`):**
| Perk | Stars | Coluna em `players` |
|---|---|---|
| 🌟 Royal Plus — +20% XP + slot extra casório + badge violeta | 50⭐/mês | `royal_plus_until` (ISO), `royal_plus_charge_id` |

**Fluxo:**
1. Callback `r:sub:royal_plus` → `bot.send_invoice(currency="XTR", subscription_period=2592000, prices=[...])`.
2. `pre_checkout_handler` aceita prefixo `sub|` (além de `prm|` do M01), valida amount/item.
3. `successful_payment_handler` detecta `sp.subscription_expiration_date` (Unix), grava ISO em `royal_plus_until`.
4. **Renovações automáticas:** Telegram envia novos `successful_payment` mensalmente com `is_recurring=True` e novo `charge_id` — o handler trata cada um igualzinho (idempotente por charge_id, atualiza `royal_plus_until`).
5. **Cancelamento:** usuário cancela no próprio Telegram (Settings → My Stars → Subscriptions). Bot só observa: quando `now > royal_plus_until` → assinatura expirou → perks param de aplicar.

**Migration v11:** cols `royal_plus_until` + `royal_plus_charge_id` em `players`.

**TODO aplicação dos perks (Sprint 4/5):**
- Checar `now < royal_plus_until` em hot paths de XP (mult 1.2×).
- Permitir 2 casórios ativos se Royal Plus ativo (1 default).
- Render `royal_plus_until > now` como badge violeta no profile card.

## 💡 M03 — Dica paga da PALAVRA

Comando `/royalpaldica` (DM ou grupo). Consome 1 crédito de `prm_hints` (coluna em `players`, populada por compra Stars do item `prm_hint` em M01 — 1⭐ por crédito).

**Fluxo:**
1. Resolve grupo via `resolve_dm_chat`, lê `get_active_challenge`.
2. Decrementa `prm_hints` com `UPDATE … WHERE prm_hints>0` (atomic, evita double-spend).
3. Escolhe posição alfabética aleatória, revela letra.
4. Envia na DM com `EFFECT_FIRE`; fallback `<tg-spoiler>` no chat atual se DM falhar.

Compra de créditos: `/royalloja` → 💎 Premium → 💡 Dica da Palavra.

## 🎁 M02 — Presentes (gifts de florins)

Comando `/royalpresentear` (grupo-only). Transferencia atomica de florins entre players.

**Uso:**
- `/royalpresentear @user 100` — explicito
- Reply na msg do destinatario + `/royalpresentear 100` — implícito

**Limites:** `GIFT_MIN=10`, `GIFT_MAX=5000` (em `main.py`). Bloqueia self-gift, valida saldo, transação atomica com `UPDATE … WHERE gold>=?` pra evitar race.

**Migration v12:** tabela `gifts` (ledger auditavel: from_user, to_user, amount, sent_at).

Conquista `generoso` desbloqueada no 1º presente enviado.

## 🏅 M11 — Conquistas (Achievements)

**11 slugs MVP** em `ACHIEVEMENTS` dict (`main.py`): primeiro_acerto, dez_acertos, cem_acertos, primeiro_boss, lvl_dez/vinte_cinco/cinquenta, primeiro_amor, mecenas, nobreza, generoso.

**API:** `unlock_achievement(chat_id, uid, slug)` — idempotente via PK `(chat_id, user_id, slug)`. Retorna True só na 1ª unlock + dispara notificação DM (`_notify_achievement_dm`) com `EFFECT_PARTY`.

**Triggers wired:**
- `award_xp_immediate` (level-up) → `check_level_achievements` (lvl 10/25/50)
- `attempt_word` (PALAVRA win) → `check_palavra_achievements` (1/10/100, count via challenges WHERE status='won')
- `finalize_boss` → `primeiro_boss` pra cada atacante
- `assign_couple` → `primeiro_amor` pros 2 noivos
- `_grant_premium_perk` → `nobreza` (royal_plus) ou `mecenas` (one-shot)
- `/royalpresentear` → `generoso`

**Comando:** `/royalconquistas` — lista do user no chat ativo, locked com `🔒 <s>tachado</s>`.

**Migration v12:** tabela `achievements` + índice por user_id.

## ⚙️ M19 — Preferências do user (`/royalconfig`)

Flags persistidas em `user_dm_settings.prefs_json` (TEXT JSON; migration v12).

**Flags MVP (`USER_PREFS_DEFAULTS`):**
| key | default | efeito |
|---|---|---|
| `silent_levelup` | False | suprime card de level-up na DM (check em `_schedule_levelup_dm`) |
| `hide_rank` | False | reservado (wiring no Sprint 5 — filtrar do ranking) |
| `palavra_ping` | True | reservado (ping de spawn na DM, futuro) |

**API:** `get_user_prefs(uid)` retorna dict merged com defaults; `set_user_pref(uid, key, val)` upsert.

**Comando:** `/royalconfig` — InlineKeyboard com 1 botão por flag (estilo verde=ON, vermelho=OFF). Callback `r:cfg:{key}` toggla e re-renderiza.

## 🗺️ M05 — Quests diárias (`/royalmissoes`)

4 missões diárias em `DAILY_QUESTS` (`main.py`), reset automático por dia (chave `today_key()`):
| id | evento | target | recompensa |
|---|---|---|---|
| `msgs` 💬 | message | 20 msgs | +60 XP +30🪙 |
| `palavra` 🎯 | palavra_win | 1 acerto | +80 XP +50🪙 |
| `boss` 🐉 | boss_hit | 3 hits | +50 XP +40🪙 |
| `social` 👍 | reaction | 5 reactions | +30 XP +20🪙 |

**API:** `quest_bump(chat_id, uid, event, n=1)` incrementa progresso (cap no target, atomic via `MIN(progress+?, ?)`). Só roda em grupo (`chat_id<0`). `get_quest_state(chat_id, uid)` retorna lista com progress/done/claimed.

**Triggers wired:** `track()` (message) · `attempt_word` win (palavra_win) · `boss_attack` (boss_hit) · `on_message_reaction` (reaction).

**Comando:** `/royalmissoes` (DM/grupo via `resolve_dm_chat`) — barra de progresso + botão **Resgatar** (verde) por missão concluída. Callback `r:quest:{id}` faz claim atômico (`UPDATE … WHERE progress>=target AND claimed=0`), concede XP+gold.

**Migration v13:** tabela `quest_progress` (PK `chat_id,user_id,day,quest_id`).

## 👍 M06 — Reactions = XP

Reagir com emoji a qualquer mensagem de grupo dá `REACTION_XP=3` XP, cap diário `REACTION_XP_DAILY_CAP=10` por user/chat.

**Handler:** `@dp.message_reaction` (`on_message_reaction`) — só premia quando ADICIONA reaction (`len(new) > len(old)`), respeita mute, conta no cap (`reaction_xp_daily`), concede XP via `award_xp_immediate` + bump da quest `social`.

⚠️ **Requer `allowed_updates` com `message_reaction`** — resolvido automaticamente em `main()` via `dp.resolve_used_update_types()` passado pro `start_polling`.

**Migration v13:** tabela `reaction_xp_daily` (PK `chat_id,user_id,day`).

## 🎉 M09 — Eventos sazonais (boost de XP) — `/royalevento`

Boost de XP global por data, sem DB — pura lógica em `SEASONAL_EVENTS` (`main.py`):
| evento | data | mult |
|---|---|---|
| 🎆 Reveillon Real | 31/12–01/01 | 2.0× |
| 🎄 Natal dos Nobres | 24–25/12 | 2.0× |
| 🔥 Festa Junina Real | 23–24/06 | 1.5× |
| 🎃 Noite Sombria | 31/10 | 1.5× |
| ❤️ Dia dos Namorados | 12/06 | 1.5× |
| 🍻 Fim de Semana Real | sáb/dom (fallback) | 1.5× |

**API:** `active_seasonal_event(d=None)` (datas especiais > FDS) · `event_xp_mult()` retorna float. Aplicado em `award_xp_immediate` e `award_xp_message` (depois dos bônus de classe/casamento).

**Comando:** `/royalevento` — mostra evento ativo (ON-AIR/OFFLINE).

## 🔧 Env vars

- `BOT_TOKEN` (obrigatório) — token do BotFather
- `DATABASE_PATH` (opcional, default `./data/royal_casorios.sqlite3`;
  **no Railway use `/data/royal_casorios.sqlite3` com volume mountado —
  ver seção "Persistência de dados" acima**)
- `TZ` (opcional, default `America/Sao_Paulo`)
- `AUTO_HOURS` (opcional, default `9,15,21`)
- **`TEST_CHAT_IDS`** (opcional, comma-separated) — chat_ids de grupos de
  teste. Esses grupos **não** aparecem no picker de DM nem no fallback
  do inline mode. Ex.: `TEST_CHAT_IDS="-1001234567890,-1009876543210"`
- **`OWNER_USER_ID`** (recomendado) — user_id do dono. Habilita `/royallog`, `/royalmudo`, `/royalpalavratest`. Sem isso, comandos owner-only ficam inacessíveis (modo seguro).
- **`GH_TOKEN`** (opcional) — Personal Access Token do GitHub com scope `gist`. Habilita upload automático dos logs pro gist secreto a cada 5min. Sem isso, só DM do owner recebe.
- **`LOG_DUMP_INTERVAL_SEC`** (opcional, default `300`) — intervalo entre dumps automáticos.
- **`LOG_DUMP_ENABLED`** (opcional, default `1`) — `0` desliga o job de auto-dump (mantém `/royallog` manual).
- **`LOG_JSON`** (opcional, default `0`) — `1` faz o stdout root sair em JSON (1 linha por record com `ts`/`lvl`/`logger`/`msg` + extras + `exc`). Ring buffer de `/royallog` **sempre** sai em texto humano (independe da flag). Ativa em Railway pra ingest em Logtail/Better Stack/Loki/etc.
- **`STASH_CHAT_ID`** (opcional, override) — chat_id de canal privado
  pra upload silencioso do **identity card**. **Hardcoded** em
  `main.py` como `-1003941532741` (canal privado só do dono +
  bot). Só seta a env var se precisar trocar o canal.

## 🪪 Identity Card (inline mode)

Cada player tem um **identity card 1080×1080 fixo** (só avatar + ROY#ID
+ nome — sem level/XP/stats). Gerado 1x no `ensure_player` e regenerado
**apenas** quando muda nome ou avatar.

**Triggers de regen:**
- Player novo (em `ensure_player`) → fire-and-forget render.
- Sweep diário em `identity_card_sweep_job()` (~3h local) → compara
  `inline_card_hash` salvo com `_identity_card_hash(name, avatar_slug)`
  atual; regenera onde divergiu. Throttle: 1 upload/s.

**Storage:** colunas `inline_card_file_id` + `inline_card_hash` em
`players` (migration v5). Hash = `sha1(name|avatar_slug)[:16]`.

**Upload silencioso:** `ensure_identity_card_async` faz `send_photo`
pro `STASH_CHAT_ID` com `disable_notification=True`, captura o
`file_id` da resposta e salva no DB. Sem `STASH_CHAT_ID` configurado,
o sweep loga e pula (no-op gracioso).

**Inline mode:** `inline_profile` prefere `inline_card_file_id` do DB
quando existe; mantém também os results antigos (profile card + texto)
como fallback.

## 💬 DM + inline mode

- Os comandos pessoais (`/royalperfil`, `/royalficha`, `/royalranking`,
  `/royalinventario`, `/royalsaldo`, `/royalmeuscasorios`, configs) rodam
  tanto em grupo quanto em DM. Em DM, o bot resolve o "grupo ativo" via
  `user_dm_settings` (migration v4).
- 1 grupo elegível → auto-seleciona. 2+ → mostra picker. Troca via
  `/royalgrupo`.
- **Inline mode** (`@nomedobot` em qualquer chat) envia o card de perfil.
  Usa file_id cacheado em memória populado quando `send_profile_card`
  roda; sem cache, fallback texto + botão "Gerar foto na DM".
- ⚠️ Inline mode **precisa estar habilitado no BotFather**:
  `/setinline` → texto placeholder (ex.: "Enviar meu perfil Royal").
  Sem isso o Telegram não dispara `inline_query`.

## 📋 Menus (BotCommands)

Registrados em `register_bot_commands()` em `main.py`. Telegram atualiza
o autocomplete `/` automaticamente na 1ª inicialização com novo token.

**Grupo** (`BotCommandScopeAllGroupChats`): `royal`, `royalperfil`,
`royalficha`, `royalavatar`, `royalup`, `royalclasse`, `royalinventario`,
`royalloja`, `royalsaldo`, `royalranking`, `royalpalavra`, `royalboss`,
`royalcasorios`, `royalmeuscasorios`, `royalencalhar`, `royaldesencalhar`,
`royalcasar` (admin), `royalativar` (admin), `royaltutorial`, `royalajuda`.

**DM** (`BotCommandScopeAllPrivateChats`): `royal`, `royalperfil`,
`royalavatar`, `royalficha`, `royalranking`, `royalinventario`,
`royalsaldo`, `royalmeuscasorios`, `royalgrupo`, `royaltutorial`,
`royalajuda`, `royalprivacidade`, `royaldados`.

> 💡 Se você trocou o `BOT_TOKEN` e o menu antigo persiste, basta reiniciar
> o bot uma vez — `register_bot_commands()` reescreve os dois scopes.

## User preferences
- **Versões fixas (regra do usuário):**
  - **Telegram Bot API: 10** — usar sempre, não fazer downgrade
  - **aiogram: 3.28.2** (última estável, lançada em 10/05/2026)
- **Sincronia obrigatória com o código:** sempre que mexer no bot, **revisar e atualizar quando aplicável**:
  - `/start` (handler `start_cmd` em `main.py` ~3958) — lista de rotas pessoais e inline mode
  - `/royalajuda` (constante `ROYAL_HELP` em `main.py` ~4059) — manual completo de comandos
  - `/royaltutorial` (`tutorial_block()`) — explicação do gameplay
  - `replit.md` — seções de env vars, persistência, doutrina visual, menus
  - `register_bot_commands()` (BotCommands do menu `/`) — se adicionou comando novo
  Se a mudança for puramente infra interna (cache, retry, log format), só atualizar `replit.md` (e mencionar no commit que /start/help/tutorial foram auditados e não precisaram mudar).

## 🎛️ UI: botões coloridos (Bot API 10) + helpers de UX

> 🧠 **MEMÓRIA PERMANENTE (não esquecer / não reverter):** Botões coloridos nativos EXISTEM no Bot API 10 / aiogram 3.28.2 via campo `style`. Já implementado e em produção. Se algum agente futuro afirmar "não existe", está errado — consultar `https://docs.aiogram.dev/en/latest/api/enums/button_style.html`.

### ✅ Bot API 10 SUPORTA botão colorido nativo
`InlineKeyboardButton` e `KeyboardButton` têm o campo `style=` (aiogram 3.28.2):
- `'success'` → **verde**
- `'danger'` → **vermelho**
- `'primary'` → **azul**
- omitido → cor padrão do tema do cliente

Doc: https://docs.aiogram.dev/en/latest/api/enums/button_style.html

⚠️ **Não existe `'warning'`/amarelo nativo** — pra essa categoria usamos só emoji ⚠️.

### Convenção (constantes em `main.py:575-584` — usar SEMPRE em par emoji+style)

| Constante emoji | Constante style | Cor | Uso |
|---|---|---|---|
| `BTN_OK` ✅ | `STYLE_OK` `"success"` | verde | confirmar / aplicar / ir |
| `BTN_NO` ❌ | `STYLE_NO` `"danger"` | vermelho | cancelar / fechar / destrutivo |
| `BTN_INFO` 🔵 | `STYLE_INFO` `"primary"` | azul | informação / navegar |
| `BTN_WARN` ⚠️ | — | (só emoji) | atenção / reversível com custo |
| `BTN_BACK` ◀️ | — | neutro | voltar |
| `BTN_GO` ▶️ | — | neutro | avançar |

### Helper `ikb()` em `main.py:587`
```python
ikb(f"{BTN_OK} Confirmar", callback_data="x", style=STYLE_OK)
```
Atalho que aceita `style=` opcional. Emoji líder mantido como fallback pra clientes antigos + acessibilidade (leitores de tela, modo monocromo).

### Helpers de UX em `main.py` (usar SEMPRE quando aplicável)
- `await auto_delete_after(msg, delay=8.0)` — agenda exclusão automática. Ideal para acks efêmeros (rate-limit, "sem pontos", warnings) que poluem chat de grupo.
- `await react_to(chat_id, message_id, emoji)` — bot reage com emoji ao comando do user (Bot API 7.0+). Feedback instantâneo antes da resposta completa renderizar. Whitelist do Telegram aplica.
- `await type_then_send(chat_id, text, delay=1.2, action="typing")` — mostra "... digitando" por N segundos antes de enviar. Para fotos: `action="upload_photo"`.
- `await safe_typing(chat_id, action)` — só dispara chat action, sem delay.
- `**effect_kw(chat.type, EFFECT_*)` — sparkles/fire/heart em DM 1:1 (Bot API 7.7).

## 🎨 Identidade visual — Retro-futurist dystopian

Tudo que o bot envia segue essa linguagem (texto **e** imagens).

### Conceito
- **8-bit retrô** (terminal CRT velho, monospace, ASCII art)
- **Futurista** (`>>`, `//`, `[BRACKETS]`, `ID#0042`, `v0.1.ALPHA`)
- **Dark/dystopian** (paleta sempre escura, cores ácidas como acento, sem brilho moderno)
- **Cores variando por player** — cada `royal_id` recebe uma paleta consistente (TOXIC, AMBER, PLASMA, ARCTIC, BIOLAB, BLOOD)

### Quando gerar imagem (caro/lento) vs caption (rápido)
- **Card 1080×1080:** só pra momentos de destaque (perfil, ranking pódio, casório, boss derrotado).
- **Caption / texto puro:** padrão pra TODO o resto. Usar `term_block()` e `term_pre()` (em `main.py`).
- Cards usam `asyncio.to_thread(render_xxx_card, ...)` pra não travar o loop.

### Voz padrão (texto puro)
Use os helpers em `main.py`:
- `term_block(title, body, status="OK", status_color="ACID|HOT|AMBER|CYAN", stamp=None)` — header `> TITLE.SYS // STATUS` + corpo + stamp opcional
- `term_pre(rows)` — tabela monospace `KEY :: VALUE` em `<pre>`

Convenções de tom:
- Título em CAPS, formato `PALAVRA.SYS`
- Prefixos: `>` (saída do terminal), `>>` (sub-comando), `//` (comentário), `!!` (alerta)
- Status: `OK`, `CONECTADO`, `CONFIG`, `ALERTA`, `OFFLINE`, etc.
- Stamp final em itálico entre `[ ... ]`

### Formatação HTML do Telegram (usar TUDO)
`<b>` `<i>` `<u>` `<s>` `<code>` `<pre>` `<a>` `<blockquote>` `<blockquote expandable>` `<tg-spoiler>`

Caption de foto tem **limite de 1024 chars** — sempre incluir guarda no código.

### Efeitos de mensagem (Bot API 7.7)
Helper `effect_kw(chat_type, EFFECT_*)` em `main.py`:
- `EFFECT_PARTY` 🎉 boas-vindas, ganhar Palavra
- `EFFECT_FIRE` 🔥 dano crítico, golpe final no boss
- `EFFECT_HEART` ❤️ casório
- `EFFECT_THUMBS_UP`, `EFFECT_POO`, `EFFECT_THUMBS_DOWN`

⚠️ Só funciona em **DM 1:1** — sempre passar `**effect_kw(message.chat.type, ID)`.

### Chat actions
Helper `safe_typing(chat_id, "typing" | "upload_photo")` — não exige admin, falha silenciosamente.

### Paleta dystopian (`royal_render.py`)
- `BG_DEEP` `#0C0A0E` — vazio
- `BG` `#16121A` — painel
- `INK` `#D2C4A8` — texto bone
- `DIM` `#706054` — texto secundário
- Acentos rotativos: `HOT` (sangue), `ACID` (toxic), `CYAN` (holograma), `GOLD`, `RUST`, `PURPLE`, `AMBER`, `MAGENTA`, `NEON_BLUE`, `JADE`

### Pós-processamento de cards
Sempre aplicar:
1. `apply_scanlines(img, every=3, alpha=55)` — CRT
2. `apply_vignette(img, strength=160)` — distopia
3. `apply_grain(img, intensity=18)` — TV velha
