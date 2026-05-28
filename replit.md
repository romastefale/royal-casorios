# RPG - Royal para Geeks

> **Nome oficial do jogo:** **RPG - Royal para Geeks** (usar em UI/comunicação pública).
> Antigo nome interno: "Royal Casorios".

Telegram bot em **aiogram 3.28.2 / Bot API 10 / Python 3.12**. Roda como **worker** (sem
frontend), persistindo em **SQLite local**. No Replit fazemos **apenas o código** — não
é necessário rodar/configurar workflow aqui; o deploy é no Railway.

## 📑 Índice
1. [Deploy & push](#-deploy--push-crítico) · CRÍTICO
2. [Persistência de dados](#-persistência-de-dados-crítico) · CRÍTICO
3. [User preferences (regras do dono)](#-user-preferences-regras-do-dono)
4. [Estrutura de arquivos](#-estrutura-de-arquivos)
5. [Env vars](#-env-vars)
6. [Testes & CI](#-testes--ci)
7. [Features de jogo (M-series)](#-features-de-jogo-m-series)
8. [Premium · Telegram Stars](#-premium--telegram-stars-m01--m04--m03)
9. [Infra hardening (F-series)](#️-infra-hardening-f-series)
10. [DM, inline mode & identity card](#-dm-inline-mode--identity-card)
11. [Menus (BotCommands)](#-menus-botcommands)
12. [UI: botões coloridos + helpers UX](#️-ui-botões-coloridos-bot-api-10--helpers-ux)
13. [Identidade visual](#-identidade-visual--retro-futurist-dystopian)

---

## 🚀 Deploy & push (CRÍTICO)

**Deploy:** Railway (config em `railway.json`). O Railway escuta a branch **`royalRPG`**.

**Push (sempre `GITHUB_TOKEN`, NUNCA `GH_TOKEN`):**
```bash
git push "https://x-access-token:${GITHUB_TOKEN}@github.com/romastefale/royal-casorios.git" main:royalRPG
```
- `GITHUB_TOKEN` é auto-provisionado pelo Replit nesta workspace, sempre disponível.
- `GH_TOKEN` **não existe** como secret aqui (é só env var de runtime do Railway, p/ gist).
- A branch `main` LOCAL **nunca** é alterada no remoto — sempre `main:royalRPG`.

---

## 💾 Persistência de dados (CRÍTICO)

**O container do Railway é efêmero.** A cada `git push` que dispara redeploy o filesystem
é recriado do snapshot do repo. Se o SQLite morar em path RELATIVO
(`./data/royal_casorios.sqlite3`), o arquivo volta ao estado commitado e **TODOS os dados
de runtime são perdidos** (perfis, royal_id, XP, casórios, inventário, saldo, etc).

**Solução obrigatória — Railway Volumes:**
1. Dashboard Railway → serviço do bot → aba **Volumes** → **+ New Volume**.
2. Mount path `/data`, size 1 GB (sobra).
3. Aba **Variables** → `DATABASE_PATH=/data/royal_casorios.sqlite3`.
4. Redeploy. Na 1ª boot, `_ensure_db_persistence()` (`main.py`) detecta `/data/...` vazio e
   **copia o seed** do repo pro volume — uma única vez. Daí em diante o volume persiste.

**Confirmar que funciona (logs):**
- 1ª boot pós-volume: `[DB] BOOTSTRAP: copiei seed do repo ...`
- Boots seguintes: `[DB] usando /data/royal_casorios.sqlite3 (XXX KB)`
- ⚠️ `[DB] !! ATENCAO: DATABASE_PATH eh relativo ...` → volume não montado / env var não
  setada → **corrigir antes de qualquer deploy.**

> ⚠️ Enquanto não houver volume, **não commitar** `data/royal_casorios.sqlite3` — cada
> commit do DB sobrescreve o snapshot "restaurado" no redeploy. O `.gitignore` já ignora
> `data/` e `*.sqlite3`; o arquivo trackado precisa de `git rm --cached
> data/royal_casorios.sqlite3` (destrutivo — pedir ao usuário ou rodar via project task).

---

## 📌 User preferences (regras do dono)

**Versões fixas (não fazer downgrade):**
- **Telegram Bot API: 10** — usar sempre.
- **aiogram: 3.28.2** (última estável, 10/05/2026).

**Sincronia obrigatória com o código** — sempre que mexer no bot, revisar/atualizar quando
aplicável:
- `/start` (`start_cmd`) — rotas pessoais + inline mode.
- `/royalajuda` (`ROYAL_HELP`) — manual completo de comandos.
- `/royaltutorial` (`send_tutorial()` + `ROYAL_TUTORIAL_PARTS`) — tutorial didático em
  **6 partes** (cada parte = 1 mensagem, ≤4096 chars). `send_tutorial(message)` envia todas
  em sequência; usado por `/start`, `/royaltutorial`, `/royalajuda`, `/help` e o botão
  "📖 Tutorial".
- `register_bot_commands()` — se adicionou comando novo.
- `replit.md` — env vars, persistência, doutrina visual, menus.
- `README.md` — overview público.

> Se a mudança for **puramente infra interna** (cache, retry, log format), só atualizar
> `replit.md` e mencionar no commit que /start/help/tutorial foram auditados e não mudaram.

**💸 Custo zero — NUNCA usar funções/serviços pagos:**
- Não usar nenhuma API/serviço/feature que cobre dinheiro (gateways pagos, APIs com
  cobrança por uso, geração de mídia paga, etc). Só recursos gratuitos.
- Telegram Stars (XTR) é exceção legítima: é **receita** do bot (usuário paga o bot),
  não custo nosso, e o Telegram processa direto sem gateway externo.

**🎬 UX no grupo — animação interativa, sem poluir (regra do dono):**
- **Comandos de escolha** (inline keyboard / menus): depois que o usuário seleciona e o ato
  conclui, **editar ou apagar a mensagem antiga** — nunca empilhar mensagem nova por cima.
  Usar `edit_message_text` / `edit_message_reply_markup` / `delete_message` (ou o helper
  `auto_delete_after` p/ acks efêmeros).
- Preferir **animação interativa via edição da MESMA mensagem** (atualizar texto/markup
  in-place conforme o fluxo avança) em vez de mandar várias mensagens em sequência.
- O fluxo deve se **resolver/concluir sozinho** in-place conforme o ato termina — uma
  interação = idealmente uma mensagem que evolui, não um rastro de mensagens.
- **Não floodar o grupo.** Menos mensagens, mais edição in-place.

> ⚠️ **Privacidade:** `attached_assets/` tem arquivos trackados no git (só
> `generated_images/` é ignorado) e vai pro GitHub no push `main:royalRPG`. **Nunca**
> commitar prints de conversa / dumps / logs com PII (telefones, user ids). Se precisar de
> assets só locais, adicionar ao `.gitignore` ou `git rm --cached` (destrutivo — via project
> task ou pedir ao usuário).

---

## 📁 Estrutura de arquivos
- `main.py` — código principal do bot.
- `royal_render.py` — gerador de cards 1080×1080 (Pillow puro, sem Chromium).
- `royal_words.py` — palavras e charadas do mini-game.
- `requirements.txt` — deps de runtime (`aiogram`, `Pillow`, `aiohttp`).
- `requirements-dev.txt` — deps SÓ de dev/CI (`pytest`, `ruff`); não vão pro Railway.
- `tests/` — pytest: `conftest.py` (DB tmp), `test_pure.py`, `test_db_smoke.py`.
- `.github/workflows/ci.yml` — CI: ruff crítico + pytest em push/PR.

---

## 🔧 Env vars

| Var | Obrig.? | Default | Para que serve |
|---|---|---|---|
| `BOT_TOKEN` | ✅ | — | Token do BotFather. |
| `DATABASE_PATH` | rec. | `./data/royal_casorios.sqlite3` | **No Railway use `/data/...` com volume** (ver Persistência). |
| `TZ` | — | `America/Sao_Paulo` | Timezone. |
| `AUTO_HOURS` | — | `9,15,21` | Horas dos casórios automáticos. |
| `OWNER_USER_ID` | rec. | — | user_id do dono. Habilita `/royallog`, `/royalmudo`, `/royalpalavratest`. Sem isso, comandos owner ficam off (modo seguro). |
| `TEST_CHAT_IDS` | — | — | chat_ids de grupos de teste (não aparecem no picker de DM nem no fallback inline). Comma-separated. |
| `STASH_CHAT_ID` | — | `-1003941532741` (hardcoded) | Canal privado p/ upload silencioso de identity card + backups. |
| `GH_TOKEN` | — | — | PAT GitHub (scope `gist`). Habilita upload de logs pro gist secreto a cada 5min. Sem isso, só DM do owner. |
| `LOG_DUMP_INTERVAL_SEC` | — | `300` | Intervalo do auto-dump de logs. |
| `LOG_DUMP_ENABLED` | — | `1` | `0` desliga o auto-dump (mantém `/royallog` manual). |
| `LOG_JSON` | — | `0` | `1` faz stdout root sair em JSON (1 linha/record). Ring buffer do `/royallog` sempre em texto humano. |
| `PORT` | — | — | Se setado, sobe health server HTTP (`GET /health`). Railway injeta com healthcheck. |
| `HEALTH_STALE_SEC` | — | `600` | `/health` → 503 se nenhuma update do Telegram nesse intervalo. |
| `BACKUP_ENABLED` | — | `1` | `0` desliga backup diário. |
| `BACKUP_HOUR` | — | `12` | Hora local do backup diário (meio-dia). |
| `BACKUP_RETENTION_DAYS` | — | `7` | Dias de backup mantidos em `<DB_DIR>/backups/`. |

---

## 🧪 Testes & CI

- **Rodar local:** `pip install -r requirements-dev.txt && python -m pytest -q`.
- **`conftest.py`** aponta `DATABASE_PATH` p/ arquivo temporário ANTES de importar `main` →
  migrations rodam em DB vazio descartável (nunca toca o DB real).
- **`test_pure.py`** — funções puras (`format_br`, `normalize_word`, `normalize_pair`,
  `level_progress`, eventos sazonais, season code, week marker, identity hash).
- **`test_db_smoke.py`** — valida migrations até v13 + tabelas críticas + integrity_check.
- **CI** (`ci.yml`, push p/ `royalRPG`/`main` + PRs): `ruff check --select E9,F63,F7,F82`
  (só bugs reais: undefined names/syntax) + `pytest -q`.
- ⚠️ O ruff crítico já pegou 3 NameErrors reais em produção (`format_br`, `TZ`, `log`) que
  crashavam saldo/boss/presente/casório/feedback-de-palavra. **Manter o gate no CI.**

---

## 🎮 Features de jogo (M-series)

### 🎁 Presentes — `/royalpresentear` (M02)
Transferência atômica de florins entre players (grupo-only).
- `/royalpresentear @user 100` (explícito) ou reply + `/royalpresentear 100` (implícito).
- Limites `GIFT_MIN=10`, `GIFT_MAX=5000`. Bloqueia self-gift, valida saldo, `UPDATE …
  WHERE gold>=?` (anti-race). Conquista `generoso` no 1º envio.
- **Migration v12:** tabela `gifts` (ledger).

### 🏅 Conquistas — `/royalconquistas` (M11)
11 slugs MVP em `ACHIEVEMENTS`: primeiro_acerto, dez_acertos, cem_acertos, primeiro_boss,
lvl_dez/vinte_cinco/cinquenta, primeiro_amor, mecenas, nobreza, generoso.
- API `unlock_achievement(chat_id, uid, slug)` — idempotente (PK `chat_id,user_id,slug`),
  retorna True só na 1ª unlock + DM `_notify_achievement_dm` (`EFFECT_PARTY`).
- **Triggers:** level-up→lvl 10/25/50 · PALAVRA win→1/10/100 · `finalize_boss`→primeiro_boss
  · `assign_couple`→primeiro_amor · `_grant_premium_perk`→nobreza/mecenas · presente→generoso.
- **Migration v12:** tabela `achievements` + índice por user_id.

### ⚙️ Preferências — `/royalconfig` (M19)
Flags em `user_dm_settings.prefs_json` (migration v12). API `get_user_prefs(uid)` /
`set_user_pref(uid, key, val)`. Callback `r:cfg:{key}` toggla (verde=ON/vermelho=OFF).

| key | default | efeito |
|---|---|---|
| `silent_levelup` | False | suprime card de level-up na DM (`_schedule_levelup_dm`) |
| `hide_rank` | False | reservado (futuro — filtrar do ranking) |
| `palavra_ping` | True | reservado (ping de spawn na DM) |

### 🗺️ Quests diárias — `/royalmissoes` (M05)
4 missões em `DAILY_QUESTS`, reset por dia (`today_key()`):

| id | evento | target | recompensa |
|---|---|---|---|
| `msgs` 💬 | message | 20 | +60 XP +30🪙 |
| `palavra` 🎯 | palavra_win | 1 | +80 XP +50🪙 |
| `boss` 🐉 | boss_hit | 3 | +50 XP +40🪙 |
| `social` 👍 | reaction | 5 | +30 XP +20🪙 |

- `quest_bump(chat_id, uid, event, n)` incrementa (cap atomic `MIN(progress+?, ?)`, só grupo).
  Claim via `r:quest:{id}` (`UPDATE … WHERE progress>=target AND claimed=0`).
- **Triggers:** `track()` · `attempt_word` win · `boss_attack` · `on_message_reaction`.
- **Migration v13:** tabela `quest_progress` (PK `chat_id,user_id,day,quest_id`).

### 👍 Reactions = XP (M06)
Reagir com emoji em grupo dá `REACTION_XP=3`, cap diário `REACTION_XP_DAILY_CAP=10`/user/chat.
- Handler `@dp.message_reaction` (`on_message_reaction`) — só premia ao ADICIONAR
  (`len(new)>len(old)`), respeita mute, conta no cap, XP via `award_xp_immediate` + quest `social`.
- ⚠️ Requer `allowed_updates` com `message_reaction` — resolvido em `main()` via
  `dp.resolve_used_update_types()`.
- **Migration v13:** tabela `reaction_xp_daily` (PK `chat_id,user_id,day`).

### 🚪 Reentrada de membro — boas-vindas com a ficha
Quando alguém que **já tem progresso** (linha em `players` daquele chat) sai e **volta** ao
grupo, o bot dá boas-vindas **marcando a pessoa** com uma **foto = ficha** (mesmo card do
`/royalperfil`), legenda `REENTRADA.SYS`. O progresso **nunca é apagado** na saída (só
`/royaldados` apaga, a pedido do user) — "restaurar" é automático; o handler só anuncia.
- Handler `@dp.chat_member` (`on_member_rejoin`) com `ChatMemberUpdatedFilter(JOIN_TRANSITION)`.
  Membro genuinamente novo (sem linha em `players`) é ignorado (nada a restaurar).
- `send_profile_card(..., caption_override=...)` reusa todo o pipeline da ficha (file_id cache
  + fallback) trocando só a legenda pela mensagem de volta com `mention()`.
- ⚠️ Requer o bot **ADMIN** no grupo: o Telegram só entrega `chat_member` updates a bots
  admin. O tipo entra em `allowed_updates` via `dp.resolve_used_update_types()` (handler
  registrado). Sem admin, o recurso fica inerte (não quebra nada).

### 🎉 Eventos sazonais — `/royalevento` (M09)
Boost de XP global por data, sem DB — `SEASONAL_EVENTS`:

| evento | data | mult |
|---|---|---|
| 🎆 Reveillon Real | 31/12–01/01 | 2.0× |
| 🎄 Natal dos Nobres | 24–25/12 | 2.0× |
| 🔥 Festa Junina Real | 23–24/06 | 1.5× |
| 🎃 Noite Sombria | 31/10 | 1.5× |
| ❤️ Dia dos Namorados | 12/06 | 1.5× |
| 🍻 Fim de Semana Real | sáb/dom (fallback) | 1.5× |

API `active_seasonal_event(d=None)` (datas especiais > FDS) · `event_xp_mult()`. Aplicado em
`award_xp_immediate` e `award_xp_message` (após bônus de classe/casamento).

### 🔇 `/royalmudo` — master switch (owner)
- **Grupo** (owner): toggle do chat (ON/OFF).
- **DM do owner:** broadcast — se qualquer grupo está ON-AIR, silencia TODOS; se todos mudos,
  religa TODOS. Log `[MUDO] BROADCAST actor=<uid> new_state=ON|OFF total=N changed=M`.

### 📜 `/royallog` — dump de logs (owner) + auto 5min
- Ring buffer in-memory `_LogRingBuffer` (maxlen 5000) no root logger (captura bot + aiogram).
- `/royallog` (owner, off-menu): snapshot → DM do owner como `.log` + atualiza gist (se
  `GH_TOKEN`). Ack auto-deletado em 12s no grupo.
- Job `log_dump_job()` a cada `LOG_DUMP_INTERVAL_SEC`: file silent p/ DM + PATCH no gist secreto
  (`public:false`, cap 500KB). Failsafe: DM falha → loga e segue; sem token → no-op.
- Desligar tudo: `LOG_DUMP_ENABLED=0`.

---

## 💎 Premium · Telegram Stars (M01 + M04 + M03)

Pagamento via **Telegram Stars (XTR)** — sem gateway externo, sem provider_token. Acesso:
`/royalloja` → **💎 Premium**.

### Itens avulsos (`PREMIUM_ITEMS`)
| Item | Stars | Perk em `players` |
|---|---|---|
| ⚡ Boost +20% XP (24h) | 50⭐ | `xp_boost_until` (ISO) |
| 💡 Dica da Palavra | 1⭐ | `prm_hints` (contador) |
| 🔱 Ressurreição no Boss | 10⭐ | `prm_ressurrects` (contador) |
| 🥇 Skin Dourada permanente | 100⭐ | `prm_skin_gold` (0/1) |

### Assinatura (`SUBSCRIPTIONS`)
| Perk | Stars | Colunas |
|---|---|---|
| 🌟 Royal Plus — +20% XP + badge violeta | 50⭐/mês | `royal_plus_until` (ISO), `royal_plus_charge_id` |

Royal Plus usa `sendInvoice(subscription_period=2592000)` (30 dias — único valor aceito em XTR).
Renovação automática gerenciada pelo Telegram; cancelamento no app (Settings → My Stars →
Subscriptions). Bot só observa: `now > royal_plus_until` → perks param.

### Fluxo técnico
1. Callback `r:xtr:{iid}` (avulso) / `r:sub:royal_plus` → `bot.send_invoice(currency="XTR", …)`.
2. `pre_checkout_handler` aceita prefixos `prm|` (avulso) e `sub|` (assinatura), valida amount/item.
3. `successful_payment_handler` grava em `stars_purchases` (idempotente por `charge_id`), concede
   via `_grant_premium_perk()`. Assinatura: lê `sp.subscription_expiration_date` → `royal_plus_until`;
   renovações chegam mensalmente (`is_recurring=True`, novo charge_id, tratadas igual).
- **Migrations:** v10 (cols premium + `stars_purchases`), v11 (`royal_plus_until` +
  `royal_plus_charge_id`).

### Dica paga da Palavra — `/royalpaldica` (M03)
Consome 1 crédito `prm_hints` (`UPDATE … WHERE prm_hints>0`, anti double-spend), revela 1 letra
alfabética aleatória. DM com `EFFECT_FIRE`; fallback `<tg-spoiler>` no chat se DM falhar.

### Aplicação dos perks — estado real
- ✅ **XP boost / Royal Plus** — `premium_xp_active(player)` checa `now < xp_boost_until` OU
  `now < royal_plus_until` → `*1.20` (`PREMIUM_XP_BUFF`) em `award_xp_immediate` e
  `award_xp_message` (após classe/casamento, antes do evento sazonal).
- ✅ **`prm_hints`** — consumido em `/royalpaldica`.
- ✅ **`prm_skin_gold`** — moldura GOLD + tag `[ * OURO * ]` (`ProfileCardData.skin_gold`); entra
  na cache_key do render + `_profile_card_data_hash`.
- ✅ **Badge violeta Royal Plus** — tag `[ PLUS+ ]` MAGENTA no card (`_royal_plus_active`);
  coexiste com a skin dourada.
- ⛔ **`prm_ressurrects` — BLOQUEADO (feature nova, não wiring):** o combate de boss não tem
  mecânica de morte do player (HP e corações no card são **cosméticos**, não há HP de combate,
  derrota ou hook de revive). Aplicar exigiria desenhar do zero HP de combate, contra-ataque,
  morte, lockout e revive — com balance inventado. Não implementado. **O item ainda é vendível
  na loja mas não tem efeito** (pendência de produto).
- ⛔ **Slot extra de casório — BLOQUEADO (feature nova, não wiring):** o código não tem limite de
  slot por usuário (`user_is_available` só checa opt_out+last_seen; `pick_couple` só evita pares
  repetidos). Aplicar exigiria primeiro CRIAR um limite (nerf em todos os free) e deixar o Plus
  burlá-lo — decisão de produto. **Por isso a copy do Royal Plus NÃO menciona slot extra.**

---

## 🛡️ Infra hardening (F-series)

> Estado **real** (auditado no código; o `ROADMAP.md` está desatualizado).

- **F10 · Backup** (`/royalbackup` owner + `backup_job`): `VACUUM INTO
  <DB_DIR>/backups/YYYY-MM-DD.sqlite3` diário em `BACKUP_HOUR` (**meio-dia**, default 12),
  precedido de `PRAGMA integrity_check` (não salva DB corrompido). Retenção
  `BACKUP_RETENTION_DAYS`. Sobe o dump pro `STASH_CHAT_ID` (silent) **e CONFIRMA no DM do
  owner** (`_notify_owner_backup` → arquivo + tamanho + retenção). Conexão sqlite separada
  (não toca o cursor global). O backup captura o DB inteiro (classe, nível, XP, saldo,
  casórios, inventário, ícone/avatar — tudo).
  - **Double backup de verificação (one-time):** na 1ª subida desta versão, `backup_job`
    roda **2 backups** (`tag=init1`/`init2`) com confirmação no DM, e só então grava o flag
    `bot_meta['initial_double_backup']='done'` (não repete em reboots). Falha → retenta no
    próximo boot.
- **F11 · Migrations transacionais** (`run_migrations`): cada migration em `BEGIN…COMMIT`; falha
  → `ROLLBACK` + aborta boot (o `user_version` só avança no sucesso).
- **F13 · Health endpoint** (`start_health_server` + `/health`): DB ping (`SELECT 1`), staleness
  de updates, `user_version`. Middleware `_track_last_update` alimenta o timer. Só sobe se `PORT`.
- **F14 · Graceful shutdown** (`@dp.shutdown` → `_on_shutdown`): aiogram trata SIGTERM/SIGINT;
  hook faz `flush_buffers_once()` + fecha health server + `db.commit()/close()`.
- **Já feitos (verificados):** F03, F04 (semaphore de render), F05, F06, F07 (`with_retry`),
  F08 (softban anti-spam), F12 (logs JSON via `LOG_JSON`), F15 (alerta de typing), F16, F17,
  F18 (`next_royal_id` à prova de colisão via UNIQUE index), F19 (cache de avatar em disco), F20.
- **F01/F02 (escala — auditados):** o cursor global (`cur`) é **seguro no design atual** — asyncio
  single-thread, sem `await` entre `execute` e `fetch`, e nenhum `to_thread` toca `cur`/`db`.
  "database is locked" mitigado por `journal_mode=WAL` + `busy_timeout=5000` +
  `synchronous=NORMAL` (em `setup_connection`). O rewrite p/ `aiosqlite` (214 call sites) **não
  foi feito**: alto risco, exige teste em runtime (indisponível aqui) — esforço dedicado, não cego.

---

## 💬 DM, inline mode & identity card

**Comandos pessoais em DM:** `/royalperfil`, `/royalficha`, `/royalranking`, `/royalinventario`,
`/royalsaldo`, `/royalmeuscasorios`, configs rodam em grupo E na DM. Na DM o bot resolve o "grupo
ativo" via `user_dm_settings` (migration v4): 1 grupo → auto; 2+ → picker; troca via `/royalgrupo`.

**Inline mode** (`@nome_do_bot` em qualquer chat): envia o card de perfil. Usa file_id cacheado
em memória (populado quando `send_profile_card` roda); sem cache → fallback texto + botão "Gerar
foto na DM". ⚠️ Precisa estar habilitado no BotFather (`/setinline` com placeholder).

**Identity card** (1080×1080 fixo: só avatar + ROY#ID + nome): gerado 1× no `ensure_player`,
regenerado só ao mudar nome/avatar.
- Triggers: player novo (fire-and-forget) + sweep diário `identity_card_sweep_job()` (~3h local;
  compara `inline_card_hash` salvo vs `_identity_card_hash(name, avatar_slug)`; throttle 1 upload/s).
- Storage: cols `inline_card_file_id` + `inline_card_hash` (migration v5; hash =
  `sha1(name|avatar_slug)[:16]`). Upload silencioso via `ensure_identity_card_async` →
  `STASH_CHAT_ID` (`disable_notification`). Sem `STASH_CHAT_ID` → no-op gracioso.

---

> ⚠️ **Ordem de handlers (regressão real já corrigida):** o catch-all
> `track` (`@dp.message(F.chat.type.in_({"group","supergroup"}))`) roda ANTES de
> alguns `@dp.message(Command(...))`. Em aiogram o 1º handler que casa **vence e
> PARA a propagação** — então comando registrado DEPOIS de um catch-all **nunca
> dispara em grupo**. Fix: `track` tem filtro `~(F.text & F.text.startswith("/"))`
> p/ não casar comandos (deixa propagar). Ao adicionar comando novo, registre-o
> ANTES do bloco "ULTIMO @dp.message" **ou** garanta que os catch-alls excluem
> comandos.

## 📋 Menus (BotCommands)

Registrados em `register_bot_commands()`. Telegram atualiza o autocomplete `/` na 1ª inicialização
com novo token. Se trocou `BOT_TOKEN` e o menu antigo persiste, reinicie o bot 1× (reescreve os
dois scopes).

- **Grupo** (`BotCommandScopeAllGroupChats`): `royal`, `royalperfil`, `royalficha`, `royalavatar`,
  `royalup`, `royalclasse`, `royalinventario`, `royalloja`, `royalsaldo`, `royalranking`,
  `royalpalavra`, `royalboss`, `royalcasorios`, `royalmeuscasorios`, `royalencalhar`,
  `royaldesencalhar`, `royalcasar` (admin), `royalativar` (admin), `royaltutorial`, `royalajuda`.
- **DM** (`BotCommandScopeAllPrivateChats`): `royal`, `royalperfil`, `royalavatar`, `royalficha`,
  `royalranking`, `royalinventario`, `royalsaldo`, `royalmeuscasorios`, `royalgrupo`,
  `royaltutorial`, `royalajuda`, `royalprivacidade`, `royaldados`.

---

## 🎛️ UI: botões coloridos (Bot API 10) + helpers UX

> 🧠 **MEMÓRIA PERMANENTE (não reverter):** botões coloridos nativos EXISTEM no Bot API 10 /
> aiogram 3.28.2 via campo `style`. Já em produção. Se algum agente afirmar "não existe", está
> errado — ver https://docs.aiogram.dev/en/latest/api/enums/button_style.html

`InlineKeyboardButton` e `KeyboardButton` têm `style=`:
- `'success'` → verde · `'danger'` → vermelho · `'primary'` → azul · omitido → tema do cliente.
- ⚠️ **Não existe `'warning'`/amarelo nativo** — pra essa categoria só emoji ⚠️.

**Convenção (constantes `main.py:575-584` — usar SEMPRE em par emoji+style):**

| Emoji | Style | Cor | Uso |
|---|---|---|---|
| `BTN_OK` ✅ | `STYLE_OK` `"success"` | verde | confirmar / aplicar / ir |
| `BTN_NO` ❌ | `STYLE_NO` `"danger"` | vermelho | cancelar / fechar / destrutivo |
| `BTN_INFO` 🔵 | `STYLE_INFO` `"primary"` | azul | informação / navegar |
| `BTN_WARN` ⚠️ | — | (só emoji) | atenção / reversível com custo |
| `BTN_BACK` ◀️ | — | neutro | voltar |
| `BTN_GO` ▶️ | — | neutro | avançar |

**Helper `ikb()` (`main.py:587`):** `ikb(f"{BTN_OK} Confirmar", callback_data="x", style=STYLE_OK)`.
Emoji líder mantido como fallback p/ clientes antigos + acessibilidade.

**Helpers de UX (`main.py` — usar quando aplicável):**
- `auto_delete_after(msg, delay=8.0)` — agenda exclusão (acks efêmeros que poluem grupo).
- `react_to(chat_id, message_id, emoji)` — bot reage ao comando (feedback instantâneo).
- `type_then_send(chat_id, text, delay=1.2, action="typing")` — "digitando" N s antes de enviar
  (fotos: `action="upload_photo"`).
- `safe_typing(chat_id, action)` — só dispara chat action, sem delay.
- `**effect_kw(chat.type, EFFECT_*)` — sparkles/fire/heart em DM 1:1 (Bot API 7.7).

---

## 🎨 Identidade visual — Retro-futurist dystopian

Tudo que o bot envia (texto **e** imagens) segue essa linguagem.

**Conceito:**
- **8-bit retrô** (terminal CRT velho, monospace, ASCII art).
- **Futurista** (`>>`, `//`, `[BRACKETS]`, `ID#0042`, `v0.1.ALPHA`).
- **Dark/dystopian** (paleta sempre escura, cores ácidas como acento).
- **Cor por player** — cada `royal_id` recebe paleta consistente (TOXIC, AMBER, PLASMA, ARCTIC,
  BIOLAB, BLOOD).

**Imagem (cara/lenta) vs caption (rápida):**
- Card 1080×1080: só momentos de destaque (perfil, pódio, casório, boss derrotado). Renderizado
  via `asyncio.to_thread(render_xxx_card, ...)`.
- Caption / texto puro: padrão p/ todo o resto, via `term_block()` / `term_pre()`.

**Voz padrão (helpers em `main.py`):**
- `term_block(title, body, status="OK", status_color="ACID|HOT|AMBER|CYAN", stamp=None)` — header
  `> TITLE.SYS // STATUS` + corpo + stamp opcional.
- `term_pre(rows)` — tabela monospace `KEY :: VALUE` em `<pre>`.
- Tom: título CAPS `PALAVRA.SYS`; prefixos `>` (saída), `>>` (sub-comando), `//` (comentário),
  `!!` (alerta); status `OK`/`CONECTADO`/`CONFIG`/`ALERTA`/`OFFLINE`; stamp final em itálico `[…]`.

**HTML do Telegram (usar tudo):** `<b> <i> <u> <s> <code> <pre> <a> <blockquote>
<blockquote expandable> <tg-spoiler>`. Caption de foto tem **limite de 1024 chars** — sempre
guardar no código.

**Efeitos (Bot API 7.7, só DM 1:1 — `effect_kw(chat_type, EFFECT_*)`):** `EFFECT_PARTY` 🎉
(boas-vindas, ganhar Palavra), `EFFECT_FIRE` 🔥 (dano crítico, golpe final), `EFFECT_HEART` ❤️
(casório), `EFFECT_THUMBS_UP`, `EFFECT_POO`, `EFFECT_THUMBS_DOWN`.

**Paleta dystopian (`royal_render.py`):** `BG_DEEP` `#0C0A0E` (vazio), `BG` `#16121A` (painel),
`INK` `#D2C4A8` (texto bone), `DIM` `#706054` (secundário); acentos rotativos `HOT`, `ACID`,
`CYAN`, `GOLD`, `RUST`, `PURPLE`, `AMBER`, `MAGENTA`, `NEON_BLUE`, `JADE`.

**Pós-processamento de cards (sempre):** `apply_scanlines(img, every=3, alpha=55)` (CRT) →
`apply_vignette(img, strength=160)` (distopia) → `apply_grain(img, intensity=18)` (TV velha).
