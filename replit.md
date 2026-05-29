# RPG - Royal para Geeks

> **Nome oficial:** **RPG - Royal para Geeks** (UI/comunicação pública). Antigo nome interno: "Royal Casorios".

Telegram bot **aiogram 3.28.2 / Bot API 10 / Python 3.12**, roda como **worker** (sem frontend),
persiste em **SQLite local**. No Replit fazemos **só o código** — deploy no Railway (não precisa
rodar/configurar workflow aqui).

> 📚 Mapa de alto nível + regras do dono. A lógica fina mora no código (`main.py`, `royal_render.py`);
> aqui só o que um agente **precisa não errar**: deploy, persistência, regras do dono, gotchas e
> decisões de produto.

---

## 🚀 Deploy & push (CRÍTICO)

**Deploy:** Railway (`railway.json`), escuta a branch **`royalRPG`**.

**Push (sempre `GITHUB_TOKEN`, NUNCA `GH_TOKEN`):**
```bash
git push "https://x-access-token:${GITHUB_TOKEN}@github.com/romastefale/royal-casorios.git" main:royalRPG
```
- `GITHUB_TOKEN` é auto-provisionado pelo Replit. `GH_TOKEN` **não existe** como secret aqui (é só
  env var de runtime do Railway, p/ gist).
- A branch `main` LOCAL **nunca** é alterada no remoto — sempre `main:royalRPG`.
- Só fazer push quando o usuário pedir **explicitamente**.

---

## 💾 Persistência de dados (CRÍTICO)

**O container do Railway é efêmero.** Cada redeploy recria o filesystem do snapshot do repo. SQLite
em path RELATIVO (`./data/...`) → volta ao estado commitado e **TODOS os dados de runtime são
perdidos** (perfis, royal_id, XP, casórios, inventário, saldo).

**Solução obrigatória — Railway Volumes:**
1. Dashboard → serviço → **Volumes** → mount path `/data`, 1 GB.
2. **Variables** → `DATABASE_PATH=/data/royal_casorios.sqlite3`. Redeploy.
3. Na 1ª boot, `_ensure_db_persistence()` detecta `/data/...` vazio e **copia o seed** do repo pro
   volume (uma vez). Daí em diante o volume persiste.

**Confirmar nos logs:** `[DB] BOOTSTRAP: copiei seed ...` (1ª boot) → `[DB] usando /data/...`
(seguintes). ⚠️ `[DB] !! ATENCAO: DATABASE_PATH eh relativo ...` → volume não montado →
**corrigir antes de qualquer deploy.**

> ⚠️ Sem volume, **não commitar** `data/royal_casorios.sqlite3` (sobrescreve o snapshot no redeploy).
> `.gitignore` ignora `data/` e `*.sqlite3`; arquivo já trackado precisa de `git rm --cached
> data/royal_casorios.sqlite3` (destrutivo — pedir ao usuário / project task).

**Migração grupo → supergrupo (CRÍTICO p/ não orfanar progresso):** ao virar supergrupo o Telegram
**troca o `chat_id`** e emite msg de serviço. `on_chat_migration` (`F.migrate_to_chat_id |
F.migrate_from_chat_id`, registrado ANTES do catch-all `track`) chama `migrate_chat_data(old,new)`:
transacional + idempotente (no-op se o id antigo já não tem dados → reprocessar nunca apaga o
migrado). Move as ~18 tabelas com `chat_id` (PK-tables: `UPDATE OR REPLACE` — progresso ANTIGO vence
SÓ em conflito, linhas não-conflitantes do supergrupo preservadas; FK-tables de PK surrogate: UPDATE
direto) + reaponta `user_dm_settings.active_chat_id`. Sem FK/cascade/trigger → REPLACE seguro.
Confirma no grupo + DM do owner. Testes: `test_migrate_chat_data_preserva_progresso_e_e_idempotente`
+ `test_migrate_listas_cobrem_todas_as_tabelas_com_chat_id`.

---

## 📌 User preferences (regras do dono)

- **Versões fixas (não fazer downgrade):** Bot API **10** · aiogram **3.28.2**.
- **💸 Custo zero — NUNCA usar funções/serviços pagos.** Só recursos gratuitos (nada de gateways
  pagos, APIs com cobrança por uso, geração de mídia paga). Telegram Stars (XTR) é exceção: é
  **receita** do bot (usuário paga), processada direto pelo Telegram.
- **🎬 UX no grupo — animação interativa, sem poluir:** comandos de escolha (inline kb/menus) → após
  o ato, **editar/apagar a msg antiga** (`edit_message_text`/`edit_message_reply_markup`/
  `delete_message` ou `auto_delete_after` p/ acks efêmeros), nunca empilhar msg nova. Preferir
  **edição in-place da MESMA msg**. **Não floodar.**

**Sincronia obrigatória com o código** — ao mexer no bot, revisar/atualizar quando aplicável:
`/start` (`start_cmd`), `/royalajuda` (`ROYAL_HELP`), `/royaltutorial` (`send_tutorial()` +
`ROYAL_TUTORIAL_PARTS`, **6 partes** ≤4096 chars), `register_bot_commands()` (se comando novo),
`replit.md`, `README.md`.
> Se a mudança for **infra interna** (cache, retry, log), só atualizar `replit.md` e mencionar no
> commit que /start/help/tutorial foram auditados e não mudaram.

> ⚠️ **Privacidade:** `attached_assets/` é trackado no git (só `generated_images/`, `*.txt` e `*.log`
> são ignorados) e vai pro GitHub no push. **Nunca** commitar prints/dumps/logs com PII (telefones,
> user ids). `.gitignore` já bloqueia `attached_assets/*.txt` e `attached_assets/*.log` (exports do
> Telegram e deployment logs). Outros assets só locais → `.gitignore` ou `git rm --cached`
> (destrutivo — project task / usuário).

---

## 📁 Estrutura de arquivos
- `main.py` — código principal do bot.
- `royal_render.py` — gerador de cards 1080×1080 (Pillow puro, sem Chromium).
- `royal_words.py` — palavras e charadas do mini-game.
- `requirements.txt` (runtime: `aiogram`, `Pillow`, `aiohttp`) · `requirements-dev.txt` (SÓ dev/CI:
  `pytest`, `ruff`; não vão pro Railway).
- `tests/` — `conftest.py` (DB tmp), `test_pure.py`, `test_db_smoke.py`.
- `.github/workflows/ci.yml` — CI: ruff crítico + pytest em push/PR.

---

## 🔧 Env vars

| Var | Obrig.? | Default | Para que serve |
|---|---|---|---|
| `BOT_TOKEN` | ✅ | — | Token do BotFather. |
| `DATABASE_PATH` | rec. | `./data/royal_casorios.sqlite3` | **No Railway use `/data/...` com volume** (ver Persistência). |
| `TZ` | — | `America/Sao_Paulo` | Timezone. |
| `AUTO_HOURS` | — | `9,15,21` | Horas dos casórios automáticos. |
| `OWNER_USER_ID` | rec. | — | Habilita comandos owner (`/royallog`, `/royalmudo`, `/royalpalavratest`). Sem isso, modo seguro (off). |
| `TEST_CHAT_IDS` | — | — | Grupos de teste (fora do picker de DM e do fallback inline). Comma-separated. |
| `STASH_CHAT_ID` | — | `-1003941532741` | Canal privado p/ upload silencioso (identity card + backups). |
| `GH_TOKEN` | — | — | PAT GitHub (scope `gist`) p/ upload de logs a cada 5min. Sem ele, só DM do owner. |
| `LOG_DUMP_INTERVAL_SEC` | — | `300` | Intervalo do auto-dump de logs. |
| `LOG_DUMP_ENABLED` | — | `1` | `0` desliga o auto-dump (mantém `/royallog` manual). |
| `LOG_JSON` | — | `0` | `1` → stdout root em JSON. Ring buffer do `/royallog` sempre em texto. |
| `PORT` | — | — | Se setado, sobe health server HTTP (`GET /health`). |
| `HEALTH_STALE_SEC` | — | `600` | `/health` → 503 se nenhuma update do Telegram nesse intervalo. |
| `BACKUP_ENABLED` | — | `1` | `0` desliga backup diário. |
| `BACKUP_HOUR` | — | `12` | Hora local do backup diário. |
| `BACKUP_RETENTION_DAYS` | — | `7` | Dias de backup mantidos em `<DB_DIR>/backups/`. |

---

## 🧪 Testes & CI

- **Local:** `pip install -r requirements-dev.txt && python -m pytest -q`.
- `conftest.py` aponta `DATABASE_PATH` p/ arquivo temporário ANTES de importar `main` → migrations
  rodam em DB vazio descartável.
- `test_pure.py` (funções puras) · `test_db_smoke.py` (migrations + integrity_check + middlewares +
  cleanup; `test_chest_claim_atomico_evita_baú_duplicado` trava a invariante do claim atômico do baú).
- **CI** (`ci.yml`, push `royalRPG`/`main` + PRs): `ruff check --select E9,F63,F7,F82` (só bugs reais:
  undefined names/syntax) + `pytest -q`. ⚠️ O ruff crítico já pegou 3 NameErrors reais em produção.
  **Manter o gate.**

---

## 🧩 Ordem de handlers & filtros de entrada (gotchas)

> ⚠️ **Ordem de handlers (regressão real já corrigida):** o catch-all `track`
> (`@dp.message(F.chat.type.in_(...))`) roda ANTES de alguns `Command(...)`. Em aiogram o 1º handler
> que casa **vence e PARA a propagação** — comando registrado DEPOIS de um catch-all **nunca dispara
> em grupo**. Fix: `track` exclui comandos via `~(F.text & F.text.startswith("/"))`. Ao adicionar
> comando novo, registre-o ANTES do bloco "ULTIMO @dp.message" **ou** garanta que os catch-alls
> excluem comandos. Handlers que precisam vir ANTES do `track`: `on_chat_migration`,
> `lucky_emoji_handler` (`F.dice`).

> 🤖 **Bots nunca viram jogadores:** `track` filtra `message.from_user.is_bot` logo na entrada.
> Admins **anônimos** postam como `@GroupAnonymousBot` (`is_bot=True`) — antes eram cadastrados na
> corte por engano. Posts de canal também caem aqui. Mensagens de outros bots não chegam (regra do
> Telegram), mas o filtro blinda o pseudo-bot de admin anônimo.
> - Anúncio one-shot da correção: `announce_no_bots_feature()` (flag `boot_announce_nobots_v1`).
> - Limpeza one-shot do legado: `cleanup_legacy_bot_players()` (flag `boot_cleanup_bot_players_v1`,
>   chamada em `main()`) apaga os bots conhecidos já cadastrados antes do filtro em TODA coluna de ref
>   de usuário (`_USER_REF_COLS`: `user_id`, `user1`/`user2` em couples/pair_scores,
>   `from_user`/`to_user` em gifts, `winner_user_id` em challenges, `voter_id` em votes) — descoberta
>   por PRAGMA, robusta a schema novo. `_LEGACY_BOT_USER_IDS` = `{1087968824 @GroupAnonymousBot,
>   136817688 @Channel_Bot, MUSIC_BOT_ID}` (só esses pseudo-bots viravam jogador; nenhum humano tem
>   esses ids). Transacional + idempotente, silencioso (só loga).
> - Testes: `test_cleanup_remove_bots_legados_e_e_idempotente`.

---

## 🎮 Features de jogo (M-series)

> Resumo + gotchas/migrations. A lógica completa está no código.

- **🎁 Presentes — `/royalpresentear` (M02):** transferência atômica de florins (grupo-only).
  `GIFT_MIN=10`/`GIFT_MAX=5000`, anti-race (`UPDATE … WHERE gold>=?`), conquista `generoso`. Bloqueia
  alvo bot. **Migration v12:** `gifts`.
- **🏅 Conquistas — `/royalconquistas` (M11):** 11 slugs em `ACHIEVEMENTS`. `unlock_achievement()`
  idempotente (PK `chat_id,user_id,slug`) + DM `EFFECT_PARTY`. Card `render_conquistas_card`/
  `ConquistasData` (GOLD), card-first + fallback texto. **Migration v12:** `achievements`.
- **⚙️ Preferências — `/royalconfig` (M19):** flags em `user_dm_settings.prefs_json` (v12). Keys:
  `silent_levelup` (suprime card de level-up na DM), `hide_rank`/`palavra_ping` (reservados).
- **🗺️ Quests diárias — `/royalmissoes` (M05):** 4 missões em `DAILY_QUESTS`, reset por dia.
  `quest_bump()` (cap atomic, só grupo), claim `r:quest:{id}`. Card `render_missoes_card`/`MissoesData`
  +`MissaoRow` (CYAN) com teclado de claim na FOTO. **Migration v13:** `quest_progress`.
- **👍 Reactions = XP (M06):** `REACTION_XP=3`, cap diário 10/user/chat. Handler `on_message_reaction`
  (só ao ADICIONAR). **Migration v13:** `reaction_xp_daily`.
- **🎰 Emoji da Sorte (M-luck):** USER manda o slot `🎰` no grupo e tira **trinca** → XP+florins; o
  bot **reage 🎉**. Valores nativos (1-64): `LUCKY_SLOT_WINS={1,22,43,64}`; jackpot
  (`LUCKY_SLOT_JACKPOT=64`) paga **em dobro** (`LUCKY_JACKPOT_XP=100`/`_GOLD=200` vs
  `LUCKY_WIN_XP=20`/`_GOLD=30`). Helper puro `lucky_reward_for(value)→(xp,gold,jackpot)`. Cap
  anti-farm `LUCKY_DAILY_CAP=5` vitórias/user/chat/dia. Handler `lucky_emoji_handler` (`F.dice`)
  registrado **ANTES** do `track`. UX sem flood: vitória = só a reaction 🎉; jackpot = 🎉 + 1 ack
  efêmero (auto-delete 30s); `sleep(2.0)` antes de comemorar (espera a animação). ⚠️ O `🎰` cosmético
  do baú (`roll_dice_visual`) **NÃO** dispara isto (`user.is_bot` filtrado). **Migration v14:**
  `lucky_emoji_daily` (PK `chat_id,user_id,day`, em `_CHAT_MIGRATE_PK_TABLES`). Sincronizado em
  `/royaltutorial` + `ROYAL_HELP`. Testes: `test_lucky_reward_*` + `lucky_emoji_daily` em
  `test_tabelas_criticas_existem`.
- **🚪 Reentrada de membro:** quem já tem progresso e volta é recebido com a ficha (foto). Progresso
  nunca é apagado na saída (só `/royaldados`). Handler `on_member_rejoin`.
- **🎉 Eventos sazonais — `/royalevento` (M09):** boost de XP global por data, sem DB
  (`SEASONAL_EVENTS`). `active_seasonal_event()` (datas especiais > FDS) · `event_xp_mult()`. Card
  `render_evento_card`/`EventoData` (ACID ativo / AMBER off).
- **🎁 Baú Real (chest):** `schedule_chest_after_palavra()` cria 1 baú `pending` p/ `CHEST_DELAY_MIN`
  no futuro a cada PALAVRA. `scheduler()` (a cada 20s) materializa pendentes via `spawn_chest()` e
  expira abertos (`expire_old_chests`). Primeiros `CHEST_MAX_CLAIMS` (5) saqueiam (`CHEST_REWARDS` por
  slot, claim atômico em `chest_claims`). **🎰 Flourish:** ao 5º claim (baú `closed`) o bot manda **1×
  `send_dice` 🎰** (`roll_dice_visual`, cosmético — não afeta recompensa). Único `send_dice`
  automático; vem do callback de claim.
- **🔇 `/royalmudo` (owner):** grupo → toggle do chat; DM do owner → broadcast (silencia/religa
  todos). **📜 `/royallog` (owner) + auto 5min:** ring buffer → DM `.log` + gist (se `GH_TOKEN`).

> ⚠️ **Cards card-first:** todos os handlers tentam `render_*` → `send_photo` e caem em fallback texto
> se falhar. Emoji de título é stripado (`re.sub(r"^\W+","")`) antes do Pillow (senão tofu).
> `quest_claim_cb`: `edit_text` falha em msg-FOTO → `except TelegramBadRequest` cai p/
> `edit_reply_markup`. `render_evento_card` usa posições verticais FIXAS (`text_size_smart.bh`
> subestima a altura e causava sobreposição).

---

## 💎 Premium · Telegram Stars (M01 + M04 + M03)

Pagamento via **Telegram Stars (XTR)** — sem gateway externo, sem provider_token. Acesso: `/royalloja`
→ **💎 Premium**.

**Itens avulsos** (`PREMIUM_ITEMS`): ⚡ Boost +20% XP 24h (50⭐ → `xp_boost_until`) · 💡 Dica da
Palavra (1⭐ → `prm_hints`) · 🔱 Ressurreição no Boss (10⭐ → `prm_ressurrects`) · 🥇 Skin Dourada
(100⭐ → `prm_skin_gold`).

**Assinatura** (`SUBSCRIPTIONS`): 🌟 Royal Plus +20% XP + badge violeta (50⭐/mês → `royal_plus_until`,
`royal_plus_charge_id`). `sendInvoice(subscription_period=2592000)` (30 dias, único valor aceito em
XTR); renovação/cancelamento via Telegram, bot só observa.

**Fluxo:** callback `r:xtr:{iid}`/`r:sub:royal_plus` → invoice → `pre_checkout_handler` (prefixos
`prm|`/`sub|`) → `successful_payment_handler` grava em `stars_purchases` (idempotente por `charge_id`)
+ `_grant_premium_perk()`. **Migrations:** v10 (cols premium + `stars_purchases`), v11 (cols Royal
Plus). Dica paga: `/royalpaldica` (M03) consome `prm_hints`, revela 1 letra.

**Estado real dos perks:**
- ✅ XP boost / Royal Plus (`premium_xp_active` → `*1.20`), `prm_hints` (`/royalpaldica`),
  `prm_skin_gold` (moldura GOLD + tag `[ * OURO * ]`), badge `[ PLUS+ ]` MAGENTA.
- ⛔ **`prm_ressurrects` — BLOQUEADO:** o boss não tem mecânica de morte do player (HP/corações são
  cosméticos). Item ainda vendível mas **sem efeito** (pendência de produto).
- ⛔ **Slot extra de casório — BLOQUEADO:** não há limite de slot no código. **Por isso a copy do
  Royal Plus NÃO menciona slot extra.**

---

## 🛡️ Infra hardening (F-series)

> Estado real (auditado no código; `ROADMAP.md` está desatualizado).

- **F10 · Backup** (`/royalbackup` + `backup_job`): `VACUUM INTO` diário em `BACKUP_HOUR`, precedido de
  `integrity_check`. Retenção `BACKUP_RETENTION_DAYS`. Upload `STASH_CHAT_ID` + confirmação no DM do
  owner. Conexão sqlite separada. One-time: double backup (flag `bot_meta['initial_double_backup']`).
- **F11 · Migrations transacionais:** cada migration em `BEGIN…COMMIT`; falha → `ROLLBACK` + aborta
  boot (`user_version` só avança no sucesso).
- **F13 · Health endpoint** (`/health`, só se `PORT`): DB ping + staleness + `user_version`.
- **F14 · Graceful shutdown** (`@dp.shutdown`): flush buffers + fecha health + commit/close.
- **Já feitos:** F03–F08, F12, F15–F20.
- **F01/F02 (escala):** cursor global `cur` é seguro no design atual (asyncio single-thread, sem
  `await` entre `execute`/`fetch`, nenhum `to_thread` toca `cur`/`db`). "database is locked" mitigado
  por WAL + `busy_timeout=5000` + `synchronous=NORMAL`. Rewrite p/ `aiosqlite` (214 call sites) **não
  foi feito** (alto risco, exige teste em runtime indisponível aqui).
- **🛡️ Anti-duplicação de spawns (multi-instância):** o `scheduler()` é DB-driven (não depende de
  `getUpdates`). Se 2 processos rodarem em overlap (redeploy: container antigo vivo quando o novo sobe;
  ou réplicas >1), ambos rodam o scheduler contra o MESMO DB e podem enviar (o 409 só barra polling,
  não `sendMessage`) → PALAVRAS/BAÚS duplicados. Mitigação (claims atômicos, WAL serializa writers, 1
  vencedor): **PALAVRA** → `UPDATE chats_rpg SET next_palavra_at=novo WHERE next_palavra_at=antigo`, só
  prossegue se `rowcount==1`. **BAÚ** → `spawn_chest()` faz `UPDATE chests SET status='open' WHERE id=?
  AND status='pending'` ANTES de enviar; só o vencedor manda. ⚠️ Defesa em profundidade — a causa raiz
  (2 instâncias) deve ser corrigida no Railway (réplicas=1 + kill do container antigo antes do novo).

---

## 💬 DM, inline mode & identity card

- **Comandos pessoais em DM** (`/royalperfil`, `/royalficha`, `/royalranking`, `/royalsaldo`, etc.)
  rodam em grupo E na DM. Na DM o bot resolve o "grupo ativo" via `user_dm_settings` (v4): 1 grupo →
  auto; 2+ → picker; troca via `/royalgrupo`.
- **Inline mode** (`@bot`): envia o card de perfil (file_id cacheado; sem cache → fallback texto +
  botão "Gerar foto na DM"). ⚠️ Precisa `/setinline` no BotFather.
- **Identity card** (1080×1080: avatar + ROY#ID + nome): gerado 1× no `ensure_player`, regenerado só
  ao mudar nome/avatar. Sweep diário `identity_card_sweep_job()` (compara `inline_card_hash`). Storage:
  cols `inline_card_file_id`/`inline_card_hash` (v5). Upload silencioso → `STASH_CHAT_ID`.

---

## 📋 Menus (BotCommands)

Registrados em `register_bot_commands()` (scopes `AllGroupChats` e `AllPrivateChats`). Telegram
atualiza o autocomplete `/` na 1ª inicialização com novo token; se o menu antigo persistir após trocar
`BOT_TOKEN`, reinicie 1×. Ao adicionar comando novo, atualizar a lista do scope correspondente.

---

## 🎛️ UI: botões coloridos (Bot API 10) + helpers UX

> 🧠 **MEMÓRIA PERMANENTE (não reverter):** botões coloridos nativos EXISTEM no Bot API 10 / aiogram
> 3.28.2 via campo `style`. Já em produção. Se algum agente afirmar "não existe", está errado — ver
> https://docs.aiogram.dev/en/latest/api/enums/button_style.html

`InlineKeyboardButton`/`KeyboardButton` têm `style=`: `'success'` (verde), `'danger'` (vermelho),
`'primary'` (azul), omitido (tema do cliente). ⚠️ **Não existe `'warning'`/amarelo nativo** — só emoji ⚠️.

**Convenção (constantes em `main.py`, sempre em par emoji+style):** `BTN_OK` ✅ +`STYLE_OK` `"success"`
(confirmar) · `BTN_NO` ❌ +`STYLE_NO` `"danger"` (cancelar/destrutivo) · `BTN_INFO` 🔵 +`STYLE_INFO`
`"primary"` (navegar) · `BTN_WARN` ⚠️ (só emoji) · `BTN_BACK` ◀️ / `BTN_GO` ▶️ (neutros). Helper
`ikb(text, callback_data, style=...)`.

**Helpers de UX (`main.py`):** `auto_delete_after(msg, delay)` · `react_to(chat, msg_id, emoji)` ·
`type_then_send(chat, text, delay, action)` · `safe_typing(chat, action)` · `**effect_kw(chat.type,
EFFECT_*)` (sparkles/fire/heart em DM 1:1, Bot API 7.7).

**❌ Botão Fechar (universal):** helpers `close_btn(owner_id=None)` + `with_close(kb, owner_id)`.
Callback `r:close:{uid}` (cards) ou `r:close` puro (menus). Branch `action=="close"` no TOPO de
`hub_cb`: com uid → checa `cb.from_user.id==uid`; sem uid → `assert_owner`; depois `delete_msg_safe`.
Anexado em: menus owner-locked (up/classe/inv/loja/premium, via `close_btn()`) e cards pessoais
(perfil, ranking, conquistas, missões, evento, saldo — via `with_close(kb, uid)`). Apaga a própria msg
(bot é admin no grupo). ⚠️ NÃO anexar em boas-vindas de reentrada (`send_profile_card` sem `close_uid`).

**🆙 Level-up anunciado no grupo:** `announce_level_up_group(chat_id,user_id,new_lvl)` posta texto leve
(`term_block` ACID, sem card) marcando via `mention()` (link `tg://user?id=…` → ping mesmo com nome
anonimizado). Disparado em `_schedule_levelup_dm` por `loop.create_task(...)` ANTES do early-return da
pref `silent_levelup` — o anúncio público é GLOBAL e independe dessa pref (que controla só o card de
level-up na DM).

---

## 🎨 Identidade visual — Retro-futurist dystopian

Tudo que o bot envia (texto **e** imagens) segue essa linguagem: **8-bit retrô** (CRT, monospace,
ASCII), **futurista** (`>>`, `//`, `[BRACKETS]`, `ID#0042`), **dark/dystopian** (paleta escura,
acentos ácidos), **cor por player** (cada `royal_id` recebe paleta consistente).

- **Imagem vs caption:** card 1080×1080 só em momentos de destaque (perfil, pódio, casório, boss,
  conquistas, missões, evento), via `asyncio.to_thread(render_*)`. Resto → caption/texto via
  `term_block()` / `term_pre()`.
- **Voz** (helpers `main.py`): `term_block(title, body, status, status_color, stamp)` (header
  `> TITLE.SYS // STATUS`) · `term_pre(rows)` (tabela `<pre>`). Prefixos `>` (saída), `>>`
  (sub-comando), `//` (comentário), `!!` (alerta).
- **HTML Telegram:** `<b> <i> <u> <s> <code> <pre> <a> <blockquote> <blockquote expandable>
  <tg-spoiler>`. Caption de foto: **limite 1024 chars** (usar `cap1024()`).
- **Efeitos (Bot API 7.7, só DM 1:1):** `EFFECT_PARTY` 🎉 · `EFFECT_FIRE` 🔥 · `EFFECT_HEART` ❤️ ·
  `EFFECT_THUMBS_UP/DOWN` · `EFFECT_POO`.
- **Paleta (`royal_render.py`):** `BG_DEEP` `#0C0A0E`, `BG` `#16121A`, `INK` `#D2C4A8`, `DIM`
  `#706054`; acentos `HOT ACID CYAN GOLD RUST PURPLE AMBER MAGENTA NEON_BLUE JADE`.
- **Pós-processamento de cards (sempre):** `apply_scanlines(every=3, alpha=55)` →
  `apply_vignette(strength=160)` → `apply_grain(intensity=18)`.
