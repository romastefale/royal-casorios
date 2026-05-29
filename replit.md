# RPG - Royal para Geeks

> **Nome oficial:** **RPG - Royal para Geeks** (usar em UI/comunicação pública).
> Antigo nome interno: "Royal Casorios".

Telegram bot em **aiogram 3.28.2 / Bot API 10 / Python 3.12**. Roda como **worker** (sem
frontend), persistindo em **SQLite local**. No Replit fazemos **apenas o código** — o deploy
é no Railway (não precisa rodar/configurar workflow aqui).

> 📚 Este README é o mapa de alto nível + regras do dono. Detalhes finos de cada feature
> moram no código (`main.py`, `royal_render.py`). Mantenha aqui só o que um agente **precisa
> não errar**: deploy, persistência, regras do dono, gotchas e decisões de produto.

---

## 🚀 Deploy & push (CRÍTICO)

**Deploy:** Railway (`railway.json`), escuta a branch **`royalRPG`**.

**Push (sempre `GITHUB_TOKEN`, NUNCA `GH_TOKEN`):**
```bash
git push "https://x-access-token:${GITHUB_TOKEN}@github.com/romastefale/royal-casorios.git" main:royalRPG
```
- `GITHUB_TOKEN` é auto-provisionado pelo Replit (sempre disponível). `GH_TOKEN` **não existe**
  como secret aqui (é só env var de runtime do Railway, p/ gist).
- A branch `main` LOCAL **nunca** é alterada no remoto — sempre `main:royalRPG`.
- Só fazer push quando o usuário pedir explicitamente.

---

## 💾 Persistência de dados (CRÍTICO)

**O container do Railway é efêmero.** Cada `git push` que dispara redeploy recria o filesystem
do snapshot do repo. SQLite em path RELATIVO (`./data/...`) → volta ao estado commitado e
**TODOS os dados de runtime são perdidos** (perfis, royal_id, XP, casórios, inventário, saldo).

**Solução obrigatória — Railway Volumes:**
1. Dashboard → serviço → **Volumes** → **+ New Volume**, mount path `/data`, 1 GB.
2. **Variables** → `DATABASE_PATH=/data/royal_casorios.sqlite3`. Redeploy.
3. Na 1ª boot, `_ensure_db_persistence()` (`main.py`) detecta `/data/...` vazio e **copia o
   seed** do repo pro volume (uma vez). Daí em diante o volume persiste.

**Confirmar nos logs:** `[DB] BOOTSTRAP: copiei seed ...` (1ª boot) → `[DB] usando /data/...`
(seguintes). ⚠️ `[DB] !! ATENCAO: DATABASE_PATH eh relativo ...` → volume não montado →
**corrigir antes de qualquer deploy.**

> ⚠️ Sem volume, **não commitar** `data/royal_casorios.sqlite3` (sobrescreve o snapshot no
> redeploy). `.gitignore` ignora `data/` e `*.sqlite3`; o arquivo trackado precisa de
> `git rm --cached data/royal_casorios.sqlite3` (destrutivo — pedir ao usuário / project task).

**Migração grupo → supergrupo (CRÍTICO p/ não orfanar progresso):** quando um grupo vira
supergrupo, o Telegram **troca o `chat_id`** e emite mensagem de serviço. Handler
`on_chat_migration` (`F.migrate_to_chat_id | F.migrate_from_chat_id`, registrado ANTES do
catch-all `track`) chama `migrate_chat_data(old, new)`: transacional + idempotente (no-op se o
id antigo já não tem dados → reprocessar nunca apaga o migrado). Move as ~18 tabelas com
`chat_id` (PK-tables: `UPDATE OR REPLACE` — progresso ANTIGO vence SÓ em conflito, linhas
não-conflitantes do supergrupo são preservadas; FK-tables de PK surrogate: UPDATE direto sem
perda) + reaponta `user_dm_settings.active_chat_id`. Sem FK/cascade/trigger no schema → REPLACE
seguro. Confirma no grupo + DM do owner. Testes:
`test_migrate_chat_data_preserva_progresso_e_e_idempotente` +
`test_migrate_listas_cobrem_todas_as_tabelas_com_chat_id`.

---

## 📌 User preferences (regras do dono)

**Versões fixas (não fazer downgrade):** Bot API **10** · aiogram **3.28.2**.

**Sincronia obrigatória com o código** — ao mexer no bot, revisar/atualizar quando aplicável:
- `/start` (`start_cmd`), `/royalajuda` (`ROYAL_HELP`), `/royaltutorial` (`send_tutorial()` +
  `ROYAL_TUTORIAL_PARTS`, tutorial em **6 partes** ≤4096 chars).
- `register_bot_commands()` (se adicionou comando novo), `replit.md`, `README.md`.
> Se a mudança for **infra interna** (cache, retry, log), só atualizar `replit.md` e mencionar
> no commit que /start/help/tutorial foram auditados e não mudaram.

**💸 Custo zero — NUNCA usar funções/serviços pagos.** Só recursos gratuitos (nada de gateways
pagos, APIs com cobrança por uso, geração de mídia paga). Telegram Stars (XTR) é exceção
legítima: é **receita** do bot (usuário paga), processada direto pelo Telegram.

**🎬 UX no grupo — animação interativa, sem poluir:**
- Comandos de escolha (inline kb / menus): após o ato concluir, **editar/apagar a mensagem
  antiga** — nunca empilhar mensagem nova. Usar `edit_message_text` / `edit_message_reply_markup`
  / `delete_message` (ou `auto_delete_after` p/ acks efêmeros).
- Preferir **edição in-place da MESMA mensagem** conforme o fluxo avança. **Não floodar.**

> ⚠️ **Privacidade:** `attached_assets/` é trackado no git (só `generated_images/` é ignorado)
> e vai pro GitHub no push. **Nunca** commitar prints/dumps/logs com PII (telefones, user ids).
> Assets só locais → `.gitignore` ou `git rm --cached` (destrutivo — project task / usuário).

---

## 📁 Estrutura de arquivos
- `main.py` — código principal do bot.
- `royal_render.py` — gerador de cards 1080×1080 (Pillow puro, sem Chromium).
- `royal_words.py` — palavras e charadas do mini-game.
- `requirements.txt` (runtime: `aiogram`, `Pillow`, `aiohttp`) · `requirements-dev.txt`
  (SÓ dev/CI: `pytest`, `ruff`; não vão pro Railway).
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
- `conftest.py` aponta `DATABASE_PATH` p/ arquivo temporário ANTES de importar `main` →
  migrations rodam em DB vazio descartável.
- `test_pure.py` (funções puras) · `test_db_smoke.py` (migrations até v13 + integrity_check).
- **CI** (`ci.yml`, push `royalRPG`/`main` + PRs): `ruff check --select E9,F63,F7,F82` (só bugs
  reais: undefined names/syntax) + `pytest -q`.
- ⚠️ O ruff crítico já pegou 3 NameErrors reais em produção. **Manter o gate no CI.**

---

## 🎮 Features de jogo (M-series)

> Resumo + gotchas/migrations. A lógica completa está no código.

- **🎁 Presentes — `/royalpresentear` (M02):** transferência atômica de florins (grupo-only).
  `GIFT_MIN=10`/`GIFT_MAX=5000`, anti-race (`UPDATE … WHERE gold>=?`), conquista `generoso`.
  **Migration v12:** tabela `gifts`.
- **🏅 Conquistas — `/royalconquistas` (M11):** 11 slugs em `ACHIEVEMENTS`.
  `unlock_achievement()` idempotente (PK `chat_id,user_id,slug`) + DM `EFFECT_PARTY`.
  **Card visual** `render_conquistas_card`/`ConquistasData` (GOLD), card-first + fallback texto.
  **Migration v12:** tabela `achievements`.
- **⚙️ Preferências — `/royalconfig` (M19):** flags em `user_dm_settings.prefs_json` (v12). Keys:
  `silent_levelup` (suprime card de level-up na DM), `hide_rank`/`palavra_ping` (reservados).
- **🗺️ Quests diárias — `/royalmissoes` (M05):** 4 missões em `DAILY_QUESTS`, reset por dia.
  `quest_bump()` (cap atomic, só grupo), claim `r:quest:{id}`. **Card visual**
  `render_missoes_card`/`MissoesData`+`MissaoRow` (CYAN) com teclado de claim na FOTO.
  **Migration v13:** tabela `quest_progress`.
- **👍 Reactions = XP (M06):** `REACTION_XP=3`, cap diário 10/user/chat. Handler
  `on_message_reaction` (só ao ADICIONAR). **Migration v13:** `reaction_xp_daily`.
- **🚪 Reentrada de membro:** quem já tem progresso e volta ao grupo é recebido com a ficha
  (foto). Progresso nunca é apagado na saída (só `/royaldados`). Handler `on_member_rejoin`.
- **🎉 Eventos sazonais — `/royalevento` (M09):** boost de XP global por data, sem DB
  (`SEASONAL_EVENTS`). `active_seasonal_event()` (datas especiais > FDS) · `event_xp_mult()`.
  **Card visual** `render_evento_card`/`EventoData` (ACID ativo / AMBER off), card global.
- **🔇 `/royalmudo` (owner):** grupo → toggle do chat; DM do owner → broadcast (silencia/religa
  todos). **📜 `/royallog` (owner) + auto 5min:** ring buffer → DM `.log` + gist (se `GH_TOKEN`).

> ⚠️ **Cards card-first:** todos os handlers de card tentam `render_*` → `send_photo` e caem
> em fallback texto se o render falhar. Emoji de título é stripado (`re.sub(r"^\W+","")`) antes
> do Pillow p/ não virar tofu. ⚠️ `quest_claim_cb`: `edit_text` falha em msg-FOTO →
> `except TelegramBadRequest` cai p/ `edit_reply_markup` (remove botão resgatado in-place).
> ⚠️ `render_evento_card` usa posições verticais FIXAS — `text_size_smart.bh` subestima a
> altura e causava sobreposição.

---

## 💎 Premium · Telegram Stars (M01 + M04 + M03)

Pagamento via **Telegram Stars (XTR)** — sem gateway externo, sem provider_token. Acesso:
`/royalloja` → **💎 Premium**.

**Itens avulsos** (`PREMIUM_ITEMS`): ⚡ Boost +20% XP 24h (50⭐ → `xp_boost_until`) · 💡 Dica da
Palavra (1⭐ → `prm_hints`) · 🔱 Ressurreição no Boss (10⭐ → `prm_ressurrects`) · 🥇 Skin Dourada
(100⭐ → `prm_skin_gold`).

**Assinatura** (`SUBSCRIPTIONS`): 🌟 Royal Plus +20% XP + badge violeta (50⭐/mês →
`royal_plus_until`, `royal_plus_charge_id`). `sendInvoice(subscription_period=2592000)` (30 dias,
único valor aceito em XTR); renovação/cancelamento via Telegram, bot só observa.

**Fluxo:** callback `r:xtr:{iid}`/`r:sub:royal_plus` → invoice → `pre_checkout_handler` (prefixos
`prm|`/`sub|`) → `successful_payment_handler` grava em `stars_purchases` (idempotente por
`charge_id`) + `_grant_premium_perk()`. **Migrations:** v10 (cols premium + `stars_purchases`),
v11 (cols Royal Plus). Dica paga: `/royalpaldica` (M03) consome `prm_hints`, revela 1 letra.

**Estado real dos perks:**
- ✅ XP boost / Royal Plus (`premium_xp_active` → `*1.20`), `prm_hints` (`/royalpaldica`),
  `prm_skin_gold` (moldura GOLD + tag `[ * OURO * ]`), badge `[ PLUS+ ]` MAGENTA.
- ⛔ **`prm_ressurrects` — BLOQUEADO:** o boss não tem mecânica de morte do player (HP/corações
  são cosméticos). Aplicar exigiria desenhar HP de combate/morte/revive do zero. **Item ainda é
  vendível mas sem efeito** (pendência de produto).
- ⛔ **Slot extra de casório — BLOQUEADO:** não há limite de slot no código. Aplicar exigiria
  CRIAR um limite (nerf nos free) e deixar o Plus burlá-lo — decisão de produto. **Por isso a
  copy do Royal Plus NÃO menciona slot extra.**

---

## 🛡️ Infra hardening (F-series)

> Estado real (auditado no código; `ROADMAP.md` está desatualizado).

- **F10 · Backup** (`/royalbackup` + `backup_job`): `VACUUM INTO` diário em `BACKUP_HOUR`,
  precedido de `integrity_check`. Retenção `BACKUP_RETENTION_DAYS`. Upload `STASH_CHAT_ID` +
  confirmação no DM do owner. Conexão sqlite separada. One-time: double backup de verificação
  (flag `bot_meta['initial_double_backup']`).
- **F11 · Migrations transacionais:** cada migration em `BEGIN…COMMIT`; falha → `ROLLBACK` +
  aborta boot (`user_version` só avança no sucesso).
- **F13 · Health endpoint** (`/health`, só se `PORT`): DB ping + staleness + `user_version`.
- **F14 · Graceful shutdown** (`@dp.shutdown`): flush buffers + fecha health + commit/close.
- **Já feitos:** F03–F08, F12, F15–F20.
- **F01/F02 (escala):** cursor global `cur` é seguro no design atual (asyncio single-thread, sem
  `await` entre `execute`/`fetch`, nenhum `to_thread` toca `cur`/`db`). "database is locked"
  mitigado por WAL + `busy_timeout=5000` + `synchronous=NORMAL`. Rewrite p/ `aiosqlite` (214 call
  sites) **não foi feito** (alto risco, exige teste em runtime indisponível aqui).

---

## 💬 DM, inline mode & identity card

- **Comandos pessoais em DM** (`/royalperfil`, `/royalficha`, `/royalranking`, `/royalsaldo`,
  etc.) rodam em grupo E na DM. Na DM o bot resolve o "grupo ativo" via `user_dm_settings` (v4):
  1 grupo → auto; 2+ → picker; troca via `/royalgrupo`.
- **Inline mode** (`@bot`): envia o card de perfil (file_id cacheado; sem cache → fallback texto
  + botão "Gerar foto na DM"). ⚠️ Precisa `/setinline` no BotFather.
- **Identity card** (1080×1080: avatar + ROY#ID + nome): gerado 1× no `ensure_player`, regenerado
  só ao mudar nome/avatar. Sweep diário `identity_card_sweep_job()` (compara `inline_card_hash`).
  Storage: cols `inline_card_file_id`/`inline_card_hash` (v5). Upload silencioso → `STASH_CHAT_ID`.

---

> ⚠️ **Ordem de handlers (regressão real já corrigida):** o catch-all `track`
> (`@dp.message(F.chat.type.in_(...))`) roda ANTES de alguns `Command(...)`. Em aiogram o 1º
> handler que casa **vence e PARA a propagação** — comando registrado DEPOIS de um catch-all
> **nunca dispara em grupo**. Fix: `track` exclui comandos via `~(F.text & F.text.startswith("/"))`.
> Ao adicionar comando novo, registre-o ANTES do bloco "ULTIMO @dp.message" **ou** garanta que
> os catch-alls excluem comandos.

## 📋 Menus (BotCommands)

Registrados em `register_bot_commands()` (dois scopes: `AllGroupChats` e `AllPrivateChats`).
Telegram atualiza o autocomplete `/` na 1ª inicialização com novo token; se o menu antigo
persistir após trocar `BOT_TOKEN`, reinicie o bot 1×. Ao adicionar comando novo, atualizar a
lista do scope correspondente em `register_bot_commands()`.

---

## 🎛️ UI: botões coloridos (Bot API 10) + helpers UX

> 🧠 **MEMÓRIA PERMANENTE (não reverter):** botões coloridos nativos EXISTEM no Bot API 10 /
> aiogram 3.28.2 via campo `style`. Já em produção. Se algum agente afirmar "não existe", está
> errado — ver https://docs.aiogram.dev/en/latest/api/enums/button_style.html

`InlineKeyboardButton`/`KeyboardButton` têm `style=`: `'success'` (verde), `'danger'` (vermelho),
`'primary'` (azul), omitido (tema do cliente). ⚠️ **Não existe `'warning'`/amarelo nativo** — só
emoji ⚠️.

**Convenção (constantes em `main.py`, usar SEMPRE em par emoji+style):** `BTN_OK` ✅ +`STYLE_OK`
`"success"` (confirmar) · `BTN_NO` ❌ +`STYLE_NO` `"danger"` (cancelar/destrutivo) · `BTN_INFO` 🔵
+`STYLE_INFO` `"primary"` (navegar) · `BTN_WARN` ⚠️ (só emoji) · `BTN_BACK` ◀️ / `BTN_GO` ▶️
(neutros). Helper `ikb(text, callback_data, style=...)` — emoji líder mantido como fallback.

**Helpers de UX (`main.py`):** `auto_delete_after(msg, delay)` · `react_to(chat, msg_id, emoji)` ·
`type_then_send(chat, text, delay, action)` · `safe_typing(chat, action)` ·
`**effect_kw(chat.type, EFFECT_*)` (sparkles/fire/heart em DM 1:1, Bot API 7.7).

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
