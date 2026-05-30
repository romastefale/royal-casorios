# Arquitetura & referência detalhada — RPG - Royal para Geeks

> Referência de detalhe (features, perks, infra, UI, identidade visual). O **mapa operacional** +
> regras do dono + gotchas que não se pode errar ficam em [`replit.md`](../replit.md). A lógica fina
> mora no código: pacote **`royal/`** (`config` → `core` → `{jobs, handlers/*}`), com `main.py` de
> facade/entrypoint que inclui os Routers; cards em `royal_render.py`. Aqui só o que ajuda a navegar
> sem reintroduzir regressões. Verificado contra o código (migrations até **v14**).

> 🧩 **Layout modular (`royal/`):** o bot era um monólito `main.py` (~8,9k linhas); foi quebrado num
> pacote por feature num refactor **puramente estrutural** (código verbatim, sem mudança de
> comportamento/DB/migrations). Cada feature em `royal/handlers/<feature>.py` expõe um `router`
> aiogram (`@router.message`/`@router.callback_query`/…), todos incluídos em `main.py`. `config.py`
> (constantes/env) é folha; `core.py` (`bot`/`dp`/middlewares/schema/helpers) importa só config;
> `jobs.py` e os handlers importam core. Grafo **acíclico**; `main.py` re-exporta tudo p/ os testes
> (`import main`). **Detalhe de menção a `main.py` abaixo:** símbolos/constantes/helpers citados como
> "em `main.py`" agora vivem nos módulos de `royal/` (o `main.py` só re-exporta) — a localização
> lógica é a mesma, o caminho físico mudou.

---

## 🎮 Features de jogo (M-series)

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
  todos). **📜 `/royallog` (owner) + auto 5min:** ring buffer → **arquivo em `backup/logs/`**
  (rotação `LOG_BACKUP_KEEP`) + gist (se `GH_TOKEN`). **Não manda mais o log na DM.**

> ⚠️ **Cards card-first:** todos os handlers tentam `render_*` → `send_photo` e caem em fallback texto
> se falhar. Emoji de título é stripado (`re.sub(r"^\W+","")`) antes do Pillow (senão tofu).
> `quest_claim_cb`: `edit_text` falha em msg-FOTO → `except TelegramBadRequest` cai p/
> `edit_reply_markup`. `render_evento_card` usa posições verticais FIXAS (`text_size_smart.bh`
> subestima a altura e causava sobreposição).

---

## 💎 Premium · REMOVIDO (o bot nunca cobra nada)

O sistema Premium foi **removido por completo**: loja Premium, pagamento via **Telegram Stars (XTR)**,
assinatura **Royal Plus**, todos os perks pagos (Boost XP, Dica da Palavra `/royalpaldica`,
Ressurreição no Boss, Skin Dourada), o badge violeta `[ PLUS+ ]` e a moldura/tag GOLD no profile card.
**Não reintroduzir cobrança de nenhum tipo** — nem mesmo Telegram Stars. A única economia é interna:
**florins 🪙** (ganhos no jogo, gastos na loja in-game Poção/Anel e em `/royalpresentear`).

**Removido do código:** handlers `pre_checkout_handler`/`successful_payment_handler`,
`_grant_premium_perk`, `premium_xp_active`/`_royal_plus_active`/`_perk_until_active`,
`PREMIUM_ITEMS`/`SUBSCRIPTIONS`, `premium_keyboard()`, callbacks `r:prem`/`r:xtr`/`r:sub`, comando
`/royalpaldica`, o bônus +20% XP e os campos `skin_gold`/`royal_plus` do `ProfileCardData`.

**DB inerte (NÃO dropado, sem perda de dados):** migrations **v10/v11** mantidas — colunas
`xp_boost_until`, `prm_hints`, `prm_ressurrects`, `prm_skin_gold`, `royal_plus_until`,
`royal_plus_charge_id` e a tabela `stars_purchases` continuam existindo mas **nenhum código lê/escreve
nelas** (marcadas como LEGADO/INERTE nos docstrings).

---

## 🛡️ Infra hardening (F-series)

> Estado real (auditado no código; `ROADMAP.md` está desatualizado).

- **F10 · Backup** (`/royalbackup` + `backup_job`): `VACUUM INTO` diário em `BACKUP_HOUR`, precedido de
  `integrity_check`. Retenção `BACKUP_RETENTION_DAYS`. Upload `STASH_CHAT_ID` + confirmação no DM do
  owner. Conexão sqlite separada. One-time: double backup (flag `bot_meta['initial_double_backup']`).
- **F11 · Migrations transacionais:** cada migration em `BEGIN…COMMIT`; falha → `ROLLBACK` + aborta
  boot (`user_version` só avança no sucesso). `MIGRATIONS` = lista `(versão, fn)` de v1 a **v14**.
- **F21 · Log em arquivo + alerta inteligente** (`royal/alerts.py`, leaf `config ← alerts ← core`):
  `log_dump_job`/`/royallog` gravam o ring em `backup/logs/` (rotação) em vez de DM. Um
  `OwnerAlertHandler` no root logger classifica `ERROR`/`CRITICAL` via `ERROR_CATALOG` e **só manda
  DM nos casos relevantes** (bug código/DB/import/boot); transitórios do Telegram (RetryAfter, "query
  is too old", edit no-op, bot bloqueado, rede) são silenciados. Dedupe por assinatura +
  `OWNER_ALERT_TTL_SEC`; cada relevante vira arquivo em `backup/alerts/`. `is_benign_telegram_error`
  usado nos `except` (ex.: `hub_cb`) p/ não logar benigno como ERROR. Mapa: `backup/ERROR_CATALOG.md`.
  **Fixes reais:** `send_couple` TZ (`ZoneInfo(TZ_NAME)`), `ensure_identity_card_async` trata
  `TelegramRetryAfter` (retry 1×), `hub_cb` callback expirado não é mais ERROR.
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
Anexado em: menus owner-locked (up/classe/inv/loja, via `close_btn()`) e cards/saídas pessoais
(perfil, ranking, conquistas, missões, evento, saldo, **hub `/royal`, `/royalajuda`+`/help` (ROYAL_HELP),
`/royalcasorios`, `/royalativar`** — via `with_close(kb, uid)`). Apaga a própria msg
(bot é admin no grupo). ⚠️ NÃO anexar em boas-vindas de reentrada (`send_profile_card` sem `close_uid`).

**📖 Tutorial navegável (não floodar):** `send_tutorial()` manda **1 mensagem editável** com
`◀ Anterior / Próximo ▶` + Fechar (`_tutorial_block(idx)` + `_tutorial_kb(idx, uid)`; callback
`r:tut:{idx}:{uid}` edita in-place). Antes empilhava 6 msgs. Usado por `/start`,
`/royaltutorial`, `/royalajuda`, `/help`. As **6 partes** seguem em `ROYAL_TUTORIAL_PARTS` (≤4096 chars).

**👑 Avatar por botões:** `/royalavatar` mostra o mosaico com **grid 1..36** (`_avatar_kb(uid)`, 6/linha)
+ Fechar, owner-locked. Callback `r:av:{n}:{uid}` aplica via `_apply_avatar()` (fonte única; status
`ok/locked/noprofile/badnum`) e revela com `_avatar_reveal_payload()`. O **reply numérico** (`AvatarReplyFilter`)
segue como fallback. Caption diz "Toque no número" (antes "Responda com número").

**🔒 Deep-link p/ DM:** `/royalprivacidade` no grupo oferece botão `url=t.me/<bot>?start=priv`
(via `bot.me()`), facilitando ir pra DM em vez de só instruir por texto.

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
- **Ícones nos cards = sprites pixel-art, NÃO emoji-fonte.** As fontes do projeto (DejaVu/Noto
  Sans/Math/Symbols/Symbols2) não cobrem vários emojis (👑🌹🧙📜🧪🥾💍) → davam **tofu** nos cards.
  Solução: sprites 8-bit (`SPRITE_*`, '#'/'o' em grid) desenhados por `draw_sprite()`. Classes e itens
  são desenhados via `draw_icon_centered(draw, cx, cy, emoji, target_px, color, shadow=)`, que mapeia
  o emoji → sprite por `EMOJI_SPRITES` (chaves **sem** variation selector `U+FE0F`) e cai no emoji
  como texto só se não houver sprite. Cobertos: classes (`SPRITE_CROWN ROSE WIZARD SCROLL KEY` +
  `SWORD`) e itens (`POTION RING SHIELD` + `BOOT SWORD BOOK CROWN`). ⚠️ Emoji em **menu/texto** do
  Telegram (ex. 🐉 em BotCommand) renderiza nativo — não precisa de sprite. Ao adicionar classe/item
  novo, adicione o sprite + entrada em `EMOJI_SPRITES` senão volta o tofu no card.
- **Pós-processamento de cards (sempre):** `apply_scanlines(every=3, alpha=55)` →
  `apply_vignette(strength=160)` → `apply_grain(intensity=18)`.
