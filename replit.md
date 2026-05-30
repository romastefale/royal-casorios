# RPG - Royal para Geeks

> **Nome oficial:** **RPG - Royal para Geeks** (UI/comunicação pública). Antigo nome interno: "Royal Casorios".

Telegram bot **aiogram 3.28.2 / Bot API 10 / Python 3.12**, roda como **worker** (sem frontend),
persiste em **SQLite local**. No Replit fazemos **só o código** — deploy no Railway (não precisa
rodar/configurar workflow aqui).

> 📚 Este arquivo é o **mapa operacional**: deploy, persistência, regras do dono e gotchas ativos —
> o que um agente **precisa não errar**. A referência de detalhe (features, perks, infra, UI,
> identidade visual) está em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). A lógica fina mora no
> código (`main.py`, `royal_render.py`).

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
  pagos, APIs com cobrança por uso, geração de mídia paga).
- **🚫 O bot NUNCA cobra nada do usuário.** O sistema Premium (loja Premium, pagamento via Telegram
  Stars/XTR, assinatura Royal Plus e perks pagos) foi **removido por completo**. Não reintroduzir
  cobrança de nenhum tipo — nem mesmo Telegram Stars. A única economia é interna: **florins 🪙**
  (ganhos no jogo, gastos na loja in-game e em `/royalpresentear`).
- **🎬 UX no grupo — animação interativa, sem poluir:** comandos de escolha (inline kb/menus) → após
  o ato, **editar/apagar a msg antiga** (`edit_message_text`/`edit_message_reply_markup`/
  `delete_message` ou `auto_delete_after` p/ acks efêmeros), nunca empilhar msg nova. Preferir
  **edição in-place da MESMA msg**. **Não floodar.**

**Sincronia obrigatória com o código** — ao mexer no bot, revisar/atualizar quando aplicável:
`/start` (`start_cmd`), `/royalajuda` (`ROYAL_HELP`), `/royaltutorial` (`send_tutorial()` +
`ROYAL_TUTORIAL_PARTS`, **6 partes** ≤4096 chars), `register_bot_commands()` (se comando novo),
`replit.md`, `docs/ARCHITECTURE.md`, `README.md`.
> Se a mudança for **infra interna** (cache, retry, log), só atualizar `replit.md`/`docs/` e mencionar
> no commit que /start/help/tutorial foram auditados e não mudaram.

> ⚠️ **Privacidade:** `attached_assets/` é trackado no git (só `generated_images/`, `*.txt` e `*.log`
> são ignorados) e vai pro GitHub no push. **Nunca** commitar prints/dumps/logs com PII (telefones,
> user ids). `.gitignore` já bloqueia `attached_assets/*.txt` e `attached_assets/*.log` (exports do
> Telegram e deployment logs). Outros assets só locais → `.gitignore` ou `git rm --cached`
> (destrutivo — project task / usuário).

---

## 📁 Estrutura de arquivos
- `main.py` — **entrypoint/facade** (preservado p/ `python main.py` do Railway). Re-exporta tudo de
  `royal/` (testes fazem `import main; main.<símbolo>`), inclui os Routers das features na ordem
  correta e define `main()`. **Ordem de include (CRÍTICO):** `system` POR ÚLTIMO (catch-all `track`)
  e `hub` ANTES de `cfg`/`missoes` (o `hub_cb` casa `r:cfg:`/`r:quest:` — ver comentário no arquivo).
- `royal/` — pacote modular do bot (era o monólito `main.py`; split **puramente estrutural**, código
  verbatim, zero mudança de comportamento/DB):
  - `royal/config.py` — env vars + constantes de configuração (folha do grafo, sem deps internas).
  - `royal/core.py` — `bot`/`dp`, middlewares, schema/migrations, helpers, constantes não-config
    (CLASSES/ITEMS/ROYAL_HELP/ROYAL_TUTORIAL_PARTS etc.). Importa só `config`.
  - `royal/jobs.py` — tarefas de fundo (auto-casório, log dump, backup, `_on_shutdown`). Importa `core`.
  - `royal/handlers/` — 1 módulo por feature, cada um expõe um `router` aiogram (handlers via
    `@router.…`): admin, avatar, boss, casorios, cfg, classe, conquistas, economia, eventos, hub,
    inline, inventario, loja, missoes, palavra, perfil, privacidade, ranking, start, system.
    Importam `config`/`core`. Grafo acíclico: `config ← core ← {jobs, handlers} ← main`.
- `royal_render.py` — gerador de cards 1080×1080 (Pillow puro, sem Chromium).
- `royal_words.py` — palavras e charadas do mini-game.
- `requirements.txt` (runtime: `aiogram`, `Pillow`, `aiohttp`) · `requirements-dev.txt` (SÓ dev/CI:
  `pytest`, `ruff`; não vão pro Railway).
- `tests/` — `conftest.py` (DB tmp), `test_pure.py`, `test_db_smoke.py`.
- `docs/ARCHITECTURE.md` — referência detalhada (features M-series, Premium, infra F-series, UI,
  identidade visual).
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

## 🧩 Ordem de handlers & filtros de entrada (gotchas ativos)

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

## 🧠 Memórias que agentes costumam errar (não reverter)

- **Botões coloridos nativos EXISTEM** no Bot API 10 / aiogram 3.28.2 via campo `style`
  (`'success'`/`'danger'`/`'primary'`; **não há** `'warning'`/amarelo). Já em produção. Se um agente
  afirmar "não existe", está errado. → detalhe + convenções em `docs/ARCHITECTURE.md` (§ UI).
- **Premium REMOVIDO por completo** (loja Premium, pagamento Telegram Stars/XTR, assinatura Royal
  Plus, perks pagos, `/royalpaldica`, skin dourada e badge violeta no card). O bot **nunca cobra
  nada**. As colunas/tabela do DB (`xp_boost_until`, `prm_hints`, `prm_ressurrects`, `prm_skin_gold`,
  `royal_plus_until`, `royal_plus_charge_id`, `stars_purchases`) ficaram **inertes** (migrations v10/v11
  mantidas só pra não dropar dados — nenhum código lê/escreve nelas). Não reintroduzir cobrança.
- **Cursor sqlite global `cur` é seguro** no design atual (asyncio single-thread, sem `await` entre
  `execute`/`fetch`). O rewrite p/ `aiosqlite` (214 call sites) **não foi feito** de propósito (alto
  risco). Não "consertar" isso sem alinhamento. → `docs/ARCHITECTURE.md` (§ Infra F01/F02).
- **Anti-duplicação de spawns** (PALAVRA/BAÚ) depende de claims atômicos no DB — não remover os
  `UPDATE … WHERE` de guarda. → `docs/ARCHITECTURE.md` (§ Infra, anti-duplicação).
- **Cards são card-first** com fallback texto; emoji de título é stripado antes do Pillow (senão
  tofu). → `docs/ARCHITECTURE.md` (§ Features, nota card-first).
- **Ícones de classe/item nos cards são sprites pixel-art, NÃO emoji-fonte.** As fontes do projeto
  não cobrem 👑🌹🧙📜🧪🥾💍 → davam tofu. `draw_icon_centered()` mapeia emoji→`SPRITE_*` via
  `EMOJI_SPRITES` (chaves sem `U+FE0F`). Ao criar classe/item novo, adicione o sprite + entrada no
  mapa senão o tofu volta. Emoji em menu/texto (ex. 🐉) renderiza nativo no Telegram — não precisa
  sprite. → `docs/ARCHITECTURE.md` (§ Identidade visual).
- **`ROADMAP.md` está desatualizado** — a fonte de verdade do estado real é o código + `docs/`.
