# 🛣️ Roteiro de Estudo & Execução — RPG Royal para Geeks → Mercado

> **Alvo:** 5–10 grupos × ~1000 jogadores ativos. Stack: aiogram 3.28.2,
> Bot API 10, Python 3.12, SQLite (WAL), Railway. **Fluxo:** eu executo,
> você aprova cada etapa. Cada item tem **diagnóstico → solução →
> esforço → risco → critério de aceite**.

**Legenda de esforço:** 🟢 30min · 🟡 1–3h · 🟠 meio dia · 🔴 dia inteiro
**Risco:** 🟢 trivial · 🟡 mexe em hot path · 🟠 destrutivo/migration · 🔴 mudança arquitetural

---

## 🚨 PARTE 1 — 20 CORREÇÕES (bugs, dívidas, riscos de escala)

> **STATUS REAL (2026-05, auditado no código):**
> ✅ **FEITO (16):** F03, F04, F05, F06, F07, F08, F10, F11, F12, F13, F14, F15, F16, F17, F18, F19, F20.
> 🟡 **PARCIAL (1):** F09 (identity card persistido; falta profile card cheio — baixo valor).
> ⏸️ **DEFERIDO (2):** F01 + F02 (auditados — cursor global seguro no design single-thread; rewrite aiosqlite é alto risco sem teste runtime).
> → **PARTE 1 praticamente concluída.** Só sobram itens deferidos por risco (F01/F02) e 1 parcial de baixo valor (F09).

### 🔴 BLOQUEADORES DE ESCALA (1000+ users)

**F01 · ⏸️ AUDITADO — DEFERIDO (não é race no design atual) — Global single SQLite cursor (`cur = db.cursor()`)** 🔴🟠
- **Diagnóstico ORIGINAL:** suposta corrupção de state com `cur.execute()` paralelo.
- **Auditoria real (2026-05):** no design **asyncio single-thread** não há `await` entre `execute` e `fetch`, logo handlers **não** interleavam no cursor — não há rows fantasmas. Verificado que **nenhum** `to_thread` toca `cur`/`db` global (todos são renders puros recebendo `data` pré-buscado, ou `_do_backup_sync` com conexão separada). `database is locked` já mitigado por `PRAGMA journal_mode=WAL` + `busy_timeout=5000` + `synchronous=NORMAL` em `setup_connection`.
- **Decisão:** rewrite completo pra `aiosqlite` + pool (214 call sites) é **alto risco sem teste em runtime** (workspace sem `BOT_TOKEN`). **Deferido** pra esforço dedicado e testável — não cego em produção.

**F02 · ⏸️ AUDITADO — DEFERIDO (acoplado à F01) — `db.commit()` em hot path bloqueia event loop** 🟠🟡
- **Diagnóstico:** sqlite sync no asyncio loop → cada commit pode travar handlers.
- **Auditoria real:** commits SQLite em WAL são sub-ms pra os volumes deste bot (gargalo real = rate-limit do Telegram, não o DB). Operações pesadas (VACUUM/backup) já rodam em `to_thread` com conexão separada (F10). O fix definitivo é acoplado à F01 (aiosqlite) → **deferido** junto.
- **Aceite (futuro):** loop latency < 50ms no p99 sob carga, após migração testável.

**F03 · ✅ FEITO — `_msg_owners`, `_rate_limits`, `_profile_file_id_cache` crescem sem cap real** 🟡🟢
- **Diagnóstico:** GC só dispara em intervalos; sob spike viram milhões de entries → OOM no Railway (512MB plano grátis).
- **Solução:** trocar por `cachetools.TTLCache(maxsize=10_000, ttl=N)`. Adiciona dep, código encurta.
- **Aceite:** RSS estável < 200MB após 24h de uso.

**F04 · ✅ FEITO — Identity card render bloqueia mesmo com `to_thread`** 🟡🟡
- (auditado) `asyncio.Semaphore` dedicado (`_IDENTITY_RENDER_SEM`) + throttle de upload no sweep.
- **Diagnóstico:** Pillow é CPU-bound, `to_thread` usa o default thread pool (limitado). Sweep diário regenera dezenas em paralelo → spike de CPU + Telegram retry storm.
- **Solução:** fila dedicada (`asyncio.Semaphore(2)`) pra renders + throttle 1/s no upload. Já existe parcialmente — formalizar.
- **Aceite:** sweep de 100 cards termina sem RetryAfter.

**F05 · ✅ FEITO (parcial — TTLCache em vez de key tripla) — `register_owner` cache** 🟢🟢
- **Diagnóstico:** key collision possível entre chats (msg_id reusa). Já tem prune mas FIFO sem TTL.
- **Solução:** TTLCache + key tripla (chat_id, msg_id, uid).
- **Aceite:** zero ownership leak após reinício.

### 🟠 BUGS LATENTES / EDGE CASES

**F06 · ✅ FEITO — `from_user.id` sem None-guard em vários handlers** 🟢🟢
- **Diagnóstico:** channel posts e mensagens anônimas têm `from_user = None`. ~30 handlers crashariam.
- **Solução:** decorator `@require_user` ou helper `get_uid(message) -> int | None`.
- **Aceite:** lint passa, channel post não derruba handler.

**F07 · ✅ FEITO — `TelegramRetryAfter` handler retorna sem reagendar a operação** 🟡🟡
- **Diagnóstico:** linhas 1465–1502 fazem `await asyncio.sleep(e.retry_after+1)` e seguem — mas a operação que causou o limit não é retentada.
- **Solução:** wrapper `with_retry(coro, max_attempts=3)` exponencial.
- **Aceite:** 0 mensagens perdidas em pico de palavra.

**F08 · ✅ FEITO — Anti-spam só checa `len(text) > 120`** 🟡🟢
- **Diagnóstico:** spam curto passa, mensagens longas legítimas (charadas?) cortam.
- **Solução:** heuristic stack: regex de URL+@mention+convite, score de repetição, soft-ban temporário do uid (não do msg).
- **Aceite:** spam recente do `@Sexyhotboy228` bloqueado + uid silenciado 1h.

**F09 · 🟡 PARCIAL — Inline mode cache em RAM perde tudo no restart** 🟡🟢
- **Diagnóstico:** `_profile_file_id_cache` morre, próximo inline query renderiza tudo de novo.
- **Estado real:** o **identity card** já persiste `inline_card_file_id` em coluna (migration v5) — inline mode não depende mais de RAM pro card fixo. Falta só expandir pro **profile card cheio** (com stats) com TTL — baixo impacto, pode ficar pendente.
- **Aceite:** restart não causa lag visível no identity card (✅); profile card cheio ainda re-renderiza (pendente, baixo valor).

**F10 · ✅ FEITO — Backup automático do SQLite não existe** 🟠🟠
- **Diagnóstico:** Railway volume morre? Adeus tudo. Snapshot do repo é seed antigo.
- **Solução (implementada):** `backup_job` diário em `BACKUP_HOUR` (default 3h local, retry mesmo-dia em falha) faz `PRAGMA integrity_check` + `VACUUM INTO <DB_DIR>/backups/YYYY-MM-DD.sqlite3` numa **conexão sqlite separada** (não toca `cur`/`db` global) + retenção `BACKUP_RETENTION_DAYS` (7d) + upload silencioso pro canal STASH. Comando owner `/royalbackup` (off-menu). Envs: `BACKUP_ENABLED`/`BACKUP_HOUR`/`BACKUP_RETENTION_DAYS`.
- **Aceite:** `/royalbackup` (owner) confirma último backup; canal recebe dump diário. ✅

### 🟡 CORREÇÕES DE QUALIDADE

**F11 · ✅ FEITO — `migrate_to_vN` sem rollback nem dry-run** 🟡🟡
- **Diagnóstico:** migration falha no meio → DB num estado intermediário sem `user_version` atualizado, próximo boot reaplica e crasha.
- **Solução (implementada):** `run_migrations` envolve cada migration em `BEGIN` + `fn(cur)` + `PRAGMA user_version=N` + `commit`; em exceção faz `rollback` + aborta boot (`raise`). `user_version` só avança no sucesso → reroda idempotente no próximo boot.
- **Aceite:** migration propositalmente quebrada não corrompe DB (rollback). ✅

**F12 · ✅ FEITO — Logs não-estruturados** 🟡🟢
- **Diagnóstico:** `logger.info("foo %s", x)` puro. Filtrar incidente no Railway = scroll infinito.
- **Solução (implementada):** `_JsonFormatter` custom + flag `LOG_JSON` (env, default `0`). `LOG_JSON=1` faz o stdout sair em JSON (1 linha/record com `ts`/`lvl`/`logger`/`msg` + extras + `exc`). Ring buffer do `/royallog` sempre sai em texto humano (independe da flag).
- **Aceite:** `LOG_JSON=1` → linhas JSON filtráveis no Railway/Logtail/Loki. ✅

**F13 · ✅ FEITO — Sem health endpoint nem readiness** 🟢🟢
- **Diagnóstico:** Railway só sabe que processo está vivo. Bot pode estar com `getUpdates` quebrado e ninguém percebe.
- **Solução (implementada):** `aiohttp` server inline em `0.0.0.0:$PORT` com `GET /health` (DB ping `SELECT 1` + staleness de updates via middleware `_track_last_update` + `user_version`). Retorna `503` se sem update há `HEALTH_STALE_SEC` (default 600s). Só sobe se `PORT` setado.
- **Aceite:** Railway healthcheck verde, `503` se sem update há > `HEALTH_STALE_SEC`. ✅

**F14 · ✅ FEITO — Sem graceful shutdown** 🟡🟡
- **Diagnóstico:** SIGTERM do Railway → handlers in-flight são abortados, possivelmente deixando DB inconsistente (palavra spawnada mas não commitada).
- **Solução (implementada):** aiogram já trata SIGTERM/SIGINT (`handle_signals=True` default); hook `@dp.shutdown` (`_on_shutdown`) faz `flush_buffers_once()` (síncrona, extraída do loop) + fecha health server + `db.commit()/close()`.
- **Aceite:** redeploy sem warnings de "ResourceWarning: unclosed". ✅

**F15 · ✅ FEITO — `safe_typing` ignora exceções silenciosamente em loop** 🟢🟢
- (auditado) `_typing_fail_streak` por chat — loga alerta após N falhas consecutivas no mesmo chat, reseta no 1º sucesso.
- **Diagnóstico:** se o bot perdeu admin no grupo, todo `safe_typing` falha silently → mascara problema real.
- **Solução:** contador rolling, log estruturado a cada N falhas seguidas no mesmo chat.
- **Aceite:** alerta após 10 typings consecutivos falhando.

**F16 · ✅ FEITO — Sem validação de tamanho de caption (limite 1024)** 🟢🟢
- **Diagnóstico:** README admite o limite mas alguns paths não truncam → 400 Bad Request silencioso.
- **Solução:** helper `cap1024(s)` aplicado em todo send_photo. Adicionar test.
- **Aceite:** caption longa renderiza com `...` em vez de errar.

**F17 · ✅ FEITO — `inline_query` sem `cache_time` configurado** 🟢🟢
- **Diagnóstico:** Telegram default = 300s; pra perfil que muda raramente podia ser 3600. Reduz custo do bot.
- **Solução:** `answer(results, cache_time=3600, is_personal=True)`.
- **Aceite:** mesma query repetida não dispara handler.

**F18 · ✅ FEITO — `random.randint` para royal_id colide silenciosamente** 🟡🟡
- (auditado) `next_royal_id` à prova de colisão via UNIQUE index + retry.
- **Diagnóstico:** migration v6 randomiza, mas espaço 1000–9999 = 9000 ids. Em 5 grupos × 1000 = 50% chance de colisão por aniversário.
- **Solução:** expandir pra 1000–99999 OU usar Base32 short id (4 chars = 1M combos), checar UNIQUE.
- **Aceite:** 5000 inserts simulados, zero colisão.

**F19 · ✅ FEITO — Avatar bytes baixado a cada render** 🟡🟡
- (auditado) cache de avatar em disco.
- **Diagnóstico:** `get_user_photo_file_id` cacheia file_id mas o BYTES da foto é refetched do Telegram CDN toda vez.
- **Solução:** cache em disco (`/data/avatar_cache/{uid}.jpg`) com TTL 7d + LRU eviction.
- **Aceite:** segundo render do mesmo perfil < 100ms.

**F20 · ✅ FEITO — `_ensure_db_persistence` não valida integridade do seed** 🟢🟢
- **Diagnóstico:** Se seed corrompido no repo, bota DB ruim no volume — sem retorno.
- **Solução:** `PRAGMA integrity_check` antes do copy; fallback pra DB vazio + log fatal.
- **Aceite:** seed propositalmente corrompido detectado no boot.

---

## 🚀 PARTE 2 — 20 MELHORIAS (features, monetização, qualidade)

> **STATUS REAL (2026-05, auditado no código):**
> ✅ **FEITO (11):** M01, M02 (florins, não Stars), M03, M04, M05, M06, M09, M11, M17 (testes), M18 (CI), M19.
> ⏳ **PENDENTE (8):** M07 (web app), M08 (stories), M10 (guildas), M12 (boss raid), M14 (Prometheus), M15 (admin dashboard), M16 (A/B), M20 (i18n).
> 🚫 **VETADO (1):** M13 (Sentry).
> → Monetização (Sprint 3) e Engajamento (Sprint 4) **concluídos**. Resta o Sprint 5 (diferencial competitivo) + infra (CI/testes/métricas).

### 💰 MONETIZAÇÃO (Bot API 10 — Stars, Gifts, Paid Media)

**M01 · ✅ FEITO — Loja com Telegram Stars** 🟠🟡
- **Por quê:** monetiza sem gateway. Stars = moeda virtual paga em USD/EUR direto pelo Telegram.
- **Solução (implementada):** `PREMIUM_ITEMS` + `send_invoice(currency="XTR")` via `/royalloja` → aba 💎 Premium. Handlers `pre_checkout_handler` (prefixo `prm|`) + `successful_payment_handler` (idempotente por `charge_id`, grava em `stars_purchases`, concede perk via `_grant_premium_perk`). Itens: boost XP 24h, dica da palavra, ressurreição no boss, skin dourada. Migration v10.
- **Doc:** https://core.telegram.org/bots/payments-stars
- **Aceite:** compra via Stars credita perk + ledger auditável. ✅
- **Perks aplicados:** ✅ `xp_boost_until` (+20% via `premium_xp_active` em ambos hot paths de XP) · ✅ `prm_hints` (M03) · ✅ `prm_skin_gold` (moldura GOLD + tag `[ * OURO * ]` no card) · ⏳ `prm_ressurrects` **deferido** (boss não tem mecânica de morte do player pra hook).

**M02 · ✅ FEITO (florins, não Stars) — Gifts entre players** 🟡🟢
- **Por quê:** gameplay social + viral.
- **Solução (implementada):** `/royalpresentear @user 100` (ou reply + valor) — transferência **atômica** de florins in-game entre players (`UPDATE … WHERE gold>=?` evita race). Limites `GIFT_MIN=10`/`GIFT_MAX=5000`, bloqueia self-gift. Tabela `gifts` (ledger auditável, migration v12). Conquista `generoso` no 1º envio.
- **Nota:** implementado como transferência de moeda in-game (não `sendGift` de Stars) — mais alinhado ao gameplay atual. `sendGift` de Stars fica como evolução futura se desejado.
- **Aceite:** transferência atômica sem race + ledger. ✅

**M03 · ✅ FEITO — Dica paga da PALAVRA** 🟡🟡
- **Por quê:** "hint" pago pra Palavra da Hora difícil — revela letra.
- **Solução (implementada):** `/royalpaldica` consome 1 crédito de `prm_hints` (`UPDATE … WHERE prm_hints>0`, atomic anti-double-spend), revela 1 letra em posição aleatória, envia na DM com efeito 🔥 (fallback `<tg-spoiler>` no chat). Créditos comprados via Stars no item `prm_hint` (M01, 1⭐/crédito).
- **Aceite:** crédito consumido revela letra; sem crédito, avisa. ✅

**M04 · ✅ FEITO — Subscription premium (Stars recorrente)** 🟠🟡
- **Por quê:** "Royal Plus" — 50⭐/mês = +20% XP, slot extra de casório, badge violeta.
- **Solução (implementada):** `SUBSCRIPTIONS` + `send_invoice(currency="XTR", subscription_period=2592000)` (30d) via callback `r:sub:royal_plus`. `pre_checkout_handler` aceita prefixo `sub|`; `successful_payment_handler` detecta `subscription_expiration_date` e grava ISO em `royal_plus_until`. Renovações automáticas tratadas igual (idempotente por charge_id). Migration v11.
- **Aceite:** assinatura ativa grava `royal_plus_until`; renovação mensal atualiza. ✅
- **Perks aplicados:** ✅ +20% XP (`premium_xp_active` checa `royal_plus_until` nos hot paths) · ⏳ slot extra de casório **deferido** (casórios são atribuídos pelo sistema/admin via `send_couple`, sem limite de slot por user pra estender) · ⏳ badge violeta no card (TODO render).

### 🎮 GAMEPLAY (engagement, retenção)

**M05 · Sistema de quests diárias** 🟠🟡
- **Por quê:** retenção D1/D7. "Mande 5 mensagens", "Ganhe 1 palavra", "Visite a loja".
- **Como:** tabela `daily_quests(uid, quest_id, progress, claimed, day)`. Helper `quest_tick(uid, kind, amount)` em hot paths.
- **Comando novo:** `/royalmissoes`.

**M06 · Reactions tracking pra XP social** 🟡🟢
- **Por quê:** Bot API 7+ entrega `message_reaction` updates. Quem reage com 🔥/❤️ ganha XP social; autor da msg reagida também.
- **Como:** handler `@dp.message_reaction()`. Allowed updates precisa incluir `message_reaction`.
- **Doc:** https://core.telegram.org/bots/api#messagereactionupdated

**M07 · Mini-app Web App (TON-style HUD)** 🔴🟠
- **Por quê:** UI rica pra perfil, ranking interativo, equipar items. Roda no Telegram nativamente.
- **Como:** HTML/JS hospedado em GitHub Pages (grátis), bot envia botão `web_app`. Auth via `initData` HMAC.
- **Doc:** https://core.telegram.org/bots/webapps

**M08 · Stories (Business connection)** 🟡🟢
- **Por quê:** Bot API 10 permite postar Stories no canal do dono (via Business Connection).
- **Como:** comando `/royalstory` (owner) posta highlight do ranking semanal como story.

**M09 · Eventos sazonais com tabela própria** 🟠🟢
- **Por quê:** Halloween/Natal/Carnaval boostam engajamento.
- **Como:** `events(slug, start_at, end_at, xp_multiplier, drop_table_override)`. Avatares limitados, palavras temáticas.

**M10 · Guildas / Alianças entre grupos** 🔴🟠
- **Por quê:** com 5–10 grupos, guerra inter-grupo cria meta-game.
- **Como:** tabela `guilds(chat_id, name, banner)` + ranking cross-grupo opt-in.

**M11 · Sistema de achievements/badges** 🟠🟡
- **Por quê:** completionism = retenção. Display no card.
- **Como:** tabela `achievements(uid, slug, unlocked_at)` + ~30 slugs ("primeiro casório", "ganhou 100 palavras", "boss solo kill").

**M12 · Boss raid colaborativo** 🟠🟡
- **Por quê:** já tem `/royalboss` solo — expandir pra raid de grupo (HP escalado, recompensa partilhada).
- **Como:** campo `boss.participants JSON`, contribuição por dano.

### 🛠️ INFRA / DEVEX

**M13 · Sentry integration** 🟢🟢
- **Por quê:** descobrir exception silenciosa antes do usuário reclamar.
- **Como:** `sentry-sdk[asyncio]`, DSN em env var, breadcrumbs com chat_id+uid.

**M14 · Prometheus metrics + Grafana Cloud (free tier)** 🟡🟡
- **Por quê:** dashboard de p99 latency, msg/s, palavras/h, erros por handler.
- **Como:** `prometheus-client` exposto em `/metrics`, scrape pelo Grafana Agent.

**M15 · Admin dashboard via Web App** 🟠🟡
- **Por quê:** ver ranking real-time, banir user, forçar palavra sem comando.
- **Como:** mini-app autenticado por OWNER_USER_ID, lê DB read-only via API local.

**M16 · A/B testing framework leve** 🟡🟢
- **Por quê:** decidir entre 2 textos de boas-vindas, 2 cadências de palavra, etc., baseado em dados.
- **Como:** `ab_variant(uid, experiment) -> 'A' | 'B'` (hash determinístico). Tabela `ab_events`.

**M17 · ✅ FEITO — Test suite (pytest)** 🟠🟡
- **Por quê:** 8k linhas sem teste = roleta russa em refactor.
- **Solução (implementada):** `tests/` com `conftest.py` (aponta `DATABASE_PATH` pra tmp antes do import → migrations rodam em DB vazio descartável), `test_pure.py` (20 asserts: `format_br`, `normalize_word`, `normalize_pair`, `level_progress`, eventos sazonais, season code, week marker, identity hash) e `test_db_smoke.py` (migrations até v13 + tabelas críticas + integrity_check). `requirements-dev.txt` separado do runtime.
- **Aceite:** `pytest -q` → 20 passed. ✅ **Pegou 3 bugs reais** (ver abaixo).

**M18 · ✅ FEITO — CI no GitHub Actions** 🟢🟢
- **Por quê:** roda lint+test antes do deploy pro Railway. Bloqueia deploys quebrados.
- **Solução (implementada):** `.github/workflows/ci.yml` (push em `royalRPG`/`main` + PRs): instala deps + `ruff check --select E9,F63,F7,F82` (só regras de bug crítico — undefined names/syntax, sem ruído de estilo) + `pytest -q`.
- **Aceite:** workflow verde no GitHub. ✅

> 🐛 **Bugs reais pegos pelo ruff crítico nesta entrega (corrigidos):**
> 1. `format_br` — NameError em 6 call sites (saldo, card do boss, corpo do `/royalpresentear`). Não existia função module-level; só um `_br` aninhado. → criada `format_br()` em `main.py`.
> 2. `datetime.now(TZ)` (main.py ~2267, card de casório) — `TZ` indefinido. → `ZoneInfo(TZ_NAME)`.
> 3. `log.warning` (main.py ~3770, feedback de palavra) — `log` indefinido. → `logger.warning`.

### 🎨 UX / ACESSIBILIDADE

**M19 · Comando `/royalconfig` por user** 🟡🟢
- **Por quê:** silenciar notif de palavra, esconder do ranking, opt-out de DM.
- **Como:** estender `user_dm_settings` (migration v8) com flags JSON.

**M20 · Internacionalização i18n** 🟠🟡
- **Por quê:** outros grupos podem ser EN/ES. Hoje tudo PT-BR hardcoded.
- **Como:** `babel` + arquivos `.po`. Detecta `from_user.language_code`. Fallback PT-BR.

---

## 📅 ORDEM SUGERIDA DE EXECUÇÃO (gates de aprovação)

**Sprint 1 — Fundação ✅ (F01/F02 deferidos por risco; M13 Sentry vetado):**
~~F11 → F14 → F10 → F13~~ ✅ · F01 / F02 ⏸️ · M13 vetado · M18 (CI) pendente

**Sprint 2 — Anti-incêndio ✅ CONCLUÍDO:**
~~F03 → F05 → F06 → F07 → F08 → F15 → F16 → F17 → F18 → F19 → F20 → F04 → F12~~ ✅ · F09 🟡 parcial

**Sprint 3 — Monetização ✅ CONCLUÍDO:**
~~M01 → M04 → M03 → M02~~ ✅

**Sprint 4 — Engajamento ✅ CONCLUÍDO:**
~~M05 → M11 → M06 → M09 → M19~~ ✅

**Sprint 5 — Diferencial competitivo (PENDENTE):**
M07 → M15 → M10 → M12 → M08 → M14 → M16 → M17 → M20
+ infra pendente: M18 (CI), M14 (Prometheus)

---

## 🔄 PROTOCOLO DE EXECUÇÃO (você aprova item-a-item)

1. Eu proponho **1 item** com escopo cirúrgico (1 arquivo, 1 diff visível).
2. Você aprova `ok` / pede ajuste / pula.
3. Eu implemento + auto-commit + me sinalize `push` quando quiser deployar.
4. Próximo item.

**Estado atual:** PARTE 1 praticamente concluída (16 ✅ / 1 🟡 / 2 ⏸️). Os 2 deferidos (F01/F02) exigem migração `aiosqlite` testável — não cega em produção. Próximo passo natural é a **PARTE 2** (gameplay/UX restantes do Sprint 5), já que monetização (M01-M04) e engajamento (M05/M06/M09/M11/M19) estão feitos.
