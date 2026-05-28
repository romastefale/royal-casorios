# 🛣️ Roteiro de Estudo & Execução — RPG Royal para Geeks → Mercado

> **Alvo:** 5–10 grupos × ~1000 jogadores ativos. Stack: aiogram 3.28.2,
> Bot API 10, Python 3.12, SQLite (WAL), Railway. **Fluxo:** eu executo,
> você aprova cada etapa. Cada item tem **diagnóstico → solução →
> esforço → risco → critério de aceite**.

**Legenda de esforço:** 🟢 30min · 🟡 1–3h · 🟠 meio dia · 🔴 dia inteiro
**Risco:** 🟢 trivial · 🟡 mexe em hot path · 🟠 destrutivo/migration · 🔴 mudança arquitetural

---

## 🚨 PARTE 1 — 20 CORREÇÕES (bugs, dívidas, riscos de escala)

### 🔴 BLOQUEADORES DE ESCALA (1000+ users)

**F01 · Global single SQLite cursor (`cur = db.cursor()` linha 223)** 🔴🟠
- **Diagnóstico:** todo o bot compartilha **um único cursor**. Em 5–10 grupos × 1000 users com handlers concorrentes, `cur.execute()` paralelo corrompe state (fetch de um handler "rouba" linhas de outro). Já há sintomas: race quando 2 users mandam palavra ao mesmo tempo.
- **Solução:** migrar pra `aiosqlite` (já permitido) com **connection pool** de 4–8 conexões. Wrapper `async def query(sql, params) -> rows` + `async def execute(sql, params)`. Refatorar os ~214 `cur.execute` em chunks (perfil → palavra → boss → casórios → loja).
- **Aceite:** stress test com 200 mensagens/seg em 1 grupo sem `database is locked` nem rows fantasmas.

**F02 · `db.commit()` em hot path bloqueia event loop** 🟠🟡
- **Diagnóstico:** sqlite sync no asyncio loop → cada commit (~5–20ms em WAL) trava TODOS os handlers. Em pico vira fila.
- **Solução:** mover todas as escritas pra `asyncio.to_thread` (paliativo) OU completar migração pro aiosqlite (definitivo, junto da F01).
- **Aceite:** loop latency < 50ms no p99 sob carga.

**F03 · `_msg_owners`, `_rate_limits`, `_profile_file_id_cache` crescem sem cap real** 🟡🟢
- **Diagnóstico:** GC só dispara em intervalos; sob spike viram milhões de entries → OOM no Railway (512MB plano grátis).
- **Solução:** trocar por `cachetools.TTLCache(maxsize=10_000, ttl=N)`. Adiciona dep, código encurta.
- **Aceite:** RSS estável < 200MB após 24h de uso.

**F04 · Identity card render bloqueia mesmo com `to_thread`** 🟡🟡
- **Diagnóstico:** Pillow é CPU-bound, `to_thread` usa o default thread pool (limitado). Sweep diário regenera dezenas em paralelo → spike de CPU + Telegram retry storm.
- **Solução:** fila dedicada (`asyncio.Semaphore(2)`) pra renders + throttle 1/s no upload. Já existe parcialmente — formalizar.
- **Aceite:** sweep de 100 cards termina sem RetryAfter.

**F05 · `register_owner` cache de 4000 mensagens com chave (chat_id, msg_id)** 🟢🟢
- **Diagnóstico:** key collision possível entre chats (msg_id reusa). Já tem prune mas FIFO sem TTL.
- **Solução:** TTLCache + key tripla (chat_id, msg_id, uid).
- **Aceite:** zero ownership leak após reinício.

### 🟠 BUGS LATENTES / EDGE CASES

**F06 · `from_user.id` sem None-guard em vários handlers** 🟢🟢
- **Diagnóstico:** channel posts e mensagens anônimas têm `from_user = None`. ~30 handlers crashariam.
- **Solução:** decorator `@require_user` ou helper `get_uid(message) -> int | None`.
- **Aceite:** lint passa, channel post não derruba handler.

**F07 · `TelegramRetryAfter` handler retorna sem reagendar a operação** 🟡🟡
- **Diagnóstico:** linhas 1465–1502 fazem `await asyncio.sleep(e.retry_after+1)` e seguem — mas a operação que causou o limit não é retentada.
- **Solução:** wrapper `with_retry(coro, max_attempts=3)` exponencial.
- **Aceite:** 0 mensagens perdidas em pico de palavra.

**F08 · Anti-spam só checa `len(text) > 120`** 🟡🟢
- **Diagnóstico:** spam curto passa, mensagens longas legítimas (charadas?) cortam.
- **Solução:** heuristic stack: regex de URL+@mention+convite, score de repetição, soft-ban temporário do uid (não do msg).
- **Aceite:** spam recente do `@Sexyhotboy228` bloqueado + uid silenciado 1h.

**F09 · Inline mode cache em RAM perde tudo no restart** 🟡🟢
- **Diagnóstico:** `_profile_file_id_cache` morre, próximo inline query renderiza tudo de novo.
- **Solução:** persistir `inline_file_id` em coluna (já temos pra identity card; expandir pra profile card cheio com TTL de 24h).
- **Aceite:** restart não causa lag visível.

**F10 · Backup automático do SQLite não existe** 🟠🟠
- **Diagnóstico:** Railway volume morre? Adeus tudo. Snapshot do repo é seed antigo.
- **Solução:** cron interno diário (3h local) faz `VACUUM INTO /data/backups/YYYY-MM-DD.sqlite3` + retenção 7 dias + upload opcional pro canal STASH (Telegram = storage grátis).
- **Aceite:** `/royalbackup` (owner) confirma último backup; canal recebe dump diário.

### 🟡 CORREÇÕES DE QUALIDADE

**F11 · `migrate_to_vN` sem rollback nem dry-run** 🟡🟡
- **Diagnóstico:** migration falha no meio → DB num estado intermediário sem `user_version` atualizado, próximo boot reaplica e crasha.
- **Solução:** `BEGIN/COMMIT` envolvendo cada migration + log estruturado.
- **Aceite:** migration v8 fake propositalmente quebrada não corrompe DB.

**F12 · Logs não-estruturados** 🟡🟢
- **Diagnóstico:** `logger.info("foo %s", x)` puro. Filtrar incidente no Railway = scroll infinito.
- **Solução:** `structlog` ou `python-json-logger`. Campos: chat_id, uid, royal_id, action, latency_ms.
- **Aceite:** `rg '"action":"palavra_won"' logs.json` funciona.

**F13 · Sem health endpoint nem readiness** 🟢🟢
- **Diagnóstico:** Railway só sabe que processo está vivo. Bot pode estar com `getUpdates` quebrado e ninguém percebe.
- **Solução:** httpx server inline na port `$PORT` com `/health` (DB ping + última update timestamp).
- **Aceite:** Railway healthcheck verde, alerta se sem update há >5min.

**F14 · Sem graceful shutdown** 🟡🟡
- **Diagnóstico:** SIGTERM do Railway → handlers in-flight são abortados, possivelmente deixando DB inconsistente (palavra spawnada mas não commitada).
- **Solução:** `signal.SIGTERM` handler aguarda tasks pendentes (max 25s) + `db.close()`.
- **Aceite:** redeploy sem warnings de "ResourceWarning: unclosed".

**F15 · `safe_typing` ignora exceções silenciosamente em loop** 🟢🟢
- **Diagnóstico:** se o bot perdeu admin no grupo, todo `safe_typing` falha silently → mascara problema real.
- **Solução:** contador rolling, log estruturado a cada N falhas seguidas no mesmo chat.
- **Aceite:** alerta após 10 typings consecutivos falhando.

**F16 · Sem validação de tamanho de caption (limite 1024)** 🟢🟢
- **Diagnóstico:** README admite o limite mas alguns paths não truncam → 400 Bad Request silencioso.
- **Solução:** helper `cap1024(s)` aplicado em todo send_photo. Adicionar test.
- **Aceite:** caption longa renderiza com `...` em vez de errar.

**F17 · `inline_query` sem `cache_time` configurado** 🟢🟢
- **Diagnóstico:** Telegram default = 300s; pra perfil que muda raramente podia ser 3600. Reduz custo do bot.
- **Solução:** `answer(results, cache_time=3600, is_personal=True)`.
- **Aceite:** mesma query repetida não dispara handler.

**F18 · `random.randint` para royal_id colide silenciosamente** 🟡🟡
- **Diagnóstico:** migration v6 randomiza, mas espaço 1000–9999 = 9000 ids. Em 5 grupos × 1000 = 50% chance de colisão por aniversário.
- **Solução:** expandir pra 1000–99999 OU usar Base32 short id (4 chars = 1M combos), checar UNIQUE.
- **Aceite:** 5000 inserts simulados, zero colisão.

**F19 · Avatar bytes baixado a cada render** 🟡🟡
- **Diagnóstico:** `get_user_photo_file_id` cacheia file_id mas o BYTES da foto é refetched do Telegram CDN toda vez.
- **Solução:** cache em disco (`/data/avatar_cache/{uid}.jpg`) com TTL 7d + LRU eviction.
- **Aceite:** segundo render do mesmo perfil < 100ms.

**F20 · `_ensure_db_persistence` não valida integridade do seed** 🟢🟢
- **Diagnóstico:** Se seed corrompido no repo, bota DB ruim no volume — sem retorno.
- **Solução:** `PRAGMA integrity_check` antes do copy; fallback pra DB vazio + log fatal.
- **Aceite:** seed propositalmente corrompido detectado no boot.

---

## 🚀 PARTE 2 — 20 MELHORIAS (features, monetização, qualidade)

### 💰 MONETIZAÇÃO (Bot API 10 — Stars, Gifts, Paid Media)

**M01 · Loja com Telegram Stars** 🟠🟡
- **Por quê:** monetiza sem gateway. Stars = moeda virtual paga em USD/EUR direto pelo Telegram.
- **Como:** `sendInvoice` com `currency="XTR"` (Stars). Itens: skins de avatar, classes premium, boost de XP 24h, palavra hint, ressurrect no boss.
- **Comando:** estender `/royalloja` com aba "Premium" — usuário aceita o invoice no próprio chat.
- **Doc:** https://core.telegram.org/bots/payments-stars

**M02 · Gifts de aniversário (`sendGift`)** 🟡🟢
- **Por quê:** gameplay social + viral. Top jogador do mês ganha gift de Stars do bot.
- **Como:** API `sendGift` (Bot API 9+). Bot fica com saldo de Stars pra distribuir.
- **Comando novo:** `/royalpresentear @user` (gasta moeda in-game pra mandar emoji-gift).

**M03 · Paid Media nas charadas brutais** 🟡🟡
- **Por quê:** "hint" pago pra Palavra da Hora difícil — 1 Star = revela letra.
- **Como:** `sendPaidMedia` com spoiler photo. Bot recebe o pagamento, libera hint.

**M04 · Subscription premium (Stars recorrente)** 🟠🟡
- **Por quê:** "Royal Plus" — 50 stars/mês = +20% XP, slot extra de casório, badge dourado no card.
- **Como:** `createInvoiceLink` com `subscription_period`. Webhook `pre_checkout_query` + `successful_payment`.

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

**M17 · Test suite (pytest + pytest-asyncio)** 🟠🟡
- **Por quê:** 5.6k linhas sem teste = roleta russa em refactor.
- **Como:** começa por units críticas: XP calc, palavra match, royal_id collision, migrations.

**M18 · CI no GitHub Actions** 🟢🟢
- **Por quê:** roda lint+test antes do push pro Railway. Bloqueia deploys quebrados.
- **Como:** `.github/workflows/ci.yml` com ruff + pytest.

### 🎨 UX / ACESSIBILIDADE

**M19 · Comando `/royalconfig` por user** 🟡🟢
- **Por quê:** silenciar notif de palavra, esconder do ranking, opt-out de DM.
- **Como:** estender `user_dm_settings` (migration v8) com flags JSON.

**M20 · Internacionalização i18n** 🟠🟡
- **Por quê:** outros grupos podem ser EN/ES. Hoje tudo PT-BR hardcoded.
- **Como:** `babel` + arquivos `.po`. Detecta `from_user.language_code`. Fallback PT-BR.

---

## 📅 ORDEM SUGERIDA DE EXECUÇÃO (gates de aprovação)

**Sprint 1 — Fundação (não dá pra escalar sem isso):**
F01 → F02 → F11 → F14 → F10 → F13 → M13 → M18

**Sprint 2 — Anti-incêndio:**
F03 → F05 → F06 → F07 → F08 → F09 → F15 → F16 → F17 → F18 → F19 → F20 → F04 → F12

**Sprint 3 — Monetização (libera receita cedo):**
M01 → M04 → M03 → M02

**Sprint 4 — Engajamento:**
M05 → M11 → M06 → M09 → M19

**Sprint 5 — Diferencial competitivo:**
M07 → M15 → M10 → M12 → M08 → M14 → M16 → M17 → M20

---

## 🔄 PROTOCOLO DE EXECUÇÃO (você aprova item-a-item)

1. Eu proponho **1 item** com escopo cirúrgico (1 arquivo, 1 diff visível).
2. Você aprova `ok` / pede ajuste / pula.
3. Eu implemento + auto-commit + me sinalize `push` quando quiser deployar.
4. Próximo item.

**Pergunta agora:** começamos pela **F01 (aiosqlite + pool)**, que destrava todo o resto? Ou prefere pegar um **quick-win primeiro** (ex: M13 Sentry — 30min, alto valor) pra eu calibrar seu padrão de aprovação?
