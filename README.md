# 👑 Royal RPG (ex-Royal Casórios)

Bot de Telegram de RPG retro-futurista para grupos. Construído com **aiogram 3.28.2** (Python 3.12, Bot API 10), persistência em **SQLite WAL**, renderização de cards 1080×1080 em **Pillow puro** (sem Chromium). Deploy como **worker no Railway**.

> Identidade visual: 8-bit dystopian. Terminal CRT velho, monospace, ASCII art, paleta escura com acentos ácidos. Cada jogador recebe uma paleta consistente (TOXIC, AMBER, PLASMA, ARCTIC, BIOLAB, BLOOD) derivada do `royal_id`.

---

## 🆕 Novidades (mai/2026)

**UX viva (Bot API 10):**
- ⚡ **Bot reage ao seu comando** com `👀` / `✍` antes de responder — feedback instantâneo via `set_message_reaction` (helper `react_to()`)
- ⌨️ **"Digitando..."** simulado em renders pesados (`type_then_send`, `safe_typing`)
- 🧹 **Auto-deletar acks efêmeros** (rate-limit, "sem pontos") em N segundos pra não poluir grupo (`auto_delete_after`)
- 🎨 **Botões coloridos nativos (Bot API 10)** — `style='success'` (verde), `'danger'` (vermelho), `'primary'` (azul) em `InlineKeyboardButton`/`KeyboardButton`. Helper `ikb()` em `main.py:587`. Emoji líder (`BTN_OK ✅`, `BTN_NO ❌`, `BTN_INFO 🔵`, `BTN_WARN ⚠️`) mantido como fallback pra clientes antigos. *Não existe `warning`/amarelo nativo — pra essa categoria só emoji.*
- 🎉 Message effects (`EFFECT_*`) já em uso em DM 1:1


**Visual do cartão de perfil:**
- 🎨 **8 sprites 8-bit estilo Stardew** desenhados pixel-por-pixel em grids 10×10 (`SPRITE_SWORD`, `BOOT`, `HEART`, `MASK`, `TROPHY`, `BOOK`, `RINGS`, `COIN`) renderizados via helper genérico `draw_sprite()` acima de cada label
- 📝 **Abreviações → palavras inteiras:** `FOR/DES/VIT/CAR` → `FORÇA/DESTREZA/VITAL/CARISMA`, `NIVEL`→`NÍVEL`, `RANK`→`POSIÇÃO`, `CASORIOS`→`CASÓRIOS`, `[SIGIL]`→`[BRASÃO]`, `[FACE]`→`[FOTO]`. Chip alargado p/ 184 px, fonte ajustada (40→30 attrs, 28→26 status)

**Bugs corrigidos:**
- 🐛 **XP off-by-one** em `_build_level_table`: cartão mostrava `XP -282 / 519` no level 1. Adicionado sentinela `append(0)` no índice 1. Level 1 começa em 0 XP e exige 282 p/ subir.
- 🐛 **`ANON-X` aparecendo como nome do próprio dono** no cartão: `/royalperfil` não populava `users.display_name`, então quando o jogador nunca falava no grupo o fallback caía pro `user_id` numérico e o `anonize()` disparava. Novo helper `refresh_user_identity()` corrige isso sem inflar `message_count` (ver §13).

**Cascata de nome público do user:**
- `full_name` → `first_name` → `@username` → `ANON-<sufixo>` (último recurso)
- Aplicado em `display_name(message)` helper + callbacks `r:perfil` e baú

---

## Índice
1. [Como rodar](#como-rodar)
2. [Arquitetura](#arquitetura)
3. [Comandos](#comandos)
4. [Sistema de XP e Níveis](#xp-e-nivel)
5. [Atributos, Classes e HP](#atributos-classes-e-hp)
6. [Economia (Florins 🪙)](#economia)
7. [Loja & Inventário](#loja--inventário)
8. [Palavra da Hora](#palavra-da-hora)
9. [Baú Real](#baú-real)
10. [Boss Semanal](#boss-semanal)
11. [Casórios (Shipper)](#casórios)
12. [Temporadas & Hall da Fama](#temporadas)
13. [Privacidade & Anonimização](#privacidade)
14. [Renderização de Cards](#renderização)
15. [Schema do Banco](#schema)
16. [Constantes (cheat sheet)](#constantes)

---

<a name="como-rodar"></a>
## 1. Como rodar

### Secret obrigatório
- `BOT_TOKEN` — token do @BotFather

### Variáveis opcionais
| Var | Default | Função |
|---|---|---|
| `DATABASE_PATH` | `./data/royal_casorios.sqlite3` | Caminho do SQLite |
| `TZ` | `America/Sao_Paulo` | Fuso para temporadas e boss |
| `AUTO_HOURS` | `9,15,21` | Horários dos casórios automáticos |

### Deploy
Railway (config em `railway.json`). Worker, sem porta HTTP. SQLite local persistido em volume.

```bash
pip install -r requirements.txt
python main.py
```

---

<a name="arquitetura"></a>
## 2. Arquitetura

```
main.py            # bot completo (~3.650 linhas): handlers, gameplay, scheduler, DB
royal_render.py    # cards 1080×1080 e 1080×540 com Pillow (~1.000 linhas)
royal_words.py     # 154 palavras PT-BR + 35 charadas
requirements.txt   # aiogram>=3.28.2, Pillow>=11.0.0
railway.json       # deploy config
```

**Scheduler** roda a cada 20 s e cuida de:
- spawn de Palavra da Hora
- timeout de palavra (5/7/10 min ± jitter)
- spawn de baú 30 min depois da palavra
- expiração de baú 30 min depois do spawn
- spawn semanal do boss (Domingo 20h)
- rotação automática de temporada
- flush dos buffers `activity_buffer` e `pair_buffer` (reduz IO)
- casórios automáticos em `AUTO_HOURS`

**SQLite WAL** com `synchronous=NORMAL` e `busy_timeout=5000` ms (suporta concorrência alta).

---

<a name="comandos"></a>
## 3. Comandos

### Hub
| Comando | Função |
|---|---|
| `/royal` | Abre o Hub/Terminal principal (botões inline) |
| `/royalajuda`, `/help` | Lista comandos |
| `/royaltutorial` | Tutorial guiado |

### Perfil & Progressão
| Comando | Função |
|---|---|
| `/royalperfil [RYL-ID]`, `/royalficha` | Card 1080×1080 com stats. Sem ID, mostra o seu. |
| `/royalup` | Distribui pontos de atributo (1 por clique) |
| `/royalclasse` | Escolhe/troca classe (1× por temporada) |
| `/royalsaldo` | Mostra ouro atual |
| `/royalinventario` | Lista itens + botões equipar/usar |
| `/royalloja` | Compra com 🪙 |

### Rankings & PvE
| Comando | Função |
|---|---|
| `/royalranking` | Top 10 da temporada (XP) |
| `/royalpalavra` | Status da Palavra da Hora |
| `/royalboss` | Status/HP do boss semanal |

### Casórios
| Comando | Função |
|---|---|
| `/royalcasar`, `/querocasar` | (admin) Força casamento agora |
| `/royalencalhar`, `/encalhado` | Opt-out do shipper |
| `/royaldesencalhar`, `/desencalhar` | Volta a participar |
| `/royalmeuscasorios`, `/meusdivorcios` | Seu histórico + top pares |
| `/royalcasorios`, `/divorcios` | Ranking de casais do grupo |

### Admin / Privacidade
| Comando | Função |
|---|---|
| `/royalativar`, `/noivado` | (admin) Ativa o bot no grupo |
| `/royalprivacidade` | Esconder/mostrar nos rankings |
| `/royaldados` | Exportar ou apagar (GDPR) |

### Menu Privado (botões)
- `👑 Reino` → `/royal`
- `📖 Tutorial` → tutorial
- `📊 Meus casórios` → `/royalmeuscasorios`
- `❓ Como funciona` → ajuda

---

<a name="xp-e-nivel"></a>
## 4. XP & Níveis

### Tabela
Calculada via `_LEVEL_TABLE` em `level_progress(xp)`. Curva quadrática suave; cada level exige mais XP que o anterior.

### Como ganhar XP

| Ação | XP | Cooldown |
|---|---|---|
| Mensagem normal | **2** | 60 s |
| Reply para outro user | **5** | 30 s |
| Vencer Palavra da Hora | **+150** | — |
| Participar da palavra (não venceu) | **30–75** (random) | — |
| Tentou palavra errada | **+5** consolação | — |
| Casamento formado | **+25** (ambos) | — |
| Voto ❤️ no casamento | **+2** | — |
| Atacar boss | **2 + (dano)** | 300 s (5 min) |
| Tomo de Sabedoria 📚 (item) | **+100** | — |
| Baú Real (1º…5º) | **150 / 100 / 75 / 50 / 25** | — |

### Bônus multiplicativos (cumulativos)
- **Classe Cronista 📜** → **+10 % XP**
- **Casado nos últimos 14 dias** → **+10 % XP** (`COUPLE_XP_BUFF`)

### Level-up
- Notifica o jogador via DM (se possível) com card 1080×540
- Concede **3 pontos de atributo** (`PTS_PER_LEVEL`) para gastar em `/royalup`

---

<a name="atributos-classes-e-hp"></a>
## 5. Atributos, Classes & HP

### Atributos (start = 5 cada)
| Sigla | Efeito |
|---|---|
| **FOR** | Dano no boss: `FOR + random(1, 10)` |
| **DES** | Reservado p/ esquiva e cooldowns futuros |
| **VIT** | +10 HP máx por ponto |
| **CAR** | Score no shipper / casórios |

**Atributo efetivo** = base + bônus de classe + bônus de equipamento.

### Classes
| Classe | Bônus |
|---|---|
| 👑 **Monarca** | +20 % HP base |
| 🗡️ **Cavaleiro** | +2 FOR |
| 🌹 **Cortesã** | +2 CAR |
| 🧙 **Bruxo** | +2 DES |
| 📜 **Cronista** | +10 % XP |
| 🗝️ **Bobo** | +50 % ouro |

> Troca de classe permitida **1× por temporada**.

### HP máximo
```
hp_max = (50 + VIT * 10 + level * 5)
       * (1.20 se Monarca senão 1.0)
       + 10 por bônus VIT de item equipado
```

HP regenera 100 % consumindo **Poção de Vigor 🧪**.

---

<a name="economia"></a>
## 6. Economia — Florins 🪙

- **Saldo inicial:** 50 🪙
- **Ganhos:**
  - Vencer palavra → **50 🪙**
  - Matar boss → **500 🪙** divididos **proporcionalmente ao dano** entre todos que bateram
  - Baú Real → **10 🪙** por slot
  - Bobo 🗝️ → todos os ganhos × 1.5
- **Gastos:** apenas na `/royalloja`.

---

<a name="loja--inventário"></a>
## 7. Loja & Inventário

| Item | Preço | Tipo | Efeito |
|---|---|---|---|
| 🧪 Poção de Vigor | 50 🪙 | consumível | Restaura HP completo |
| 🥾 Botas Ágeis | 150 🪙 | equip | +2 DES |
| 🗡️ Espada de Ferro | 200 🪙 | equip | +3 FOR |
| 🛡️ Armadura de Couro | 200 🪙 | equip | +2 VIT (= +20 HP) |
| 💍 Anel da Corte | 250 🪙 | equip | +2 CAR |
| 📚 Tomo de Sabedoria | 300 🪙 | consumível | +100 XP instantâneo |
| 👑 Coroa Decorativa | 500 🪙 | cosmético | Mostra no perfil |

Só pode ter **1 equipamento de cada tipo** ativo (espada/armadura/botas/anel).

---

<a name="palavra-da-hora"></a>
## 8. Palavra da Hora

- **Spawn:** automático pelo scheduler, com janela de `5 / 7 / 10` min (`PALAVRA_DURATIONS_MIN`) ± 60 s de jitter
- **Tipos:** Anagrama, Letras Faltando, **Charada** (25 % de chance)
- **Banco:** 154 palavras de 4–8 letras (`royal_words.PALAVRAS`) + 35 charadas
- **Como jogar:** primeiro que digitar a palavra normalizada no chat vence
- **Anti-bot:** cooldown de **3 s** entre tentativas por user (`PALAVRA_ATTEMPT_COOLDOWN_SEC`)
- **Recompensas:**
  - Vencedor: **150 XP + 50 🪙** (effect 🎉 PARTY em DM)
  - Quem tentou e errou: **5 XP** consolação
  - Quem tentou e estava perto: **30–75 XP** (random)

---

<a name="baú-real"></a>
## 9. Baú Real

- **Spawn:** **30 min depois** de toda Palavra resolvida (`CHEST_DELAY_MIN`)
- **TTL:** **30 min** para abrir (`CHEST_TTL_MIN`)
- **Capacidade:** primeiros **5** clicantes (`CHEST_MAX_CLAIMS`)
- **Recompensas por ordem:**

| Posição | XP | 🪙 |
|---|---|---|
| 1º | 150 | 10 |
| 2º | 100 | 10 |
| 3º | 75 | 10 |
| 4º | 50 | 10 |
| 5º | 25 | 10 |

Cada user só pode reclamar **1 slot por baú**.

---

<a name="boss-semanal"></a>
## 10. Boss Semanal

- **Spawn:** todo **Domingo às 20h** local (`BOSS_SPAWN_WEEKDAY=6`, `BOSS_SPAWN_HOUR=20`), idempotente via `current_week_marker`
- **Nomes (random):** 🐉 Dragão da Corte · 👹 Ogro do Pântano · 💀 Lich Ancião · 🦂 Escorpião Real · 🦇 Vampiro da Torre · 🐺 Lobo das Sombras
- **HP:** `max(500, n_players × 200)` — escala com o grupo
- **Ataque:**
  - Cooldown **300 s (5 min)** por jogador
  - Dano: `FOR_efetivo + random(1, 10)`
  - XP por hit: **2 + dano**
- **Loot (kill):** 500 🪙 totais distribuídos proporcionalmente ao dano (effect 🔥 FIRE no golpe final)

---

<a name="casórios"></a>
## 11. Casórios (Shipper)

### Formação automática
- **3× por dia** nos horários de `AUTO_HOURS` (default `9, 15, 21`)
- Algoritmo casa quem tem maior **afinidade** mas ainda não ficou junto recentemente
- Mínimo de afinidade: **5** (`MIN_PAIR_SCORE`)

### Afinidade
Score acumulado em `pair_scores`:
- Reply pra outro user → **+6**
- Mencionar → **+4**
- Estar ativo na mesma janela de 3 min (`RECENT_WINDOW_SECONDS=180`) → **+1**

### Recompensas do casamento
- **+25 XP** para os dois (effect ❤️ HEART)
- **+10 % XP** durante 14 dias enquanto "casado"
- Voto ❤️ de espectador rende **+2 XP** ao votante

### Comandos
- `/royalencalhar` desativa você do pool
- `/royalcasar` (admin) força o casamento imediato
- `/royalmeuscasorios` mostra seu histórico + TOP PARES
- `/royalcasorios` ranking de casais mais frequentes

---

<a name="temporadas"></a>
## 12. Temporadas & Hall da Fama

- Fuso **hemisfério sul**:
  - 🌸 **Primavera** 22/09 → 20/12
  - ☀️ **Verão** 21/12 → 19/03 (cruza ano)
  - 🍂 **Outono** 20/03 → 20/06
  - ❄️ **Inverno** 21/06 → 21/09
- **Código:** `verao-2026`, `outono-2026`… ano = ano em que a estação **termina**
- **Rotação automática:** scheduler detecta mudança, então:
  1. Salva **Top 10** em `season_hall` (snapshot anonimizado)
  2. Zera `season_xp` de todos
  3. Posta o Hall da Fama no grupo
  4. Libera nova troca de classe

---

<a name="privacidade"></a>
## 13. Privacidade & Anonimização

Política: **nunca** vazar `user_id` numérico do Telegram em caption, mensagem ou registro persistido.

### Royal ID
- Formato `RYL-XXXX` (4 dígitos por grupo)
- Alocação sequencial via tabela `royal_id_seq`
- Único identificador público

### Cascata de nome público (`display_name(message)`)
Helper define o que vai parar em `users.display_name`:
1. `full_name` (Telegram display name completo) — preferido
2. `first_name` (só primeiro nome)
3. `@username` (handle público) — fallback
4. `""` vazio → chamador decide ANON via `anonize`

Mesma cascata aplicada inline nos callbacks (`r:perfil` no hub, claim de baú).

### Helper `anonize(name, royal_id)`
Se `name` é numérico (= user_id leakado) ou vazio, retorna `ANON-NN` derivado determinístico do `royal_id` (`ANON-1`…`ANON-99`).

### Helper `get_anon_name(chat_id, user_id)`
JOIN único em `users + players`, já aplica `anonize` automaticamente. **Sempre** use isso para renderizar nome de terceiros.

### Helper `refresh_user_identity(chat_id, user_id, name, username)`
Atualiza **só** `display_name`/`username`/`last_seen` em `users`. **Não** incrementa `message_count` — pode ser chamado a partir de DM ou callback sem distorcer ranking/shipper do grupo. Use sempre antes de renderizar perfil/cartão p/ garantir que o nome vivo do Telegram esteja no DB e o `anonize` não dispare pro próprio dono. Aplicado em:
- `/royalperfil` sem arg (branches grupo + DM)
- `/royalperfil RYL-XXXX` quando target == requisitante
- Callback `r:perfil` do hub inline

### Cobertura (11/11 sites)
| # | Onde | Função |
|---|---|---|
| 1–4 | Cards de perfil, level-up, captions | `anonize(get_name, royal_id)` |
| 5 | Casamento (anúncio) | `get_anon_name` |
| 6 | Boss kill drops | `get_anon_name` |
| 7 | Hall da Fama (**persistido em DB**) | `anonize` |
| 8 | Ranking público | `anonize` |
| 9 | Meus casórios → TOP PARES | `get_anon_name` |
| 10 | `/royalcasorios` ranking | `get_anon_name` |
| 11 | Callback hub casórios | `get_anon_name` |

### `/royalprivacidade`
- `privacy_hide_stats` → esconde do ranking público
- `privacy_hide_photo` → não usa foto real no card (só sigil procedural)

### `/royaldados` (GDPR)
- **Export** → dump JSON dos dados pessoais
- **Wipe** → apaga `players`, `pair_scores`, `couples`, `inventory`, mantém `royal_id` reservado

---

<a name="renderização"></a>
## 14. Renderização de Cards

Tudo via **Pillow puro** em `royal_render.py`. Sem headless browser.

### Funções públicas
| Função | Tamanho | Uso |
|---|---|---|
| `render_profile_card(data, avatar_bytes)` | 1080×1080 | `/royalperfil` |
| `render_ranking_card(season_label, entries)` | 1080×1080 | Pódio do `/royalranking` |
| `render_levelup_card(royal_id, name, lvl, class_name)` | 1080×540 | DM de level-up |

Render roda em `asyncio.to_thread(...)` para não travar o loop.

### Paleta base (dystopian)
| Token | RGB | Uso |
|---|---|---|
| `BG_DEEP` | (12, 10, 14) | vazio cósmico |
| `BG` | (22, 18, 22) | painel CRT |
| `INK` | (210, 196, 168) | texto bone |
| `DIM` | (112, 96, 84) | texto secundário |
| `HOT` | (192, 50, 50) | HP / alerta |
| `ACID` | (130, 198, 80) | toxic |
| `CYAN` | (88, 188, 200) | hologram |
| `GOLD` | (192, 152, 64) | XP / coroa |
| `RUST` | (140, 70, 40) | ferrugem |
| `PURPLE` | (110, 70, 132) | corruption |
| `AMBER` | (220, 140, 60) | terminal CRT |
| `MAGENTA` | (200, 90, 160) | plasma glitch |
| `NEON_BLUE` | (80, 140, 220) | hologram blue |
| `JADE` | (60, 180, 130) | biolab |

### Variantes por `royal_id` (`pick_palette`)
| Variante | Header | Footer | Level | XP |
|---|---|---|---|---|
| **TOXIC** | ACID | RUST | GOLD | ACID |
| **AMBER** | AMBER | RUST | AMBER | AMBER |
| **PLASMA** | MAGENTA | PURPLE | MAGENTA | MAGENTA |
| **ARCTIC** | NEON_BLUE | CYAN | CYAN | NEON_BLUE |
| **BIOLAB** | JADE | ACID | GOLD | JADE |
| **BLOOD** | HOT | RUST | GOLD | HOT |

### `load_font(size)` — escala anti-Railway
| Faixa | Escala | Piso |
|---|---|---|
| `< 14` | ×1.55 | 22 px |
| `14–63` | ×1.6 | 26 px |
| `64–119` | ×1.10 | — |
| `≥ 120` | sem escala | — |

Fallback se nenhum truetype existir: `ImageFont.load_default(size=scaled)` (Pillow ≥ 10.1).

### Avatar híbrido
1. **Brasão principal:** `procedural_sigil()` 10×10, ~55 % densidade, espelhado horizontalmente — determinístico por `royal_id`. Tag `[ BRASÃO ]`
2. **Thumbnail:** foto real 92×92 pixelizada (downscale 36×36 → quantize 16 cores → upscale NEAREST), tag `[ FOTO ]`

### Sprites 8-bit estilo Stardew (`draw_sprite`)
8 sprites desenhados pixel-por-pixel em grids 10×10, renderizados acima de cada label nos painéis ATRIBUTOS (scale 3 = 30 px) e STATUS (scale 2 = 20 px):

| Sprite | Label | Cor (variante por paleta) |
|---|---|---|
| `SPRITE_SWORD` ⚔️ | FORÇA | acento header |
| `SPRITE_BOOT` 👢 | DESTREZA | acento header |
| `SPRITE_HEART` ❤️ | VITAL | HOT |
| `SPRITE_MASK` 🎭 | CARISMA | CYAN |
| `SPRITE_TROPHY` 🏆 | POSIÇÃO | GOLD |
| `SPRITE_BOOK` 📜 | PALAVRAS | acento header |
| `SPRITE_RINGS` 💍 | CASÓRIOS | HOT |
| `SPRITE_COIN` 🪙 | FLORINS | GOLD |

Helper `draw_sprite(draw, x, y, sprite, scale, color, highlight=None)` em `royal_render.py:489` — basta criar matriz 10×10 com `0/1/2` (0 = transparente, 1 = base, 2 = highlight opcional) e chamar com qualquer escala/cor.

### Pós-processamento (sempre)
1. `apply_scanlines(every=3, alpha=55–70)` — CRT
2. `apply_vignette(strength=140–180)` — distopia
3. `apply_grain(intensity=14–18)` — TV velha

### Cache de cards
- LRU `_CARD_CACHE` (OrderedDict), max **256** entradas, TTL **300 s**
- Chave inclui `md5(avatar_bytes)` para invalidar quando foto muda

### Efeitos de mensagem (Bot API 7.7)
**Só funcionam em DM 1:1** — helper `effect_kw(chat_type, ID)` retorna `{}` em grupo.

| Constante | ID | Quando |
|---|---|---|
| `EFFECT_PARTY` 🎉 | 5046509860389126442 | Welcome, ganhar palavra |
| `EFFECT_FIRE` 🔥 | 5104841245755180586 | Dano crítico, golpe final no boss |
| `EFFECT_HEART` ❤️ | 5044134455711629726 | Casório |
| `EFFECT_THUMBS_UP` 👍 | 5107584321108051014 | OK |
| `EFFECT_THUMBS_DOWN` 👎 | 5104858069142078462 | Vote 🤮 |
| `EFFECT_POO` 💩 | 5046589136895476101 | Diss |

### Helpers de texto (em `main.py`)
- `term_block(title, body, status, status_color, stamp)` — header `> TITLE.SYS // STATUS` + corpo `<blockquote>` + stamp
- `term_pre(rows)` — tabela monospace `KEY :: VALUE` em `<pre>`
- Caption de foto: **limite 1024 chars** (guarda no código)

---

<a name="schema"></a>
## 15. Schema do Banco (SQLite WAL)

| Tabela | Função | Colunas chave |
|---|---|---|
| `users` | Tracking global do user | user_id, chat_id, display_name, username, opt_out, message_count, last_seen |
| `players` | Estado RPG por chat | chat_id, user_id, royal_id, class_id, total_xp, season_xp, attr_for/des/vit/car, pts_available, gold, joined_at |
| `royal_id_seq` | Contador sequencial por chat | chat_id, next_id |
| `inventory` | Itens do jogador | chat_id, user_id, item_id, qty, equipped |
| `daily_activity` | Mensagens por dia | chat_id, user_id, day, message_count |
| `pair_scores` | Afinidade entre pares | chat_id, user1, user2, score, last_seen |
| `couples` | Histórico de casórios | chat_id, user1, user2, source, created_at |
| `couple_votes` | Votos ❤️/🤮 | couple_id, voter, kind |
| `challenges` | Palavra da Hora ativa/histórico | chat_id, word, hint, kind, deadline_at, winner |
| `chests` | Baús ativos | chat_id, challenge_id, spawn_at, expire_at, status |
| `chest_claims` | Quem pegou cada slot | chest_id, user_id, slot, xp, gold |
| `bosses` | Bosses ativos/históricos | chat_id, name, hp, hp_max, week_marker, status |
| `boss_hits` | Dano por jogador | boss_id, user_id, dmg, ts |
| `season_hall` | Snapshot do Top 10 anonimizado | chat_id, season_code, rank, royal_id, user_id, display_name, season_xp |
| `chats` | Config + auto-post markers | chat_id, activated_at, last_palavra_at, last_boss_week |

### Migrations
Controladas por `PRAGMA user_version`. Cada `migrate_to_vN` é idempotente.

---

<a name="constantes"></a>
## 16. Constantes — cheat sheet

### Tempo & janelas
```python
RECENT_WINDOW_SECONDS    = 180     # janela de afinidade
FLUSH_INTERVAL_SECONDS   = 20      # buffer → DB
MIN_PAIR_SCORE           = 5       # afinidade mínima p/ casar
ADMIN_CACHE_TTL_SECONDS  = 300
PHOTO_CACHE_TTL_SECONDS  = 86400   # 1 dia
```

### XP
```python
XP_PER_MESSAGE             = 2
XP_PER_REPLY               = 5
XP_COOLDOWN_MSG_SECONDS    = 60
XP_COOLDOWN_REPLY_SECONDS  = 30
XP_PALAVRA_MIN             = 30
XP_PALAVRA_MAX             = 75
XP_PALAVRA_WIN_BONUS       = 150
XP_PALAVRA_CONSOLATION     = 5
XP_COUPLE_FORMED           = 25
XP_VOTE_LIKE               = 2
XP_BOSS_HIT                = 2     # + dano causado
```

### Economia
```python
STARTING_GOLD          = 50
GOLD_PALAVRA_WIN       = 50
GOLD_BOSS_KILL_TOTAL   = 500
COUPLE_XP_BUFF         = 0.10
```

### Atributos
```python
ATTR_START      = 5
PTS_PER_LEVEL   = 3
```

### Palavra da Hora
```python
PALAVRA_DURATIONS_MIN          = [5, 7, 10]
PALAVRA_JITTER_SEC             = 60
PALAVRA_ATTEMPT_COOLDOWN_SEC   = 3
```

### Baú Real
```python
CHEST_DELAY_MIN     = 30
CHEST_TTL_MIN       = 30
CHEST_MAX_CLAIMS    = 5
CHEST_REWARDS       = [(150, 10), (100, 10), (75, 10), (50, 10), (25, 10)]
```

### Boss
```python
BOSS_SPAWN_WEEKDAY        = 6      # Domingo
BOSS_SPAWN_HOUR           = 20     # 20h local
BOSS_ATTACK_COOLDOWN_SEC  = 300    # 5 min entre ataques
# HP = max(500, n_players * 200)
```

---

## Convenções de tom (voz do bot)

- Título em CAPS, formato `PALAVRA.SYS`
- Prefixos: `>` saída do terminal, `>>` sub-comando, `//` comentário, `!!` alerta
- Status: `OK`, `CONECTADO`, `CONFIG`, `ALERTA`, `OFFLINE`, `RANKING`, `HISTORICO`
- Stamp final em itálico entre `[ ... ]`
- HTML do Telegram usado integralmente: `<b> <i> <u> <s> <code> <pre> <a> <blockquote expandable> <tg-spoiler>`

---

## Decisão: imagem vs caption

| Quando | Renderizar card 1080×… | Mandar caption pura |
|---|---|---|
| Perfil próprio / ranking pódio / casório / boss derrotado | ✅ | — |
| Tudo o mais (status, listas, votos, comandos rápidos) | — | ✅ `term_block` |

Cards são caros (Pillow + scanlines + vignette + grain). Caption é instantâneo.

---

## Licença
Projeto privado. Sem licença pública declarada.
