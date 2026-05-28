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

## 🔧 Env vars

- `BOT_TOKEN` (obrigatório) — token do BotFather
- `DATABASE_PATH` (opcional, default `./data/royal_casorios.sqlite3`)
- `TZ` (opcional, default `America/Sao_Paulo`)
- `AUTO_HOURS` (opcional, default `9,15,21`)
- **`TEST_CHAT_IDS`** (opcional, comma-separated) — chat_ids de grupos de
  teste. Esses grupos **não** aparecem no picker de DM nem no fallback
  do inline mode. Ex.: `TEST_CHAT_IDS="-1001234567890,-1009876543210"`

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
