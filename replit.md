# Royal Casorios → Royal RPG

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

## User preferences
- Sempre usar as versões mais atualizadas das APIs:
  - **Telegram Bot API: 10**
  - **aiogram: última versão estável** (atualmente 3.28.2, lançada em 10/05/2026)

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
