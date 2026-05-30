---
name: Ícones dos cards = sprites pixel-art
description: Por que cards usam sprites em vez de emoji-fonte e como adicionar novos sem reintroduzir tofu
---

# Ícones dos cards (royal_render.py) são sprites pixel-art, não emoji de fonte

**Regra:** ícones de classe e item desenhados nos cards (imagens PNG via Pillow) usam sprites 8-bit
(`SPRITE_*` em grid de `'#'`/`'o'`), NÃO o emoji renderizado como glifo de fonte.

**Why:** as fontes embarcadas no projeto (DejaVu, Noto Sans/Math/Symbols/Symbols2) não têm glifo para
vários emojis usados como ícone (👑 🌹 🧙 📜 🧪 🥾 💍) → apareciam como **tofu** (▢) nos cards. Não há
fonte de emoji colorido e a regra do dono é custo zero. Sprites pixel-art combinam com a estética
CRT/8-bit e nunca dependem de fonte.

**How to apply:**
- O helper `draw_icon_centered(draw, cx, cy, emoji, target_px, color, shadow=)` resolve o emoji →
  sprite pelo dict `EMOJI_SPRITES` (chaves normalizadas **sem** variation selector `U+FE0F`). Se não
  houver sprite, cai no fallback de desenhar o emoji como texto.
- Os 4 pontos de render de ícone usam esse helper: card de classe, card de drop da loja, e os 2 loops
  de slots do inventário.
- Ao adicionar uma classe ou item novo, crie o `SPRITE_*` e adicione a entrada em `_EMOJI_SPRITE_RAW`
  — senão o ícone novo volta a dar tofu no card.
- Emoji em **menu/caption/texto** do Telegram (ex. 🐉 em BotCommand "Boss") renderiza com a fonte
  nativa do Telegram — NÃO precisa de sprite. O problema é exclusivo de glifo desenhado no Pillow.
- Atributos do perfil (força/HP/moeda/etc.) já usavam sprites próprios (`SPRITE_SWORD/BOOT/HEART/...`)
  e nunca sofreram tofu.
