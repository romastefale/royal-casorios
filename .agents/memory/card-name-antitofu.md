---
name: Card name anti-tofu (_layout_glyphs)
description: Why display-name rendering in cards never shows tofu boxes, and what NOT to revert.
---

# Anti-tofu de nomes nos cards (royal_render.py)

Display name do Telegram é texto livre (math-bold 𝕽, fullwidth Ｋｉｎｇ, cirílico,
emoji, CJK). O gerador de cards garante **zero tofu** (caixinha □) na camada de
render, não nos call sites.

**Regra:** `draw_text_smart` e `text_size_smart` DEVEM usar o mesmo helper
`_layout_glyphs(text, primary, fallbacks)` — ele resolve cada glifo em 3 camadas:
1. fonte que cobre (primary > Noto Sans/Math/Symbols/Symbols2) → math-bold e
   cirílico renderizam nativos;
2. sem cobertura → `unicodedata.normalize("NFKC", ch)` cai na forma ASCII
   coberta (conserta **fullwidth** ＡＢＣ→ABC, enclosed ①, letterlike);
3. ainda sem cobertura (emoji colorido, CJK) → glifo **descartado** (some).

**Why:** fullwidth (U+FF00-FFEF) não é coberto por NENHUMA fonte do projeto e
dava □□□□ em qualquer card com nome estilizado (bug real visto em produção via
harness). Bundlar fonte CJK/emoji colorido violaria custo-zero, então
descartar > tofu (o `royal_id` sempre acompanha o nome no card, nunca fica
anônimo). Math-bold JÁ renderiza via NotoSansMath — não "consertar" achando que
é tofu.

**How to apply:** ao mexer no render de texto, manter draw e measure usando
`_layout_glyphs` (largura tem que casar com o desenho, senão centralização
quebra). `card_safe_name()` (royal/core.py) ainda troca nome fancy por
@username/ANON em alguns paths, mas NÃO é mais a única defesa — a garantia é
universal no render. Testes: `tests/test_render.py`. Harness manual (não
versionar como teste): `.local/render_audit.py` renderiza os 34 cards com nomes
stress.

Invariante separada (mesmo arquivo de teste): TODO emoji de `CLASSES`/`ITEMS`
tem sprite em `EMOJI_SPRITES` — `draw_icon_centered` só cai em fonte-texto se
faltar sprite, e agora isso falha no CI em vez de virar tofu/sumiço no card.
