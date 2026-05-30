---
name: Sprite icon legibility (royal_render)
description: How the 10x10 pixel-art class/item icons must be shaped to read correctly on cards
---

# Sprite icon legibility

The 8-bit `SPRITE_*` icons in `royal_render.py` (10x10, chars `.`/`#`/`o`) are drawn
flat with no antialias via `draw_sprite`/`draw_icon_centered`. Shape is the only
thing carrying recognition, so a few silhouettes are traps:

- **Sword must be DIAGONAL, not vertical.** A vertical blade + horizontal crossguard
  reads as an inverted cross (†) at card scale — looks wrong/religiously off. The
  espada/Cavaleiro/FORÇA icon was redrawn as a 45° blade (handle bottom-left, tip
  top-right). Do NOT "straighten" it back to vertical.
- **Wizard hat must be a TALL NARROW cone + wide brim that sticks out both sides.**
  A short/wide stepped cone reads as a pyramid/ziggurat. Keep the cone ≥3 rows of a
  thin tip before widening.
- **Boot = L-shape** (vertical shaft on left + foot extending right). A solid filled
  triangle reads as stairs.
- **Ring = diamond gem on a HOLLOW round band.** Solid side pixels make it look like
  a bottle/figure.

**Why:** these were all flagged by the owner as "icons not rendering right." Fix was
data-only (sprite literals); the emoji→sprite map and draw functions were correct.

**How to apply:** when adding/editing a class or item icon, render it large
(scale ~24) AND inside the real card (`render_loja_card`, `render_classe_card`, etc.)
and eyeball it before shipping. Keep every sprite exactly 10 rows × 10 cols.
Ones that already read well: crown, key, book, shield, potion, scroll.
