"""
Royal RPG — gerador de cards 1080x1080.

Padrao inspirado no projeto TR3:
- Dataclass frozen separa dados de renderizacao.
- Pillow puro (sem Chromium) pra deploy leve no Railway.
- Helpers reutilizaveis (fonts, ellipsize, format BR, circle avatar...).
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

logger = logging.getLogger(__name__)

CARD_SIZE = 1080

# Paleta Royal (vinho profundo + dourado + pergaminho)
BG = (38, 14, 26)
SURFACE = (61, 23, 41)
SURFACE_SOFT = (84, 34, 56)
GOLD = (212, 175, 55)
GOLD_DIM = (160, 130, 40)
ROSE = (200, 90, 120)
PARCHMENT = (245, 235, 215)
MUTED = (188, 168, 145)
XP_BAR_BG = (50, 22, 36)

# Fonts (DejaVu = padrao Debian/Railway). Fallback chain.
FONT_REG = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
]
FONT_BOLD = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
]
FONT_ITALIC = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Italic.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansOblique.ttf",
]


def load_font(size: int, *, bold: bool = False, italic: bool = False):
    # Cadeia de fallback: tipo pedido -> regular -> bold -> bitmap default
    chains = []
    if italic:
        chains.append(FONT_ITALIC)
    if bold:
        chains.append(FONT_BOLD)
    chains.append(FONT_REG)
    chains.append(FONT_BOLD)
    for paths in chains:
        for path in paths:
            try:
                if Path(path).exists():
                    return ImageFont.truetype(path, size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def format_br(n: int | None) -> str:
    if n is None:
        return "0"
    return f"{int(n):,}".replace(",", ".")


def ellipsize(text: str, max_chars: int) -> str:
    s = " ".join(str(text or "").split())
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1].rstrip() + "..."


def text_size(draw: ImageDraw.ImageDraw, text: str, font) -> tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def rounded_rect(draw, box, radius, fill=None, outline=None, width=0):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def circle_avatar(avatar_bytes: bytes | None, size: int,
                  fallback_initial: str = "?") -> Image.Image:
    """Retorna RGBA size x size com avatar em circulo. Sem bytes = placeholder com inicial."""
    src: Image.Image | None = None
    if avatar_bytes:
        try:
            src = Image.open(io.BytesIO(avatar_bytes)).convert("RGB")
            w, h = src.size
            side = min(w, h)
            src = src.crop((
                (w - side) // 2, (h - side) // 2,
                (w + side) // 2, (h + side) // 2,
            ))
            src = src.resize((size, size), Image.LANCZOS)
        except Exception:
            logger.warning("circle_avatar decode failed", exc_info=True)
            src = None

    if src is None:
        src = Image.new("RGB", (size, size), SURFACE_SOFT)
        d = ImageDraw.Draw(src)
        d.ellipse((-size // 3, size // 4, size + size // 3, size + size),
                  fill=SURFACE)
        ch = (fallback_initial or "?")[0].upper()
        font = load_font(int(size * 0.5), bold=True)
        tw, th = text_size(d, ch, font)
        bbox = d.textbbox((0, 0), ch, font=font)
        d.text(((size - tw) // 2, (size - th) // 2 - bbox[1]),
               ch, font=font, fill=GOLD)

    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(src, (0, 0), mask)
    return out


def draw_progress_bar(draw, box, current, total, *,
                      fill=GOLD, bg=XP_BAR_BG, radius=18):
    x0, y0, x1, y1 = box
    rounded_rect(draw, box, radius, fill=bg)
    if total > 0 and current > 0:
        pct = max(0.0, min(1.0, current / total))
        fw = int((x1 - x0) * pct)
        if fw >= radius * 2:
            rounded_rect(draw, (x0, y0, x0 + fw, y1), radius, fill=fill)
        elif fw > 0:
            draw.ellipse((x0, y0, x0 + radius * 2, y1), fill=fill)


# =====================================================================
# PROFILE CARD
# =====================================================================

@dataclass(frozen=True)
class ProfileCardData:
    royal_id: str
    name: str
    initial: str
    class_name: str
    season: str
    level: int
    xp_in_level: int
    xp_needed: int
    hp_cur: int
    hp_max: int
    attr_for: int
    attr_des: int
    attr_vit: int
    attr_car: int
    pts_available: int
    rank: int
    total_players: int
    palavras_won: int
    casorios: int
    gold: int
    msg_count: int
    joined_str: str


def _draw_attribute_pill(draw, *, box, label: str, value: int,
                         label_font, value_font):
    x0, y0, x1, y1 = box
    rounded_rect(draw, box, 18, fill=SURFACE_SOFT)
    # accent bar topo
    draw.rectangle((x0 + 18, y0 + 6, x1 - 18, y0 + 9), fill=GOLD_DIM)
    # label centrado em cima
    lw, _ = text_size(draw, label, label_font)
    draw.text(((x0 + x1) // 2 - lw // 2, y0 + 18),
              label, font=label_font, fill=MUTED)
    # valor grande centrado embaixo
    vstr = str(value)
    vw, vh = text_size(draw, vstr, value_font)
    draw.text(((x0 + x1) // 2 - vw // 2, y0 + 50),
              vstr, font=value_font, fill=GOLD)


def render_profile_card(data: ProfileCardData,
                        avatar_bytes: bytes | None) -> bytes | None:
    """Renderiza o cartao de perfil. Retorna bytes JPEG ou None em falha."""
    try:
        img = Image.new("RGB", (CARD_SIZE, CARD_SIZE), BG)
        draw = ImageDraw.Draw(img)

        # Glow rose no topo
        glow = Image.new("RGBA", (CARD_SIZE, CARD_SIZE), (0, 0, 0, 0))
        gd = ImageDraw.Draw(glow)
        gd.ellipse(
            (CARD_SIZE // 2 - 420, -260, CARD_SIZE // 2 + 420, 480),
            fill=(200, 90, 120, 90),
        )
        glow = glow.filter(ImageFilter.GaussianBlur(90))
        img.paste(glow, (0, 0), glow)

        # Glow dourado no rodape
        glow2 = Image.new("RGBA", (CARD_SIZE, CARD_SIZE), (0, 0, 0, 0))
        g2 = ImageDraw.Draw(glow2)
        g2.ellipse(
            (-200, CARD_SIZE - 280, CARD_SIZE + 200, CARD_SIZE + 260),
            fill=(212, 175, 55, 55),
        )
        glow2 = glow2.filter(ImageFilter.GaussianBlur(110))
        img.paste(glow2, (0, 0), glow2)

        # ---- Header ----
        rounded_rect(draw, (36, 30, CARD_SIZE - 36, 100), 22, fill=SURFACE)
        brand_font = load_font(30, bold=True)
        season_font = load_font(22)
        brand = f"ROYAL  ✦  {data.royal_id}"
        draw.text((64, 50), brand, font=brand_font, fill=GOLD)
        sw, _ = text_size(draw, data.season, season_font)
        draw.text((CARD_SIZE - 64 - sw, 56),
                  data.season, font=season_font, fill=MUTED)

        # ---- Avatar com anel dourado ----
        avatar_size = 280
        ax = (CARD_SIZE - avatar_size) // 2
        ay = 140
        # anel externo grosso
        draw.ellipse(
            (ax - 14, ay - 14, ax + avatar_size + 14, ay + avatar_size + 14),
            fill=GOLD,
        )
        # gap escuro
        draw.ellipse(
            (ax - 4, ay - 4, ax + avatar_size + 4, ay + avatar_size + 4),
            fill=BG,
        )
        av = circle_avatar(avatar_bytes, avatar_size, data.initial)
        img.paste(av, (ax, ay), av)

        # ---- Nome + classe + temporada ----
        # ajusta tamanho do nome se for muito longo
        name_clean = ellipsize(data.name, 28)
        name_size = 56 if len(name_clean) <= 16 else 44 if len(name_clean) <= 22 else 36
        name_font = load_font(name_size, bold=True)
        nw, _ = text_size(draw, name_clean, name_font)
        draw.text(((CARD_SIZE - nw) // 2, 450),
                  name_clean, font=name_font, fill=PARCHMENT)

        class_label = ellipsize(data.class_name, 30)
        class_font = load_font(28, italic=True)
        cw, _ = text_size(draw, class_label, class_font)
        draw.text(((CARD_SIZE - cw) // 2, 518),
                  class_label, font=class_font, fill=GOLD_DIM)

        # ---- Painel stats ----
        panel = (40, 575, CARD_SIZE - 40, 905)
        rounded_rect(draw, panel, 28, fill=SURFACE)

        # Level + XP bar
        lvl_font = load_font(34, bold=True)
        lvl_label_font = load_font(22, bold=True)
        xp_font = load_font(22, bold=True)
        small_font = load_font(20)

        draw.text((72, 600), f"NÍVEL  {data.level}",
                  font=lvl_font, fill=PARCHMENT)
        if data.pts_available > 0:
            pts_txt = f"+{data.pts_available} pts pra distribuir"
            pw, _ = text_size(draw, pts_txt, lvl_label_font)
            draw.text((CARD_SIZE - 72 - pw, 610),
                      pts_txt, font=lvl_label_font, fill=ROSE)

        xp_text = f"{format_br(data.xp_in_level)} / {format_br(data.xp_needed)} XP"
        xw, _ = text_size(draw, xp_text, xp_font)
        draw.text((CARD_SIZE - 72 - xw, 658),
                  xp_text, font=xp_font, fill=MUTED)
        draw_progress_bar(draw,
                          (72, 695, CARD_SIZE - 72, 728),
                          data.xp_in_level, data.xp_needed,
                          fill=GOLD, bg=XP_BAR_BG, radius=16)

        # HP
        hp_font = load_font(24, bold=True)
        draw.text((72, 748),
                  f"HP  {data.hp_cur} / {data.hp_max}",
                  font=hp_font, fill=PARCHMENT)

        # Attribute pills
        attrs = [
            ("FORÇA", data.attr_for),
            ("DESTREZA", data.attr_des),
            ("VITALIDADE", data.attr_vit),
            ("CARISMA", data.attr_car),
        ]
        pill_y0, pill_y1 = 800, 882
        total_w = CARD_SIZE - 144
        gap = 14
        pill_w = (total_w - gap * 3) // 4
        attr_label_font = load_font(15, bold=True)
        attr_val_font = load_font(30, bold=True)
        for i, (lbl, val) in enumerate(attrs):
            px0 = 72 + i * (pill_w + gap)
            px1 = px0 + pill_w
            _draw_attribute_pill(
                draw,
                box=(px0, pill_y0, px1, pill_y1),
                label=lbl, value=val,
                label_font=attr_label_font,
                value_font=attr_val_font,
            )

        # ---- Footer mini stats ----
        mini_val_font = load_font(28, bold=True)
        mini_label_font = load_font(17)
        items = [
            (f"#{data.rank}", f"de {data.total_players}"),
            (str(data.palavras_won), "palavras"),
            (str(data.casorios), "casórios"),
            (format_br(data.gold), "florins"),
        ]
        col_w = CARD_SIZE // 4
        foot_y = 930
        for i, (val, lbl) in enumerate(items):
            cx = col_w * i + col_w // 2
            vw, _ = text_size(draw, val, mini_val_font)
            draw.text((cx - vw // 2, foot_y),
                      val, font=mini_val_font, fill=GOLD)
            lw, _ = text_size(draw, lbl, mini_label_font)
            draw.text((cx - lw // 2, foot_y + 38),
                      lbl, font=mini_label_font, fill=MUTED)

        # ---- Tagline rodape ----
        tag = f"{format_br(data.msg_count)} mensagens desde {data.joined_str}"
        tag_font = load_font(17, italic=True)
        tw, _ = text_size(draw, tag, tag_font)
        draw.text(((CARD_SIZE - tw) // 2, 1028),
                  tag, font=tag_font, fill=MUTED)

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=92, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_PROFILE_CARD_RENDER_FAILED royal_id=%s",
                         getattr(data, "royal_id", "?"))
        return None
