"""
Royal RPG — gerador de cards 1080x1080.

Estetica: 8-bit retro + dark dystopian (CRT/terminal corrompido).
Pillow puro (sem Chromium) pra deploy leve no Railway.
"""

from __future__ import annotations

import io
import logging
import random
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

logger = logging.getLogger(__name__)

CARD_SIZE = 1080

# ---------------------------------------------------------------------
# Paleta dystopian 8-bit (palette fechada, sem gradientes suaves)
# ---------------------------------------------------------------------
BG_DEEP = (12, 10, 14)           # vazio cosmico
BG = (22, 18, 22)                # painel base CRT
PANEL = (32, 26, 32)             # painel sobreposto
PANEL_HI = (48, 38, 48)          # destaque
INK = (210, 196, 168)            # texto bone/pergaminho corroido
DIM = (112, 96, 84)              # texto secundario, ash
HOT = (192, 50, 50)              # sangue / alerta / HP
ACID = (130, 198, 80)            # verde toxico "funcional"
CYAN = (88, 188, 200)            # holograma frio
GOLD = (192, 152, 64)            # ouro fosco, decadente
GOLD_DIM = (124, 96, 38)
RUST = (140, 70, 40)             # ferrugem
PURPLE = (110, 70, 132)          # ametista corrompida
WHITE = (240, 232, 220)
BLACK = (0, 0, 0)


# ---------------------------------------------------------------------
# Fonts — preferimos MONO pra dar cara de terminal/8-bit
# ---------------------------------------------------------------------
FONT_MONO = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
]
FONT_MONO_REG = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
]
FONT_BOLD = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
]
FONT_REG = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
]


def load_font(size: int, *, mono: bool = True, bold: bool = True):
    if mono:
        chain = FONT_MONO if bold else FONT_MONO_REG
    else:
        chain = FONT_BOLD if bold else FONT_REG
    for path in chain:
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
    return s[: max_chars - 1].rstrip() + "."


def text_size(draw: ImageDraw.ImageDraw, text: str, font) -> tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


# ---------------------------------------------------------------------
# Helpers de desenho pixel-art
# ---------------------------------------------------------------------

def pixel_rect(draw, box, fill):
    """Retangulo simples (sem antialiasing)."""
    draw.rectangle(box, fill=fill)


def chunky_border(draw, box, *, outer=BLACK, inner=INK, thick=6):
    """Borda dupla estilo SNES/Stardew: linha grossa + linha fina por dentro."""
    x0, y0, x1, y1 = box
    # outer
    draw.rectangle((x0, y0, x1, y0 + thick), fill=outer)
    draw.rectangle((x0, y1 - thick, x1, y1), fill=outer)
    draw.rectangle((x0, y0, x0 + thick, y1), fill=outer)
    draw.rectangle((x1 - thick, y0, x1, y1), fill=outer)
    # inner highlight 2px dentro do outer
    pad = thick + 4
    t2 = 2
    draw.rectangle((x0 + pad, y0 + pad, x1 - pad, y0 + pad + t2), fill=inner)
    draw.rectangle((x0 + pad, y1 - pad - t2, x1 - pad, y1 - pad), fill=inner)
    draw.rectangle((x0 + pad, y0 + pad, x0 + pad + t2, y1 - pad), fill=inner)
    draw.rectangle((x1 - pad - t2, y0 + pad, x1 - pad, y1 - pad), fill=inner)
    # cantos decorativos
    csz = thick * 2
    for cx, cy in (
        (x0, y0), (x1 - csz, y0),
        (x0, y1 - csz), (x1 - csz, y1 - csz),
    ):
        draw.rectangle((cx, cy, cx + csz, cy + csz), fill=inner)
        draw.rectangle((cx + 2, cy + 2, cx + csz - 2, cy + csz - 2), fill=outer)


def pixelated_avatar(avatar_bytes: bytes | None, size: int,
                     fallback_initial: str = "?") -> Image.Image:
    """Avatar 'baixado' pra 32x32 e re-upscalado com NEAREST = mosaico 8-bit.
    Retorna RGBA quadrado size x size (com borda preta pixel)."""
    inner = size - 12  # deixa espaco pra moldura

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
            # downsample EXTREMO pra pixelar
            src = src.resize((36, 36), Image.LANCZOS)
            # quantize pra paleta limitada (cara 8-bit)
            src = src.quantize(colors=16, dither=Image.Dither.FLOYDSTEINBERG).convert("RGB")
            src = src.resize((inner, inner), Image.NEAREST)
        except Exception:
            logger.warning("pixelated_avatar decode failed", exc_info=True)
            src = None

    if src is None:
        # placeholder pixel-art com inicial em cores acid/hot
        small = Image.new("RGB", (16, 16), PANEL)
        d = ImageDraw.Draw(small)
        # mancha de "estatica"
        rng = random.Random(hash(fallback_initial) & 0xFFFF)
        for _ in range(48):
            d.point((rng.randrange(16), rng.randrange(16)),
                    fill=rng.choice([PANEL_HI, DIM, RUST]))
        # inicial chunky no centro
        f = load_font(12, mono=True, bold=True)
        ch = (fallback_initial or "?")[0].upper()
        tw, th = text_size(d, ch, f)
        d.text(((16 - tw) // 2, (16 - th) // 2 - 1), ch, font=f, fill=ACID)
        src = small.resize((inner, inner), Image.NEAREST)

    # monta com moldura preta + borda interna acid
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    od = ImageDraw.Draw(out)
    pixel_rect(od, (0, 0, size, size), BLACK)
    pixel_rect(od, (4, 4, size - 4, size - 4), GOLD_DIM)
    pixel_rect(od, (6, 6, size - 6, size - 6), BLACK)
    out.paste(src, (6, 6))
    return out


def draw_chunky_bar(draw, box, current, total, *,
                    fill=ACID, bg=PANEL_HI, segments=20):
    """XP/HP bar feita de blocos discretos (estilo 8-bit)."""
    x0, y0, x1, y1 = box
    pixel_rect(draw, box, BLACK)
    pixel_rect(draw, (x0 + 2, y0 + 2, x1 - 2, y1 - 2), bg)
    if total <= 0:
        return
    pct = max(0.0, min(1.0, current / total))
    filled = int(round(segments * pct))
    seg_w = (x1 - x0 - 4) / segments
    for i in range(filled):
        sx0 = int(x0 + 2 + i * seg_w) + 1
        sx1 = int(x0 + 2 + (i + 1) * seg_w) - 1
        pixel_rect(draw, (sx0, y0 + 4, sx1, y1 - 4), fill)
        # highlight 1px topo
        pixel_rect(draw, (sx0, y0 + 4, sx1, y0 + 6),
                   tuple(min(255, int(c * 1.25)) for c in fill))


def draw_pixel_heart(draw, x, y, scale, color=HOT, empty=False):
    """Coracao pixel art 7x6 unidades, escalado por `scale`."""
    pattern = [
        "0110110",
        "1111111",
        "1111111",
        "0111110",
        "0011100",
        "0001000",
    ]
    if empty:
        for row, line in enumerate(pattern):
            for col, ch in enumerate(line):
                if ch == "1":
                    px = x + col * scale
                    py = y + row * scale
                    pixel_rect(draw, (px, py, px + scale, py + scale), PANEL_HI)
                    pixel_rect(draw, (px + 1, py + 1, px + scale - 1, py + scale - 1), BG)
    else:
        for row, line in enumerate(pattern):
            for col, ch in enumerate(line):
                if ch == "1":
                    px = x + col * scale
                    py = y + row * scale
                    pixel_rect(draw, (px, py, px + scale, py + scale), color)
        # highlight (canto superior-esquerdo do coracao)
        hl = tuple(min(255, int(c * 1.35)) for c in color)
        for px_off, py_off in ((1, 1), (5, 1)):
            pixel_rect(draw,
                       (x + px_off * scale, y + py_off * scale,
                        x + (px_off + 1) * scale, y + (py_off + 1) * scale),
                       hl)


def apply_scanlines(img: Image.Image, every: int = 3, alpha: int = 70):
    """Linhas escuras horizontais a cada `every` px = CRT."""
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    for y in range(0, img.size[1], every):
        od.line([(0, y), (img.size[0], y)], fill=(0, 0, 0, alpha))
    img.alpha_composite(overlay) if img.mode == "RGBA" else img.paste(
        Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB"))
    return img


def apply_vignette(img: Image.Image, strength: int = 180):
    """Escurece bordas = vibe distopica."""
    w, h = img.size
    mask = Image.new("L", (w, h), 0)
    md = ImageDraw.Draw(mask)
    md.ellipse((-w // 4, -h // 4, w + w // 4, h + h // 4), fill=255)
    mask = mask.filter(ImageFilter.GaussianBlur(140))
    dark = Image.new("RGB", (w, h), BG_DEEP)
    inv = Image.eval(mask, lambda v: max(0, strength - v))
    img.paste(dark, (0, 0), inv)
    return img


def apply_grain(img: Image.Image, intensity: int = 14):
    """Ruido leve = filme antigo / TV velha."""
    w, h = img.size
    noise = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    px = noise.load()
    rng = random.Random(42)
    for _ in range(w * h // 60):
        x = rng.randrange(w)
        y = rng.randrange(h)
        v = rng.randrange(-intensity, intensity + 1)
        a = 90 if v < 0 else 60
        c = max(0, min(255, 128 + v))
        px[x, y] = (c, c, c, a)
    img.paste(noise, (0, 0), noise)
    return img


# ---------------------------------------------------------------------
# PROFILE CARD
# ---------------------------------------------------------------------

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


def _draw_label_value(draw, *, x, y, label, value,
                      label_font, value_font, label_color=DIM, value_color=INK,
                      bracket_color=GOLD_DIM):
    """[LABEL value] estilo terminal."""
    bracket_l = "["
    bracket_r = "]"
    sep = " "
    label_w, _ = text_size(draw, label, label_font)
    bl_w, _ = text_size(draw, bracket_l, label_font)
    br_w, _ = text_size(draw, bracket_r, label_font)
    sep_w, _ = text_size(draw, sep, label_font)
    val_w, _ = text_size(draw, value, value_font)

    cx = x
    draw.text((cx, y), bracket_l, font=label_font, fill=bracket_color)
    cx += bl_w
    draw.text((cx, y), label, font=label_font, fill=label_color)
    cx += label_w + sep_w
    draw.text((cx, y - 2), value, font=value_font, fill=value_color)
    cx += val_w + sep_w
    draw.text((cx, y), bracket_r, font=label_font, fill=bracket_color)
    return cx + br_w  # x final


def render_profile_card(data: ProfileCardData,
                        avatar_bytes: bytes | None) -> bytes | None:
    """Renderiza cartao 8-bit dystopian. Retorna bytes JPEG ou None."""
    try:
        # fundo base
        img = Image.new("RGB", (CARD_SIZE, CARD_SIZE), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # Padrao xadrez sutil de pixels no fundo (vazio cosmico)
        rng = random.Random(0xDEAD)
        for _ in range(1400):
            x = rng.randrange(CARD_SIZE)
            y = rng.randrange(CARD_SIZE)
            c = rng.choice([(20, 16, 22), (16, 14, 20), (28, 22, 30)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        # ===== Painel principal =====
        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, CARD_SIZE - OUT_PAD, CARD_SIZE - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK, inner=GOLD_DIM, thick=8)

        # ===== Header (terminal bar) =====
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      CARD_SIZE - OUT_PAD - 28, OUT_PAD + 96)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), GOLD_DIM)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), GOLD_DIM)

        title_font = load_font(26, mono=True, bold=True)
        season_font = load_font(20, mono=True, bold=False)
        # "ROYAL.TERMINAL // ID#0042"
        title = f"ROYAL.TERMINAL // ID#{data.royal_id.replace('RYL-', '')}"
        draw.text((header_box[0] + 22, header_box[1] + 22),
                  title, font=title_font, fill=ACID)
        # season a direita
        season_txt = f">> {data.season.upper()}"
        sw, _ = text_size(draw, season_txt, season_font)
        draw.text((header_box[2] - 22 - sw, header_box[1] + 26),
                  season_txt, font=season_font, fill=DIM)

        # blinker fake
        pixel_rect(draw, (header_box[2] - 18, header_box[1] + 26,
                          header_box[2] - 10, header_box[1] + 38), HOT)

        # ===== Avatar pixelado =====
        avatar_size = 296
        ax = OUT_PAD + 60
        ay = OUT_PAD + 130
        av = pixelated_avatar(avatar_bytes, avatar_size, data.initial)
        img.paste(av, (ax, ay), av)
        # tag "SUBJECT" debaixo do avatar
        tag_font = load_font(16, mono=True, bold=True)
        tag = "[ SUBJECT ]"
        tw, _ = text_size(draw, tag, tag_font)
        draw.text((ax + (avatar_size - tw) // 2, ay + avatar_size + 14),
                  tag, font=tag_font, fill=GOLD_DIM)

        # ===== Bloco direita: nome, classe, nivel =====
        info_x = ax + avatar_size + 48
        info_w = CARD_SIZE - OUT_PAD - 60 - info_x

        # Nome (cortado se longo)
        name_clean = ellipsize(data.name, 16).upper()
        name_size = 48 if len(name_clean) <= 10 else 38 if len(name_clean) <= 14 else 32
        name_font = load_font(name_size, mono=True, bold=True)
        draw.text((info_x, ay - 4), name_clean, font=name_font, fill=INK)

        # underline gold sob o nome
        nw, nh = text_size(draw, name_clean, name_font)
        pixel_rect(draw, (info_x, ay - 4 + nh + 6, info_x + nw, ay - 4 + nh + 10),
                   GOLD_DIM)

        # Classe em "tag"
        class_font = load_font(20, mono=True, bold=True)
        class_label = ellipsize(data.class_name, 22).upper()
        class_txt = f"// CLASSE: {class_label}"
        draw.text((info_x, ay - 4 + nh + 24),
                  class_txt, font=class_font, fill=CYAN)

        # Nivel em destaque
        lvl_label_font = load_font(18, mono=True, bold=False)
        lvl_num_font = load_font(64, mono=True, bold=True)
        lvl_y = ay - 4 + nh + 70
        draw.text((info_x, lvl_y), "NIVEL", font=lvl_label_font, fill=DIM)
        draw.text((info_x + 110, lvl_y - 18),
                  f"{data.level:02d}", font=lvl_num_font, fill=GOLD)
        if data.pts_available > 0:
            pts_font = load_font(18, mono=True, bold=True)
            pts_txt = f"!! +{data.pts_available} PTS"
            draw.text((info_x + 250, lvl_y + 8),
                      pts_txt, font=pts_font, fill=HOT)

        # XP bar chunky
        xp_y = lvl_y + 78
        xp_label_font = load_font(16, mono=True, bold=False)
        xp_val_font = load_font(18, mono=True, bold=True)
        draw.text((info_x, xp_y), "XP", font=xp_label_font, fill=DIM)
        xp_val = f"{format_br(data.xp_in_level)} / {format_br(data.xp_needed)}"
        vw, _ = text_size(draw, xp_val, xp_val_font)
        draw.text((info_x + info_w - vw, xp_y),
                  xp_val, font=xp_val_font, fill=INK)
        draw_chunky_bar(draw, (info_x, xp_y + 28, info_x + info_w, xp_y + 56),
                        data.xp_in_level, data.xp_needed,
                        fill=ACID, bg=PANEL_HI, segments=20)

        # HP em coracoes pixel art
        hp_y = xp_y + 80
        draw.text((info_x, hp_y), "HP", font=xp_label_font, fill=DIM)
        hearts_total = 10
        hearts_filled = max(0, min(hearts_total,
                                   round(hearts_total * data.hp_cur / max(1, data.hp_max))))
        heart_x = info_x + 50
        heart_scale = 4
        gap = 6
        for i in range(hearts_total):
            draw_pixel_heart(draw, heart_x + i * (7 * heart_scale + gap),
                             hp_y - 2, heart_scale,
                             color=HOT, empty=(i >= hearts_filled))
        hp_val = f"{data.hp_cur}/{data.hp_max}"
        hpw, _ = text_size(draw, hp_val, xp_val_font)
        draw.text((info_x + info_w - hpw, hp_y),
                  hp_val, font=xp_val_font, fill=HOT)

        # ===== Painel ATRIBUTOS =====
        attr_box = (OUT_PAD + 60, OUT_PAD + 540,
                    CARD_SIZE - OUT_PAD - 60, OUT_PAD + 700)
        pixel_rect(draw, attr_box, PANEL)
        # cabeçalho
        chdr = "// ATRIBUTOS"
        chdr_font = load_font(18, mono=True, bold=True)
        draw.text((attr_box[0] + 16, attr_box[1] + 12),
                  chdr, font=chdr_font, fill=GOLD_DIM)
        # divisor de tracejados
        dash_y = attr_box[1] + 44
        for dx in range(attr_box[0] + 16, attr_box[2] - 16, 12):
            pixel_rect(draw, (dx, dash_y, dx + 8, dash_y + 2), DIM)

        attrs = [
            ("FOR", data.attr_for, RUST),
            ("DES", data.attr_des, CYAN),
            ("VIT", data.attr_vit, HOT),
            ("CAR", data.attr_car, PURPLE),
        ]
        attr_label_font = load_font(18, mono=True, bold=True)
        attr_val_font = load_font(40, mono=True, bold=True)
        col_w = (attr_box[2] - attr_box[0] - 32) // 4
        for i, (lbl, val, accent) in enumerate(attrs):
            cx = attr_box[0] + 16 + i * col_w + col_w // 2
            # colored chip atras
            chip_x0 = cx - 50
            chip_x1 = cx + 50
            chip_y0 = attr_box[1] + 64
            chip_y1 = attr_box[1] + 138
            pixel_rect(draw, (chip_x0, chip_y0, chip_x1, chip_y1), BG)
            # borda accent top
            pixel_rect(draw, (chip_x0, chip_y0, chip_x1, chip_y0 + 4), accent)
            # label
            lw, _ = text_size(draw, lbl, attr_label_font)
            draw.text((cx - lw // 2, chip_y0 + 12),
                      lbl, font=attr_label_font, fill=accent)
            # valor
            vstr = f"{val:02d}"
            vw, _ = text_size(draw, vstr, attr_val_font)
            draw.text((cx - vw // 2, chip_y0 + 32),
                      vstr, font=attr_val_font, fill=INK)

        # ===== Painel STATUS =====
        status_box = (OUT_PAD + 60, OUT_PAD + 720,
                      CARD_SIZE - OUT_PAD - 60, OUT_PAD + 900)
        pixel_rect(draw, status_box, PANEL)
        draw.text((status_box[0] + 16, status_box[1] + 12),
                  "// STATUS", font=chdr_font, fill=GOLD_DIM)
        dash_y = status_box[1] + 44
        for dx in range(status_box[0] + 16, status_box[2] - 16, 12):
            pixel_rect(draw, (dx, dash_y, dx + 8, dash_y + 2), DIM)

        stats = [
            ("RANK", f"#{data.rank}/{data.total_players}"),
            ("PALAVRAS", str(data.palavras_won)),
            ("CASORIOS", str(data.casorios)),
            ("FLORINS", format_br(data.gold)),
        ]
        st_label_font = load_font(16, mono=True, bold=False)
        st_val_font = load_font(28, mono=True, bold=True)
        cell_w = (status_box[2] - status_box[0] - 32) // 4
        for i, (lbl, val) in enumerate(stats):
            cx = status_box[0] + 16 + i * cell_w + cell_w // 2
            lw, _ = text_size(draw, lbl, st_label_font)
            draw.text((cx - lw // 2, status_box[1] + 60),
                      lbl, font=st_label_font, fill=DIM)
            vw, _ = text_size(draw, val, st_val_font)
            draw.text((cx - vw // 2, status_box[1] + 88),
                      val, font=st_val_font, fill=ACID)

        # ===== Rodape: footer terminal =====
        foot_font = load_font(14, mono=True, bold=False)
        foot_left = (
            f"> LOG: {format_br(data.msg_count)} TX  ::  SINCE {data.joined_str}"
        )
        foot_right = "v0.1.ALPHA // DYSTOPIA_MODE"
        draw.text((OUT_PAD + 60, CARD_SIZE - OUT_PAD - 50),
                  foot_left, font=foot_font, fill=DIM)
        fw, _ = text_size(draw, foot_right, foot_font)
        draw.text((CARD_SIZE - OUT_PAD - 60 - fw, CARD_SIZE - OUT_PAD - 50),
                  foot_right, font=foot_font, fill=RUST)

        # ===== Pos processamento CRT =====
        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        # Vignette/escurecimento bordas
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=160)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_PROFILE_CARD_RENDER_FAILED royal_id=%s",
                         getattr(data, "royal_id", "?"))
        return None
