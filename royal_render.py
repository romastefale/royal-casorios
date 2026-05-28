"""
Royal RPG — gerador de cards 1080x1080.

Estetica: 8-bit retro + dark dystopian (CRT/terminal corrompido).
Pillow puro (sem Chromium) pra deploy leve no Railway.
"""

from __future__ import annotations

import hashlib
import io
import logging
import random
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

logger = logging.getLogger(__name__)

CARD_SIZE = 1080


# ---------------------------------------------------------------------
# Cache LRU pros cards renderizados — dimensionado pra ~500 users
# Key = tuple imutável (royal_id, level, xp_in_level, hp, gold, attrs...)
# Cache miss apenas quando estado muda. TTL extra evita lixo eterno.
# ---------------------------------------------------------------------
_CARD_CACHE: "OrderedDict[tuple, tuple[float, bytes]]" = OrderedDict()
_CARD_CACHE_MAX = 256
_CARD_CACHE_TTL = 300  # 5 min
# Renders rodam em asyncio.to_thread, entao threads concorrentes mexem na LRU.
# Lock protege move_to_end/popitem que NAO sao atomicos sob GIL composto.
_CARD_CACHE_LOCK = threading.Lock()


def cache_get(key: tuple) -> bytes | None:
    with _CARD_CACHE_LOCK:
        entry = _CARD_CACHE.get(key)
        if not entry:
            return None
        ts, payload = entry
        if time.time() - ts > _CARD_CACHE_TTL:
            _CARD_CACHE.pop(key, None)
            return None
        _CARD_CACHE.move_to_end(key)
        return payload


def cache_put(key: tuple, payload: bytes) -> None:
    with _CARD_CACHE_LOCK:
        _CARD_CACHE[key] = (time.time(), payload)
        _CARD_CACHE.move_to_end(key)
        while len(_CARD_CACHE) > _CARD_CACHE_MAX:
            _CARD_CACHE.popitem(last=False)


def cache_stats() -> dict:
    with _CARD_CACHE_LOCK:
        return {"size": len(_CARD_CACHE),
                "max": _CARD_CACHE_MAX,
                "ttl": _CARD_CACHE_TTL}

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
AMBER = (220, 140, 60)           # CRT ambar
MAGENTA = (200, 90, 160)         # plasma glitch
NEON_BLUE = (80, 140, 220)       # holograma frio
JADE = (60, 180, 130)            # bioluminescente
WHITE = (240, 232, 220)
BLACK = (0, 0, 0)


# ---------------------------------------------------------------------
# Variantes de paleta (retro-futurist dark). 1 por player.
# Seleciona por hash do royal_id pra manter consistencia entre renders.
# ---------------------------------------------------------------------
PALETTE_VARIANTS = [
    # 0 — toxic hospital (verde acido + ferrugem)
    {"name": "TOXIC",   "header": ACID,    "footer": RUST,
     "level": GOLD,     "xp": ACID,
     "chips": [RUST, CYAN, HOT, PURPLE]},
    # 1 — CRT ambar (terminal antigo)
    {"name": "AMBER",   "header": AMBER,   "footer": RUST,
     "level": AMBER,    "xp": AMBER,
     "chips": [AMBER, GOLD, HOT, JADE]},
    # 2 — plasma violet (glitch sintetico)
    {"name": "PLASMA",  "header": MAGENTA, "footer": PURPLE,
     "level": MAGENTA,  "xp": MAGENTA,
     "chips": [PURPLE, MAGENTA, HOT, CYAN]},
    # 3 — arctic monitor (frio clinico)
    {"name": "ARCTIC",  "header": NEON_BLUE, "footer": CYAN,
     "level": CYAN,     "xp": NEON_BLUE,
     "chips": [CYAN, NEON_BLUE, HOT, PURPLE]},
    # 4 — biolab (verde jade + ouro)
    {"name": "BIOLAB",  "header": JADE,    "footer": ACID,
     "level": GOLD,     "xp": JADE,
     "chips": [JADE, ACID, HOT, AMBER]},
    # 5 — sangue real (vinho real corrompido)
    {"name": "BLOOD",   "header": HOT,     "footer": RUST,
     "level": GOLD,     "xp": HOT,
     "chips": [HOT, GOLD, AMBER, RUST]},
]


def pick_palette(seed: str | int | None) -> dict:
    """Retorna 1 variante de paleta determinada pelo seed (royal_id)."""
    if seed is None:
        return PALETTE_VARIANTS[0]
    h = abs(hash(str(seed))) % len(PALETTE_VARIANTS)
    return PALETTE_VARIANTS[h]


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


# Telegram em mobile renderiza fotos em ~320-480px de largura. Com canvas
# 1080, o scale chega a ~30-45% — então uma fonte source de 14px vira ~5px
# na tela. Tiers diferentes pra fontes legíveis SEM quebrar layout:
#  - footers/tags pequenos (<14): scale 1.55, piso 22
#  - corpo normal (14-63): scale 1.6, piso 26
#  - displays médios (64-119): scale 1.10 (já grandes)
#  - hero/headline (>=120): sem scale (já gigantes)
def load_font(size: int, *, mono: bool = True, bold: bool = True):
    if size >= 120:
        scaled = size
    elif size >= 64:
        scaled = int(round(size * 1.10))
    elif size >= 14:
        scaled = max(int(round(size * 1.6)), 26)
    else:
        scaled = max(int(round(size * 1.55)), 22)
    if mono:
        chain = FONT_MONO if bold else FONT_MONO_REG
    else:
        chain = FONT_BOLD if bold else FONT_REG
    for path in chain:
        try:
            if Path(path).exists():
                return ImageFont.truetype(path, size=scaled)
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


def procedural_sigil(seed_key: str, size: int,
                     palette: dict | None = None) -> Image.Image:
    """Sigilo 8-bit determinístico (estilo identicon retro).
    Gera um padrão 10x10 espelhado horizontalmente (= simétrico) com 2 cores
    da paleta sobre fundo escuro, então upscala NEAREST pra `size`.
    Sempre o MESMO sigilo pro MESMO royal_id — funciona como brasão do player.
    """
    rng = random.Random(hash(seed_key) & 0xFFFFFFFF)
    grid = 10  # resolução do sigilo (par pra espelhar limpo)
    bg = (14, 12, 18)
    if palette is None:
        fg_a = ACID
        fg_b = HOT
    else:
        fg_a = palette.get("header", ACID)
        fg_b = palette.get("level", HOT)

    base = Image.new("RGB", (grid, grid), bg)
    px = base.load()
    half = grid // 2
    # densidade ~55% nos pixels ativos pra dar peso visual sem encher
    for y in range(grid):
        for x in range(half):
            r = rng.random()
            if r < 0.55:
                if r < 0.18:
                    px[x, y] = fg_b
                else:
                    px[x, y] = fg_a
            # espelha horizontal
            px[grid - 1 - x, y] = px[x, y]

    return base.resize((size, size), Image.NEAREST)


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
    """Renderiza cartao 8-bit dystopian. Retorna bytes JPEG ou None.
    Paleta varia por royal_id (5+ variantes retro-futuristas dark).
    Resultado é cacheado por estado do player (LRU 256 entries, TTL 5min)."""
    # cache key = tudo que afeta o pixel final
    cache_key = (
        "profile", data.royal_id, data.name, data.class_name, data.season,
        data.level, data.xp_in_level, data.xp_needed,
        data.hp_cur, data.hp_max,
        data.attr_for, data.attr_des, data.attr_vit, data.attr_car,
        data.pts_available, data.rank, data.total_players,
        data.palavras_won, data.casorios, data.gold,
        data.msg_count, data.joined_str,
        # Hash da foto: invalida cache se o player trocar a foto no Telegram
        hashlib.md5(avatar_bytes).hexdigest() if avatar_bytes else None,
    )
    cached = cache_get(cache_key)
    if cached:
        return cached
    try:
        pal = pick_palette(data.royal_id)
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

        title_font = load_font(22, mono=True, bold=True)
        season_font = load_font(16, mono=True, bold=False)
        # Título curto pra caber junto da season à direita
        title = f"ID#{data.royal_id.replace('RYL-', '')} // {pal['name']}"
        draw.text((header_box[0] + 22, header_box[1] + 22),
                  title, font=title_font, fill=pal["header"])
        # season a direita
        season_txt = f">> {data.season.upper()}"
        sw, _ = text_size(draw, season_txt, season_font)
        draw.text((header_box[2] - 22 - sw, header_box[1] + 28),
                  season_txt, font=season_font, fill=DIM)

        # blinker fake (canto direito do header, abaixo da season)
        pixel_rect(draw, (header_box[2] - 14, header_box[3] - 18,
                          header_box[2] - 8, header_box[3] - 10), HOT)

        # ===== Avatar híbrido: SIGILO 8-bit principal + foto real no canto =====
        avatar_size = 296
        ax = OUT_PAD + 60
        ay = OUT_PAD + 130

        # 1) Sigilo procedural (brasão do player — determinístico por royal_id)
        sigil_inner = avatar_size - 12
        sigil = procedural_sigil(data.royal_id, sigil_inner, pal)
        # Moldura do quadro principal
        pixel_rect(draw, (ax, ay, ax + avatar_size, ay + avatar_size), BLACK)
        pixel_rect(draw, (ax + 4, ay + 4,
                          ax + avatar_size - 4, ay + avatar_size - 4), GOLD_DIM)
        pixel_rect(draw, (ax + 6, ay + 6,
                          ax + avatar_size - 6, ay + avatar_size - 6), BLACK)
        img.paste(sigil, (ax + 6, ay + 6))

        # 2) Thumbnail da foto real no canto inferior direito (vestígio)
        if avatar_bytes:
            try:
                face_outer = 92
                face = pixelated_avatar(avatar_bytes, face_outer, data.initial)
                fx = ax + avatar_size - face_outer - 8
                fy = ay + avatar_size - face_outer - 8
                img.paste(face, (fx, fy), face)
                # micro-label discreto acima do thumbnail
                face_tag_font = load_font(11, mono=True, bold=True)
                ft = "[FACE]"
                ftw, _ = text_size(draw, ft, face_tag_font)
                draw.text((fx + (face_outer - ftw) // 2, fy - 18),
                          ft, font=face_tag_font, fill=DIM)
            except Exception:
                logger.warning("face thumbnail failed", exc_info=True)

        # tag "[ SIGIL ]" debaixo do avatar
        tag_font = load_font(16, mono=True, bold=True)
        tag = "[ SIGIL ]"
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
                  f"{data.level:02d}", font=lvl_num_font, fill=pal["level"])
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
                        fill=pal["xp"], bg=PANEL_HI, segments=20)

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
            ("FOR", data.attr_for, pal["chips"][0]),
            ("DES", data.attr_des, pal["chips"][1]),
            ("VIT", data.attr_vit, pal["chips"][2]),
            ("CAR", data.attr_car, pal["chips"][3]),
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
                      val, font=st_val_font, fill=pal["xp"])

        # ===== Rodape: footer terminal =====
        # Textos curtos pra não colidir mesmo escalados; 12px = ~22px piso
        foot_font = load_font(12, mono=True, bold=False)
        foot_left = f"> LOG: {format_br(data.msg_count)} TX"
        foot_right = f"v0.1 // {pal['name']}_MODE"
        draw.text((OUT_PAD + 60, CARD_SIZE - OUT_PAD - 50),
                  foot_left, font=foot_font, fill=DIM)
        fw, _ = text_size(draw, foot_right, foot_font)
        draw.text((CARD_SIZE - OUT_PAD - 60 - fw, CARD_SIZE - OUT_PAD - 50),
                  foot_right, font=foot_font, fill=pal["footer"])

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
        payload = buf.getvalue()
        cache_put(cache_key, payload)
        return payload
    except Exception:
        logger.exception("ROYAL_PROFILE_CARD_RENDER_FAILED royal_id=%s",
                         getattr(data, "royal_id", "?"))
        return None


# =====================================================================
# RANKING PODIUM CARD — top 3 da temporada em estilo arcade
# =====================================================================

@dataclass(frozen=True)
class RankingEntry:
    rank: int
    royal_id: str
    name: str
    season_xp: int
    level: int


def render_ranking_card(season_label: str,
                        entries: tuple[RankingEntry, ...]) -> bytes | None:
    """Pódio top-3 estilo arcade high-score. Recebe tupla (hashavel) pra cache."""
    if not entries:
        return None
    cache_key = ("ranking", season_label,
                 tuple((e.rank, e.royal_id, e.name, e.season_xp, e.level)
                       for e in entries[:3]))
    cached = cache_get(cache_key)
    if cached:
        return cached
    try:
        pal = pick_palette(season_label)
        img = Image.new("RGB", (CARD_SIZE, CARD_SIZE), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # estatica de fundo
        rng = random.Random(hash(season_label) & 0xFFFF)
        for _ in range(1200):
            x = rng.randrange(CARD_SIZE)
            y = rng.randrange(CARD_SIZE)
            c = rng.choice([(20, 16, 22), (16, 14, 20), (28, 22, 30)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, CARD_SIZE - OUT_PAD, CARD_SIZE - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK, inner=GOLD_DIM, thick=8)

        # Header
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      CARD_SIZE - OUT_PAD - 28, OUT_PAD + 110)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), GOLD_DIM)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), GOLD_DIM)
        title_font = load_font(28, mono=True, bold=True)
        season_font = load_font(20, mono=True, bold=False)
        draw.text((header_box[0] + 22, header_box[1] + 22),
                  "ROYAL.HIGHSCORE.SYS", font=title_font, fill=pal["header"])
        season_txt = f">> {season_label.upper()}"
        sw, _ = text_size(draw, season_txt, season_font)
        draw.text((header_box[2] - 22 - sw, header_box[1] + 28),
                  season_txt, font=season_font, fill=DIM)
        pixel_rect(draw, (header_box[2] - 18, header_box[1] + 28,
                          header_box[2] - 10, header_box[1] + 40), HOT)

        # Pódio: posições 2-1-3 (1º no centro mais alto)
        medal_colors = {1: GOLD, 2: (180, 180, 180), 3: (180, 100, 60)}
        order = [(2, 0), (1, 1), (3, 2)]  # rank -> coluna
        top3 = {e.rank: e for e in entries if e.rank <= 3}

        podium_y = OUT_PAD + 200
        col_w = (CARD_SIZE - OUT_PAD * 2 - 80) // 3
        col_gap = 20
        base_x = OUT_PAD + 40

        for rank, col in order:
            e = top3.get(rank)
            cx0 = base_x + col * (col_w + col_gap)
            cx1 = cx0 + col_w
            # altura do pódio varia
            heights = {1: 320, 2: 240, 3: 200}
            ph = heights[rank]
            podium_top = podium_y + (320 - ph) + 280
            podium_bot = OUT_PAD + 28 + 800

            # bloco pódio
            pixel_rect(draw, (cx0, podium_top, cx1, podium_bot), PANEL)
            pixel_rect(draw, (cx0, podium_top, cx1, podium_top + 6),
                       medal_colors[rank])
            # numero gigante
            num_font = load_font(120, mono=True, bold=True)
            num = str(rank)
            nw, nh = text_size(draw, num, num_font)
            draw.text((cx0 + (col_w - nw) // 2, podium_top + 20),
                      num, font=num_font, fill=medal_colors[rank])

            # caixa do jogador acima do pódio
            box_top = podium_top - 160
            pixel_rect(draw, (cx0, box_top, cx1, podium_top - 12), PANEL)
            pixel_rect(draw, (cx0, box_top, cx1, box_top + 4),
                       medal_colors[rank])
            if e:
                medal_txt = {1: "1ST", 2: "2ND", 3: "3RD"}[rank]
                mt_font = load_font(16, mono=True, bold=True)
                mw, _ = text_size(draw, medal_txt, mt_font)
                draw.text((cx0 + (col_w - mw) // 2, box_top + 12),
                          medal_txt, font=mt_font, fill=medal_colors[rank])

                name_clean = ellipsize(e.name, 14).upper()
                ns = 22 if len(name_clean) <= 10 else 18
                name_font = load_font(ns, mono=True, bold=True)
                nw2, _ = text_size(draw, name_clean, name_font)
                draw.text((cx0 + (col_w - nw2) // 2, box_top + 40),
                          name_clean, font=name_font, fill=INK)

                id_font = load_font(14, mono=True, bold=False)
                idw, _ = text_size(draw, e.royal_id, id_font)
                draw.text((cx0 + (col_w - idw) // 2, box_top + 72),
                          e.royal_id, font=id_font, fill=DIM)

                xp_font = load_font(20, mono=True, bold=True)
                xp_txt = format_br(e.season_xp) + " XP"
                xpw, _ = text_size(draw, xp_txt, xp_font)
                draw.text((cx0 + (col_w - xpw) // 2, box_top + 100),
                          xp_txt, font=xp_font, fill=pal["xp"])

                lv_font = load_font(14, mono=True, bold=False)
                lv_txt = f"LV {e.level:02d}"
                lvw, _ = text_size(draw, lv_txt, lv_font)
                draw.text((cx0 + (col_w - lvw) // 2, box_top + 130),
                          lv_txt, font=lv_font, fill=DIM)
            else:
                empty_font = load_font(16, mono=True, bold=False)
                draw.text((cx0 + 20, box_top + 60),
                          "[ VAGO ]", font=empty_font, fill=DIM)

        # Lista dos demais (4-10) em rodapé
        rest = [e for e in entries if e.rank > 3][:7]
        if rest:
            rest_y = OUT_PAD + 28 + 820
            rest_font = load_font(16, mono=True, bold=False)
            for i, e in enumerate(rest):
                col = i % 2
                row = i // 2
                rx = OUT_PAD + 40 + col * ((CARD_SIZE - OUT_PAD * 2 - 80) // 2 + 20)
                ry = rest_y + row * 22
                name_short = ellipsize(e.name, 14)
                line = f" {e.rank:02d}. {name_short:<14} {format_br(e.season_xp)} XP"
                draw.text((rx, ry), line, font=rest_font, fill=DIM)

        # Footer — textos curtos pra evitar colisão
        foot_font = load_font(12, mono=True, bold=False)
        foot_left = f"> N={len(entries)}"
        foot_right = f"v0.1 // {pal['name']}_MODE"
        draw.text((OUT_PAD + 60, CARD_SIZE - OUT_PAD - 50),
                  foot_left, font=foot_font, fill=DIM)
        fw, _ = text_size(draw, foot_right, foot_font)
        draw.text((CARD_SIZE - OUT_PAD - 60 - fw, CARD_SIZE - OUT_PAD - 50),
                  foot_right, font=foot_font, fill=pal["footer"])

        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=160)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90, optimize=True)
        payload = buf.getvalue()
        cache_put(cache_key, payload)
        return payload
    except Exception:
        logger.exception("ROYAL_RANKING_CARD_RENDER_FAILED")
        return None


# =====================================================================
# LEVEL-UP CARD — pop celebrativo, leve, sem cachear (sempre fresh)
# =====================================================================

def render_levelup_card(royal_id: str, name: str,
                        new_level: int, class_name: str = "") -> bytes | None:
    """Card menor (1080x540) pra anunciar level-up. Sem cache (evento único)."""
    try:
        pal = pick_palette(royal_id)
        W, H = CARD_SIZE, 540
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # estatica
        rng = random.Random(hash(royal_id) & 0xFFFF)
        for _ in range(600):
            x = rng.randrange(W); y = rng.randrange(H)
            c = rng.choice([(20, 16, 22), (28, 22, 30), pal["header"]])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, W - OUT_PAD, H - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK, inner=pal["header"], thick=8)

        center_x = W // 2

        # Header alerta (topo)
        alert_font = load_font(18, mono=True, bold=True)
        alert_txt = ">> LEVEL_UP.SYS // ALERTA"
        aw, ah = text_size(draw, alert_txt, alert_font)
        draw.text((center_x - aw // 2, OUT_PAD + 24),
                  alert_txt, font=alert_font, fill=HOT)

        # Nome (logo abaixo do alerta)
        name_clean = ellipsize(name, 22).upper()
        nfont = load_font(22, mono=True, bold=True)
        nw2, nh2 = text_size(draw, name_clean, nfont)
        name_y = OUT_PAD + 24 + ah + 14
        draw.text((center_x - nw2 // 2, name_y),
                  name_clean, font=nfont, fill=INK)
        next_y = name_y + nh2 + 6
        if class_name:
            cfont = load_font(14, mono=True, bold=False)
            ctxt = f"// {ellipsize(class_name, 24).upper()}"
            cw, ch_ = text_size(draw, ctxt, cfont)
            draw.text((center_x - cw // 2, next_y),
                      ctxt, font=cfont, fill=CYAN)
            next_y += ch_ + 4

        # Footer / PTS (parte de baixo)
        pts_font = load_font(16, mono=True, bold=True)
        pts_txt = "!! +1 PT  ::  /royalup"
        pw, ph = text_size(draw, pts_txt, pts_font)
        pts_y = H - OUT_PAD - ph - 24
        draw.text((center_x - pw // 2, pts_y),
                  pts_txt, font=pts_font, fill=ACID)

        # Label "NIVEL ATINGIDO" (acima do número)
        lab_font = load_font(18, mono=True, bold=True)
        lab = "NIVEL ATINGIDO"
        lw, lh = text_size(draw, lab, lab_font)

        # NIVEL gigante centrado no espaço entre header e pts
        big_font = load_font(140, mono=True, bold=True)
        num = f"{new_level:02d}"
        nw, nh = text_size(draw, num, big_font)
        avail_top = next_y + 10
        avail_bot = pts_y - 10
        block_h = lh + 8 + nh
        block_top = avail_top + max(0, ((avail_bot - avail_top) - block_h) // 2)
        # Label primeiro
        draw.text((center_x - lw // 2, block_top),
                  lab, font=lab_font, fill=DIM)
        # Numero embaixo do label
        num_y = block_top + lh + 8
        draw.text((center_x - nw // 2 + 6, num_y + 6),
                  num, font=big_font, fill=BLACK)
        draw.text((center_x - nw // 2, num_y),
                  num, font=big_font, fill=pal["level"])

        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=140)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=88, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_LEVELUP_CARD_RENDER_FAILED royal_id=%s", royal_id)
        return None
