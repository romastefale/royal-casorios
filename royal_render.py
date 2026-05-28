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

import royal_avatars

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
# Fontes BUNDLED no repo (fonts/) — prioritárias pra garantir suporte a
# acentos PT-BR (ç ã í ó) no Railway, onde /usr/share/fonts pode não existir
# ou o Pillow cair pro PILfont bitmap ASCII-only.
_FONTS_DIR = Path(__file__).parent / "fonts"
FONT_MONO = [
    str(_FONTS_DIR / "DejaVuSansMono-Bold.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
    str(_FONTS_DIR / "DejaVuSansMono.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
]
FONT_MONO_REG = [
    str(_FONTS_DIR / "DejaVuSansMono.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    str(_FONTS_DIR / "DejaVuSansMono-Bold.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
]
FONT_BOLD = [
    str(_FONTS_DIR / "DejaVuSans-Bold.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    str(_FONTS_DIR / "DejaVuSansMono-Bold.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
]
FONT_REG = [
    str(_FONTS_DIR / "DejaVuSans.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    str(_FONTS_DIR / "DejaVuSansMono.ttf"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
]

# =====================================================================
# Anti-tofu — fallback per-glyph pra display names com Unicode estilizado
# =====================================================================
# Telegram permite nomes tipo "𝓜𝓪𝓻𝓲𝓪" (Mathematical Alphanumeric U+1D400+),
# "ⓟⓛⓐⓨⓔⓡ" (Enclosed Alphanumerics U+2460+), "Ｆｕｌｌｗｉｄｔｈ" (U+FF00+),
# que DejaVu nao cobre. Resultado: tofu (caixinha vazia) no card.
#
# Solucao baseada em pesquisa oficial:
# - Google Noto family (No-Tofu) eh explicitamente projetada pra cobrir
#   todo o Unicode. License: SIL OFL 1.1 (uso comercial OK).
# - Noto Sans Math cobre U+1D400-U+1D7FF (math alphanumeric).
# - Noto Sans Symbols cobre U+2460-U+24FF (enclosed alphanumerics).
# - Noto Sans Symbols 2 cobre extras (musica, jogos, technical).
# - Pillow NAO tem fallback nativo -> implementado per-glyph aqui.
#
# Fontes baixadas em fonts/ via repo oficial notofonts/notofonts.github.io.
FALLBACK_FONT_PATHS = [
    str(_FONTS_DIR / "NotoSans-Regular.ttf"),
    str(_FONTS_DIR / "NotoSansMath-Regular.ttf"),
    str(_FONTS_DIR / "NotoSansSymbols-Regular.ttf"),
    str(_FONTS_DIR / "NotoSansSymbols2-Regular.ttf"),
]

# Cache (path, size) -> ImageFont. Evita reabrir TTF a cada glifo.
_FB_FONT_CACHE: dict[tuple[str, int], ImageFont.FreeTypeFont] = {}
# Cache path -> set(int codepoints). Lido 1x do TTF via fonttools.
# Pillow 12 NAO expoe cmap (font.font nao tem getbest_cmap nem similar
# confiavel), e bbox heuristica falha (notdef tem mesmo bbox de chars
# reais). fonttools eh a unica forma confiavel de saber se um glifo
# existe sem renderizar e comparar pixels.
_FONT_CMAP_CACHE: dict[str, frozenset] = {}


def _font_cmap(path: str) -> frozenset:
    """Set imutavel de codepoints (int) que a fonte cobre. Cached."""
    cm = _FONT_CMAP_CACHE.get(path)
    if cm is not None:
        return cm
    try:
        from fontTools.ttLib import TTFont
        with TTFont(path, lazy=True) as tt:
            cm = frozenset(tt.getBestCmap().keys())
    except Exception:
        cm = frozenset()  # nao sabe -> primary sempre escolhida
    _FONT_CMAP_CACHE[path] = cm
    return cm


def _fb_font(path: str, size: int):
    key = (path, size)
    f = _FB_FONT_CACHE.get(key)
    if f is None:
        try:
            f = ImageFont.truetype(path, size=size)
            _FB_FONT_CACHE[key] = f
        except Exception:
            return None
    return f


def _font_has_char(font, char: str) -> bool:
    """True se a fonte tem glifo real pro char (nao eh 'notdef'/tofu).
    Usa o cmap real parseado via fontTools (Pillow 12 nao expoe)."""
    try:
        path = getattr(font, "path", None)
        if not path:
            return True  # fonte sem path conhecido -> assume cobre
        return ord(char) in _font_cmap(path)
    except Exception:
        return True


def _fallback_fonts_for(size: int) -> list:
    return [_fb_font(p, size) for p in FALLBACK_FONT_PATHS]


def _resolve_glyph_font(char: str, primary, fallbacks: list):
    """Primary se cobrir, senao 1o fallback que cobre. Em ultimo caso volta
    pra primary (renderiza tofu mas nao crasha)."""
    if _font_has_char(primary, char):
        return primary
    for f in fallbacks:
        if f is not None and _font_has_char(f, char):
            return f
    return primary


def _char_advance(draw, ch: str, font) -> float:
    """Advance horizontal pro cursor — usa textlength quando disponivel
    (respeita side bearings/kerning intra-char), fallback bbox width."""
    try:
        return draw.textlength(ch, font=font)
    except Exception:
        bbox = draw.textbbox((0, 0), ch, font=font)
        return float(bbox[2] - bbox[0])


def draw_text_smart(draw, xy, text: str, font, fill) -> None:
    """draw.text() char-por-char com fallback Noto pra evitar tofu em
    display names com Unicode estilizado (math, symbols, fullwidth).
    Mesma signature visual que draw.text mas em forma de helper."""
    if not text:
        return
    x, y = xy
    fb = _fallback_fonts_for(font.size)
    for ch in text:
        f = _resolve_glyph_font(ch, font, fb)
        draw.text((round(x), y), ch, font=f, fill=fill)
        x += _char_advance(draw, ch, f)


def text_size_smart(draw: ImageDraw.ImageDraw, text: str, primary) -> tuple[int, int]:
    """text_size() considerando fallbacks per-glyph. Width somado via
    textlength (mesmo metodo de advance que draw_text_smart usa, entao
    width casa com o desenho). Height = max por glifo."""
    if not text:
        return (0, 0)
    fb = _fallback_fonts_for(primary.size)
    total_w = 0.0
    max_h = 0
    for ch in text:
        f = _resolve_glyph_font(ch, primary, fb)
        total_w += _char_advance(draw, ch, f)
        bbox = draw.textbbox((0, 0), ch, font=f)
        h = bbox[3] - bbox[1]
        if h > max_h:
            max_h = h
    return (int(round(total_w)), max_h)


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
    # Fallback Railway-safe: load_default(size=...) requer Pillow >= 10.1 e
    # retorna a DejaVuSans embutida na lib (TrueType, escalavel). Sem o
    # parametro `size`, retorna um bitmap fixo ~10px que IGNORA o tamanho
    # — foi o que causou o bug das letras minusculas no deploy.
    try:
        return ImageFont.load_default(size=scaled)
    except TypeError:
        # Pillow muito antigo — pelo menos NAO travar
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


# =====================================================================
# 8-BIT ICONS — sprites simples estilo Stardew/SNES
# =====================================================================
# Cada sprite eh uma lista de strings com '.' (vazio), '#' (cor principal)
# e 'o' (highlight mais claro, opcional). Tamanho 10x10 unidades por
# default. Renderizadas via draw_sprite() com pixel_rect (sem antialias).

SPRITE_SWORD = [   # FORCA — espada vertical com guarda
    "....##....",
    "....##....",
    "....##....",
    "....##....",
    "....##....",
    ".########.",
    "....##....",
    "....##....",
    "....##....",
    "...####...",
]
SPRITE_BOOT = [    # DESTREZA — bota lateral
    "..........",
    "..........",
    "..####....",
    "..#####...",
    "..######..",
    "..#######.",
    "..########",
    ".#########",
    "##########",
    "##########",
]
SPRITE_HEART = [   # VITAL — coracao cheio
    ".##..##...",
    "########..",
    "########..",
    ".######...",
    "..####....",
    "...##.....",
    "..........",
    "..........",
    "..........",
    "..........",
]
SPRITE_MASK = [    # CARISMA — mascara de teatro
    "..######..",
    ".########.",
    "##.####.##",
    "##.####.##",
    "##.####.##",
    ".########.",
    ".########.",
    "..######..",
    "...####...",
    "....##....",
]
SPRITE_TROPHY = [  # POSICAO — trofeu
    "#########.",
    "#.#####.#.",
    "#.#####.#.",
    ".#######..",
    "..#####...",
    "...###....",
    "...###....",
    "..#####...",
    ".#######..",
    "#########.",
]
SPRITE_BOOK = [    # PALAVRAS — livro/pergaminho
    "##########",
    "#........#",
    "#.######.#",
    "#........#",
    "#.######.#",
    "#........#",
    "#.######.#",
    "#........#",
    "#.######.#",
    "##########",
]
SPRITE_RINGS = [   # CASORIOS — dois aneis entrelacados
    "..........",
    ".####.....",
    "#....#....",
    "#..####...",
    "#.##..#...",
    ".##.#..#..",
    "...#..##..",
    "...#..#.#.",
    "....####.#",
    ".........#",
]
SPRITE_COIN = [    # FLORINS — moeda com F dentro
    "..######..",
    ".########.",
    "##.####.##",
    "##.####.##",
    "##.#....##",
    "##.######.",
    "##.####.##",
    "##.####.##",
    ".########.",
    "..######..",
]


def draw_sprite(draw, x: int, y: int, sprite: list[str],
                scale: int, color, highlight=None):
    """Desenha sprite 8-bit a partir de strings.

    '#' = cor principal, 'o' = highlight (se None usa cor *1.4).
    Sem antialiasing — todos os pixels alinhados.
    """
    if highlight is None:
        highlight = tuple(min(255, int(c * 1.35)) for c in color)
    for row, line in enumerate(sprite):
        for col, ch in enumerate(line):
            if ch == "#":
                px = x + col * scale
                py = y + row * scale
                pixel_rect(draw, (px, py, px + scale, py + scale), color)
            elif ch == "o":
                px = x + col * scale
                py = y + row * scale
                pixel_rect(draw, (px, py, px + scale, py + scale), highlight)


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
    avatar_slug: str | None = None


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
        data.avatar_slug,
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

        # ===== Avatar híbrido: PORTRAIT 256x256 principal + foto real no canto =====
        avatar_size = 296
        ax = OUT_PAD + 60
        ay = OUT_PAD + 130

        # 1) Avatar escolhido pelo player (ou default deterministico por royal_id).
        #    Fallback pro sigilo procedural se PNG sumir.
        inner_size = avatar_size - 12
        resolved_slug = royal_avatars.resolve_slug(
            data.avatar_slug, data.royal_id)
        portrait = royal_avatars.load_avatar(resolved_slug, inner_size)
        # Moldura do quadro principal
        pixel_rect(draw, (ax, ay, ax + avatar_size, ay + avatar_size), BLACK)
        pixel_rect(draw, (ax + 4, ay + 4,
                          ax + avatar_size - 4, ay + avatar_size - 4), GOLD_DIM)
        pixel_rect(draw, (ax + 6, ay + 6,
                          ax + avatar_size - 6, ay + avatar_size - 6), BLACK)
        if portrait is not None:
            img.paste(portrait, (ax + 6, ay + 6), portrait)
        else:
            sigil = procedural_sigil(data.royal_id, inner_size, pal)
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
                ft = "[FOTO]"
                ftw, _ = text_size(draw, ft, face_tag_font)
                draw.text((fx + (face_outer - ftw) // 2, fy - 18),
                          ft, font=face_tag_font, fill=DIM)
            except Exception:
                logger.warning("face thumbnail failed", exc_info=True)

        # tag "[ BRASAO ]" debaixo do avatar
        tag_font = load_font(16, mono=True, bold=True)
        tag = "[ BRASÃO ]"
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
        draw_text_smart(draw, (info_x, ay - 4), name_clean, name_font, INK)

        # (sem underline — cortava letras com descender tipo @ no usuario)
        nw, nh = text_size(draw, name_clean, name_font)

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
        draw.text((info_x, lvl_y), "NÍVEL", font=lvl_label_font, fill=DIM)
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
            ("FORÇA",    data.attr_for, pal["chips"][0], SPRITE_SWORD),
            ("DESTREZA", data.attr_des, pal["chips"][1], SPRITE_BOOT),
            ("VITAL",    data.attr_vit, pal["chips"][2], SPRITE_HEART),
            ("CARISMA",  data.attr_car, pal["chips"][3], SPRITE_MASK),
        ]
        attr_label_font = load_font(16, mono=True, bold=True)
        attr_val_font = load_font(22, mono=True, bold=True)
        col_w = (attr_box[2] - attr_box[0] - 32) // 4
        for i, (lbl, val, accent, sprite) in enumerate(attrs):
            cx = attr_box[0] + 16 + i * col_w + col_w // 2
            # colored chip atras (mais largo p/ caber palavras inteiras)
            chip_x0 = cx - 92
            chip_x1 = cx + 92
            chip_y0 = attr_box[1] + 54
            chip_y1 = attr_box[1] + 156
            pixel_rect(draw, (chip_x0, chip_y0, chip_x1, chip_y1), BG)
            # borda accent top
            pixel_rect(draw, (chip_x0, chip_y0, chip_x1, chip_y0 + 4), accent)
            # icone 8-bit no topo (grid 10x10 * scale 3 = 30x30 px)
            sprite_scale = 3
            sprite_w = 10 * sprite_scale
            draw_sprite(draw, cx - sprite_w // 2, chip_y0 + 10,
                        sprite, sprite_scale, accent)
            # label
            lw, _ = text_size(draw, lbl, attr_label_font)
            draw.text((cx - lw // 2, chip_y0 + 46),
                      lbl, font=attr_label_font, fill=accent)
            # valor
            vstr = f"{val:02d}"
            vw, _ = text_size(draw, vstr, attr_val_font)
            draw.text((cx - vw // 2, chip_y0 + 68),
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
            ("POSIÇÃO",  f"#{data.rank}/{data.total_players}", SPRITE_TROPHY, pal["chips"][0]),
            ("PALAVRAS", str(data.palavras_won),                SPRITE_BOOK,   pal["chips"][1]),
            ("CASÓRIOS", str(data.casorios),                    SPRITE_RINGS,  pal["chips"][2]),
            ("FLORINS",  format_br(data.gold),                  SPRITE_COIN,   pal["chips"][3]),
        ]
        st_label_font = load_font(14, mono=True, bold=False)
        st_val_font = load_font(26, mono=True, bold=True)
        cell_w = (status_box[2] - status_box[0] - 32) // 4
        for i, (lbl, val, sprite, accent) in enumerate(stats):
            cx = status_box[0] + 16 + i * cell_w + cell_w // 2
            # icone 8-bit
            sprite_scale = 2
            sprite_w = 10 * sprite_scale
            draw_sprite(draw, cx - sprite_w // 2, status_box[1] + 52,
                        sprite, sprite_scale, accent)
            # label
            lw, _ = text_size(draw, lbl, st_label_font)
            draw.text((cx - lw // 2, status_box[1] + 80),
                      lbl, font=st_label_font, fill=DIM)
            vw, _ = text_size(draw, val, st_val_font)
            draw.text((cx - vw // 2, status_box[1] + 108),
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
    avatar_slug: str | None = None


def render_ranking_card(season_label: str,
                        entries: tuple[RankingEntry, ...]) -> bytes | None:
    """Pódio top-3 estilo arcade high-score. Recebe tupla (hashavel) pra cache."""
    if not entries:
        return None
    cache_key = ("ranking", season_label,
                 tuple((e.rank, e.royal_id, e.name, e.season_xp, e.level,
                        e.avatar_slug)
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
                # Portrait do top — 120x120 centrado acima da caixa do nome.
                # Tamanho varia conforme rank pra reforcar hierarquia.
                port_size = {1: 140, 2: 120, 3: 110}[rank]
                resolved = royal_avatars.resolve_slug(
                    e.avatar_slug, e.royal_id)
                portrait = royal_avatars.load_avatar(resolved, port_size)
                if portrait is not None:
                    pxc = cx0 + (col_w - port_size) // 2
                    pyc = box_top - port_size - 14
                    # Moldura medal-color em volta do portrait
                    pixel_rect(draw,
                               (pxc - 4, pyc - 4,
                                pxc + port_size + 4, pyc + port_size + 4),
                               BLACK)
                    pixel_rect(draw,
                               (pxc - 2, pyc - 2,
                                pxc + port_size + 2, pyc + port_size + 2),
                               medal_colors[rank])
                    img.paste(portrait, (pxc, pyc), portrait)

                medal_txt = {1: "1ST", 2: "2ND", 3: "3RD"}[rank]
                mt_font = load_font(16, mono=True, bold=True)
                mw, _ = text_size(draw, medal_txt, mt_font)
                draw.text((cx0 + (col_w - mw) // 2, box_top + 12),
                          medal_txt, font=mt_font, fill=medal_colors[rank])

                name_clean = ellipsize(e.name, 14).upper()
                ns = 22 if len(name_clean) <= 10 else 18
                name_font = load_font(ns, mono=True, bold=True)
                nw2, _ = text_size_smart(draw, name_clean, name_font)
                draw_text_smart(draw, (cx0 + (col_w - nw2) // 2, box_top + 40),
                                name_clean, name_font, INK)

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
                draw_text_smart(draw, (rx, ry), line, rest_font, DIM)

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
                        new_level: int, class_name: str = "",
                        avatar_slug: str | None = None) -> bytes | None:
    """Card menor (1080x540) pra anunciar level-up. Sem cache (evento único)."""
    try:
        pal = pick_palette(royal_id)
        W, H = CARD_SIZE, 540
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # Portrait do avatar (top-left dentro do painel) — decora sem
        # atrapalhar o conteudo centralizado existente.
        resolved = royal_avatars.resolve_slug(avatar_slug, royal_id)
        portrait = royal_avatars.load_avatar(resolved, 128)

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

        # Portrait do avatar nos dois cantos inferiores (espelho decorativo)
        if portrait is not None:
            psz = 128
            py = H - OUT_PAD - psz - 24
            # canto esquerdo
            pxL = OUT_PAD + 28
            pixel_rect(draw, (pxL - 3, py - 3,
                              pxL + psz + 3, py + psz + 3), pal["header"])
            img.paste(portrait, (pxL, py), portrait)
            # canto direito (flip horizontal — efeito espelho retro)
            mirror = portrait.transpose(Image.FLIP_LEFT_RIGHT)
            pxR = W - OUT_PAD - psz - 28
            pixel_rect(draw, (pxR - 3, py - 3,
                              pxR + psz + 3, py + psz + 3), pal["header"])
            img.paste(mirror, (pxR, py), mirror)

        # Header alerta (topo)
        alert_font = load_font(18, mono=True, bold=True)
        alert_txt = ">> LEVEL_UP.SYS // ALERTA"
        aw, ah = text_size(draw, alert_txt, alert_font)
        draw.text((center_x - aw // 2, OUT_PAD + 24),
                  alert_txt, font=alert_font, fill=HOT)

        # Nome (logo abaixo do alerta)
        name_clean = ellipsize(name, 22).upper()
        nfont = load_font(22, mono=True, bold=True)
        nw2, nh2 = text_size_smart(draw, name_clean, nfont)
        name_y = OUT_PAD + 24 + ah + 14
        draw_text_smart(draw, (center_x - nw2 // 2, name_y),
                        name_clean, nfont, INK)
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


# ---------------------------------------------------------------------
# Card da PALAVRA da hora (modo spoiler_img)
# A palavra inteira aparece em destaque no card; Telegram aplica o blur
# nativo via has_spoiler=True no send_photo. Sem cache (palavra varia).
# ---------------------------------------------------------------------
def render_palavra_spoiler_card(word: str,
                                seed: str | int | None = None) -> bytes | None:
    try:
        pal = pick_palette(seed if seed is not None else word)
        W = H = CARD_SIZE
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # Estatica de fundo (terminal corrompido)
        rng = random.Random(hash((word, seed)) & 0xFFFF)
        for _ in range(1200):
            x = rng.randrange(W); y = rng.randrange(H)
            c = rng.choice([(20, 16, 22), (28, 22, 30),
                            pal["header"], pal["footer"]])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        OUT_PAD = 36
        panel_box = (OUT_PAD, OUT_PAD, W - OUT_PAD, H - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK,
                      inner=pal["header"], thick=10)

        center_x = W // 2

        # Header
        alert_font = load_font(28, mono=True, bold=True)
        alert_txt = ">> PALAVRA.SECRETA // CLASSIFICADO"
        aw, ah = text_size(draw, alert_txt, alert_font)
        draw.text((center_x - aw // 2, OUT_PAD + 40),
                  alert_txt, font=alert_font, fill=pal["header"])

        sub_font = load_font(18, mono=True, bold=False)
        sub_txt = "// nivel de seguranca: MAX"
        sw, _ = text_size(draw, sub_txt, sub_font)
        draw.text((center_x - sw // 2, OUT_PAD + 40 + ah + 12),
                  sub_txt, font=sub_font, fill=DIM)

        # PALAVRA gigante no centro (ajusta size por comprimento)
        word_up = word.upper()
        n = len(word_up)
        if n <= 5:
            sz = 220
        elif n <= 7:
            sz = 180
        elif n <= 9:
            sz = 140
        else:
            sz = 110
        wf = load_font(sz, mono=True, bold=True)
        ww, wh = text_size(draw, word_up, wf)
        wx = center_x - ww // 2
        wy = (H - wh) // 2 - 30
        # Sombra chunky duplicada
        for dx, dy in [(10, 10), (5, 5)]:
            draw.text((wx + dx, wy + dy), word_up, font=wf, fill=BLACK)
        draw.text((wx, wy), word_up, font=wf, fill=pal["level"])

        # Tags decorativas
        tag_font = load_font(20, mono=True, bold=True)
        tags = f"[ {n} CHAR ]  [ PT-BR ]  [ {pal['name']} ]"
        tw, _ = text_size(draw, tags, tag_font)
        draw.text((center_x - tw // 2, wy + wh + 40),
                  tags, font=tag_font, fill=pal["xp"])

        # Footer
        foot_font = load_font(22, mono=True, bold=True)
        foot_txt = "!! TOQUE PRA REVELAR  ::  RESPONDA NO CHAT"
        fw, fh = text_size(draw, foot_txt, foot_font)
        draw.text((center_x - fw // 2, H - OUT_PAD - fh - 40),
                  foot_txt, font=foot_font, fill=pal["footer"])

        # Pos-processamento
        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=160)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=88, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_PALAVRA_SPOILER_RENDER_FAILED word=%r", word)
        return None


# =====================================================================
# CASORIO CARD — anuncia par formado (2 avatares + coracao pixel)
# Mesmo padrao dos demais: 1080x1080, sem cache (evento unico).
# Fontes >=16 em qualquer lugar pra evitar letra pequena.
# =====================================================================

@dataclass(frozen=True)
class CasorioPartner:
    royal_id: str
    name: str
    level: int
    avatar_slug: str | None = None


def render_casorio_card(p1: CasorioPartner, p2: CasorioPartner,
                        season_label: str = "",
                        source: str = "auto",
                        date_str: str = "") -> bytes | None:
    """Card 1080x1080 do casorio: dois avatares lado-a-lado com pixel-heart
    no meio, nomes e Royal IDs embaixo. Sem cache (evento unico). Retorna
    JPEG bytes ou None se Pillow falhar."""
    try:
        # Normaliza defensivamente — caller pode passar None em royal_id/level
        def _norm(p: CasorioPartner) -> CasorioPartner:
            return CasorioPartner(
                royal_id=p.royal_id or "RYL-????",
                name=p.name or "?",
                level=int(p.level) if p.level is not None else 1,
                avatar_slug=p.avatar_slug,
            )
        p1 = _norm(p1)
        p2 = _norm(p2)
        # Paleta determinada pelo par (estavel pro mesmo casal)
        pal_seed = "::".join(sorted([p1.royal_id, p2.royal_id]))
        pal = pick_palette(pal_seed)
        W = H = CARD_SIZE
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # Estatica de fundo (CRT corrompido)
        rng = random.Random(hash(pal_seed) & 0xFFFF)
        for _ in range(1400):
            x = rng.randrange(W); y = rng.randrange(H)
            c = rng.choice([(20, 16, 22), (16, 14, 20), (28, 22, 30),
                            (40, 20, 30)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        # Painel principal
        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, W - OUT_PAD, H - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK, inner=HOT, thick=8)

        center_x = W // 2

        # ===== Header =====
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      W - OUT_PAD - 28, OUT_PAD + 110)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), HOT)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), HOT)

        title_font = load_font(26, mono=True, bold=True)
        title = "> CASORIO.SYS  // CONFIRMADO"
        draw.text((header_box[0] + 22, header_box[1] + 24),
                  title, font=title_font, fill=HOT)

        # Source/season a direita
        if season_label:
            season_font = load_font(18, mono=True, bold=False)
            season_txt = f">> {season_label.upper()}"
            sw, _ = text_size(draw, season_txt, season_font)
            draw.text((header_box[2] - 22 - sw, header_box[1] + 30),
                      season_txt, font=season_font, fill=DIM)

        # Blinker
        pixel_rect(draw, (header_box[2] - 18, header_box[3] - 18,
                          header_box[2] - 10, header_box[3] - 10), HOT)

        # ===== Dois avatares lado a lado =====
        # Layout: avatar A em (80, 200) 340x340, avatar B em (660, 200) 340x340.
        # Sobra 240px no meio (420-660) pro pixel-heart gigante.
        AVATAR_SIZE = 340
        AY = 200
        AX_L = 80
        AX_R = W - 80 - AVATAR_SIZE  # 660

        for cx0, partner in ((AX_L, p1), (AX_R, p2)):
            # Moldura chunky preta + dourada
            pixel_rect(draw, (cx0 - 6, AY - 6,
                              cx0 + AVATAR_SIZE + 6, AY + AVATAR_SIZE + 6),
                       BLACK)
            pixel_rect(draw, (cx0 - 3, AY - 3,
                              cx0 + AVATAR_SIZE + 3, AY + AVATAR_SIZE + 3),
                       HOT)
            pixel_rect(draw, (cx0, AY, cx0 + AVATAR_SIZE, AY + AVATAR_SIZE),
                       BLACK)
            resolved = royal_avatars.resolve_slug(
                partner.avatar_slug, partner.royal_id)
            portrait = royal_avatars.load_avatar(resolved, AVATAR_SIZE)
            if portrait is not None:
                img.paste(portrait, (cx0, AY), portrait)
            else:
                # Fallback: tag textual no centro do quadro
                f_font = load_font(20, mono=True, bold=True)
                ft = "[ SEM AVATAR ]"
                fw, fh = text_size(draw, ft, f_font)
                draw.text((cx0 + (AVATAR_SIZE - fw) // 2,
                           AY + (AVATAR_SIZE - fh) // 2),
                          ft, font=f_font, fill=DIM)

        # ===== Coracao pixel gigante no meio =====
        # Heart 7px wide na malha. scale 14 → 98px wide. Centro horiz = 540.
        heart_scale = 14
        heart_w = 7 * heart_scale  # 98
        heart_x = center_x - heart_w // 2
        heart_y = AY + (AVATAR_SIZE // 2) - (heart_scale * 3)
        # Sombra preta atras pra dar profundidade
        draw_pixel_heart(draw, heart_x + 6, heart_y + 6,
                         heart_scale, color=BLACK, empty=False)
        draw_pixel_heart(draw, heart_x, heart_y,
                         heart_scale, color=HOT, empty=False)

        # Tag ">>" cima e baixo do coracao (decor)
        tag_font = load_font(28, mono=True, bold=True)
        for tag_txt, ty in (("//", heart_y - 60), ("//", heart_y + 7 * heart_scale + 20)):
            tw, _ = text_size(draw, tag_txt, tag_font)
            draw.text((center_x - tw // 2, ty),
                      tag_txt, font=tag_font, fill=DIM)

        # ===== Nomes + Royal IDs embaixo dos avatares =====
        info_y = AY + AVATAR_SIZE + 32
        for cx0, partner in ((AX_L, p1), (AX_R, p2)):
            # Nome centralizado na coluna do avatar. Limita 12 chars pra
            # caber sem virar letra pequena.
            name_clean = ellipsize(partner.name or "?", 12).upper()
            name_size = 38 if len(name_clean) <= 8 else 30 if len(name_clean) <= 11 else 26
            name_font = load_font(name_size, mono=True, bold=True)
            nw, nh = text_size_smart(draw, name_clean, name_font)
            draw_text_smart(draw, (cx0 + (AVATAR_SIZE - nw) // 2, info_y),
                            name_clean, name_font, INK)

            # Royal ID + LV embaixo
            sub_font = load_font(20, mono=True, bold=True)
            sub_txt = f"{partner.royal_id}  ·  LV{partner.level:02d}"
            sw, sh = text_size(draw, sub_txt, sub_font)
            draw.text((cx0 + (AVATAR_SIZE - sw) // 2, info_y + nh + 14),
                      sub_txt, font=sub_font, fill=GOLD_DIM)

        # ===== Footer (status do casal) =====
        foot_box = (OUT_PAD + 60, H - OUT_PAD - 140,
                    W - OUT_PAD - 60, H - OUT_PAD - 50)
        pixel_rect(draw, foot_box, PANEL)
        pixel_rect(draw, (foot_box[0], foot_box[1],
                          foot_box[2], foot_box[1] + 3), HOT)

        line1_font = load_font(22, mono=True, bold=True)
        line1 = f">> PAR FORMADO  ::  {source.upper()}"
        l1w, l1h = text_size(draw, line1, line1_font)
        draw.text((center_x - l1w // 2, foot_box[1] + 14),
                  line1, font=line1_font, fill=HOT)

        line2_font = load_font(18, mono=True, bold=False)
        line2 = "[ casados ate o divorcio cair na votacao ]"
        l2w, _ = text_size(draw, line2, line2_font)
        draw.text((center_x - l2w // 2, foot_box[1] + 14 + l1h + 12),
                  line2, font=line2_font, fill=DIM)

        if date_str:
            ds_font = load_font(16, mono=True, bold=False)
            dw, _ = text_size(draw, date_str, ds_font)
            draw.text((W - OUT_PAD - 60 - dw, H - OUT_PAD - 32),
                      date_str, font=ds_font, fill=DIM)

        # ===== Pos-processamento (mesmo padrao dos demais) =====
        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=170)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_CASORIO_CARD_RENDER_FAILED p1=%s p2=%s",
                         p1.royal_id, p2.royal_id)
        return None


# =====================================================================
# BOSS KILL CARD — anuncia boss derrotado (top 3 atacantes + loot)
# Mesmo padrao do casorio: 1080x1080, sem cache, fontes >=14, anti
# letra-pequena. Acento HOT (sangue) + RUST.
# =====================================================================

@dataclass(frozen=True)
class BossKillAttacker:
    rank: int          # 1, 2, 3
    royal_id: str
    name: str
    damage: int
    gold: int
    avatar_slug: str | None = None


@dataclass(frozen=True)
class BossKillData:
    boss_name: str
    boss_max_hp: int
    total_damage: int
    total_attackers: int
    duration_str: str   # ex.: "03:24" ou "1h 12min"
    total_gold: int
    season_label: str
    top3: tuple[BossKillAttacker, ...]  # ate 3, ordenados por rank


def render_boss_kill_card(data: BossKillData) -> bytes | None:
    """Card 1080x1080 do boss derrotado: header skull + nome do boss em
    destaque, podium 3 colunas dos top atacantes (avatar + dmg + gold),
    footer com duracao/loot total. Sem cache (evento unico)."""
    try:
        pal = pick_palette(f"BOSS::{data.boss_name}")
        W = H = CARD_SIZE
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # Estatica de fundo (CRT corrompido, com mais ferrugem)
        rng = random.Random(hash(data.boss_name) & 0xFFFF)
        for _ in range(1400):
            x = rng.randrange(W); y = rng.randrange(H)
            c = rng.choice([(20, 16, 22), (16, 14, 20), (28, 22, 30),
                            (40, 24, 18), (50, 18, 18)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, W - OUT_PAD, H - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK, inner=HOT, thick=8)

        center_x = W // 2

        # ===== Header =====
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      W - OUT_PAD - 28, OUT_PAD + 110)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), HOT)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), HOT)

        title_font = load_font(26, mono=True, bold=True)
        title = "> BOSS.SYS  // ABATIDO"
        draw.text((header_box[0] + 22, header_box[1] + 24),
                  title, font=title_font, fill=HOT)

        if data.season_label:
            season_font = load_font(18, mono=True, bold=False)
            stxt = f">> {data.season_label.upper()}"
            sw, _ = text_size(draw, stxt, season_font)
            draw.text((header_box[2] - 22 - sw, header_box[1] + 30),
                      stxt, font=season_font, fill=DIM)

        pixel_rect(draw, (header_box[2] - 18, header_box[3] - 18,
                          header_box[2] - 10, header_box[3] - 10), HOT)

        # ===== Nome do boss + HP zerado =====
        boss_name_up = ellipsize(data.boss_name or "?", 22).upper()
        bn_size = 56 if len(boss_name_up) <= 14 else 44 if len(boss_name_up) <= 18 else 36
        bn_font = load_font(bn_size, mono=True, bold=True)
        bw, bh = text_size(draw, boss_name_up, bn_font)
        bn_y = OUT_PAD + 150
        # Sombra preta
        draw.text((center_x - bw // 2 + 5, bn_y + 5),
                  boss_name_up, font=bn_font, fill=BLACK)
        draw.text((center_x - bw // 2, bn_y),
                  boss_name_up, font=bn_font, fill=RUST)

        # HP 0/MAX riscado em vermelho
        hp_font = load_font(22, mono=True, bold=True)
        hp_txt = f"HP 0 / {data.boss_max_hp}"
        hw, hh = text_size(draw, hp_txt, hp_font)
        hp_y = bn_y + bh + 18
        draw.text((center_x - hw // 2, hp_y),
                  hp_txt, font=hp_font, fill=DIM)
        # linha riscando (chunky)
        pixel_rect(draw, (center_x - hw // 2 - 4, hp_y + hh // 2 - 1,
                          center_x + hw // 2 + 4, hp_y + hh // 2 + 3), HOT)

        # ===== Stats line (duracao · atacantes · dmg total) =====
        stats_font = load_font(20, mono=True, bold=True)
        stats_txt = (
            f"[ {data.duration_str} ]  ·  "
            f"[ {data.total_attackers} CACADORES ]  ·  "
            f"[ {data.total_damage} DMG ]"
        )
        sw2, sh2 = text_size(draw, stats_txt, stats_font)
        # Reduz se nao couber
        if sw2 > W - OUT_PAD * 2 - 60:
            stats_font = load_font(16, mono=True, bold=True)
            sw2, sh2 = text_size(draw, stats_txt, stats_font)
        stats_y = hp_y + hh + 18
        draw.text((center_x - sw2 // 2, stats_y),
                  stats_txt, font=stats_font, fill=CYAN)

        # ===== Podium top 3 =====
        # Layout 3 colunas iguais (com gaps), avatares 180px,
        # nome + dmg + gold embaixo. Ordem visual: 2-1-3 (1o central).
        podium_y_top = stats_y + sh2 + 50
        col_gap = 20
        col_w = (W - OUT_PAD * 2 - 80 - col_gap * 2) // 3  # ~284
        base_x = OUT_PAD + 40
        medal_colors = {1: GOLD, 2: (180, 180, 180), 3: (180, 100, 60)}
        medal_label = {1: "1ST", 2: "2ND", 3: "3RD"}
        top3 = {a.rank: a for a in data.top3}
        order = [(2, 0), (1, 1), (3, 2)]  # rank -> coluna

        AVATAR = 180
        for rank, col in order:
            cx0 = base_x + col * (col_w + col_gap)
            cx1 = cx0 + col_w
            # Painel da coluna
            col_top = podium_y_top
            col_bot = col_top + 380
            pixel_rect(draw, (cx0, col_top, cx1, col_bot), PANEL)
            pixel_rect(draw, (cx0, col_top, cx1, col_top + 4),
                       medal_colors[rank])

            a = top3.get(rank)
            if a is None:
                # Vago — placeholder
                vac_font = load_font(20, mono=True, bold=True)
                vt = "[ VAGO ]"
                vw, vh = text_size(draw, vt, vac_font)
                draw.text((cx0 + (col_w - vw) // 2, col_top + 160),
                          vt, font=vac_font, fill=DIM)
                continue

            # Medalha label topo
            m_font = load_font(18, mono=True, bold=True)
            mt = medal_label[rank]
            mw, mh = text_size(draw, mt, m_font)
            draw.text((cx0 + (col_w - mw) // 2, col_top + 14),
                      mt, font=m_font, fill=medal_colors[rank])

            # Avatar
            resolved = royal_avatars.resolve_slug(a.avatar_slug, a.royal_id)
            portrait = royal_avatars.load_avatar(resolved, AVATAR)
            ax = cx0 + (col_w - AVATAR) // 2
            ay = col_top + 40
            pixel_rect(draw, (ax - 3, ay - 3,
                              ax + AVATAR + 3, ay + AVATAR + 3), BLACK)
            pixel_rect(draw, (ax - 1, ay - 1,
                              ax + AVATAR + 1, ay + AVATAR + 1),
                       medal_colors[rank])
            if portrait is not None:
                img.paste(portrait, (ax, ay), portrait)
            else:
                f_font = load_font(16, mono=True, bold=True)
                ft = "[ ? ]"
                fw, fh = text_size(draw, ft, f_font)
                draw.text((ax + (AVATAR - fw) // 2, ay + (AVATAR - fh) // 2),
                          ft, font=f_font, fill=DIM)

            # Nome (ellipsize 10 — coluna estreita)
            name_clean = ellipsize(a.name or "?", 10).upper()
            n_size = 22 if len(name_clean) <= 7 else 18
            name_font = load_font(n_size, mono=True, bold=True)
            nw, nh = text_size_smart(draw, name_clean, name_font)
            name_y = ay + AVATAR + 16
            draw_text_smart(draw, (cx0 + (col_w - nw) // 2, name_y),
                            name_clean, name_font, INK)

            # Royal ID
            id_font = load_font(16, mono=True, bold=False)
            idw, idh = text_size(draw, a.royal_id, id_font)
            draw.text((cx0 + (col_w - idw) // 2, name_y + nh + 8),
                      a.royal_id, font=id_font, fill=DIM)

            # Dmg destacado
            dmg_font = load_font(22, mono=True, bold=True)
            dmg_txt = f"{a.damage} DMG"
            dw, dh = text_size(draw, dmg_txt, dmg_font)
            dmg_y = name_y + nh + 8 + idh + 10
            draw.text((cx0 + (col_w - dw) // 2, dmg_y),
                      dmg_txt, font=dmg_font, fill=HOT)

            # Gold ganho (logo abaixo do dano)
            if a.gold > 0:
                gold_font = load_font(18, mono=True, bold=True)
                gold_txt = f"+{a.gold} GOLD"
                gw, _ = text_size(draw, gold_txt, gold_font)
                draw.text((cx0 + (col_w - gw) // 2, dmg_y + dh + 6),
                          gold_txt, font=gold_font, fill=GOLD)

        # ===== Footer (loot total) =====
        foot_box = (OUT_PAD + 60, H - OUT_PAD - 120,
                    W - OUT_PAD - 60, H - OUT_PAD - 50)
        pixel_rect(draw, foot_box, PANEL)
        pixel_rect(draw, (foot_box[0], foot_box[1],
                          foot_box[2], foot_box[1] + 3), GOLD)

        loot_font = load_font(24, mono=True, bold=True)
        loot_txt = f">> LOOT TOTAL  ::  {format_br(data.total_gold)}  GOLD"
        lw, lh = text_size(draw, loot_txt, loot_font)
        draw.text((center_x - lw // 2, foot_box[1] + 18),
                  loot_txt, font=loot_font, fill=GOLD)

        sub_font = load_font(16, mono=True, bold=False)
        sub_txt = "[ recompensa distribuida proporcional ao dano ]"
        sw3, _ = text_size(draw, sub_txt, sub_font)
        draw.text((center_x - sw3 // 2, foot_box[1] + 18 + lh + 6),
                  sub_txt, font=sub_font, fill=DIM)

        # Pos-processamento
        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=180)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=20)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_BOSS_KILL_CARD_RENDER_FAILED boss=%r",
                         data.boss_name)
        return None


# =====================================================================
# IDENTITY CARD — minimal, fixed, cached by file_id no DB.
# So tem avatar + ROY#ID + nome. SEM level/XP/atributos.
# Regenerado APENAS quando nome ou avatar muda (sweep diario).
# =====================================================================

@dataclass(frozen=True)
class IdentityCardData:
    royal_id: str
    name: str
    avatar_slug: str | None = None
    username: str | None = None  # @handle do Telegram (sem @), opcional


def render_identity_card(data: IdentityCardData) -> bytes | None:
    """Card 1080x1080 de identidade fixa do jogador. Sem dados dinamicos
    (level/xp/gold) — so identifica que este user eh ESTE personagem do
    Reino. Retorna JPEG bytes ou None."""
    try:
        royal_id = (data.royal_id or "RYL-????").upper()
        name = (data.name or "?").strip() or "?"

        pal = pick_palette(royal_id)
        W = H = CARD_SIZE
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # Estatica de fundo
        rng = random.Random(hash(royal_id) & 0xFFFF)
        for _ in range(1200):
            x = rng.randrange(W); y = rng.randrange(H)
            c = rng.choice([(18, 16, 22), (14, 12, 20), (24, 20, 28)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        # Painel principal
        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, W - OUT_PAD, H - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        accent = pal.get("header", ACID)
        chunky_border(draw, panel_box, outer=BLACK, inner=accent, thick=8)

        # ===== Header =====
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      W - OUT_PAD - 28, OUT_PAD + 110)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), accent)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), accent)

        title_font = load_font(26, mono=True, bold=True)
        title = "> ROYAL.ID  // CADASTRADO"
        draw.text((header_box[0] + 22, header_box[1] + 24),
                  title, font=title_font, fill=accent)

        # Game tag a direita
        tag_font = load_font(18, mono=True, bold=False)
        tag_txt = ">> RPG.REINO"
        tw, _ = text_size(draw, tag_txt, tag_font)
        draw.text((header_box[2] - 22 - tw, header_box[1] + 30),
                  tag_txt, font=tag_font, fill=DIM)

        # Blinker
        pixel_rect(draw, (header_box[2] - 18, header_box[3] - 18,
                          header_box[2] - 10, header_box[3] - 10), accent)

        # ===== Avatar GIGANTE centrado =====
        AVATAR_SIZE = 560
        AX = (W - AVATAR_SIZE) // 2
        AY = 200
        # Moldura tripla preta + accent + preta
        pixel_rect(draw, (AX - 8, AY - 8,
                          AX + AVATAR_SIZE + 8, AY + AVATAR_SIZE + 8),
                   BLACK)
        pixel_rect(draw, (AX - 4, AY - 4,
                          AX + AVATAR_SIZE + 4, AY + AVATAR_SIZE + 4),
                   accent)
        pixel_rect(draw, (AX, AY, AX + AVATAR_SIZE, AY + AVATAR_SIZE),
                   BLACK)

        resolved = royal_avatars.resolve_slug(data.avatar_slug, royal_id)
        portrait = royal_avatars.load_avatar(resolved, AVATAR_SIZE)
        if portrait is not None:
            img.paste(portrait, (AX, AY),
                      portrait if portrait.mode == "RGBA" else None)
        else:
            # Fallback: sigilo procedural grande
            sigil = procedural_sigil(royal_id, AVATAR_SIZE, palette=pal)
            img.paste(sigil, (AX, AY))

        # ===== Royal ID =====
        id_y = AY + AVATAR_SIZE + 32
        id_font = load_font(32, mono=True, bold=True)
        id_txt = f"ROY#{royal_id.replace('RYL-', '').replace('ROY-', '')}"
        iw, ih = text_size(draw, id_txt, id_font)
        # Caixa de ID
        id_pad_x = 28
        id_pad_y = 12
        id_box_x0 = (W - iw) // 2 - id_pad_x
        id_box_y0 = id_y - id_pad_y
        id_box_x1 = (W + iw) // 2 + id_pad_x
        id_box_y1 = id_y + ih + id_pad_y
        pixel_rect(draw, (id_box_x0, id_box_y0, id_box_x1, id_box_y1), PANEL)
        pixel_rect(draw, (id_box_x0, id_box_y0,
                          id_box_x1, id_box_y0 + 3), accent)
        pixel_rect(draw, (id_box_x0, id_box_y1 - 3,
                          id_box_x1, id_box_y1), accent)
        draw.text(((W - iw) // 2, id_y), id_txt, font=id_font, fill=accent)

        # ===== Handle (@username) do jogador =====
        # Mostra APENAS @username — handles do Telegram sao ASCII puro,
        # entao a fonte mono renderiza 100%. Se o user nao tem @username
        # configurado, NAO renderiza nada (sem fallback pro display_name,
        # que pode conter emoji/glifos que viram tofu). ROY#ID ja identifica.
        handle_raw = (data.username or "").strip().lstrip("@")
        if handle_raw:
            display_handle = "@" + ellipsize(handle_raw, 20)
            for fsize in (38, 32, 26):
                name_font = load_font(fsize, mono=True, bold=True)
                nw, nh = text_size(draw, display_handle, name_font)
                if nw <= W - 120:
                    break
            name_y = id_box_y1 + 22
            draw.text(((W - nw) // 2 + 3, name_y + 3),
                      display_handle, font=name_font, fill=BLACK)
            draw.text(((W - nw) // 2, name_y),
                      display_handle, font=name_font, fill=INK)

        # ===== Footer =====
        footer_font = load_font(18, mono=True, bold=False)
        footer_txt = "[ JOGADOR DO REINO  //  RPG ROYAL PARA GEEKS ]"
        fw, fh = text_size(draw, footer_txt, footer_font)
        if fw > W - 80:
            footer_txt = "[ JOGADOR DO REINO ]"
            fw, fh = text_size(draw, footer_txt, footer_font)
        footer_y = H - OUT_PAD - 60
        draw.text(((W - fw) // 2, footer_y),
                  footer_txt, font=footer_font, fill=DIM)

        # Pos-processamento
        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=160)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_IDENTITY_CARD_RENDER_FAILED rid=%r",
                         data.royal_id)
        return None


# =====================================================================
# CLASSE CARD — exibido quando user escolhe classe pela 1a vez.
# Pequeno, sem cache (evento unico). Avatar + emoji classe + nome + bonus.
# =====================================================================

@dataclass(frozen=True)
class ClasseCardData:
    royal_id: str
    name: str
    class_emoji: str
    class_name: str
    class_bonus: str
    avatar_slug: str | None = None
    season_label: str = ""


def render_classe_card(data: ClasseCardData) -> bytes | None:
    """Card 1080x1080 de selecao de classe. Sem cache."""
    try:
        royal_id = (data.royal_id or "RYL-????").upper()
        name = ellipsize(data.name or "?", 28)
        pal = pick_palette(royal_id)
        W = H = CARD_SIZE
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        rng = random.Random(hash(royal_id + data.class_name) & 0xFFFF)
        for _ in range(1300):
            x = rng.randrange(W); y = rng.randrange(H)
            c = rng.choice([(20, 18, 26), (16, 14, 22), (28, 22, 32)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, W - OUT_PAD, H - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        accent = pal.get("header", CYAN)
        chunky_border(draw, panel_box, outer=BLACK, inner=accent, thick=8)

        # Header
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      W - OUT_PAD - 28, OUT_PAD + 110)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), accent)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), accent)
        title_font = load_font(26, mono=True, bold=True)
        draw.text((header_box[0] + 22, header_box[1] + 24),
                  "> CLASSE.SYS  // SELECIONADA",
                  font=title_font, fill=accent)
        if data.season_label:
            season_font = load_font(18, mono=True, bold=False)
            stxt = f">> {data.season_label.upper()}"
            sw, _ = text_size(draw, stxt, season_font)
            draw.text((header_box[2] - 22 - sw, header_box[1] + 30),
                      stxt, font=season_font, fill=DIM)
        pixel_rect(draw, (header_box[2] - 18, header_box[3] - 18,
                          header_box[2] - 10, header_box[3] - 10), accent)

        # Avatar a esquerda
        AVATAR_SIZE = 360
        AX = 80
        AY = 220
        pixel_rect(draw, (AX - 6, AY - 6,
                          AX + AVATAR_SIZE + 6, AY + AVATAR_SIZE + 6), BLACK)
        pixel_rect(draw, (AX - 3, AY - 3,
                          AX + AVATAR_SIZE + 3, AY + AVATAR_SIZE + 3), accent)
        pixel_rect(draw, (AX, AY, AX + AVATAR_SIZE, AY + AVATAR_SIZE), BLACK)
        resolved = royal_avatars.resolve_slug(data.avatar_slug, royal_id)
        portrait = royal_avatars.load_avatar(resolved, AVATAR_SIZE)
        if portrait is not None:
            img.paste(portrait, (AX, AY),
                      portrait if portrait.mode == "RGBA" else None)
        else:
            sigil = procedural_sigil(royal_id, AVATAR_SIZE, palette=pal)
            img.paste(sigil, (AX, AY))

        # Nome embaixo do avatar
        nm_font = load_font(20, mono=False, bold=True)
        nw, nh = text_size_smart(draw, name, nm_font)
        draw_text_smart(draw, (AX + (AVATAR_SIZE - nw) // 2, AY + AVATAR_SIZE + 18),
                        name, nm_font, INK)
        id_font = load_font(18, mono=True, bold=True)
        id_txt = f"ROY#{royal_id.replace('RYL-', '').replace('ROY-', '')}"
        iw, _ = text_size(draw, id_txt, id_font)
        draw.text((AX + (AVATAR_SIZE - iw) // 2, AY + AVATAR_SIZE + 50),
                  id_txt, font=id_font, fill=accent)

        # Bloco da classe a direita
        BX = AX + AVATAR_SIZE + 60
        BY = AY
        BW = W - BX - 80
        BH = AVATAR_SIZE
        pixel_rect(draw, (BX, BY, BX + BW, BY + BH), PANEL)
        chunky_border(draw, (BX, BY, BX + BW, BY + BH),
                      outer=BLACK, inner=accent, thick=6)

        # Emoji da classe gigante
        emoji_font = load_font(120, mono=False, bold=True)
        ew, eh = text_size(draw, data.class_emoji, emoji_font)
        draw.text((BX + (BW - ew) // 2, BY + 30),
                  data.class_emoji, font=emoji_font, fill=INK)

        # Nome da classe
        cname_font = load_font(32, mono=True, bold=True)
        cw, ch = text_size(draw, data.class_name.upper(), cname_font)
        if cw > BW - 40:
            cname_font = load_font(26, mono=True, bold=True)
            cw, ch = text_size(draw, data.class_name.upper(), cname_font)
        cny = BY + 30 + eh + 30
        draw.text((BX + (BW - cw) // 2 + 2, cny + 2),
                  data.class_name.upper(), font=cname_font, fill=BLACK)
        draw.text((BX + (BW - cw) // 2, cny),
                  data.class_name.upper(), font=cname_font, fill=accent)

        # Bonus
        bn_font = load_font(20, mono=True, bold=False)
        bonus = data.class_bonus
        # quebra texto se grande
        max_chars = 22
        if len(bonus) > max_chars:
            bonus = ellipsize(bonus, max_chars * 2)
        bw, bh = text_size(draw, bonus, bn_font)
        if bw > BW - 30:
            bonus = ellipsize(bonus, max_chars)
            bw, bh = text_size(draw, bonus, bn_font)
        by = cny + ch + 24
        draw.text((BX + (BW - bw) // 2, by),
                  bonus, font=bn_font, fill=INK)

        # Footer
        footer_font = load_font(18, mono=True, bold=False)
        ftxt = "[ IDENTIDADE FIXADA  //  RPG.REINO ]"
        fw, _ = text_size(draw, ftxt, footer_font)
        draw.text(((W - fw) // 2, H - OUT_PAD - 60),
                  ftxt, font=footer_font, fill=DIM)

        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=160)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_CLASSE_CARD_RENDER_FAILED rid=%r",
                         data.royal_id)
        return None


# =====================================================================
# LOJA DROP CARD — exibido em compra de item raro/lendario.
# Item gigante centrado + nome + price + rarity stamp.
# =====================================================================

@dataclass(frozen=True)
class LojaDropData:
    royal_id: str
    buyer_name: str
    item_emoji: str
    item_name: str
    item_desc: str
    price: int
    rarity: str  # "RARO" | "LENDARIO"


def render_loja_drop_card(data: LojaDropData) -> bytes | None:
    """Card 1080x1080 de drop de item. Sem cache."""
    try:
        royal_id = (data.royal_id or "RYL-????").upper()
        buyer = ellipsize(data.buyer_name or "?", 22)
        rarity = (data.rarity or "RARO").upper()
        # Paleta por rarity
        if rarity == "LENDARIO":
            accent = (180, 90, 220)   # purple
            sub_accent = GOLD
        else:
            accent = GOLD
            sub_accent = AMBER

        W = H = CARD_SIZE
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        rng = random.Random(hash(royal_id + data.item_name) & 0xFFFF)
        for _ in range(1400):
            x = rng.randrange(W); y = rng.randrange(H)
            c = rng.choice([(22, 18, 22), (16, 14, 18), (30, 24, 28),
                            (40, 28, 18)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, W - OUT_PAD, H - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK, inner=accent, thick=8)

        # Header
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      W - OUT_PAD - 28, OUT_PAD + 110)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), accent)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), accent)
        title_font = load_font(26, mono=True, bold=True)
        draw.text((header_box[0] + 22, header_box[1] + 24),
                  "> LOJA.SYS  // AQUISICAO",
                  font=title_font, fill=accent)
        # Rarity tag a direita
        rar_font = load_font(20, mono=True, bold=True)
        rar_txt = f"!! {rarity}"
        rw, _ = text_size(draw, rar_txt, rar_font)
        draw.text((header_box[2] - 22 - rw, header_box[1] + 28),
                  rar_txt, font=rar_font, fill=sub_accent)
        pixel_rect(draw, (header_box[2] - 18, header_box[3] - 18,
                          header_box[2] - 10, header_box[3] - 10), accent)

        # Caixa do item centrada
        ITEM_BOX = 480
        IX = (W - ITEM_BOX) // 2
        IY = 200
        pixel_rect(draw, (IX, IY, IX + ITEM_BOX, IY + ITEM_BOX), PANEL)
        chunky_border(draw, (IX, IY, IX + ITEM_BOX, IY + ITEM_BOX),
                      outer=BLACK, inner=accent, thick=8)

        # Emoji do item GIGANTE (centro do quadrado)
        em_font = load_font(280, mono=False, bold=True)
        ew, eh = text_size(draw, data.item_emoji, em_font)
        # textbbox tem offset weird com emojis; compensa
        ex = IX + (ITEM_BOX - ew) // 2
        ey = IY + (ITEM_BOX - eh) // 2 - 30
        # sombra
        draw.text((ex + 4, ey + 4), data.item_emoji,
                  font=em_font, fill=BLACK)
        draw.text((ex, ey), data.item_emoji, font=em_font, fill=INK)

        # Nome do item
        nm_font = load_font(36, mono=True, bold=True)
        nm = data.item_name.upper()
        nw, nh = text_size(draw, nm, nm_font)
        if nw > W - 120:
            nm_font = load_font(28, mono=True, bold=True)
            nw, nh = text_size(draw, nm, nm_font)
        nm_y = IY + ITEM_BOX + 30
        draw.text(((W - nw) // 2 + 3, nm_y + 3),
                  nm, font=nm_font, fill=BLACK)
        draw.text(((W - nw) // 2, nm_y),
                  nm, font=nm_font, fill=accent)

        # Price riscado (PAGO)
        pr_font = load_font(22, mono=True, bold=True)
        price_txt = f"-{format_br(data.price)} florins"
        pw, ph = text_size(draw, price_txt, pr_font)
        pr_y = nm_y + nh + 22
        draw.text(((W - pw) // 2, pr_y),
                  price_txt, font=pr_font, fill=HOT)
        # risco em cima do preco
        pixel_rect(draw, ((W - pw) // 2 - 6, pr_y + ph // 2,
                          (W + pw) // 2 + 6, pr_y + ph // 2 + 3),
                   HOT)

        # Buyer line
        buy_font = load_font(18, mono=True, bold=False)
        rid_short = royal_id.replace('RYL-', '').replace('ROY-', '')
        buy_txt = f">> ROY#{rid_short}  ::  {buyer}"
        bw_w, _ = text_size_smart(draw, buy_txt, buy_font)
        if bw_w > W - 100:
            buy_txt = f">> ROY#{rid_short}"
            bw_w, _ = text_size_smart(draw, buy_txt, buy_font)
        draw_text_smart(draw, ((W - bw_w) // 2, pr_y + ph + 24),
                        buy_txt, buy_font, DIM)

        # Footer
        footer_font = load_font(16, mono=True, bold=False)
        ftxt = "[ INVENTARIO ATUALIZADO ]"
        fw, _ = text_size(draw, ftxt, footer_font)
        draw.text(((W - fw) // 2, H - OUT_PAD - 50),
                  ftxt, font=footer_font, fill=sub_accent)

        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=170)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90, optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("ROYAL_LOJA_DROP_CARD_RENDER_FAILED rid=%r item=%r",
                         data.royal_id, data.item_name)
        return None


# =====================================================================
# CASORIOS RANKING CARD — top-10 casais do grupo (pódio 3 + lista 4-10)
# =====================================================================

@dataclass(frozen=True)
class CouplePodiumEntry:
    rank: int
    p1_royal_id: str
    p1_name: str
    p1_avatar_slug: str | None
    p2_royal_id: str
    p2_name: str
    p2_avatar_slug: str | None
    total: int


def render_casorios_ranking_card(
        season_label: str,
        entries: tuple[CouplePodiumEntry, ...]) -> bytes | None:
    """Pódio top-3 casais (dois retratos + heart no meio) + lista 4-10."""
    if not entries:
        return None
    cache_key = ("casorios_rank", season_label,
                 tuple((e.rank,
                        e.p1_royal_id, e.p1_name, e.p1_avatar_slug,
                        e.p2_royal_id, e.p2_name, e.p2_avatar_slug,
                        e.total)
                       for e in entries[:10]))
    cached = cache_get(cache_key)
    if cached:
        return cached
    try:
        pal = pick_palette(season_label)
        img = Image.new("RGB", (CARD_SIZE, CARD_SIZE), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # estática de fundo
        rng = random.Random(hash(("couples", season_label)) & 0xFFFF)
        for _ in range(1200):
            x = rng.randrange(CARD_SIZE)
            y = rng.randrange(CARD_SIZE)
            c = rng.choice([(20, 16, 22), (16, 14, 20), (28, 22, 30)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD,
                     CARD_SIZE - OUT_PAD, CARD_SIZE - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK, inner=HOT, thick=8)

        # Header
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      CARD_SIZE - OUT_PAD - 28, OUT_PAD + 110)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), HOT)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), HOT)
        title_font = load_font(28, mono=True, bold=True)
        season_font = load_font(20, mono=True, bold=False)
        draw.text((header_box[0] + 22, header_box[1] + 22),
                  "ROYAL.COUPLES.SYS", font=title_font, fill=pal["header"])
        season_txt = f">> {season_label.upper()}"
        sw, _ = text_size(draw, season_txt, season_font)
        draw.text((header_box[2] - 22 - sw, header_box[1] + 28),
                  season_txt, font=season_font, fill=DIM)
        # heart cursor no canto
        draw_pixel_heart(draw, header_box[2] - 18,
                         header_box[1] + 28, 2, color=HOT)

        # Pódio: posições 2-1-3 (1º maior, centro)
        medal_colors = {1: GOLD, 2: (180, 180, 180), 3: (180, 100, 60)}
        order = [(2, 0), (1, 1), (3, 2)]
        top3 = {e.rank: e for e in entries if e.rank <= 3}

        col_w = (CARD_SIZE - OUT_PAD * 2 - 80) // 3
        col_gap = 20
        base_x = OUT_PAD + 40
        podium_y = OUT_PAD + 200

        for rank, col in order:
            e = top3.get(rank)
            cx0 = base_x + col * (col_w + col_gap)
            cx1 = cx0 + col_w
            heights = {1: 320, 2: 240, 3: 200}
            ph = heights[rank]
            podium_top = podium_y + (320 - ph) + 280
            podium_bot = OUT_PAD + 28 + 800

            # bloco pódio
            pixel_rect(draw, (cx0, podium_top, cx1, podium_bot), PANEL)
            pixel_rect(draw, (cx0, podium_top, cx1, podium_top + 6),
                       medal_colors[rank])
            num_font = load_font(120, mono=True, bold=True)
            num = str(rank)
            nw, nh = text_size(draw, num, num_font)
            draw.text((cx0 + (col_w - nw) // 2, podium_top + 20),
                      num, font=num_font, fill=medal_colors[rank])

            # caixa do casal acima do pódio
            box_top = podium_top - 200
            pixel_rect(draw, (cx0, box_top, cx1, podium_top - 12), PANEL)
            pixel_rect(draw, (cx0, box_top, cx1, box_top + 4),
                       medal_colors[rank])
            if e:
                # Dois retratos lado-a-lado com heart no meio (escala c/ rank)
                port_size = {1: 96, 2: 84, 3: 76}[rank]
                heart_scale = {1: 6, 2: 5, 3: 4}[rank]
                heart_w = 7 * heart_scale
                total_w = port_size * 2 + heart_w + 24
                start_x = cx0 + (col_w - total_w) // 2
                ports_y = box_top - port_size - 16

                for idx, (rid, slug) in enumerate(
                        [(e.p1_royal_id, e.p1_avatar_slug),
                         (e.p2_royal_id, e.p2_avatar_slug)]):
                    resolved = royal_avatars.resolve_slug(slug, rid)
                    portrait = royal_avatars.load_avatar(resolved, port_size)
                    pxc = start_x + idx * (port_size + heart_w + 24)
                    pyc = ports_y
                    pixel_rect(draw,
                               (pxc - 3, pyc - 3,
                                pxc + port_size + 3, pyc + port_size + 3),
                               BLACK)
                    pixel_rect(draw,
                               (pxc - 1, pyc - 1,
                                pxc + port_size + 1, pyc + port_size + 1),
                               medal_colors[rank])
                    if portrait is not None:
                        img.paste(portrait, (pxc, pyc), portrait)
                # heart no meio
                hx = start_x + port_size + 12
                hy = ports_y + (port_size - 6 * heart_scale) // 2
                draw_pixel_heart(draw, hx, hy, heart_scale, color=HOT)

                medal_txt = {1: "1ST", 2: "2ND", 3: "3RD"}[rank]
                mt_font = load_font(16, mono=True, bold=True)
                mw, _ = text_size(draw, medal_txt, mt_font)
                draw.text((cx0 + (col_w - mw) // 2, box_top + 12),
                          medal_txt, font=mt_font, fill=medal_colors[rank])

                # par "Nome1 ❤ Nome2" — ellipsize curto
                n1 = ellipsize(e.p1_name, 8).upper()
                n2 = ellipsize(e.p2_name, 8).upper()
                pair_txt = f"{n1} ♥ {n2}"
                ns = 16 if len(pair_txt) <= 20 else 14
                name_font = load_font(ns, mono=True, bold=True)
                nw2, _ = text_size_smart(draw, pair_txt, name_font)
                if nw2 > col_w - 12:
                    pair_txt = f"{n1[:6]} ♥ {n2[:6]}"
                    nw2, _ = text_size_smart(draw, pair_txt, name_font)
                draw_text_smart(draw,
                                (cx0 + (col_w - nw2) // 2, box_top + 40),
                                pair_txt, name_font, INK)

                # IDs em DIM
                id_font = load_font(12, mono=True, bold=False)
                ids = f"{e.p1_royal_id} + {e.p2_royal_id}"
                idw, _ = text_size(draw, ids, id_font)
                if idw > col_w - 12:
                    ids = f"{e.p1_royal_id}"
                    idw, _ = text_size(draw, ids, id_font)
                draw.text((cx0 + (col_w - idw) // 2, box_top + 72),
                          ids, font=id_font, fill=DIM)

                # Total casórios — destaque
                tot_font = load_font(22, mono=True, bold=True)
                tot_txt = f"{format_br(e.total)}x ♥"
                tw, _ = text_size_smart(draw, tot_txt, tot_font)
                draw_text_smart(draw,
                                (cx0 + (col_w - tw) // 2, box_top + 100),
                                tot_txt, tot_font, pal["xp"])

                lv_font = load_font(12, mono=True, bold=False)
                lv_txt = "CASORIOS"
                lvw, _ = text_size(draw, lv_txt, lv_font)
                draw.text((cx0 + (col_w - lvw) // 2, box_top + 130),
                          lv_txt, font=lv_font, fill=DIM)
            else:
                empty_font = load_font(16, mono=True, bold=False)
                draw.text((cx0 + 20, box_top + 60),
                          "[ VAGO ]", font=empty_font, fill=DIM)

        # Lista 4-10 no rodapé (2 colunas)
        rest = [e for e in entries if e.rank > 3][:7]
        if rest:
            rest_y = OUT_PAD + 28 + 820
            rest_font = load_font(15, mono=True, bold=False)
            for i, e in enumerate(rest):
                col = i % 2
                row = i // 2
                rx = OUT_PAD + 40 + col * (
                    (CARD_SIZE - OUT_PAD * 2 - 80) // 2 + 20)
                ry = rest_y + row * 22
                n1 = ellipsize(e.p1_name, 8)
                n2 = ellipsize(e.p2_name, 8)
                line = f" {e.rank:02d}. {n1}♥{n2} {format_br(e.total)}x"
                draw_text_smart(draw, (rx, ry), line, rest_font, DIM)

        # Footer
        foot_font = load_font(12, mono=True, bold=False)
        foot_left = f"> CASAIS={len(entries)}"
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
        logger.exception("ROYAL_CASORIOS_RANKING_CARD_RENDER_FAILED")
        return None


# =====================================================================
# MEUS CASORIOS CARD — perfil pessoal: avatar central + total + top 5
# =====================================================================

@dataclass(frozen=True)
class PartnerMini:
    royal_id: str
    name: str
    avatar_slug: str | None
    total: int


@dataclass(frozen=True)
class MeusCasoriosData:
    self_royal_id: str
    self_name: str
    self_avatar_slug: str | None
    total_casorios: int
    top_partners: tuple[PartnerMini, ...]   # ate 5
    season_label: str = ""


def render_meuscasorios_card(data: MeusCasoriosData) -> bytes | None:
    """Card pessoal de casórios: avatar central + número gigante de total
    + grade horizontal com 5 mini-portraits dos top parceiros."""
    cache_key = ("meus_casorios", data.self_royal_id,
                 data.self_name, data.self_avatar_slug,
                 data.total_casorios,
                 tuple((p.royal_id, p.name, p.avatar_slug, p.total)
                       for p in data.top_partners),
                 data.season_label)
    cached = cache_get(cache_key)
    if cached:
        return cached
    try:
        pal = pick_palette(data.self_royal_id)
        W = H = CARD_SIZE
        img = Image.new("RGB", (W, H), BG_DEEP)
        draw = ImageDraw.Draw(img)

        # estática de fundo
        rng = random.Random(hash(data.self_royal_id) & 0xFFFF)
        for _ in range(1100):
            x = rng.randrange(W)
            y = rng.randrange(H)
            c = rng.choice([(20, 16, 22), (16, 14, 20), (28, 22, 30)])
            pixel_rect(draw, (x, y, x + 3, y + 3), c)

        OUT_PAD = 28
        panel_box = (OUT_PAD, OUT_PAD, W - OUT_PAD, H - OUT_PAD)
        pixel_rect(draw, panel_box, BG)
        chunky_border(draw, panel_box, outer=BLACK, inner=HOT, thick=8)

        # Header
        header_box = (OUT_PAD + 28, OUT_PAD + 28,
                      W - OUT_PAD - 28, OUT_PAD + 110)
        pixel_rect(draw, header_box, PANEL)
        pixel_rect(draw, (header_box[0], header_box[1],
                          header_box[2], header_box[1] + 4), HOT)
        pixel_rect(draw, (header_box[0], header_box[3] - 4,
                          header_box[2], header_box[3]), HOT)
        title_font = load_font(28, mono=True, bold=True)
        draw.text((header_box[0] + 22, header_box[1] + 22),
                  "MEUS.CASORIOS.SYS", font=title_font, fill=pal["header"])
        if data.season_label:
            season_font = load_font(20, mono=True, bold=False)
            season_txt = f">> {data.season_label.upper()}"
            sw, _ = text_size(draw, season_txt, season_font)
            draw.text((header_box[2] - 22 - sw, header_box[1] + 28),
                      season_txt, font=season_font, fill=DIM)
        draw_pixel_heart(draw, header_box[2] - 18,
                         header_box[1] + 28, 2, color=HOT)

        # Avatar central GRANDE (220px)
        port_size = 220
        port_x = (W - port_size) // 2
        port_y = OUT_PAD + 160
        resolved = royal_avatars.resolve_slug(
            data.self_avatar_slug, data.self_royal_id)
        portrait = royal_avatars.load_avatar(resolved, port_size)
        # Moldura
        pixel_rect(draw, (port_x - 6, port_y - 6,
                          port_x + port_size + 6, port_y + port_size + 6),
                   BLACK)
        pixel_rect(draw, (port_x - 3, port_y - 3,
                          port_x + port_size + 3, port_y + port_size + 3),
                   pal["header"])
        if portrait is not None:
            img.paste(portrait, (port_x, port_y), portrait)

        # Nome + royal_id abaixo do avatar
        name_font = load_font(24, mono=True, bold=True)
        name = ellipsize(data.self_name, 22).upper()
        nw, _ = text_size_smart(draw, name, name_font)
        draw_text_smart(draw, ((W - nw) // 2, port_y + port_size + 16),
                        name, name_font, INK)
        id_font = load_font(16, mono=True, bold=False)
        idw, _ = text_size(draw, data.self_royal_id, id_font)
        draw.text(((W - idw) // 2, port_y + port_size + 48),
                  data.self_royal_id, font=id_font, fill=DIM)

        # Painel de TOTAL casórios — grande e centrado
        tot_panel_y = port_y + port_size + 86
        tot_panel = (OUT_PAD + 80, tot_panel_y,
                     W - OUT_PAD - 80, tot_panel_y + 110)
        pixel_rect(draw, tot_panel, PANEL)
        pixel_rect(draw, (tot_panel[0], tot_panel[1],
                          tot_panel[2], tot_panel[1] + 4), HOT)
        pixel_rect(draw, (tot_panel[0], tot_panel[3] - 4,
                          tot_panel[2], tot_panel[3]), HOT)
        label_font = load_font(14, mono=True, bold=False)
        draw.text((tot_panel[0] + 18, tot_panel[1] + 12),
                  ">> CASORIOS TOTAIS", font=label_font, fill=DIM)
        big_font = load_font(56, mono=True, bold=True)
        big_txt = format_br(data.total_casorios)
        bw, bh = text_size_smart(draw, big_txt, big_font)
        draw_text_smart(draw,
                        (tot_panel[0] + (tot_panel[2] - tot_panel[0] - bw) // 2,
                         tot_panel[1] + 36),
                        big_txt, big_font, pal["xp"])

        # Painel TOP PARES — 5 mini-portraits em fila
        top_y = tot_panel[3] + 24
        top_panel = (OUT_PAD + 28, top_y,
                     W - OUT_PAD - 28, top_y + 220)
        pixel_rect(draw, top_panel, PANEL)
        pixel_rect(draw, (top_panel[0], top_panel[1],
                          top_panel[2], top_panel[1] + 4), pal["footer"])
        head_font = load_font(16, mono=True, bold=True)
        draw.text((top_panel[0] + 18, top_panel[1] + 12),
                  ">> TOP PARES", font=head_font, fill=pal["footer"])

        partners = list(data.top_partners)[:5]
        if partners:
            mini_size = 80
            n = len(partners)
            slot_w = (top_panel[2] - top_panel[0] - 40) // 5
            for i, p in enumerate(partners):
                slot_x0 = top_panel[0] + 20 + i * slot_w
                cx = slot_x0 + (slot_w - mini_size) // 2
                cy = top_panel[1] + 44
                mini_resolved = royal_avatars.resolve_slug(
                    p.avatar_slug, p.royal_id)
                mini = royal_avatars.load_avatar(mini_resolved, mini_size)
                pixel_rect(draw, (cx - 2, cy - 2,
                                  cx + mini_size + 2, cy + mini_size + 2),
                           BLACK)
                pixel_rect(draw, (cx - 1, cy - 1,
                                  cx + mini_size + 1, cy + mini_size + 1),
                           pal["footer"])
                if mini is not None:
                    img.paste(mini, (cx, cy), mini)
                # nome curto abaixo
                pn_font = load_font(11, mono=True, bold=True)
                pn = ellipsize(p.name, 8).upper()
                pnw, _ = text_size_smart(draw, pn, pn_font)
                draw_text_smart(draw,
                                (slot_x0 + (slot_w - pnw) // 2,
                                 cy + mini_size + 8),
                                pn, pn_font, INK)
                # total Nx
                pt_font = load_font(12, mono=True, bold=True)
                pt = f"{p.total}x ♥"
                ptw, _ = text_size_smart(draw, pt, pt_font)
                draw_text_smart(draw,
                                (slot_x0 + (slot_w - ptw) // 2,
                                 cy + mini_size + 26),
                                pt, pt_font, HOT)
        else:
            empty_font = load_font(16, mono=True, bold=False)
            etxt = "[ NENHUM PAR REGISTRADO AINDA ]"
            ew, _ = text_size(draw, etxt, empty_font)
            draw.text(((W - ew) // 2, top_panel[1] + 90),
                      etxt, font=empty_font, fill=DIM)

        # Footer
        foot_font = load_font(12, mono=True, bold=False)
        foot_left = "> /royalmeuscasorios"
        foot_right = f"v0.1 // {pal['name']}_MODE"
        draw.text((OUT_PAD + 60, H - OUT_PAD - 50),
                  foot_left, font=foot_font, fill=DIM)
        fw, _ = text_size(draw, foot_right, foot_font)
        draw.text((W - OUT_PAD - 60 - fw, H - OUT_PAD - 50),
                  foot_right, font=foot_font, fill=pal["footer"])

        img = img.convert("RGBA")
        apply_scanlines(img, every=3, alpha=55)
        vimg = img.convert("RGB")
        apply_vignette(vimg, strength=170)
        img = vimg.convert("RGBA")
        apply_grain(img, intensity=18)
        img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90, optimize=True)
        payload = buf.getvalue()
        cache_put(cache_key, payload)
        return payload
    except Exception:
        logger.exception("ROYAL_MEUS_CASORIOS_CARD_RENDER_FAILED rid=%r",
                         data.self_royal_id)
        return None
