"""Catalogo de 32 avatares Stardew-style (12 zodiaco chines + 20 humanos
diversos + 4 extras animais), 36 slots no mosaico (4 vazios decorativos).

Player escolhe via /royalavatar respondendo ao mosaico com o numero 01-36.
Mudanca permitida 1x por temporada (validacao no handler do bot).
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

from PIL import Image

_DIR = Path(__file__).parent / "assets" / "avatars"
MOSAIC_PATH = Path(__file__).parent / "assets" / "avatars_mosaic_preview.png"

# (slug, nome_curto) — ordem fixa = posicao 1..36 no mosaico
CATALOG: list[tuple[str, str]] = [
    # Zodiaco chines (01-12)
    ("z01-rato",                "Rato"),
    ("z02-boi",                 "Boi"),
    ("z03-tigre",               "Tigre"),
    ("z04-coelho",              "Coelho"),
    ("z05-dragao",              "Dragão"),
    ("z06-serpente",            "Cobra"),
    ("z07-cavalo",              "Cavalo"),
    ("z08-cabra",               "Cabra"),
    ("z09-macaco",              "Macaco"),
    ("z10-galo",                "Galo"),
    ("z11-cao",                 "Cão"),
    ("z12-porco",               "Porco"),
    # Humanos diversos (13-32) — etnia, idade e genero variados
    ("h01-eastasian-f-young",   "Sábia"),
    ("h02-eastasian-m-adult",   "Samurai"),
    ("h03-eastasian-f-elder",   "Imperatriz"),
    ("h04-southasian-f-young",  "Princesa"),
    ("h05-southasian-m-adult",  "Marajá"),
    ("h06-african-f-young",     "Rainha"),
    ("h07-african-m-adult",     "Guerreiro"),
    ("h08-african-f-elder",     "Anciã"),
    ("h09-mideast-f-young",     "Odalisca"),
    ("h10-mideast-m-adult",     "Sultão"),
    ("h11-european-f-young",    "Donzela"),
    ("h12-european-m-adult",    "Cavaleiro"),
    ("h13-european-m-elder",    "Mago"),
    ("h14-indigenous-f-young",  "Curandeira"),
    ("h15-indigenous-m-adult",  "Cacique"),
    ("h16-latina-f-young",      "Florista"),
    ("h17-latino-m-adult",      "Fidalgo"),
    ("h18-pacific-f-young",     "Sereia"),
    ("h19-pacific-m-adult",     "Tatuado"),
    ("h20-nonbinary-mixed",     "Lunar"),
    # Extras animais (33-36)
    ("x01-lobo",                "Lobo"),
    ("x02-coruja",              "Coruja"),
    ("x03-leao",                "Leão"),
    ("x04-raposa",              "Raposa"),
]

SLUGS: list[str] = [s for s, _ in CATALOG]
SLUG_TO_INDEX: dict[str, int] = {s: i for i, s in enumerate(SLUGS)}  # 0-indexed
SLUG_TO_NAME: dict[str, str] = {s: n for s, n in CATALOG}


def default_slug(royal_id: str) -> str:
    """Default deterministico por royal_id (hash MD5). Sempre o mesmo pro
    mesmo player ate ele escolher manualmente via /royalavatar."""
    seed = (royal_id or "RYL-0000").encode("utf-8")
    h = int(hashlib.md5(seed).hexdigest(), 16)
    return SLUGS[h % len(SLUGS)]


def resolve_slug(slug: Optional[str], royal_id: str) -> str:
    """Slug valido salvo no DB OU fallback ao default deterministico."""
    if slug and slug in SLUG_TO_NAME:
        return slug
    return default_slug(royal_id)


def display_name(slug: str) -> str:
    return SLUG_TO_NAME.get(slug, "?")


def number_of(slug: str) -> int:
    """Posicao 1-indexed no mosaico (1..36). Retorna 0 se slug invalido."""
    idx = SLUG_TO_INDEX.get(slug)
    return idx + 1 if idx is not None else 0


def slug_at(number: int) -> Optional[str]:
    """Lookup 1-indexed (1..36). Retorna None se fora do range OU se for
    um slot vazio decorativo (33-36 sao validos; > 36 nao)."""
    if 1 <= number <= len(SLUGS):
        return SLUGS[number - 1]
    return None


# Cache LRU simples de imagens redimensionadas
_IMG_CACHE: dict[tuple[str, int], Image.Image] = {}
_CACHE_MAX = 128


def load_avatar(slug: Optional[str], size: int = 256) -> Optional[Image.Image]:
    """Carrega avatar PNG RGBA redimensionado pra `size`. Cacheia em memoria.
    Retorna None se arquivo nao existe ou Pillow falhar."""
    if not slug or slug not in SLUG_TO_NAME:
        return None
    key = (slug, size)
    if key in _IMG_CACHE:
        return _IMG_CACHE[key]
    p = _DIR / f"{slug}.png"
    if not p.exists():
        return None
    try:
        img = Image.open(p).convert("RGBA")
        if img.size != (size, size):
            img = img.resize((size, size), Image.LANCZOS)
        if len(_IMG_CACHE) >= _CACHE_MAX:
            # evict um item aleatorio (cache simples — sem LRU real)
            _IMG_CACHE.pop(next(iter(_IMG_CACHE)))
        _IMG_CACHE[key] = img
        return img
    except Exception:
        return None


def mosaic_bytes() -> Optional[bytes]:
    """Le o mosaico do disco (1862x1862 PNG). None se faltar arquivo."""
    try:
        return MOSAIC_PATH.read_bytes()
    except Exception:
        return None
