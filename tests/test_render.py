"""Testes da camada de texto anti-tofu do gerador de cards (royal_render).

Garantem que nomes com Unicode estilizado não viram "caixinha" (tofu) no card:
  - fullwidth/enclosed/letterlike → NFKC cai na forma ASCII coberta pelas fontes;
  - emoji colorido / CJK sem fonte → glifo descartado (some, nunca tofu);
  - math-bold (𝕽𝓮) continua renderizando via NotoSansMath (não regredir);
  - ASCII/acentos/cirílico normais passam intactos.
Também trava a invariante: TODO emoji de CLASSES/ITEMS tem sprite mapeado
(draw_icon_centered cai em fonte-texto só se faltar sprite — agora isso é
pego no CI, não como tofu no card).
"""
import main  # noqa: F401  (conftest aponta DATABASE_PATH p/ tmp antes do import)
import royal_render as rr
from royal.core import CLASSES, ITEMS


def _layout_str(text: str) -> str:
    """Roda _layout_glyphs e devolve só os chars que SERIAM desenhados."""
    primary = rr.load_font(40, mono=False, bold=True)
    fb = rr._fallback_fonts_for(40)
    return "".join(ch for ch, _ in rr._layout_glyphs(text, primary, fb))


def test_fullwidth_vira_ascii_legivel():
    # Ｋｉｎｇ (U+FF2B…) não é coberto por nenhuma fonte → NFKC → "King".
    assert _layout_str("Ｋｉｎｇ") == "King"


def test_enclosed_e_letterlike_nao_viram_tofu():
    # ① (enclosed) e ℝ (letterlike) SÃO cobertos pelos fallbacks Noto
    # (symbols/math) → renderizam como estão, sem virar caixinha nem sumir.
    assert _layout_str("①ℝ") == "①ℝ"


def test_emoji_e_cjk_sem_fonte_sao_descartados_sem_tofu():
    # Emoji colorido e ideograma CJK não têm fonte → somem (nada de tofu).
    assert _layout_str("João✨王") == "João"


def test_ascii_acentos_e_cirilico_passam_intactos():
    assert _layout_str("José Бот") == "José Бот"


def test_acento_decomposto_renderiza_sem_sumir():
    # "e" + combining acute (U+0301) — não pode descartar o acento nem virar
    # tofu (DejaVu cobre a combining mark; NFKC compõe pra "é" se preciso).
    out = _layout_str("e\u0301")
    assert out and "e" in out


def test_nome_totalmente_sem_cobertura_nao_quebra():
    # Nome só com CJK (sem fonte no projeto) → tudo descartado → string vazia,
    # sem crash (o card segue legível pelo royal_id, que acompanha o nome).
    # Obs: alguns emoji (ex. 😀) SÃO cobertos monocromáticos por NotoSymbols2 e
    # renderizam — o que importa é nunca virar tofu; CJK não tem fonte → some.
    assert _layout_str("王五") == ""


def test_math_bold_continua_renderizando_nao_descarta():
    # 𝕽𝓮 são cobertos por NotoSansMath → NÃO podem ser descartados.
    out = _layout_str("𝕽𝓮")
    assert len(out) == 2


def test_layout_e_width_consistentes_entre_draw_e_size():
    # text_size_smart usa o MESMO _layout_glyphs que draw_text_smart → largura
    # de um nome com fullwidth deve casar com a do equivalente ASCII.
    from PIL import Image, ImageDraw
    img = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(img)
    font = rr.load_font(40, mono=False, bold=True)
    assert rr.text_size_smart(draw, "Ｋｉｎｇ", font) == \
        rr.text_size_smart(draw, "King", font)


def test_todo_emoji_de_classe_tem_sprite():
    faltando = [c["emoji"] for c in CLASSES.values()
                if c["emoji"].replace("\ufe0f", "") not in rr.EMOJI_SPRITES]
    assert not faltando, f"classes sem sprite (dariam tofu/sumiço): {faltando}"


def test_todo_emoji_de_item_tem_sprite():
    faltando = [it["emoji"] for it in ITEMS.values()
                if it["emoji"].replace("\ufe0f", "") not in rr.EMOJI_SPRITES]
    assert not faltando, f"itens sem sprite (dariam tofu/sumiço): {faltando}"
