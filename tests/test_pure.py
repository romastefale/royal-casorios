"""Testes de funcoes puras criticas do bot.

Cobre regressao dos bugs corrigidos (format_br indefinido) + invariantes de
XP/level, normalizacao de palavra, eventos sazonais e hash do identity card.
"""
from datetime import date, datetime, timedelta

import main


# ---------------------------------------------------------------- format_br
# (regressao: format_br era NameError em 6 call sites — saldo/boss/presente)
def test_format_br_thousands():
    assert main.format_br(1234567) == "1.234.567"
    assert main.format_br(1000) == "1.000"
    assert main.format_br(999) == "999"
    assert main.format_br(0) == "0"


def test_format_br_aceita_float_e_str():
    assert main.format_br(1500.0) == "1.500"
    assert main.format_br("2500") == "2.500"


# ------------------------------------------------------------- normalize_word
def test_normalize_word_remove_acento_e_caso():
    assert main.normalize_word("Ação") == "acao"
    assert main.normalize_word("  Café  ") == "cafe"
    assert main.normalize_word("São-Paulo!") == "saopaulo"


def test_normalize_word_vazio():
    assert main.normalize_word("") == ""
    assert main.normalize_word(None) == ""


# ------------------------------------------------------------- normalize_pair
def test_normalize_pair_ordena():
    assert main.normalize_pair(5, 3) == (3, 5)
    assert main.normalize_pair(3, 5) == (3, 5)
    assert main.normalize_pair(7, 7) == (7, 7)


# -------------------------------------------------------------- level_progress
def test_level_progress_comeca_no_1():
    lvl, in_level, needed, _ = main.level_progress(0)
    assert lvl == 1
    assert in_level == 0
    assert needed >= 1


def test_level_progress_monotonico():
    last = 0
    for xp in [0, 500, 5000, 50000, 500000]:
        lvl, *_ = main.level_progress(xp)
        assert lvl >= last
        last = lvl


def test_level_progress_threshold_nivel_2():
    t2 = main._LEVEL_TABLE[2]
    assert main.level_progress(t2)[0] == 2
    assert main.level_progress(t2 - 1)[0] == 1


# ----------------------------------------------------------- seasonal events
def test_evento_natal_dobra_xp():
    ev = main.active_seasonal_event(date(2026, 12, 25))
    assert ev is not None and ev["id"] == "natal" and ev["xp_mult"] == 2.0


def test_evento_reveillon_cruza_ano():
    assert main.active_seasonal_event(date(2026, 12, 31))["id"] == "reveillon"
    assert main.active_seasonal_event(date(2026, 1, 1))["id"] == "reveillon"


def test_evento_namorados():
    ev = main.active_seasonal_event(date(2026, 6, 12))
    assert ev["id"] == "dia_namorados" and ev["xp_mult"] == 1.5


def test_fim_de_semana_fallback():
    d = date(2026, 3, 1)  # marco nao tem evento especial
    while d.weekday() != 5:  # acha um sabado
        d += timedelta(days=1)
    ev = main.active_seasonal_event(d)
    assert ev is not None and ev["id"] == "fds" and ev["xp_mult"] == 1.5


def test_dia_util_sem_evento():
    d = date(2026, 3, 1)
    while d.weekday() != 2:  # acha uma quarta
        d += timedelta(days=1)
    assert main.active_seasonal_event(d) is None


# ----------------------------------------------------------- season code
def test_season_code_verao_cruza_ano():
    assert main.current_season_code(date(2025, 12, 25)) == "verao-2026"
    assert main.current_season_code(date(2026, 1, 10)) == "verao-2026"
    assert main.current_season_code(date(2026, 4, 1)) == "outono-2026"
    assert main.current_season_code(date(2026, 7, 1)) == "inverno-2026"
    assert main.current_season_code(date(2026, 10, 1)) == "primavera-2026"


# ----------------------------------------------------------- week marker
def test_week_marker_formato():
    m = main.current_week_marker(datetime(2026, 1, 5, 12, 0))
    assert m.startswith("2026-W")
    assert len(m.split("W")[1]) == 2


# ----------------------------------------------------------- identity hash
def test_identity_hash_estavel():
    a = main._identity_card_hash("Rei", "toxic", "user", "ROY#1")
    b = main._identity_card_hash("Rei", "toxic", "user", "ROY#1")
    assert a == b and len(a) == 16


def test_identity_hash_muda_com_avatar():
    a = main._identity_card_hash("Rei", "toxic")
    b = main._identity_card_hash("Rei", "amber")
    assert a != b


# ----------------------------------------------- lucky_reward_for (Emoji Sorte)
# 🎰 slot do Telegram: trincas = 1(bar) 22(uva) 43(limao) 64(7️⃣7️⃣7️⃣=jackpot).
# So trinca premia; jackpot paga em dobro; resto (e None) NAO premia.
def test_lucky_reward_jackpot():
    xp, gold, jackpot = main.lucky_reward_for(64)
    assert (xp, gold, jackpot) == (
        main.LUCKY_JACKPOT_XP, main.LUCKY_JACKPOT_GOLD, True)


def test_lucky_reward_trincas_simples():
    for v in (1, 22, 43):
        xp, gold, jackpot = main.lucky_reward_for(v)
        assert (xp, gold, jackpot) == (
            main.LUCKY_WIN_XP, main.LUCKY_WIN_GOLD, False), f"valor {v}"


def test_lucky_reward_nao_trinca_e_none():
    for v in (2, 10, 33, 50, 63, 0, None):
        assert main.lucky_reward_for(v) == (0, 0, False), f"valor {v}"


def test_lucky_constantes_coerentes():
    # jackpot tem que estar dentro do conjunto de trincas e pagar mais
    assert main.LUCKY_SLOT_JACKPOT in main.LUCKY_SLOT_WINS
    assert main.LUCKY_JACKPOT_XP > main.LUCKY_WIN_XP
    assert main.LUCKY_JACKPOT_GOLD > main.LUCKY_WIN_GOLD
    assert main.LUCKY_DAILY_CAP >= 1
    assert main.LUCKY_EMOJI == "🎰"


def _flat_buttons(kb):
    return [b for row in kb.inline_keyboard for b in row]


def test_tutorial_kb_lock_duravel_uid_embutido():
    # Menus persistentes (auto_delete_secs=0) NAO podem depender do TTL de
    # _msg_owners: o uid do dono tem que estar embutido em TODO callback de
    # navegacao (r:tut:{idx}:{uid}) e no Fechar (r:close:{uid}), senao apos o
    # TTL expirar qualquer um operaria o tutorial alheio.
    uid = 4242
    last = len(main.ROYAL_TUTORIAL_PARTS) - 1
    for idx in (0, max(0, last // 2), last):
        kb = main._tutorial_kb(idx, uid)
        nav = [b for b in _flat_buttons(kb)
               if b.callback_data and b.callback_data.startswith("r:tut:")]
        # ha ao menos um botao de navegacao em cada extremo (exceto deck de 1)
        for b in nav:
            assert b.callback_data.endswith(f":{uid}"), b.callback_data
            assert len(b.callback_data.split(":")) == 4
        closes = [b for b in _flat_buttons(kb)
                  if b.callback_data == f"r:close:{uid}"]
        assert closes, "tutorial sem botao Fechar owner-locked"


def test_avatar_kb_lock_duravel_uid_embutido():
    uid = 777
    kb = main._avatar_kb(uid)
    grid = [b for b in _flat_buttons(kb)
            if b.callback_data and b.callback_data.startswith("r:av:")]
    assert len(grid) == len(main.royal_avatars.SLUGS)
    for b in grid:
        assert b.callback_data.endswith(f":{uid}"), b.callback_data
        assert len(b.callback_data.split(":")) == 4
    closes = [b for b in _flat_buttons(kb)
              if b.callback_data == f"r:close:{uid}"]
    assert closes, "mosaico de avatar sem botao Fechar owner-locked"


# ---------------------------------------------------- icones dos cards (sprites)
# (regressao: fontes do projeto nao cobrem 👑🌹🧙📜🧪🥾💍 -> tofu no card.
#  Cada classe/item precisa de um sprite mapeado em EMOJI_SPRITES.)
def _strip_vs(s: str) -> str:
    return (s or "").replace("\ufe0f", "")


def test_emoji_sprites_lookup_ignora_variation_selector():
    import royal_render as rr
    # 🛡️ (com U+FE0F) e 🛡 (sem) resolvem o mesmo sprite
    assert _strip_vs("🛡️") in rr.EMOJI_SPRITES
    assert rr.EMOJI_SPRITES[_strip_vs("🛡️")] is rr.EMOJI_SPRITES[_strip_vs("🛡")]


def test_toda_classe_tem_sprite_no_card():
    import royal_render as rr
    faltando = [cid for cid, info in main.CLASSES.items()
                if _strip_vs(info["emoji"]) not in rr.EMOJI_SPRITES]
    assert not faltando, f"classes sem sprite (dao tofu no card): {faltando}"


def test_todo_item_tem_sprite_no_card():
    import royal_render as rr
    faltando = [iid for iid, info in main.ITEMS.items()
                if _strip_vs(info["emoji"]) not in rr.EMOJI_SPRITES]
    assert not faltando, f"itens sem sprite (dao tofu no card): {faltando}"
