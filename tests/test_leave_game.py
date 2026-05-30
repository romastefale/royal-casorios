"""Etapa 4 — entrada/saida do jogo (reversivel).

Trava as invariantes do sinalizador `players.left_game` (migration v16):
- saida NUNCA apaga dados, so alterna o flag;
- quem saiu (left_game=1) some do ranking publico e dos casorios
  automaticos, mas volta intacto depois de set_player_out(..., False).
"""
import main


def test_left_game_default_zero_e_toggle_reversivel():
    chat = -1001
    main.ensure_player(chat, 11)
    # default = jogando
    assert main.is_player_out(chat, 11) is False
    main.set_player_out(chat, 11, True)
    assert main.is_player_out(chat, 11) is True
    # voltar -> intacto (a linha continua existindo, so o flag muda)
    main.set_player_out(chat, 11, False)
    assert main.is_player_out(chat, 11) is False


def test_top_season_players_exclui_quem_saiu():
    chat = -1002
    for uid in (21, 22):
        main.ensure_player(chat, uid)
    main.cur.execute(
        "UPDATE players SET season_xp=100 WHERE chat_id=? AND user_id=?",
        (chat, 21))
    main.cur.execute(
        "UPDATE players SET season_xp=50 WHERE chat_id=? AND user_id=?",
        (chat, 22))
    main.db.commit()
    assert [r["user_id"] for r in main.top_season_players(chat)] == [21, 22]
    main.set_player_out(chat, 21, True)
    # quem saiu nao e rankeado; o resto do ranking segue igual
    assert [r["user_id"] for r in main.top_season_players(chat)] == [22]
    # voltar -> reaparece no topo (dados preservados)
    main.set_player_out(chat, 21, False)
    assert [r["user_id"] for r in main.top_season_players(chat)] == [21, 22]


def test_user_is_available_respeita_left_game():
    chat = -1003
    uid = 31
    main.ensure_player(chat, uid)
    main.upsert_user(chat, uid, "User31", None)
    main.db.commit()
    assert main.user_is_available(chat, uid) is True
    main.set_player_out(chat, uid, True)
    assert main.user_is_available(chat, uid) is False
    main.set_player_out(chat, uid, False)
    assert main.user_is_available(chat, uid) is True


def _season_xp(chat, uid):
    row = main.cur.execute(
        "SELECT season_xp FROM players WHERE chat_id=? AND user_id=?",
        (chat, uid)).fetchone()
    return row["season_xp"] if row else None


def test_award_xp_immediate_no_op_para_quem_saiu():
    # Chokepoint central: fora do jogo nao pontua por NENHUMA via (chest, boss,
    # quest, lucky, tomo, vote_like, palavra, couple todas passam por aqui).
    # Usa deltas (event_xp_mult/buffs podem alterar o valor absoluto).
    chat = -1004
    uid = 41
    main.ensure_player(chat, uid)
    main.award_xp_immediate(chat, uid, 100, reason="test")
    assert _season_xp(chat, uid) > 0  # jogando -> pontua
    main.set_player_out(chat, uid, True)
    frozen = _season_xp(chat, uid)
    main.award_xp_immediate(chat, uid, 100, reason="test")  # ignorado
    assert _season_xp(chat, uid) == frozen  # fora -> nao move
    # voltar -> volta a pontuar
    main.set_player_out(chat, uid, False)
    main.award_xp_immediate(chat, uid, 50, reason="test")
    assert _season_xp(chat, uid) > frozen


def test_award_xp_message_no_op_para_quem_saiu():
    chat = -1005
    uid = 51
    main.ensure_player(chat, uid)
    main.set_player_out(chat, uid, True)
    before = _season_xp(chat, uid)
    main.award_xp_message(chat, uid, is_reply=False)  # ignorado
    assert _season_xp(chat, uid) == before
