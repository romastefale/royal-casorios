"""Smoke test: importar `main` num DB temporario roda as migrations ate o fim
sem corromper schema. Garante que run_migrations aplica todas as versoes numa
base vazia (o que o boot real faz no 1o deploy).
"""
import asyncio
import types

import main


def _run_msg_mw(message):
    """Roda a outer-middleware de mensagem e diz se o handler foi chamado."""
    called = {"hit": False}

    async def _handler(msg, data):
        called["hit"] = True
        return "OK"

    asyncio.run(main._require_user_for_commands(_handler, message, {}))
    return called["hit"]


def _fake_msg(*, is_bot=None, uid=1, text=None,
              migrate_to=None, migrate_from=None, chat_id=None):
    fu = None
    if is_bot is not None:
        fu = types.SimpleNamespace(is_bot=is_bot, id=uid)
    chat = types.SimpleNamespace(id=chat_id) if chat_id is not None else None
    return types.SimpleNamespace(
        from_user=fu, text=text, chat=chat,
        migrate_to_chat_id=migrate_to, migrate_from_chat_id=migrate_from)


def test_middleware_dropa_bot_mas_passa_humano():
    # admin anonimo / canal = is_bot True -> dropado
    assert _run_msg_mw(_fake_msg(is_bot=True, uid=1087968824)) is False
    # humano normal -> passa
    assert _run_msg_mw(_fake_msg(is_bot=False, uid=42)) is True
    # bot de musica postando faixa (sem "/") -> passa p/ handle_music_bot_post
    assert _run_msg_mw(_fake_msg(is_bot=True, uid=main.MUSIC_BOT_ID)) is True
    # ...mas o bot de musica mandando um COMANDO -> dropado (nao vira jogador)
    assert _run_msg_mw(_fake_msg(
        is_bot=True, uid=main.MUSIC_BOT_ID, text="/royalperfil")) is False


def test_middleware_allowlist_grupo_ponte_passa_bot_da_mira():
    """A resposta da @Mira chega como is_bot=True NO grupo-ponte. A middleware
    precisa deixar passar (allowlist) p/ o capture rodar — senao o drop is_bot
    mataria a resposta ANTES do router e a ponte nunca funcionaria."""
    bridge = main.IA_BRIDGE_CHAT_ID
    if not (main.MIRA_ENABLED and bridge is not None):
        return  # ponte off neste ambiente -> nada a checar
    # bot da @Mira no grupo-ponte -> PASSA (allowlist)
    assert _run_msg_mw(
        _fake_msg(is_bot=True, uid=8377231659, chat_id=bridge)) is True
    # o MESMO bot em OUTRO chat -> dropado (allowlist e so do grupo-ponte)
    assert _run_msg_mw(
        _fake_msg(is_bot=True, uid=8377231659, chat_id=bridge + 1)) is False


def test_callback_middleware_dropa_bot():
    """Admins anonimos pressionando botoes chegam como bot (is_bot=True) e
    nao podem cair em ensure_player via callbacks."""
    called = {"hit": False}

    async def _handler(cb, data):
        called["hit"] = True
        return "OK"

    def _cb(is_bot):
        return types.SimpleNamespace(
            from_user=types.SimpleNamespace(is_bot=is_bot, id=7))

    asyncio.run(main._block_bot_callbacks(_handler, _cb(True), {}))
    assert called["hit"] is False
    called["hit"] = False
    asyncio.run(main._block_bot_callbacks(_handler, _cb(False), {}))
    assert called["hit"] is True


def test_middleware_nao_bloqueia_migracao_de_admin_anonimo():
    """Regressao: msg de servico de migracao vinda de @GroupAnonymousBot
    (is_bot=True) NAO pode ser dropada — on_chat_migration precisa dela."""
    assert _run_msg_mw(_fake_msg(
        is_bot=True, uid=1087968824, migrate_to=-100123)) is True
    assert _run_msg_mw(_fake_msg(
        is_bot=True, uid=1087968824, migrate_from=-100456)) is True


def test_cleanup_remove_bots_legados_e_e_idempotente():
    """Varredura one-shot remove SO os bots conhecidos (legado pre-filtro),
    preserva humanos e e idempotente (flag em bot_meta)."""
    chat = -99900123
    bot_uid = 1087968824   # @GroupAnonymousBot (confirmado bot)
    human = 70123          # jogador humano
    main.ensure_player(chat, bot_uid)
    main.ensure_player(chat, human)
    # ref de usuario sob outros nomes de coluna: couples(user1/user2),
    # gifts(from_user/to_user)
    main.cur.execute(
        "INSERT INTO couples (chat_id, user1, user2, source, created_at) "
        "VALUES (?, ?, ?, 'auto', '2026-01-01')", (chat, bot_uid, human))
    main.cur.execute(
        "INSERT INTO gifts (chat_id, from_user, to_user, amount, sent_at) "
        "VALUES (?, ?, ?, 50, '2026-01-01')", (chat, human, bot_uid))
    # votes(voter_id) — outra coluna de ref de usuario
    main.cur.execute(
        "INSERT INTO votes (couple_id, voter_id, type, created_at) "
        "VALUES (?, ?, 'like', '2026-01-01')", (999111, bot_uid))

    res = main.cleanup_legacy_bot_players()
    assert res.get("players", 0) >= 1
    assert res.get("couples", 0) >= 1
    assert res.get("gifts", 0) >= 1
    assert res.get("votes", 0) >= 1
    assert main.cur.execute(
        "SELECT 1 FROM votes WHERE voter_id=?", (bot_uid,)).fetchone() is None
    # bot removido em todas as superficies, humano preservado
    assert main.cur.execute(
        "SELECT 1 FROM players WHERE chat_id=? AND user_id=?",
        (chat, bot_uid)).fetchone() is None
    assert main.cur.execute(
        "SELECT 1 FROM players WHERE chat_id=? AND user_id=?",
        (chat, human)).fetchone() is not None
    assert main.cur.execute(
        "SELECT 1 FROM couples WHERE chat_id=? AND (user1=? OR user2=?)",
        (chat, bot_uid, bot_uid)).fetchone() is None
    assert main.cur.execute(
        "SELECT 1 FROM gifts WHERE chat_id=? AND (from_user=? OR to_user=?)",
        (chat, bot_uid, bot_uid)).fetchone() is None
    # idempotente: 2a chamada nao faz nada (flag setada)
    assert main.cleanup_legacy_bot_players() == {}


def test_migrations_aplicam_ate_a_ultima_versao():
    v = main.cur.execute("PRAGMA user_version").fetchone()[0]
    assert v >= 14


def test_tabelas_criticas_existem():
    rows = main.cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {r["name"] for r in rows}
    for t in ("players", "couples", "gifts", "achievements",
              "quest_progress", "stars_purchases", "lucky_emoji_daily",
              "palavra_pool"):
        assert t in names, f"tabela {t} ausente"


def test_integridade_do_db():
    res = main.cur.execute("PRAGMA integrity_check").fetchone()[0]
    assert res == "ok"


def test_migrate_chat_data_preserva_progresso_e_e_idempotente():
    old, new = -111111, -1009999999999
    uid, uid2 = 42, 99

    # progresso real no grupo antigo
    main.cur.execute(
        "INSERT INTO players (chat_id, user_id, royal_id, total_xp, gold) "
        "VALUES (?,?,?,?,?)", (old, uid, "RYL-0042", 5000, 777))
    main.cur.execute(
        "INSERT INTO achievements (chat_id, user_id, slug, unlocked_at) "
        "VALUES (?,?,?,?)", (old, uid, "generoso", main.utc_iso()))
    main.cur.execute(
        "INSERT INTO couples (chat_id, user1, user2, source, created_at) "
        "VALUES (?,?,?,?,?)", (old, uid, 43, "auto", main.utc_iso()))
    # DM do user aponta p/ o grupo antigo
    main.cur.execute(
        "INSERT INTO user_dm_settings (user_id, active_chat_id) VALUES (?,?)",
        (uid, old))
    # CONFLITO: mesma PK (uid) ja existe no supergrupo com linha zerada
    main.cur.execute(
        "INSERT INTO players (chat_id, user_id, royal_id, total_xp, gold) "
        "VALUES (?,?,?,?,?)", (new, uid, "RYL-9999", 0, 50))
    # SEM conflito: outro user que so existe no supergrupo (deve sobreviver)
    main.cur.execute(
        "INSERT INTO players (chat_id, user_id, royal_id, total_xp, gold) "
        "VALUES (?,?,?,?,?)", (new, uid2, "RYL-0099", 1200, 300))
    main.db.commit()

    moved = main.migrate_chat_data(old, new)
    assert moved.get("players") == 1
    assert moved.get("achievements") == 1
    assert moved.get("couples") == 1

    # em conflito, o progresso ANTIGO venceu (nao a linha zerada)
    row = main.cur.execute(
        "SELECT total_xp, gold, royal_id FROM players WHERE chat_id=? AND user_id=?",
        (new, uid)).fetchone()
    assert row["total_xp"] == 5000 and row["gold"] == 777
    assert row["royal_id"] == "RYL-0042"
    # linha SEM conflito do supergrupo foi PRESERVADA
    row2 = main.cur.execute(
        "SELECT total_xp FROM players WHERE chat_id=? AND user_id=?",
        (new, uid2)).fetchone()
    assert row2 is not None and row2["total_xp"] == 1200
    # ponteiro de DM reapontado p/ o novo id
    assert main.cur.execute(
        "SELECT active_chat_id AS a FROM user_dm_settings WHERE user_id=?",
        (uid,)).fetchone()["a"] == new
    # nada sobrou sob o id antigo
    assert main.cur.execute(
        "SELECT COUNT(*) AS c FROM players WHERE chat_id=?", (old,)
    ).fetchone()["c"] == 0
    assert main.cur.execute(
        "SELECT COUNT(*) AS c FROM couples WHERE chat_id=?", (new,)
    ).fetchone()["c"] == 1

    # idempotente: rodar de novo NAO apaga os dados ja migrados
    assert main.migrate_chat_data(old, new) == {}
    assert main.cur.execute(
        "SELECT total_xp FROM players WHERE chat_id=? AND user_id=?",
        (new, uid)).fetchone()["total_xp"] == 5000


def test_migrate_conflito_duplo_pk_e_unique_em_players():
    """Conflito 'duplo' no novo id: uma linha colide por user_id (PK) e OUTRA
    linha (user diferente) colide por royal_id (UNIQUE). UPDATE OR REPLACE
    descarta AMBAS e a identidade ANTIGA vence. Comportamento esperado fixado."""
    old, new = -222222, -1008888888888
    uid = 7

    main.cur.execute(
        "INSERT INTO players (chat_id, user_id, royal_id, total_xp, gold) "
        "VALUES (?,?,?,?,?)", (old, uid, "RYL-0007", 9000, 111))
    # colide por PK (mesmo user_id)
    main.cur.execute(
        "INSERT INTO players (chat_id, user_id, royal_id, total_xp, gold) "
        "VALUES (?,?,?,?,?)", (new, uid, "RYL-1111", 0, 0))
    # colide por UNIQUE (mesmo royal_id, user_id diferente)
    main.cur.execute(
        "INSERT INTO players (chat_id, user_id, royal_id, total_xp, gold) "
        "VALUES (?,?,?,?,?)", (new, 8, "RYL-0007", 50, 5))
    main.db.commit()

    main.migrate_chat_data(old, new)

    rows = main.cur.execute(
        "SELECT user_id, royal_id, total_xp FROM players WHERE chat_id=?",
        (new,)).fetchall()
    assert len(rows) == 1
    assert rows[0]["user_id"] == uid
    assert rows[0]["royal_id"] == "RYL-0007"
    assert rows[0]["total_xp"] == 9000


def test_chest_claim_atomico_evita_baú_duplicado():
    """Regressao anti-duplicacao (multi-instancia): o claim pending->open eh
    atomico. Simula 2 instancias 'concorrendo' pelo mesmo bau pendente: a 1a
    transicao casa (rowcount 1) e envia; a 2a vê status != 'pending' (rowcount
    0) e aborta sem reenviar. Replica exatamente o WHERE de spawn_chest."""
    chat_id = -1007777777777
    spawn_at = main.utc_iso()
    main.cur.execute(
        "INSERT INTO chests (chat_id, spawn_at, status) VALUES (?, ?, 'pending')",
        (chat_id, spawn_at))
    chest_id = main.cur.lastrowid
    main.db.commit()

    def _claim() -> int:
        main.cur.execute(
            "UPDATE chests SET status='open', spawned_at=? "
            "WHERE id=? AND status='pending'",
            (main.utc_iso(), chest_id))
        main.db.commit()
        return main.cur.rowcount

    assert _claim() == 1   # 1a instancia ganha → envia
    assert _claim() == 0   # 2a instancia perde → aborta (sem duplicar)
    st = main.cur.execute(
        "SELECT status FROM chests WHERE id=?", (chest_id,)).fetchone()["status"]
    assert st == "open"


def test_consume_consumable_atomico_evita_uso_duplicado():
    """Regressao anti-exploit: o card de inventario nao apaga o botao 'usar'
    in-place apos o uso, entao ele persiste. `consume_consumable` so concede o
    efeito se houver qty>0 (guard atomico) — clicar o botao depois do item
    zerar retorna False (sem +XP/heal de graca). Cobre tambem item inexistente."""
    chat_id = -1008888888888
    uid = 555
    iid = "tomo"
    main.cur.execute(
        "INSERT OR REPLACE INTO inventory (chat_id, user_id, item_id, qty) "
        "VALUES (?, ?, ?, 1)", (chat_id, uid, iid))
    main.db.commit()

    assert main.consume_consumable(chat_id, uid, iid) is True   # 1o uso ok
    assert main.consume_consumable(chat_id, uid, iid) is False  # zerou -> nega
    # linha removida quando qty<=0 (sem qty negativo)
    assert main.cur.execute(
        "SELECT 1 FROM inventory WHERE chat_id=? AND user_id=? AND item_id=?",
        (chat_id, uid, iid)).fetchone() is None
    # item que o jogador nunca teve -> tambem nega
    assert main.consume_consumable(chat_id, uid, "nao_existe") is False


def test_privacy_chat_id_prefere_grupo_ativo():
    """Regressao bug: configs de privacidade na DM miram o grupo ATIVO (escolha
    em /royalgrupo), nao um arbitrario (`LIMIT 1`). Antes, quem tinha perfil em
    varios grupos so conseguia configurar a privacidade de um deles."""
    from royal.handlers.privacidade import _privacy_chat_id
    uid = 990011
    chat_a = -1009000000001
    chat_b = -1009000000002
    for c, rid in ((chat_a, "RYL-9001"), (chat_b, "RYL-9002")):
        main.cur.execute(
            "INSERT OR REPLACE INTO players (chat_id, user_id, royal_id) "
            "VALUES (?, ?, ?)", (c, uid, rid))
    main.db.commit()

    # sem grupo ativo -> fallback p/ algum grupo com perfil
    assert _privacy_chat_id(uid) in (chat_a, chat_b)
    # ativo = B -> retorna exatamente B (nao o arbitrario)
    main.set_dm_active_chat(uid, chat_b)
    assert _privacy_chat_id(uid) == chat_b
    # ativo aponta p/ grupo SEM perfil -> ignora e cai no fallback valido
    main.set_dm_active_chat(uid, -1009999999999)
    assert _privacy_chat_id(uid) in (chat_a, chat_b)
    # usuario sem nenhum perfil -> None
    assert _privacy_chat_id(880022) is None


def test_migrate_listas_cobrem_todas_as_tabelas_com_chat_id():
    """Guarda de regressao: nenhuma tabela com coluna chat_id pode ficar fora
    das listas de migracao (senao novo schema orfanaria dados na migracao)."""
    coberto = set(main._CHAT_MIGRATE_PK_TABLES) | set(main._CHAT_MIGRATE_FK_TABLES)
    tabelas = [r["name"] for r in main.cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    com_chat_id = set()
    for t in tabelas:
        cols = {c["name"] for c in main.cur.execute(
            f"PRAGMA table_info({t})").fetchall()}
        if "chat_id" in cols:
            com_chat_id.add(t)
    faltando = com_chat_id - coberto
    assert not faltando, f"tabelas com chat_id fora da migracao: {faltando}"


def _route_callback(data):
    """Feed a callback through o dispatcher REAL e devolve o nome do handler que
    venceu (sem chamar a API: os handlers-alvo sao espionados)."""
    from datetime import datetime
    from aiogram.types import CallbackQuery, User, Chat, Message, Update

    import royal.handlers.hub as hubmod
    import royal.handlers.missoes as missmod
    import royal.handlers.cfg as cfgmod

    winner = {"name": None}
    spies = {
        id(hubmod.hub_cb): "hub_cb",
        id(missmod.quest_claim_cb): "quest_claim_cb",
        id(cfgmod.cfg_cb): "cfg_cb",
    }
    originals = []

    def _make(name):
        async def _spy(cb):
            winner["name"] = name
        return _spy

    for r in main.dp.sub_routers:
        for h in r.callback_query.handlers:
            nm = spies.get(id(h.callback))
            if nm:
                originals.append((h, h.callback))
                h.callback = _make(nm)
    try:
        user = User(id=42, is_bot=False, first_name="T")
        chat = Chat(id=-1002556760909, type="supergroup")
        msg = Message(message_id=10, date=datetime.now(), chat=chat,
                      from_user=user, text="x")
        cb = CallbackQuery(id="1", from_user=user, chat_instance="ci",
                           message=msg, data=data)
        # main.bot pode ser None no ambiente de teste (sem token real); o
        # dispatcher so usa bot.id no log final, depois do handler ja ter rodado.
        fake_bot = main.bot or types.SimpleNamespace(id=0)
        asyncio.run(main.dp.feed_update(
            fake_bot, Update(update_id=1, callback_query=cb)))
    finally:
        for h, orig in originals:
            h.callback = orig
    return winner["name"]


def test_callbacks_dedicados_nao_sao_engolidos_pelo_hub_cb():
    """Regressao: o catch-all hub_cb (`r:` & ~_HUB_DEDICATED_PREFIXES) e
    incluido ANTES de missoes/cfg. Se "r:quest:"/"r:cfg:" nao estiverem na
    allowlist de prefixos dedicados, o hub_cb vence e da `cb.answer()` mudo ->
    o botao Resgatar (missao) e os toggles do /royalconfig morrem em silencio.
    """
    from royal.core import _HUB_DEDICATED_PREFIXES
    assert "r:quest:" in _HUB_DEDICATED_PREFIXES
    assert "r:cfg:" in _HUB_DEDICATED_PREFIXES
    # Roteamento real: cada prefixo dedicado chega ao SEU handler...
    assert _route_callback("r:quest:q1") == "quest_claim_cb"
    assert _route_callback("r:cfg:hide_rank") == "cfg_cb"
    # ...e o que e do hub continua no hub (bau de recompensa + menu perfil).
    assert _route_callback("r:chest:5") == "hub_cb"
    assert _route_callback("r:perfil") == "hub_cb"
