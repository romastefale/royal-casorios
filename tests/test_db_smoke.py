"""Smoke test: importar `main` num DB temporario roda as migrations ate o fim
sem corromper schema. Garante que run_migrations aplica todas as versoes numa
base vazia (o que o boot real faz no 1o deploy).
"""
import main


def test_migrations_aplicam_ate_a_ultima_versao():
    v = main.cur.execute("PRAGMA user_version").fetchone()[0]
    assert v >= 14


def test_tabelas_criticas_existem():
    rows = main.cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {r["name"] for r in rows}
    for t in ("players", "couples", "gifts", "achievements",
              "quest_progress", "stars_purchases", "lucky_emoji_daily"):
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
