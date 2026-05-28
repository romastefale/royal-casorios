"""Smoke test: importar `main` num DB temporario roda as migrations ate o fim
sem corromper schema. Garante que run_migrations aplica todas as versoes numa
base vazia (o que o boot real faz no 1o deploy).
"""
import main


def test_migrations_aplicam_ate_a_ultima_versao():
    v = main.cur.execute("PRAGMA user_version").fetchone()[0]
    assert v >= 13


def test_tabelas_criticas_existem():
    rows = main.cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {r["name"] for r in rows}
    for t in ("players", "couples", "gifts", "achievements",
              "quest_progress", "stars_purchases"):
        assert t in names, f"tabela {t} ausente"


def test_integridade_do_db():
    res = main.cur.execute("PRAGMA integrity_check").fetchone()[0]
    assert res == "ok"
