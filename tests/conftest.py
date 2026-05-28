"""Configuracao de testes.

Aponta DATABASE_PATH pra um arquivo temporario ANTES de qualquer import de
`main`, garantindo que o import (que roda setup_connection + run_migrations no
nivel de modulo) opere num DB vazio descartavel — nunca no DB real do repo.
"""
import os
import tempfile
import pathlib

_TMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="royal-test-"))
os.environ["DATABASE_PATH"] = str(_TMP_DIR / "test.sqlite3")
os.environ.setdefault("TZ", "America/Sao_Paulo")
