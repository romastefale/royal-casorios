"""Sistema de classificacao de erros + alerta pro dono.

Objetivo: parar de floodar a DM do dono com dump de log a cada 5min e, no
lugar, mandar DM **somente** quando acontece um erro REALMENTE relevante
(bug de codigo, falha de DB, etc.) — erros transitorios/esperados do
Telegram (rate-limit, callback expirado, msg nao modificada...) NAO geram
DM. Tudo (relevante ou nao) continua no stdout/ring/`backup/logs`.

Grafo de deps (acICLICO): `config <- alerts <- core`. Este modulo e folha:
importa SO de `royal.config` (que e a folha de config) + stdlib. Quem injeta
o `bot`/envio de DM e o `core` via `register_owner_alerts(dm_sender)`.

O catalogo (`ERROR_CATALOG`) e a fonte-de-verdade em codigo do mapa de casos
documentado em `backup/ERROR_CATALOG.md` — manter os dois em sincronia.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from royal.config import (LOG_BACKUP_DIR, OWNER_ALERT_TTL_SEC,
                          OWNER_ALERTS_ENABLED, OWNER_USER_ID, TZ_NAME, logger)


# --- Catalogo de casos -------------------------------------------------------
# Cada entrada: id, title, types (nomes de excecao), subs (substrings, lower),
# relevant (manda DM pro dono?), hint (como corrigir). A 1a entrada que casar
# vence. `types`/`subs` casam contra o nome da excecao e o texto (msg + trace).
ERROR_CATALOG: list[dict] = [
    # --- TRANSITORIOS / ESPERADOS (relevant=False, nao manda DM) ------------
    {
        "id": "flood_control",
        "title": "Flood control / RetryAfter (rate-limit do Telegram)",
        "types": ("TelegramRetryAfter",),
        "subs": ("flood control exceeded", "too many requests",
                 "retry after"),
        "relevant": False,
        "hint": "Rate-limit do Telegram. O codigo ja espera e tenta de novo. "
                "So agir se for CONSTANTE: espacar uploads (identity sweep) "
                "ou reduzir mensagens por segundo.",
    },
    {
        "id": "callback_expired",
        "title": "Callback query expirado (botao clicado, ack tarde demais)",
        "types": (),
        "subs": ("query is too old", "query id is invalid",
                 "response timeout expired"),
        "relevant": False,
        "hint": "O handler demorou > ~15s pra responder o cb.answer(). Benigno "
                "(o usuario so nao ve o spinner sumir). So agir se a lentidao "
                "for cronica (ver duracoes altas no log).",
    },
    {
        "id": "message_edit_noop",
        "title": "Edicao/remocao de mensagem benigna",
        "types": (),
        "subs": ("message is not modified", "message to edit not found",
                 "message to delete not found", "message can't be deleted",
                 "message_id_invalid", "message can't be edited"),
        "relevant": False,
        "hint": "Tentou editar/apagar msg que mudou/sumiu. Esperado no fluxo "
                "de edicao in-place. Nada a corrigir.",
    },
    {
        "id": "user_unreachable",
        "title": "Bot bloqueado / chat inacessivel",
        "types": ("TelegramForbidden",),
        "subs": ("bot was blocked by the user", "user is deactivated",
                 "chat not found", "bot can't initiate conversation",
                 "not enough rights", "have no rights",
                 "bot is not a member"),
        "relevant": False,
        "hint": "Usuario bloqueou o bot / saiu / bot sem permissao. Esperado. "
                "Nao da pra corrigir no codigo.",
    },
    {
        "id": "network_transient",
        "title": "Rede/servidor do Telegram instavel",
        "types": ("TelegramNetworkError", "TelegramServerError",
                  "ClientConnectorError", "ServerDisconnectedError",
                  "TimeoutError", "asyncio.TimeoutError"),
        "subs": ("temporary failure", "bad gateway",
                 "service is unavailable", "connection reset"),
        "relevant": False,
        "hint": "Falha transitoria de rede/Telegram. O polling reconecta "
                "sozinho. So agir se persistir por muito tempo.",
    },
    # --- RELEVANTES (relevant=True, manda DM) -------------------------------
    {
        "id": "code_bug",
        "title": "Bug de codigo (excecao nao tratada)",
        "types": ("NameError", "AttributeError", "KeyError", "IndexError",
                  "TypeError", "ValueError", "UnboundLocalError",
                  "ZeroDivisionError", "AssertionError"),
        "subs": (),
        "relevant": True,
        "hint": "Bug real no codigo. Olhar o traceback (arquivo:linha), "
                "reproduzir e corrigir. Rodar `ruff check --select F` p/ "
                "pegar nomes indefinidos.",
    },
    {
        "id": "import_error",
        "title": "Import/dependencia quebrada",
        "types": ("ImportError", "ModuleNotFoundError"),
        "subs": ("no module named",),
        "relevant": True,
        "hint": "Falta dependencia ou import errado. Conferir requirements.txt "
                "e os imports do modulo citado no trace.",
    },
    {
        "id": "db_error",
        "title": "Erro de banco (SQLite)",
        "types": ("OperationalError", "IntegrityError", "DatabaseError",
                  "ProgrammingError"),
        "subs": ("database is locked", "no such column", "no such table",
                 "unique constraint", "integrity", "disk i/o error",
                 "malformed"),
        "relevant": True,
        "hint": "Problema de schema/dados. Conferir migrations, integrity_check "
                "e se o volume /data esta montado. 'database is locked' => "
                "transacao presa.",
    },
    {
        "id": "boot_migration",
        "title": "Falha de boot / migration",
        "types": (),
        "subs": ("migration", "bootstrap", "integrity_check", "schema"),
        "relevant": True,
        "hint": "Falha critica na subida. Sem isso o bot nao serve. Conferir "
                "logs de boot e o volume /data.",
    },
]


# Conjuntos derivados p/ checagem rapida (excecoes benignas do Telegram).
_BENIGN_TYPES: frozenset[str] = frozenset(
    t for e in ERROR_CATALOG if not e["relevant"] for t in e["types"])
_BENIGN_SUBS: tuple[str, ...] = tuple(
    s for e in ERROR_CATALOG if not e["relevant"] for s in e["subs"])


def is_benign_telegram_error(exc: BaseException) -> bool:
    """True se a excecao for um caso transitorio/esperado (rate-limit,
    callback expirado, msg nao modificada, bot bloqueado, rede). Usado nos
    `except` dos handlers p/ NAO logar como ERROR (evita ruido e alerta a toa)."""
    name = type(exc).__name__
    if name in _BENIGN_TYPES:
        return True
    text = str(exc).lower()
    return any(sub in text for sub in _BENIGN_SUBS)


def _record_text(record: logging.LogRecord) -> str:
    """Mensagem + traceback (se houver) em lower, p/ casar contra o catalogo."""
    parts = [record.getMessage()]
    if record.exc_info:
        try:
            parts.append(logging.Formatter().formatException(record.exc_info))
        except Exception:
            pass
    return "\n".join(parts)


def _exc_type_name(record: logging.LogRecord) -> str | None:
    if record.exc_info and record.exc_info[0] is not None:
        return record.exc_info[0].__name__
    return None


def classify_record(record: logging.LogRecord) -> dict:
    """Classifica um LogRecord de nivel ERROR/CRITICAL.

    Retorna dict: {relevant, id, title, hint, signature}. `relevant=True`
    => merece DM pro dono. Regra: 1a entrada do ERROR_CATALOG que casar
    (por tipo de excecao ou substring) define o veredito; se nada casar,
    e relevante quando ha traceback (excecao real nao tratada) ou nivel
    CRITICAL — senao (logger.error solto sem trace) e tratado como ruido."""
    text = _record_text(record).lower()
    exc_name = _exc_type_name(record)
    signature = (f"{record.module}:{record.lineno}:"
                 f"{exc_name or record.funcName}")

    for entry in ERROR_CATALOG:
        if exc_name and exc_name in entry["types"]:
            return {"relevant": entry["relevant"], "id": entry["id"],
                    "title": entry["title"], "hint": entry["hint"],
                    "signature": signature}
        if entry["subs"] and any(sub in text for sub in entry["subs"]):
            return {"relevant": entry["relevant"], "id": entry["id"],
                    "title": entry["title"], "hint": entry["hint"],
                    "signature": signature}

    # Sem match no catalogo: excecao real (tem trace) ou CRITICAL => relevante.
    relevant = bool(record.exc_info) or record.levelno >= logging.CRITICAL
    return {
        "relevant": relevant,
        "id": "uncategorized",
        "title": "Erro nao catalogado",
        "hint": ("Erro com traceback nao previsto no catalogo. Investigar o "
                 "trace; se for recorrente, adicionar entrada no ERROR_CATALOG."
                 if relevant else
                 "logger.error sem traceback e fora do catalogo — tratado como "
                 "ruido (sem DM)."),
        "signature": signature,
    }


class OwnerAlertHandler(logging.Handler):
    """logging.Handler (nivel ERROR) que classifica cada erro e, se for
    relevante, (1) grava um relatorio em `backup/alerts/` e (2) agenda uma DM
    concisa pro dono — com dedupe por assinatura + TTL p/ nao floodar.

    O envio de DM e injetado (`dm_sender`, coroutine) pelo core, que tem o
    `bot`. emit() e sincrono: agenda a DM no event loop ativo (se houver)."""

    def __init__(self, dm_sender, *, alerts_dir: str,
                 ttl_sec: int = 1800) -> None:
        super().__init__(level=logging.ERROR)
        self._dm_sender = dm_sender
        self._alerts_dir = alerts_dir
        self._ttl = ttl_sec
        self._last_sent: dict[str, float] = {}

    def _now_stamp(self) -> str:
        try:
            return datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def _build_report(self, record: logging.LogRecord, info: dict) -> str:
        lines = [
            f"[{self._now_stamp()}] {record.levelname} {record.name}",
            f"caso: {info['title']} (id={info['id']})",
            f"assinatura: {info['signature']}",
            f"como corrigir: {info['hint']}",
            "",
            "mensagem:",
            record.getMessage(),
        ]
        if record.exc_info:
            try:
                lines.append("")
                lines.append("traceback:")
                lines.append(
                    logging.Formatter().formatException(record.exc_info))
            except Exception:
                pass
        return "\n".join(lines)

    def _write_file(self, report: str, info: dict) -> None:
        try:
            os.makedirs(self._alerts_dir, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
            path = os.path.join(self._alerts_dir, f"{ts}-{info['id']}.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(report)
        except Exception:
            pass  # alerta nunca pode derrubar o logging

    def _schedule_dm(self, text: str) -> None:
        if not OWNER_USER_ID or self._dm_sender is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # sem loop ativo (import/migration) — fica so o arquivo
        try:
            loop.create_task(self._dm_sender(text))
        except Exception:
            pass

    def emit(self, record: logging.LogRecord) -> None:
        try:
            info = classify_record(record)
            if not info["relevant"]:
                return
            # Arquivo SEMPRE (observabilidade). OWNER_ALERTS_ENABLED so controla a DM.
            report = self._build_report(record, info)
            self._write_file(report, info)
            if not OWNER_ALERTS_ENABLED:
                return
            now = time.time()
            sig = info["signature"]
            last = self._last_sent.get(sig, 0.0)
            if now - last < self._ttl:
                return  # ja avisei esse erro ha pouco — dedupe
            self._last_sent[sig] = now
            dm = (f"\U0001f6a8 RPG bot — erro relevante\n"
                  f"{info['title']}\n"
                  f"\u2022 {info['signature']}\n"
                  f"\u2022 {record.getMessage()[:300]}\n\n"
                  f"\U0001f527 {info['hint']}\n"
                  f"(detalhe em backup/alerts/; proximos iguais silenciados "
                  f"por {self._ttl // 60}min)")
            self._schedule_dm(dm)
        except Exception:
            self.handleError(record)


def register_owner_alerts(dm_sender, *, alerts_dir: str | None = None,
                          ttl_sec: int | None = None) -> OwnerAlertHandler:
    """Cria o OwnerAlertHandler e anexa no root logger (captura tudo: nossos
    logger.exception + as excecoes nao tratadas que o aiogram loga como ERROR).
    Idempotente: remove instancia anterior antes de anexar."""
    root = logging.getLogger()
    for h in list(root.handlers):
        if isinstance(h, OwnerAlertHandler):
            root.removeHandler(h)
    handler = OwnerAlertHandler(
        dm_sender,
        alerts_dir=alerts_dir or os.path.join(LOG_BACKUP_DIR, "alerts"),
        ttl_sec=ttl_sec if ttl_sec is not None else OWNER_ALERT_TTL_SEC)
    root.addHandler(handler)
    logger.info("[ALERTS] owner-alert handler ON (dir=%s ttl=%ds enabled=%s)",
                handler._alerts_dir, handler._ttl, OWNER_ALERTS_ENABLED)
    return handler
