import asyncio
import html
import io
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

from cachetools import TTLCache

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import (TelegramBadRequest, TelegramNetworkError,
                                 TelegramRetryAfter, TelegramServerError)
from aiogram.filters import (
    Command,
    CommandStart,
    Filter,
    JOIN_TRANSITION,
    ChatMemberUpdatedFilter,
)
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllChatAdministrators,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    BufferedInputFile,
    CallbackQuery,
    ChatMemberUpdated,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultCachedPhoto,
    InlineQueryResultsButton,
    InputTextMessageContent,
    KeyboardButton,
    Message,
    MessageReactionUpdated,
    ReactionTypeEmoji,
    ReplyKeyboardMarkup,
)

import royal_avatars
from royal_words import PALAVRAS
from royal_render import (
    BossKillAttacker,
    BossKillData,
    CasorioPartner,
    ClasseCardData,
    ConquistasData,
    EventoData,
    IdentityCardData,
    LojaDropData,
    MissaoRow,
    MissoesData,
    ProfileCardData,
    RankingEntry,
    render_boss_kill_card,
    render_casorio_card,
    render_boss_status_card,
    render_casorios_ranking_card,
    render_classe_card,
    render_conquistas_card,
    render_evento_card,
    render_identity_card,
    render_inventario_card,
    render_levelup_card,
    render_loja_card,
    render_loja_drop_card,
    render_meuscasorios_card,
    render_missoes_card,
    render_palavra_active_card,
    render_palavra_spoiler_card,
    render_profile_card,
    render_shipper_card,
    render_ranking_card,
    render_saldo_card,
    BossStatusData,
    CouplePodiumEntry,
    InventarioData,
    InventarioSlot,
    LojaData,
    LojaSlot,
    MeusCasoriosData,
    PalavraActiveData,
    PartnerMini,
    SaldoData,
    ShipperData,
)
import hashlib
from aiogram import Router


LOG_JSON = os.getenv("LOG_JSON", "0") == "1"


_HUMAN_FMT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


class _JsonFormatter(logging.Formatter):
    """Formatter compacto: 1 linha JSON por record. Compativel com
    qualquer log aggregator. Inclui exception info quando houver."""
    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "lvl": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # Permite logger.info("...", extra={"uid": 123, ...}) → vira top-level.
        for k, v in record.__dict__.items():
            if k in ("args", "asctime", "created", "exc_info", "exc_text",
                     "filename", "funcName", "levelname", "levelno",
                     "lineno", "message", "module", "msecs", "msg",
                     "name", "pathname", "process", "processName",
                     "relativeCreated", "stack_info", "thread",
                     "threadName", "taskName"):
                continue
            try:
                json.dumps(v)
                payload[k] = v
            except Exception:
                payload[k] = repr(v)
        try:
            return json.dumps(payload, ensure_ascii=False)
        except Exception:
            return f'{{"lvl":"{record.levelname}","msg":"<unserializable>"}}'


_root_handler = logging.StreamHandler(sys.stdout)


_root_handler.setFormatter(_JsonFormatter() if LOG_JSON
                            else logging.Formatter(_HUMAN_FMT))


logging.basicConfig(level=logging.INFO, handlers=[_root_handler], force=True)


logger = logging.getLogger("royal-casorios")


class _LogRingBuffer(logging.Handler):
    """Handler que mantem em memoria as ultimas N linhas formatadas
    (sempre no formato humano, mesmo com LOG_JSON ligado — o ring eh
    consumido por humanos via /royallog na DM)."""
    def __init__(self, maxlen: int = 5000) -> None:
        super().__init__(level=logging.INFO)
        self.buffer: deque[str] = deque(maxlen=maxlen)
        self.setFormatter(logging.Formatter(_HUMAN_FMT))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.buffer.append(self.format(record))
        except Exception:
            pass

    def snapshot(self) -> str:
        return "\n".join(self.buffer)


_log_ring = _LogRingBuffer(maxlen=5000)


logging.getLogger().addHandler(_log_ring)  # root: captura aiogram + nosso


BOT_TOKEN = os.getenv("BOT_TOKEN")


TEST_CHAT_IDS: set[int] = {
    int(x.strip())
    for x in os.getenv("TEST_CHAT_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
}


_STASH_DEFAULT = -1003941532741


_stash_raw = os.getenv("STASH_CHAT_ID", "").strip()


STASH_CHAT_ID: int | None = (
    int(_stash_raw) if _stash_raw.lstrip("-").isdigit() else _STASH_DEFAULT
)


_owner_raw = os.getenv("OWNER_USER_ID", "").strip()


OWNER_USER_ID: int | None = int(_owner_raw) if _owner_raw.lstrip("-").isdigit() else None


LOG_DUMP_INTERVAL_SEC = int(os.getenv("LOG_DUMP_INTERVAL_SEC", "300"))


LOG_DUMP_ENABLED = os.getenv("LOG_DUMP_ENABLED", "1").strip() not in ("0", "false", "False")


GH_LOG_TOKEN = (os.getenv("GH_TOKEN", "").strip()
                or os.getenv("GITHUB_TOKEN", "").strip())


LOG_GIST_ID_KEY = "log_dump_gist_id"


# Raiz dos snapshots de log + relatorios de alerta. No Railway (container
# efemero) aponte pro volume p/ persistir: LOG_BACKUP_DIR=/data/backup.
LOG_BACKUP_DIR = os.getenv("LOG_BACKUP_DIR", "backup").strip() or "backup"


# Quantos snapshots de log manter em backup/logs (rotacao).
LOG_BACKUP_KEEP = int(os.getenv("LOG_BACKUP_KEEP", "50"))


# Liga/desliga as DMs de alerta pro dono (os arquivos sao gravados de todo jeito).
OWNER_ALERTS_ENABLED = os.getenv("OWNER_ALERTS_ENABLED", "1").strip() not in ("0", "false", "False")


# Janela de dedupe por assinatura de erro (s): o mesmo erro nao re-alerta dentro dela.
OWNER_ALERT_TTL_SEC = int(os.getenv("OWNER_ALERT_TTL_SEC", "1800"))


DB_PATH = os.getenv("DATABASE_PATH", "./data/royal_casorios.sqlite3")


TZ_NAME = os.getenv("TZ", "America/Sao_Paulo")


AUTO_HOURS = [int(x.strip()) for x in os.getenv("AUTO_HOURS", "9,15,21").split(",") if x.strip()]


# === Inteligência royal — ponte com a IA @Mira ===
# A @Mira é uma conta SEPARADA que o dono mantém (a IA roda do lado dela; o jogo
# só SOLICITA e INGERE conteúdo → custo zero, nenhuma IA paga no jogo). A captura
# da resposta é por REPLY ao nosso pedido (modelo TR3), e o grupo-ponte está na
# allowlist da outer-middleware (core.py) p/ a resposta não ser dropada como
# is_bot. (O "Bot-to-Bot Communication Mode" do @BotFather NÃO é o lever aqui.)
# @username da @Mira (sem o @). Default = "Mira" (ponte LIGADA por padrão). Vazio
# explícito (MIRA_USERNAME="") desliga a feature. Também é usado p/ ENDEREÇAR o
# pedido no grupo-ponte (texto "@username <prompt>").
MIRA_USERNAME = os.getenv("MIRA_USERNAME", "Mira").strip().lstrip("@")


# user_id da @Mira. Mantido por compatibilidade de env e diagnóstico (aparece no
# log "[MIRA] bridge msg"), mas o match da resposta NÃO depende mais dele: a
# correlação é por REPLY ao nosso pedido (modelo TR3, ver royal/mira.py). Default
# = ID conhecido da @Mira; vazio/inválido = usa o ID default.
_mira_uid_raw = os.getenv("MIRA_USER_ID", "").strip()
MIRA_USER_ID: int | None = (
    int(_mira_uid_raw) if _mira_uid_raw.lstrip("-").isdigit() else 8377231659
)


# chat_id do grupo-ponte onde o jogo fala com a @Mira. Default = supergrupo
# atual (prefixo -100). Se mudar de novo, o id muda → on_chat_migration/safe_send
# se auto-curam (mas o bridge não guarda dados de jogo, então é só re-rota).
_mira_bridge_raw = os.getenv("IA_BRIDGE_CHAT_ID", "").strip()


IA_BRIDGE_CHAT_ID: int | None = (
    int(_mira_bridge_raw) if _mira_bridge_raw.lstrip("-").isdigit() else -1003624946383
)


# Ponte ativa só se temos username + grupo-ponte.
MIRA_ENABLED = bool(MIRA_USERNAME and IA_BRIDGE_CHAT_ID)


# Hora local (0-23) do pedido diário de "palavras do dia" à @Mira.
MIRA_PALAVRAS_HOUR = int(os.getenv("MIRA_PALAVRAS_HOUR", "6"))


# Quantas palavras pedir por dia.
MIRA_PALAVRAS_COUNT = int(os.getenv("MIRA_PALAVRAS_COUNT", "40"))


# Quanto esperar (s) pela resposta da @Mira antes de desistir do pedido.
MIRA_REQUEST_TIMEOUT_SEC = int(os.getenv("MIRA_REQUEST_TIMEOUT_SEC", "180"))


# Quantas das últimas palavras usadas no grupo evitar repetir na escolha.
PALAVRA_NO_REPEAT_RECENT = int(os.getenv("PALAVRA_NO_REPEAT_RECENT", "30"))


RECENT_WINDOW_SECONDS = 180


FLUSH_INTERVAL_SECONDS = 20


MIN_PAIR_SCORE = 5


ADMIN_CACHE_TTL_SECONDS = 300


PHOTO_CACHE_TTL_SECONDS = 86400


XP_PER_MESSAGE = 2


XP_PER_REPLY = 5


XP_COOLDOWN_MSG_SECONDS = 60


XP_COOLDOWN_REPLY_SECONDS = 30


MUSIC_BOT_ID = 8589834936


MUSIC_BOT_XP_MULTIPLIER = 3


MUSIC_BOT_REACTION = "👾"  # whitelist oficial do Telegram (✨ NAO esta na lista → REACTION_INVALID)


XP_PALAVRA_MIN = 30


XP_PALAVRA_MAX = 75


XP_PALAVRA_WIN_BONUS = 150


XP_PALAVRA_CONSOLATION = 5


XP_COUPLE_FORMED = 25


XP_VOTE_LIKE = 2


XP_BOSS_HIT = 2


STARTING_GOLD = 50


GOLD_PALAVRA_WIN = 50


GOLD_BOSS_KILL_TOTAL = 500


COUPLE_XP_BUFF = 0.10  # +10% XP enquanto casado


REACTION_XP = 3


REACTION_XP_DAILY_CAP = 10


DAILY_QUESTS = [
    {"id": "msgs", "icon": "💬",
     "desc": "Mande 20 mensagens no reino",
     "target": 20, "xp": 60, "gold": 30, "event": "message"},
    {"id": "palavra", "icon": "🎯",
     "desc": "Acerte 1 PALAVRA da Hora",
     "target": 1, "xp": 80, "gold": 50, "event": "palavra_win"},
    {"id": "boss", "icon": "🐉",
     "desc": "Acerte o Boss da Semana 3x",
     "target": 3, "xp": 50, "gold": 40, "event": "boss_hit"},
    {"id": "social", "icon": "👍",
     "desc": "Reaja a 5 mensagens",
     "target": 5, "xp": 30, "gold": 20, "event": "reaction"},
]


DAILY_QUESTS_BY_ID = {q["id"]: q for q in DAILY_QUESTS}


SEASONAL_EVENTS = [
    {"id": "reveillon", "label": "🎆 Reveillon Real",
     "start": (12, 31), "end": (1, 1), "xp_mult": 2.0},
    {"id": "natal", "label": "🎄 Natal dos Nobres",
     "start": (12, 24), "end": (12, 25), "xp_mult": 2.0},
    {"id": "sao_joao", "label": "🔥 Festa Junina Real",
     "start": (6, 23), "end": (6, 24), "xp_mult": 1.5},
    {"id": "halloween", "label": "🎃 Noite Sombria",
     "start": (10, 31), "end": (10, 31), "xp_mult": 1.5},
    {"id": "dia_namorados", "label": "❤️ Dia dos Namorados",
     "start": (6, 12), "end": (6, 12), "xp_mult": 1.5},
]


ATTR_START = 5


PTS_PER_LEVEL = 3


PALAVRA_DURATIONS_MIN = [5, 7, 10]


PALAVRA_JITTER_SEC = 60


PALAVRA_ATTEMPT_COOLDOWN_SEC = 3


LUCKY_EMOJI = "🎰"


LUCKY_SLOT_WINS = {1, 22, 43, 64}


LUCKY_SLOT_JACKPOT = 64


QUIZ_XP_PER_POINT = 15


LUCKY_WIN_XP = 20


LUCKY_WIN_GOLD = 30


LUCKY_JACKPOT_XP = 100


LUCKY_JACKPOT_GOLD = 200


LUCKY_DAILY_CAP = 5


def lucky_reward_for(value: int | None) -> tuple[int, int, bool]:
    """Recompensa (xp, gold, jackpot) p/ um valor de slot 🎰 do Telegram.
    Só TRINCAS (3 iguais) premiam; jackpot (7️⃣7️⃣7️⃣ = 64) paga em dobro.
    Retorna (0, 0, False) p/ qualquer valor que não seja trinca (e p/ None)."""
    if value == LUCKY_SLOT_JACKPOT:
        return LUCKY_JACKPOT_XP, LUCKY_JACKPOT_GOLD, True
    if value in LUCKY_SLOT_WINS:
        return LUCKY_WIN_XP, LUCKY_WIN_GOLD, False
    return 0, 0, False


CHEST_DELAY_MIN = 30


CHEST_TTL_MIN = 30


CHEST_MAX_CLAIMS = 5


CHEST_REWARDS = [(150, 10), (100, 10), (75, 10), (50, 10), (25, 10)]


BOSS_SPAWN_WEEKDAY = 6  # 6 = Domingo (Mon=0..Sun=6)


BOSS_SPAWN_HOUR = 20


BOSS_ATTACK_COOLDOWN_SEC = 300


