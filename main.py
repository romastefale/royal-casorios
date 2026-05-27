"""
Royal RPG — Telegram bot (aiogram 3.28)
========================================

Bot completo de RPG do grupo Royal:
- Shipper original (casamentos automaticos) preservado
- Sistema RPG: Royal ID, perfil, XP, atributos, classes, economia
- Mini-game Palavra da Hora
- Boss semanal cooperativo
- Temporadas com Hall da Fama

Roda como worker (sem frontend) em Railway com volume SQLite alocado.
"""

import asyncio
import html
import logging
import os
import random
import sqlite3
import unicodedata
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from royal_words import PALAVRAS, CHARADAS

# =====================================================================
# CONFIG & LOGGING
# =====================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("royal-casorios")

BOT_TOKEN = os.getenv("BOT_TOKEN")

DB_PATH = os.getenv("DATABASE_PATH", "./data/royal_casorios.sqlite3")
TZ_NAME = os.getenv("TZ", "America/Sao_Paulo")
AUTO_HOURS = [int(x.strip()) for x in os.getenv("AUTO_HOURS", "9,15,21").split(",") if x.strip()]

# Tempo / janelas
RECENT_WINDOW_SECONDS = 180
FLUSH_INTERVAL_SECONDS = 20
MIN_PAIR_SCORE = 5
ADMIN_CACHE_TTL_SECONDS = 300
PHOTO_CACHE_TTL_SECONDS = 86400

# XP (meio termo)
XP_PER_MESSAGE = 2
XP_PER_REPLY = 5
XP_COOLDOWN_MSG_SECONDS = 60
XP_COOLDOWN_REPLY_SECONDS = 30
XP_PALAVRA_MIN = 30
XP_PALAVRA_MAX = 75
XP_PALAVRA_WIN_BONUS = 150
XP_PALAVRA_CONSOLATION = 5
XP_COUPLE_FORMED = 25
XP_VOTE_LIKE = 2
XP_BOSS_HIT = 2

# Economia
STARTING_GOLD = 50
GOLD_PALAVRA_WIN = 50
GOLD_BOSS_KILL_TOTAL = 500
COUPLE_XP_BUFF = 0.10  # +10% XP enquanto casado

# Atributos
ATTR_START = 5
PTS_PER_LEVEL = 3

# Palavra da Hora
PALAVRA_DURATIONS_MIN = [5, 7, 10]
PALAVRA_JITTER_SEC = 60
PALAVRA_ATTEMPT_COOLDOWN_SEC = 3

# Boss
BOSS_SPAWN_WEEKDAY = 6  # 6 = Domingo (Mon=0..Sun=6)
BOSS_SPAWN_HOUR = 20
BOSS_ATTACK_COOLDOWN_SEC = 300

# =====================================================================
# BOT & DISPATCHER (deferred init)
# =====================================================================

bot: Bot | None = None
dp = Dispatcher()

# =====================================================================
# DB CONNECTION + PRAGMA SETUP (per-connection PRAGMAs ALWAYS applied)
# =====================================================================

DB_DIR = os.path.dirname(DB_PATH) or "."
os.makedirs(DB_DIR, exist_ok=True)

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
cur = db.cursor()


def setup_connection() -> None:
    cur.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA busy_timeout=5000;
        """
    )


setup_connection()


# =====================================================================
# MIGRATIONS (PRAGMA user_version)
# =====================================================================

def migrate_to_v1(c: sqlite3.Cursor) -> None:
    """Schema do shipper original."""
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER, chat_id INTEGER,
            display_name TEXT, username TEXT,
            opt_out INTEGER DEFAULT 0,
            message_count INTEGER DEFAULT 0,
            last_seen TEXT,
            PRIMARY KEY(user_id, chat_id)
        );
        CREATE TABLE IF NOT EXISTS daily_activity (
            chat_id INTEGER, user_id INTEGER, day TEXT,
            message_count INTEGER DEFAULT 0,
            PRIMARY KEY(chat_id, user_id, day)
        );
        CREATE TABLE IF NOT EXISTS pair_scores (
            chat_id INTEGER, user1 INTEGER, user2 INTEGER,
            score INTEGER DEFAULT 0, last_seen TEXT,
            PRIMARY KEY(chat_id, user1, user2)
        );
        CREATE TABLE IF NOT EXISTS couples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER, user1 INTEGER, user2 INTEGER,
            source TEXT DEFAULT 'auto', created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS votes (
            couple_id INTEGER, voter_id INTEGER, type TEXT, created_at TEXT,
            PRIMARY KEY(couple_id, voter_id)
        );
        CREATE TABLE IF NOT EXISTS chats (
            chat_id INTEGER PRIMARY KEY, title TEXT,
            enabled INTEGER DEFAULT 1, last_auto_post TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_users_chat_seen ON users(chat_id, last_seen);
        CREATE INDEX IF NOT EXISTS idx_daily_activity_chat_day ON daily_activity(chat_id, day);
        CREATE INDEX IF NOT EXISTS idx_pair_scores_chat_score ON pair_scores(chat_id, score DESC);
        CREATE INDEX IF NOT EXISTS idx_couples_chat_created ON couples(chat_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_votes_couple ON votes(couple_id);
        """
    )


def migrate_to_v2(c: sqlite3.Cursor) -> None:
    """Schema do RPG: players, inventario, desafios, bosses, temporadas."""
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS players (
            chat_id INTEGER, user_id INTEGER,
            royal_id TEXT,
            class_id TEXT,
            total_xp INTEGER DEFAULT 0,
            season_xp INTEGER DEFAULT 0,
            attr_for INTEGER DEFAULT 5,
            attr_des INTEGER DEFAULT 5,
            attr_vit INTEGER DEFAULT 5,
            attr_car INTEGER DEFAULT 5,
            pts_available INTEGER DEFAULT 0,
            gold INTEGER DEFAULT 50,
            rpg_message_count INTEGER DEFAULT 0,
            privacy_hide_ranking INTEGER DEFAULT 0,
            privacy_hide_stats INTEGER DEFAULT 0,
            joined_at TEXT,
            last_xp_msg_at TEXT,
            last_xp_reply_at TEXT,
            PRIMARY KEY(chat_id, user_id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_players_chat_royal_id
            ON players(chat_id, royal_id) WHERE royal_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_players_season_xp
            ON players(chat_id, season_xp DESC);

        CREATE TABLE IF NOT EXISTS royal_id_seq (
            chat_id INTEGER PRIMARY KEY,
            next_id INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS inventory (
            chat_id INTEGER, user_id INTEGER, item_id TEXT,
            qty INTEGER DEFAULT 1,
            equipped INTEGER DEFAULT 0,
            PRIMARY KEY(chat_id, user_id, item_id)
        );

        CREATE TABLE IF NOT EXISTS challenges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            type TEXT,
            word TEXT,
            display TEXT,
            hint TEXT,
            started_at TEXT,
            ends_at TEXT,
            duration_min INTEGER,
            winner_user_id INTEGER,
            winner_ms INTEGER,
            attempts_count INTEGER DEFAULT 0,
            message_id INTEGER,
            status TEXT DEFAULT 'open'
        );
        CREATE INDEX IF NOT EXISTS idx_challenges_chat_status
            ON challenges(chat_id, status);

        CREATE TABLE IF NOT EXISTS bosses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            name TEXT,
            hp INTEGER,
            max_hp INTEGER,
            spawned_at TEXT,
            killed_at TEXT,
            message_id INTEGER,
            week_marker TEXT,
            status TEXT DEFAULT 'alive'
        );
        CREATE INDEX IF NOT EXISTS idx_bosses_chat_status
            ON bosses(chat_id, status);

        CREATE TABLE IF NOT EXISTS boss_hits (
            boss_id INTEGER,
            user_id INTEGER,
            damage INTEGER,
            ts TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_boss_hits_boss ON boss_hits(boss_id);

        CREATE TABLE IF NOT EXISTS chats_rpg (
            chat_id INTEGER PRIMARY KEY,
            next_palavra_at TEXT,
            current_season TEXT
        );

        CREATE TABLE IF NOT EXISTS season_hall (
            chat_id INTEGER,
            season_code TEXT,
            rank INTEGER,
            royal_id TEXT,
            user_id INTEGER,
            display_name TEXT,
            season_xp INTEGER,
            snapshot_at TEXT,
            PRIMARY KEY(chat_id, season_code, rank)
        );
        """
    )


MIGRATIONS = [
    (1, migrate_to_v1),
    (2, migrate_to_v2),
]


def run_migrations() -> None:
    current = cur.execute("PRAGMA user_version").fetchone()[0]
    for version, fn in MIGRATIONS:
        if current < version:
            logger.info("Applying migration v%d", version)
            fn(cur)
            cur.execute(f"PRAGMA user_version = {version}")
            db.commit()
    final = cur.execute("PRAGMA user_version").fetchone()[0]
    logger.info("Database schema at version %d", final)


run_migrations()


# =====================================================================
# IN-MEMORY STATE
# =====================================================================

pair_buffer: dict[tuple[int, int, int], int] = defaultdict(int)
activity_buffer: dict[tuple[int, int, str], int] = defaultdict(int)
recent_messages: dict[int, deque] = defaultdict(lambda: deque(maxlen=120))

admin_cache: dict[tuple[int, int], tuple[bool, float]] = {}
photo_cache: dict[int, tuple[str | None, float]] = {}
# (challenge_id, user_id) -> last_ts
attempt_cooldowns: dict[tuple[int, int], float] = {}
# (boss_id, user_id) -> last_ts
boss_attack_cooldowns: dict[tuple[int, int], float] = {}


# =====================================================================
# CONSTANTES DE UI
# =====================================================================

private_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="👑 Reino")],
        [KeyboardButton(text="📊 Meus casórios"), KeyboardButton(text="❓ Como funciona")],
    ],
    resize_keyboard=True,
)

ROMANTICAS = [
    "O destino nunca erra quando decide unir dois corações 💫",
    "Algo no ar diz que isso não é coincidência ❤️",
    "Quando duas almas se encontram, o universo conspira ✨",
    "Tem conexão aí que nem o tempo explica ⏳❤️",
    "Parece que o coração falou mais alto hoje 💘",
    "Isso aqui tá com cara de história que começa agora 📖❤️",
    "O acaso só existe até o destino aparecer 💫",
    "Dois caminhos que finalmente se cruzaram ❤️",
    "Tem química que não dá pra ignorar 🔥❤️",
    "O universo deu um empurrãozinho aqui 👀💘",
]

CASAL_FRASES = [
    "Parece que isso aqui já tava escrito 😏",
    "Olha… isso aqui faz sentido 👀",
    "Não é aleatório não, hein… 💭",
    "Tá com cara de casal mesmo 🔥",
    "Isso aqui pode dar problema… ou dar muito certo 😳",
    "O grupo percebeu algo aqui 👁️",
    "Tem história começando aqui 📖",
    "Isso aqui tá diferente… 👀",
    "Alguém explica isso? 😏",
    "Isso aqui não foi por acaso 💘",
]

CLASSES: dict[str, dict] = {
    "monarca":   {"emoji": "👑", "name": "Monarca",    "bonus": "+20% HP base"},
    "cavaleiro": {"emoji": "🗡️", "name": "Cavaleiro",  "bonus": "+2 FOR"},
    "cortesa":   {"emoji": "🌹", "name": "Cortesã",    "bonus": "+2 CAR"},
    "bruxo":     {"emoji": "🧙", "name": "Bruxo",      "bonus": "+2 DES"},
    "cronista":  {"emoji": "📜", "name": "Cronista",   "bonus": "+10% XP"},
    "bobo":      {"emoji": "🗝️", "name": "Bobo",       "bonus": "+50% ouro"},
}

# Loja: itens consumiveis + equipaveis
ITEMS: dict[str, dict] = {
    "pocao":   {"emoji": "🧪", "name": "Poção de Vigor",     "price": 50,  "type": "consumable",
                "desc": "Restaura HP completo",
                "effect": "heal"},
    "espada":  {"emoji": "🗡️", "name": "Espada de Ferro",    "price": 200, "type": "equip",
                "desc": "+3 FOR enquanto equipada",
                "stat": "for", "bonus": 3},
    "armadura":{"emoji": "🛡️", "name": "Armadura de Couro",   "price": 200, "type": "equip",
                "desc": "+15 HP máximo",
                "stat": "vit", "bonus": 2},
    "botas":   {"emoji": "🥾", "name": "Botas Ágeis",         "price": 150, "type": "equip",
                "desc": "+2 DES enquanto equipadas",
                "stat": "des", "bonus": 2},
    "anel":    {"emoji": "💍", "name": "Anel da Corte",       "price": 250, "type": "equip",
                "desc": "+2 CAR enquanto equipado",
                "stat": "car", "bonus": 2},
    "tomo":    {"emoji": "📚", "name": "Tomo de Sabedoria",   "price": 300, "type": "consumable",
                "desc": "Concede 100 XP imediatamente",
                "effect": "xp100"},
    "coroa":   {"emoji": "👑", "name": "Coroa Decorativa",    "price": 500, "type": "cosmetic",
                "desc": "Cosmético, mostra status no perfil"},
}

# Temporadas (hemisferio sul)
SEASONS = [
    ("primavera", "🌸 Primavera", (9, 22),  (12, 20)),
    ("verao",     "☀️ Verão",     (12, 21), (3, 19)),
    ("outono",    "🍂 Outono",    (3, 20),  (6, 20)),
    ("inverno",   "❄️ Inverno",   (6, 21),  (9, 21)),
]


# =====================================================================
# HELPERS DE TEMPO
# =====================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso() -> str:
    return utc_now().isoformat()


def local_now() -> datetime:
    return datetime.now(ZoneInfo(TZ_NAME))


def today_key() -> str:
    return local_now().date().isoformat()


def current_season_code(d: date | None = None) -> str:
    """Retorna codigo da estacao atual no hemisferio sul."""
    if d is None:
        d = local_now().date()
    md = (d.month, d.day)
    # verao cruza ano: 21/12 ate 19/03
    if md >= (12, 21) or md <= (3, 19):
        # season_code inclui o ano que TERMINA a estacao
        year = d.year + 1 if md >= (12, 21) else d.year
        return f"verao-{year}"
    if (3, 20) <= md <= (6, 20):
        return f"outono-{d.year}"
    if (6, 21) <= md <= (9, 21):
        return f"inverno-{d.year}"
    return f"primavera-{d.year}"


def current_season_label() -> str:
    code = current_season_code()
    name, year = code.split("-")
    mapping = {"primavera": "🌸 Primavera", "verao": "☀️ Verão",
               "outono": "🍂 Outono", "inverno": "❄️ Inverno"}
    return f"{mapping[name]} {year}"


def current_week_marker(d: datetime | None = None) -> str:
    """ISO week marker p/ idempotencia do boss semanal."""
    if d is None:
        d = local_now()
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


# =====================================================================
# HELPERS COMUNS
# =====================================================================

def display_name(message: Message) -> str:
    user = message.from_user
    if not user:
        return "Usuário"
    return user.full_name or user.first_name or "Usuário"


def mention(user_id: int, name: str) -> str:
    return f'<a href="tg://user?id={user_id}">{html.escape(name or "Usuário")}</a>'


def normalize_pair(u1: int, u2: int) -> tuple[int, int]:
    return (u1, u2) if u1 < u2 else (u2, u1)


def is_group(message: Message) -> bool:
    return message.chat.type in {"group", "supergroup"}


def normalize_word(s: str) -> str:
    """lowercase, sem acento, sem espaços extras, só alfanumerico."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s.lower().strip())
    return "".join(c for c in s if not unicodedata.combining(c) and c.isalnum())


def ensure_chat(chat_id: int, title: str | None = None) -> None:
    cur.execute(
        """
        INSERT INTO chats (chat_id, title, enabled)
        VALUES (?, ?, 1)
        ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, enabled=1
        """,
        (chat_id, title),
    )


def upsert_user(chat_id: int, user_id: int, name: str, username: str | None) -> None:
    cur.execute(
        """
        INSERT INTO users (user_id, chat_id, display_name, username, message_count, last_seen)
        VALUES (?, ?, ?, ?, 1, ?)
        ON CONFLICT(user_id, chat_id) DO UPDATE SET
            display_name=excluded.display_name,
            username=excluded.username,
            message_count=users.message_count + 1,
            last_seen=excluded.last_seen
        """,
        (user_id, chat_id, name, username, utc_iso()),
    )


def get_name(chat_id: int, user_id: int) -> str:
    cur.execute("SELECT display_name FROM users WHERE chat_id=? AND user_id=?",
                (chat_id, user_id))
    row = cur.fetchone()
    return row["display_name"] if row else str(user_id)


# =====================================================================
# SHIPPER (legado, preservado)
# =====================================================================

def vote_keyboard(couple_id: int, likes: int = 0, dislikes: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"❤️ Apoio {likes}",  callback_data=f"ship_like:{couple_id}"),
        InlineKeyboardButton(text=f"🤮 Ciúmes {dislikes}", callback_data=f"ship_dislike:{couple_id}"),
    ]])


def get_votes(couple_id: int) -> tuple[int, int]:
    cur.execute("SELECT type, COUNT(*) AS total FROM votes WHERE couple_id=? GROUP BY type",
                (couple_id,))
    totals = {row["type"]: row["total"] for row in cur.fetchall()}
    return totals.get("like", 0), totals.get("dislike", 0)


def pair_recently_used(chat_id: int, u1: int, u2: int) -> bool:
    cutoff = (utc_now() - timedelta(hours=72)).isoformat()
    cur.execute(
        "SELECT 1 FROM couples WHERE chat_id=? AND user1=? AND user2=? AND created_at>=? LIMIT 1",
        (chat_id, u1, u2, cutoff),
    )
    return cur.fetchone() is not None


def user_is_available(chat_id: int, user_id: int) -> bool:
    cutoff = (utc_now() - timedelta(hours=48)).isoformat()
    cur.execute(
        "SELECT 1 FROM users WHERE chat_id=? AND user_id=? AND opt_out=0 AND last_seen>=? LIMIT 1",
        (chat_id, user_id, cutoff),
    )
    return cur.fetchone() is not None


def pick_couple(chat_id: int) -> tuple[int, int] | None:
    """Escolhe melhor par, com boost por CAR dos jogadores do RPG."""
    cur.execute(
        """
        SELECT ps.user1, ps.user2, ps.score,
               COALESCE(SUM(CASE WHEN v.type='like' THEN 1 ELSE 0 END), 0) AS likes,
               COALESCE(SUM(CASE WHEN v.type='dislike' THEN 1 ELSE 0 END), 0) AS dislikes,
               COALESCE(p1.attr_car, 0) AS car1,
               COALESCE(p2.attr_car, 0) AS car2
        FROM pair_scores ps
        LEFT JOIN couples c ON c.chat_id=ps.chat_id AND c.user1=ps.user1 AND c.user2=ps.user2
        LEFT JOIN votes   v ON v.couple_id=c.id
        LEFT JOIN players p1 ON p1.chat_id=ps.chat_id AND p1.user_id=ps.user1
        LEFT JOIN players p2 ON p2.chat_id=ps.chat_id AND p2.user_id=ps.user2
        WHERE ps.chat_id=?
        GROUP BY ps.user1, ps.user2, ps.score, p1.attr_car, p2.attr_car
        ORDER BY (ps.score + likes * 2 - dislikes + (car1 + car2) * 0.2) DESC
        LIMIT 80
        """,
        (chat_id,),
    )
    for row in cur.fetchall():
        u1, u2, score = row["user1"], row["user2"], row["score"]
        if score < MIN_PAIR_SCORE:
            continue
        if not user_is_available(chat_id, u1) or not user_is_available(chat_id, u2):
            continue
        if pair_recently_used(chat_id, u1, u2):
            continue
        return u1, u2
    return None


async def send_couple(chat_id: int, source: str = "auto") -> bool:
    assert bot is not None
    pair = pick_couple(chat_id)
    if not pair:
        if source == "manual":
            await bot.send_message(
                chat_id,
                "💍👑 𝐂𝐀𝐒𝐀𝐌𝐄𝐍𝐓𝐎 𝐑𝐄𝐀𝐋 👑\n\n"
                "😶 Ainda não existe interação suficiente para formar um casal real.\n"
                "🔥 Respondam mensagens, conversem e tentem de novo em alguns minutos!",
            )
        return False

    u1, u2 = pair
    n1 = get_name(chat_id, u1)
    n2 = get_name(chat_id, u2)

    cur.execute(
        "INSERT INTO couples (chat_id, user1, user2, source, created_at) VALUES (?, ?, ?, ?, ?)",
        (chat_id, u1, u2, source, utc_iso()),
    )
    couple_id = cur.lastrowid

    # XP pros dois pelo casamento formado
    for uid in (u1, u2):
        award_xp_immediate(chat_id, uid, XP_COUPLE_FORMED, reason="couple")

    db.commit()

    frase_topo = random.choice(ROMANTICAS)
    frase_casal = random.choice(CASAL_FRASES)

    text = (
        "💍👑 𝐂𝐀𝐒𝐀𝐌𝐄𝐍𝐓𝐎 𝐑𝐄𝐀𝐋 👑\n\n"
        f"{frase_topo}\n\n"
        f"{mention(u1, n1)} ❤️ {mention(u2, n2)}\n"
        f"{frase_casal}\n\n"
        "<i>🚨 Se alguém presente souber de alguma razão para que este casal não deva se unir no santo "
        "matrimônio, fale agora ou cale-se para sempre! 👀💍</i>"
    )
    await safe_send(chat_id, text, reply_markup=vote_keyboard(couple_id))
    return True


async def safe_send(chat_id: int, text: str, **kwargs):
    """Envia mensagem com retry simples em caso de RetryAfter."""
    assert bot is not None
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramRetryAfter as e:
        logger.warning("RetryAfter %ds on send to %d", e.retry_after, chat_id)
        await asyncio.sleep(e.retry_after + 1)
        try:
            return await bot.send_message(chat_id, text, **kwargs)
        except Exception:
            logger.exception("safe_send retry failed")
    except Exception:
        logger.exception("safe_send failed chat=%d", chat_id)


async def safe_edit(chat_id: int, message_id: int, text: str, **kwargs):
    """Edita mensagem com tratamento de erros comuns."""
    assert bot is not None
    try:
        return await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        logger.warning("edit failed: %s", e)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
    except Exception:
        logger.exception("safe_edit failed")


# =====================================================================
# ADMIN CHECK (cacheado)
# =====================================================================

async def is_admin(message: Message) -> bool:
    assert bot is not None
    if not message.from_user:
        return False
    key = (message.chat.id, message.from_user.id)
    now = utc_now().timestamp()
    cached = admin_cache.get(key)
    if cached and cached[1] > now:
        return cached[0]
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        result = member.status in {"administrator", "creator"}
    except Exception:
        logger.exception("get_chat_member failed for %s", key)
        return False
    admin_cache[key] = (result, now + ADMIN_CACHE_TTL_SECONDS)
    return result


# =====================================================================
# RPG — IDENTIDADE: ROYAL ID, PLAYERS
# =====================================================================

def next_royal_id(chat_id: int) -> str:
    cur.execute(
        "INSERT INTO royal_id_seq (chat_id, next_id) VALUES (?, 1) "
        "ON CONFLICT(chat_id) DO NOTHING",
        (chat_id,),
    )
    cur.execute("SELECT next_id FROM royal_id_seq WHERE chat_id=?", (chat_id,))
    row = cur.fetchone()
    nid = row["next_id"]
    cur.execute("UPDATE royal_id_seq SET next_id=? WHERE chat_id=?", (nid + 1, chat_id))
    return f"RYL-{nid:04d}"


def ensure_player(chat_id: int, user_id: int) -> dict:
    cur.execute("SELECT * FROM players WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    if row:
        return dict(row)
    royal_id = next_royal_id(chat_id)
    cur.execute(
        """
        INSERT INTO players (chat_id, user_id, royal_id, total_xp, season_xp,
                             attr_for, attr_des, attr_vit, attr_car,
                             gold, joined_at)
        VALUES (?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?)
        """,
        (chat_id, user_id, royal_id, ATTR_START, ATTR_START, ATTR_START, ATTR_START,
         STARTING_GOLD, utc_iso()),
    )
    db.commit()
    cur.execute("SELECT * FROM players WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    return dict(cur.fetchone())


def get_player(chat_id: int, user_id: int) -> dict | None:
    cur.execute("SELECT * FROM players WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    return dict(row) if row else None


def get_player_by_royal_id(chat_id: int, royal_id: str) -> dict | None:
    cur.execute(
        "SELECT * FROM players WHERE chat_id=? AND royal_id=?",
        (chat_id, royal_id.upper()),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def resolve_owner_chat(user_id: int, fallback_chat: int | None = None) -> int | None:
    """
    Resolve em qual grupo Royal um usuário tem perfil mais ativo.
    - Em grupo, retorna fallback_chat (chat atual) se o user tiver player lá.
    - Em DM, retorna o chat com maior total_xp.
    """
    if fallback_chat is not None:
        cur.execute(
            "SELECT 1 FROM players WHERE chat_id=? AND user_id=? LIMIT 1",
            (fallback_chat, user_id),
        )
        if cur.fetchone():
            return fallback_chat
    cur.execute(
        "SELECT chat_id FROM players WHERE user_id=? ORDER BY total_xp DESC LIMIT 1",
        (user_id,),
    )
    row = cur.fetchone()
    return row["chat_id"] if row else None


# =====================================================================
# RPG — XP, NIVEIS, ATRIBUTOS
# =====================================================================

_LEVEL_TABLE: list[int] = []  # cumulative XP to reach level (index = level)


def _build_level_table(max_level: int = 200) -> None:
    """Constroi tabela de XP cumulativo. Level 1 = 0 XP."""
    _LEVEL_TABLE.clear()
    _LEVEL_TABLE.append(0)  # nivel 1
    total = 0
    for n in range(2, max_level + 2):
        total += int(100 * (n ** 1.5))
        _LEVEL_TABLE.append(total)


_build_level_table()


def level_progress(xp: int) -> tuple[int, int, int, int]:
    """Retorna (level, xp_neste_nivel, xp_pro_proximo, xp_total_pro_proximo)."""
    lvl = 1
    for n in range(1, len(_LEVEL_TABLE) - 1):
        if _LEVEL_TABLE[n] <= xp:
            lvl = n
        else:
            break
    base = _LEVEL_TABLE[lvl]
    nxt = _LEVEL_TABLE[lvl + 1] if lvl + 1 < len(_LEVEL_TABLE) else base + 99999
    in_level = xp - base
    needed = nxt - base
    return lvl, in_level, max(1, needed), nxt


def hp_max(player: dict) -> int:
    """Calcula HP maximo baseado em VIT + level + classe + itens equipados."""
    vit = player["attr_vit"]
    lvl, *_ = level_progress(player["total_xp"])
    base = 50 + vit * 10 + lvl * 5
    if player.get("class_id") == "monarca":
        base = int(base * 1.20)
    # bonus de itens equipados que aumentam VIT
    cur.execute(
        "SELECT item_id FROM inventory WHERE chat_id=? AND user_id=? AND equipped=1",
        (player["chat_id"], player["user_id"]),
    )
    for r in cur.fetchall():
        item = ITEMS.get(r["item_id"])
        if item and item.get("type") == "equip" and item.get("stat") == "vit":
            base += item.get("bonus", 0) * 10
    return base


def effective_attr(player: dict, attr: str) -> int:
    """Atributo efetivo (base + classe + equipamentos)."""
    value = player[f"attr_{attr}"]
    cls = player.get("class_id")
    if cls == "cavaleiro" and attr == "for": value += 2
    if cls == "cortesa"   and attr == "car": value += 2
    if cls == "bruxo"     and attr == "des": value += 2
    cur.execute(
        "SELECT item_id FROM inventory WHERE chat_id=? AND user_id=? AND equipped=1",
        (player["chat_id"], player["user_id"]),
    )
    for r in cur.fetchall():
        item = ITEMS.get(r["item_id"])
        if item and item.get("type") == "equip" and item.get("stat") == attr:
            value += item.get("bonus", 0)
    return value


def player_has_active_couple(chat_id: int, user_id: int) -> bool:
    """Verifica se jogador esta num casamento ativo (formado nas ultimas 14 dias)."""
    cutoff = (utc_now() - timedelta(days=14)).isoformat()
    cur.execute(
        "SELECT 1 FROM couples WHERE chat_id=? AND (user1=? OR user2=?) AND created_at>=? LIMIT 1",
        (chat_id, user_id, user_id, cutoff),
    )
    return cur.fetchone() is not None


def award_xp_immediate(chat_id: int, user_id: int, amount: int, reason: str = "") -> None:
    """Concede XP imediato sem checar cooldown (usado para eventos: casamento, palavra, boss)."""
    player = ensure_player(chat_id, user_id)
    # bonus de classe cronista (+10%)
    if player.get("class_id") == "cronista":
        amount = int(amount * 1.10)
    # bonus de casamento ativo (+10%)
    if player_has_active_couple(chat_id, user_id):
        amount = int(amount * (1 + COUPLE_XP_BUFF))
    if amount <= 0:
        return
    old_xp = player["total_xp"]
    new_xp = old_xp + amount
    old_lvl, *_ = level_progress(old_xp)
    new_lvl, *_ = level_progress(new_xp)
    pts_gain = (new_lvl - old_lvl) * PTS_PER_LEVEL if new_lvl > old_lvl else 0
    cur.execute(
        "UPDATE players SET total_xp=?, season_xp=season_xp+?, pts_available=pts_available+? "
        "WHERE chat_id=? AND user_id=?",
        (new_xp, amount, pts_gain, chat_id, user_id),
    )
    db.commit()
    logger.info("xp+%d uid=%d chat=%d reason=%s level %d→%d",
                amount, user_id, chat_id, reason, old_lvl, new_lvl)


def award_xp_message(chat_id: int, user_id: int, is_reply: bool) -> None:
    """Concede XP de mensagem normal ou reply, com cooldown."""
    player = ensure_player(chat_id, user_id)
    now = utc_now()
    field = "last_xp_reply_at" if is_reply else "last_xp_msg_at"
    cooldown = XP_COOLDOWN_REPLY_SECONDS if is_reply else XP_COOLDOWN_MSG_SECONDS
    amount = XP_PER_REPLY if is_reply else XP_PER_MESSAGE
    last = player.get(field)
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            if (now - last_dt).total_seconds() < cooldown:
                # so incrementa contador de mensagens, sem XP
                cur.execute(
                    "UPDATE players SET rpg_message_count=rpg_message_count+1 "
                    "WHERE chat_id=? AND user_id=?", (chat_id, user_id))
                return
        except Exception:
            pass
    # bonus cronista + casamento
    real_amount = amount
    if player.get("class_id") == "cronista":
        real_amount = int(real_amount * 1.10)
    if player_has_active_couple(chat_id, user_id):
        real_amount = int(real_amount * (1 + COUPLE_XP_BUFF))
    old_xp = player["total_xp"]
    new_xp = old_xp + real_amount
    old_lvl, *_ = level_progress(old_xp)
    new_lvl, *_ = level_progress(new_xp)
    pts_gain = (new_lvl - old_lvl) * PTS_PER_LEVEL if new_lvl > old_lvl else 0
    cur.execute(
        f"UPDATE players SET total_xp=?, season_xp=season_xp+?, pts_available=pts_available+?, "
        f"rpg_message_count=rpg_message_count+1, {field}=? "
        f"WHERE chat_id=? AND user_id=?",
        (new_xp, real_amount, pts_gain, now.isoformat(), chat_id, user_id),
    )


# =====================================================================
# RPG — FOTO DE PERFIL
# =====================================================================

async def get_user_photo_file_id(user_id: int) -> str | None:
    """Retorna file_id da foto de perfil (cacheado 24h). None se nao publica."""
    assert bot is not None
    now = utc_now().timestamp()
    cached = photo_cache.get(user_id)
    if cached and cached[1] > now:
        return cached[0]
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0 and photos.photos and photos.photos[0]:
            # Pega a maior resolucao da primeira foto
            file_id = photos.photos[0][-1].file_id
            photo_cache[user_id] = (file_id, now + PHOTO_CACHE_TTL_SECONDS)
            return file_id
    except Exception:
        logger.exception("get_user_profile_photos failed uid=%d", user_id)
    photo_cache[user_id] = (None, now + PHOTO_CACHE_TTL_SECONDS)
    return None


# =====================================================================
# RPG — CARTAO DE PERFIL
# =====================================================================

def progress_bar(current: int, total: int, length: int = 10) -> str:
    if total <= 0:
        return "▱" * length
    filled = max(0, min(length, int(round(current / total * length))))
    return "▰" * filled + "▱" * (length - filled)


def build_profile_text(chat_id: int, user_id: int) -> str:
    p = ensure_player(chat_id, user_id)
    name = get_name(chat_id, user_id) or "Nobre"
    lvl, in_lvl, needed, _ = level_progress(p["total_xp"])
    bar = progress_bar(in_lvl, needed)

    # Ranking (posicao na temporada)
    cur.execute(
        "SELECT COUNT(*) AS total FROM players WHERE chat_id=?", (chat_id,))
    total_players = cur.fetchone()["total"]
    cur.execute(
        "SELECT COUNT(*)+1 AS rank FROM players WHERE chat_id=? AND season_xp > ?",
        (chat_id, p["season_xp"]))
    rank = cur.fetchone()["rank"]

    # Casorios (couples envolvendo o user)
    cur.execute(
        "SELECT COUNT(*) AS total FROM couples WHERE chat_id=? AND (user1=? OR user2=?)",
        (chat_id, user_id, user_id))
    casorios = cur.fetchone()["total"]

    # Vitorias palavra
    cur.execute(
        "SELECT COUNT(*) AS total FROM challenges WHERE chat_id=? AND winner_user_id=?",
        (chat_id, user_id))
    palavras_won = cur.fetchone()["total"]

    class_id = p.get("class_id")
    class_str = f"{CLASSES[class_id]['emoji']} {CLASSES[class_id]['name']}" if class_id and class_id in CLASSES else "🎭 sem classe"

    hp = hp_max(p)
    f_ = effective_attr(p, "for")
    d_ = effective_attr(p, "des")
    v_ = effective_attr(p, "vit")
    c_ = effective_attr(p, "car")
    pts_txt = f"   (+{p['pts_available']} pts)" if p['pts_available'] > 0 else ""

    joined_str = "?"
    if p.get("joined_at"):
        try:
            joined_dt = datetime.fromisoformat(p["joined_at"])
            joined_str = joined_dt.strftime("%d/%m")
        except Exception:
            pass

    msg_count = p.get("rpg_message_count", 0)
    season = current_season_label()

    return (
        f"👑 <b>{p['royal_id']}</b>\n"
        f"\n"
        f"   {mention(user_id, name)}\n"
        f"   {class_str} · {season}\n"
        f"\n"
        f"⭐ Nível {lvl}     {bar}  {in_lvl} / {needed} XP\n"
        f"🩸 {hp}/{hp}     💪{f_}  🏃{d_}  ❤️{v_}  ✨{c_}{pts_txt}\n"
        f"\n"
        f"🏆 #{rank} de {total_players}  ·  🎯 {palavras_won}  ·  💍 {casorios}  ·  🪙 {p['gold']}\n"
        f"💬 {msg_count} mensagens desde {joined_str}"
    )


async def send_profile_card(chat_id_to: int, owner_chat: int, owner_uid: int):
    """Envia cartao de perfil (com foto se publica) para chat_id_to."""
    assert bot is not None
    text = build_profile_text(owner_chat, owner_uid)
    photo_id = await get_user_photo_file_id(owner_uid)
    try:
        if photo_id:
            await bot.send_photo(chat_id_to, photo=photo_id, caption=text, parse_mode="HTML")
        else:
            await bot.send_message(chat_id_to, text)
    except TelegramBadRequest as e:
        logger.warning("send_profile_card fallback: %s", e)
        await bot.send_message(chat_id_to, text)
    except Exception:
        logger.exception("send_profile_card failed")
        await bot.send_message(chat_id_to, text)


# =====================================================================
# RPG — HUB (/royal menu)
# =====================================================================

def hub_keyboard_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👑 Meu Perfil",       callback_data="r:perfil"),
         InlineKeyboardButton(text="⬆️ Distribuir Pts",   callback_data="r:up:menu")],
        [InlineKeyboardButton(text="🎭 Escolher Classe",  callback_data="r:cls:menu"),
         InlineKeyboardButton(text="🎒 Inventário",       callback_data="r:inv")],
        [InlineKeyboardButton(text="🪙 Loja",             callback_data="r:loja"),
         InlineKeyboardButton(text="🏆 Ranking",          callback_data="r:rank")],
        [InlineKeyboardButton(text="🎯 Palavra da Hora",  callback_data="r:pal"),
         InlineKeyboardButton(text="🐉 Boss",             callback_data="r:boss")],
        [InlineKeyboardButton(text="💍 Casórios",         callback_data="r:cas"),
         InlineKeyboardButton(text="❓ Ajuda",            callback_data="r:help")],
        [InlineKeyboardButton(text="🔒 Privacidade",      callback_data="r:priv")],
    ])


HUB_TEXT = (
    "👑 <b>REINO ROYAL</b>\n\n"
    f"📅 {current_season_label()}\n\n"
    "Escolha o que deseja:"
)


# =====================================================================
# RPG — PALAVRA DA HORA
# =====================================================================

def make_anagrama(word: str) -> str:
    letters = list(word.upper())
    original = "".join(letters)
    for _ in range(10):
        random.shuffle(letters)
        if "".join(letters) != original or len(set(letters)) <= 1:
            break
    return " ".join(letters)


def make_letras_faltando(word: str) -> str:
    n = len(word)
    if n <= 3:
        hide = 1
    elif n <= 5:
        hide = 2
    else:
        hide = 3
    positions = random.sample(range(n), hide)
    return " ".join(c.upper() if i not in positions else "_" for i, c in enumerate(word))


def get_active_challenge(chat_id: int) -> dict | None:
    cur.execute(
        "SELECT * FROM challenges WHERE chat_id=? AND status='open' "
        "ORDER BY id DESC LIMIT 1",
        (chat_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def format_challenge_text(ch: dict, status: str = "open", winner_name: str = "") -> str:
    type_label = {"anagrama": "🔤 Anagrama", "letras": "🔡 Letras Faltando", "charada": "💭 Charada"}.get(ch["type"], "🎯 Desafio")
    base = f"🎯 <b>PALAVRA DA HORA</b>\n{type_label}\n\n"
    if status == "open":
        try:
            ends_at = datetime.fromisoformat(ch["ends_at"])
            mins_left = max(0, int((ends_at - utc_now()).total_seconds() / 60))
        except Exception:
            mins_left = ch.get("duration_min", 5)
        if ch["type"] == "charada":
            base += f"<i>{html.escape(ch['hint'])}</i>\n\n"
        else:
            base += f"<code>{ch['display']}</code>\n\n"
        base += (
            f"⏱️ Tempo: ~{mins_left} min\n"
            f"💬 Tentativas: {ch.get('attempts_count', 0)}\n"
            f"⚡ Primeiro a acertar leva <b>{XP_PALAVRA_WIN_BONUS} XP + {GOLD_PALAVRA_WIN} 🪙</b>\n\n"
            f"<i>Responda diretamente no chat</i>"
        )
    elif status == "win":
        base += (
            f"🏆 <b>{winner_name} acertou!</b>\n"
            f"A palavra era <b>{ch['word'].upper()}</b>\n"
            f"⏱️ {(ch.get('winner_ms') or 0) / 1000:.1f}s · "
            f"💬 {ch.get('attempts_count', 0)} tentativas"
        )
    else:  # timeout
        base += (
            f"⏰ <b>Tempo esgotado!</b>\n"
            f"A palavra era <b>{ch['word'].upper()}</b>\n"
            f"😶 Ninguém acertou."
        )
    return base


async def spawn_palavra(chat_id: int) -> None:
    """Cria novo desafio e envia mensagem."""
    assert bot is not None
    use_charada = random.random() < 0.25 and CHARADAS
    if use_charada:
        hint, word = random.choice(CHARADAS)
        challenge_type = "charada"
        display = ""
    else:
        word = random.choice(PALAVRAS)
        if random.random() < 0.5:
            challenge_type = "anagrama"
            display = make_anagrama(word)
        else:
            challenge_type = "letras"
            display = make_letras_faltando(word)
        hint = ""

    duration_min = random.choice(PALAVRA_DURATIONS_MIN)
    started = utc_now()
    ends = started + timedelta(minutes=duration_min)

    cur.execute(
        """
        INSERT INTO challenges
            (chat_id, type, word, display, hint, started_at, ends_at, duration_min,
             attempts_count, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'open')
        """,
        (chat_id, challenge_type, normalize_word(word), display, hint,
         started.isoformat(), ends.isoformat(), duration_min),
    )
    cid = cur.lastrowid
    db.commit()

    ch = {
        "id": cid, "chat_id": chat_id, "type": challenge_type,
        "word": normalize_word(word), "display": display, "hint": hint,
        "started_at": started.isoformat(), "ends_at": ends.isoformat(),
        "duration_min": duration_min, "attempts_count": 0,
    }
    msg = await safe_send(chat_id, format_challenge_text(ch))
    if msg:
        cur.execute("UPDATE challenges SET message_id=? WHERE id=?", (msg.message_id, cid))
        db.commit()
    logger.info("palavra spawned chat=%d cid=%d type=%s dur=%d", chat_id, cid, challenge_type, duration_min)


async def handle_palavra_attempt(message: Message, ch: dict) -> bool:
    """Processa tentativa. Retorna True se acertou (winner)."""
    if not message.from_user or not message.text:
        return False
    uid = message.from_user.id
    chat_id = message.chat.id

    # cooldown anti-spam
    now_ts = utc_now().timestamp()
    cd_key = (ch["id"], uid)
    last = attempt_cooldowns.get(cd_key, 0)
    if now_ts - last < PALAVRA_ATTEMPT_COOLDOWN_SEC:
        return False
    attempt_cooldowns[cd_key] = now_ts

    attempt = normalize_word(message.text)
    if not attempt:
        return False
    if len(attempt) > 30:  # ignora mensagens longas
        return False

    target = ch["word"]
    correct = attempt == target

    cur.execute(
        "UPDATE challenges SET attempts_count=attempts_count+1 WHERE id=?",
        (ch["id"],),
    )

    if not correct:
        return False

    # winner! atomic claim contra race entre 2+ acertos simultaneos
    try:
        started_dt = datetime.fromisoformat(ch["started_at"])
        winner_ms = int((utc_now() - started_dt).total_seconds() * 1000)
    except Exception:
        winner_ms = 0

    cur.execute(
        "UPDATE challenges SET status='won', winner_user_id=?, winner_ms=? "
        "WHERE id=? AND status='open'",
        (uid, winner_ms, ch["id"]),
    )
    if cur.rowcount == 0:
        # outro jogador ganhou antes; ainda dá XP de consolo
        award_xp_immediate(chat_id, uid, XP_PALAVRA_CONSOLATION, reason="palavra_consolation")
        db.commit()
        return False
    db.commit()

    # XP + gold
    base_xp = random.randint(XP_PALAVRA_MIN, XP_PALAVRA_MAX) + XP_PALAVRA_WIN_BONUS
    award_xp_immediate(chat_id, uid, base_xp, reason="palavra_win")
    p = ensure_player(chat_id, uid)
    gold_award = GOLD_PALAVRA_WIN
    if p.get("class_id") == "bobo":
        gold_award = int(gold_award * 1.5)
    cur.execute("UPDATE players SET gold=gold+? WHERE chat_id=? AND user_id=?",
                (gold_award, chat_id, uid))
    db.commit()

    # Atualiza mensagem
    cur.execute("SELECT * FROM challenges WHERE id=?", (ch["id"],))
    ch_final = dict(cur.fetchone())
    name = display_name(message)
    if ch_final.get("message_id"):
        await safe_edit(chat_id, ch_final["message_id"],
                        format_challenge_text(ch_final, status="win", winner_name=mention(uid, name)))
    return True


async def finalize_expired_challenges() -> None:
    """Marca como timeout desafios cujo ends_at ja passou."""
    now = utc_iso()
    cur.execute(
        "SELECT * FROM challenges WHERE status='open' AND ends_at < ?",
        (now,),
    )
    expired = [dict(r) for r in cur.fetchall()]
    for ch in expired:
        cur.execute("UPDATE challenges SET status='timeout' WHERE id=?", (ch["id"],))
        if ch.get("message_id"):
            await safe_edit(ch["chat_id"], ch["message_id"],
                            format_challenge_text(ch, status="timeout"))
    if expired:
        db.commit()


def schedule_next_palavra(chat_id: int) -> None:
    """Agenda proximo desafio: hora cheia seguinte +/- jitter."""
    now = local_now()
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    jitter = random.randint(-PALAVRA_JITTER_SEC, PALAVRA_JITTER_SEC)
    next_at = next_hour + timedelta(seconds=jitter)
    cur.execute(
        "INSERT INTO chats_rpg (chat_id, next_palavra_at) VALUES (?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET next_palavra_at=excluded.next_palavra_at",
        (chat_id, next_at.isoformat()),
    )
    db.commit()


# =====================================================================
# RPG — BOSS SEMANAL
# =====================================================================

BOSS_NAMES = [
    "🐉 Dragão da Corte", "👹 Ogro do Pântano", "💀 Lich Ancião",
    "🦂 Escorpião Real", "🦇 Vampiro da Torre", "🐺 Lobo das Sombras",
]


def boss_max_hp_for(chat_id: int) -> int:
    """HP do boss escala com numero de jogadores ativos."""
    cur.execute("SELECT COUNT(*) AS total FROM players WHERE chat_id=?", (chat_id,))
    n = cur.fetchone()["total"]
    return max(500, n * 200)


def get_active_boss(chat_id: int) -> dict | None:
    cur.execute(
        "SELECT * FROM bosses WHERE chat_id=? AND status='alive' ORDER BY id DESC LIMIT 1",
        (chat_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def boss_keyboard(boss_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⚔️ Atacar!", callback_data=f"r:atk:{boss_id}")
    ]])


def format_boss_text(boss: dict) -> str:
    bar_len = 20
    pct = boss["hp"] / max(1, boss["max_hp"])
    filled = max(0, min(bar_len, int(round(pct * bar_len))))
    bar = "█" * filled + "░" * (bar_len - filled)
    cur.execute("SELECT COUNT(DISTINCT user_id) AS n FROM boss_hits WHERE boss_id=?", (boss["id"],))
    attackers = cur.fetchone()["n"]
    return (
        f"🐉 <b>BOSS DA SEMANA</b>\n"
        f"{boss['name']}\n\n"
        f"❤️ {boss['hp']} / {boss['max_hp']}\n"
        f"{bar}\n\n"
        f"⚔️ {attackers} bravos atacando\n"
        f"<i>Toque em Atacar para causar dano!</i>"
    )


async def spawn_boss_if_due() -> None:
    """Spawna boss em todos chats habilitados se for domingo 20h e ainda nao spawnou esta semana."""
    now = local_now()
    if now.weekday() != BOSS_SPAWN_WEEKDAY or now.hour != BOSS_SPAWN_HOUR:
        return
    week = current_week_marker(now)
    cur.execute("SELECT chat_id FROM chats WHERE enabled=1")
    chats = [r["chat_id"] for r in cur.fetchall()]
    for chat_id in chats:
        # ja spawnou esta semana?
        cur.execute(
            "SELECT 1 FROM bosses WHERE chat_id=? AND week_marker=? LIMIT 1",
            (chat_id, week))
        if cur.fetchone():
            continue
        max_hp = boss_max_hp_for(chat_id)
        name = random.choice(BOSS_NAMES)
        cur.execute(
            "INSERT INTO bosses (chat_id, name, hp, max_hp, spawned_at, week_marker, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'alive')",
            (chat_id, name, max_hp, max_hp, utc_iso(), week))
        boss_id = cur.lastrowid
        db.commit()
        boss = {"id": boss_id, "chat_id": chat_id, "name": name,
                "hp": max_hp, "max_hp": max_hp}
        msg = await safe_send(chat_id,
                              "🚨 <b>UM BOSS APARECEU!</b>\n\n" + format_boss_text(boss),
                              reply_markup=boss_keyboard(boss_id))
        if msg:
            cur.execute("UPDATE bosses SET message_id=? WHERE id=?",
                        (msg.message_id, boss_id))
            db.commit()
        logger.info("boss spawned chat=%d id=%d hp=%d", chat_id, boss_id, max_hp)


async def handle_boss_attack(cb: CallbackQuery, boss_id: int) -> None:
    assert bot is not None
    if not cb.from_user or not cb.message:
        await cb.answer()
        return
    uid = cb.from_user.id
    cur.execute("SELECT * FROM bosses WHERE id=?", (boss_id,))
    row = cur.fetchone()
    if not row or row["status"] != "alive":
        await cb.answer("Este boss já foi derrotado 🪦", show_alert=False)
        return
    boss = dict(row)
    chat_id = boss["chat_id"]

    # cooldown
    now_ts = utc_now().timestamp()
    cd = boss_attack_cooldowns.get((boss_id, uid), 0)
    if now_ts - cd < BOSS_ATTACK_COOLDOWN_SEC:
        wait = int(BOSS_ATTACK_COOLDOWN_SEC - (now_ts - cd))
        await cb.answer(f"⏳ Aguarde {wait}s antes de atacar novamente", show_alert=False)
        return
    boss_attack_cooldowns[(boss_id, uid)] = now_ts

    p = ensure_player(chat_id, uid)
    f_ = effective_attr(p, "for")
    damage = f_ + random.randint(1, 10)
    new_hp = max(0, boss["hp"] - damage)
    cur.execute("UPDATE bosses SET hp=? WHERE id=?", (new_hp, boss_id))
    cur.execute(
        "INSERT INTO boss_hits (boss_id, user_id, damage, ts) VALUES (?, ?, ?, ?)",
        (boss_id, uid, damage, utc_iso()))
    db.commit()
    award_xp_immediate(chat_id, uid, XP_BOSS_HIT * damage // 2 + 5, reason="boss_hit")

    await cb.answer(f"⚔️ Você causou {damage} de dano!", show_alert=False)

    boss["hp"] = new_hp
    if new_hp <= 0:
        await finalize_boss(boss)
    else:
        if boss.get("message_id"):
            await safe_edit(chat_id, boss["message_id"], format_boss_text(boss),
                            reply_markup=boss_keyboard(boss_id))


async def finalize_boss(boss: dict) -> None:
    assert bot is not None
    chat_id = boss["chat_id"]
    boss_id = boss["id"]
    cur.execute("UPDATE bosses SET status='dead', killed_at=? WHERE id=?",
                (utc_iso(), boss_id))
    # distribui ouro proporcional ao dano
    cur.execute(
        "SELECT user_id, SUM(damage) AS dmg FROM boss_hits WHERE boss_id=? GROUP BY user_id",
        (boss_id,))
    rows = cur.fetchall()
    total_dmg = sum(r["dmg"] for r in rows) or 1
    drops_text = []
    for r in rows:
        share = int(GOLD_BOSS_KILL_TOTAL * r["dmg"] / total_dmg)
        if share > 0:
            cur.execute("UPDATE players SET gold=gold+? WHERE chat_id=? AND user_id=?",
                        (share, chat_id, r["user_id"]))
            name = get_name(chat_id, r["user_id"])
            drops_text.append(f"• {html.escape(name)}: {share}🪙 ({r['dmg']} dano)")
    db.commit()

    final_msg = (
        f"💀 <b>{boss['name']} foi derrotado!</b>\n\n"
        f"🏆 Recompensas distribuídas:\n" + "\n".join(drops_text[:10])
    )
    if boss.get("message_id"):
        await safe_edit(chat_id, boss["message_id"], final_msg)
    else:
        await safe_send(chat_id, final_msg)
    logger.info("boss killed chat=%d id=%d attackers=%d", chat_id, boss_id, len(rows))


# =====================================================================
# RPG — TEMPORADAS / HALL DA FAMA
# =====================================================================

async def check_season_change() -> None:
    """Se a estacao mudou desde o ultimo check em algum chat, snapshot + reset."""
    season = current_season_code()
    cur.execute("SELECT chat_id, current_season FROM chats_rpg")
    rows = cur.fetchall()
    for r in rows:
        if r["current_season"] != season:
            await close_season(r["chat_id"], r["current_season"], season)
    # garante que chats novos ganham a season atual sem fechar nada
    cur.execute("SELECT chat_id FROM chats WHERE enabled=1")
    for r in cur.fetchall():
        cid = r["chat_id"]
        cur.execute(
            "INSERT INTO chats_rpg (chat_id, current_season) VALUES (?, ?) "
            "ON CONFLICT(chat_id) DO NOTHING", (cid, season))
    db.commit()


async def close_season(chat_id: int, old_code: str | None, new_code: str) -> None:
    """Arquiva top 10 da temporada antiga, reseta season_xp, anuncia hall da fama."""
    assert bot is not None
    if old_code:
        cur.execute(
            "SELECT royal_id, user_id, season_xp FROM players "
            "WHERE chat_id=? AND season_xp > 0 ORDER BY season_xp DESC LIMIT 10",
            (chat_id,))
        top = cur.fetchall()
        if top:
            snap = utc_iso()
            lines = []
            medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
            for i, r in enumerate(top, 1):
                name = get_name(chat_id, r["user_id"])
                cur.execute(
                    "INSERT OR REPLACE INTO season_hall "
                    "(chat_id, season_code, rank, royal_id, user_id, display_name, season_xp, snapshot_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (chat_id, old_code, i, r["royal_id"], r["user_id"], name, r["season_xp"], snap))
                lines.append(f"{medals[i-1]} {i}. {html.escape(name)} ({r['royal_id']}) — {r['season_xp']} XP")
            db.commit()
            try:
                await safe_send(chat_id,
                                f"🏛️ <b>HALL DA FAMA — {old_code}</b>\n\n" + "\n".join(lines))
            except Exception:
                logger.exception("hall da fama send failed")

        cur.execute("UPDATE players SET season_xp=0 WHERE chat_id=?", (chat_id,))
    cur.execute("UPDATE chats_rpg SET current_season=? WHERE chat_id=?", (new_code, chat_id))
    db.commit()
    if old_code:
        try:
            await safe_send(chat_id,
                            f"🌟 Uma nova temporada começa: <b>{current_season_label()}</b>\n"
                            f"O ranking foi resetado. Que comecem os feitos!")
        except Exception:
            pass


# =====================================================================
# HANDLERS — COMANDOS GERAIS
# =====================================================================

@dp.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type != "private":
        return
    await message.answer(
        "👑 <b>Bem-vindo ao Royal!</b>\n\n"
        "Aqui é a corte. Você pode:\n"
        "• 👑 Acessar o reino com /royal\n"
        "• 👤 Ver seu perfil com /royalperfil\n"
        "• 💍 Acompanhar casórios e ranking\n\n"
        "Use os botões abaixo ou /royalajuda",
        reply_markup=private_menu,
    )


# === /royal — HUB ===

@dp.message(Command("royal"))
async def royal_hub(message: Message):
    await message.answer(HUB_TEXT, reply_markup=hub_keyboard_main())


@dp.message(F.text == "👑 Reino")
async def royal_hub_btn(message: Message):
    await message.answer(HUB_TEXT, reply_markup=hub_keyboard_main())


# === /royalajuda ===

ROYAL_HELP = (
    "👑 <b>COMANDOS DO REINO</b>\n\n"
    "<b>Geral</b>\n"
    "/royal — abre o menu principal\n"
    "/royalperfil — seu cartão de perfil\n"
    "/royalperfil RYL-0042 — perfil de outro nobre\n"
    "/royalficha — atalho pro perfil\n"
    "/royalajuda — esta ajuda\n\n"
    "<b>RPG</b>\n"
    "/royalup — distribuir pontos de atributo\n"
    "/royalclasse — escolher classe\n"
    "/royalinventario — ver itens\n"
    "/royalloja — comprar itens\n"
    "/royalsaldo — saldo de florins\n"
    "/royalranking — ranking da temporada\n\n"
    "<b>Eventos</b>\n"
    "/royalpalavra — status da Palavra da Hora\n"
    "/royalboss — status do boss da semana\n\n"
    "<b>Casórios (corte)</b>\n"
    "/royalcasorios — ranking dos casais\n"
    "/royalmeuscasorios — seu histórico\n"
    "/royalencalhar — sair do sistema\n"
    "/royaldesencalhar — voltar ao sistema\n"
    "/royalcasar — admin: forçar casório\n"
    "/royalativar — admin: ativar bot no grupo\n\n"
    "<b>Privacidade</b>\n"
    "/royalprivacidade — controles de privacidade\n"
    "/royaldados — exportar/apagar seus dados"
)


@dp.message(Command("royalajuda"))
async def royal_ajuda(message: Message):
    await message.answer(ROYAL_HELP)


@dp.message(Command("help"))
async def help_cmd(message: Message):
    await message.answer(ROYAL_HELP)


# === /royaladdbot @username [perm1+perm2+...] ===
# Gera botao deep-link pra adicionar OUTRO bot ao grupo atual com 1 toque.
# Telegram nao permite bot adicionar bot via API — humano admin precisa
# tocar e selecionar o grupo. Esse comando so monta o link contextualizado.

_VALID_ADMIN_PERMS = {
    "change_info", "delete_messages", "restrict_members", "invite_users",
    "pin_messages", "promote_members", "manage_video_chats",
    "manage_topics", "post_stories", "edit_stories", "delete_stories",
    "anonymous",
}
_DEFAULT_ADMIN_PERMS = "delete_messages+pin_messages+invite_users+restrict_members"


@dp.message(Command("royaladdbot"))
async def royal_add_bot(message: Message):
    if not message.from_user or not is_group(message):
        await message.reply("Use este comando dentro do grupo onde quer adicionar o bot.")
        return

    # so admin do grupo pode usar
    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    except TelegramBadRequest:
        await message.reply("Nao consegui verificar suas permissoes.")
        return
    if member.status not in ("creator", "administrator"):
        await message.reply("👑 Apenas admins do grupo podem usar isso.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.reply(
            "Uso: <code>/royaladdbot @usernamedobot [perm1+perm2+...]</code>\n\n"
            "<b>Permissoes validas</b> (separe com +):\n"
            "<code>change_info, delete_messages, restrict_members, invite_users, "
            "pin_messages, promote_members, manage_video_chats, manage_topics, "
            "post_stories, edit_stories, delete_stories, anonymous</code>\n\n"
            "Sem permissoes → entra como membro comum.\n"
            "Exemplo: <code>/royaladdbot @MeuBot delete_messages+pin_messages</code>"
        )
        return

    username = parts[1].lstrip("@").strip()
    if not username or not all(c.isalnum() or c == "_" for c in username):
        await message.reply("Username invalido. Use formato @MeuBot.")
        return

    # permissoes (opcional)
    if len(parts) >= 3:
        raw = parts[2]
        requested = {p.strip() for p in raw.split("+") if p.strip()}
        invalid = requested - _VALID_ADMIN_PERMS
        if invalid:
            await message.reply(
                f"Permissoes invalidas: <code>{html.escape(', '.join(sorted(invalid)))}</code>"
            )
            return
        perms_str = "+".join(sorted(requested))
    else:
        perms_str = _DEFAULT_ADMIN_PERMS

    if perms_str:
        url = f"https://t.me/{username}?startgroup=true&admin={perms_str}"
        perms_pretty = perms_str.replace("+", ", ")
        body = (
            f"➕ Toque pra adicionar <b>@{html.escape(username)}</b> neste grupo "
            f"como <b>admin</b> com:\n<code>{html.escape(perms_pretty)}</code>\n\n"
            f"<i>O Telegram vai abrir o seletor — escolha este mesmo grupo.</i>"
        )
    else:
        url = f"https://t.me/{username}?startgroup=true"
        body = (
            f"➕ Toque pra adicionar <b>@{html.escape(username)}</b> "
            f"neste grupo como membro comum.\n\n"
            f"<i>O Telegram vai abrir o seletor — escolha este mesmo grupo.</i>"
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"➕ Adicionar @{username}", url=url)
    ]])
    await message.reply(body, reply_markup=kb)


# === /royalperfil ===

@dp.message(Command("royalperfil", "royalficha"))
async def royal_perfil(message: Message):
    if not message.from_user:
        return
    # /royalperfil RYL-0042
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        royal_id = parts[1].strip().upper()
        lookup_chat = (message.chat.id if is_group(message)
                       else resolve_owner_chat(message.from_user.id))
        if lookup_chat is None:
            await message.answer("Use este comando no grupo Royal.")
            return
        p = get_player_by_royal_id(lookup_chat, royal_id)
        if not p:
            await message.answer(f"😶 Nobre <b>{html.escape(royal_id)}</b> não encontrado neste reino.")
            return
        await send_profile_card(message.chat.id, p["chat_id"], p["user_id"])
        return

    # Sem argumento: perfil proprio
    if is_group(message):
        ensure_chat(message.chat.id, message.chat.title)
        ensure_player(message.chat.id, message.from_user.id)
        db.commit()
        await send_profile_card(message.chat.id, message.chat.id, message.from_user.id)
    else:
        owner_chat = resolve_owner_chat(message.from_user.id)
        if owner_chat is None:
            await message.answer(
                "😶 Você ainda não tem perfil no Reino.\n"
                "Envie uma mensagem no grupo Royal para começar!")
            return
        await send_profile_card(message.chat.id, owner_chat, message.from_user.id)


# === /royalup ===

@dp.message(Command("royalup"))
async def royal_up(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer("Use no grupo Royal para distribuir pontos.")
        return
    chat_id = message.chat.id
    p = ensure_player(chat_id, message.from_user.id)
    db.commit()
    if p["pts_available"] <= 0:
        await message.answer("Você não tem pontos para distribuir. Suba de nível primeiro! ⭐")
        return
    await message.answer(
        f"⬆️ Você tem <b>{p['pts_available']}</b> ponto(s) para distribuir.\n"
        f"Escolha um atributo:",
        reply_markup=up_keyboard(),
    )


def up_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💪 FOR", callback_data="r:up:for"),
        InlineKeyboardButton(text="🏃 DES", callback_data="r:up:des"),
        InlineKeyboardButton(text="❤️ VIT", callback_data="r:up:vit"),
        InlineKeyboardButton(text="✨ CAR", callback_data="r:up:car"),
    ]])


# === /royalclasse ===

def classe_keyboard() -> InlineKeyboardMarkup:
    rows = []
    items = list(CLASSES.items())
    for i in range(0, len(items), 2):
        row = []
        for cid, info in items[i:i+2]:
            row.append(InlineKeyboardButton(
                text=f"{info['emoji']} {info['name']}",
                callback_data=f"r:cls:set:{cid}"))
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("royalclasse"))
async def royal_classe(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer("Use no grupo Royal.")
        return
    ensure_player(message.chat.id, message.from_user.id)
    db.commit()
    txt = "🎭 <b>Escolha sua classe</b>\n\n"
    for cid, info in CLASSES.items():
        txt += f"{info['emoji']} <b>{info['name']}</b> — {info['bonus']}\n"
    await message.answer(txt, reply_markup=classe_keyboard())


# === /royalinventario ===

@dp.message(Command("royalinventario"))
async def royal_inv(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer("Use no grupo Royal.")
        return
    chat_id = message.chat.id
    uid = message.from_user.id
    ensure_player(chat_id, uid)
    db.commit()
    cur.execute(
        "SELECT item_id, qty, equipped FROM inventory WHERE chat_id=? AND user_id=? ORDER BY equipped DESC",
        (chat_id, uid))
    rows = cur.fetchall()
    if not rows:
        await message.answer("🎒 Seu inventário está vazio. Visite a /royalloja!")
        return
    txt = "🎒 <b>Inventário</b>\n\n"
    for r in rows:
        item = ITEMS.get(r["item_id"])
        if not item:
            continue
        eq = "✅ " if r["equipped"] else ""
        txt += f"{eq}{item['emoji']} <b>{item['name']}</b> x{r['qty']}\n<i>{item['desc']}</i>\n\n"
    await message.answer(txt, reply_markup=inv_keyboard(chat_id, uid))


def inv_keyboard(chat_id: int, uid: int) -> InlineKeyboardMarkup:
    cur.execute(
        "SELECT item_id, equipped FROM inventory WHERE chat_id=? AND user_id=? AND qty>0",
        (chat_id, uid))
    rows = cur.fetchall()
    buttons = []
    for r in rows:
        item = ITEMS.get(r["item_id"])
        if not item:
            continue
        if item["type"] == "equip":
            label = "❌ Desequipar" if r["equipped"] else "✅ Equipar"
            buttons.append([InlineKeyboardButton(
                text=f"{item['emoji']} {label}",
                callback_data=f"r:inv:eq:{r['item_id']}")])
        elif item["type"] == "consumable":
            buttons.append([InlineKeyboardButton(
                text=f"{item['emoji']} Usar",
                callback_data=f"r:inv:use:{r['item_id']}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[
        InlineKeyboardButton(text="🛒 Ir à loja", callback_data="r:loja")]])


# === /royalloja ===

@dp.message(Command("royalloja"))
async def royal_loja(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer("Use no grupo Royal.")
        return
    p = ensure_player(message.chat.id, message.from_user.id)
    db.commit()
    txt = (f"🪙 <b>LOJA REAL</b>\n"
           f"Seu saldo: <b>{p['gold']}</b> 🪙\n\n")
    for iid, item in ITEMS.items():
        txt += f"{item['emoji']} <b>{item['name']}</b> — {item['price']}🪙\n<i>{item['desc']}</i>\n\n"
    await message.answer(txt, reply_markup=loja_keyboard())


def loja_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for iid, item in ITEMS.items():
        rows.append([InlineKeyboardButton(
            text=f"💳 {item['emoji']} {item['name']} ({item['price']}🪙)",
            callback_data=f"r:buy:{iid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# === /royalsaldo ===

@dp.message(Command("royalsaldo"))
async def royal_saldo(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer("Use no grupo Royal.")
        return
    p = ensure_player(message.chat.id, message.from_user.id)
    db.commit()
    await message.answer(f"🪙 <b>{p['gold']}</b> florins")


# === /royalranking ===

@dp.message(Command("royalranking"))
async def royal_ranking(message: Message):
    if not is_group(message):
        await message.answer("Use no grupo Royal.")
        return
    cur.execute(
        "SELECT royal_id, user_id, season_xp FROM players "
        "WHERE chat_id=? AND season_xp > 0 AND privacy_hide_ranking=0 "
        "ORDER BY season_xp DESC LIMIT 10", (message.chat.id,))
    rows = cur.fetchall()
    if not rows:
        await message.answer("🏆 Ninguém pontuou nesta temporada ainda. Interaja para subir!")
        return
    medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
    txt = f"🏆 <b>RANKING — {current_season_label()}</b>\n\n"
    for i, r in enumerate(rows, 1):
        name = get_name(message.chat.id, r["user_id"])
        lvl, *_ = level_progress(r["season_xp"] if r["season_xp"] > _LEVEL_TABLE[1] else _LEVEL_TABLE[1] + 1)
        # mostra so XP da temporada e level total (do lifetime)
        cur.execute("SELECT total_xp FROM players WHERE chat_id=? AND user_id=?",
                    (message.chat.id, r["user_id"]))
        total = cur.fetchone()["total_xp"]
        full_lvl, *_ = level_progress(total)
        txt += f"{medals[i-1]} {i}. {html.escape(name)} ({r['royal_id']}) — Lvl {full_lvl} · {r['season_xp']} XP\n"
    await message.answer(txt)


# === /royalpalavra ===

@dp.message(Command("royalpalavra"))
async def royal_palavra_status(message: Message):
    if not is_group(message):
        await message.answer("Use no grupo Royal.")
        return
    ch = get_active_challenge(message.chat.id)
    if not ch:
        cur.execute("SELECT next_palavra_at FROM chats_rpg WHERE chat_id=?", (message.chat.id,))
        r = cur.fetchone()
        nxt = r["next_palavra_at"] if r else None
        if nxt:
            try:
                nxt_dt = datetime.fromisoformat(nxt)
                mins = max(0, int((nxt_dt - local_now()).total_seconds() / 60))
                await message.answer(f"🎯 Sem desafio ativo. Próximo em ~<b>{mins} min</b>.")
                return
            except Exception:
                pass
        await message.answer("🎯 Sem desafio ativo. Aguarde o próximo!")
        return
    await message.answer(format_challenge_text(ch))


# === /royalboss ===

@dp.message(Command("royalboss"))
async def royal_boss_status(message: Message):
    if not is_group(message):
        await message.answer("Use no grupo Royal.")
        return
    boss = get_active_boss(message.chat.id)
    if not boss:
        await message.answer(
            f"🐉 Nenhum boss ativo. O próximo nasce no <b>domingo às {BOSS_SPAWN_HOUR}h</b>!")
        return
    await message.answer(format_boss_text(boss), reply_markup=boss_keyboard(boss["id"]))


# === Privacidade ===

@dp.message(Command("royalprivacidade"))
async def royal_priv(message: Message):
    if not message.from_user:
        return
    if is_group(message):
        await message.answer(
            "🔒 Use /royalprivacidade no chat privado comigo para configurar suas opções.")
        return
    cur.execute("SELECT chat_id FROM players WHERE user_id=? LIMIT 1", (message.from_user.id,))
    row = cur.fetchone()
    if not row:
        await message.answer("Você ainda não tem perfil no Reino.")
        return
    chat_id = row["chat_id"]
    p = ensure_player(chat_id, message.from_user.id)
    db.commit()
    await message.answer(
        f"🔒 <b>Privacidade</b>\n\n"
        f"Esconder do ranking: {'✅' if p['privacy_hide_ranking'] else '❌'}\n"
        f"Esconder stats detalhados: {'✅' if p['privacy_hide_stats'] else '❌'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Toggle ranking", callback_data="r:priv:rank")],
            [InlineKeyboardButton(text="Toggle stats", callback_data="r:priv:stats")],
        ]))


@dp.message(Command("royaldados"))
async def royal_dados(message: Message):
    if not message.from_user:
        return
    if is_group(message):
        await message.answer("Use /royaldados no chat privado comigo.")
        return
    await message.answer(
        "📦 <b>Seus dados</b>\n\n"
        "Use os botões abaixo:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Exportar JSON", callback_data="r:dados:export")],
            [InlineKeyboardButton(text="🗑️ Apagar tudo", callback_data="r:dados:wipe")],
        ]))


# === Comandos do Shipper com prefixo royal ===

@dp.message(Command("royalativar", "noivado"))
async def royal_ativar(message: Message):
    if not is_group(message):
        await message.answer("Use no grupo.")
        return
    ensure_chat(message.chat.id, message.chat.title)
    cur.execute(
        "INSERT INTO chats_rpg (chat_id, current_season) VALUES (?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET current_season=excluded.current_season",
        (message.chat.id, current_season_code()))
    schedule_next_palavra(message.chat.id)
    db.commit()
    await message.answer(
        f"👑 <b>Royal ativado neste grupo!</b>\n\n"
        f"💍 Casórios automáticos: 3x ao dia\n"
        f"🎯 Palavra da Hora: a cada 60 min\n"
        f"🐉 Boss: domingos às {BOSS_SPAWN_HOUR}h\n"
        f"📅 Temporada: {current_season_label()}\n\n"
        f"💬 Chat ID: <code>{message.chat.id}</code>")


@dp.message(Command("royalcasar", "querocasar"))
async def royal_casar(message: Message):
    if not is_group(message):
        await message.answer("Use no grupo.")
        return
    if not await is_admin(message):
        await message.answer("🚫 Só admin pode invocar um casório agora.")
        return
    ensure_chat(message.chat.id, message.chat.title)
    db.commit()
    await send_couple(message.chat.id, source="manual")


@dp.message(Command("royalencalhar", "encalhado"))
async def royal_encalhar(message: Message):
    if not message.from_user:
        return
    chat_id = message.chat.id
    upsert_user(chat_id, message.from_user.id, display_name(message), message.from_user.username)
    cur.execute("UPDATE users SET opt_out=1 WHERE chat_id=? AND user_id=?",
                (chat_id, message.from_user.id))
    db.commit()
    await message.answer("🚫💔 Você entrou no modo encalhado(a). O sistema não vai mais te colocar em casórios.")


@dp.message(Command("royaldesencalhar", "desencalhar"))
async def royal_desencalhar(message: Message):
    if not message.from_user:
        return
    chat_id = message.chat.id
    upsert_user(chat_id, message.from_user.id, display_name(message), message.from_user.username)
    cur.execute("UPDATE users SET opt_out=0 WHERE chat_id=? AND user_id=?",
                (chat_id, message.from_user.id))
    db.commit()
    await message.answer("🔄💘 Você voltou para o jogo dos casórios!")


@dp.message(Command("royalmeuscasorios", "meusdivorcios"))
async def royal_meus(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    chat_id = message.chat.id
    cur.execute("SELECT COUNT(*) AS total FROM couples WHERE chat_id=? AND (user1=? OR user2=?)",
                (chat_id, uid, uid))
    total = cur.fetchone()["total"]
    cur.execute(
        """
        SELECT CASE WHEN user1=? THEN user2 ELSE user1 END AS partner, COUNT(*) AS total
        FROM couples WHERE chat_id=? AND (user1=? OR user2=?)
        GROUP BY partner ORDER BY total DESC LIMIT 5
        """, (uid, chat_id, uid, uid))
    rows = cur.fetchall()
    text = f"📊💔 <b>Seus casórios</b>\n\nVocê já participou de <b>{total}</b> casórios! 😳\n"
    if rows:
        text += "\n🔥 <b>Top pares:</b>\n"
        for row in rows:
            text += f"• {html.escape(get_name(chat_id, row['partner']))} — {row['total']}x\n"
    await message.answer(text)


@dp.message(Command("royalcasorios", "divorcios"))
async def royal_casorios(message: Message):
    chat_id = message.chat.id
    cur.execute(
        """
        SELECT user1, user2, COUNT(*) AS total FROM couples
        WHERE chat_id=? GROUP BY user1, user2 ORDER BY total DESC LIMIT 10
        """, (chat_id,))
    rows = cur.fetchall()
    if not rows:
        await message.answer("🏆💔 Ainda não existem casórios suficientes para ranking.")
        return
    text = "🏆💔 <b>Ranking dos Casórios</b>\n\n"
    for i, row in enumerate(rows, start=1):
        text += (f"{i}. {html.escape(get_name(chat_id, row['user1']))} ❤️ "
                 f"{html.escape(get_name(chat_id, row['user2']))} — {row['total']}x\n")
    await message.answer(text)


# === Botoes do menu privado ===

@dp.message(F.text == "📊 Meus casórios")
async def btn_meus(message: Message):
    await royal_meus(message)


@dp.message(F.text == "❓ Como funciona")
async def btn_como_funciona(message: Message):
    await message.answer(
        "💡 <b>Como funciona o Royal</b>\n\n"
        "👀 O bot observa interações no grupo:\n"
        "• respostas, menções, proximidade de conversa\n\n"
        "💍 <b>Casórios:</b> 3x ao dia, votação ❤️/🤮\n"
        "🎯 <b>Palavra da Hora:</b> a cada 60 min, primeiro a acertar leva\n"
        "🐉 <b>Boss da Semana:</b> domingo 20h, todos atacam juntos\n"
        "⭐ <b>XP:</b> ganho por interagir, sobe nível, distribui atributos\n"
        "🏆 <b>Temporadas:</b> seguem as estações do ano\n\n"
        "Use /royal para acessar o menu completo.")


# =====================================================================
# CALLBACKS — VOTAÇÃO CASÓRIO
# =====================================================================

@dp.callback_query(F.data.startswith(("ship_like:", "ship_dislike:")))
async def vote_cb(cb: CallbackQuery):
    if not cb.data:
        return
    action, couple_id_raw = cb.data.split(":", 1)
    couple_id = int(couple_id_raw)
    vote_type = "like" if action == "ship_like" else "dislike"
    try:
        cur.execute(
            "INSERT INTO votes (couple_id, voter_id, type, created_at) VALUES (?, ?, ?, ?)",
            (couple_id, cb.from_user.id, vote_type, utc_iso()))
        db.commit()
    except sqlite3.IntegrityError:
        await cb.answer("Você já votou nesse casório 😶")
        return
    likes, dislikes = get_votes(couple_id)
    # XP pro casal por like
    if vote_type == "like":
        cur.execute("SELECT chat_id, user1, user2 FROM couples WHERE id=?", (couple_id,))
        crow = cur.fetchone()
        if crow:
            for uid in (crow["user1"], crow["user2"]):
                award_xp_immediate(crow["chat_id"], uid, XP_VOTE_LIKE, reason="vote_like")
    try:
        await cb.message.edit_reply_markup(reply_markup=vote_keyboard(couple_id, likes, dislikes))
    except TelegramBadRequest:
        pass
    await cb.answer("Voto registrado 👑")


# =====================================================================
# CALLBACKS — HUB (r:*)
# =====================================================================

# Filtro exclui prefixos com handlers dedicados (r:inv:, r:priv:, r:dados:)
# para evitar que hub_cb capture primeiro e bloqueie os outros handlers.
_HUB_DEDICATED_PREFIXES = ("r:inv:", "r:priv:", "r:dados:")


@dp.callback_query(F.data.startswith("r:") & ~F.data.startswith(_HUB_DEDICATED_PREFIXES))
async def hub_cb(cb: CallbackQuery):
    if not cb.data or not cb.message or not cb.from_user:
        return
    parts = cb.data.split(":")
    if len(parts) < 2:
        await cb.answer()
        return
    action = parts[1]

    # chat_id efetivo: em grupo, o proprio chat; em DM, o reino mais ativo do user
    fallback = cb.message.chat.id if is_group_chat(cb.message) else None
    chat_id = resolve_owner_chat(cb.from_user.id, fallback)

    # Acoes que requerem contexto de chat Royal:
    needs_chat = {"rank", "pal", "boss", "loja", "cas", "up", "cls", "buy", "atk", "inv"}
    if action in needs_chat and chat_id is None:
        await cb.answer("Use no grupo Royal primeiro.", show_alert=True)
        return

    try:
        if action == "perfil":
            target_chat = chat_id or (cb.message.chat.id if is_group_chat(cb.message) else None)
            if target_chat is None:
                await cb.answer("Você ainda não tem perfil. Mande mensagem no grupo Royal.",
                                show_alert=True)
                return
            await send_profile_card(cb.message.chat.id, target_chat, cb.from_user.id)
            await cb.answer()
            return

        if action == "rank":
            cur.execute(
                "SELECT royal_id, user_id, season_xp, total_xp FROM players "
                "WHERE chat_id=? AND season_xp > 0 AND privacy_hide_ranking=0 "
                "ORDER BY season_xp DESC LIMIT 10", (chat_id,))
            rows = cur.fetchall()
            if not rows:
                await cb.message.answer("🏆 Ninguém pontuou ainda nesta temporada.")
            else:
                medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
                txt = f"🏆 <b>RANKING — {current_season_label()}</b>\n\n"
                for i, r in enumerate(rows, 1):
                    name = get_name(chat_id, r["user_id"])
                    lvl, *_ = level_progress(r["total_xp"])
                    txt += f"{medals[i-1]} {i}. {html.escape(name)} ({r['royal_id']}) — Lvl {lvl} · {r['season_xp']} XP\n"
                await cb.message.answer(txt)
            await cb.answer()
            return

        if action == "pal":
            ch = get_active_challenge(chat_id)
            await cb.message.answer(format_challenge_text(ch) if ch else "🎯 Sem desafio ativo agora.")
            await cb.answer()
            return

        if action == "boss":
            boss = get_active_boss(chat_id)
            if boss:
                await cb.message.answer(format_boss_text(boss),
                                        reply_markup=boss_keyboard(boss["id"]))
            else:
                await cb.message.answer(f"🐉 Sem boss agora. Próximo: domingo {BOSS_SPAWN_HOUR}h")
            await cb.answer()
            return

        if action == "help":
            await cb.message.answer(ROYAL_HELP)
            await cb.answer()
            return

        if action == "loja":
            p = ensure_player(chat_id, cb.from_user.id)
            db.commit()
            txt = f"🪙 <b>LOJA REAL</b>\nSeu saldo: <b>{p['gold']}</b> 🪙\n\n"
            for iid, item in ITEMS.items():
                txt += f"{item['emoji']} <b>{item['name']}</b> — {item['price']}🪙\n<i>{item['desc']}</i>\n\n"
            await cb.message.answer(txt, reply_markup=loja_keyboard())
            await cb.answer()
            return

        if action == "inv":
            ensure_player(chat_id, cb.from_user.id)
            db.commit()
            cur.execute(
                "SELECT item_id, qty, equipped FROM inventory "
                "WHERE chat_id=? AND user_id=? AND qty>0 ORDER BY equipped DESC",
                (chat_id, cb.from_user.id))
            rows = cur.fetchall()
            if not rows:
                await cb.message.answer("🎒 Seu inventário está vazio. Visite a /royalloja!")
            else:
                txt = "🎒 <b>Inventário</b>\n\n"
                for r in rows:
                    it = ITEMS.get(r["item_id"])
                    if not it:
                        continue
                    eq = "✅ " if r["equipped"] else ""
                    txt += f"{eq}{it['emoji']} <b>{it['name']}</b> x{r['qty']}\n<i>{it['desc']}</i>\n\n"
                await cb.message.answer(txt, reply_markup=inv_keyboard(chat_id, cb.from_user.id))
            await cb.answer()
            return

        if action == "cas":
            cur.execute(
                "SELECT user1, user2, COUNT(*) AS total FROM couples "
                "WHERE chat_id=? GROUP BY user1, user2 ORDER BY total DESC LIMIT 10",
                (chat_id,))
            rows = cur.fetchall()
            if not rows:
                await cb.message.answer("💍 Sem casórios ainda.")
            else:
                txt = "🏆💔 <b>Ranking dos Casórios</b>\n\n"
                for i, r in enumerate(rows, 1):
                    txt += (f"{i}. {html.escape(get_name(chat_id, r['user1']))} ❤️ "
                            f"{html.escape(get_name(chat_id, r['user2']))} — {r['total']}x\n")
                await cb.message.answer(txt)
            await cb.answer()
            return

        if action == "up":
            sub = parts[2] if len(parts) > 2 else "menu"
            if sub == "menu":
                p = ensure_player(chat_id, cb.from_user.id)
                db.commit()
                await cb.message.answer(
                    f"⬆️ Você tem <b>{p['pts_available']}</b> pontos. Escolha:",
                    reply_markup=up_keyboard())
                await cb.answer()
                return
            attr = sub
            if attr not in {"for", "des", "vit", "car"}:
                await cb.answer()
                return
            p = ensure_player(chat_id, cb.from_user.id)
            if p["pts_available"] <= 0:
                await cb.answer("Sem pontos disponíveis.", show_alert=True)
                return
            cur.execute(
                f"UPDATE players SET attr_{attr}=attr_{attr}+1, pts_available=pts_available-1 "
                f"WHERE chat_id=? AND user_id=? AND pts_available>0",
                (chat_id, cb.from_user.id))
            db.commit()
            p = ensure_player(chat_id, cb.from_user.id)
            await cb.answer(f"+1 {attr.upper()} ({p['pts_available']} pts restantes)")
            return

        if action == "cls":
            sub = parts[2] if len(parts) > 2 else "menu"
            if sub == "menu":
                txt = "🎭 <b>Escolha sua classe</b>\n\n"
                for cid, info in CLASSES.items():
                    txt += f"{info['emoji']} <b>{info['name']}</b> — {info['bonus']}\n"
                await cb.message.answer(txt, reply_markup=classe_keyboard())
                await cb.answer()
                return
            if sub == "set" and len(parts) > 3:
                cid = parts[3]
                if cid not in CLASSES:
                    await cb.answer()
                    return
                ensure_player(chat_id, cb.from_user.id)
                cur.execute("UPDATE players SET class_id=? WHERE chat_id=? AND user_id=?",
                            (cid, chat_id, cb.from_user.id))
                db.commit()
                await cb.answer(f"Classe: {CLASSES[cid]['name']} ✓")
                return
            await cb.answer()
            return

        if action == "buy":
            if len(parts) < 3:
                await cb.answer()
                return
            iid = parts[2]
            item = ITEMS.get(iid)
            if not item:
                await cb.answer("Item inexistente", show_alert=True)
                return
            p = ensure_player(chat_id, cb.from_user.id)
            if p["gold"] < item["price"]:
                await cb.answer("🪙 Florins insuficientes", show_alert=True)
                return
            # debita atomicamente
            cur.execute(
                "UPDATE players SET gold=gold-? "
                "WHERE chat_id=? AND user_id=? AND gold>=?",
                (item["price"], chat_id, cb.from_user.id, item["price"]))
            if cur.rowcount == 0:
                await cb.answer("🪙 Saldo insuficiente.", show_alert=True)
                return
            cur.execute(
                "INSERT INTO inventory (chat_id, user_id, item_id, qty) VALUES (?, ?, ?, 1) "
                "ON CONFLICT(chat_id, user_id, item_id) DO UPDATE SET qty=qty+1",
                (chat_id, cb.from_user.id, iid))
            db.commit()
            await cb.answer(f"Comprou {item['name']} ✓")
            return

        if action == "atk":
            if len(parts) < 3:
                await cb.answer()
                return
            await handle_boss_attack(cb, int(parts[2]))
            return

        await cb.answer()
    except Exception:
        logger.exception("hub_cb failed for data=%s", cb.data)
        try:
            await cb.answer("Erro interno", show_alert=False)
        except Exception:
            pass


def is_group_chat(message: Message) -> bool:
    return message.chat.type in {"group", "supergroup"}


# === Callback: inventario equipa/usa ===

@dp.callback_query(F.data.startswith("r:inv:"))
async def inv_cb(cb: CallbackQuery):
    if not cb.data or not cb.from_user or not cb.message:
        return
    parts = cb.data.split(":")
    if len(parts) < 4:
        await cb.answer()
        return
    sub = parts[2]
    iid = parts[3]
    chat_id = cb.message.chat.id
    uid = cb.from_user.id

    if sub == "eq":
        cur.execute("SELECT equipped FROM inventory WHERE chat_id=? AND user_id=? AND item_id=?",
                    (chat_id, uid, iid))
        row = cur.fetchone()
        if not row:
            await cb.answer()
            return
        new_eq = 0 if row["equipped"] else 1
        if new_eq == 1:
            # desequipa outros do mesmo stat
            item = ITEMS.get(iid, {})
            if item.get("stat"):
                cur.execute(
                    "SELECT inventory.item_id FROM inventory WHERE chat_id=? AND user_id=? AND equipped=1",
                    (chat_id, uid))
                for r in cur.fetchall():
                    other = ITEMS.get(r["item_id"])
                    if other and other.get("stat") == item.get("stat"):
                        cur.execute(
                            "UPDATE inventory SET equipped=0 WHERE chat_id=? AND user_id=? AND item_id=?",
                            (chat_id, uid, r["item_id"]))
        cur.execute("UPDATE inventory SET equipped=? WHERE chat_id=? AND user_id=? AND item_id=?",
                    (new_eq, chat_id, uid, iid))
        db.commit()
        await cb.answer("Equipado ✓" if new_eq else "Desequipado")
        return

    if sub == "use":
        item = ITEMS.get(iid, {})
        if item.get("type") != "consumable":
            await cb.answer()
            return
        effect = item.get("effect")
        if effect == "xp100":
            award_xp_immediate(chat_id, uid, 100, reason="tomo")
            cur.execute(
                "UPDATE inventory SET qty=qty-1 WHERE chat_id=? AND user_id=? AND item_id=?",
                (chat_id, uid, iid))
            cur.execute("DELETE FROM inventory WHERE chat_id=? AND user_id=? AND item_id=? AND qty<=0",
                        (chat_id, uid, iid))
            db.commit()
            await cb.answer("+100 XP ✓")
            return
        if effect == "heal":
            cur.execute(
                "UPDATE inventory SET qty=qty-1 WHERE chat_id=? AND user_id=? AND item_id=?",
                (chat_id, uid, iid))
            cur.execute("DELETE FROM inventory WHERE chat_id=? AND user_id=? AND item_id=? AND qty<=0",
                        (chat_id, uid, iid))
            db.commit()
            await cb.answer("HP restaurado ✓")
            return
        await cb.answer()
        return

    await cb.answer()


# === Callback: privacidade ===

@dp.callback_query(F.data.startswith("r:priv:"))
async def priv_cb(cb: CallbackQuery):
    if not cb.data or not cb.from_user:
        return
    parts = cb.data.split(":")
    if len(parts) < 3:
        await cb.answer()
        return
    sub = parts[2]
    cur.execute("SELECT chat_id FROM players WHERE user_id=? LIMIT 1", (cb.from_user.id,))
    row = cur.fetchone()
    if not row:
        await cb.answer("Sem perfil", show_alert=True)
        return
    chat_id = row["chat_id"]
    if sub == "rank":
        cur.execute(
            "UPDATE players SET privacy_hide_ranking=1-privacy_hide_ranking "
            "WHERE chat_id=? AND user_id=?", (chat_id, cb.from_user.id))
    elif sub == "stats":
        cur.execute(
            "UPDATE players SET privacy_hide_stats=1-privacy_hide_stats "
            "WHERE chat_id=? AND user_id=?", (chat_id, cb.from_user.id))
    db.commit()
    await cb.answer("Atualizado ✓")


# === Callback: dados (export/wipe) ===

@dp.callback_query(F.data.startswith("r:dados:"))
async def dados_cb(cb: CallbackQuery):
    assert bot is not None
    if not cb.data or not cb.from_user:
        return
    parts = cb.data.split(":")
    sub = parts[2] if len(parts) > 2 else ""
    uid = cb.from_user.id
    if sub == "export":
        cur.execute("SELECT * FROM players WHERE user_id=?", (uid,))
        players = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM users WHERE user_id=?", (uid,))
        users = [dict(r) for r in cur.fetchall()]
        import json
        payload = json.dumps({"players": players, "users": users}, indent=2, default=str)
        if len(payload) < 3500:
            await bot.send_message(cb.from_user.id, f"<pre>{html.escape(payload)}</pre>")
        else:
            await bot.send_message(cb.from_user.id, "📦 Dados muito grandes. Resumo:\n" +
                                   f"Perfis: {len(players)}\nRegistros user: {len(users)}")
        await cb.answer("Enviado por DM")
        return
    if sub == "wipe":
        cur.execute("DELETE FROM players WHERE user_id=?", (uid,))
        cur.execute("DELETE FROM inventory WHERE user_id=?", (uid,))
        cur.execute("UPDATE users SET opt_out=1 WHERE user_id=?", (uid,))
        db.commit()
        await cb.answer("Dados apagados ✓", show_alert=True)
        return
    await cb.answer()


# =====================================================================
# TRACK — handler universal de mensagens em grupo
# (deve ser o ULTIMO @dp.message pra nao capturar comandos)
# =====================================================================

@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def track(message: Message):
    if not message.from_user:
        return
    if message.text and message.text.startswith("/"):
        return
    chat_id = message.chat.id
    uid = message.from_user.id
    name = display_name(message)
    username = message.from_user.username
    ensure_chat(chat_id, message.chat.title)
    upsert_user(chat_id, uid, name, username)
    activity_buffer[(chat_id, uid, today_key())] += 1

    # RPG: garante player + XP por mensagem
    try:
        ensure_player(chat_id, uid)
        is_reply = bool(
            message.reply_to_message and message.reply_to_message.from_user
            and not message.reply_to_message.from_user.is_bot
            and message.reply_to_message.from_user.id != uid
        )
        award_xp_message(chat_id, uid, is_reply=is_reply)
    except Exception:
        logger.exception("RPG XP failed")

    # PALAVRA: checa desafio ativo
    try:
        ch = get_active_challenge(chat_id)
        if ch and message.text:
            await handle_palavra_attempt(message, ch)
    except Exception:
        logger.exception("palavra attempt failed")

    # SHIPPER: afinidade
    now_ts = utc_now().timestamp()
    if message.reply_to_message and message.reply_to_message.from_user and not message.reply_to_message.from_user.is_bot:
        target = message.reply_to_message.from_user.id
        if target != uid:
            u1, u2 = normalize_pair(uid, target)
            pair_buffer[(chat_id, u1, u2)] += 6

    if message.entities:
        for entity in message.entities:
            if entity.type == "text_mention" and entity.user and entity.user.id != uid and not entity.user.is_bot:
                u1, u2 = normalize_pair(uid, entity.user.id)
                pair_buffer[(chat_id, u1, u2)] += 4

    for previous_uid, previous_ts in list(recent_messages[chat_id]):
        if previous_uid != uid and now_ts - previous_ts <= RECENT_WINDOW_SECONDS:
            u1, u2 = normalize_pair(uid, previous_uid)
            pair_buffer[(chat_id, u1, u2)] += 1
    recent_messages[chat_id].append((uid, now_ts))


# =====================================================================
# TASKS ASSÍNCRONAS
# =====================================================================

async def flush_buffers():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        try:
            for (chat_id, uid, day), count in list(activity_buffer.items()):
                cur.execute(
                    "INSERT INTO daily_activity (chat_id, user_id, day, message_count) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(chat_id, user_id, day) "
                    "DO UPDATE SET message_count=message_count + excluded.message_count",
                    (chat_id, uid, day, count))
            activity_buffer.clear()

            for (chat_id, u1, u2), score in list(pair_buffer.items()):
                cur.execute(
                    "INSERT INTO pair_scores (chat_id, user1, user2, score, last_seen) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(chat_id, user1, user2) "
                    "DO UPDATE SET score=score + excluded.score, last_seen=excluded.last_seen",
                    (chat_id, u1, u2, score, utc_iso()))
            pair_buffer.clear()
            db.commit()
        except Exception:
            logger.exception("buffer flush failed")


async def cleanup_job():
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff_pairs = (utc_now() - timedelta(days=3)).isoformat()
            cutoff_activity = (local_now() - timedelta(days=7)).date().isoformat()
            cur.execute("DELETE FROM pair_scores WHERE last_seen < ?", (cutoff_pairs,))
            cur.execute("DELETE FROM daily_activity WHERE day < ?", (cutoff_activity,))

            now = utc_now().timestamp()
            expired_admin = [k for k, (_, exp) in admin_cache.items() if exp <= now]
            for k in expired_admin:
                admin_cache.pop(k, None)
            expired_photo = [k for k, (_, exp) in photo_cache.items() if exp <= now]
            for k in expired_photo:
                photo_cache.pop(k, None)

            # limpa cooldowns antigos (>1h)
            cutoff_ts = now - 3600
            for k in list(attempt_cooldowns.keys()):
                if attempt_cooldowns[k] < cutoff_ts:
                    attempt_cooldowns.pop(k, None)
            for k in list(boss_attack_cooldowns.keys()):
                if boss_attack_cooldowns[k] < cutoff_ts:
                    boss_attack_cooldowns.pop(k, None)

            db.commit()
            logger.info("cleanup_job ok (admin=%d photo=%d)",
                        len(expired_admin), len(expired_photo))
        except Exception:
            logger.exception("cleanup_job failed")


async def scheduler():
    """Loop unificado: casórios, palavra, boss, temporadas."""
    last_minute = -1
    while True:
        await asyncio.sleep(20)
        try:
            now_local = local_now()

            # === SHIPPER: casórios automáticos por hora ===
            if now_local.hour in AUTO_HOURS:
                marker = now_local.strftime("%Y-%m-%d-%H")
                cur.execute("SELECT chat_id, last_auto_post FROM chats WHERE enabled=1")
                for row in cur.fetchall():
                    if row["last_auto_post"] == marker:
                        continue
                    ok = await send_couple(row["chat_id"], source="auto")
                    if ok:
                        cur.execute("UPDATE chats SET last_auto_post=? WHERE chat_id=?",
                                    (marker, row["chat_id"]))
                        db.commit()

            # === PALAVRA DA HORA ===
            cur.execute("SELECT chat_id FROM chats WHERE enabled=1")
            enabled = [r["chat_id"] for r in cur.fetchall()]
            for chat_id in enabled:
                cur.execute("SELECT next_palavra_at FROM chats_rpg WHERE chat_id=?", (chat_id,))
                r = cur.fetchone()
                if not r or not r["next_palavra_at"]:
                    schedule_next_palavra(chat_id)
                    continue
                try:
                    nxt = datetime.fromisoformat(r["next_palavra_at"])
                except Exception:
                    schedule_next_palavra(chat_id)
                    continue
                if now_local >= nxt:
                    # Spawna se nao tem um ativo
                    if not get_active_challenge(chat_id):
                        await spawn_palavra(chat_id)
                    schedule_next_palavra(chat_id)

            # === FINALIZA DESAFIOS EXPIRADOS ===
            await finalize_expired_challenges()

            # === BOSS SEMANAL ===
            await spawn_boss_if_due()

            # === TEMPORADAS (check a cada minuto, no segundo 0-20) ===
            if now_local.minute != last_minute:
                last_minute = now_local.minute
                await check_season_change()

        except Exception:
            logger.exception("scheduler failed")


async def healthcheck():
    while True:
        await asyncio.sleep(300)
        try:
            logger.info(
                "healthcheck activity_buf=%d pair_buf=%d admin_cache=%d photo_cache=%d",
                len(activity_buffer), len(pair_buffer),
                len(admin_cache), len(photo_cache))
        except Exception:
            logger.exception("healthcheck failed")


# =====================================================================
# BOT COMMANDS REGISTRATION
# =====================================================================

async def register_bot_commands():
    assert bot is not None
    group_cmds = [
        BotCommand(command="royal",             description="👑 Menu principal"),
        BotCommand(command="royalperfil",       description="📜 Ver perfil"),
        BotCommand(command="royalficha",        description="📜 Ver ficha"),
        BotCommand(command="royalup",           description="⬆️ Distribuir pontos"),
        BotCommand(command="royalclasse",       description="🎭 Escolher classe"),
        BotCommand(command="royalinventario",   description="🎒 Ver inventário"),
        BotCommand(command="royalloja",         description="🪙 Loja"),
        BotCommand(command="royalsaldo",        description="💰 Saldo"),
        BotCommand(command="royalranking",      description="🏆 Ranking da temporada"),
        BotCommand(command="royalpalavra",      description="🎯 Palavra da hora"),
        BotCommand(command="royalboss",         description="🐉 Boss da semana"),
        BotCommand(command="royalcasorios",     description="💍 Ranking dos casais"),
        BotCommand(command="royalmeuscasorios", description="📊 Meus casórios"),
        BotCommand(command="royalencalhar",     description="🚫 Sair dos casórios"),
        BotCommand(command="royaldesencalhar",  description="💘 Voltar pros casórios"),
        BotCommand(command="royalcasar",        description="💍 (admin) Forçar casório"),
        BotCommand(command="royalativar",       description="🔧 (admin) Ativar bot"),
        BotCommand(command="royalajuda",        description="❓ Ajuda completa"),
    ]
    private_cmds = [
        BotCommand(command="royal",             description="👑 Menu principal"),
        BotCommand(command="royalperfil",       description="📜 Meu perfil"),
        BotCommand(command="royalajuda",        description="❓ Ajuda"),
        BotCommand(command="royalprivacidade",  description="🔒 Privacidade"),
        BotCommand(command="royaldados",        description="📦 Meus dados"),
    ]
    try:
        await bot.set_my_commands(group_cmds, scope=BotCommandScopeAllGroupChats())
        await bot.set_my_commands(private_cmds, scope=BotCommandScopeAllPrivateChats())
    except Exception:
        logger.exception("set_my_commands failed")


# =====================================================================
# ENTRY POINT
# =====================================================================

async def main():
    global bot
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is required")
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    logger.info("Royal RPG starting")
    await register_bot_commands()
    asyncio.create_task(flush_buffers())
    asyncio.create_task(cleanup_job())
    asyncio.create_task(scheduler())
    asyncio.create_task(healthcheck())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
