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
import io
import logging
import os
import random
import sqlite3
import time
import unicodedata
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart, Filter
from aiogram.types import (
    BotCommand,
    BotCommandScopeAllChatAdministrators,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InlineQueryResultCachedPhoto,
    InlineQueryResultsButton,
    InputFile,
    InputTextMessageContent,
    KeyboardButton,
    Message,
    ReactionTypeEmoji,
    ReplyKeyboardMarkup,
)

import royal_avatars
from royal_words import PALAVRAS, CHARADAS
from royal_render import (
    CasorioPartner,
    ProfileCardData,
    RankingEntry,
    render_casorio_card,
    render_levelup_card,
    render_palavra_spoiler_card,
    render_profile_card,
    render_ranking_card,
)

# =====================================================================
# CONFIG & LOGGING
# =====================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("royal-casorios")

BOT_TOKEN = os.getenv("BOT_TOKEN")
# Grupos de teste — separados por virgula. Esses chat_ids NAO aparecem
# na lista de grupos do picker em DM nem na lista de inline mode.
# Ex.: TEST_CHAT_IDS="-1001234567890,-1009876543210"
TEST_CHAT_IDS: set[int] = {
    int(x.strip())
    for x in os.getenv("TEST_CHAT_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
}

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

# Bot de musica externo: quando publica uma faixa marcando um user via
# text_mention, pontuamos o user marcado com 3x o XP de mensagem normal.
# 0 desativa o recurso. Privacy mode do bot RPG precisa estar OFF pra
# enxergar mensagens deste bot no grupo.
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

# Baú Real — spawna 30min após cada Palavra; primeiros 5 abrem
CHEST_DELAY_MIN = 30
CHEST_TTL_MIN = 30
CHEST_MAX_CLAIMS = 5
# (xp, gold) por ordem de chegada: 1º até 5º
CHEST_REWARDS = [(150, 10), (100, 10), (75, 10), (50, 10), (25, 10)]

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


def _ensure_db_persistence() -> None:
    """
    Garante persistencia do DB entre deploys.

    - Loga path absoluto + tamanho atual do arquivo.
    - Se DB_PATH aponta pra lugar vazio (1a boot apos volume novo no
      Railway) e existe um seed no repo em ./data/royal_casorios.sqlite3,
      copia ele pra la (one-time bootstrap).
    - Avisa em CAPS se DB_PATH eh relativo (provavelmente efemero em
      container sem volume mountado).
    """
    import shutil
    target = os.path.abspath(DB_PATH)
    repo_seed = os.path.abspath("./data/royal_casorios.sqlite3")

    if os.path.exists(target):
        size_kb = os.path.getsize(target) // 1024
        logger.info("[DB] usando %s (%d KB)", target, size_kb)
    else:
        if os.path.exists(repo_seed) and repo_seed != target:
            shutil.copy2(repo_seed, target)
            size_kb = os.path.getsize(target) // 1024
            logger.warning(
                "[DB] BOOTSTRAP: copiei seed do repo %s -> %s (%d KB)",
                repo_seed, target, size_kb,
            )
        else:
            logger.warning("[DB] criando NOVO arquivo vazio em %s", target)

    if not os.path.isabs(DB_PATH):
        logger.warning(
            "[DB] !! ATENCAO: DATABASE_PATH eh relativo (%s). Em "
            "container (Railway) sem volume mountado, esse arquivo "
            "EVAPORA a cada deploy. Configure um volume e setenv "
            "DATABASE_PATH=/data/royal_casorios.sqlite3",
            DB_PATH,
        )


_ensure_db_persistence()

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

        CREATE TABLE IF NOT EXISTS chests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            spawn_at TEXT NOT NULL,
            spawned_at TEXT,
            expires_at TEXT,
            message_id INTEGER,
            status TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE INDEX IF NOT EXISTS idx_chests_status_spawn
            ON chests(status, spawn_at);
        CREATE INDEX IF NOT EXISTS idx_chests_chat_status
            ON chests(chat_id, status);

        CREATE TABLE IF NOT EXISTS chest_claims (
            chest_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            slot INTEGER NOT NULL,
            xp INTEGER NOT NULL,
            gold INTEGER NOT NULL,
            name TEXT,
            claimed_at TEXT,
            PRIMARY KEY(chest_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_chest_claims_chest
            ON chest_claims(chest_id);

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


def migrate_to_v4(c: sqlite3.Cursor) -> None:
    """Persistencia da escolha de 'grupo ativo' do user em DM. Quando o
    user roda comandos em DM, esse eh o chat-dono usado por padrao.
    NULL = sem escolha; sera perguntado via picker."""
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS user_dm_settings (
            user_id INTEGER PRIMARY KEY,
            active_chat_id INTEGER,
            set_at TEXT
        );
        """
    )


def migrate_to_v3(c: sqlite3.Cursor) -> None:
    """Avatar do player (escolhido via /royalavatar). avatar_slug eh o
    identificador do PNG em assets/avatars/. avatar_season grava a label
    da temporada em que foi escolhido (pra enforce 1x por temporada).
    Ambos NULL = usa default deterministico por royal_id."""
    # ADD COLUMN eh idempotente apenas se nao existe — guard com try/except
    for col, ddl in (
        ("avatar_slug",   "ALTER TABLE players ADD COLUMN avatar_slug TEXT"),
        ("avatar_season", "ALTER TABLE players ADD COLUMN avatar_season TEXT"),
    ):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise


MIGRATIONS = [
    (1, migrate_to_v1),
    (2, migrate_to_v2),
    (3, migrate_to_v3),
    (4, migrate_to_v4),
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
        [KeyboardButton(text="👑 Reino"), KeyboardButton(text="📖 Tutorial")],
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
    """Nome publico do user: display name (full_name/first_name) e, se nao
    tiver, fallback pro @username. Se nem isso, retorna string vazia — o
    chamador (refresh_user_identity/anonize) decide o ANON-X final."""
    user = message.from_user
    if not user:
        return ""
    return (user.full_name or user.first_name
            or (f"@{user.username}" if user.username else ""))


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


# =====================================================================
# UX HELPERS — message effects (Bot API 7.7+, DM-only) + chat actions
# =====================================================================

EFFECT_FIRE = "5104841245755180586"
EFFECT_THUMBS_UP = "5107584321108051014"
EFFECT_HEART = "5044134455711629726"
EFFECT_PARTY = "5046509860389126442"
EFFECT_POO = "5046589136895476101"
EFFECT_THUMBS_DOWN = "5104858069142078462"


def effect_kw(chat_type: str | None, effect_id: str) -> dict:
    """message_effect_id so funciona em DM 1:1; em grupos retorna dict vazio."""
    return {"message_effect_id": effect_id} if chat_type == "private" else {}


async def safe_typing(chat_id: int, action: str = "typing") -> None:
    """Envia chat action ignorando falhas (nao precisa admin)."""
    if bot is None:
        return
    try:
        await bot.send_chat_action(chat_id, action)
    except Exception:
        pass


# =====================================================================
# UI: "cores" via emoji + auto-delete + bot reactions + typing realista
# =====================================================================
# BOTOES COLORIDOS — Bot API 10 / aiogram 3.28.2
# InlineKeyboardButton e KeyboardButton aceitam `style=` nativo:
#   'success' -> verde   |  'danger' -> vermelho   |  'primary' -> azul
#   omitido   -> cor do tema (app-specific default)
# Doc: https://docs.aiogram.dev/en/latest/api/enums/button_style.html
#
# Mantemos TAMBEM emoji lider no texto pra:
#   (a) clientes antigos que ainda nao renderizam style
#   (b) leitores de tela e modo monocromo
# Convencao adotada — use SEMPRE em pares (emoji + style):

STYLE_OK   = "success"   # verde
STYLE_NO   = "danger"    # vermelho
STYLE_INFO = "primary"   # azul

BTN_OK   = "✅"   # par com STYLE_OK     — confirmar / aplicar / ir
BTN_NO   = "❌"   # par com STYLE_NO     — cancelar / fechar / destrutivo
BTN_INFO = "🔵"   # par com STYLE_INFO   — informacao / navegar / abrir
BTN_WARN = "⚠️"   # SEM style nativo (API 10 nao tem 'warning')
BTN_BACK = "◀️"   # neutro    — voltar
BTN_GO   = "▶️"   # neutro    — avancar / proximo


def ikb(text: str, *, style: str | None = None, **kwargs) -> InlineKeyboardButton:
    """Atalho pra InlineKeyboardButton com style opcional (Bot API 10).
    Uso: ikb('✅ Ok', style=STYLE_OK, callback_data='x')"""
    if style:
        return InlineKeyboardButton(text=text, style=style, **kwargs)
    return InlineKeyboardButton(text=text, **kwargs)


async def auto_delete_after(msg: Message, delay: float = 8.0) -> None:
    """Agenda exclusao da mensagem em N segundos sem bloquear o handler.
    Ideal pra acks efemeros (rate-limit, '0 pontos', etc) — mantem o chat
    limpo sem o usuario precisar deletar manualmente."""
    if bot is None or msg is None:
        return
    async def _task():
        try:
            await asyncio.sleep(delay)
            await bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass
    asyncio.create_task(_task())


async def react_to(chat_id: int, message_id: int, emoji: str,
                   big: bool = False) -> None:
    """Bot reage com emoji a uma mensagem do usuario (setMessageReaction,
    Bot API 7.0+). So aceita emojis da whitelist oficial do Telegram —
    falha silenciosa pra qualquer outro."""
    if bot is None:
        return
    try:
        await bot.set_message_reaction(
            chat_id=chat_id, message_id=message_id,
            reaction=[ReactionTypeEmoji(emoji=emoji)],
            is_big=big,
        )
    except Exception as e:
        logger.warning("react_to falhou chat=%s mid=%s emoji=%r: %s",
                       chat_id, message_id, emoji, e)


async def type_then_send(chat_id: int, text: str, delay: float = 1.2,
                         action: str = "typing", **kwargs):
    """Mostra '... digitando' por `delay` segundos antes de mandar o texto.
    Da sensacao de 'humano pensando'. Pra fotos use action='upload_photo'.
    Repassa **kwargs pro send_message (reply_markup, parse_mode, etc)."""
    if bot is None:
        return None
    await safe_typing(chat_id, action)
    await asyncio.sleep(delay)
    return await bot.send_message(chat_id, text, **kwargs)


async def reveal_text(chat_id: int, full_text: str,
                      *, chunk_delay: float = 0.7,
                      seed_text: str = ">> ...",
                      parse_mode: str | None = "HTML",
                      **kwargs):
    """Anima a "digitacao" de uma mensagem editando-a em chunks.

    Quebra `full_text` por linha (se >=2) ou em 3 metades. Respeita
    rate-limit do Telegram (1 edit/seg em grupo, ~3/seg em DM) usando
    `chunk_delay`. Em caso de TelegramRetryAfter, espera e segue.
    Retorna o Message final (com todas as edicoes aplicadas) ou None.
    NAO usar pra ack curto — eh deliberadamente lento (cinematografico).
    """
    if bot is None:
        return None
    try:
        msg = await bot.send_message(chat_id, seed_text,
                                     parse_mode=parse_mode, **kwargs)
    except Exception:
        return None
    lines = [l for l in full_text.split("\n") if l.strip()]
    chunks: list[str] = []
    if len(lines) >= 2:
        acc = ""
        for l in lines:
            acc = (acc + "\n" + l) if acc else l
            chunks.append(acc)
    else:
        n = max(1, len(full_text) // 3)
        for k in (n, 2 * n, len(full_text)):
            chunks.append(full_text[:k])
    for chunk in chunks:
        await asyncio.sleep(chunk_delay)
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=msg.message_id,
                text=chunk, parse_mode=parse_mode,
            )
        except TelegramRetryAfter as e:
            try:
                await asyncio.sleep(getattr(e, "retry_after", 2))
            except Exception:
                pass
        except Exception:
            pass
    return msg


def hp_bar(current: int | float, maximum: int | float, width: int = 12) -> str:
    """Barra visual estilo `█████░░░░░ NN%` pra HP, XP, etc.
    Saturada em [0,100]%. Usar dentro de <pre>...</pre> pro alinhamento."""
    try:
        cur_v = float(current or 0)
        max_v = float(maximum or 1)
    except (TypeError, ValueError):
        return "░" * width + "  0%"
    pct = max(0.0, min(1.0, cur_v / max(1.0, max_v)))
    filled = int(round(pct * width))
    return "█" * filled + "░" * (width - filled) + f" {int(pct * 100):>3}%"


async def roll_dice_visual(chat_id: int, emoji: str = "🎲",
                           settle_delay: float = 0.0) -> int | None:
    """Envia dado animado nativo do Telegram (🎲 🎯 🎰 🏀 ⚽ 🎳).

    O cliente anima ~3s ate parar. Retorna o valor sorteado pelo
    Telegram (1-6 pra dado/dart/bowling/basquete/futebol; 1-64 pra slot).

    ATENCAO: NAO usar pra calculo de jogo — eh flourish visual. O valor
    eh decidido pelo servidor do Telegram no momento do envio.
    `settle_delay` so vale a pena se voce precisa do valor de retorno.
    """
    if bot is None:
        return None
    try:
        msg = await bot.send_dice(chat_id, emoji=emoji)
        if settle_delay > 0:
            await asyncio.sleep(settle_delay)
        return msg.dice.value if msg and msg.dice else None
    except Exception:
        return None


# Anti-spam: cooldown por (uid, acao). Pensado pra 500+ users simultaneos.
# Comandos pesados (perfil, ranking, render) limitam 1 chamada / cooldown.
_rate_limits: dict[tuple[int, str], float] = {}
_RATE_LIMIT_GC_INTERVAL = 600   # 10 min entre garbage collects
_RATE_LIMIT_TTL = 3600          # entradas mais velhas que 1h sao removidas
_rate_limits_last_gc: float = 0.0


def rate_limited(uid: int, action: str, cooldown: float = 10.0) -> int:
    """Retorna 0 se OK, ou segundos restantes se ainda em cooldown.
    Faz GC oportunistico do dict (evita crescimento ilimitado em prod)."""
    global _rate_limits_last_gc
    now = utc_now().timestamp()
    if now - _rate_limits_last_gc > _RATE_LIMIT_GC_INTERVAL:
        # snapshot pra evitar 'dict changed size during iteration'
        cutoff = now - _RATE_LIMIT_TTL
        stale = [k for k, ts in list(_rate_limits.items()) if ts < cutoff]
        for k in stale:
            _rate_limits.pop(k, None)
        _rate_limits_last_gc = now
    last = _rate_limits.get((uid, action), 0.0)
    if now - last < cooldown:
        return int(cooldown - (now - last)) + 1
    _rate_limits[(uid, action)] = now
    return 0


async def deny_if_rate_limited(message: Message, action: str,
                                cooldown: float = 10.0) -> bool:
    """Helper: responde com aviso e retorna True se rate-limited."""
    if not message.from_user:
        return False
    wait = rate_limited(message.from_user.id, action, cooldown)
    if wait > 0:
        await message.answer(
            term_block(action, f"⏳ Aguarde <b>{wait}s</b> antes de invocar de novo.",
                       status="THROTTLED", status_color="AMBER"))
        return True
    return False


GROUP_ONLY_MSG = (
    "🏰 Esse comando vive nos grupos do Reino.\n"
    "<i>Volta pro grupo Royal pra usar ele lá ✨</i>"
)


# =====================================================================
# UX VOZ — terminal retro-futurista dystopian (combina com royal_render)
# Use term_block() pra QUALQUER resposta de texto do bot. Fica leve
# (sem render de imagem) mas mantem identidade visual do reino.
# =====================================================================

def term_block(title: str, body: str, *,
               status: str = "OK",
               status_color: str = "ACID",
               stamp: str | None = None) -> str:
    """
    Resposta padrao em formato terminal retro:

        > TITLE.SYS  // <status>
        ─────────────────────────
        body (pode usar <b>, <i>, <code>, <pre>, etc.)
        <i>[ stamp ]</i>

    Usa <code> no header e <blockquote expandable> quando o body for grande.
    `status_color`: ACID (verde), HOT (vermelho), AMBER (laranja), CYAN.
    """
    status_emoji = {
        "ACID": "🟢", "HOT": "🔴", "AMBER": "🟡", "CYAN": "🔵",
    }.get(status_color, "🟢")
    head = f"<code>&gt; {html.escape(title.upper())}.SYS</code>  {status_emoji} <i>{html.escape(status)}</i>"
    parts = [head, body.strip()]
    if stamp:
        parts.append(f"<i>[ {html.escape(stamp)} ]</i>")
    return "\n".join(parts)


def term_pre(rows: list[tuple[str, str]]) -> str:
    """Tabela 2-col em <pre> (monospace) — chave alinhada esquerda, valor direita."""
    if not rows:
        return ""
    key_w = max(len(k) for k, _ in rows)
    val_w = max(len(str(v)) for _, v in rows)
    width = key_w + val_w + 6  # 6 = padding " :: "
    lines = []
    for k, v in rows:
        pad = " " * (width - key_w - len(str(v)) - 4)
        lines.append(f"{k.upper():<{key_w}} ::{pad}{v}")
    return "<pre>" + html.escape("\n".join(lines)) + "</pre>"


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


def refresh_user_identity(chat_id: int, user_id: int, name: str,
                          username: str | None) -> None:
    """Atualiza apenas display_name/username/last_seen SEM incrementar
    message_count. Usar antes de renderizar perfil/cartao p/ garantir que
    o nome vivo do Telegram seja escrito no DB, evitando que get_name caia
    pro fallback str(user_id) (digitos) e dispare anonize() no proprio dono.

    Diferente de upsert_user, NAO conta como atividade — pode ser chamado
    a partir de DM/callback sem distorcer ranking/shipper do grupo.
    """
    if not name:
        return
    cur.execute(
        """
        INSERT INTO users (user_id, chat_id, display_name, username, message_count, last_seen)
        VALUES (?, ?, ?, ?, 0, ?)
        ON CONFLICT(user_id, chat_id) DO UPDATE SET
            display_name=excluded.display_name,
            username=excluded.username,
            last_seen=excluded.last_seen
        """,
        (user_id, chat_id, name, username, utc_iso()),
    )


def get_name(chat_id: int, user_id: int) -> str:
    cur.execute("SELECT display_name FROM users WHERE chat_id=? AND user_id=?",
                (chat_id, user_id))
    row = cur.fetchone()
    return row["display_name"] if row else str(user_id)


def get_username(chat_id: int, user_id: int) -> str | None:
    """Retorna @username (sem @) salvo na tabela users, ou None."""
    cur.execute("SELECT username FROM users WHERE chat_id=? AND user_id=?",
                (chat_id, user_id))
    row = cur.fetchone()
    u = (row["username"] if row else None) or None
    return u.lstrip("@") if u else None


# Faixas Unicode usadas por geradores de "fonte fancy" (lingojam, fancytext,
# coolsymbol). NÃO são fontes — são codepoints reais. Telegram renderiza
# nativamente, mas Pillow + DejaVu NÃO cobrem essas faixas → viram tofu ▯.
# Quando o nome tem esses chars, caímos pro @username (sempre ASCII).
_FANCY_RANGES = (
    (0x1D400, 0x1D7FF),  # Mathematical Alphanumeric (Bold/Italic/Script/Fraktur/Double-struck/Sans/Mono)
    (0x2100, 0x214F),    # Letterlike Symbols (ℳ ℕ ℝ ℘ ℒ etc)
    (0x2460, 0x24FF),    # Enclosed Alphanumerics (① ⓐ Ⓐ)
    (0xFF00, 0xFFEF),    # Halfwidth/Fullwidth (ＡＢＣ)
    (0x1F100, 0x1F1FF),  # Enclosed Alphanumeric Supplement (🅰 🅱 🆎)
    (0x1F130, 0x1F189),  # Squared Latin (🅰-🆉)
)


def _has_fancy_unicode(s: str) -> bool:
    if not s:
        return False
    for ch in s:
        cp = ord(ch)
        for lo, hi in _FANCY_RANGES:
            if lo <= cp <= hi:
                return True
    return False


def card_safe_name(chat_id: int, user_id: int, name: str,
                   royal_id: str | None = None) -> str:
    """Nome seguro pra renderizar em card PNG (Pillow + DejaVu).
    Se `name` tem chars de 'fonte fancy' (Unicode Math/Fullwidth/etc) que
    Pillow não consegue desenhar com DejaVu, fallback nesta ordem:
      1) @username salvo no DB
      2) anonize() → ANON-<suffix> via royal_id
    Caption HTML do Telegram mantém o nome fancy original — esse fallback
    é exclusivo do card."""
    if not _has_fancy_unicode(name):
        return name
    u = get_username(chat_id, user_id)
    if u:
        return f"@{u}"
    return anonize("", royal_id)  # vira "Nobre" ou ANON-<suffix> se tiver rid


def anonize(name: str, royal_id: str | None) -> str:
    """Privacidade: se `name` for puro digito (= user_id numerico do Telegram
    como fallback de get_name), retorna alias ANON-<suffix> derivado do
    royal_id. NUNCA expor user_id cru do Telegram em texto/caption/card.
    """
    if not name:
        return "Nobre"
    if name.lstrip("-").isdigit():
        rid = royal_id or "RYL-????"
        suffix = rid.replace("RYL-", "").lstrip("0") or "0"
        return f"ANON-{suffix}"
    return name


def get_anon_name(chat_id: int, user_id: int) -> str:
    """Versao segura de get_name: ja aplica anonize() consultando o royal_id
    do mesmo player numa unica query. Usar SEMPRE que o nome for renderizado
    em qualquer caption, mensagem, hall da fama ou registro publico.
    """
    cur.execute(
        """
        SELECT u.display_name AS dn, p.royal_id AS rid
        FROM users u
        LEFT JOIN players p
          ON p.chat_id = u.chat_id AND p.user_id = u.user_id
        WHERE u.chat_id = ? AND u.user_id = ?
        """,
        (chat_id, user_id),
    )
    row = cur.fetchone()
    if not row:
        # Sem registro em users — fallback puro pra royal_id (se houver)
        cur.execute(
            "SELECT royal_id FROM players WHERE chat_id=? AND user_id=?",
            (chat_id, user_id))
        prow = cur.fetchone()
        return anonize(str(user_id), prow["royal_id"] if prow else None)
    return anonize(row["dn"] or str(user_id), row["rid"])


# =====================================================================
# SHIPPER (legado, preservado)
# =====================================================================

def vote_keyboard(couple_id: int, likes: int = 0, dislikes: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        ikb(f"❤️ Apoio {likes}",     callback_data=f"ship_like:{couple_id}",    style=STYLE_OK),
        ikb(f"🤮 Ciúmes {dislikes}", callback_data=f"ship_dislike:{couple_id}", style=STYLE_NO),
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
    n1 = get_anon_name(chat_id, u1)
    n2 = get_anon_name(chat_id, u2)

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

    # === Card 1080x1080 do casorio (foto). Fallback pra texto se falhar. ===
    try:
        p1 = ensure_player(chat_id, u1)
        p2 = ensure_player(chat_id, u2)
        db.commit()
        # `players` nao tem coluna `level` — derivar de total_xp via level_progress
        lvl1, *_ = level_progress(p1["total_xp"] or 0)
        lvl2, *_ = level_progress(p2["total_xp"] or 0)
        partners = (
            CasorioPartner(
                royal_id=p1["royal_id"] or "RYL-????", name=n1,
                level=int(lvl1 or 1),
                avatar_slug=p1["avatar_slug"]),
            CasorioPartner(
                royal_id=p2["royal_id"] or "RYL-????", name=n2,
                level=int(lvl2 or 1),
                avatar_slug=p2["avatar_slug"]),
        )
        card = await asyncio.to_thread(
            render_casorio_card, partners[0], partners[1],
            current_season_label(), source,
            datetime.now(TZ).strftime("%d/%m/%Y %H:%M"))
        if card:
            # Caption tem limite de 1024 chars — text atual cabe folgado.
            await bot.send_photo(
                chat_id,
                photo=BufferedInputFile(card, filename=f"casorio-{couple_id}.jpg"),
                caption=text,
                reply_markup=vote_keyboard(couple_id),
            )
            return True
    except Exception:
        logger.exception("send_couple: render do card falhou, fallback texto")

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


async def safe_edit_caption(chat_id: int, message_id: int, caption: str, **kwargs):
    """Edita CAPTION de uma foto/midia (usado pelo modo spoiler_img da Palavra)."""
    assert bot is not None
    try:
        return await bot.edit_message_caption(
            chat_id=chat_id, message_id=message_id, caption=caption, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        logger.warning("edit_caption failed: %s", e)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
    except Exception:
        logger.exception("safe_edit_caption failed")


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


def is_test_chat(chat_id: int) -> bool:
    """True se o chat_id estiver na lista TEST_CHAT_IDS (env). Esses chats
    sao escondidos do picker em DM e do inline mode."""
    return chat_id in TEST_CHAT_IDS


def get_dm_active_chat(user_id: int) -> int | None:
    """Le a escolha persistida do user (em DM, qual grupo eh o ativo)."""
    cur.execute(
        "SELECT active_chat_id FROM user_dm_settings WHERE user_id=?",
        (user_id,),
    )
    row = cur.fetchone()
    return row["active_chat_id"] if row and row["active_chat_id"] else None


def set_dm_active_chat(user_id: int, chat_id: int) -> None:
    cur.execute(
        "INSERT INTO user_dm_settings (user_id, active_chat_id, set_at) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "active_chat_id=excluded.active_chat_id, set_at=excluded.set_at",
        (user_id, chat_id, utc_iso()),
    )
    db.commit()


def clear_dm_active_chat(user_id: int) -> None:
    cur.execute(
        "UPDATE user_dm_settings SET active_chat_id=NULL WHERE user_id=?",
        (user_id,),
    )
    db.commit()


def list_user_groups(user_id: int) -> list[tuple[int, str]]:
    """Grupos onde o user tem player (chat_id, titulo). EXCLUI TEST_CHAT_IDS.
    Ordenado por season_xp desc (mais ativo primeiro)."""
    cur.execute(
        """
        SELECT p.chat_id AS chat_id,
               COALESCE(c.title, '') AS title,
               COALESCE(p.season_xp, 0) AS sxp
        FROM players p
        LEFT JOIN chats c ON c.chat_id = p.chat_id
        WHERE p.user_id = ?
        ORDER BY sxp DESC
        """,
        (user_id,),
    )
    out: list[tuple[int, str]] = []
    for r in cur.fetchall():
        cid = r["chat_id"]
        if is_test_chat(cid):
            continue
        title = (r["title"] or "").strip() or f"Grupo {cid}"
        out.append((cid, title))
    return out


def resolve_owner_chat(user_id: int, fallback_chat: int | None = None) -> int | None:
    """
    Resolve em qual grupo Royal um usuário tem perfil mais ativo.
    - Em grupo, retorna fallback_chat (chat atual) se o user tiver player lá.
    - Em DM, prioriza a escolha salva em user_dm_settings; senao, retorna
      o chat com maior total_xp (excluindo TEST_CHAT_IDS).
    """
    if fallback_chat is not None:
        cur.execute(
            "SELECT 1 FROM players WHERE chat_id=? AND user_id=? LIMIT 1",
            (fallback_chat, user_id),
        )
        if cur.fetchone():
            return fallback_chat
    # DM: tenta escolha salva primeiro
    saved = get_dm_active_chat(user_id)
    if saved and not is_test_chat(saved):
        cur.execute(
            "SELECT 1 FROM players WHERE chat_id=? AND user_id=? LIMIT 1",
            (saved, user_id),
        )
        if cur.fetchone():
            return saved
        # escolha invalida (user nao tem mais player la) — limpa
        clear_dm_active_chat(user_id)
    # Fallback: chat com maior XP, EXCLUI test chats
    if TEST_CHAT_IDS:
        placeholders = ",".join("?" for _ in TEST_CHAT_IDS)
        cur.execute(
            f"SELECT chat_id FROM players "
            f"WHERE user_id=? AND chat_id NOT IN ({placeholders}) "
            f"ORDER BY total_xp DESC LIMIT 1",
            (user_id, *TEST_CHAT_IDS),
        )
    else:
        cur.execute(
            "SELECT chat_id FROM players WHERE user_id=? "
            "ORDER BY total_xp DESC LIMIT 1",
            (user_id,),
        )
    row = cur.fetchone()
    return row["chat_id"] if row else None


# Cache em memoria de file_id da ULTIMA foto de perfil enviada — usado pelo
# inline mode pra reaproveitar o mesmo file_id sem renderizar de novo.
# Key = (owner_chat, owner_uid). Value = (file_id, ts_unix).
_profile_file_id_cache: dict[tuple[int, int], tuple[str, float]] = {}
_PROFILE_FILE_ID_TTL = 60 * 60 * 6  # 6h


def cache_profile_file_id(owner_chat: int, owner_uid: int, file_id: str) -> None:
    _profile_file_id_cache[(owner_chat, owner_uid)] = (file_id, time.time())


def get_cached_profile_file_id(owner_chat: int, owner_uid: int) -> str | None:
    e = _profile_file_id_cache.get((owner_chat, owner_uid))
    if not e:
        return None
    fid, ts = e
    if time.time() - ts > _PROFILE_FILE_ID_TTL:
        _profile_file_id_cache.pop((owner_chat, owner_uid), None)
        return None
    return fid


def group_picker_kb(groups: list[tuple[int, str]],
                    *, prefix: str = "g:pick") -> InlineKeyboardMarkup:
    """Teclado inline com 1 botao por grupo (chat_id, titulo).
    callback_data = f'{prefix}:{chat_id}'. Truncamos titulo a 40 chars."""
    rows = []
    for cid, title in groups[:20]:
        label = title if len(title) <= 40 else title[:37] + "..."
        rows.append([ikb(
            f"🏰 {label}",
            callback_data=f"{prefix}:{cid}",
            style=STYLE_INFO)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_group_picker(message: Message,
                            groups: list[tuple[int, str]],
                            *, action_hint: str = "") -> None:
    """Manda o card 'escolhe teu grupo' em DM."""
    hint = f"\n<i>// {action_hint}</i>" if action_hint else ""
    body = (
        "<i>Voce participa de mais de um grupo Royal.\n"
        "Escolhe qual eh o ativo pra DM:</i>"
        f"{hint}"
    )
    await message.answer(
        term_block("REINO", body, status="SELECT", status_color="CYAN",
                   stamp="trocavel via /royalgrupo"),
        reply_markup=group_picker_kb(groups))


async def resolve_dm_chat(message: Message,
                          *, action_hint: str = "") -> int | None:
    """Resolve qual chat-dono usar pra este comando.
    - Em grupo: retorna message.chat.id.
    - Em DM: usa user_dm_settings; se faltar e o user tiver 1 grupo,
      auto-seleciona; se tiver varios, manda o picker e retorna None.
    Caller DEVE `return` imediatamente se receber None."""
    if is_group(message):
        return message.chat.id
    if not message.from_user:
        return None
    uid = message.from_user.id
    active = get_dm_active_chat(uid)
    if active and not is_test_chat(active):
        if get_player(active, uid):
            return active
        clear_dm_active_chat(uid)
    groups = list_user_groups(uid)
    if not groups:
        await message.answer(term_block(
            "REINO",
            "<i>Voce ainda nao tem perfil em nenhum grupo Royal.\n"
            "Manda umas mensagens no grupo Royal pra ser registrado ✨</i>",
            status="SEM_REINO", status_color="AMBER"))
        return None
    if len(groups) == 1:
        set_dm_active_chat(uid, groups[0][0])
        return groups[0][0]
    await send_group_picker(message, groups, action_hint=action_hint)
    return None


# =====================================================================
# RPG — XP, NIVEIS, ATRIBUTOS
# =====================================================================

_LEVEL_TABLE: list[int] = []  # cumulative XP to reach level (index = level)


def _build_level_table(max_level: int = 200) -> None:
    """Constroi tabela de XP cumulativo. _LEVEL_TABLE[n] = XP total p/ estar no nivel n.

    Index 0 = sentinela. Index 1 = 0 (player comeca em level 1 com 0 XP).
    A partir do index 2, cada entrada acumula int(100 * n^1.5).
    """
    _LEVEL_TABLE.clear()
    _LEVEL_TABLE.append(0)  # sentinela (index 0)
    _LEVEL_TABLE.append(0)  # nivel 1 comeca em 0 XP (index 1)
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
    if new_lvl > old_lvl:
        _schedule_levelup_dm(chat_id, user_id, player, new_lvl)


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
    if new_lvl > old_lvl:
        _schedule_levelup_dm(chat_id, user_id, player, new_lvl)


def _schedule_levelup_dm(chat_id: int, user_id: int,
                          player: dict, new_lvl: int) -> None:
    """Agenda envio de card de level-up na DM (não bloqueia)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    royal_id = player.get("royal_id") or ""
    class_id = player.get("class_id")
    name = anonize(get_name(chat_id, user_id), royal_id)
    # Card PNG não renderiza fontes "fancy" Unicode → cai pro @username.
    name = card_safe_name(chat_id, user_id, name, royal_id)
    avatar_slug = royal_avatars.resolve_slug(
        player.get("avatar_slug"), royal_id)
    loop.create_task(notify_level_up_dm(
        user_id, royal_id, name, new_lvl, class_id, avatar_slug))


async def notify_level_up_dm(user_id: int, royal_id: str, name: str,
                              new_level: int, class_id: str | None,
                              avatar_slug: str | None = None) -> None:
    """Tenta enviar card de level-up na DM. Silencioso se user bloqueou ou
    nunca falou com o bot em privado."""
    if bot is None or not royal_id:
        return
    try:
        class_name = ""
        if class_id and class_id in CLASSES:
            class_name = CLASSES[class_id].get("name", "")
        png = await asyncio.to_thread(
            render_levelup_card, royal_id, name, new_level, class_name,
            avatar_slug)
        caption = term_block(
            "LEVEL_UP",
            f"<b>NÍVEL {new_level:02d} ATINGIDO</b>\n"
            f"<i>+1 ponto de atributo. Use /royalup no grupo pra distribuir.</i>",
            status="ALERTA", status_color="HOT",
        )
        if png:
            await bot.send_photo(
                user_id,
                BufferedInputFile(png, filename="royal_levelup.jpg"),
                caption=caption,
                message_effect_id=EFFECT_THUMBS_UP,
            )
        else:
            await bot.send_message(user_id, caption,
                                   message_effect_id=EFFECT_THUMBS_UP)
    except Exception:
        # user nunca abriu DM ou bloqueou — silencioso
        pass


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


async def get_user_photo_bytes(user_id: int) -> bytes | None:
    """Baixa os bytes da foto de perfil pra renderizar dentro de cards."""
    assert bot is not None
    file_id = await get_user_photo_file_id(user_id)
    if not file_id:
        return None
    try:
        buf = io.BytesIO()
        await bot.download(file_id, destination=buf)
        return buf.getvalue()
    except Exception:
        logger.warning("download avatar bytes failed uid=%d", user_id, exc_info=True)
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
    name = anonize(get_name(chat_id, user_id), p.get("royal_id"))
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


def build_profile_card_data(chat_id: int, user_id: int) -> ProfileCardData:
    """Coleta dados pra renderizar o cartao 1080x1080."""
    p = ensure_player(chat_id, user_id)
    royal_id = p.get("royal_id") or "RYL-????"
    name = anonize(get_name(chat_id, user_id), royal_id)
    # Card PNG não renderiza fontes "fancy" Unicode (𝓢𝓬𝓻𝓲𝓹𝓽, 𝐁𝐨𝐥𝐝, etc):
    # cai pro @username quando detecta.
    name = card_safe_name(chat_id, user_id, name, royal_id)
    lvl, in_lvl, needed, _ = level_progress(p["total_xp"])

    cur.execute("SELECT COUNT(*) AS total FROM players WHERE chat_id=?", (chat_id,))
    total_players = cur.fetchone()["total"]
    cur.execute(
        "SELECT COUNT(*)+1 AS rank FROM players WHERE chat_id=? AND season_xp > ?",
        (chat_id, p["season_xp"]),
    )
    rank = cur.fetchone()["rank"]
    cur.execute(
        "SELECT COUNT(*) AS total FROM couples WHERE chat_id=? AND (user1=? OR user2=?)",
        (chat_id, user_id, user_id),
    )
    casorios = cur.fetchone()["total"]
    cur.execute(
        "SELECT COUNT(*) AS total FROM challenges WHERE chat_id=? AND winner_user_id=?",
        (chat_id, user_id),
    )
    palavras_won = cur.fetchone()["total"]

    class_id = p.get("class_id")
    class_name = (CLASSES[class_id]["name"]
                  if class_id and class_id in CLASSES else "Sem classe")

    hp = hp_max(p)
    joined_str = "?"
    if p.get("joined_at"):
        try:
            joined_dt = datetime.fromisoformat(p["joined_at"])
            joined_str = joined_dt.strftime("%d/%m/%Y")
        except Exception:
            pass

    return ProfileCardData(
        royal_id=royal_id,
        name=name,
        initial=(name[:1] or "?").upper(),
        class_name=class_name,
        season=current_season_label(),
        level=lvl,
        xp_in_level=in_lvl,
        xp_needed=needed,
        hp_cur=hp,
        hp_max=hp,
        attr_for=effective_attr(p, "for"),
        attr_des=effective_attr(p, "des"),
        attr_vit=effective_attr(p, "vit"),
        attr_car=effective_attr(p, "car"),
        pts_available=p.get("pts_available", 0),
        rank=rank,
        total_players=total_players,
        palavras_won=palavras_won,
        casorios=casorios,
        gold=p.get("gold", 0),
        msg_count=p.get("rpg_message_count", 0),
        joined_str=joined_str,
        avatar_slug=royal_avatars.resolve_slug(
            p.get("avatar_slug"), royal_id),
    )


def build_profile_caption(chat_id: int, user_id: int) -> str:
    """Caption do card usando TODAS as formatacoes nativas do Telegram:
    <b>, <i>, <u>, <s>, <code>, <pre>, <a>, <blockquote expandable>, <tg-spoiler>.
    Limite de caption do Telegram: 1024 chars.
    """
    p = ensure_player(chat_id, user_id)
    name = anonize(get_name(chat_id, user_id), p.get("royal_id"))
    lvl, in_lvl, needed, _ = level_progress(p["total_xp"])

    cur.execute("SELECT COUNT(*) AS total FROM players WHERE chat_id=?", (chat_id,))
    total_players = cur.fetchone()["total"]
    cur.execute(
        "SELECT COUNT(*)+1 AS rank FROM players WHERE chat_id=? AND season_xp > ?",
        (chat_id, p["season_xp"]),
    )
    rank = cur.fetchone()["rank"]
    cur.execute(
        "SELECT COUNT(*) AS total FROM couples WHERE chat_id=? AND (user1=? OR user2=?)",
        (chat_id, user_id, user_id),
    )
    casorios = cur.fetchone()["total"]
    cur.execute(
        "SELECT COUNT(*) AS total FROM challenges WHERE chat_id=? AND winner_user_id=?",
        (chat_id, user_id),
    )
    palavras_won = cur.fetchone()["total"]

    class_id = p.get("class_id")
    if class_id and class_id in CLASSES:
        class_html = (
            f"{html.escape(CLASSES[class_id]['emoji'])} "
            f"{html.escape(CLASSES[class_id]['name'])}"
        )
    else:
        # Strikethrough nativo (<s>) — montado FORA do escape
        class_html = "🎭 <s>Sem classe</s>"
    season = current_season_label()
    hp = hp_max(p)

    def _br(n):
        return f"{int(n):,}".replace(",", ".")

    pts_extra = (f"  ·  <u>+{p['pts_available']} pts</u>"
                 if p.get("pts_available", 0) > 0 else "")

    f_ = effective_attr(p, "for")
    d_ = effective_attr(p, "des")
    v_ = effective_attr(p, "vit")
    c_ = effective_attr(p, "car")

    joined_str = "?"
    if p.get("joined_at"):
        try:
            joined_str = datetime.fromisoformat(p["joined_at"]).strftime("%d/%m/%Y")
        except Exception:
            pass

    royal_id = p["royal_id"] or "RYL-????"

    # Header: bold + italic + mention link
    # class_html ja inclui tag <s> quando sem classe — NAO escapar
    header = (
        f"👑 <b>{html.escape(royal_id)}</b> · {mention(user_id, name)}\n"
        f"<i>{class_html} · {html.escape(season)}</i>"
    )

    # Ficha em blockquote expandable. NÃO usar <pre> aqui: em mobile com
    # nome/temporada longos, <pre> estoura largura do chat e gera scroll
    # horizontal sobrepondo UI do Telegram. Layout inline com emojis cabe
    # em qualquer tela.
    ficha = (
        f"<blockquote expandable>"
        f"<b>📜 Ficha do nobre</b>\n"
        f"⭐ <b>Nv {lvl}</b> · {_br(in_lvl)}/{_br(needed)} XP{pts_extra}\n"
        f"🩸 HP <b>{hp}</b>/{hp}\n"
        f"\n"
        f"⚔️ <b>{f_}</b> FOR · 🏹 <b>{d_}</b> DES\n"
        f"🛡️ <b>{v_}</b> VIT · 💎 <b>{c_}</b> CAR\n"
        f"\n"
        f"🏆 <b>#{rank}</b>/{total_players} · 🎯 {palavras_won} pal · 💍 {casorios}\n"
        f"🪙 <code>{_br(p['gold'])}</code> florins\n"
        f"<i>💬 {_br(p.get('rpg_message_count', 0))} msgs desde {joined_str}</i>"
        f"</blockquote>"
    )

    full = f"{header}\n\n{ficha}"
    # Guarda do limite de caption do Telegram (1024 chars).
    if len(full) > 1024:
        full = f"{header}\n\n{ficha[: max(0, 1024 - len(header) - 2 - len('</blockquote>'))]}</blockquote>"
    if len(full) > 1024:
        full = full[:1020] + "…"
    return full


async def send_profile_card(chat_id_to: int, owner_chat: int, owner_uid: int):
    """Envia cartao de perfil renderizado 1080x1080. Fallback pra texto puro."""
    assert bot is not None
    await safe_typing(chat_id_to, "upload_photo")

    # Tenta render do card primeiro
    try:
        data = build_profile_card_data(owner_chat, owner_uid)
        avatar_bytes = await get_user_photo_bytes(owner_uid)
        card = await asyncio.to_thread(render_profile_card, data, avatar_bytes)
        if card:
            caption = build_profile_caption(owner_chat, owner_uid)
            sent = await bot.send_photo(
                chat_id_to,
                photo=BufferedInputFile(card, filename=f"perfil-{data.royal_id}.jpg"),
                caption=caption,
            )
            # Cacheia file_id pro inline mode reaproveitar sem re-renderizar
            try:
                if sent and sent.photo:
                    cache_profile_file_id(
                        owner_chat, owner_uid, sent.photo[-1].file_id)
            except Exception:
                pass
            return
    except Exception:
        logger.exception("render_profile_card path failed; caindo pro fallback")

    # Fallback: texto puro (com foto bruta se houver)
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
    # Bot API 10: style nativo verde/vermelho/azul. Emoji lider mantido
    # como fallback pra clientes antigos + acessibilidade.
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb("👑 Perfil",                 callback_data="r:perfil"),
         ikb(f"{BTN_OK} Subir Pts",       callback_data="r:up:menu",  style=STYLE_OK)],
        [ikb("🎭 Classe",                 callback_data="r:cls:menu"),
         ikb("🎒 Mochila",                callback_data="r:inv")],
        [ikb("🪙 Loja",                   callback_data="r:loja"),
         ikb("🏆 Ranking",                callback_data="r:rank")],
        [ikb("🎯 Palavra",                callback_data="r:pal"),
         ikb("🐉 Boss",                   callback_data="r:boss")],
        [ikb("💍 Casórios",               callback_data="r:cas"),
         ikb(f"{BTN_INFO} Ajuda",         callback_data="r:help",     style=STYLE_INFO)],
        [ikb(f"{BTN_WARN} Privacidade",   callback_data="r:priv")],
    ])


def hub_text() -> str:
    body = (
        "<b>// REINO ROYAL</b> — terminal de comando ativo.\n"
        "<i>Bem-vindo de volta, nobre.</i>\n"
        "<blockquote>Toque num botão abaixo pra navegar pela corte 👇</blockquote>"
    )
    return term_block("ROYAL", body,
                      status="CONECTADO",
                      stamp=current_season_label())


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
    type_label = {
        "anagrama":    "🔤 ANAGRAMA",
        "letras":      "🔡 LETRAS_FALTANDO",
        "charada":     "💭 CHARADA",
        "spoiler_img": "🖼️ SPOILER_IMG",
    }.get(ch["type"], "🎯 DESAFIO")

    if status == "open":
        try:
            ends_at = datetime.fromisoformat(ch["ends_at"])
            mins_left = max(0, int((ends_at - utc_now()).total_seconds() / 60))
        except Exception:
            mins_left = ch.get("duration_min", 5)
        if ch["type"] == "spoiler_img":
            puzzle = "<i>// palavra oculta na imagem acima — toque pra revelar</i>"
        elif ch["type"] == "charada":
            puzzle = f"<blockquote>💭 <i>{html.escape(ch['hint'])}</i></blockquote>"
        else:
            puzzle = f"<pre>{html.escape(ch['display'])}</pre>"
        body = (
            f">> <b>{type_label}</b>\n"
            f"{puzzle}\n"
            f"⏱️ <code>~{mins_left}min</code>  ·  "
            f"💬 <code>{ch.get('attempts_count', 0)}</code> tentativas\n"
            f"⚡ recompensa: <b>{XP_PALAVRA_WIN_BONUS} XP + {GOLD_PALAVRA_WIN}🪙</b>\n"
            f"<i>Responda no chat pra tentar (acento/case/palavra extra OK).</i>"
        )
        return term_block("PALAVRA", body,
                          status="TRANSMITINDO", status_color="ACID")
    if status == "win":
        body = (
            f"🏆 <b>{winner_name} ACERTOU!</b>\n"
            f">> palavra: <code>{ch['word'].upper()}</code>\n"
            f"⏱️ {(ch.get('winner_ms') or 0) / 1000:.1f}s  ·  "
            f"💬 {ch.get('attempts_count', 0)} tentativas"
        )
        return term_block("PALAVRA", body,
                          status="RESOLVIDO", status_color="ACID",
                          stamp=type_label)
    # timeout
    body = (
        f"⏰ <i>Tempo esgotado.</i>\n"
        f">> palavra: <code>{ch['word'].upper()}</code>\n"
        f"😶 Ninguém acertou."
    )
    return term_block("PALAVRA", body,
                      status="TIMEOUT", status_color="HOT",
                      stamp=type_label)


async def spawn_palavra(chat_id: int) -> None:
    """Cria novo desafio (modo spoiler_img — palavra inteira em imagem com blur do Telegram)."""
    assert bot is not None
    word_raw = random.choice(PALAVRAS)
    word = normalize_word(word_raw)
    challenge_type = "spoiler_img"
    display = ""
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
        (chat_id, challenge_type, word, display, hint,
         started.isoformat(), ends.isoformat(), duration_min),
    )
    cid = cur.lastrowid
    db.commit()

    ch = {
        "id": cid, "chat_id": chat_id, "type": challenge_type,
        "word": word, "display": display, "hint": hint,
        "started_at": started.isoformat(), "ends_at": ends.isoformat(),
        "duration_min": duration_min, "attempts_count": 0,
    }
    caption = format_challenge_text(ch)
    if len(caption) > 1024:
        caption = caption[:1020] + "…"

    msg = None
    try:
        png = await asyncio.to_thread(
            render_palavra_spoiler_card, word_raw, f"{chat_id}:{cid}")
        if png:
            await safe_typing(chat_id, "upload_photo")
            msg = await bot.send_photo(
                chat_id,
                BufferedInputFile(png, filename=f"palavra-{cid}.jpg"),
                caption=caption,
                parse_mode="HTML",
                has_spoiler=True,
            )
    except TelegramRetryAfter as e:
        logger.warning("RetryAfter %ds on spawn_palavra to %d", e.retry_after, chat_id)
        await asyncio.sleep(e.retry_after + 1)
    except Exception:
        logger.exception("spawn_palavra send_photo failed chat=%d", chat_id)

    # Fallback texto se a foto falhar
    if msg is None:
        msg = await safe_send(chat_id,
            term_block("PALAVRA",
                f">> <b>🖼️ SPOILER_IMG</b>\n"
                f"<tg-spoiler>{html.escape(word_raw.upper())}</tg-spoiler>\n"
                f"⚡ recompensa: <b>{XP_PALAVRA_WIN_BONUS} XP + {GOLD_PALAVRA_WIN}🪙</b>\n"
                f"<i>Responda no chat pra tentar (acento/case/palavra extra OK).</i>",
                status="TRANSMITINDO", status_color="ACID"))

    if msg:
        cur.execute("UPDATE challenges SET message_id=? WHERE id=?", (msg.message_id, cid))
        db.commit()
    logger.info("palavra spawned chat=%d cid=%d type=%s dur=%d word=%s",
                chat_id, cid, challenge_type, duration_min, word)
    # Agenda baú real pra 30min depois
    try:
        schedule_chest_after_palavra(chat_id)
    except Exception:
        logger.exception("schedule_chest_after_palavra failed chat=%d", chat_id)


async def handle_palavra_attempt(message: Message, ch: dict) -> bool:
    """Processa tentativa. Retorna True se acertou (winner)."""
    if not message.from_user or not message.text:
        return False
    uid = message.from_user.id
    chat_id = message.chat.id
    logger.info("[PALAVRA] tentativa ch=%s uid=%d mid=%d text=%r",
                ch["id"], uid, message.message_id, (message.text or "")[:120])

    # cooldown anti-spam
    now_ts = utc_now().timestamp()
    cd_key = (ch["id"], uid)
    last = attempt_cooldowns.get(cd_key, 0)
    if now_ts - last < PALAVRA_ATTEMPT_COOLDOWN_SEC:
        logger.info("[PALAVRA] cooldown ch=%s uid=%d (%.1fs<%ss)",
                    ch["id"], uid, now_ts - last, PALAVRA_ATTEMPT_COOLDOWN_SEC)
        return False
    attempt_cooldowns[cd_key] = now_ts

    # Limite generoso: aceita frases curtas tipo "acho que é gato" (3-5 palavras)
    if message.text and len(message.text) > 120:
        logger.info("[PALAVRA] text>120 chars ignorado ch=%s uid=%d", ch["id"], uid)
        return False

    target = ch["word"]
    # Match tolerante: tokeniza ANTES de normalizar (normalize_word remove
    # espaços, então tokenização precisa rodar primeiro). Aceita a palavra
    # alvo em qualquer posição da frase, ignorando acento/case/pontuação.
    # Bordas de palavra evitam falso positivo: "casamento" NÃO mata "casa".
    raw_tokens = re.split(r"\s+", (message.text or "").strip())
    norm_tokens = [normalize_word(t) for t in raw_tokens if t]
    if not norm_tokens:
        return False
    correct = target in norm_tokens
    logger.info("[PALAVRA] match ch=%s uid=%d target=%r tokens=%r correct=%s",
                ch["id"], uid, target, norm_tokens, correct)

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
        logger.info("[PALAVRA] consolacao ch=%s uid=%d (outro ja venceu)", ch["id"], uid)
        award_xp_immediate(chat_id, uid, XP_PALAVRA_CONSOLATION, reason="palavra_consolation")
        db.commit()
        return False
    logger.info("[PALAVRA] VENCEDOR ch=%s uid=%d mid=%d", ch["id"], uid, message.message_id)
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
        new_text = format_challenge_text(ch_final, status="win", winner_name=mention(uid, name))
        if ch_final.get("type") == "spoiler_img":
            await safe_edit_caption(chat_id, ch_final["message_id"], new_text, parse_mode="HTML")
        else:
            await safe_edit(chat_id, ch_final["message_id"], new_text)

    # Feedback direto na mensagem do vencedor: reacao 🏆 + reply confirmando
    await react_to(chat_id, message.message_id, "🏆")
    try:
        secs = max(1, winner_ms // 1000) if winner_ms else 0
        stamp_bits = [f"+{base_xp} XP", f"+{gold_award}🪙"]
        if secs:
            stamp_bits.append(f"{secs}s")
        feedback = term_block(
            "ACERTOU.SYS",
            (
                f">> <b>{mention(uid, name)}</b> respondeu primeiro\n"
                f"// palavra confirmada // recompensa creditada"
            ),
            status="PRIMEIRO",
            status_color="ACID",
            stamp=" · ".join(stamp_bits),
        )
        await message.reply(
            feedback,
            parse_mode="HTML",
            **effect_kw(message.chat.type, EFFECT_PARTY),
        )
    except Exception as e:
        log.warning(f"palavra winner feedback falhou: {e}")
    return True


async def finalize_expired_challenges() -> None:
    """Marca como timeout desafios cujo ends_at ja passou."""
    now = utc_iso()
    cur.execute(
        "SELECT * FROM challenges WHERE status='open' AND ends_at < ?",
        (now,),
    )
    expired = [dict(r) for r in cur.fetchall()]
    any_updated = False
    for ch in expired:
        # Atomic: só vira timeout se ainda estiver open. Protege contra race
        # com handle_palavra_attempt que pode ter virado 'won' entre o SELECT
        # acima e este UPDATE.
        cur.execute(
            "UPDATE challenges SET status='timeout' WHERE id=? AND status='open'",
            (ch["id"],),
        )
        if cur.rowcount == 0:
            continue  # outro caminho (win) já fechou o desafio
        any_updated = True
        if ch.get("message_id"):
            new_text = format_challenge_text(ch, status="timeout")
            if ch.get("type") == "spoiler_img":
                await safe_edit_caption(ch["chat_id"], ch["message_id"],
                                        new_text, parse_mode="HTML")
            else:
                await safe_edit(ch["chat_id"], ch["message_id"], new_text)
    if any_updated:
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
# RPG — BAU REAL (spawna 30min apos cada Palavra; primeiros 5 abrem)
# =====================================================================

def schedule_chest_after_palavra(chat_id: int) -> None:
    """Cria registro de bau pendente CHEST_DELAY_MIN minutos no futuro."""
    spawn_at = utc_now() + timedelta(minutes=CHEST_DELAY_MIN)
    cur.execute(
        "INSERT INTO chests (chat_id, spawn_at, status) VALUES (?, ?, 'pending')",
        (chat_id, spawn_at.isoformat()),
    )
    db.commit()


def chest_keyboard(chest_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        ikb("🎁 Abrir Baú", callback_data=f"r:chest:{chest_id}", style=STYLE_OK)
    ]])


def format_chest_text(chest_id: int, status: str = "open") -> str:
    cur.execute(
        "SELECT slot, name, xp, gold, user_id FROM chest_claims "
        "WHERE chest_id=? ORDER BY slot",
        (chest_id,),
    )
    claims = [dict(r) for r in cur.fetchall()]
    remaining = CHEST_MAX_CLAIMS - len(claims)

    rows = []
    for c in claims:
        nm = (c["name"] or "anon")[:18]
        rows.append(f"[{c['slot']}] {nm:<18}  +{c['xp']:>3} XP  +{c['gold']:>2}🪙")
    for slot in range(len(claims) + 1, CHEST_MAX_CLAIMS + 1):
        xp, gold = CHEST_REWARDS[slot - 1]
        rows.append(f"[{slot}] {'???':<18}  +{xp:>3} XP  +{gold:>2}🪙")

    pre = "<pre>" + html.escape("\n".join(rows)) + "</pre>"
    body = ">> baú real materializou // primeiros 5 saqueiam\n" + pre

    if status == "open":
        body += f"\n// vagas restantes: <b>{remaining}/{CHEST_MAX_CLAIMS}</b>"
        st, color = "ABERTO", "ACID"
    elif status == "closed":
        body += "\n!! saqueado por completo"
        st, color = "FECHADO", "AMBER"
    else:  # expired
        body += "\n!! cofre selou — vagas perdidas no éter"
        st, color = "EXPIRADO", "HOT"

    return term_block("BAU", body, status=st, status_color=color)


async def spawn_chest(chat_id: int, chest_id: int) -> None:
    """Envia mensagem do bau e marca como aberto."""
    expires_at = utc_now() + timedelta(minutes=CHEST_TTL_MIN)
    text = format_chest_text(chest_id, status="open")
    msg = await safe_send(chat_id, text, reply_markup=chest_keyboard(chest_id))
    if msg:
        cur.execute(
            "UPDATE chests SET status='open', spawned_at=?, expires_at=?, message_id=? "
            "WHERE id=?",
            (utc_iso(), expires_at.isoformat(), msg.message_id, chest_id),
        )
        db.commit()
        logger.info("chest spawned chat=%d chest=%d", chat_id, chest_id)
    else:
        cur.execute("UPDATE chests SET status='expired' WHERE id=?", (chest_id,))
        db.commit()


async def expire_old_chests() -> None:
    """Fecha baus cujo TTL passou."""
    now = utc_iso()
    cur.execute(
        "SELECT id, chat_id, message_id FROM chests "
        "WHERE status='open' AND expires_at IS NOT NULL AND expires_at < ?",
        (now,),
    )
    expired = [dict(r) for r in cur.fetchall()]
    for ch in expired:
        cur.execute("UPDATE chests SET status='expired' WHERE id=?", (ch["id"],))
        db.commit()
        if ch["message_id"]:
            try:
                await safe_edit(
                    ch["chat_id"], ch["message_id"],
                    format_chest_text(ch["id"], status="expired"),
                    reply_markup=None,
                )
            except Exception:
                logger.exception("expire_old_chests edit failed")


async def handle_chest_claim(cb: CallbackQuery, chest_id: int) -> None:
    """Processa clique em Abrir Baú. Aloca slot atomicamente."""
    if not cb.message or not cb.from_user:
        await cb.answer()
        return
    chat_id = cb.message.chat.id
    uid = cb.from_user.id

    cur.execute("SELECT * FROM chests WHERE id=? AND chat_id=?", (chest_id, chat_id))
    row = cur.fetchone()
    if not row:
        await cb.answer("🪦 Baú sumiu.", show_alert=False)
        return
    chest = dict(row)

    if chest["status"] != "open":
        await cb.answer("🔒 Baú já está fechado.", show_alert=False)
        return

    # TTL
    try:
        if chest["expires_at"]:
            exp = datetime.fromisoformat(chest["expires_at"])
            if utc_now() > exp:
                cur.execute("UPDATE chests SET status='expired' WHERE id=?", (chest_id,))
                db.commit()
                await cb.answer("⏰ Baú expirou.", show_alert=False)
                if chest["message_id"]:
                    await safe_edit(
                        chat_id, chest["message_id"],
                        format_chest_text(chest_id, status="expired"),
                        reply_markup=None,
                    )
                return
    except Exception:
        pass

    # Ja claimou?
    cur.execute(
        "SELECT slot FROM chest_claims WHERE chest_id=? AND user_id=?",
        (chest_id, uid),
    )
    prev = cur.fetchone()
    if prev:
        await cb.answer(f"Você já abriu (slot {prev['slot']}).", show_alert=False)
        return

    # Aloca slot atomicamente
    cur.execute("SELECT COUNT(*) AS n FROM chest_claims WHERE chest_id=?", (chest_id,))
    n = cur.fetchone()["n"]
    if n >= CHEST_MAX_CLAIMS:
        cur.execute(
            "UPDATE chests SET status='closed' WHERE id=? AND status='open'",
            (chest_id,),
        )
        db.commit()
        await cb.answer("🔒 Baú esvaziou.", show_alert=False)
        if chest["message_id"]:
            await safe_edit(
                chat_id, chest["message_id"],
                format_chest_text(chest_id, status="closed"),
                reply_markup=None,
            )
        return

    slot = n + 1
    xp, gold = CHEST_REWARDS[slot - 1]
    name = (cb.from_user.full_name or cb.from_user.first_name
            or (f"@{cb.from_user.username}" if cb.from_user.username else "anon"))[:32]

    cur.execute(
        "INSERT OR IGNORE INTO chest_claims "
        "(chest_id, user_id, slot, xp, gold, name, claimed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (chest_id, uid, slot, xp, gold, name, utc_iso()),
    )
    if cur.rowcount == 0:
        db.commit()
        await cb.answer("Você já abriu.", show_alert=False)
        return

    ensure_player(chat_id, uid)
    award_xp_immediate(chat_id, uid, xp, reason="chest")
    cur.execute(
        "UPDATE players SET gold=gold+? WHERE chat_id=? AND user_id=?",
        (gold, chat_id, uid),
    )
    db.commit()

    # Fechou no ultimo slot?
    final_status = "open"
    if slot >= CHEST_MAX_CLAIMS:
        cur.execute("UPDATE chests SET status='closed' WHERE id=?", (chest_id,))
        db.commit()
        final_status = "closed"

    if chest["message_id"]:
        kb = chest_keyboard(chest_id) if final_status == "open" else None
        try:
            await safe_edit(
                chat_id, chest["message_id"],
                format_chest_text(chest_id, status=final_status),
                reply_markup=kb,
            )
        except Exception:
            logger.exception("chest edit failed")

    await cb.answer(f"🎁 +{xp} XP  +{gold}🪙", show_alert=False)

    # Quando o bau eh totalmente saqueado, joga uma slot machine 🎰
    # como celebracao (animacao nativa do Telegram, ~3s). Eh PURO
    # flourish visual: o valor sorteado NAO afeta nada do jogo, ja
    # que as recompensas foram fixadas em CHEST_REWARDS por slot.
    if final_status == "closed":
        await roll_dice_visual(chat_id, emoji="🎰")


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
        ikb("⚔️ Atacar!", callback_data=f"r:atk:{boss_id}", style=STYLE_NO)
    ]])


def format_boss_text(boss: dict) -> str:
    bar_len = 20
    pct = boss["hp"] / max(1, boss["max_hp"])
    filled = max(0, min(bar_len, int(round(pct * bar_len))))
    bar = "█" * filled + "░" * (bar_len - filled)
    pct_int = int(pct * 100)
    cur.execute("SELECT COUNT(DISTINCT user_id) AS n FROM boss_hits WHERE boss_id=?", (boss["id"],))
    attackers = cur.fetchone()["n"]
    hp_now = f"{int(boss['hp']):,}".replace(",", ".")
    hp_max = f"{int(boss['max_hp']):,}".replace(",", ".")
    body = (
        f">> <b>{boss['name']}</b>\n"
        f"<pre>HP {bar} {pct_int:>3}%</pre>"
        f"❤️ <code>{hp_now}/{hp_max}</code>  ·  "
        f"⚔️ <code>{attackers}</code> atacantes\n"
        f"<i>Toca em Atacar pra causar dano!</i>"
    )
    return term_block("BOSS", body,
                      status="HOSTIL", status_color="HOT")


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
        # Drama cinematografico: alerta digita-se char-by-char antes da
        # ficha do boss aparecer. Eh nova mensagem separada do card —
        # auto-deleta em 60s pra nao poluir o grupo a longo prazo.
        intro = await reveal_text(
            chat_id,
            ">> !! ALERTA — ANOMALIA DETECTADA NO REINO\n"
            ">> CONECTANDO PROTOCOLO BOSS_v0.7 ...\n"
            ">> ENTIDADE MATERIALIZANDO ...",
            chunk_delay=0.9,
        )
        if intro:
            await auto_delete_after(intro, delay=60.0)
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
            name = get_anon_name(chat_id, r["user_id"])
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
                # Hall da Fama eh persistido — anonimizar usando o royal_id
                # do proprio row pra evitar query extra
                name = anonize(get_name(chat_id, r["user_id"]), r["royal_id"])
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

# ---------------------------------------------------------------------
# TUTORIAL.SYS — ensina a jogar passo a passo (usado em /start, /help,
# /royalajuda e /royaltutorial). Cada passo num blockquote expandable
# pro user abrir só o que quiser ler.
# ---------------------------------------------------------------------

ROYAL_TUTORIAL = (
    "<b>// TUTORIAL.SYS · COMO JOGAR</b>\n"
    "<i>Toca em cada passo pra expandir ▾</i>\n\n"

    "<blockquote expandable>"
    "<b>>> PASSO 1 · CHEGADA</b>\n"
    "Entra num grupo onde o Royal tá ativo. Manda <code>/royal</code> "
    "pra abrir o terminal principal e ver o que rola no reino.\n"
    "<i>// se for admin do grupo, use /royalativar primeiro.</i>"
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 2 · GANHE XP</b>\n"
    f"Você ganha <b>{XP_PER_MESSAGE} XP</b> por mensagem e "
    f"<b>{XP_PER_REPLY} XP</b> por reply.\n"
    f">> cooldown: {XP_COOLDOWN_MSG_SECONDS}s msg / "
    f"{XP_COOLDOWN_REPLY_SECONDS}s reply.\n"
    "Quanto mais você conversa, mais sobe."
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 3 · SUA FICHA</b>\n"
    "Usa <code>/royalperfil</code> pra ver seu cartão de identidade — "
    "level, atributos, ranking, casórios, florins.\n"
    "<i>// cada nobre tem um RYL ID único.</i>"
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 4 · LEVEL UP</b>\n"
    f"Cada nível te dá <b>{PTS_PER_LEVEL} ponto(s)</b> de atributo.\n"
    "Usa <code>/royalup</code> pra distribuir em:\n"
    "• 💪 FORÇA — mais dano no boss\n"
    "• 🏃 DESTREZA — mais XP em palavras\n"
    "• 🛡️ VITALIDADE — mais HP\n"
    "• ✨ CARISMA — mais chance em casórios"
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 5 · ESCOLHA UMA CLASSE</b>\n"
    "<code>/royalclasse</code> — define teu papel no reino.\n"
    "Cada classe tem um bônus passivo. Escolha com cuidado, "
    "<i>vale pra temporada inteira.</i>"
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 6 · PALAVRA DA HORA</b>\n"
    "A cada <b>60 min</b> o bot solta um desafio no grupo "
    "(anagrama, letras faltando ou charada).\n"
    f"<i>Primeiro a acertar leva <b>{XP_PALAVRA_WIN_BONUS} XP</b> "
    f"+ <b>{GOLD_PALAVRA_WIN} 🪙</b>.</i>\n"
    ">> só responder no chat. /royalpalavra mostra o ativo."
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 7 · BOSS DA SEMANA</b>\n"
    f"Todo <b>domingo às {BOSS_SPAWN_HOUR}h</b> nasce um boss. "
    "Todo mundo do grupo ataca junto.\n"
    "Usa <code>/royalboss</code> pra ver HP e dar o golpe.\n"
    "<i>// recompensa em XP + florins se derrubarem.</i>"
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 8 · CASORIOS</b>\n"
    "<b>3x ao dia</b> o bot escolhe um par e abre votação ❤️/🤮.\n"
    "Casamentos ativos dão <b>+10% XP</b> e contam pro ranking.\n"
    ">> <code>/royalmeuscasorios</code> · <code>/royalcasorios</code>\n"
    "<i>// /royalencalhar pra sair da fila.</i>"
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 9 · LOJA &amp; INVENTARIO</b>\n"
    "Florins 🪙 vêm de palavras, boss e eventos.\n"
    "<code>/royalloja</code> compra itens, "
    "<code>/royalinventario</code> equipa.\n"
    "<i>// alguns itens dão buff passivo.</i>"
    "</blockquote>"

    "<blockquote expandable>"
    "<b>>> PASSO 10 · TEMPORADAS</b>\n"
    "O reino segue as estações do ano. Cada temporada zera o "
    "ranking, mas <b>seu nível total fica.</b>\n"
    ">> <code>/royalranking</code> pra ver o top 10 atual."
    "</blockquote>"

    "<i>>> pronto, nobre. boa caçada ⚔️</i>"
)


def tutorial_block() -> str:
    """Retorna tutorial envolvido em term_block."""
    return term_block(
        "TUTORIAL", ROYAL_TUTORIAL,
        status="ONBOARDING", status_color="CYAN",
        stamp=current_season_label(),
    )


@dp.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type != "private":
        return
    body = (
        "<b>// SISTEMA ROYAL INICIALIZADO</b>\n"
        "<i>A corte te aguardava, nobre. Bem-vindo ao </i>"
        "<b>RPG - Royal para Geeks</b><i>.</i>\n"
        "<blockquote expandable>"
        ">> ROTAS PESSOAIS (aqui na DM)\n"
        "• 👤 /royalperfil — cartao de identidade\n"
        "• 📜 /royalficha — atalho pro perfil\n"
        "• 🏆 /royalranking — top 10 da temporada\n"
        "• 🎒 /royalinventario — seus itens\n"
        "• 💰 /royalsaldo — florins na arca 🪙\n"
        "• 📊 /royalmeuscasorios — seu historico\n"
        "• 👑 /royalavatar — escolher avatar\n"
        "• 🏰 /royalgrupo — trocar grupo ativo\n"
        "• 🔒 /royalprivacidade — controles\n"
        "• 📦 /royaldados — exportar / apagar\n"
        "• 📖 /royaltutorial — aprender a jogar\n"
        "• ❓ /royalajuda — manual completo"
        "</blockquote>"
        "<blockquote expandable>"
        ">> INLINE — usa em QUALQUER chat\n"
        "Digita <code>@nome_do_bot</code> em qualquer conversa\n"
        "e envia seu cartao de perfil onde quiser ✨"
        "</blockquote>"
        "<i>>> Se voce participa de mais de um grupo Royal,\n"
        "use /royalgrupo pra escolher qual eh o ativo na DM.</i>"
    )
    await message.answer(
        term_block("BOOT", body, status="CONECTADO",
                   stamp=current_season_label()),
        reply_markup=private_menu,
        **effect_kw(message.chat.type, EFFECT_PARTY),
    )
    # Tutorial completo logo em seguida (primeira impressão = ensina o jogo)
    await message.answer(tutorial_block())


# === /royal — HUB ===

@dp.message(Command("royal"))
async def royal_hub(message: Message):
    # Bot reage ao comando antes de responder — feedback instantaneo
    if message.from_user:
        await react_to(message.chat.id, message.message_id, "👀")
    await safe_typing(message.chat.id)
    await message.answer(hub_text(), reply_markup=hub_keyboard_main())


@dp.message(F.text == "👑 Reino")
async def royal_hub_btn(message: Message):
    await message.answer(hub_text(), reply_markup=hub_keyboard_main())


# === /royalajuda ===

ROYAL_HELP = (
    "👑 <b>RPG - Royal para Geeks — Guia do Reino</b>\n"
    "<i>Toca em cada seção pra expandir ▾</i>\n\n"
    "<blockquote expandable>🏰 <b>Geral</b>\n"
    "/royal — menu principal\n"
    "/royalperfil — seu cartão de perfil (DM ou grupo)\n"
    "/royalperfil RYL-0042 — perfil de outro nobre\n"
    "/royalficha — atalho pro perfil\n"
    "/royalavatar — escolher avatar (1x por temporada)\n"
    "/royaltutorial — aprender a jogar\n"
    "/royalajuda — esta ajuda</blockquote>\n"
    "<blockquote expandable>⚔️ <b>RPG (grupo)</b>\n"
    "/royalup — distribuir pontos de atributo\n"
    "/royalclasse — escolher classe\n"
    "/royalloja — comprar itens</blockquote>\n"
    "<blockquote expandable>📜 <b>Pessoal (DM ou grupo)</b>\n"
    "/royalinventario — ver seus itens\n"
    "/royalsaldo — quantos florins você tem 🪙\n"
    "/royalranking — top 10 da temporada\n"
    "/royalmeuscasorios — seu histórico de casais</blockquote>\n"
    "<blockquote expandable>🎯 <b>Eventos (grupo)</b>\n"
    "/royalpalavra — status da Palavra da Hora\n"
    "/royalboss — status do boss da semana\n"
    "/royalcasorios — ranking dos casais</blockquote>\n"
    "<blockquote expandable>💍 <b>Shipper (grupo)</b>\n"
    "/royalencalhar — sair do shipper\n"
    "/royaldesencalhar — voltar pro shipper\n"
    "/royalcasar — admin: forçar casório\n"
    "/royalativar — admin: ativar bot no grupo</blockquote>\n"
    "<blockquote expandable>💬 <b>DM + Inline</b>\n"
    "Os comandos pessoais (perfil/ficha/ranking/inventario/saldo/\n"
    "meuscasorios) funcionam aqui na DM também. Se você participa\n"
    "de mais de um grupo Royal, escolha o ativo com:\n"
    "/royalgrupo — ver ou trocar grupo ativo\n\n"
    "🔗 <b>Inline mode:</b> digita <code>@nome_do_bot</code> em "
    "qualquer chat pra enviar seu cartão de perfil 📜</blockquote>\n"
    "<blockquote expandable>🔒 <b>Privacidade &amp; dados (DM)</b>\n"
    "/royalprivacidade — controles de privacidade\n"
    "/royaldados — exportar ou apagar seus dados</blockquote>"
)


@dp.message(Command("royalajuda"))
async def royal_ajuda(message: Message):
    # Tutorial primeiro (ensina), depois manual de comandos (referência)
    await message.answer(tutorial_block())
    await message.answer(ROYAL_HELP)


@dp.message(Command("help"))
async def help_cmd(message: Message):
    await message.answer(tutorial_block())
    await message.answer(ROYAL_HELP)


@dp.message(Command("royaltutorial"))
async def royal_tutorial_cmd(message: Message):
    await message.answer(tutorial_block())


# === /royalperfil ===

@dp.message(Command("royalperfil", "royalficha"))
async def royal_perfil(message: Message):
    if not message.from_user:
        return
    # Reage 👀 imediatamente — o user ve feedback antes do card renderizar
    await react_to(message.chat.id, message.message_id, "👀")
    await safe_typing(message.chat.id, "upload_photo")
    # /royalperfil RYL-0042
    parts = (message.text or "").split(maxsplit=1)
    live_name = display_name(message)
    live_username = message.from_user.username
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
        # Se o alvo for o proprio requisitante, refresca o nome vivo p/ evitar
        # ANON-X quando users.display_name esta vazio
        if p["user_id"] == message.from_user.id:
            refresh_user_identity(p["chat_id"], p["user_id"], live_name, live_username)
            db.commit()
        await send_profile_card(message.chat.id, p["chat_id"], p["user_id"])
        return

    # Sem argumento: perfil proprio
    if is_group(message):
        ensure_chat(message.chat.id, message.chat.title)
        # Garante users.display_name fresco — evita fallback p/ str(user_id)
        # que dispararia anonize() no proprio dono e mostraria "ANON-X"
        refresh_user_identity(message.chat.id, message.from_user.id, live_name, live_username)
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
        # Idem: refresca o nome no chat-dono (grupo) sem inflar message_count
        refresh_user_identity(owner_chat, message.from_user.id, live_name, live_username)
        db.commit()
        await send_profile_card(message.chat.id, owner_chat, message.from_user.id)


# === /royalavatar ===

AVATAR_MOSAIC_MARKER = "AVATAR.SYS"


class AvatarReplyFilter(Filter):
    """Matcheia apenas mensagens que sao reply ao mosaico do /royalavatar
    contendo um numero 1..36. Retorna False (cai pro proximo handler) caso
    contrario, pra nao engolir mensagens normais."""

    async def __call__(self, message: Message) -> bool:
        rep = message.reply_to_message
        if rep is None or rep.from_user is None or not rep.from_user.is_bot:
            return False
        cap = rep.caption or rep.text or ""
        if AVATAR_MOSAIC_MARKER not in cap or "ESCOLHA SEU AVATAR" not in cap:
            return False
        txt = (message.text or "").strip()
        if not txt.isdigit():
            return False
        n = int(txt)
        return 1 <= n <= len(royal_avatars.SLUGS)


@dp.message(Command("royalavatar"))
async def royal_avatar_cmd(message: Message):
    if not message.from_user:
        return
    await react_to(message.chat.id, message.message_id, "👀")
    # Resolve em qual chat-dono este user tem perfil RPG
    if is_group(message):
        owner_chat = message.chat.id
        ensure_chat(message.chat.id, message.chat.title)
        ensure_player(message.chat.id, message.from_user.id)
        db.commit()
    else:
        owner_chat = resolve_owner_chat(message.from_user.id)
        if owner_chat is None:
            await message.answer(
                "😶 Você ainda não tem perfil no Reino.\n"
                "Envie uma mensagem no grupo Royal para começar!")
            return

    p = get_player(owner_chat, message.from_user.id)
    if not p:
        await message.answer("😶 Você ainda não tem perfil no Reino.")
        return

    season = current_season_label()
    royal_id = p.get("royal_id") or ""
    cur_slug = royal_avatars.resolve_slug(p.get("avatar_slug"), royal_id)
    cur_num = royal_avatars.number_of(cur_slug)
    cur_name = royal_avatars.display_name(cur_slug)
    is_default = not p.get("avatar_slug")

    # Bloqueio: ja escolheu nesta temporada
    if p.get("avatar_slug") and p.get("avatar_season") == season:
        msg = await message.answer(term_block(
            "AVATAR.SYS",
            (f"<b>!! Voce ja escolheu seu avatar nesta temporada.</b>\n"
             f"<i>Atual: #{cur_num:02d} {cur_name}</i>\n"
             f"// Proxima troca disponivel na proxima temporada."),
            status="BLOQUEADO", status_color="HOT",
            stamp=season,
        ))
        await auto_delete_after(msg, delay=15.0)
        return

    await safe_typing(message.chat.id, "upload_photo")
    png = royal_avatars.mosaic_bytes()
    if not png:
        await message.answer("⚠️ Mosaico de avatares indisponivel.")
        return

    atual_label = (f"<i>Atual (default): #{cur_num:02d} {cur_name}</i>"
                   if is_default else
                   f"<i>Atual: #{cur_num:02d} {cur_name}</i>")
    caption = term_block(
        AVATAR_MOSAIC_MARKER,
        (f"<b>👑 ESCOLHA SEU AVATAR</b>\n"
         f"{atual_label}\n\n"
         f"<b>&gt;&gt; Responda esta mensagem com um numero de 1 a 36.</b>\n"
         f"⚠️ A escolha vale por toda a temporada (so podera trocar na proxima)."),
        status="ESCOLHA", status_color="ACID",
        stamp=season,
    )
    await message.answer_photo(
        BufferedInputFile(png, filename="royal_avatares_mosaico.png"),
        caption=caption,
    )


@dp.message(AvatarReplyFilter())
async def handle_avatar_reply(message: Message):
    if not message.from_user:
        return
    n = int((message.text or "").strip())
    slug = royal_avatars.slug_at(n)
    if slug is None:
        return  # filtro ja validou range, defensivo

    # Resolve chat-dono
    if is_group(message):
        owner_chat = message.chat.id
    else:
        owner_chat = resolve_owner_chat(message.from_user.id)
        if owner_chat is None:
            return

    p = get_player(owner_chat, message.from_user.id)
    if not p:
        ack = await message.reply("😶 Voce ainda nao tem perfil no Reino.")
        await auto_delete_after(ack, delay=8.0)
        return

    season = current_season_label()
    if p.get("avatar_slug") and p.get("avatar_season") == season:
        cur_slug = p["avatar_slug"]
        ack = await message.reply(
            f"❌ Voce ja escolheu seu avatar nesta temporada "
            f"(#{royal_avatars.number_of(cur_slug):02d} "
            f"{royal_avatars.display_name(cur_slug)}). "
            f"Proxima troca disponivel na proxima temporada."
        )
        await auto_delete_after(ack, delay=12.0)
        return

    cur.execute(
        "UPDATE players SET avatar_slug=?, avatar_season=? "
        "WHERE chat_id=? AND user_id=?",
        (slug, season, owner_chat, message.from_user.id),
    )
    db.commit()
    # Invalida o card cacheado (proximo /royalperfil re-renderiza com o novo avatar)

    name = royal_avatars.display_name(slug)
    royal_id = p.get("royal_id") or ""
    caption = term_block(
        AVATAR_MOSAIC_MARKER,
        (f"<b>✅ Avatar atualizado: #{n:02d} {name}</b>\n"
         f"<i>Esta escolha vale por toda a temporada {season}.</i>\n"
         f"// Use /royalperfil para ver o novo card."),
        status="OK", status_color="ACID",
        stamp=royal_id,
    )
    fp = os.path.join(os.path.dirname(__file__),
                      "assets", "avatars", f"{slug}.png")
    try:
        with open(fp, "rb") as f:
            png = f.read()
        await message.reply_photo(
            BufferedInputFile(png, filename=f"avatar-{slug}.png"),
            caption=caption,
            **effect_kw(message.chat.type, EFFECT_PARTY),
        )
    except Exception:
        await message.reply(
            caption, **effect_kw(message.chat.type, EFFECT_PARTY))


# === /royalup ===

@dp.message(Command("royalup"))
async def royal_up(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    chat_id = message.chat.id
    p = ensure_player(chat_id, message.from_user.id)
    db.commit()
    if p["pts_available"] <= 0:
        # Ack efemero: bot reage 🤷, manda card AMARELO de "vazio" e
        # auto-deleta em 10s pra nao poluir o chat do grupo
        await react_to(chat_id, message.message_id, "🤷‍♂️")
        ack = await message.answer(term_block(
            "ATRIBUTOS",
            "<i>Sem pontos. Suba de nível primeiro ⭐</i>",
            status="VAZIO", status_color="AMBER"))
        await auto_delete_after(ack, delay=10.0)
        return
    # Reage ✅ quando ha pontos — feedback positivo imediato
    await react_to(chat_id, message.message_id, "✍")
    body = (
        f"<b>{p['pts_available']}</b> ponto(s) para distribuir.\n"
        f"<i>Escolha um atributo abaixo:</i>"
    )
    await message.answer(
        term_block("ATRIBUTOS", body, status="READY"),
        reply_markup=up_keyboard(),
    )


def up_keyboard() -> InlineKeyboardMarkup:
    # FOR -> vermelho (combate), VIT -> verde (vida), DES/CAR -> azul (skill)
    return InlineKeyboardMarkup(inline_keyboard=[[
        ikb("💪 FORÇA",    callback_data="r:up:for", style=STYLE_NO),
        ikb("🏃 DESTREZA", callback_data="r:up:des", style=STYLE_INFO),
        ikb("❤️ VITAL",    callback_data="r:up:vit", style=STYLE_OK),
        ikb("✨ CARISMA",  callback_data="r:up:car", style=STYLE_INFO),
    ]])


# === /royalclasse ===

def classe_keyboard() -> InlineKeyboardMarkup:
    rows = []
    items = list(CLASSES.items())
    for i in range(0, len(items), 2):
        row = []
        for cid, info in items[i:i+2]:
            row.append(ikb(
                f"{info['emoji']} {info['name']}",
                callback_data=f"r:cls:set:{cid}",
                style=STYLE_INFO))
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("royalclasse"))
async def royal_classe(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    ensure_player(message.chat.id, message.from_user.id)
    db.commit()
    lines = []
    for cid, info in CLASSES.items():
        lines.append(f"{info['emoji']} <b>{info['name']}</b> — <i>{info['bonus']}</i>")
    body = "<i>Escolha sua identidade no reino:</i>\n\n" + "\n".join(lines)
    await message.answer(
        term_block("CLASSE", body, status="SELECAO", status_color="CYAN"),
        reply_markup=classe_keyboard())


# === /royalinventario ===

@dp.message(Command("royalinventario"))
async def royal_inv(message: Message):
    if not message.from_user:
        return
    chat_id = await resolve_dm_chat(message, action_hint="ver inventario")
    if chat_id is None:
        return
    uid = message.from_user.id
    ensure_player(chat_id, uid)
    db.commit()
    cur.execute(
        "SELECT item_id, qty, equipped FROM inventory WHERE chat_id=? AND user_id=? ORDER BY equipped DESC",
        (chat_id, uid))
    rows = cur.fetchall()
    if not rows:
        await message.answer(term_block(
            "INVENTARIO",
            "<i>Cofre vazio. Visita a /royalloja pra comprar itens.</i>",
            status="VAZIO", status_color="AMBER"))
        return
    parts = []
    for r in rows:
        item = ITEMS.get(r["item_id"])
        if not item:
            continue
        eq = "✅ " if r["equipped"] else "  "
        parts.append(
            f"{eq}{item['emoji']} <b>{item['name']}</b> "
            f"<code>x{r['qty']}</code>\n   <i>{item['desc']}</i>"
        )
    body = "\n\n".join(parts)
    await message.answer(
        term_block("INVENTARIO", body, status="LOADED"),
        reply_markup=inv_keyboard(chat_id, uid))


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
            buttons.append([ikb(
                f"{item['emoji']} {label}",
                callback_data=f"r:inv:eq:{r['item_id']}",
                style=STYLE_NO if r["equipped"] else STYLE_OK)])
        elif item["type"] == "consumable":
            buttons.append([ikb(
                f"{item['emoji']} Usar",
                callback_data=f"r:inv:use:{r['item_id']}",
                style=STYLE_OK)])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[
        ikb("🛒 Ir à loja", callback_data="r:loja", style=STYLE_INFO)]])


# === /royalloja ===

@dp.message(Command("royalloja"))
async def royal_loja(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    p = ensure_player(message.chat.id, message.from_user.id)
    db.commit()
    gold_br = f"{int(p['gold']):,}".replace(",", ".")
    parts = [f">> SALDO: <b><code>{gold_br}</code></b> florins 🪙\n"]
    for iid, item in ITEMS.items():
        parts.append(
            f"{item['emoji']} <b>{item['name']}</b> "
            f"— <code>{item['price']}</code>🪙\n   <i>{item['desc']}</i>"
        )
    body = "\n\n".join(parts)
    await message.answer(
        term_block("LOJA", body, status="OPEN", status_color="ACID"),
        reply_markup=loja_keyboard())


def loja_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for iid, item in ITEMS.items():
        rows.append([ikb(
            f"💳 {item['emoji']} {item['name']} ({item['price']}🪙)",
            callback_data=f"r:buy:{iid}",
            style=STYLE_INFO)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# === /royalsaldo ===

@dp.message(Command("royalsaldo"))
async def royal_saldo(message: Message):
    if not message.from_user:
        return
    owner_chat = await resolve_dm_chat(message, action_hint="ver saldo")
    if owner_chat is None:
        return
    p = ensure_player(owner_chat, message.from_user.id)
    db.commit()
    gold_br = f"{int(p['gold']):,}".replace(",", ".")
    body = (
        f"🪙 Suas arcas guardam <b><code>{gold_br}</code></b> florins.\n"
        f"<i>Gasta com sabedoria em /royalloja.</i>"
    )
    await message.answer(term_block("FLORINS", body,
                                    status="SALDO_OK",
                                    stamp=f"ID {p.get('royal_id', 'RYL-????')}"))


# === /royalranking ===

@dp.message(Command("royalranking"))
async def royal_ranking(message: Message):
    if not message.from_user:
        return
    owner_chat = await resolve_dm_chat(message, action_hint="ver ranking")
    if owner_chat is None:
        return
    if await deny_if_rate_limited(message, "RANKING", cooldown=15.0):
        return
    await send_ranking(owner_chat, target_chat_id=message.chat.id)


async def send_ranking(source_chat_id: int, *, target_chat_id: int) -> None:
    """Envia ranking: card pódio (top 3) + caption com top 10."""
    cur.execute(
        "SELECT royal_id, user_id, season_xp, total_xp, avatar_slug "
        "FROM players "
        "WHERE chat_id=? AND season_xp > 0 AND privacy_hide_ranking=0 "
        "ORDER BY season_xp DESC LIMIT 10", (source_chat_id,))
    rows = cur.fetchall()
    if not rows:
        await safe_send(target_chat_id, term_block(
            "RANKING",
            "<i>Ninguém pontuou nesta temporada ainda.\n"
            "Interaja pra subir no pódio ⚔️</i>",
            status="VAZIO", status_color="AMBER",
            stamp=current_season_label()))
        return

    medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
    season = current_season_label()
    lines = []
    entries = []
    for i, r in enumerate(rows, 1):
        # Ranking publico — usa royal_id do row direto pra anonimizar
        name = anonize(get_name(source_chat_id, r["user_id"]), r["royal_id"])
        # Caption HTML mantém fancy; card PNG cai pro @username se Pillow
        # não conseguir desenhar (Unicode Math/Fraktur/Fullwidth → tofu ▯).
        card_name = card_safe_name(source_chat_id, r["user_id"],
                                   name, r["royal_id"])
        full_lvl, *_ = level_progress(r["total_xp"] or 0)
        lines.append(
            f"{medals[i-1]} <b>{i}.</b> {html.escape(name)} "
            f"<code>{r['royal_id']}</code> — "
            f"Lvl <b>{full_lvl}</b> · {r['season_xp']} XP"
        )
        entries.append(RankingEntry(
            rank=i, royal_id=r["royal_id"], name=card_name,
            season_xp=int(r["season_xp"]), level=int(full_lvl),
            avatar_slug=royal_avatars.resolve_slug(
                r["avatar_slug"], r["royal_id"])))

    body = "\n".join(lines)
    caption = term_block("RANKING", body,
                         status="LIVE", status_color="ACID",
                         stamp=season)
    if len(caption) > 1024:
        caption = term_block("RANKING",
                             "\n".join(lines[:6]),
                             status="LIVE", status_color="ACID",
                             stamp=season)

    # tenta enviar card pódio (top 3); fallback caption-only
    await safe_typing(target_chat_id, "upload_photo")
    try:
        png = await asyncio.to_thread(
            render_ranking_card, season, tuple(entries))
        if png:
            await bot.send_photo(
                target_chat_id,
                BufferedInputFile(png, filename="royal_ranking.jpg"),
                caption=caption)
            return
    except Exception:
        logger.exception("ranking card send failed; falling back to text")
    await safe_send(target_chat_id, caption)


# === /royalpalavratest (admin) — dispara 1 desafio spoiler_img na hora ===

@dp.message(Command("royalpalavratest"))
async def royal_palavra_test(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    if not await is_admin(message):
        ack = await message.answer(term_block(
            "PALAVRA", ">> <b>!! ACESSO NEGADO</b>\n"
            "<i>// só admins podem disparar testes.</i>",
            status="NEGADO", status_color="HOT"))
        if ack:
            await auto_delete_after(ack, delay=8.0)
        return
    existing = get_active_challenge(message.chat.id)
    if existing:
        ack = await message.answer(term_block(
            "PALAVRA", ">> <b>já existe desafio ativo</b>\n"
            f"<i>// /royalpalavra mostra o atual (id={existing['id']}).</i>",
            status="OCUPADO", status_color="AMBER"))
        if ack:
            await auto_delete_after(ack, delay=8.0)
        return
    await safe_typing(message.chat.id, "upload_photo")
    await spawn_palavra(message.chat.id)


# === /royalpalavra ===

@dp.message(Command("royalpalavra"))
async def royal_palavra_status(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
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
                await message.answer(term_block(
                    "PALAVRA",
                    f"<i>Sem transmissão ativa.</i>\n>> próxima em <b>~{mins} min</b>",
                    status="STANDBY", status_color="AMBER"))
                return
            except Exception:
                pass
        await message.answer(term_block(
            "PALAVRA",
            "<i>Sem transmissão ativa. Aguarde o próximo sinal.</i>",
            status="STANDBY", status_color="AMBER"))
        return
    await message.answer(format_challenge_text(ch))


# === /royalboss ===

@dp.message(Command("royalboss"))
async def royal_boss_status(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    boss = get_active_boss(message.chat.id)
    if not boss:
        await message.answer(term_block(
            "BOSS",
            f"<i>Nenhuma anomalia detectada.</i>\n"
            f">> próximo spawn: <b>domingo {BOSS_SPAWN_HOUR}h</b>",
            status="OFFLINE", status_color="AMBER"))
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
    hide_rank = bool(p["privacy_hide_ranking"])
    hide_stats = bool(p["privacy_hide_stats"])
    rank_btn = ("🙈 Estou oculto do ranking" if hide_rank
                else "🏆 Estou no ranking")
    stats_btn = ("🙈 Stats detalhados ocultos" if hide_stats
                 else "📊 Stats detalhados visíveis")
    rows = [
        ("RANKING", "OCULTO" if hide_rank else "VISIVEL"),
        ("STATS DETALHADOS", "OCULTOS" if hide_stats else "VISIVEIS"),
    ]
    body = f"{term_pre(rows)}<i>Toca nos botões pra alternar.</i>"
    await message.answer(
        term_block("PRIVACIDADE", body,
                   status="CONFIG", status_color="CYAN",
                   stamp="dados sob seu controle"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [ikb(rank_btn, callback_data="r:priv:rank", style=STYLE_INFO)],
            [ikb(stats_btn, callback_data="r:priv:stats", style=STYLE_INFO)],
        ]))


@dp.message(Command("royaldados"))
async def royal_dados(message: Message):
    if not message.from_user:
        return
    if is_group(message):
        await message.answer(term_block(
            "DADOS",
            "<i>Use /royaldados no chat privado comigo.</i>",
            status="DM_ONLY", status_color="AMBER"))
        return
    body = (
        "<i>Você controla seus dados no reino:</i>\n"
        "<blockquote>"
        "📥 <b>Exportar</b> — JSON com seus perfis e registros\n"
        "🗑️ <b>Apagar</b> — remove tudo (irrecuperável)"
        "</blockquote>"
    )
    await message.answer(
        term_block("DADOS", body, status="CONFIG", status_color="CYAN",
                   stamp="LGPD · dados sob seu controle"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [ikb("📥 Exportar JSON", callback_data="r:dados:export", style=STYLE_INFO)],
            [ikb("🗑️ Apagar tudo", callback_data="r:dados:wipe", style=STYLE_NO)],
        ]))


# === /royalgrupo (DM) — ver/trocar grupo ativo ===

@dp.message(Command("royalgrupo"))
async def royal_grupo(message: Message):
    if not message.from_user:
        return
    if is_group(message):
        await message.answer(term_block(
            "REINO",
            "<i>Esse comando configura o grupo ativo da sua DM.\n"
            "Roda /royalgrupo no chat privado comigo.</i>",
            status="DM_ONLY", status_color="AMBER"))
        return
    uid = message.from_user.id
    groups = list_user_groups(uid)
    if not groups:
        await message.answer(term_block(
            "REINO",
            "<i>Voce ainda nao tem perfil em nenhum grupo Royal.</i>",
            status="SEM_REINO", status_color="AMBER"))
        return
    active = get_dm_active_chat(uid)
    active_title = next((t for c, t in groups if c == active), None)
    rows = [
        ("ATIVO", active_title or "—"),
        ("GRUPOS", str(len(groups))),
    ]
    body = (
        f"{term_pre(rows)}"
        "<i>Escolhe abaixo qual grupo passa a ser o ativo na DM:</i>"
    )
    await message.answer(
        term_block("REINO", body, status="CONFIG", status_color="CYAN",
                   stamp="trocavel a qualquer hora"),
        reply_markup=group_picker_kb(groups))


@dp.callback_query(F.data.startswith("g:pick:"))
async def cb_group_pick(cb: CallbackQuery):
    if not cb.from_user or not cb.data:
        await cb.answer()
        return
    try:
        chat_id = int(cb.data.split(":", 2)[2])
    except (ValueError, IndexError):
        await cb.answer("ID invalido", show_alert=False)
        return
    uid = cb.from_user.id
    if is_test_chat(chat_id) or not get_player(chat_id, uid):
        await cb.answer("Grupo indisponivel.", show_alert=True)
        return
    set_dm_active_chat(uid, chat_id)
    cur.execute("SELECT title FROM chats WHERE chat_id=?", (chat_id,))
    r = cur.fetchone()
    title = (r["title"] if r and r["title"] else f"Grupo {chat_id}")
    await cb.answer(f"✅ Ativo: {title}", show_alert=False)
    try:
        if cb.message:
            await cb.message.edit_text(term_block(
                "REINO",
                f"<b>>> ATIVO:</b> <i>{html.escape(title)}</i>\n"
                "<i>// Pronto. Agora /royalperfil, /royalranking, /royalficha "
                "e configs usam esse grupo por padrao.</i>",
                status="OK", status_color="ACID",
                stamp="troca via /royalgrupo"))
    except TelegramBadRequest:
        pass


# === Inline mode — /royalperfil em qualquer chat ===

@dp.inline_query()
async def inline_profile(iq: InlineQuery):
    """Permite o user enviar o card de perfil em qualquer chat via @bot.
    Usa o grupo ativo da DM (user_dm_settings) ou auto-resolve."""
    uid = iq.from_user.id if iq.from_user else 0
    if not uid:
        await iq.answer(results=[], cache_time=5, is_personal=True)
        return
    owner_chat = resolve_owner_chat(uid)
    if not owner_chat:
        await iq.answer(
            results=[InlineQueryResultArticle(
                id="no-profile",
                title="🚫 Sem perfil Royal",
                description="Manda mensagem no grupo Royal pra registrar.",
                input_message_content=InputTextMessageContent(
                    message_text=term_block(
                        "REINO",
                        "<i>Sem perfil no Reino ainda.</i>",
                        status="SEM_REINO", status_color="AMBER"),
                    parse_mode="HTML"),
            )],
            cache_time=10,
            is_personal=True,
            button=InlineQueryResultsButton(
                text="🏰 Configurar grupo na DM",
                start_parameter="grupo"),
        )
        return

    p = get_player(owner_chat, uid)
    royal_id = (p.get("royal_id") if p else None) or "RYL-????"
    caption = build_profile_caption(owner_chat, uid)
    if len(caption) > 1024:
        caption = caption[:1020] + "..."
    text_full = build_profile_text(owner_chat, uid)

    results: list = []
    file_id = get_cached_profile_file_id(owner_chat, uid)
    if file_id:
        results.append(InlineQueryResultCachedPhoto(
            id=f"perfil-photo-{owner_chat}-{uid}",
            photo_file_id=file_id,
            caption=caption,
            parse_mode="HTML",
        ))
    # Sempre adiciona o fallback de texto (cliente escolhe)
    results.append(InlineQueryResultArticle(
        id=f"perfil-text-{owner_chat}-{uid}",
        title=f"📜 Meu perfil — {royal_id}",
        description="Envia a ficha (texto) no chat atual.",
        input_message_content=InputTextMessageContent(
            message_text=text_full,
            parse_mode="HTML"),
    ))

    button = None
    if not file_id:
        # Sem foto cacheada — guia o user pra gerar uma via /royalperfil na DM
        button = InlineQueryResultsButton(
            text="📸 Gerar foto do perfil (abrir o bot)",
            start_parameter="cacheperfil")

    await iq.answer(
        results=results,
        cache_time=30,
        is_personal=True,
        button=button,
    )


# === Comandos do Shipper com prefixo royal ===

@dp.message(Command("royalativar", "noivado"))
async def royal_ativar(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    ensure_chat(message.chat.id, message.chat.title)
    cur.execute(
        "INSERT INTO chats_rpg (chat_id, current_season) VALUES (?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET current_season=excluded.current_season",
        (message.chat.id, current_season_code()))
    schedule_next_palavra(message.chat.id)
    db.commit()
    rows = [
        ("CASORIOS", "3x / dia"),
        ("PALAVRA", "60 min"),
        ("BOSS", f"dom {BOSS_SPAWN_HOUR}h"),
        ("TEMPORADA", current_season_label()),
        ("CHAT ID", str(message.chat.id)),
    ]
    body = (
        "<b>// SISTEMA ATIVADO NESTE GRUPO</b>\n"
        f"{term_pre(rows)}"
        "<i>Que comecem os feitos, nobre.</i>"
    )
    await message.answer(term_block(
        "ROYAL", body, status="ONLINE",
        stamp=current_season_label()),
        **effect_kw(message.chat.type, EFFECT_PARTY))


@dp.message(Command("royalcasar", "querocasar"))
async def royal_casar(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
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
    await message.answer(term_block(
        "SHIPPER",
        "🚫💔 <i>Modo encalhado(a) ativado.</i>\n"
        ">> sistema não vai mais te colocar em casórios.",
        status="OPT_OUT", status_color="AMBER"))


@dp.message(Command("royaldesencalhar", "desencalhar"))
async def royal_desencalhar(message: Message):
    if not message.from_user:
        return
    chat_id = message.chat.id
    upsert_user(chat_id, message.from_user.id, display_name(message), message.from_user.username)
    cur.execute("UPDATE users SET opt_out=0 WHERE chat_id=? AND user_id=?",
                (chat_id, message.from_user.id))
    db.commit()
    await message.answer(term_block(
        "SHIPPER",
        "🔄💘 <i>De volta ao jogo dos casórios!</i>",
        status="OPT_IN"),
        **effect_kw(message.chat.type, EFFECT_HEART))


@dp.message(Command("royalmeuscasorios", "meusdivorcios"))
async def royal_meus(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    chat_id = await resolve_dm_chat(message, action_hint="ver meus casorios")
    if chat_id is None:
        return
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
    body = f"<i>Você já participou de <b>{total}</b> casórios. 😳</i>"
    if rows:
        body += "\n\n<b>>> TOP PARES</b>\n" + "\n".join(
            f"• {html.escape(get_anon_name(chat_id, row['partner']))} — "
            f"<code>{row['total']}x</code>"
            for row in rows
        )
    await message.answer(term_block(
        "CASORIOS", body, status="HISTORICO", status_color="CYAN"))


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
        await message.answer(term_block(
            "CASORIOS",
            "<i>Ainda não existem casórios suficientes pra ranking.</i>",
            status="VAZIO", status_color="AMBER"))
        return
    medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
    lines = []
    for i, row in enumerate(rows, start=1):
        lines.append(
            f"{medals[i-1]} <b>{i}.</b> "
            f"{html.escape(get_anon_name(chat_id, row['user1']))} ❤️ "
            f"{html.escape(get_anon_name(chat_id, row['user2']))} — "
            f"<code>{row['total']}x</code>")
    await message.answer(term_block(
        "CASORIOS", "\n".join(lines),
        status="RANKING", stamp=current_season_label()))


# === Botoes do menu privado ===

@dp.message(F.text == "📊 Meus casórios")
async def btn_meus(message: Message):
    await royal_meus(message)


@dp.message(F.text == "📖 Tutorial")
async def btn_tutorial(message: Message):
    await message.answer(tutorial_block())


@dp.message(F.text == "❓ Como funciona")
async def btn_como_funciona(message: Message):
    body = (
        "<i>O bot observa interações no grupo (respostas, menções, conversa).</i>\n"
        "<blockquote expandable>"
        "💍 <b>CASORIOS</b> — 3x/dia, votação ❤️/🤮\n"
        "🎯 <b>PALAVRA</b> — 60min, primeiro a acertar leva XP+🪙\n"
        "🐉 <b>BOSS</b> — domingo 20h, todos atacam juntos\n"
        "⭐ <b>XP</b> — ganho ao interagir, sobe nível, distribui atributos\n"
        "🏆 <b>TEMPORADAS</b> — seguem as estações do ano"
        "</blockquote>"
        "<i>Use /royal pra acessar o menu completo.</i>"
    )
    await message.answer(term_block(
        "MANUAL", body, status="DOC", status_color="CYAN",
        stamp=current_season_label()))


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
        await cb.answer("🏰 Esse atalho precisa de um grupo do Reino. Volta pra lá pra usar.",
                        show_alert=True)
        return

    # Baú: callback vem do próprio chat onde foi postado; ignora needs_chat
    if action == "chest":
        if len(parts) < 3:
            await cb.answer()
            return
        try:
            chest_id = int(parts[2])
        except ValueError:
            await cb.answer()
            return
        await handle_chest_claim(cb, chest_id)
        return

    try:
        if action == "perfil":
            target_chat = chat_id or (cb.message.chat.id if is_group_chat(cb.message) else None)
            if target_chat is None:
                await cb.answer("Você ainda não tem perfil. Mande mensagem no grupo Royal.",
                                show_alert=True)
                return
            # Refresca nome vivo do Telegram p/ evitar ANON-X no proprio dono
            live = (cb.from_user.full_name or cb.from_user.first_name
                    or (f"@{cb.from_user.username}" if cb.from_user.username else ""))
            refresh_user_identity(target_chat, cb.from_user.id, live, cb.from_user.username)
            db.commit()
            await send_profile_card(cb.message.chat.id, target_chat, cb.from_user.id)
            await cb.answer()
            return

        if action == "rank":
            wait = rate_limited(cb.from_user.id, "RANKING", cooldown=15.0)
            if wait > 0:
                await cb.answer(f"⏳ Aguarde {wait}s", show_alert=False)
                return
            await cb.answer()
            await send_ranking(chat_id, target_chat_id=cb.message.chat.id)
            return

        if action == "pal":
            ch = get_active_challenge(chat_id)
            if ch:
                await cb.message.answer(format_challenge_text(ch))
            else:
                await cb.message.answer(term_block(
                    "PALAVRA",
                    "<i>Sem transmissão ativa agora.</i>",
                    status="STANDBY", status_color="AMBER"))
            await cb.answer()
            return

        if action == "boss":
            boss = get_active_boss(chat_id)
            if boss:
                await cb.message.answer(format_boss_text(boss),
                                        reply_markup=boss_keyboard(boss["id"]))
            else:
                await cb.message.answer(term_block(
                    "BOSS",
                    f"<i>Nenhuma anomalia detectada.</i>\n"
                    f">> próximo spawn: <b>domingo {BOSS_SPAWN_HOUR}h</b>",
                    status="OFFLINE", status_color="AMBER"))
            await cb.answer()
            return

        if action == "help":
            await cb.message.answer(ROYAL_HELP)
            await cb.answer()
            return

        if action == "loja":
            p = ensure_player(chat_id, cb.from_user.id)
            db.commit()
            gold_br = f"{int(p['gold']):,}".replace(",", ".")
            parts_b = [f">> SALDO: <b><code>{gold_br}</code></b> 🪙\n"]
            for iid, item in ITEMS.items():
                parts_b.append(
                    f"{item['emoji']} <b>{item['name']}</b> "
                    f"— <code>{item['price']}</code>🪙\n   <i>{item['desc']}</i>")
            await cb.message.answer(
                term_block("LOJA", "\n\n".join(parts_b),
                           status="OPEN", status_color="ACID"),
                reply_markup=loja_keyboard())
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
                await cb.message.answer(term_block(
                    "INVENTARIO",
                    "<i>Cofre vazio. Visita a /royalloja.</i>",
                    status="VAZIO", status_color="AMBER"))
            else:
                parts_b = []
                for r in rows:
                    it = ITEMS.get(r["item_id"])
                    if not it:
                        continue
                    eq = "✅ " if r["equipped"] else "  "
                    parts_b.append(
                        f"{eq}{it['emoji']} <b>{it['name']}</b> "
                        f"<code>x{r['qty']}</code>\n   <i>{it['desc']}</i>")
                await cb.message.answer(
                    term_block("INVENTARIO", "\n\n".join(parts_b),
                               status="LOADED"),
                    reply_markup=inv_keyboard(chat_id, cb.from_user.id))
            await cb.answer()
            return

        if action == "cas":
            cur.execute(
                "SELECT user1, user2, COUNT(*) AS total FROM couples "
                "WHERE chat_id=? GROUP BY user1, user2 ORDER BY total DESC LIMIT 10",
                (chat_id,))
            rows = cur.fetchall()
            if not rows:
                await cb.message.answer(term_block(
                    "CASORIOS",
                    "<i>Sem casórios suficientes ainda.</i>",
                    status="VAZIO", status_color="AMBER"))
            else:
                medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
                lines = []
                for i, r in enumerate(rows, 1):
                    lines.append(
                        f"{medals[i-1]} <b>{i}.</b> "
                        f"{html.escape(get_anon_name(chat_id, r['user1']))} ❤️ "
                        f"{html.escape(get_anon_name(chat_id, r['user2']))} — "
                        f"<code>{r['total']}x</code>")
                await cb.message.answer(term_block(
                    "CASORIOS", "\n".join(lines),
                    status="RANKING", stamp=current_season_label()))
            await cb.answer()
            return

        if action == "up":
            sub = parts[2] if len(parts) > 2 else "menu"
            if sub == "menu":
                p = ensure_player(chat_id, cb.from_user.id)
                db.commit()
                await cb.message.answer(
                    term_block("ATRIBUTOS",
                               f"<b>{p['pts_available']}</b> ponto(s) disponíveis.\n"
                               f"<i>Escolha um atributo abaixo:</i>",
                               status="READY"),
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
                lines = []
                for cid, info in CLASSES.items():
                    lines.append(
                        f"{info['emoji']} <b>{info['name']}</b> — "
                        f"<i>{info['bonus']}</i>")
                await cb.message.answer(
                    term_block("CLASSE",
                               "<i>Escolha sua identidade no reino:</i>\n\n"
                               + "\n".join(lines),
                               status="SELECAO", status_color="CYAN"),
                    reply_markup=classe_keyboard())
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

# =====================================================================
# MUSIC BOT BRIDGE — pontua user marcado em publicacao do bot de musica
# externo (MUSIC_BOT_ID). Privacy mode do RPG precisa estar OFF.
# =====================================================================

def _extract_text_mentioned_users(message: Message) -> list[tuple[int, str]]:
    """Retorna [(user_id, display_name), ...] de text_mentions em
    entities + caption_entities (audio/voice tem caption_entities).
    Loga entity-a-entity pra debug do music bot bridge."""
    out: list[tuple[int, str]] = []
    seen: set[int] = set()
    sources: list[tuple[str, list]] = []
    if message.entities:
        sources.append(("entities", message.entities))
    if message.caption_entities:
        sources.append(("caption_entities", message.caption_entities))
    for label, entlist in sources:
        for i, ent in enumerate(entlist):
            u = getattr(ent, "user", None)
            uid = getattr(u, "id", None) if u else None
            is_bot = getattr(u, "is_bot", None) if u else None
            logger.info(
                "[MUSIC_BOT] ent[%s#%d] type=%s offset=%s length=%s user_id=%s is_bot=%s",
                label, i, ent.type, ent.offset, ent.length, uid, is_bot,
            )
            if ent.type != "text_mention":
                continue
            if not u or uid is None:
                logger.warning("[MUSIC_BOT] text_mention sem User populado — entity ignorada")
                continue
            if is_bot or uid in seen:
                continue
            seen.add(uid)
            nm = (u.full_name or u.username or f"user{uid}")
            out.append((uid, nm))
    return out


@dp.message(
    F.chat.type.in_({"group", "supergroup"}),
    F.from_user.id == MUSIC_BOT_ID,
)
async def handle_music_bot_post(message: Message):
    """Bot de musica publicou faixa marcando um user → pontua o user
    marcado com 3x XP_PER_MESSAGE e reage ✨ na publicacao."""
    if MUSIC_BOT_ID == 0:
        return
    chat_id = message.chat.id
    sc = message.sender_chat
    logger.info(
        "[MUSIC_BOT] recebido chat=%d mid=%d ctype=%s from_user_id=%s sender_chat_id=%s "
        "has_text=%s has_caption=%s entities=%d caption_entities=%d",
        chat_id, message.message_id, message.content_type,
        (message.from_user.id if message.from_user else None),
        (sc.id if sc else None),
        bool(message.text), bool(message.caption),
        len(message.entities or []), len(message.caption_entities or []),
    )
    try:
        ensure_chat(chat_id, message.chat.title)
    except Exception:
        logger.exception("ensure_chat failed for music bot post chat=%d", chat_id)

    mentioned = _extract_text_mentioned_users(message)
    if not mentioned:
        logger.info("[MUSIC_BOT] nenhum text_mention valido chat=%d mid=%d",
                    chat_id, message.message_id)
        return

    xp_amount = XP_PER_MESSAGE * MUSIC_BOT_XP_MULTIPLIER
    success_count = 0
    for uid, name in mentioned:
        try:
            upsert_user(chat_id, uid, name, None)
            ensure_player(chat_id, uid)
            award_xp_immediate(chat_id, uid, xp_amount, reason="music_mention")
            success_count += 1
            logger.info("music_mention xp+%d uid=%d chat=%d mid=%d",
                        xp_amount, uid, chat_id, message.message_id)
        except Exception:
            logger.exception("music_mention award failed uid=%d chat=%d", uid, chat_id)

    if success_count > 0:
        try:
            await react_to(chat_id, message.message_id, MUSIC_BOT_REACTION)
        except Exception:
            logger.exception("music_mention reaction failed chat=%d mid=%d",
                             chat_id, message.message_id)


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

            # === BAUS REAIS (spawn pendentes + expira abertos) ===
            try:
                cur.execute(
                    "SELECT id, chat_id FROM chests "
                    "WHERE status='pending' AND spawn_at <= ?",
                    (utc_iso(),),
                )
                pending = [dict(r) for r in cur.fetchall()]
                for ch in pending:
                    await spawn_chest(ch["chat_id"], ch["id"])
                await expire_old_chests()
            except Exception:
                logger.exception("chest scheduler tick failed")

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
        BotCommand(command="royalavatar",       description="👑 Escolher avatar"),
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
        BotCommand(command="royaltutorial",     description="📖 Como jogar"),
        BotCommand(command="royalajuda",        description="❓ Ajuda completa"),
    ]
    private_cmds = [
        BotCommand(command="royal",             description="👑 Menu principal"),
        BotCommand(command="royalperfil",       description="📜 Meu perfil"),
        BotCommand(command="royalavatar",       description="👑 Escolher avatar"),
        BotCommand(command="royalficha",        description="📜 Minha ficha"),
        BotCommand(command="royalranking",      description="🏆 Ranking"),
        BotCommand(command="royalinventario",   description="🎒 Inventário"),
        BotCommand(command="royalsaldo",        description="💰 Saldo"),
        BotCommand(command="royalmeuscasorios", description="📊 Meus casórios"),
        BotCommand(command="royalgrupo",        description="🏰 Trocar grupo ativo"),
        BotCommand(command="royaltutorial",     description="📖 Como jogar"),
        BotCommand(command="royalajuda",        description="❓ Ajuda"),
        BotCommand(command="royalprivacidade",  description="🔒 Privacidade"),
        BotCommand(command="royaldados",        description="📦 Meus dados"),
    ]
    admin_cmds = [
        BotCommand(command="royalpalavratest", description="🧪 (admin) Disparar Palavra de teste"),
    ]

    try:
        await bot.set_my_commands(group_cmds, scope=BotCommandScopeAllGroupChats())
        await bot.set_my_commands(private_cmds, scope=BotCommandScopeAllPrivateChats())
        await bot.set_my_commands(admin_cmds, scope=BotCommandScopeAllChatAdministrators())
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
