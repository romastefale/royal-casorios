import asyncio
import html
import logging
import os
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("royal-casorios")

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")

DB_PATH = os.getenv("DATABASE_PATH", "./data/royal_casorios.sqlite3")
TZ_NAME = os.getenv("TZ", "America/Sao_Paulo")
AUTO_HOURS = [int(x.strip()) for x in os.getenv("AUTO_HOURS", "9,15,21").split(",") if x.strip()]
RECENT_WINDOW_SECONDS = 180
FLUSH_INTERVAL_SECONDS = 20
MIN_PAIR_SCORE = 5

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

DB_DIR = os.path.dirname(DB_PATH) or "."
os.makedirs(DB_DIR, exist_ok=True)

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
cur = db.cursor()

cur.executescript(
    """
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA busy_timeout=5000;

    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER,
        chat_id INTEGER,
        display_name TEXT,
        username TEXT,
        opt_out INTEGER DEFAULT 0,
        message_count INTEGER DEFAULT 0,
        last_seen TEXT,
        PRIMARY KEY(user_id, chat_id)
    );

    CREATE TABLE IF NOT EXISTS daily_activity (
        chat_id INTEGER,
        user_id INTEGER,
        day TEXT,
        message_count INTEGER DEFAULT 0,
        PRIMARY KEY(chat_id, user_id, day)
    );

    CREATE TABLE IF NOT EXISTS pair_scores (
        chat_id INTEGER,
        user1 INTEGER,
        user2 INTEGER,
        score INTEGER DEFAULT 0,
        last_seen TEXT,
        PRIMARY KEY(chat_id, user1, user2)
    );

    CREATE TABLE IF NOT EXISTS couples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        user1 INTEGER,
        user2 INTEGER,
        source TEXT DEFAULT 'auto',
        created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS votes (
        couple_id INTEGER,
        voter_id INTEGER,
        type TEXT,
        created_at TEXT,
        PRIMARY KEY(couple_id, voter_id)
    );

    CREATE TABLE IF NOT EXISTS chats (
        chat_id INTEGER PRIMARY KEY,
        title TEXT,
        enabled INTEGER DEFAULT 1,
        last_auto_post TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_users_chat_seen ON users(chat_id, last_seen);
    CREATE INDEX IF NOT EXISTS idx_daily_activity_chat_day ON daily_activity(chat_id, day);
    CREATE INDEX IF NOT EXISTS idx_pair_scores_chat_score ON pair_scores(chat_id, score DESC);
    CREATE INDEX IF NOT EXISTS idx_couples_chat_created ON couples(chat_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_votes_couple ON votes(couple_id);
    """
)
db.commit()

pair_buffer = defaultdict(int)
activity_buffer = defaultdict(int)
recent_messages = defaultdict(lambda: deque(maxlen=120))

private_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Meus casórios")],
        [KeyboardButton(text="🚫 Ficar encalhado"), KeyboardButton(text="💘 Voltar pro jogo")],
        [KeyboardButton(text="❓ Como funciona")],
    ],
    resize_keyboard=True,
)


def utc_now() -> datetime:
    return datetime.utcnow()


def utc_iso() -> str:
    return utc_now().isoformat()


def local_now() -> datetime:
    return datetime.now(ZoneInfo(TZ_NAME))


def today_key() -> str:
    return local_now().date().isoformat()


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


def ensure_chat(chat_id: int, title: str | None = None) -> None:
    """Silently activates and keeps a group registered for automatic casorios."""
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


def keyboard(couple_id: int, likes: int = 0, dislikes: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"❤️ Apoio {likes}",
                    callback_data=f"ship_like:{couple_id}",
                    style="primary",
                ),
                InlineKeyboardButton(
                    text=f"💔 Ciúmes {dislikes}",
                    callback_data=f"ship_dislike:{couple_id}",
                    style="danger",
                ),
            ]
        ]
    )


def get_name(chat_id: int, user_id: int) -> str:
    cur.execute("SELECT display_name FROM users WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    return row["display_name"] if row else str(user_id)


def get_votes(couple_id: int) -> tuple[int, int]:
    cur.execute("SELECT type, COUNT(*) AS total FROM votes WHERE couple_id=? GROUP BY type", (couple_id,))
    totals = {row["type"]: row["total"] for row in cur.fetchall()}
    return totals.get("like", 0), totals.get("dislike", 0)


def pair_recently_used(chat_id: int, u1: int, u2: int) -> bool:
    cutoff = (utc_now() - timedelta(hours=72)).isoformat()
    cur.execute(
        """
        SELECT 1 FROM couples
        WHERE chat_id=? AND user1=? AND user2=? AND created_at>=?
        LIMIT 1
        """,
        (chat_id, u1, u2, cutoff),
    )
    return cur.fetchone() is not None


def user_is_available(chat_id: int, user_id: int) -> bool:
    cutoff = (utc_now() - timedelta(hours=48)).isoformat()
    cur.execute(
        """
        SELECT 1 FROM users
        WHERE chat_id=? AND user_id=? AND opt_out=0 AND last_seen>=?
        LIMIT 1
        """,
        (chat_id, user_id, cutoff),
    )
    return cur.fetchone() is not None


def pick_couple(chat_id: int) -> tuple[int, int] | None:
    cur.execute(
        """
        SELECT ps.user1, ps.user2, ps.score,
               COALESCE(SUM(CASE WHEN v.type='like' THEN 1 ELSE 0 END), 0) AS likes,
               COALESCE(SUM(CASE WHEN v.type='dislike' THEN 1 ELSE 0 END), 0) AS dislikes
        FROM pair_scores ps
        LEFT JOIN couples c ON c.chat_id=ps.chat_id AND c.user1=ps.user1 AND c.user2=ps.user2
        LEFT JOIN votes v ON v.couple_id=c.id
        WHERE ps.chat_id=?
        GROUP BY ps.user1, ps.user2, ps.score
        ORDER BY (ps.score + likes * 2 - dislikes) DESC
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
    pair = pick_couple(chat_id)
    if not pair:
        if source == "manual":
            await bot.send_message(
                chat_id,
                "💍👑 <b>Royal Casórios</b>\n\n😶 Ainda não existe interação suficiente para formar um casal real.\n🔥 Respondam mensagens, conversem e tentem de novo em alguns minutos!",
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
    db.commit()

    text = (
        "💍👑 <b>Royal Casórios</b>\n\n"
        "✨ O destino do grupo acabou de agir...\n"
        f"{mention(u1, n1)} ❤️ {mention(u2, n2)}\n\n"
        "🔥 O algoritmo viu movimentação entre vocês.\n"
        "👀 Agora o grupo decide:"
    )
    await bot.send_message(chat_id, text, reply_markup=keyboard(couple_id))
    return True


async def is_admin(message: Message) -> bool:
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    return member.status in {"administrator", "creator"}


@dp.message(Command("start"))
async def start(message: Message):
    if message.chat.type != "private":
        return
    await message.answer(
        "💍👑 <b>Royal Casórios</b>\n\n"
        "Bem-vindo ao painel dos casórios! 😏\n\n"
        "💘 Aqui você pode ver seu histórico, sair do sistema ou entender como tudo funciona.\n\n"
        "👇 Use os botões abaixo:",
        reply_markup=private_menu,
    )


@dp.message(Command("help"))
async def help_cmd(message: Message):
    await message.answer(
        "💍👑 <b>Royal Casórios</b>\n\n"
        "📌 <b>Comandos do grupo</b>\n"
        "/querocasar - 💍 gerar casal agora, apenas admin\n"
        "/divorcios - 🏆 ranking dos casórios\n"
        "/meusdivorcios - 📊 seu histórico\n"
        "/encalhado - 🚫 sair do sistema\n"
        "/desencalhar - 💘 voltar ao sistema\n"
        "/noivado - 💍 ativar ou confirmar o grupo\n\n"
        "🔥 Nos posts, vote com ❤️ Apoio ou 💔 Ciúmes."
    )


@dp.message(Command("noivado"))
async def noivado(message: Message):
    if not is_group(message):
        await message.answer("💍 Esse comando deve ser usado dentro do grupo.")
        return
    ensure_chat(message.chat.id, message.chat.title)
    db.commit()
    await message.answer(
        "💍👑 <b>Noivado confirmado!</b>\n\n"
        f"👤 User ID: <code>{message.from_user.id}</code>\n"
        f"💬 Chat ID: <code>{message.chat.id}</code>\n\n"
        "✨ O Royal Casórios agora está ativo neste grupo.\n"
        "🔥 Os casórios acontecerão automaticamente 3x ao dia."
    )


@dp.message(Command("querocasar"))
async def querocasar(message: Message):
    if not is_group(message):
        await message.answer("💍 Use esse comando dentro do grupo.")
        return
    if not await is_admin(message):
        await message.answer("🚫 Só admin pode invocar um casório real agora.")
        return
    ensure_chat(message.chat.id, message.chat.title)
    db.commit()
    await send_couple(message.chat.id, source="manual")


@dp.message(Command("encalhado"))
async def encalhado(message: Message):
    if not message.from_user:
        return
    chat_id = message.chat.id
    upsert_user(chat_id, message.from_user.id, display_name(message), message.from_user.username)
    cur.execute("UPDATE users SET opt_out=1 WHERE chat_id=? AND user_id=?", (chat_id, message.from_user.id))
    db.commit()
    await message.answer("🚫💔 Você entrou no modo encalhado(a). O Royal Casórios não vai mais te colocar em casórios.")


@dp.message(Command("desencalhar"))
async def desencalhar(message: Message):
    if not message.from_user:
        return
    chat_id = message.chat.id
    upsert_user(chat_id, message.from_user.id, display_name(message), message.from_user.username)
    cur.execute("UPDATE users SET opt_out=0 WHERE chat_id=? AND user_id=?", (chat_id, message.from_user.id))
    db.commit()
    await message.answer("🔄💘 Você voltou para o jogo dos casórios!")


@dp.message(Command("meusdivorcios"))
async def meusdivorcios(message: Message):
    uid = message.from_user.id
    chat_id = message.chat.id
    cur.execute("SELECT COUNT(*) AS total FROM couples WHERE chat_id=? AND (user1=? OR user2=?)", (chat_id, uid, uid))
    total = cur.fetchone()["total"]
    cur.execute(
        """
        SELECT CASE WHEN user1=? THEN user2 ELSE user1 END AS partner, COUNT(*) AS total
        FROM couples
        WHERE chat_id=? AND (user1=? OR user2=?)
        GROUP BY partner
        ORDER BY total DESC
        LIMIT 5
        """,
        (uid, chat_id, uid, uid),
    )
    rows = cur.fetchall()
    text = f"📊💔 <b>Seus casórios</b>\n\nVocê já participou de <b>{total}</b> casórios! 😳\n"
    if rows:
        text += "\n🔥 <b>Top pares:</b>\n"
        for row in rows:
            text += f"• {html.escape(get_name(chat_id, row['partner']))} — {row['total']}x\n"
    await message.answer(text)


@dp.message(Command("divorcios"))
async def divorcios(message: Message):
    chat_id = message.chat.id
    cur.execute(
        """
        SELECT user1, user2, COUNT(*) AS total
        FROM couples
        WHERE chat_id=?
        GROUP BY user1, user2
        ORDER BY total DESC
        LIMIT 10
        """,
        (chat_id,),
    )
    rows = cur.fetchall()
    if not rows:
        await message.answer("🏆💔 Ainda não existem casórios suficientes para ranking.")
        return
    text = "🏆💔 <b>Ranking dos Casórios</b>\n\n"
    for i, row in enumerate(rows, start=1):
        text += f"{i}. {html.escape(get_name(chat_id, row['user1']))} ❤️ {html.escape(get_name(chat_id, row['user2']))} — {row['total']}x\n"
    await message.answer(text)


@dp.message(F.text == "📊 Meus casórios")
async def btn_meus(message: Message):
    await meusdivorcios(message)


@dp.message(F.text == "🚫 Ficar encalhado")
async def btn_encalhado(message: Message):
    await encalhado(message)


@dp.message(F.text == "💘 Voltar pro jogo")
async def btn_desencalhar(message: Message):
    await desencalhar(message)


@dp.message(F.text == "❓ Como funciona")
async def btn_como_funciona(message: Message):
    await message.answer(
        "💡👑 <b>Como funciona o Royal Casórios</b>\n\n"
        "👀 O bot analisa interações no grupo:\n"
        "• respostas entre membros\n"
        "• proximidade de conversa\n"
        "• atividade recente\n\n"
        "💍 Ele forma casórios automaticamente 3x ao dia.\n"
        "🔥 O grupo vota com ❤️ Apoio ou 💔 Ciúmes.\n"
        "🚫 Você pode sair quando quiser usando /encalhado."
    )


@dp.callback_query(F.data.startswith(("ship_like:", "ship_dislike:")))
async def vote(cb: CallbackQuery):
    action, couple_id_raw = cb.data.split(":", 1)
    couple_id = int(couple_id_raw)
    vote_type = "like" if action == "ship_like" else "dislike"
    try:
        cur.execute(
            "INSERT INTO votes (couple_id, voter_id, type, created_at) VALUES (?, ?, ?, ?)",
            (couple_id, cb.from_user.id, vote_type, utc_iso()),
        )
        db.commit()
    except sqlite3.IntegrityError:
        await cb.answer("Você já votou nesse casório 😶")
        return
    likes, dislikes = get_votes(couple_id)
    await cb.message.edit_reply_markup(reply_markup=keyboard(couple_id, likes, dislikes))
    await cb.answer("Voto registrado 👑")


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
    db.commit()


async def flush_buffers():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        try:
            for (chat_id, uid, day), count in list(activity_buffer.items()):
                cur.execute(
                    """
                    INSERT INTO daily_activity (chat_id, user_id, day, message_count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(chat_id, user_id, day)
                    DO UPDATE SET message_count=message_count + excluded.message_count
                    """,
                    (chat_id, uid, day, count),
                )
            activity_buffer.clear()

            for (chat_id, u1, u2), score in list(pair_buffer.items()):
                cur.execute(
                    """
                    INSERT INTO pair_scores (chat_id, user1, user2, score, last_seen)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(chat_id, user1, user2)
                    DO UPDATE SET score=score + excluded.score, last_seen=excluded.last_seen
                    """,
                    (chat_id, u1, u2, score, utc_iso()),
                )
            pair_buffer.clear()
            db.commit()
        except Exception:
            logger.exception("buffer flush failed")


async def scheduler():
    while True:
        await asyncio.sleep(30)
        try:
            current = local_now()
            if current.hour not in AUTO_HOURS:
                continue
            today_marker = current.strftime("%Y-%m-%d-%H")
            cur.execute("SELECT chat_id, last_auto_post FROM chats WHERE enabled=1")
            for row in cur.fetchall():
                if row["last_auto_post"] == today_marker:
                    continue
                ok = await send_couple(row["chat_id"], source="auto")
                if ok:
                    cur.execute("UPDATE chats SET last_auto_post=? WHERE chat_id=?", (today_marker, row["chat_id"]))
                    db.commit()
        except Exception:
            logger.exception("scheduler failed")


async def main():
    logger.info("Royal Casorios starting")
    asyncio.create_task(flush_buffers())
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
