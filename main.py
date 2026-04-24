import asyncio
import html
import os
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DATABASE_PATH", "./data/royal_casorios.sqlite3")
AUTO_HOURS = [9, 15, 21]

bot = Bot(BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher()

os.makedirs("./data", exist_ok=True)

# =========================
# DB INIT
# =========================
db = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = db.cursor()

cur.executescript("""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER,
    chat_id INTEGER,
    display_name TEXT,
    opt_out INTEGER DEFAULT 0,
    message_count INTEGER DEFAULT 0,
    last_seen TEXT,
    PRIMARY KEY(user_id, chat_id)
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
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS votes (
    couple_id INTEGER,
    voter_id INTEGER,
    type TEXT,
    PRIMARY KEY(couple_id, voter_id)
);

CREATE TABLE IF NOT EXISTS chats (
    chat_id INTEGER PRIMARY KEY,
    last_auto_post TEXT
);

CREATE INDEX IF NOT EXISTS idx_pair_scores ON pair_scores(chat_id, score DESC);
""")

db.commit()

# =========================
# CACHE (ESCALA)
# =========================
pair_buffer = defaultdict(int)
user_activity = defaultdict(int)

# =========================
# UTILS
# =========================
def now():
    return datetime.utcnow().isoformat()

def mention(user_id, name):
    safe = html.escape(name)
    return f'<a href="tg://user?id={user_id}">{safe}</a>'

def normalize_pair(u1, u2):
    return tuple(sorted([u1, u2]))

def keyboard(couple_id, likes=0, dislikes=0):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=f"❤️ Apoio {likes}",
            callback_data=f"like:{couple_id}",
            style="primary"
        ),
        InlineKeyboardButton(
            text=f"💔 Ciúmes {dislikes}",
            callback_data=f"dislike:{couple_id}",
            style="danger"
        )
    ]])

# =========================
# TRACKING (RÁPIDO)
# =========================
@dp.message()
async def track(message: Message):
    if not message.from_user or message.chat.type not in ["group", "supergroup"]:
        return

    uid = message.from_user.id
    name = message.from_user.first_name
    chat_id = message.chat.id

    cur.execute("""
    INSERT OR IGNORE INTO users (user_id, chat_id, display_name, last_seen)
    VALUES (?, ?, ?, ?)
    """, (uid, chat_id, name, now()))

    user_activity[(chat_id, uid)] += 1

    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user.id
        if target != uid:
            u1, u2 = normalize_pair(uid, target)
            pair_buffer[(chat_id, u1, u2)] += 6

# =========================
# FLUSH BUFFER (ESCALA)
# =========================
async def flush():
    while True:
        await asyncio.sleep(20)

        for (chat_id, u1, u2), score in list(pair_buffer.items()):
            cur.execute("""
            INSERT INTO pair_scores (chat_id, user1, user2, score, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, user1, user2)
            DO UPDATE SET score = score + ?, last_seen=?
            """, (chat_id, u1, u2, score, now(), score, now()))

        pair_buffer.clear()
        db.commit()

# =========================
# MATCHER
# =========================
def pick_couple(chat_id):
    cur.execute("""
    SELECT user1, user2, score
    FROM pair_scores
    WHERE chat_id=?
    ORDER BY score DESC
    LIMIT 50
    """, (chat_id,))

    rows = cur.fetchall()

    for u1, u2, score in rows:
        if score < 5:
            continue

        cur.execute("""
        SELECT 1 FROM users
        WHERE user_id IN (?, ?) AND opt_out=1 AND chat_id=?
        """, (u1, u2, chat_id))

        if cur.fetchone():
            continue

        return u1, u2

    return None

# =========================
# SEND COUPLE
# =========================
async def send_couple(chat_id):
    pair = pick_couple(chat_id)
    if not pair:
        return

    u1, u2 = pair

    cur.execute("SELECT display_name FROM users WHERE user_id=? AND chat_id=?", (u1, chat_id))
    n1 = cur.fetchone()[0]

    cur.execute("SELECT display_name FROM users WHERE user_id=? AND chat_id=?", (u2, chat_id))
    n2 = cur.fetchone()[0]

    text = (
        "💍👑 <b>Royal Casórios</b>\n\n"
        f"{mention(u1, n1)} ❤️ {mention(u2, n2)}\n\n"
        "✨ O destino do grupo falou...\n"
        "👀 Agora decidam:\n"
    )

    cur.execute("INSERT INTO couples (chat_id, user1, user2, created_at) VALUES (?, ?, ?, ?)",
                (chat_id, u1, u2, now()))
    couple_id = cur.lastrowid
    db.commit()

    await bot.send_message(chat_id, text, reply_markup=keyboard(couple_id))

# =========================
# SCHEDULER (ROBUSTO)
# =========================
async def scheduler():
    while True:
        await asyncio.sleep(30)

        now_dt = datetime.now()
        hour = now_dt.hour

        if hour not in AUTO_HOURS:
            continue

        cur.execute("SELECT chat_id, last_auto_post FROM chats")
        for chat_id, last in cur.fetchall():
            if last:
                last_dt = datetime.fromisoformat(last)
                if (now_dt - last_dt).total_seconds() < 3600:
                    continue

            await send_couple(chat_id)

            cur.execute("UPDATE chats SET last_auto_post=? WHERE chat_id=?", (now(), chat_id))
            db.commit()

# =========================
# COMMANDS
# =========================
@dp.message(Command("querocasar"))
async def querocasar(message: Message):
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    if member.status not in ["administrator", "creator"]:
        await message.answer("🚫 Só admin pode invocar um casório real.")
        return

    await send_couple(message.chat.id)

@dp.message(Command("encalhado"))
async def encalhado(message: Message):
    cur.execute("UPDATE users SET opt_out=1 WHERE user_id=? AND chat_id=?", (message.from_user.id, message.chat.id))
    db.commit()
    await message.answer("🚫💔 Você agora está encalhado...")

@dp.message(Command("desencalhar"))
async def desencalhar(message: Message):
    cur.execute("UPDATE users SET opt_out=0 WHERE user_id=? AND chat_id=?", (message.from_user.id, message.chat.id))
    db.commit()
    await message.answer("🔄💘 Você voltou ao jogo!")

@dp.message(Command("divorcios"))
async def divorcios(message: Message):
    cur.execute("""
    SELECT user1, user2, COUNT(*) as c
    FROM couples
    WHERE chat_id=?
    GROUP BY user1, user2
    ORDER BY c DESC LIMIT 10
    """, (message.chat.id,))
    rows = cur.fetchall()

    txt = "🏆💔 Ranking dos Casórios:\n\n"
    for u1, u2, c in rows:
        txt += f"{u1} ❤️ {u2} — {c}x\n"

    await message.answer(txt)

@dp.message(Command("meusdivorcios"))
async def meus(message: Message):
    cur.execute("""
    SELECT COUNT(*) FROM couples
    WHERE chat_id=? AND (user1=? OR user2=?)
    """, (message.chat.id, message.from_user.id, message.from_user.id))
    c = cur.fetchone()[0]

    await message.answer(f"📊 Você já participou de {c} casórios!")

@dp.message(Command("configcasal"))
async def configcasal(message: Message):
    cur.execute("INSERT OR IGNORE INTO chats (chat_id) VALUES (?)", (message.chat.id,))
    db.commit()

    await message.answer(
        f"⚙️ Configurado!\nChat ID: <code>{message.chat.id}</code>",
        parse_mode="HTML"
    )

@dp.message(Command("help"))
async def help_cmd(message: Message):
    await message.answer(
        "💍👑 <b>Royal Casórios</b>\n\n"
        "/querocasar\n/divorcios\n/meusdivorcios\n/encalhado\n/desencalhar\n/configcasal"
    )

# =========================
# VOTES
# =========================
@dp.callback_query(F.data.startswith(("like", "dislike")))
async def vote(cb: CallbackQuery):
    action, couple_id = cb.data.split(":")
    voter = cb.from_user.id

    try:
        cur.execute("INSERT INTO votes VALUES (?, ?, ?)", (couple_id, voter, action))
        db.commit()
    except:
        await cb.answer("Você já votou")
        return

    cur.execute("SELECT COUNT(*) FROM votes WHERE couple_id=? AND type='like'", (couple_id,))
    likes = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM votes WHERE couple_id=? AND type='dislike'", (couple_id,))
    dislikes = cur.fetchone()[0]

    await cb.message.edit_reply_markup(reply_markup=keyboard(couple_id, likes, dislikes))
    await cb.answer("Registrado")

# =========================
# MAIN
# =========================
async def main():
    asyncio.create_task(flush())
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
