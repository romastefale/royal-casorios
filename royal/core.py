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

from royal.config import (ADMIN_CACHE_TTL_SECONDS, ATTR_START, AUTO_HOURS, BOSS_ATTACK_COOLDOWN_SEC, BOSS_SPAWN_HOUR, BOSS_SPAWN_WEEKDAY, BOT_TOKEN, CHEST_DELAY_MIN, CHEST_MAX_CLAIMS, CHEST_REWARDS, CHEST_TTL_MIN, COUPLE_XP_BUFF, DAILY_QUESTS, DB_PATH, GH_LOG_TOKEN, GOLD_BOSS_KILL_TOTAL, GOLD_PALAVRA_WIN, LOG_GIST_ID_KEY, LUCKY_DAILY_CAP, LUCKY_EMOJI, LUCKY_JACKPOT_GOLD, LUCKY_JACKPOT_XP, LUCKY_WIN_GOLD, LUCKY_WIN_XP, MIN_PAIR_SCORE, MUSIC_BOT_ID, OWNER_USER_ID, PALAVRA_ATTEMPT_COOLDOWN_SEC, PALAVRA_DURATIONS_MIN, PALAVRA_JITTER_SEC, PALAVRA_NO_REPEAT_RECENT, PHOTO_CACHE_TTL_SECONDS, PTS_PER_LEVEL, REACTION_XP, REACTION_XP_DAILY_CAP, SEASONAL_EVENTS, STARTING_GOLD, STASH_CHAT_ID, TEST_CHAT_IDS, TZ_NAME, XP_BOSS_HIT, XP_COOLDOWN_MSG_SECONDS, XP_COOLDOWN_REPLY_SECONDS, XP_COUPLE_FORMED, XP_PALAVRA_CONSOLATION, XP_PALAVRA_MAX, XP_PALAVRA_MIN, XP_PALAVRA_WIN_BONUS, XP_PER_MESSAGE, XP_PER_REPLY, LOG_BACKUP_DIR, LOG_BACKUP_KEEP, _log_ring, logger)

from royal.alerts import is_benign_telegram_error, register_owner_alerts

bot: Bot | None = (
    Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    if BOT_TOKEN else None)


dp = Dispatcher()


_last_update_monotonic: float = time.monotonic()


@dp.update.outer_middleware()
async def _track_last_update(handler, event, data):
    # roda ANTES de qualquer handler, em TODA update — barato e seguro.
    global _last_update_monotonic
    _last_update_monotonic = time.monotonic()
    return await handler(event, data)


def uid_of(obj) -> int | None:
    """Retorna from_user.id ou None. Use em handlers pra blindar contra
    channel posts/anonymous admins (from_user=None em aiogram).
    Aceita Message, CallbackQuery, InlineQuery, etc."""
    try:
        fu = getattr(obj, "from_user", None)
        return fu.id if fu is not None else None
    except Exception:
        return None


_CAPTION_MAX = 1024


_HTML_TAG_RE = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9-]*)[^>]*>")


def cap1024(text: str | None, suffix: str = "…") -> str:
    """Trunca caption pra <=1024 chars com sufixo, mantendo HTML valido.
    1) Recua se cortou no meio de uma tag (`<b>`, `<code href=...>`).
    2) Detecta tags abertas no fragmento e fecha em ordem reversa
       (ex: `<b><i>texto…` → `<b><i>texto…</i></b>`), evitando
       'Can't find end of the entity starting at byte offset X' do
       Telegram com parse_mode=HTML.
    """
    if not text:
        return text or ""
    if len(text) <= _CAPTION_MAX:
        return text
    body = _CAPTION_MAX - len(suffix)
    cut = text[:body]
    last_lt = cut.rfind("<")
    last_gt = cut.rfind(">")
    if last_lt > last_gt:
        cut = cut[:last_lt]
    # Detecta tags abertas que faltaram fechar no fragmento truncado
    stack: list[str] = []
    for m in _HTML_TAG_RE.finditer(cut):
        is_close, name = m.group(1), m.group(2).lower()
        if is_close:
            if stack and stack[-1] == name:
                stack.pop()
            # mismatch (raro em texto bem-formado): ignora
        else:
            stack.append(name)
    closing = "".join(f"</{t}>" for t in reversed(stack))
    # Garante que tudo cabe em 1024 (overhead = sufixo + tags de fecho)
    overhead = len(suffix) + len(closing)
    if len(cut) + overhead > _CAPTION_MAX:
        cut = cut[: _CAPTION_MAX - overhead]
        last_lt = cut.rfind("<")
        last_gt = cut.rfind(">")
        if last_lt > last_gt:
            cut = cut[:last_lt]
    return cut + suffix + closing


@dp.message.outer_middleware()
async def _require_user_for_commands(handler, message: Message, data):
    """F06 — channel posts e anonymous admins tem from_user=None.
    Sem isso, qualquer comando deles crashava ~30 handlers que
    fazem message.from_user.id sem guard. Dropa silenciosamente
    comandos sem from_user; outras mensagens passam normal.

    Bots NUNCA viram jogadores: admins anonimos chegam como
    @GroupAnonymousBot (is_bot=True), posts de canal como @Channel_Bot.
    Antes, qualquer msg/comando deles cadastrava um perfil de bot na corte.
    Dropa is_bot na entrada — UNICA excecao: o bot de musica (MUSIC_BOT_ID),
    que tem handler proprio creditando os usuarios MENCIONADOS (reais)."""
    try:
        # Allowlist: msgs de SERVICO de migracao grupo→supergrupo chegam com
        # from_user=@GroupAnonymousBot (is_bot=True) quando o upgrade parte de
        # admin anonimo. NAO podem ser dropadas — on_chat_migration precisa
        # delas p/ migrate_chat_data (senao orfana progresso).
        if getattr(message, "migrate_to_chat_id", None) is not None \
                or getattr(message, "migrate_from_chat_id", None) is not None:
            return await handler(message, data)
        fu = message.from_user
        text = getattr(message, "text", None) or ""
        if fu is not None and fu.is_bot:
            # O bot de musica posta faixas (NUNCA comandos) → deixa passar p/
            # handle_music_bot_post, que credita os usuarios MENCIONADOS (reais).
            # Comando vindo de bot — inclusive o de musica — e dropado: bots
            # nunca viram jogadores (nenhum handler de comando deve cadastra-lo).
            if fu.id == MUSIC_BOT_ID and not text.startswith("/"):
                return await handler(message, data)
            return None
        if text.startswith("/") and fu is None:
            return None
    except Exception:
        pass
    return await handler(message, data)


@dp.callback_query.outer_middleware()
async def _block_bot_callbacks(handler, cb: CallbackQuery, data):
    """Admins anonimos pressionando botoes inline chegam como
    @GroupAnonymousBot (is_bot=True). Varios callbacks fazem ensure_player
    → cadastrariam um bot. Nenhum bot legitimo aperta nossos botoes, entao
    dropa is_bot silenciosamente (sem excecoes)."""
    try:
        fu = cb.from_user
        if fu is not None and fu.is_bot:
            return None
    except Exception:
        pass
    return await handler(cb, data)


DB_DIR = os.path.dirname(DB_PATH) or "."


os.makedirs(DB_DIR, exist_ok=True)


def _sqlite_integrity_ok(path: str) -> tuple[bool, str]:
    """F20: PRAGMA integrity_check. Retorna (ok, detail).
    'ok' eh a unica resposta valida pro SQLite. Qualquer outra coisa
    significa corrupcao (pages erradas, indexes batidos, etc)."""
    try:
        conn = sqlite3.connect(path)
        try:
            row = conn.execute("PRAGMA integrity_check").fetchone()
            detail = (row[0] if row else "no-result")
            return (detail == "ok", str(detail))
        finally:
            conn.close()
    except Exception as e:
        return (False, f"open-failed: {type(e).__name__}: {e}")


def _ensure_db_persistence() -> None:
    """
    Garante persistencia do DB entre deploys.

    - Loga path absoluto + tamanho atual do arquivo.
    - Se DB_PATH aponta pra lugar vazio (1a boot apos volume novo no
      Railway) e existe um seed no repo em ./data/royal_casorios.sqlite3,
      valida integridade do seed e copia (one-time bootstrap). Se o seed
      estiver corrompido, cria DB vazio + log fatal.
    - F20: roda integrity_check no DB final pra detectar corrupcao em
      uso (volume com problema, etc). Loga warning, nao crasha.
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
            # F20: valida seed ANTES de copiar pro volume. Se quebrado,
            # nao bota lixo dentro do volume — boota com DB vazio.
            seed_ok, seed_detail = _sqlite_integrity_ok(repo_seed)
            if not seed_ok:
                logger.error(
                    "[DB] !! SEED CORROMPIDO em %s (%s) — pulando copy, "
                    "vou criar DB vazio em %s",
                    repo_seed, seed_detail, target)
            else:
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

    # F20: integrity check do DB em uso. Falha NAO crasha (bot pode
    # responder commands manuais mesmo com corrupcao parcial — melhor
    # ver no log do que tomar restart loop).
    if os.path.exists(target):
        ok, detail = _sqlite_integrity_ok(target)
        if ok:
            logger.info("[DB] integrity_check ok")
        else:
            logger.error("[DB] !! INTEGRITY_CHECK FALHOU: %s", detail)


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


def migrate_to_v5(c: sqlite3.Cursor) -> None:
    """Identity card: file_id do Telegram + hash do (nome, avatar_slug)
    pra detectar quando precisa regenerar. Atualizacao via sweep diario."""
    for col, ddl in (
        ("inline_card_file_id", "ALTER TABLE players ADD COLUMN inline_card_file_id TEXT"),
        ("inline_card_hash",    "ALTER TABLE players ADD COLUMN inline_card_hash TEXT"),
    ):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise


def migrate_to_v7(c: sqlite3.Cursor) -> None:
    """Tabela kv pra flags persistentes (anuncios one-shot, etc)."""
    c.execute(
        "CREATE TABLE IF NOT EXISTS bot_meta ("
        "  key TEXT PRIMARY KEY,"
        "  value TEXT,"
        "  updated_at TEXT"
        ")")


def migrate_to_v8(c: sqlite3.Cursor) -> None:
    """Mute per-chat: /royalmudo (owner) silencia auto-posts no grupo
    durante deploys/manutencao. Comandos manuais continuam respondendo."""
    for col, ddl in (
        ("muted",    "ALTER TABLE chats_rpg ADD COLUMN muted INTEGER DEFAULT 0"),
        ("muted_at", "ALTER TABLE chats_rpg ADD COLUMN muted_at TEXT"),
    ):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise


def migrate_to_v9(c: sqlite3.Cursor) -> None:
    """F09: persiste file_id do profile card no DB (sobrevive restart).
    Hash invalida quando dados mudam (level/xp/gold/hp/...). TTL 24h
    cobre mudancas externas (foto do Telegram trocada etc)."""
    for col, ddl in (
        ("profile_card_file_id",
         "ALTER TABLE players ADD COLUMN profile_card_file_id TEXT"),
        ("profile_card_hash",
         "ALTER TABLE players ADD COLUMN profile_card_hash TEXT"),
        ("profile_card_at",
         "ALTER TABLE players ADD COLUMN profile_card_at TEXT"),
    ):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise


def migrate_to_v10(c: sqlite3.Cursor) -> None:
    """LEGADO/INERTE: colunas do antigo Premium (removido). Mantidas só
    pra nao dropar dados historicos — nenhum codigo le/escreve nelas.
    xp_boost_until, prm_hints, prm_ressurrects, prm_skin_gold."""
    for col, ddl in (
        ("xp_boost_until",
         "ALTER TABLE players ADD COLUMN xp_boost_until TEXT"),
        ("prm_hints",
         "ALTER TABLE players ADD COLUMN prm_hints INTEGER DEFAULT 0"),
        ("prm_ressurrects",
         "ALTER TABLE players ADD COLUMN prm_ressurrects INTEGER DEFAULT 0"),
        ("prm_skin_gold",
         "ALTER TABLE players ADD COLUMN prm_skin_gold INTEGER DEFAULT 0"),
    ):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise
    # LEGADO/INERTE: ledger do antigo Premium (Stars). Tabela mantida pra
    # nao perder historico; nenhum codigo escreve nela apos a remocao.
    c.execute(
        "CREATE TABLE IF NOT EXISTS stars_purchases ("
        "  charge_id TEXT PRIMARY KEY,"
        "  user_id INTEGER NOT NULL,"
        "  chat_id INTEGER,"
        "  item_id TEXT NOT NULL,"
        "  stars INTEGER NOT NULL,"
        "  payload TEXT,"
        "  granted_at TEXT NOT NULL"
        ")")


def migrate_to_v11(c: sqlite3.Cursor) -> None:
    """LEGADO/INERTE: colunas da antiga assinatura Royal Plus (removida).
    royal_plus_until / royal_plus_charge_id mantidas pra nao dropar dados
    historicos — nenhum codigo le/escreve nelas."""
    for col, ddl in (
        ("royal_plus_until",
         "ALTER TABLE players ADD COLUMN royal_plus_until TEXT"),
        ("royal_plus_charge_id",
         "ALTER TABLE players ADD COLUMN royal_plus_charge_id TEXT"),
    ):
        try:
            c.execute(ddl)
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise


def migrate_to_v12(c: sqlite3.Cursor) -> None:
    """Sprint 3/4: achievements (M11), gifts (M02), user prefs (M19).
    - achievements: (chat_id, user_id, slug) com unlocked_at; PK composta
      garante idempotencia (unlock_achievement usa INSERT OR IGNORE).
    - gifts: ledger auditavel de presentes de florins entre players
      (M02 — /royalpresentear).
    - user_dm_settings.prefs_json: blob JSON com flags do user (M19 —
      /royalconfig). Default NULL = todos os defaults."""
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS achievements (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            slug TEXT NOT NULL,
            unlocked_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, user_id, slug)
        );
        CREATE INDEX IF NOT EXISTS idx_ach_user
            ON achievements(user_id);

        CREATE TABLE IF NOT EXISTS gifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            from_user INTEGER NOT NULL,
            to_user INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            sent_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_gifts_from
            ON gifts(from_user, sent_at);
        CREATE INDEX IF NOT EXISTS idx_gifts_to
            ON gifts(to_user, sent_at);
        """
    )
    try:
        c.execute("ALTER TABLE user_dm_settings ADD COLUMN prefs_json TEXT")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            raise


def migrate_to_v13(c: sqlite3.Cursor) -> None:
    """Sprint 4: M05 quests diarias + M06 reactions XP (cap diario).
    - quest_progress: progresso por (chat, user, dia, quest); PK composta
      garante 1 linha por missao/dia. claimed=1 trava double-claim.
    - reaction_xp_daily: contador de reactions premiadas por user/dia
      (cap anti-abuso em REACTION_XP_DAILY_CAP)."""
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS quest_progress (
            chat_id  INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            day      TEXT    NOT NULL,
            quest_id TEXT    NOT NULL,
            progress INTEGER NOT NULL DEFAULT 0,
            claimed  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (chat_id, user_id, day, quest_id)
        );
        CREATE INDEX IF NOT EXISTS idx_quest_day ON quest_progress(day);

        CREATE TABLE IF NOT EXISTS reaction_xp_daily (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            day     TEXT    NOT NULL,
            count   INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (chat_id, user_id, day)
        );
        """
    )


def migrate_to_v14(c: sqlite3.Cursor) -> None:
    """M-luck: Emoji da Sorte. lucky_emoji_daily = cap diário de vitórias
    premiadas no 🎰 por user/chat (anti-farm em LUCKY_DAILY_CAP). Mesma forma
    de reaction_xp_daily (PK composta, 1 linha por user/dia)."""
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS lucky_emoji_daily (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            day     TEXT    NOT NULL,
            count   INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (chat_id, user_id, day)
        );
        """
    )


def migrate_to_v15(c: sqlite3.Cursor) -> None:
    """Inteligência royal (Objetivo 1 — "palavras do dia" da @Mira): banco
    dinâmico de palavras. word_norm é a chave de dedup (normalizada como em
    normalize_word); word_raw guarda o original p/ o card. Convive com a lista
    fixa royal_words.PALAVRAS (pick_palavra_word une as duas e evita repetir as
    recentes)."""
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS palavra_pool (
            word_norm TEXT PRIMARY KEY,
            word_raw  TEXT NOT NULL,
            source    TEXT,
            added_at  TEXT NOT NULL,
            day       TEXT
        );
        """
    )


def migrate_to_v6(c: sqlite3.Cursor) -> None:
    """Randomiza royal_ids existentes (RYL-0001, 0002... -> RYL-NNNN
    sortidos entre 1000-9999). Pool por chat = 9000, retry on collision.
    Invalida inline_card_file_id + inline_card_hash pra forcar regen do
    identity card com o novo ID na proxima sweep/inline."""
    import random as _r
    c.execute("SELECT DISTINCT chat_id FROM players WHERE royal_id IS NOT NULL")
    chats = [r[0] for r in c.fetchall()]
    total = 0
    for chat_id in chats:
        c.execute(
            "SELECT user_id, royal_id FROM players "
            "WHERE chat_id=? AND royal_id IS NOT NULL", (chat_id,))
        players = c.fetchall()
        used: set[str] = set()
        # Sortear sem colisao no mesmo chat.
        for user_id, _old_rid in players:
            for _ in range(200):
                suffix = _r.randint(1000, 9999)
                candidate = f"RYL-{suffix:04d}"
                if candidate not in used:
                    used.add(candidate)
                    break
            else:
                # Pool exausto (>9000 players num chat) — usa 5 digitos.
                while True:
                    suffix = _r.randint(10000, 99999)
                    candidate = f"RYL-{suffix:05d}"
                    if candidate not in used:
                        used.add(candidate)
                        break
            c.execute(
                "UPDATE players SET royal_id=?, inline_card_file_id=NULL, "
                "inline_card_hash=NULL WHERE chat_id=? AND user_id=?",
                (candidate, chat_id, user_id))
            total += 1
    logger.info("migrate_to_v6: randomized %d royal_ids across %d chats",
                total, len(chats))


MIGRATIONS = [
    (1, migrate_to_v1),
    (2, migrate_to_v2),
    (3, migrate_to_v3),
    (4, migrate_to_v4),
    (5, migrate_to_v5),
    (6, migrate_to_v6),
    (7, migrate_to_v7),
    (8, migrate_to_v8),
    (9, migrate_to_v9),
    (10, migrate_to_v10),
    (11, migrate_to_v11),
    (12, migrate_to_v12),
    (13, migrate_to_v13),
    (14, migrate_to_v14),
    (15, migrate_to_v15),
]


def bot_meta_get(key: str) -> str | None:
    try:
        row = cur.execute(
            "SELECT value FROM bot_meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None
    except Exception:
        return None


def bot_meta_set(key: str, value: str) -> None:
    cur.execute(
        "INSERT INTO bot_meta (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
        "updated_at=excluded.updated_at",
        (key, value, utc_iso()))
    db.commit()


def is_chat_muted(chat_id: int) -> bool:
    """True se /royalmudo ON pra esse chat. Falha-segura: False em erro."""
    try:
        row = cur.execute(
            "SELECT muted FROM chats_rpg WHERE chat_id=?", (chat_id,)).fetchone()
        return bool(row and row["muted"])
    except Exception:
        return False


def set_chat_muted(chat_id: int, muted: bool) -> None:
    """Liga/desliga mute do chat. Cria row em chats_rpg se nao existir."""
    cur.execute(
        "INSERT INTO chats_rpg (chat_id, muted, muted_at) VALUES (?, ?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET muted=excluded.muted, "
        "muted_at=excluded.muted_at",
        (chat_id, 1 if muted else 0, utc_iso() if muted else None))
    db.commit()


ACHIEVEMENTS: dict[str, tuple[str, str]] = {
    "primeiro_acerto":   ("🎯 Primeiro Acerto",   "Acertou sua 1ª PALAVRA."),
    "dez_acertos":       ("🏹 Caçador de Palavras", "Acertou 10 PALAVRAS."),
    "cem_acertos":       ("⚡ Mestre das Letras", "Acertou 100 PALAVRAS."),
    "primeiro_boss":     ("🐉 Matador de Titãs",  "Participou de 1 boss kill."),
    "lvl_dez":           ("⭐ Veterano",          "Atingiu nível 10."),
    "lvl_vinte_cinco":   ("🌟 Lendário",          "Atingiu nível 25."),
    "lvl_cinquenta":     ("👑 Imortal",           "Atingiu nível 50."),
    "primeiro_amor":     ("💍 Coração da Corte",  "Primeiro casório no reino."),
    "generoso":          ("🎁 Generoso",          "Presenteou outro jogador."),
}


def unlock_achievement(chat_id: int, user_id: int, slug: str) -> bool:
    """Insere conquista (idempotente). Retorna True se foi NOVA unlock.
    Dispara anuncio no GRUPO (mencionando a pessoa) fire-and-forget na unlock nova."""
    spec = ACHIEVEMENTS.get(slug)
    if not spec:
        return False
    try:
        cur.execute(
            "INSERT OR IGNORE INTO achievements "
            "(chat_id, user_id, slug, unlocked_at) VALUES (?, ?, ?, ?)",
            (chat_id, user_id, slug, utc_iso()))
        is_new = cur.rowcount > 0
        db.commit()
    except Exception:
        logger.exception("unlock_achievement falhou uid=%s slug=%s",
                         user_id, slug)
        return False
    if is_new:
        logger.info("[ACH] UNLOCKED uid=%s chat=%s slug=%s",
                    user_id, chat_id, slug)
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(announce_achievement_group(chat_id, user_id, slug))
        except RuntimeError:
            pass
    return is_new


async def announce_achievement_group(chat_id: int, user_id: int,
                                     slug: str) -> None:
    """Anuncia a conquista no GRUPO, mencionando quem desbloqueou. O ping vem
    do link tg://user?id=… do mention(). Silencioso em erro (não derruba o
    fluxo de XP). Não usa message_effect_id (efeitos só valem em DM)."""
    if bot is None:
        return
    spec = ACHIEVEMENTS.get(slug)
    if not spec:
        return
    title, desc = spec
    try:
        name = get_name(chat_id, user_id)
        body = (
            f">> {mention(user_id, name)} desbloqueou uma conquista! 🏅\n"
            f"// <b>{title}</b>\n"
            f"// <i>{desc}</i>\n"
            f"\n<i>Ver todas em /royalconquistas.</i>"
        )
        await bot.send_message(
            chat_id,
            term_block("CONQUISTA", body, status="UNLOCK",
                       status_color="GOLD"),
        )
    except Exception:
        logger.exception("announce_achievement_group falhou chat=%s uid=%s",
                         chat_id, user_id)


def check_level_achievements(chat_id: int, user_id: int,
                              new_lvl: int) -> None:
    """Dispara unlocks de level (chamado apos levelup)."""
    if new_lvl >= 10:
        unlock_achievement(chat_id, user_id, "lvl_dez")
    if new_lvl >= 25:
        unlock_achievement(chat_id, user_id, "lvl_vinte_cinco")
    if new_lvl >= 50:
        unlock_achievement(chat_id, user_id, "lvl_cinquenta")


def check_palavra_achievements(chat_id: int, user_id: int) -> None:
    """Dispara unlocks de PALAVRA baseado em count historico de wins."""
    try:
        row = cur.execute(
            "SELECT COUNT(*) AS n FROM challenges "
            "WHERE chat_id=? AND status='won' AND winner_user_id=?",
            (chat_id, user_id)).fetchone()
        n = int(row["n"] if row else 0)
    except Exception:
        return
    if n >= 1:
        unlock_achievement(chat_id, user_id, "primeiro_acerto")
    if n >= 10:
        unlock_achievement(chat_id, user_id, "dez_acertos")
    if n >= 100:
        unlock_achievement(chat_id, user_id, "cem_acertos")


USER_PREFS_DEFAULTS: dict[str, bool] = {
    "hide_rank":      False,   # esconder do ranking publico
}


def get_user_prefs(user_id: int) -> dict:
    """Retorna prefs do user merge com defaults. Sempre retorna dict."""
    try:
        row = cur.execute(
            "SELECT prefs_json FROM user_dm_settings WHERE user_id=?",
            (user_id,)).fetchone()
        raw = row["prefs_json"] if row else None
        loaded = json.loads(raw) if raw else {}
    except Exception:
        loaded = {}
    out = dict(USER_PREFS_DEFAULTS)
    if isinstance(loaded, dict):
        for k in USER_PREFS_DEFAULTS:
            if k in loaded:
                out[k] = bool(loaded[k])
    return out


def set_user_pref(user_id: int, key: str, value: bool) -> None:
    """Atualiza 1 pref. Cria row em user_dm_settings se nao existir."""
    if key not in USER_PREFS_DEFAULTS:
        return
    prefs = get_user_prefs(user_id)
    prefs[key] = bool(value)
    payload = json.dumps(prefs, separators=(",", ":"))
    cur.execute(
        "INSERT INTO user_dm_settings (user_id, prefs_json) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET prefs_json=excluded.prefs_json",
        (user_id, payload))
    db.commit()


def run_migrations() -> None:
    # F11: cada migration roda dentro de uma transacao explicita. Se fn()
    # falhar no meio, ROLLBACK desfaz o parcial e o user_version NAO avanca
    # — no proximo boot a mesma migration reroda do zero (idempotente) em
    # vez de deixar o schema num estado intermediario corrompido.
    current = cur.execute("PRAGMA user_version").fetchone()[0]
    for version, fn in MIGRATIONS:
        if current < version:
            logger.info("Applying migration v%d", version)
            try:
                cur.execute("BEGIN")
                fn(cur)
                cur.execute(f"PRAGMA user_version = {version}")
                db.commit()
                logger.info("[MIGRATE] v%d OK", version)
            except Exception:
                db.rollback()
                logger.exception("[MIGRATE] v%d FALHOU — rollback aplicado, "
                                 "abortando boot", version)
                raise
    final = cur.execute("PRAGMA user_version").fetchone()[0]
    logger.info("Database schema at version %d", final)


run_migrations()


pair_buffer: dict[tuple[int, int, int], int] = defaultdict(int)


activity_buffer: dict[tuple[int, int, str], int] = defaultdict(int)


recent_messages: dict[int, deque] = defaultdict(lambda: deque(maxlen=120))


admin_cache: dict[tuple[int, int], tuple[bool, float]] = {}


photo_cache: dict[int, tuple[str | None, float]] = {}


attempt_cooldowns: dict[tuple[int, int], float] = {}


boss_attack_cooldowns: dict[tuple[int, int], float] = {}


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


SEASONS = [
    ("primavera", "🌸 Primavera", (9, 22),  (12, 20)),
    ("verao",     "☀️ Verão",     (12, 21), (3, 19)),
    ("outono",    "🍂 Outono",    (3, 20),  (6, 20)),
    ("inverno",   "❄️ Inverno",   (6, 21),  (9, 21)),
]


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


def active_seasonal_event(d: date | None = None) -> dict | None:
    """Retorna o evento sazonal ativo hoje (ou boost de fim de semana),
    ou None se nao houver. Datas especiais tem prioridade sobre o FDS."""
    if d is None:
        d = local_now().date()
    md = (d.month, d.day)
    for ev in SEASONAL_EVENTS:
        s, e = ev["start"], ev["end"]
        if s <= e:
            if s <= md <= e:
                return ev
        else:  # cruza a virada de ano (ex.: 31/12 -> 01/01)
            if md >= s or md <= e:
                return ev
    if d.weekday() >= 5:  # sabado(5)/domingo(6)
        return {"id": "fds", "label": "🍻 Fim de Semana Real",
                "xp_mult": 1.5}
    return None


def event_xp_mult() -> float:
    """Multiplicador de XP do evento ativo (1.0 se nenhum)."""
    ev = active_seasonal_event()
    return float(ev["xp_mult"]) if ev else 1.0


def quest_bump(chat_id: int, user_id: int, event: str, n: int = 1) -> None:
    """Incrementa o progresso das missoes diarias ligadas a `event`,
    cap no target. No-op se ja claimed (claimed nao volta a contar mas
    tambem nao quebra). Idempotente por (chat,user,dia,quest)."""
    if chat_id >= 0:  # so em grupo
        return
    day = today_key()
    try:
        for q in DAILY_QUESTS:
            if q["event"] != event:
                continue
            cur.execute(
                "INSERT INTO quest_progress "
                "(chat_id, user_id, day, quest_id, progress) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(chat_id, user_id, day, quest_id) "
                "DO UPDATE SET progress=MIN(progress + ?, ?)",
                (chat_id, user_id, day, q["id"], min(n, q["target"]),
                 n, q["target"]))
        db.commit()
    except Exception:
        logger.exception("quest_bump failed chat=%s uid=%s ev=%s",
                         chat_id, user_id, event)


def get_quest_state(chat_id: int, user_id: int) -> list[dict]:
    """Estado das missoes do dia pro user: progress + claimed por quest."""
    day = today_key()
    rows = cur.execute(
        "SELECT quest_id, progress, claimed FROM quest_progress "
        "WHERE chat_id=? AND user_id=? AND day=?",
        (chat_id, user_id, day)).fetchall()
    state = {r["quest_id"]: (r["progress"], r["claimed"]) for r in rows}
    out = []
    for q in DAILY_QUESTS:
        progress, claimed = state.get(q["id"], (0, 0))
        out.append({**q, "progress": progress,
                    "done": progress >= q["target"],
                    "claimed": bool(claimed)})
    return out


def current_week_marker(d: datetime | None = None) -> str:
    """ISO week marker p/ idempotencia do boss semanal."""
    if d is None:
        d = local_now()
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


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


def format_br(n) -> str:
    """Formata inteiro com separador de milhar estilo BR (1234567 -> 1.234.567)."""
    return f"{int(n):,}".replace(",", ".")


EFFECT_FIRE = "5104841245755180586"


EFFECT_THUMBS_UP = "5107584321108051014"


EFFECT_HEART = "5044134455711629726"


EFFECT_PARTY = "5046509860389126442"


EFFECT_POO = "5046589136895476101"


EFFECT_THUMBS_DOWN = "5104858069142078462"


def effect_kw(chat_type: str | None, effect_id: str) -> dict:
    """message_effect_id so funciona em DM 1:1; em grupos retorna dict vazio."""
    return {"message_effect_id": effect_id} if chat_type == "private" else {}


_typing_fail_streak: dict[int, int] = {}


_TYPING_ALERT_THRESHOLD = 10


async def with_retry(coro_factory, *, attempts: int = 3,
                     label: str = "tg"):
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await coro_factory()
        except TelegramRetryAfter as e:
            wait = getattr(e, "retry_after", 2) + 1
            logger.warning("[RETRY] %s flood attempt=%d/%d wait=%ds",
                           label, attempt, attempts, wait)
            await asyncio.sleep(wait)
            last_exc = e
        except (TelegramNetworkError, TelegramServerError) as e:
            wait = min(2 ** attempt, 10)
            logger.warning("[RETRY] %s %s attempt=%d/%d wait=%ds",
                           label, type(e).__name__, attempt, attempts, wait)
            await asyncio.sleep(wait)
            last_exc = e
    if last_exc:
        raise last_exc
    return None  # unreachable: attempts>=1 garante ao menos 1 tentativa


async def safe_typing(chat_id: int, action: str = "typing") -> None:
    """Envia chat action ignorando falhas (nao precisa admin).
    F15: conta falhas consecutivas por chat e loga alerta a cada N
    seguidas (default 10) — sintoma comum eh bot kickado/sem permissao.
    Reset zera ao primeiro sucesso."""
    if bot is None:
        return
    try:
        await bot.send_chat_action(chat_id, action)
        if _typing_fail_streak.pop(chat_id, 0):
            pass  # streak resetado ao sucesso
    except Exception as e:
        streak = _typing_fail_streak.get(chat_id, 0) + 1
        _typing_fail_streak[chat_id] = streak
        if streak % _TYPING_ALERT_THRESHOLD == 0:
            logger.warning(
                "[TYPING] alert chat_id=%s streak=%s action=%s err=%s",
                chat_id, streak, action, type(e).__name__)


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


def close_btn(owner_id: int | None = None) -> InlineKeyboardButton:
    """Botão universal ❌ Fechar. Se owner_id for dado, embute no callback
    (cards SEM owner-lock → só o dono fecha mesmo após restart/TTL); senão
    usa 'r:close' puro (menus já trancados por register_owner → assert_owner)."""
    cd = f"r:close:{owner_id}" if owner_id is not None else "r:close"
    return ikb(f"{BTN_NO} Fechar", style=STYLE_NO, callback_data=cd)


def with_close(kb: InlineKeyboardMarkup | None,
               owner_id: int | None = None) -> InlineKeyboardMarkup:
    """Anexa uma linha [❌ Fechar] a um teclado existente (ou cria um novo
    só com o Fechar). Não muta o teclado original."""
    rows = [list(r) for r in kb.inline_keyboard] if kb else []
    rows.append([close_btn(owner_id)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


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


_msg_owners: TTLCache = TTLCache(maxsize=10_000, ttl=900)


def register_owner(msg: Message | None, uid: int,
                   auto_delete_secs: float = 60.0) -> None:
    """Tranca um menu pro dono uid + agenda auto-delete em N segundos.
    Use ao postar qualquer mensagem com reply_markup que so faca sentido
    pro usuario que disparou o comando (classe/loja/inv/up etc).
    auto_delete_secs=0 desliga o auto-delete (so trava ownership)."""
    if msg is None or uid is None:
        return
    key = (msg.chat.id, msg.message_id)
    _msg_owners[key] = uid
    if auto_delete_secs > 0:
        asyncio.create_task(_auto_delete_owned(key, auto_delete_secs))


async def _auto_delete_owned(key: tuple[int, int], delay: float) -> None:
    try:
        await asyncio.sleep(delay)
        if bot is not None:
            try:
                await bot.delete_message(key[0], key[1])
            except Exception:
                pass
    finally:
        _msg_owners.pop(key, None)


async def assert_owner(cb: CallbackQuery,
                       deny_msg: str = "❌ Esse comando não é seu. "
                                       "Use o seu próprio /royal..."
                       ) -> bool:
    """Retorna True se o cb.from_user.id pode interagir com cb.message.
    Mensagens nao-trancadas (sem entrada em _msg_owners) sao liberadas
    pra todos — assim hub/boss/chest/ship_vote continuam coletivos.
    Em caso de negacao, mostra toast nao-bloqueante e retorna False."""
    if not cb.message or not cb.from_user:
        return True
    key = (cb.message.chat.id, cb.message.message_id)
    owner = _msg_owners.get(key)
    if owner is None or owner == cb.from_user.id:
        return True
    try:
        await cb.answer(deny_msg, show_alert=False)
    except Exception:
        pass
    return False


async def delete_msg_safe(msg: Message | None) -> None:
    """Deleta msg ignorando erros (já apagada, sem permissao, etc).
    Tambem libera a entrada de owner lock."""
    if msg is None or bot is None:
        return
    _msg_owners.pop((msg.chat.id, msg.message_id), None)
    try:
        await bot.delete_message(msg.chat.id, msg.message_id)
    except Exception:
        pass


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
        # Casos esperados (msg deletada / reaction invalida no client) —
        # rebaixa pra DEBUG pra nao poluir o log. Erros reais (rate
        # limit, network, etc) continuam WARNING.
        es = str(e)
        if ("MESSAGE_ID_INVALID" in es
                or "message to react not found" in es.lower()
                or "REACTION_INVALID" in es):
            logger.debug("react_to skip chat=%s mid=%s emoji=%r: %s",
                         chat_id, message_id, emoji, e)
        else:
            logger.warning("react_to falhou chat=%s mid=%s emoji=%r: %s",
                           chat_id, message_id, emoji, e)


async def typewriter_animate(chat_id: int, full_text: str, *,
                             chunk: int = 5, delay: float = 0.4,
                             cursor: str = "▌",
                             final_parse_mode: str | None = None) -> "Message | None":
    """Envia mensagem e edita progressivamente, simulando digitacao
    letra por letra (efeito terminal CRT).

    Telegram tem rate-limit ~1 edit/s em grupos, entao usamos chunks de
    N chars + delay >=0.35s. Durante a anim, parse_mode=None pra evitar
    quebra de tags HTML parciais. Render final pode usar HTML via
    final_parse_mode (aplicado num edit unico no fim).
    """
    if bot is None or not full_text:
        return None
    try:
        msg = await bot.send_message(chat_id, cursor or " ",
                                     parse_mode=None,
                                     disable_notification=True)
    except Exception:
        logger.exception("typewriter: send inicial falhou")
        return None
    text = full_text
    i = 0
    last_render = ""
    while i < len(text):
        i = min(len(text), i + max(1, chunk))
        render = text[:i] + (cursor if i < len(text) else "")
        if render == last_render:
            await asyncio.sleep(delay)
            continue
        try:
            await bot.edit_message_text(
                render, chat_id=chat_id, message_id=msg.message_id,
                parse_mode=None)
            last_render = render
        except Exception as e:
            es = str(e)
            if "MESSAGE_NOT_MODIFIED" in es:
                pass
            elif "Too Many Requests" in es or "retry after" in es.lower():
                await asyncio.sleep(1.5)
                continue
            else:
                logger.debug("typewriter edit fail: %s", e)
                break
        await asyncio.sleep(delay)
    if final_parse_mode:
        try:
            await bot.edit_message_text(
                text, chat_id=chat_id, message_id=msg.message_id,
                parse_mode=final_parse_mode)
        except Exception:
            pass
    return msg


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


_rate_limits: TTLCache = TTLCache(maxsize=20_000, ttl=3600)


_spam_score: TTLCache = TTLCache(maxsize=20_000, ttl=300)  # 5min window


_softban_until: TTLCache = TTLCache(maxsize=20_000, ttl=600)


SPAM_THRESHOLD = 6


SPAM_BAN_SEC = 120


def is_softbanned(uid: int) -> int:
    """Retorna segundos restantes de softban ou 0 se livre."""
    until = _softban_until.get(uid, 0.0)
    now = utc_now().timestamp()
    if until > now:
        return int(until - now) + 1
    return 0


def _bump_spam_score(uid: int) -> tuple[int, bool]:
    """Incrementa score; retorna (novo_score, virou_softban_agora)."""
    score = int(_spam_score.get(uid, 0)) + 1
    _spam_score[uid] = score
    if score >= SPAM_THRESHOLD and not is_softbanned(uid):
        _softban_until[uid] = utc_now().timestamp() + SPAM_BAN_SEC
        logger.warning("[SPAM] softban uid=%d score=%d duration=%ds",
                       uid, score, SPAM_BAN_SEC)
        return score, True
    return score, False


def rate_limited(uid: int, action: str, cooldown: float = 10.0) -> int:
    """Retorna 0 se OK, ou segundos restantes se ainda em cooldown."""
    now = utc_now().timestamp()
    last = _rate_limits.get((uid, action), 0.0)
    if now - last < cooldown:
        return int(cooldown - (now - last)) + 1
    _rate_limits[(uid, action)] = now
    return 0


async def deny_if_rate_limited(message: Message, action: str,
                                cooldown: float = 10.0) -> bool:
    """Helper: responde com aviso e retorna True se rate-limited.
    F08: softban silencioso. Se o user ja estiver banido, ignora a
    mensagem sem responder (evita amplificar spam). Cada hit de
    cooldown soma 1 ponto; ao cruzar SPAM_THRESHOLD, aplica softban
    + envia aviso UNICO com auto-delete."""
    if not message.from_user:
        return False
    uid = message.from_user.id
    if is_softbanned(uid):
        return True  # silent ignore
    wait = rate_limited(uid, action, cooldown)
    if wait > 0:
        _, just_banned = _bump_spam_score(uid)
        if just_banned:
            try:
                ack = await message.answer(term_block(
                    "ANTISPAM",
                    f"🚫 Detectado flood. Bot vai te ignorar por "
                    f"<b>{SPAM_BAN_SEC}s</b>. Relax e respira.",
                    status="BANIDO", status_color="HOT"))
                await auto_delete_after(ack, delay=15.0)
            except Exception:
                pass
        else:
            await message.answer(term_block(
                action, f"⏳ Aguarde <b>{wait}s</b> antes de invocar de novo.",
                status="THROTTLED", status_color="AMBER"))
        return True
    return False


GROUP_ONLY_MSG = (
    "🏰 Esse comando vive nos grupos do Reino.\n"
    "<i>Volta pro grupo Royal pra usar ele lá ✨</i>"
)


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
    # M11: 1º casorio do reino pra cada um dos noivos
    unlock_achievement(chat_id, u1, "primeiro_amor")
    unlock_achievement(chat_id, u2, "primeiro_amor")

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
            datetime.now(ZoneInfo(TZ_NAME)).strftime("%d/%m/%Y %H:%M"))
        if card:
            # Caption tem limite de 1024 chars — text atual cabe folgado.
            await bot.send_photo(
                chat_id,
                photo=BufferedInputFile(card, filename=f"casorio-{couple_id}.jpg"),
                caption=cap1024(text),
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
            chat_id=chat_id, message_id=message_id, caption=cap1024(caption), **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        logger.warning("edit_caption failed: %s", e)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
    except Exception:
        logger.exception("safe_edit_caption failed")


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


def _random_royal_suffix() -> int:
    """Sufixo aleatorio 1000-99999 — pool de 99k por chat (F18: expandido
    de 9k pra 99k pra reduzir colisao em chats >500 players, paradoxo do
    aniversario). Sem sequencia visivel (ROY#0001, 0002...) pra preservar
    privacidade da ordem de cadastro e impedir adivinhacao trivial de
    IDs vizinhos. royal_ids ja emitidos (4 digitos) continuam validos."""
    import random as _r
    return _r.randint(1000, 99999)


def next_royal_id(chat_id: int) -> str:
    """Gera royal_id aleatorio (RYL-NNNN, 1000-9999) com retry on collision
    via UNIQUE INDEX idx_players_chat_royal_id. Mantem royal_id_seq apenas
    como contador de quantos players foram cadastrados (compat historica)."""
    cur.execute(
        "INSERT INTO royal_id_seq (chat_id, next_id) VALUES (?, 1) "
        "ON CONFLICT(chat_id) DO UPDATE SET next_id = next_id + 1",
        (chat_id,),
    )
    for _ in range(80):
        suffix = _random_royal_suffix()
        candidate = f"RYL-{suffix:04d}"
        cur.execute(
            "SELECT 1 FROM players WHERE chat_id=? AND royal_id=?",
            (chat_id, candidate),
        )
        if cur.fetchone() is None:
            return candidate
    # Fallback extremamente improvavel (chat com >>1000 players ativos):
    # expande pool pra 10000-99999 (5 digitos) e tenta de novo.
    for _ in range(40):
        suffix = __import__("random").randint(10000, 99999)
        candidate = f"RYL-{suffix:05d}"
        cur.execute(
            "SELECT 1 FROM players WHERE chat_id=? AND royal_id=?",
            (chat_id, candidate),
        )
        if cur.fetchone() is None:
            return candidate
    raise RuntimeError(f"next_royal_id: pool exhausted for chat {chat_id}")


def get_or_create_player(chat_id: int, user_id: int) -> tuple[dict, bool]:
    """Igual ensure_player mas retorna (row, is_new). Permite que o
    chamador customize feedback pro player recem-cadastrado vs existente."""
    cur.execute("SELECT * FROM players WHERE chat_id=? AND user_id=?",
                (chat_id, user_id))
    row = cur.fetchone()
    if row:
        return dict(row), False
    return ensure_player(chat_id, user_id), True


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
    new_row = dict(cur.fetchone())
    # Player novo — agenda render do identity card em background. O hash
    # ainda eh None, entao ensure_identity_card_async vai gerar/upload.
    schedule_identity_card_refresh(chat_id, user_id)
    return new_row


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


_profile_file_id_cache: TTLCache = TTLCache(maxsize=5_000, ttl=60 * 60 * 6)


def cache_profile_file_id(owner_chat: int, owner_uid: int, file_id: str) -> None:
    _profile_file_id_cache[(owner_chat, owner_uid)] = file_id


def get_cached_profile_file_id(owner_chat: int, owner_uid: int) -> str | None:
    return _profile_file_id_cache.get((owner_chat, owner_uid))


_PROFILE_CARD_TTL_SEC = 24 * 3600


def _profile_card_data_hash(data, photo_fid: str | None = None) -> str:
    """sha1 dos campos visuais do ProfileCardData (mesmo conjunto que
    a cache_key de render_profile_card). Invalida ao primeiro pixel
    diferente. Inclui photo_fid pra invalidar quando o user troca a
    foto do Telegram (paridade com cache_key do renderer que usa
    md5(avatar_bytes))."""
    payload = "|".join(str(x) for x in (
        data.royal_id, data.name, data.class_name, data.season,
        data.level, data.xp_in_level, data.xp_needed,
        data.hp_cur, data.hp_max,
        data.attr_for, data.attr_des, data.attr_vit, data.attr_car,
        data.pts_available, data.rank, data.total_players,
        data.palavras_won, data.casorios, data.gold,
        data.msg_count, data.joined_str,
        data.avatar_slug, photo_fid or "",
    ))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def get_persisted_profile_fid(chat_id: int, user_id: int,
                              want_hash: str) -> str | None:
    """Retorna file_id persistido se hash bate E age <= TTL."""
    try:
        row = cur.execute(
            "SELECT profile_card_file_id AS fid, profile_card_hash AS h, "
            "       profile_card_at AS ts "
            "FROM players WHERE chat_id=? AND user_id=?",
            (chat_id, user_id)).fetchone()
        if not row or not row["fid"] or row["h"] != want_hash:
            return None
        if not row["ts"]:
            return None  # fail-closed: sem timestamp eh estado invalido
        try:
            age = (utc_now() - datetime.fromisoformat(row["ts"])
                   ).total_seconds()
        except Exception:
            return None  # fail-closed: ts malformado/naive => invalida
        if age > _PROFILE_CARD_TTL_SEC:
            return None
        return row["fid"]
    except Exception:
        logger.exception("[F09] get_persisted_profile_fid failed")
        return None


def save_persisted_profile_fid(chat_id: int, user_id: int,
                               hash_: str, file_id: str) -> None:
    try:
        cur.execute(
            "UPDATE players SET profile_card_file_id=?, "
            "profile_card_hash=?, profile_card_at=? "
            "WHERE chat_id=? AND user_id=?",
            (file_id, hash_, utc_now().isoformat(), chat_id, user_id))
        db.commit()
    except Exception:
        logger.exception("[F09] save_persisted_profile_fid failed")


def _identity_card_hash(name: str, avatar_slug: str | None,
                        username: str | None = None,
                        royal_id: str | None = None) -> str:
    """Hash estavel pra detectar mudancas. Sem temporada — virada de season
    SEM mudanca de avatar/handle/royal_id nao deve regenerar."""
    raw = (f"{(name or '').strip()}|{(avatar_slug or '').strip()}|"
           f"{(username or '').strip().lstrip('@').lower()}|"
           f"{(royal_id or '').strip().upper()}")
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _get_identity_card_file_id(chat_id: int, user_id: int) -> str | None:
    cur.execute(
        "SELECT inline_card_file_id FROM players "
        "WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    row = cur.fetchone()
    return row["inline_card_file_id"] if row else None


_identity_card_locks: dict[tuple[int, int], asyncio.Lock] = {}


_IDENTITY_RENDER_SEM = asyncio.Semaphore(3)


def _identity_lock(chat_id: int, user_id: int) -> asyncio.Lock:
    key = (chat_id, user_id)
    lk = _identity_card_locks.get(key)
    if lk is None:
        lk = asyncio.Lock()
        _identity_card_locks[key] = lk
    return lk


async def ensure_identity_card_async(chat_id: int, user_id: int,
                                     force: bool = False) -> str | None:
    """Garante que o identity card do player exista no DB (file_id cacheado).
    Se o hash atual (nome, avatar_slug) for igual ao salvo, no-op. Caso
    contrario re-renderiza, sobe pro STASH_CHAT_ID (silencioso), captura o
    file_id e atualiza o DB. Retorna o file_id atual ou None se falhou."""
    if not bot:
        return None
    async with _identity_lock(chat_id, user_id):
        cur.execute(
            "SELECT p.royal_id, p.avatar_slug, p.inline_card_file_id, "
            "       p.inline_card_hash, u.display_name, u.username "
            "FROM players p "
            "LEFT JOIN users u ON u.chat_id=p.chat_id AND u.user_id=p.user_id "
            "WHERE p.chat_id=? AND p.user_id=?", (chat_id, user_id))
        row = cur.fetchone()
        if not row:
            return None
        raw_name = (row["display_name"] or "").strip()
        avatar_slug = row["avatar_slug"]
        username = (row["username"] or "").strip() or None
        royal_id = row["royal_id"] or "RYL-????"
        # PII guard: nunca renderizar com fallback de user_id numerico.
        # Sem display_name E sem @username → adia (precisa de ao menos 1
        # identificador legivel). Sweep pega depois que upsert_user popular.
        if not raw_name and not username:
            logger.info("identity_card: sem display_name nem @username p/ "
                        "uid=%d chat=%d, adiando (rid=%s)",
                        user_id, chat_id, royal_id)
            return row["inline_card_file_id"]
        name = raw_name or (f"@{username}" if username else "?")
        target_hash = _identity_card_hash(name, avatar_slug, username,
                                          royal_id)
        current_fid = row["inline_card_file_id"]
        current_hash = row["inline_card_hash"]
        if not force and current_fid and current_hash == target_hash:
            return current_fid
        if not STASH_CHAT_ID:
            logger.info("identity_card: STASH_CHAT_ID nao configurado, "
                        "pulando upload (rid=%s)", royal_id)
            return current_fid
        try:
            # F04: cappa renders+uploads concorrentes pra evitar tempest
            # de inline_profile + sweep + ensure_player no mesmo segundo.
            async with _IDENTITY_RENDER_SEM:
                data = IdentityCardData(
                    royal_id=royal_id, name=name, avatar_slug=avatar_slug,
                    username=username)
                card = await asyncio.to_thread(render_identity_card, data)
                if not card:
                    return current_fid
                try:
                    sent = await bot.send_photo(
                        STASH_CHAT_ID,
                        photo=BufferedInputFile(card, filename=f"id-{royal_id}.jpg"),
                        disable_notification=True,
                    )
                except TelegramRetryAfter as e:
                    # #2 flood control no STASH: em vez de cair no except
                    # generico (era logado como ERROR e o refresh era pulado),
                    # espera o retry_after e tenta 1x de novo. Cap de 30s.
                    wait = min(e.retry_after, 30)
                    logger.warning(
                        "identity_card RetryAfter %ds rid=%s — aguardando %ds "
                        "e tentando de novo", e.retry_after, royal_id, wait)
                    await asyncio.sleep(wait + 0.5)
                    sent = await bot.send_photo(
                        STASH_CHAT_ID,
                        photo=BufferedInputFile(card, filename=f"id-{royal_id}.jpg"),
                        disable_notification=True,
                    )
            if not sent or not sent.photo:
                return current_fid
            new_fid = sent.photo[-1].file_id
            cur.execute(
                "UPDATE players SET inline_card_file_id=?, inline_card_hash=? "
                "WHERE chat_id=? AND user_id=?",
                (new_fid, target_hash, chat_id, user_id))
            db.commit()
            logger.info("identity_card refreshed rid=%s uid=%d chat=%d",
                        royal_id, user_id, chat_id)
            return new_fid
        except TelegramRetryAfter as e:
            # 2a falha seguida de flood: transitorio, nao e bug. Warning (nao
            # ERROR) p/ nao disparar alerta — o sweep tenta de novo depois.
            logger.warning("identity_card flood persistente rid=%s (%ds) — "
                           "pulando", royal_id, e.retry_after)
            return current_fid
        except Exception:
            logger.exception("identity_card upload/render failed rid=%s", royal_id)
            return current_fid


def schedule_identity_card_refresh(chat_id: int, user_id: int) -> None:
    """Fire-and-forget pro ensure_identity_card_async. Usa
    get_running_loop — se nao houver loop ativo (ex: chamado de
    contexto sync de teste), loga warning e nao silencia."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning("schedule_identity_card_refresh: sem event loop "
                       "ativo, refresh dropado (chat=%d uid=%d)",
                       chat_id, user_id)
        return
    loop.create_task(ensure_identity_card_async(chat_id, user_id))


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
    # M09: boost de evento sazonal global
    mult = event_xp_mult()
    if mult != 1.0:
        amount = int(amount * mult)
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
        _schedule_levelup_announce(chat_id, user_id, new_lvl)
        check_level_achievements(chat_id, user_id, new_lvl)


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
    # M09: boost de evento sazonal global
    _ev_mult = event_xp_mult()
    if _ev_mult != 1.0:
        real_amount = int(real_amount * _ev_mult)
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
        _schedule_levelup_announce(chat_id, user_id, new_lvl)


async def announce_level_up_group(chat_id: int, user_id: int,
                                  new_lvl: int) -> None:
    """Posta no GRUPO marcando quem subiu de nível. Mensagem leve em texto
    (sem render de card) p/ não pesar nem floodar. O ping vem do link
    tg://user?id=… do mention(), que notifica a pessoa mesmo com nome
    anonimizado no client."""
    if bot is None:
        return
    try:
        name = get_name(chat_id, user_id)
        body = (
            f">> {mention(user_id, name)} subiu para o "
            f"<b>NÍVEL {new_lvl:02d}</b>! 🆙\n"
            f"// novos pontos de atributo te esperam — use "
            f"<code>/royalup</code> pra distribuir."
        )
        await bot.send_message(
            chat_id,
            term_block("LEVEL_UP", body, status=f"NV{new_lvl:02d}",
                       status_color="ACID"),
        )
    except Exception:
        logger.exception("announce_level_up_group falhou chat=%s uid=%s",
                         chat_id, user_id)


def _schedule_levelup_announce(chat_id: int, user_id: int,
                               new_lvl: int) -> None:
    """Agenda o anúncio público de level-up no GRUPO (marca a pessoa). Não
    bloqueia. O bot não manda mais card de level-up na DM — o ping vem do
    link tg://user?id=… do mention() no grupo."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(announce_level_up_group(chat_id, user_id, new_lvl))


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


_AVATAR_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(DB_PATH)) or ".", "avatar_cache")


_AVATAR_CACHE_MAX_BYTES = 512 * 1024  # cap defensivo por arquivo


def _avatar_cache_paths(user_id: int) -> tuple[str, str]:
    base = os.path.join(_AVATAR_CACHE_DIR, str(user_id))
    return base + ".bin", base + ".meta"


async def get_user_photo_bytes(user_id: int) -> bytes | None:
    """Baixa os bytes da foto de perfil pra renderizar dentro de cards.
    F19: cache em disco por user_id, invalidado quando o file_id muda.
    Mora junto do DB (volume persistido no Railway). Custo zero quando
    o avatar nao muda — economia grande no render path do profile card
    (que hoje baixa os bytes a cada /royalperfil)."""
    assert bot is not None
    file_id = await get_user_photo_file_id(user_id)
    if not file_id:
        return None
    bin_path, meta_path = _avatar_cache_paths(user_id)
    try:
        if (os.path.exists(bin_path) and os.path.exists(meta_path)
                and os.path.getsize(bin_path) <= _AVATAR_CACHE_MAX_BYTES):
            with open(meta_path, "r", encoding="utf-8") as f:
                if f.read().strip() == file_id:
                    with open(bin_path, "rb") as bf:
                        return bf.read()
    except Exception:
        logger.warning("[F19] avatar disk cache read fail uid=%d",
                       user_id, exc_info=True)
    try:
        buf = io.BytesIO()
        await bot.download(file_id, destination=buf)
        data = buf.getvalue()
    except Exception:
        logger.warning("download avatar bytes failed uid=%d", user_id,
                       exc_info=True)
        return None
    try:
        os.makedirs(_AVATAR_CACHE_DIR, exist_ok=True)
        if len(data) <= _AVATAR_CACHE_MAX_BYTES:
            # F19: writes atomicos (tmp + os.replace) pra evitar
            # bin truncado + meta valido se o processo morrer durante
            # write — cenario do Railway com SIGKILL em redeploy.
            tmp_bin = bin_path + ".tmp"
            tmp_meta = meta_path + ".tmp"
            with open(tmp_bin, "wb") as bf:
                bf.write(data)
                bf.flush()
                os.fsync(bf.fileno())
            os.replace(tmp_bin, bin_path)
            with open(tmp_meta, "w", encoding="utf-8") as f:
                f.write(file_id)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_meta, meta_path)
    except Exception:
        logger.warning("[F19] avatar disk cache write fail uid=%d",
                       user_id, exc_info=True)
    return data


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


async def send_profile_card(chat_id_to: int, owner_chat: int, owner_uid: int,
                            caption_override: str | None = None,
                            close_uid: int | None = None):
    """Envia cartao de perfil renderizado 1080x1080. Fallback pra texto puro.
    `caption_override` troca a legenda padrao (usado p/ boas-vindas de
    reentrada: foto = ficha, legenda = aviso marcando a pessoa).
    `close_uid` (quando o card é pedido pelo próprio user via /royalperfil
    ou hub) adiciona o botão ❌ Fechar; omitido em boas-vindas de reentrada."""
    assert bot is not None
    ck = with_close(None, close_uid) if close_uid is not None else None
    await safe_typing(chat_id_to, "upload_photo")

    # Tenta render do card primeiro
    try:
        data = build_profile_card_data(owner_chat, owner_uid)
        photo_fid = await get_user_photo_file_id(owner_uid)
        # F09: fast path — se o hash atual bate com file_id persistido
        # (e TTL nao expirou), reenviar por file_id sem re-render.
        data_hash = None
        try:
            data_hash = _profile_card_data_hash(data, photo_fid)
            persisted_fid = get_persisted_profile_fid(
                owner_chat, owner_uid, data_hash)
            if persisted_fid:
                caption = caption_override or build_profile_caption(
                    owner_chat, owner_uid)
                try:
                    await bot.send_photo(
                        chat_id_to,
                        photo=persisted_fid,
                        caption=cap1024(caption),
                        reply_markup=ck)
                    cache_profile_file_id(
                        owner_chat, owner_uid, persisted_fid)
                    return
                except TelegramBadRequest as e:
                    # So invalida se o erro for de file_id invalido /
                    # CDN expirado — outros 400 (caption/entity/chat)
                    # nao devem limpar o cache.
                    msg = str(e).lower()
                    if ("file" in msg and (
                            "invalid" in msg or "not found" in msg
                            or "wrong" in msg or "reuse" in msg)):
                        save_persisted_profile_fid(
                            owner_chat, owner_uid, "", "")
                    else:
                        raise
        except TelegramBadRequest:
            raise
        except Exception:
            logger.exception("[F09] fast-path skip; renderizando do zero")
        avatar_bytes = await get_user_photo_bytes(owner_uid)
        card = await asyncio.to_thread(render_profile_card, data, avatar_bytes)
        if card:
            caption = caption_override or build_profile_caption(
                owner_chat, owner_uid)
            sent = await bot.send_photo(
                chat_id_to,
                photo=BufferedInputFile(card, filename=f"perfil-{data.royal_id}.jpg"),
                caption=cap1024(caption),
                reply_markup=ck,
            )
            # Cacheia file_id (memoria + DB) pra proximos /royalperfil e
            # pra sobreviver ao restart do bot. Hash discrimina por dados
            # + foto atual.
            try:
                if sent and sent.photo:
                    fid = sent.photo[-1].file_id
                    cache_profile_file_id(owner_chat, owner_uid, fid)
                    save_persisted_profile_fid(
                        owner_chat, owner_uid,
                        data_hash or _profile_card_data_hash(data, photo_fid),
                        fid)
            except Exception:
                pass
            return
    except Exception:
        logger.exception("render_profile_card path failed; caindo pro fallback")

    # Fallback: texto puro (com foto bruta se houver)
    text = caption_override or build_profile_text(owner_chat, owner_uid)
    photo_id = await get_user_photo_file_id(owner_uid)
    try:
        if photo_id:
            await bot.send_photo(chat_id_to, photo=photo_id, caption=cap1024(text), parse_mode="HTML", reply_markup=ck)
        else:
            await bot.send_message(chat_id_to, text, reply_markup=ck)
    except TelegramBadRequest as e:
        logger.warning("send_profile_card fallback: %s", e)
        await bot.send_message(chat_id_to, text, reply_markup=ck)
    except Exception:
        logger.exception("send_profile_card failed")
        await bot.send_message(chat_id_to, text, reply_markup=ck)


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


def pool_ingest_words(words, source: str = "mira") -> int:
    """Grava palavras no banco dinâmico (palavra_pool), dedupando por forma
    normalizada e IGNORANDO as que já existem na lista fixa PALAVRAS. Retorna
    quantas linhas NOVAS entraram. Idempotente (ON CONFLICT DO NOTHING) — pedir
    as mesmas palavras de novo é no-op."""
    if not words:
        return 0
    static_norm = {normalize_word(w) for w in PALAVRAS}
    now = utc_iso()
    day = local_now().date().isoformat()
    inserted = 0
    for raw in words:
        raw = (raw or "").strip()
        norm = normalize_word(raw)
        if len(norm) < 3 or norm in static_norm:
            continue
        try:
            cur.execute(
                "INSERT INTO palavra_pool (word_norm, word_raw, source, added_at, day) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(word_norm) DO NOTHING",
                (norm, raw, source, now, day))
            if cur.rowcount == 1:
                inserted += 1
        except Exception:
            logger.exception("pool_ingest_words insert failed word=%r", raw)
    db.commit()
    return inserted


def pick_palavra_word(chat_id: int) -> str:
    """Escolhe a próxima palavra unindo o banco dinâmico (palavra_pool, vindo
    da @Mira) com a lista fixa PALAVRAS e EVITANDO repetir as últimas
    PALAVRA_NO_REPEAT_RECENT palavras usadas NESTE chat. Se o conjunto fresco
    esgotar, libera as antigas (nunca trava). Fallback total = PALAVRAS."""
    try:
        rows = cur.execute(
            "SELECT word FROM challenges WHERE chat_id=? ORDER BY id DESC LIMIT ?",
            (chat_id, PALAVRA_NO_REPEAT_RECENT)).fetchall()
        recent = {(r["word"] or "").strip() for r in rows if r["word"]}
    except Exception:
        recent = set()
    candidates = list(PALAVRAS)
    try:
        prows = cur.execute("SELECT word_raw FROM palavra_pool").fetchall()
        candidates.extend(p["word_raw"] for p in prows if p["word_raw"])
    except Exception:
        pass
    fresh = [w for w in candidates if normalize_word(w) not in recent]
    pool = fresh or candidates
    return random.choice(pool) if pool else random.choice(PALAVRAS)


async def spawn_palavra(chat_id: int) -> None:
    """Cria novo desafio (modo spoiler_img — palavra inteira em imagem com blur do Telegram)."""
    assert bot is not None
    word_raw = pick_palavra_word(chat_id)
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
                caption=cap1024(caption),
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
    # M11: thresholds de PALAVRA (1, 10, 100)
    check_palavra_achievements(chat_id, uid)
    # M05: quest diaria de PALAVRA
    quest_bump(chat_id, uid, "palavra_win")

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
        logger.warning(f"palavra winner feedback falhou: {e}")
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


def _compute_next_palavra_at() -> datetime:
    """Instante do proximo desafio: hora cheia seguinte +/- jitter."""
    now = local_now()
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    jitter = random.randint(-PALAVRA_JITTER_SEC, PALAVRA_JITTER_SEC)
    return next_hour + timedelta(seconds=jitter)


def schedule_next_palavra(chat_id: int) -> None:
    """Agenda proximo desafio: hora cheia seguinte +/- jitter."""
    next_at = _compute_next_palavra_at()
    cur.execute(
        "INSERT INTO chats_rpg (chat_id, next_palavra_at) VALUES (?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET next_palavra_at=excluded.next_palavra_at",
        (chat_id, next_at.isoformat()),
    )
    db.commit()


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
    """Envia mensagem do bau e marca como aberto.

    CLAIM ATOMICO anti-duplicacao (multi-instancia): vira pending->open ANTES
    de enviar; so o vencedor da transicao manda a mensagem. Se 2 processos
    rodarem em overlap (ex: redeploy), o 2o vê status != 'pending' → rowcount
    0 → aborta sem enviar. Em instancia unica, rowcount eh sempre 1 (no-op)."""
    # expires_at JA entra no claim: se o processo morrer entre o claim e o
    # send, o bau fica 'open' COM expires_at → expire_old_chests() o reapa no
    # TTL (sem zumbi permanente). Sem expires_at no claim, um crash deixaria
    # status='open'/expires_at NULL fora do sweep → preso pra sempre.
    expires_at = utc_now() + timedelta(minutes=CHEST_TTL_MIN)
    cur.execute(
        "UPDATE chests SET status='open', spawned_at=?, expires_at=? "
        "WHERE id=? AND status='pending'",
        (utc_iso(), expires_at.isoformat(), chest_id),
    )
    db.commit()
    if cur.rowcount != 1:
        logger.info("chest spawn skip (ja claimado) chat=%d chest=%d",
                    chat_id, chest_id)
        return
    text = format_chest_text(chest_id, status="open")
    msg = await safe_send(chat_id, text, reply_markup=chest_keyboard(chest_id))
    if msg:
        cur.execute(
            "UPDATE chests SET message_id=? WHERE id=?",
            (msg.message_id, chest_id),
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
        if is_chat_muted(chat_id):
            continue  # mute: adia spawn ate desmutar (tick seguinte tenta)
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
    # M05: quest diaria de boss
    quest_bump(chat_id, uid, "boss_hit")

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
    killed_iso = utc_iso()
    cur.execute("UPDATE bosses SET status='dead', killed_at=? WHERE id=?",
                (killed_iso, boss_id))
    # distribui ouro proporcional ao dano — ja ordena por dmg DESC pra
    # alimentar o card sem re-query
    cur.execute(
        "SELECT user_id, SUM(damage) AS dmg FROM boss_hits "
        "WHERE boss_id=? GROUP BY user_id ORDER BY dmg DESC",
        (boss_id,))
    rows = cur.fetchall()
    total_dmg = sum(r["dmg"] for r in rows) or 1
    drops_text: list[str] = []
    total_gold_distributed = 0
    shares: dict[int, int] = {}  # user_id -> gold dado, pro card
    for r in rows:
        share = int(GOLD_BOSS_KILL_TOTAL * r["dmg"] / total_dmg)
        shares[r["user_id"]] = share
        if share > 0:
            cur.execute("UPDATE players SET gold=gold+? WHERE chat_id=? AND user_id=?",
                        (share, chat_id, r["user_id"]))
            total_gold_distributed += share
            name = get_anon_name(chat_id, r["user_id"])
            drops_text.append(f"• {html.escape(name)}: {share}🪙 ({r['dmg']} dano)")
        # M11: 1º boss derrotado pra cada atacante
        unlock_achievement(chat_id, r["user_id"], "primeiro_boss")
    db.commit()

    if drops_text:
        final_msg = (
            f"💀 <b>{boss['name']} foi derrotado!</b>\n\n"
            f"🏆 Recompensas distribuídas:\n" + "\n".join(drops_text[:10])
        )
    else:
        final_msg = (
            f"💀 <b>{boss['name']} foi derrotado!</b>\n\n"
            f"<i>Ninguém causou dano — nenhuma recompensa distribuída.</i>"
        )
    if boss.get("message_id"):
        await safe_edit(chat_id, boss["message_id"], final_msg)
    else:
        await safe_send(chat_id, final_msg)

    # === Card 1080x1080 do boss derrotado (foto separada — celebratoria). ===
    # Edit do msg original eh texto, nao da pra virar foto; mandamos foto nova.
    try:
        # Duracao spawned_at -> killed_at (ambos ISO UTC)
        duration_str = ""
        try:
            spawn_dt = datetime.fromisoformat(boss["spawned_at"])
            kill_dt = datetime.fromisoformat(killed_iso)
            secs = max(0, int((kill_dt - spawn_dt).total_seconds()))
            if secs < 3600:
                duration_str = f"{secs // 60:02d}:{secs % 60:02d}"
            else:
                duration_str = f"{secs // 3600}h {(secs % 3600) // 60:02d}min"
        except Exception:
            duration_str = "??:??"

        top3: list[BossKillAttacker] = []
        for rank, r in enumerate(rows[:3], start=1):
            uid = r["user_id"]
            p = ensure_player(chat_id, uid)
            top3.append(BossKillAttacker(
                rank=rank,
                royal_id=p["royal_id"] or "RYL-????",
                name=get_anon_name(chat_id, uid),
                damage=int(r["dmg"] or 0),
                gold=int(shares.get(uid, 0)),
                avatar_slug=p["avatar_slug"],
            ))
        db.commit()

        data = BossKillData(
            boss_name=boss["name"],
            boss_max_hp=int(boss["max_hp"] or 0),
            total_damage=int(total_dmg),
            total_attackers=len(rows),
            duration_str=duration_str,
            total_gold=int(total_gold_distributed),
            season_label=current_season_label(),
            top3=tuple(top3),
        )
        card = await asyncio.to_thread(render_boss_kill_card, data)
        if card:
            caption = (f"💀 <b>{html.escape(boss['name'])}</b> caiu.\n"
                       f"<i>{len(rows)} caçadores · {duration_str} · "
                       f"{total_gold_distributed}🪙 distribuídos.</i>")
            # Bot API limita caption a 1024 chars
            if len(caption) > 1024:
                caption = caption[:1021] + "..."
            await bot.send_photo(
                chat_id,
                photo=BufferedInputFile(card, filename=f"boss-kill-{boss_id}.jpg"),
                caption=cap1024(caption),
            )
    except Exception:
        logger.exception("finalize_boss: card render/send falhou")

    logger.info("boss killed chat=%d id=%d attackers=%d", chat_id, boss_id, len(rows))


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


ROYAL_TUTORIAL_PARTS: list[tuple[str, str]] = [
    # ---------------------------------------------------------------- 1/6
    ("TUTORIAL 1/6", (
        "<b>// MANUAL DO REINO · COMO JOGAR</b>\n"
        "<i>Bem-vindo, nobre. Este guia te leva do zero ao topo do "
        "ranking. Toca em cada seção pra expandir ▾</i>\n\n"

        "<blockquote expandable>"
        "<b>>> O QUE É O RPG - Royal para Geeks</b>\n"
        "Um RPG que vive <b>dentro do seu grupo do Telegram</b>. "
        "Você não precisa de app nenhum: <b>conversar já é jogar</b>. "
        "Cada mensagem te dá XP, sobe de nível, libera atributos e "
        "te coloca pra disputar o trono do reino.\n"
        "// não existe \"sair do jogo\" — ele roda 24h no grupo."
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> PASSO 1 · ENTRAR NO REINO</b>\n"
        "1. Esteja num grupo onde o Royal está ativo.\n"
        "2. Manda <code>/royal</code> pra abrir o terminal principal "
        "(o HUB com botões de tudo).\n"
        "3. No 1º <code>/royal</code> você é cadastrado e ganha um "
        "<b>RYL ID</b> único (tipo <code>RYL-0042</code>) — sua "
        "identidade pra sempre.\n"
        "<i>// admin do grupo? rode /royalativar uma vez pra ligar o "
        "bot ali.</i>"
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> PASSO 2 · SUA FICHA</b>\n"
        "<code>/royalperfil</code> (ou <code>/royalficha</code>) "
        "gera seu <b>cartão de identidade</b> 1080×1080: avatar, "
        "level, XP, HP, atributos, ranking, casórios e florins 🪙.\n"
        "• <code>/royalavatar</code> — escolhe seu avatar "
        "(1× por temporada).\n"
        "• <code>/royalperfil RYL-0042</code> — espia a ficha de "
        "outro nobre.\n"
        "<i>// funciona no grupo E na sua DM com o bot.</i>"
        "</blockquote>"
    )),

    # ---------------------------------------------------------------- 2/6
    ("TUTORIAL 2/6", (
        "<b>// PROGRESSÃO · XP, NÍVEL E ATRIBUTOS</b>\n\n"

        "<blockquote expandable>"
        "<b>>> COMO GANHAR XP</b>\n"
        f"• <b>+{XP_PER_MESSAGE} XP</b> por mensagem "
        f"(cooldown {XP_COOLDOWN_MSG_SECONDS}s).\n"
        f"• <b>+{XP_PER_REPLY} XP</b> por reply "
        f"(cooldown {XP_COOLDOWN_REPLY_SECONDS}s) — responder vale "
        "mais!\n"
        f"• <b>+{REACTION_XP} XP</b> por reagir com emoji a uma msg "
        f"(até {REACTION_XP_DAILY_CAP}/dia).\n"
        f"• <b>🎰 Emoji da Sorte</b>: mande {LUCKY_EMOJI} no grupo — "
        f"trinca paga <b>+{LUCKY_WIN_XP} XP +{LUCKY_WIN_GOLD}🪙</b> "
        f"(jackpot 7️⃣7️⃣7️⃣: <b>+{LUCKY_JACKPOT_XP} XP "
        f"+{LUCKY_JACKPOT_GOLD}🪙</b>), até {LUCKY_DAILY_CAP}/dia.\n"
        f"• <b>+{XP_PALAVRA_WIN_BONUS} XP</b> ao vencer a Palavra, "
        "XP no boss, em casórios, missões e baús.\n"
        "<i>// o cooldown evita spam: mandar 10 msgs em 5s não "
        "multiplica XP.</i>"
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> SUBIR DE NÍVEL</b>\n"
        f"Cada nível te dá <b>{PTS_PER_LEVEL} pontos</b> de atributo. "
        "Quando tiver pontos, o perfil mostra <b>+N pts</b> e o bot "
        "te avisa.\n"
        "Distribua com <code>/royalup</code>."
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> OS 4 ATRIBUTOS</b>\n"
        "• 💪 <b>FORÇA</b> — mais dano no boss.\n"
        "• 🏃 <b>DESTREZA</b> — mais XP ao acertar a Palavra.\n"
        "• 🛡️ <b>VITALIDADE</b> — mais HP máximo.\n"
        "• ✨ <b>CARISMA</b> — mais chance/peso nos casórios.\n"
        "<i>// não existe build errada, mas foque no que você mais "
        "joga: boss → FORÇA, palavra → DESTREZA, shipper → CARISMA.</i>"
        "</blockquote>"
    )),

    # ---------------------------------------------------------------- 3/6
    ("TUTORIAL 3/6", (
        "<b>// CLASSES · SEU PAPEL NA CORTE</b>\n\n"

        "<blockquote expandable>"
        "<b>>> ESCOLHER CLASSE</b>\n"
        "<code>/royalclasse</code> define teu papel e te dá um "
        "<b>bônus passivo permanente</b>. Escolha com calma: "
        "<i>vale pra temporada inteira.</i>\n\n"
        "• 👑 <b>Monarca</b> — +20% HP base\n"
        "• 🗡️ <b>Cavaleiro</b> — +2 FORÇA\n"
        "• 🌹 <b>Cortesã</b> — +2 CARISMA\n"
        "• 🧙 <b>Bruxo</b> — +2 DESTREZA\n"
        "• 📜 <b>Cronista</b> — +10% XP em tudo\n"
        "• 🗝️ <b>Bobo</b> — +50% ouro 🪙\n"
        "<i>// Cronista acelera level; Bobo enche a arca pra loja.</i>"
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> DICA DE SINERGIA</b>\n"
        "Casado dá <b>+10% XP</b>, Cronista dá <b>+10% XP</b>, e "
        "eventos sazonais empilham por cima. Combinar bônus é o que "
        "separa o top 3 do resto do ranking."
        "</blockquote>"
    )),

    # ---------------------------------------------------------------- 4/6
    ("TUTORIAL 4/6", (
        "<b>// ATIVIDADES · PALAVRA, BOSS E CASÓRIOS</b>\n\n"

        "<blockquote expandable>"
        "<b>>> 🎯 PALAVRA DA HORA</b>\n"
        "De <b>hora em hora</b> o bot solta um desafio no grupo: "
        "anagrama, letras faltando ou charada. Fica aberto por "
        f"<b>{min(PALAVRA_DURATIONS_MIN)}–{max(PALAVRA_DURATIONS_MIN)} "
        "min</b>.\n"
        "• Só <b>responder no chat</b> — o 1º a acertar leva "
        f"<b>{XP_PALAVRA_WIN_BONUS} XP + {GOLD_PALAVRA_WIN} 🪙</b>.\n"
        "• <code>/royalpalavra</code> mostra o desafio ativo.\n"
        f"<i>// {PALAVRA_ATTEMPT_COOLDOWN_SEC}s de cooldown entre "
        "tentativas pra ninguém brutar.</i>"
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> 🐉 BOSS DA SEMANA</b>\n"
        f"Todo <b>domingo às {BOSS_SPAWN_HOUR}h</b> nasce um boss "
        "com HP gigante. <b>O grupo inteiro ataca junto.</b>\n"
        "• <code>/royalboss</code> mostra o HP e dá o golpe.\n"
        "• Seu dano escala com <b>FORÇA</b>.\n"
        "• Derrubou? Todo mundo que bateu leva <b>XP + florins</b> "
        "(quem mais bateu, mais leva).\n"
        "<i>// é cooperativo: chama a galera pro raid.</i>"
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> 🧠 QUIZ REAL</b>\n"
        "Um <b>admin</b> abre um quiz com <code>/rquiz &lt;tema&gt;</code>: "
        "a inteligência royal monta as perguntas e o grupo responde em "
        "<b>enquetes</b>. Quem entrar na rodada e acertar mais leva o "
        "<b>pódio (top-5)</b>.\n"
        "<i>// é por diversão: o quiz não dá XP.</i>"
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> 💍 CASÓRIOS (SHIPPER)</b>\n"
        f"<b>{len(AUTO_HOURS)}× ao dia</b> o bot escolhe um par do "
        "grupo e abre votação ❤️/🤮. Quanto mais vocês interagem, "
        "mais chance de serem shippados.\n"
        "• Casamento ativo dá <b>+10% XP</b> e conta pro ranking.\n"
        "• <code>/royalmeuscasorios</code> — seu histórico.\n"
        "• <code>/royalcasorios</code> — ranking dos casais.\n"
        "• <code>/royalencalhar</code> sai da fila · "
        "<code>/royaldesencalhar</code> volta.\n"
        "<i>// CARISMA pesa na escolha do par.</i>"
        "</blockquote>"
    )),

    # ---------------------------------------------------------------- 5/6
    ("TUTORIAL 5/6", (
        "<b>// ECONOMIA · FLORINS, LOJA E RECOMPENSAS</b>\n\n"

        "<blockquote expandable>"
        "<b>>> 🪙 DE ONDE VÊM OS FLORINS</b>\n"
        "Palavra, boss, missões, eventos e baús. Bobo ganha +50%. "
        "Veja a arca com <code>/royalsaldo</code>."
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> 🛒 LOJA &amp; INVENTÁRIO</b>\n"
        "<code>/royalloja</code> compra · "
        "<code>/royalinventario</code> equipa:\n"
        "• 🧪 Poção de Vigor (50) — restaura HP\n"
        "• 🥾 Botas Ágeis (150) — +2 DES\n"
        "• 🗡️ Espada de Ferro (200) — +3 FOR\n"
        "• 🛡️ Armadura de Couro (200) — +HP\n"
        "• 💍 Anel da Corte (250) — +2 CAR\n"
        "• 📚 Tomo de Sabedoria (300) — +100 XP\n"
        "• 👑 Coroa Decorativa (500) — cosmético\n"
        "<i>// equipáveis dão buff enquanto equipados.</i>"
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> 🎁 PRESENTES &amp; 🏅 CONQUISTAS</b>\n"
        "• <code>/royalpresentear @user 100</code> manda florins "
        "(ou reply + <code>/royalpresentear 100</code>). "
        "Entre 10 e 5000 🪙.\n"  # GIFT_MIN / GIFT_MAX (def. mais abaixo)
        "• <code>/royalconquistas</code> — medalhas que você "
        "desbloqueia jogando (1º acerto, 1º boss, 1º amor...).</blockquote>"

        "<blockquote expandable>"
        "<b>>> 🗺️ MISSÕES &amp; 🎉 EVENTOS</b>\n"
        "• <code>/royalmissoes</code> — 4 missões diárias (mandar "
        "msgs, acertar Palavra, bater no boss, reagir) → resgata "
        "<b>XP + 🪙</b>. Resetam todo dia.\n"
        "• <code>/royalevento</code> — datas especiais e fins de "
        "semana dão <b>boost de XP</b> em tudo."
        "</blockquote>"
    )),

    # ---------------------------------------------------------------- 6/6
    ("TUTORIAL 6/6", (
        "<b>// AVANÇADO · DM, TEMPORADAS E CONFIG</b>\n\n"

        "<blockquote expandable>"
        "<b>>> 💬 DM + INLINE</b>\n"
        "Comandos pessoais (perfil, ficha, ranking, inventário, "
        "saldo, casórios) funcionam na <b>sua DM com o bot</b> "
        "também.\n"
        "• Em mais de um grupo Royal? <code>/royalgrupo</code> "
        "escolhe qual é o ativo na DM.\n"
        "• <b>Inline:</b> digite <code>@nome_do_bot</code> em "
        "qualquer chat pra enviar seu cartão de perfil ✨"
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> ⚙️ PREFERÊNCIAS &amp; 🔒 PRIVACIDADE</b>\n"
        "• <code>/royalconfig</code> — suas preferências "
        "(privacidade no ranking).\n"
        "• <code>/royalprivacidade</code> — seus controles.\n"
        "• <code>/royaldados</code> — exportar ou apagar seus dados."
        "</blockquote>"

        "<blockquote expandable>"
        "<b>>> 🗓️ TEMPORADAS</b>\n"
        "O reino segue as estações do ano. Cada temporada <b>zera o "
        "ranking</b>, mas <b>seu nível total fica</b>. "
        "<code>/royalranking</code> mostra o top 10 atual."
        "</blockquote>"

        "<i>>> fim do manual. agora é com você, nobre. boa caçada ⚔️\n"
        "// dúvida rápida? /royalajuda lista todos os comandos.</i>"
    )),
]


def _tutorial_block(idx: int) -> str:
    """Renderiza a parte `idx` do tutorial como term_block (carimbo da
    temporada só na última parte)."""
    last = len(ROYAL_TUTORIAL_PARTS) - 1
    title, body = ROYAL_TUTORIAL_PARTS[idx]
    return term_block(
        title, body, status="ONBOARDING", status_color="CYAN",
        stamp=current_season_label() if idx == last else None,
    )


def _tutorial_kb(idx: int, owner_id: int) -> InlineKeyboardMarkup:
    """Navegação ◀ Anterior / Próximo ▶ + ❌ Fechar para o tutorial
    in-place (uma única mensagem editável, sem floodar o grupo)."""
    last = len(ROYAL_TUTORIAL_PARTS) - 1
    nav: list[InlineKeyboardButton] = []
    # uid embutido no callback → lock durável (não depende do TTL de _msg_owners,
    # que expira em 15min; menus persistentes precisam disso).
    if idx > 0:
        nav.append(ikb(f"{BTN_BACK} Anterior",
                       callback_data=f"r:tut:{idx - 1}:{owner_id}"))
    if idx < last:
        nav.append(ikb(f"Próximo {BTN_GO}",
                       callback_data=f"r:tut:{idx + 1}:{owner_id}"))
    rows: list[list[InlineKeyboardButton]] = []
    if nav:
        rows.append(nav)
    rows.append([close_btn(owner_id)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_tutorial(message: Message) -> None:
    """Envia o tutorial como UMA mensagem navegável (◀/▶ + ❌ Fechar),
    respeitando o limite de 4096 chars por parte. Usado por /start,
    /royaltutorial, /royalajuda e /help. Antes empilhava 6 mensagens;
    agora edita in-place (regra de UX do dono: não floodar)."""
    uid = message.from_user.id if message.from_user else 0
    sent = await message.answer(
        _tutorial_block(0), reply_markup=_tutorial_kb(0, uid))
    # Trava a navegação pro dono; auto_delete=0 → persiste até Fechar.
    register_owner(sent, uid, auto_delete_secs=0.0)


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
    "/royalloja — comprar itens com florins 🪙</blockquote>\n"
    "<blockquote expandable>🎁 <b>Presentes & Conquistas</b>\n"
    "/royalpresentear @user 100 — manda florins pra outro nobre\n"
    "  <i>(ou: reply na mensagem + /royalpresentear 100)</i>\n"
    "/royalconquistas — vê quais medalhas você já desbloqueou\n"
    "/royalconfig — preferências (privacidade no ranking, etc.)\n"
    "</blockquote>\n"
    "<blockquote expandable>🗺️ <b>Missões & Eventos</b>\n"
    "/royalmissoes — missões diárias (manda msgs, acerta PALAVRA, "
    "bate no boss, reage) → resgata XP + florins\n"
    "/royalevento — vê o boost de XP ativo agora "
    "<i>(datas especiais + fim de semana +50%)</i>\n"
    "<i>💡 Reagir a mensagens (emoji) também dá XP — até 10/dia.</i>\n"
    f"<i>🎰 Mande {LUCKY_EMOJI} no grupo: trinca paga XP + florins "
    f"(jackpot 7️⃣7️⃣7️⃣ em dobro), até {LUCKY_DAILY_CAP}/dia.</i>\n"
    "</blockquote>\n"
    "<blockquote expandable>📜 <b>Pessoal (DM ou grupo)</b>\n"
    "/royalinventario — ver seus itens\n"
    "/royalsaldo — quantos florins você tem 🪙\n"
    "/royalranking — top 10 da temporada\n"
    "/royalmeuscasorios — seu histórico de casais</blockquote>\n"
    "<blockquote expandable>🎯 <b>Eventos (grupo)</b>\n"
    "/royalpalavra — status da Palavra da Hora\n"
    "/royalboss — status do boss da semana\n"
    "/royalcasorios — ranking dos casais\n"
    "/rquiz &lt;tema&gt; — admin: quiz com a inteligência royal "
    "(enquetes; top-5 no fim)</blockquote>\n"
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


AVATAR_MOSAIC_MARKER = "AVATAR.SYS"


def _avatar_kb(owner_id: int) -> InlineKeyboardMarkup:
    """Grid de botões 1..36 (6 por linha) + ❌ Fechar — escolha por toque,
    sem precisar responder com número (regra de UX: mais fácil e limpo)."""
    n = len(royal_avatars.SLUGS)
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    # uid embutido → lock durável (mosaico persiste; TTL de _msg_owners não basta).
    for i in range(1, n + 1):
        row.append(ikb(f"{i:02d}", callback_data=f"r:av:{i}:{owner_id}"))
        if len(row) == 6:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([close_btn(owner_id)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _apply_avatar(owner_chat: int, uid: int, n: int) -> tuple[str, object]:
    """Aplica a escolha de avatar nº `n`. Fonte única usada tanto pelos
    botões (callback) quanto pelo fallback de reply numérico.
    Retorna (status, payload):
      'ok'        -> payload = slug aplicado
      'locked'    -> payload = slug atual (já escolheu nesta temporada)
      'noprofile' -> payload = None
      'badnum'    -> payload = None"""
    slug = royal_avatars.slug_at(n)
    if slug is None:
        return ("badnum", None)
    p = get_player(owner_chat, uid)
    if not p:
        return ("noprofile", None)
    season = current_season_label()
    if p.get("avatar_slug") and p.get("avatar_season") == season:
        return ("locked", p["avatar_slug"])
    cur.execute(
        "UPDATE players SET avatar_slug=?, avatar_season=? "
        "WHERE chat_id=? AND user_id=?",
        (slug, season, owner_chat, uid))
    db.commit()
    return ("ok", slug)


def _avatar_reveal_payload(owner_chat: int, uid: int, slug,
                           n: int) -> tuple[bytes | None, str]:
    """Monta (png, caption) da revelação do avatar escolhido. png=None
    cai no fallback texto."""
    p = get_player(owner_chat, uid) or {}
    royal_id = p.get("royal_id") or ""
    season = current_season_label()
    name = royal_avatars.display_name(slug)
    caption = term_block(
        AVATAR_MOSAIC_MARKER,
        (f"<b>✅ Avatar atualizado: #{n:02d} {name}</b>\n"
         f"<i>Esta escolha vale por toda a temporada {season}.</i>\n"
         f"// Use /royalperfil para ver o novo card."),
        status="OK", status_color="ACID", stamp=royal_id,
    )
    fp = os.path.join(os.path.dirname(__file__),
                      "assets", "avatars", f"{slug}.png")
    png: bytes | None = None
    try:
        with open(fp, "rb") as f:
            png = f.read()
    except Exception:
        png = None
    return png, caption


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


def up_keyboard() -> InlineKeyboardMarkup:
    # FOR -> vermelho (combate), VIT -> verde (vida), DES/CAR -> azul (skill)
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb("💪 FORÇA",    callback_data="r:up:for", style=STYLE_NO),
            ikb("🏃 DESTREZA", callback_data="r:up:des", style=STYLE_INFO),
            ikb("❤️ VITAL",    callback_data="r:up:vit", style=STYLE_OK),
            ikb("✨ CARISMA",  callback_data="r:up:car", style=STYLE_INFO),
        ],
        [close_btn()],
    ])


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
    rows.append([close_btn()])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def inv_keyboard(chat_id: int, uid: int,
                 snapshot: list | None = None) -> InlineKeyboardMarkup:
    """Monta teclado de inventario. Se snapshot fornecido (rows com
    item_id + equipped), usa ele em vez de requery — garante sincronia
    visual com card cacheado no mesmo handler."""
    if snapshot is None:
        cur.execute(
            "SELECT item_id, equipped FROM inventory WHERE chat_id=? AND user_id=? AND qty>0",
            (chat_id, uid))
        rows = cur.fetchall()
    else:
        rows = snapshot
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
    base = buttons or [[
        ikb("🛒 Ir à loja", callback_data="r:loja", style=STYLE_INFO)]]
    base.append([close_btn()])
    return InlineKeyboardMarkup(inline_keyboard=base)


def loja_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for iid, item in ITEMS.items():
        rows.append([ikb(
            f"💳 {item['emoji']} {item['name']} ({item['price']}🪙)",
            callback_data=f"r:buy:{iid}",
            style=STYLE_INFO)])
    rows.append([close_btn()])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_ranking(source_chat_id: int, *, target_chat_id: int,
                       owner_uid: int | None = None) -> None:
    """Envia ranking: card pódio (top 3) + caption com top 10.
    `owner_uid` adiciona o botão ❌ Fechar p/ quem pediu o ranking."""
    ck = with_close(None, owner_uid) if owner_uid is not None else None
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
            stamp=current_season_label()), reply_markup=ck)
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
                caption=cap1024(caption), reply_markup=ck)
            return
    except Exception:
        logger.exception("ranking card send failed; falling back to text")
    await safe_send(target_chat_id, caption, reply_markup=ck)


async def dump_logs_to_owner_dm() -> bool:
    """Envia snapshot do _log_ring como file pro DM do OWNER_USER_ID.
    Falha-segura: retorna False sem crashar se DM nao iniciada ou bot bloqueado."""
    if not OWNER_USER_ID or bot is None:
        return False
    content = _log_ring.snapshot()
    if not content:
        return False
    try:
        ts = datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y%m%d-%H%M%S")
        fname = f"royal-rpg-{ts}.log"
        doc = BufferedInputFile(content.encode("utf-8"), filename=fname)
        await bot.send_document(
            OWNER_USER_ID, doc,
            caption=f"<code>log dump {len(_log_ring.buffer)} linhas</code>",
            disable_notification=True)
        return True
    except Exception:
        logger.exception("[LOGS] dump_logs_to_owner_dm falhou")
        return False


async def dump_logs_to_gist() -> str | None:
    """Cria/atualiza secret gist com snapshot. Retorna html_url ou None.
    Persiste gist_id em bot_meta. No-op se GH_LOG_TOKEN nao setado."""
    if not GH_LOG_TOKEN:
        return None
    content = _log_ring.snapshot()
    if not content:
        return None
    # Cap 500KB pra ficar bem dentro do limite de gist (1MB).
    content = content[-500_000:]
    try:
        import aiohttp
    except Exception:
        logger.warning("[LOGS] aiohttp indisponivel — pulando gist")
        return None
    headers = {
        "Authorization": f"token {GH_LOG_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {
        "description": "RPG Royal para Geeks - log dump (auto, 5min)",
        "public": False,
        "files": {"royal-rpg.log": {"content": content}},
    }
    gist_id = bot_meta_get(LOG_GIST_ID_KEY)
    try:
        async with aiohttp.ClientSession() as sess:
            if gist_id:
                async with sess.patch(
                    f"https://api.github.com/gists/{gist_id}",
                    headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status == 200:
                        return (await r.json()).get("html_url")
                    if r.status == 404:
                        gist_id = None  # apagado externamente, recria abaixo
                    else:
                        logger.warning("[LOGS] gist PATCH %d", r.status)
            if not gist_id:
                async with sess.post(
                    "https://api.github.com/gists",
                    headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status == 201:
                        data = await r.json()
                        bot_meta_set(LOG_GIST_ID_KEY, data["id"])
                        logger.info("[LOGS] gist criado id=%s", data["id"])
                        return data.get("html_url")
                    body = (await r.text())[:200]
                    logger.warning("[LOGS] gist POST %d: %s", r.status, body)
    except Exception:
        logger.exception("[LOGS] dump_logs_to_gist falhou")
    return None


def dump_logs_to_file() -> str | None:
    """Grava snapshot do _log_ring em `<LOG_BACKUP_DIR>/logs/royal-rpg-<ts>.log`
    e aplica rotacao (mantem os LOG_BACKUP_KEEP mais recentes). Retorna o path
    ou None. Substitui o antigo dump na DM do dono (que floodava a cada 5min)."""
    content = _log_ring.snapshot()
    if not content:
        return None
    logs_dir = os.path.join(LOG_BACKUP_DIR, "logs")
    try:
        os.makedirs(logs_dir, exist_ok=True)
        ts = datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y%m%d-%H%M%S")
        path = os.path.join(logs_dir, f"royal-rpg-{ts}.log")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        files = sorted(fn for fn in os.listdir(logs_dir)
                       if fn.startswith("royal-rpg-") and fn.endswith(".log"))
        excess = len(files) - LOG_BACKUP_KEEP
        for fn in files[:max(0, excess)]:
            try:
                os.remove(os.path.join(logs_dir, fn))
            except OSError:
                pass
        return path
    except Exception:
        logger.exception("[LOGS] dump_logs_to_file falhou")
        return None


BACKUP_DIR = os.path.join(DB_DIR, "backups")


BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "7"))


BACKUP_ENABLED = os.getenv("BACKUP_ENABLED", "1") != "0"


BACKUP_HOUR = int(os.getenv("BACKUP_HOUR", "12"))  # meio-dia local


def _do_backup_sync(tag: str = "") -> tuple[str, int]:
    """Executa VACUUM INTO num arquivo datado + prune. Roda em thread.
    `tag` opcional vira sufixo no nome (p/ backups distintos no mesmo dia,
    ex.: double backup de verificacao). Retorna (path, size_bytes).
    Levanta em caso de erro."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = local_now().strftime("%Y-%m-%d")
    suffix = f"_{tag}" if tag else ""
    dest = os.path.join(BACKUP_DIR, f"{stamp}{suffix}.sqlite3")
    # conexao isolada (read) — VACUUM INTO le o estado commitado.
    src = sqlite3.connect(DB_PATH)
    try:
        # integridade antes de gerar backup (nao adianta salvar lixo).
        chk = src.execute("PRAGMA integrity_check").fetchone()
        if not chk or chk[0] != "ok":
            raise RuntimeError(f"integrity_check falhou: {chk}")
        if os.path.exists(dest):
            os.remove(dest)  # idempotente p/ re-run no mesmo dia
        src.execute("VACUUM INTO ?", (dest,))
    finally:
        src.close()
    size = os.path.getsize(dest)
    # retencao: remove backups mais velhos que N dias
    cutoff = (local_now() - timedelta(days=BACKUP_RETENTION_DAYS)).date()
    for fn in os.listdir(BACKUP_DIR):
        if not fn.endswith(".sqlite3"):
            continue
        try:
            d = datetime.strptime(fn[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < cutoff:
            try:
                os.remove(os.path.join(BACKUP_DIR, fn))
            except OSError:
                pass
    return dest, size


async def _notify_owner_backup(dest: str, size: int, label: str) -> None:
    """DM de CONFIRMACAO pro OWNER_USER_ID a cada backup (pedido do dono).
    Failsafe: sem owner/bot ou erro de envio -> loga e segue."""
    if not OWNER_USER_ID or bot is None:
        return
    try:
        files = sorted(fn for fn in os.listdir(BACKUP_DIR)
                       if fn.endswith(".sqlite3"))
    except OSError:
        files = []
    body = "\n".join([
        f">> {label}" if label else ">> backup",
        f">> arquivo: <code>{html.escape(os.path.basename(dest))}</code>",
        f">> tamanho: <b>{size // 1024} KB</b>",
        f">> stash: <b>{'ok' if STASH_CHAT_ID else 'off (sem STASH)'}</b>",
        "// dados salvos: classe, nivel, XP, saldo, casorios, inventario",
        f"// retencao: {len(files)} backups ({BACKUP_RETENTION_DAYS}d)",
    ])
    try:
        await bot.send_message(
            OWNER_USER_ID,
            term_block("BACKUP", body, status="SALVO", status_color="ACID",
                       stamp=local_now().strftime("%d/%m/%Y %H:%M")),
            **effect_kw("private", EFFECT_PARTY))
        logger.info("[BACKUP] confirmacao DM owner ok (%s)", label or "diario")
    except Exception:
        logger.exception("[BACKUP] confirmacao DM owner falhou")


async def run_backup(upload: bool = False, notify_owner: bool = False,
                     label: str = "", tag: str = "") -> tuple[str, int] | None:
    """Roda o backup em thread. Se upload=True e STASH_CHAT_ID setado,
    sobe o arquivo pro canal (silencioso). Se notify_owner=True, manda DM
    de confirmacao pro owner. `tag` -> sufixo no nome do arquivo.
    Retorna (path, size) ou None."""
    try:
        dest, size = await asyncio.to_thread(_do_backup_sync, tag)
    except Exception:
        logger.exception("[BACKUP] falhou")
        return None
    logger.info("[BACKUP] OK %s (%d KB)%s", dest, size // 1024,
                f" [{label}]" if label else "")
    if upload and STASH_CHAT_ID and bot is not None:
        try:
            await bot.send_document(
                STASH_CHAT_ID,
                FSInputFile(dest, filename=os.path.basename(dest)),
                caption=f"// DB backup {os.path.basename(dest)} "
                        f"({size // 1024} KB)" + (f" [{label}]" if label else ""),
                disable_notification=True)
            logger.info("[BACKUP] upload STASH ok")
        except Exception:
            logger.exception("[BACKUP] upload STASH falhou")
    if notify_owner:
        await _notify_owner_backup(dest, size, label)
    return dest, size


_HUB_DEDICATED_PREFIXES = ("r:inv:", "r:priv:", "r:dados:")


def is_group_chat(message: Message) -> bool:
    return message.chat.type in {"group", "supergroup"}


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


_rejoin_welcome_ts: dict[tuple[int, int], float] = {}


REJOIN_DEDUP_SEC = 120.0


_CHAT_MIGRATE_PK_TABLES = (
    "users", "daily_activity", "pair_scores", "chats", "players",
    "royal_id_seq", "inventory", "chats_rpg", "season_hall",
    "achievements", "quest_progress", "reaction_xp_daily",
    "lucky_emoji_daily",
)


_CHAT_MIGRATE_FK_TABLES = (
    "couples", "challenges", "bosses", "chests", "gifts", "stars_purchases",
)


def migrate_chat_data(old_id: int, new_id: int) -> dict[str, int]:
    """Move TODOS os dados de um chat antigo p/ o novo chat_id.

    Transacional (tudo ou nada) e idempotente: se o chat antigo nao tem mais
    dados, e no-op — isso protege contra reprocessamento do update (rodar 2x
    NUNCA apaga os dados ja migrados). Tambem reaponta o "grupo ativo" das DMs
    (`user_dm_settings.active_chat_id`). Sem awaits: roda no event-loop como
    todo o resto do DB (cursor global single-thread).
    """
    if old_id == new_id:
        return {}

    # idempotencia: so age se o id ANTIGO ainda tem algo a migrar (dados em
    # alguma tabela OU ponteiro de "grupo ativo" de DM apontando p/ ele).
    # Rodar 2x e no-op: depois da 1a vez nada resta sob old_id.
    has_old = False
    for t in _CHAT_MIGRATE_PK_TABLES + _CHAT_MIGRATE_FK_TABLES:
        try:
            if cur.execute(
                f"SELECT 1 FROM {t} WHERE chat_id=? LIMIT 1", (old_id,)
            ).fetchone():
                has_old = True
                break
        except Exception:
            continue
    if not has_old:
        has_old = bool(cur.execute(
            "SELECT 1 FROM user_dm_settings WHERE active_chat_id=? LIMIT 1",
            (old_id,)).fetchone())
    if not has_old:
        logger.info("[MIGRATE] chat %d -> %d: nada a migrar (ja migrado/vazio)",
                    old_id, new_id)
        return {}

    moved: dict[str, int] = {}
    try:
        # PK-tables: old vence SO em conflito; linhas novas nao-conflitantes
        # do supergrupo sao preservadas.
        for t in _CHAT_MIGRATE_PK_TABLES:
            n = cur.execute(
                f"SELECT COUNT(*) AS c FROM {t} WHERE chat_id=?", (old_id,)
            ).fetchone()["c"]
            cur.execute(f"UPDATE OR REPLACE {t} SET chat_id=? WHERE chat_id=?",
                        (new_id, old_id))
            if n:
                moved[t] = n
        # FK-tables (PK surrogate): UPDATE direto, sem perda.
        for t in _CHAT_MIGRATE_FK_TABLES:
            n = cur.execute(
                f"SELECT COUNT(*) AS c FROM {t} WHERE chat_id=?", (old_id,)
            ).fetchone()["c"]
            cur.execute(f"UPDATE {t} SET chat_id=? WHERE chat_id=?",
                        (new_id, old_id))
            if n:
                moved[t] = n
        cur.execute(
            "UPDATE user_dm_settings SET active_chat_id=? WHERE active_chat_id=?",
            (new_id, old_id))
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("[MIGRATE] FALHA chat %d -> %d (rollback)", old_id, new_id)
        raise

    bot_meta_set(f"chat_migrated:{old_id}->{new_id}", utc_iso())
    logger.info("[MIGRATE] OK chat %d -> %d moved=%s", old_id, new_id, moved)
    return moved


def flush_buffers_once() -> None:
    """Persiste activity_buffer + pair_buffer no DB num unico commit.
    Reusado pelo loop periodico E pelo shutdown gracioso (F14)."""
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


GIFT_MIN = 10


GIFT_MAX = 5000


def _config_kb(prefs: dict) -> InlineKeyboardMarkup:
    rows = []
    labels = {
        "hide_rank":      "👻 Esconder do ranking publico",
    }
    for key, label in labels.items():
        state = "ON" if prefs.get(key) else "OFF"
        style = STYLE_OK if prefs.get(key) else STYLE_NO
        rows.append([ikb(f"{label} [{state}]",
                         callback_data=f"r:cfg:{key}", style=style)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _quests_render(chat_id: int, uid: int) -> tuple[str, InlineKeyboardMarkup | None]:
    quests = get_quest_state(chat_id, uid)
    lines = []
    claimable = []
    for q in quests:
        bar = progress_bar(q["progress"], q["target"])
        cnt = f"{min(q['progress'], q['target'])}/{q['target']}"
        if q["claimed"]:
            tag = "✅ <s>resgatada</s>"
        elif q["done"]:
            tag = "🎁 <b>pronta!</b>"
            claimable.append(q)
        else:
            tag = ""
        lines.append(
            f"{q['icon']} <b>{html.escape(q['desc'])}</b> {tag}\n"
            f"   <code>{bar}</code> {cnt} · "
            f"+{q['xp']}XP +{q['gold']}🪙")
    body = "\n".join(lines)
    kb = None
    if claimable:
        rows = [[ikb(f"{BTN_OK} Resgatar {q['icon']}",
                     callback_data=f"r:quest:{q['id']}", style=STYLE_OK)]
                for q in claimable]
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
    return body, kb


async def _send_owner_alert_dm(text: str) -> None:
    """Envia a DM de alerta pro dono. Swallow TOTAL: nunca logar erro aqui
    (geraria recursao no OwnerAlertHandler). parse_mode=None p/ o traceback
    (com <, >, &) nao quebrar o parser de HTML."""
    if not OWNER_USER_ID or bot is None:
        return
    try:
        await bot.send_message(OWNER_USER_ID, text, parse_mode=None,
                               disable_notification=False)
    except Exception:
        pass


# Liga o sistema de alerta: anexa o OwnerAlertHandler no root logger. Captura
# nossos logger.exception + as excecoes nao tratadas que o aiogram loga como
# ERROR, classifica e so manda DM nos casos relevantes (ver royal/alerts.py).
register_owner_alerts(_send_owner_alert_dm)


