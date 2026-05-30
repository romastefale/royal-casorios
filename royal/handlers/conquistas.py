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

from royal.config import (TZ_NAME, logger)
from royal.core import (ACHIEVEMENTS, bot, cap1024, cur, db, dp, ensure_player, get_anon_name, resolve_dm_chat, safe_typing, term_block, with_close)

router = Router()

@router.message(Command("royalconquistas"))
async def royal_conquistas(message: Message):
    """M11: lista conquistas do user (do chat ativo)."""
    if not message.from_user:
        return
    owner_chat = await resolve_dm_chat(message, action_hint="ver conquistas")
    if owner_chat is None:
        return
    uid = message.from_user.id
    rows = cur.execute(
        "SELECT slug, unlocked_at FROM achievements "
        "WHERE chat_id=? AND user_id=? ORDER BY unlocked_at DESC",
        (owner_chat, uid)).fetchall()
    unlocked = {r["slug"]: r["unlocked_at"] for r in rows}
    total = len(ACHIEVEMENTS)
    got = len(unlocked)
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        p = ensure_player(owner_chat, uid)
        db.commit()
        rid = p.get("royal_id") or "RYL-????"
        items = tuple(
            (re.sub(r"^\W+", "", title).strip(), slug in unlocked)
            for slug, (title, _desc) in ACHIEVEMENTS.items()
        )
        data = ConquistasData(
            self_royal_id=rid,
            self_name=get_anon_name(owner_chat, uid),
            self_avatar_slug=p.get("avatar_slug"),
            owner_uid=int(uid),
            got=got, total=total, items=items,
        )
        await safe_typing(message.chat.id, "upload_photo")
        png = await asyncio.to_thread(render_conquistas_card, data)
        if png and bot is not None:
            caption = (f"<i>Conquistas — <b>{got}/{total}</b> "
                       f"desbloqueadas.</i>")
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename=f"conquistas-{rid}.jpg"),
                caption=cap1024(caption), parse_mode="HTML",
                reply_markup=with_close(None, uid))
            return
    except Exception:
        logger.exception("render_conquistas_card path failed; fallback texto")
    # === Fallback texto ===
    lines = []
    lines.append(f">> <b>{got}/{total}</b> conquistas desbloqueadas")
    lines.append("")
    for slug, (title, desc) in ACHIEVEMENTS.items():
        if slug in unlocked:
            try:
                d = datetime.fromisoformat(unlocked[slug]).astimezone(
                    ZoneInfo(TZ_NAME)).strftime("%d/%m/%y")
            except Exception:
                d = unlocked[slug][:10]
            lines.append(f"✅ <b>{title}</b>")
            lines.append(f"   <i>{desc}</i> · <code>{d}</code>")
        else:
            lines.append(f"🔒 <s>{title}</s>")
            lines.append(f"   <i>{desc}</i>")
    await message.answer(
        term_block("CONQUISTAS", "\n".join(lines),
                   status=f"{got}/{total}", status_color="GOLD"),
        reply_markup=with_close(None, uid))


