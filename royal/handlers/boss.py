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

from royal.config import (BOSS_SPAWN_HOUR, logger)
from royal.core import (GROUP_ONLY_MSG, boss_keyboard, bot, cap1024, cur, dp, format_boss_text, format_br, get_active_boss, is_group, react_to, safe_typing, term_block)

router = Router()

@router.message(Command("royalboss"))
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
    if message.from_user:
        await react_to(message.chat.id, message.message_id, "⚔")
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        await safe_typing(message.chat.id, "upload_photo")
        cur.execute(
            "SELECT COUNT(DISTINCT user_id) AS n FROM boss_hits WHERE boss_id=?",
            (boss["id"],))
        att_row = cur.fetchone()
        attackers = int(att_row["n"] if att_row else 0)
        data = BossStatusData(
            boss_id=int(boss["id"]),
            name=str(boss["name"]),
            hp=int(boss["hp"]),
            max_hp=int(boss["max_hp"]),
            attackers=attackers,
            week_marker=str(boss.get("week_marker") or ""),
        )
        png = await asyncio.to_thread(render_boss_status_card, data)
        if png and bot is not None:
            pct = int(100 * data.hp / max(1, data.max_hp))
            caption = (f"<b>⚔️ {html.escape(data.name)}</b>\n"
                       f"<i>HP <b>{format_br(data.hp)}</b>/"
                       f"{format_br(data.max_hp)} "
                       f"({pct}%) · {attackers} atacantes</i>")
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename=f"boss_{boss['id']}.jpg"),
                caption=cap1024(caption),
                parse_mode="HTML",
                reply_markup=boss_keyboard(boss["id"]),
            )
            return
    except Exception:
        logger.exception("render_boss_status_card path failed; fallback texto")
    # === Fallback texto ===
    await message.answer(format_boss_text(boss),
                         reply_markup=boss_keyboard(boss["id"]))


