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

from royal.config import (logger)
from royal.core import (GROUP_ONLY_MSG, ITEMS, bot, cap1024, db, dp, ensure_player, format_br, get_anon_name, is_group, loja_keyboard, react_to, register_owner, safe_typing, term_block)

router = Router()

@router.message(Command("royalloja"))
async def royal_loja(message: Message):
    if not message.from_user or not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    p = ensure_player(message.chat.id, message.from_user.id)
    db.commit()
    await react_to(message.chat.id, message.message_id, "🛒")
    saldo = int(p["gold"] or 0)
    rid = p.get("royal_id") or "RYL-????"
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        await safe_typing(message.chat.id, "upload_photo")
        slots = tuple(
            LojaSlot(
                item_id=iid,
                emoji=item.get("emoji", "📦"),
                name=item.get("name", iid),
                price=int(item.get("price", 0)),
                item_type=item.get("type", "item"),
                affordable=saldo >= int(item.get("price", 0)),
            )
            for iid, item in list(ITEMS.items())[:8]
        )
        data = LojaData(
            viewer_royal_id=rid,
            viewer_name=get_anon_name(message.chat.id, message.from_user.id),
            viewer_avatar_slug=p.get("avatar_slug"),
            saldo=saldo,
            slots=slots,
            owner_uid=int(message.from_user.id),
        )
        png = await asyncio.to_thread(render_loja_card, data)
        if png and bot is not None:
            n_aff = sum(1 for s in slots if s.affordable)
            caption = (f"<b>🛒 LOJA REAL // OPEN_24H</b>\n"
                       f"<i>{len(slots)} itens à venda · "
                       f"{n_aff} que podes pagar · "
                       f"saldo <b>{format_br(saldo)}</b> 🪙</i>")
            sent = await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename="royal_loja.jpg"),
                caption=cap1024(caption),
                parse_mode="HTML",
                reply_markup=loja_keyboard(),
            )
            register_owner(sent, message.from_user.id, auto_delete_secs=60.0)
            return
    except Exception:
        logger.exception("render_loja_card path failed; fallback texto")
    # === Fallback texto ===
    gold_br = f"{saldo:,}".replace(",", ".")
    parts = [f">> SALDO: <b><code>{gold_br}</code></b> florins 🪙\n"]
    for iid, item in ITEMS.items():
        parts.append(
            f"{item['emoji']} <b>{item['name']}</b> "
            f"— <code>{item['price']}</code>🪙\n   <i>{item['desc']}</i>"
        )
    body = "\n\n".join(parts)
    sent = await message.answer(
        term_block("LOJA", body, status="OPEN", status_color="ACID"),
        reply_markup=loja_keyboard())
    register_owner(sent, message.from_user.id, auto_delete_secs=60.0)


