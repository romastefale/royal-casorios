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

from royal.core import (USER_PREFS_DEFAULTS, _config_kb, dp, get_user_prefs, set_user_pref, term_block)

router = Router()

@router.message(Command("royalconfig"))
async def royal_config(message: Message):
    """M19: toggles de preferencias do user (persiste em prefs_json)."""
    if not message.from_user:
        return
    uid = message.from_user.id
    prefs = get_user_prefs(uid)
    body = (
        ">> AJUSTES PESSOAIS\n"
        "// toca pra alternar cada flag.\n"
        "<i>preferencias salvas no perfil, valem em todos os reinos.</i>"
    )
    await message.answer(
        term_block("CONFIG", body, status="OPEN", status_color="CYAN"),
        reply_markup=_config_kb(prefs))


@router.callback_query(F.data.startswith("r:cfg:"))
async def cfg_cb(cb: CallbackQuery):
    if not cb.data or not cb.from_user or not cb.message:
        return
    key = cb.data.split(":", 2)[2]
    if key not in USER_PREFS_DEFAULTS:
        await cb.answer()
        return
    prefs = get_user_prefs(cb.from_user.id)
    new_val = not prefs.get(key)
    set_user_pref(cb.from_user.id, key, new_val)
    prefs[key] = new_val
    try:
        await cb.message.edit_reply_markup(reply_markup=_config_kb(prefs))
    except Exception:
        pass
    await cb.answer(f"{key} = {'ON' if new_val else 'OFF'}")


