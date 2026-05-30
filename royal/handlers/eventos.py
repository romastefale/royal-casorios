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
from royal.core import (active_seasonal_event, bot, cap1024, current_season_label, dp, safe_typing, term_block, with_close)

router = Router()

@router.message(Command("royalevento"))
async def royal_evento(message: Message):
    """M09: mostra o evento sazonal ativo (boost de XP) ou avisa que nao
    ha nenhum no momento."""
    uid_ev = message.from_user.id if message.from_user else None
    ev = active_seasonal_event()
    active = ev is not None
    if ev:
        pct = int(round((ev["xp_mult"] - 1.0) * 100))
        label = re.sub(r"^\W+", "", ev["label"]).strip()
        eid = str(ev.get("id") or ev["label"])
    else:
        pct = 0
        label = "Sem evento ativo"
        eid = "off"
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        data = EventoData(
            label=label, pct=pct, active=active,
            season_label=current_season_label(), event_id=eid,
        )
        await safe_typing(message.chat.id, "upload_photo")
        png = await asyncio.to_thread(render_evento_card, data)
        if png and bot is not None:
            if active:
                caption = (f"<i>{html.escape(label)} — "
                           f"<b>+{pct}% XP</b> em todo o reino agora.</i>")
            else:
                caption = ("<i>Sem evento agora — fins de semana têm "
                           "<b>+50% XP</b> automático.</i>")
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename="evento.jpg"),
                caption=cap1024(caption), parse_mode="HTML",
                reply_markup=with_close(None, uid_ev))
            return
    except Exception:
        logger.exception("render_evento_card path failed; fallback texto")
    # === Fallback texto ===
    if ev:
        body = (
            f">> {ev['label']}\n"
            f"// boost de <b>+{pct}% XP</b> em TODO o reino agora\n"
            f"<i>vale pra mensagem, PALAVRA, boss, casorio e reactions.</i>"
        )
        await message.answer(term_block(
            "EVENTO", body, status="ON-AIR", status_color="ACID",
            stamp=current_season_label()),
            reply_markup=with_close(None, uid_ev))
    else:
        body = (
            ">> nenhum evento ativo agora\n"
            "// fins de semana tem <b>+50% XP</b> automatico\n"
            "<i>fica de olho em datas especiais do reino.</i>"
        )
        await message.answer(term_block(
            "EVENTO", body, status="OFFLINE", status_color="AMBER",
            stamp=current_season_label()),
            reply_markup=with_close(None, uid_ev))


