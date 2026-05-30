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

from royal.core import (CLASSES, GROUP_ONLY_MSG, auto_delete_after, classe_keyboard, db, dp, ensure_player, is_group, react_to, register_owner, term_block, up_keyboard)

router = Router()

@router.message(Command("royalup"))
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
    sent = await message.answer(
        term_block("ATRIBUTOS", body, status="READY"),
        reply_markup=up_keyboard(),
    )
    register_owner(sent, message.from_user.id, auto_delete_secs=60.0)


@router.message(Command("royalclasse"))
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
    sent = await message.answer(
        term_block("CLASSE", body, status="SELECAO", status_color="CYAN"),
        reply_markup=classe_keyboard())
    register_owner(sent, message.from_user.id, auto_delete_secs=60.0)


