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

from royal.core import (db, display_name, dp, ensure_chat, ensure_player, get_player_by_royal_id, is_group, react_to, refresh_user_identity, resolve_owner_chat, safe_typing, send_profile_card)

router = Router()

@router.message(Command("royalperfil", "royalficha"))
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
        await send_profile_card(message.chat.id, p["chat_id"], p["user_id"],
                                close_uid=message.from_user.id)
        return

    # Sem argumento: perfil proprio
    if is_group(message):
        ensure_chat(message.chat.id, message.chat.title)
        # Garante users.display_name fresco — evita fallback p/ str(user_id)
        # que dispararia anonize() no proprio dono e mostraria "ANON-X"
        refresh_user_identity(message.chat.id, message.from_user.id, live_name, live_username)
        ensure_player(message.chat.id, message.from_user.id)
        db.commit()
        await send_profile_card(message.chat.id, message.chat.id, message.from_user.id,
                                close_uid=message.from_user.id)
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
        await send_profile_card(message.chat.id, owner_chat, message.from_user.id,
                                close_uid=message.from_user.id)


