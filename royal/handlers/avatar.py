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

from royal.core import (AVATAR_MOSAIC_MARKER, AvatarReplyFilter, EFFECT_PARTY, _apply_avatar, _avatar_kb, _avatar_reveal_payload, auto_delete_after, cap1024, current_season_label, db, dp, effect_kw, ensure_chat, ensure_player, get_player, is_group, react_to, register_owner, resolve_owner_chat, safe_typing, term_block)

router = Router()

@router.message(Command("royalavatar"))
async def royal_avatar_cmd(message: Message):
    if not message.from_user:
        return
    await react_to(message.chat.id, message.message_id, "👀")
    # Resolve em qual chat-dono este user tem perfil RPG
    if is_group(message):
        owner_chat = message.chat.id
        ensure_chat(message.chat.id, message.chat.title)
        ensure_player(message.chat.id, message.from_user.id)
        db.commit()
    else:
        owner_chat = resolve_owner_chat(message.from_user.id)
        if owner_chat is None:
            await message.answer(
                "😶 Você ainda não tem perfil no Reino.\n"
                "Envie uma mensagem no grupo Royal para começar!")
            return

    p = get_player(owner_chat, message.from_user.id)
    if not p:
        await message.answer("😶 Você ainda não tem perfil no Reino.")
        return

    season = current_season_label()
    royal_id = p.get("royal_id") or ""
    cur_slug = royal_avatars.resolve_slug(p.get("avatar_slug"), royal_id)
    cur_num = royal_avatars.number_of(cur_slug)
    cur_name = royal_avatars.display_name(cur_slug)
    is_default = not p.get("avatar_slug")

    # Bloqueio: ja escolheu nesta temporada
    if p.get("avatar_slug") and p.get("avatar_season") == season:
        msg = await message.answer(term_block(
            "AVATAR.SYS",
            (f"<b>!! Voce ja escolheu seu avatar nesta temporada.</b>\n"
             f"<i>Atual: #{cur_num:02d} {cur_name}</i>\n"
             f"// Proxima troca disponivel na proxima temporada."),
            status="BLOQUEADO", status_color="HOT",
            stamp=season,
        ))
        await auto_delete_after(msg, delay=15.0)
        return

    await safe_typing(message.chat.id, "upload_photo")
    png = royal_avatars.mosaic_bytes()
    if not png:
        await message.answer("⚠️ Mosaico de avatares indisponivel.")
        return

    atual_label = (f"<i>Atual (default): #{cur_num:02d} {cur_name}</i>"
                   if is_default else
                   f"<i>Atual: #{cur_num:02d} {cur_name}</i>")
    caption = term_block(
        AVATAR_MOSAIC_MARKER,
        (f"<b>👑 ESCOLHA SEU AVATAR</b>\n"
         f"{atual_label}\n\n"
         f"<b>&gt;&gt; Toque no número do seu avatar (1 a 36).</b>\n"
         f"⚠️ A escolha vale por toda a temporada (so podera trocar na proxima)."),
        status="ESCOLHA", status_color="ACID",
        stamp=season,
    )
    sent = await message.answer_photo(
        BufferedInputFile(png, filename="royal_avatares_mosaico.png"),
        caption=cap1024(caption),
        reply_markup=_avatar_kb(message.from_user.id),
    )
    # Trava os botões pro dono; auto_delete=0 → persiste até escolher/Fechar.
    register_owner(sent, message.from_user.id, auto_delete_secs=0.0)


@router.message(AvatarReplyFilter())
async def handle_avatar_reply(message: Message):
    if not message.from_user:
        return
    n = int((message.text or "").strip())

    # Resolve chat-dono
    if is_group(message):
        owner_chat = message.chat.id
    else:
        owner_chat = resolve_owner_chat(message.from_user.id)
        if owner_chat is None:
            return

    status, payload = _apply_avatar(owner_chat, message.from_user.id, n)
    if status == "noprofile":
        ack = await message.reply("😶 Voce ainda nao tem perfil no Reino.")
        await auto_delete_after(ack, delay=8.0)
        return
    if status == "locked":
        cur_slug = payload
        ack = await message.reply(
            f"❌ Voce ja escolheu seu avatar nesta temporada "
            f"(#{royal_avatars.number_of(cur_slug):02d} "
            f"{royal_avatars.display_name(cur_slug)}). "
            f"Proxima troca disponivel na proxima temporada."
        )
        await auto_delete_after(ack, delay=12.0)
        return
    if status != "ok":
        return  # badnum — filtro ja validou range, defensivo

    slug = payload
    png, caption = _avatar_reveal_payload(
        owner_chat, message.from_user.id, slug, n)
    if png:
        await message.reply_photo(
            BufferedInputFile(png, filename=f"avatar-{slug}.png"),
            caption=cap1024(caption),
            **effect_kw(message.chat.type, EFFECT_PARTY),
        )
    else:
        await message.reply(
            caption, **effect_kw(message.chat.type, EFFECT_PARTY))


