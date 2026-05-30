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
from royal.core import (_get_identity_card_file_id, db, dp, ensure_identity_card_async, get_player, refresh_user_identity, resolve_owner_chat, term_block)

router = Router()

@router.inline_query()
async def inline_profile(iq: InlineQuery):
    """Permite o user enviar o card de perfil em qualquer chat via @bot.
    Usa o grupo ativo da DM (user_dm_settings) ou auto-resolve."""
    uid = iq.from_user.id if iq.from_user else 0
    if not uid:
        await iq.answer(results=[], cache_time=5, is_personal=True)
        return
    owner_chat = resolve_owner_chat(uid)
    if not owner_chat:
        await iq.answer(
            results=[InlineQueryResultArticle(
                id="no-profile",
                title="🚫 Sem perfil Royal",
                description="Manda mensagem no grupo Royal pra registrar.",
                input_message_content=InputTextMessageContent(
                    message_text=term_block(
                        "REINO",
                        "<i>Sem perfil no Reino ainda.</i>",
                        status="SEM_REINO", status_color="AMBER"),
                    parse_mode="HTML"),
            )],
            cache_time=10,
            is_personal=True,
            button=InlineQueryResultsButton(
                text="🏰 Configurar grupo na DM",
                start_parameter="grupo"),
        )
        return

    p = get_player(owner_chat, uid)
    royal_id = (p.get("royal_id") if p else None) or "RYL-????"

    # Refresh do username vivo do Telegram (caso user mudou @ desde a
    # ultima msg no grupo) — garante que o card reflita o handle atual.
    if iq.from_user and iq.from_user.username:
        live_name = (iq.from_user.full_name
                     or iq.from_user.first_name or "").strip()
        try:
            refresh_user_identity(owner_chat, uid, live_name,
                                  iq.from_user.username)
            db.commit()
        except Exception:
            logger.exception("inline_profile: refresh_user_identity failed")

    # SEMPRE chama ensure (idempotente — short-circuit quando hash bate).
    # Garante que cards velhos (gerados sem @username) regenerem na hora.
    try:
        identity_fid = await ensure_identity_card_async(owner_chat, uid)
    except Exception:
        logger.exception("inline_profile: ensure_identity_card_async failed "
                         "uid=%d chat=%d", uid, owner_chat)
        identity_fid = _get_identity_card_file_id(owner_chat, uid)

    if identity_fid:
        # F17: cache_time alto (1h) — identity card so muda quando o
        # sweep regenera (raro). Reduz handler calls em ~99% pra mesma
        # query repetida. is_personal=True garante isolamento por user.
        await iq.answer(
            results=[InlineQueryResultCachedPhoto(
                id=f"identity-{owner_chat}-{uid}",
                photo_file_id=identity_fid,
                caption=f"<b>{html.escape(royal_id)}</b> // Jogador do Reino",
                parse_mode="HTML",
            )],
            cache_time=3600,
            is_personal=True,
        )
        return

    # Sem identity card disponivel ainda — guia user a interagir 1x no grupo
    # pra popular display_name/username e disparar o render.
    await iq.answer(
        results=[],
        cache_time=10,
        is_personal=True,
        button=InlineQueryResultsButton(
            text="📸 Gerar identidade (abrir o bot)",
            start_parameter="identity"),
    )


