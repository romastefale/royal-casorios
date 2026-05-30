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

from royal.config import (GOLD_PALAVRA_WIN, XP_PALAVRA_WIN_BONUS, logger)
from royal.core import (GROUP_ONLY_MSG, bot, cap1024, cur, dp, format_challenge_text, get_active_challenge, is_group, local_now, react_to, safe_typing, term_block, utc_now)

router = Router()

@router.message(Command("royalpalavra"))
async def royal_palavra_status(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    ch = get_active_challenge(message.chat.id)
    if not ch:
        cur.execute("SELECT next_palavra_at FROM chats_rpg WHERE chat_id=?", (message.chat.id,))
        r = cur.fetchone()
        nxt = r["next_palavra_at"] if r else None
        if nxt:
            try:
                nxt_dt = datetime.fromisoformat(nxt)
                mins = max(0, int((nxt_dt - local_now()).total_seconds() / 60))
                await message.answer(term_block(
                    "PALAVRA",
                    f"<i>Sem transmissão ativa.</i>\n>> próxima em <b>~{mins} min</b>",
                    status="STANDBY", status_color="AMBER"))
                return
            except Exception:
                pass
        await message.answer(term_block(
            "PALAVRA",
            "<i>Sem transmissão ativa. Aguarde o próximo sinal.</i>",
            status="STANDBY", status_color="AMBER"))
        return
    if message.from_user:
        await react_to(message.chat.id, message.message_id, "🔤")
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        await safe_typing(message.chat.id, "upload_photo")
        try:
            ends_at = datetime.fromisoformat(ch["ends_at"])
            mins_left = max(0, int((ends_at - utc_now()).total_seconds() / 60))
        except Exception:
            mins_left = ch.get("duration_min", 5)
        # display: charada usa hint, demais usa display
        if ch["type"] == "charada":
            disp = ch.get("hint") or ""
        elif ch["type"] == "spoiler_img":
            disp = "TOQUE A IMAGEM PRA REVELAR"
        else:
            disp = ch.get("display") or ""
        data = PalavraActiveData(
            challenge_id=int(ch["id"]),
            ch_type=ch["type"],
            display=disp,
            mins_left=mins_left,
            attempts=int(ch.get("attempts_count") or 0),
            reward_xp=XP_PALAVRA_WIN_BONUS,
            reward_gold=GOLD_PALAVRA_WIN,
        )
        png = await asyncio.to_thread(render_palavra_active_card, data)
        if png and bot is not None:
            caption = (f"<i>Transmissão ativa — "
                       f"<b>~{mins_left}min</b> restantes</i>")
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename="palavra.jpg"),
                caption=cap1024(caption))
            return
    except Exception:
        logger.exception("[ROYALPALAVRA] card render failed; fallback texto")
    await message.answer(format_challenge_text(ch))


