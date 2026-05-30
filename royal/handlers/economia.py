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
from royal.core import (EFFECT_HEART, GIFT_MAX, GIFT_MIN, GROUP_ONLY_MSG, auto_delete_after, bot, cap1024, cur, db, display_name, dp, effect_kw, ensure_player, format_br, get_anon_name, is_group, level_progress, mention, react_to, resolve_dm_chat, safe_typing, term_block, unlock_achievement, utc_iso, with_close)

router = Router()

@router.message(Command("royalsaldo"))
async def royal_saldo(message: Message):
    if not message.from_user:
        return
    owner_chat = await resolve_dm_chat(message, action_hint="ver saldo")
    if owner_chat is None:
        return
    p = ensure_player(owner_chat, message.from_user.id)
    db.commit()
    await react_to(message.chat.id, message.message_id, "🪙")
    saldo = int(p["gold"] or 0)
    rid = p.get("royal_id") or "RYL-????"
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        await safe_typing(message.chat.id, "upload_photo")
        total_xp = int(p.get("total_xp") or 0)
        lvl, _, _, _ = level_progress(total_xp)
        data = SaldoData(
            self_royal_id=rid,
            self_name=get_anon_name(owner_chat, message.from_user.id),
            self_avatar_slug=p.get("avatar_slug"),
            saldo=saldo,
            level=int(lvl),
            season_xp=int(p.get("season_xp") or 0),
            owner_uid=int(message.from_user.id),
        )
        png = await asyncio.to_thread(render_saldo_card, data)
        if png and bot is not None:
            caption = (f"<i>Suas arcas — "
                       f"<b>{format_br(saldo)}</b> 🪙</i>")
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename=f"saldo-{rid}.jpg"),
                caption=cap1024(caption),
                parse_mode="HTML",
                reply_markup=with_close(None, message.from_user.id),
            )
            return
    except Exception:
        logger.exception("render_saldo_card path failed; fallback texto")
    # === Fallback texto ===
    gold_br = f"{saldo:,}".replace(",", ".")
    body = (
        f"🪙 Suas arcas guardam <b><code>{gold_br}</code></b> florins.\n"
        f"<i>Gasta com sabedoria em /royalloja.</i>"
    )
    await message.answer(term_block("FLORINS", body,
                                    status="SALDO_OK",
                                    stamp=f"ID {rid}"),
                         reply_markup=with_close(None, message.from_user.id))


@router.message(Command("royalpresentear"))
async def royal_presentear(message: Message):
    """M02: presenteia florins a outro jogador (reply ou @user X)."""
    if not message.from_user:
        return
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    chat_id = message.chat.id
    sender = message.from_user.id

    target_uid: int | None = None
    target_name = "?"
    raw = (message.text or "").strip()
    parts = raw.split()
    amount: int | None = None

    # 1) reply -> destinatario eh quem foi respondido; arg eh valor
    if message.reply_to_message and message.reply_to_message.from_user:
        if message.reply_to_message.from_user.is_bot:
            ack = await message.answer(term_block(
                "PRESENTE",
                ">> nao da pra presentear um bot 🤖",
                status="NEGADO", status_color="HOT"))
            await auto_delete_after(ack, delay=8.0)
            return
        target_uid = message.reply_to_message.from_user.id
        target_name = display_name(message.reply_to_message)
        if len(parts) >= 2:
            try:
                amount = int(parts[1])
            except ValueError:
                pass
    # 2) /royalpresentear @user X
    elif len(parts) >= 3 and parts[1].startswith("@"):
        username = parts[1].lstrip("@").lower()
        try:
            amount = int(parts[2])
        except ValueError:
            amount = None
        row = cur.execute(
            "SELECT user_id, display_name FROM users "
            "WHERE chat_id=? AND LOWER(username)=? LIMIT 1",
            (chat_id, username)).fetchone()
        if row:
            target_uid = int(row["user_id"])
            target_name = row["display_name"] or username

    if target_uid is None or amount is None:
        ack = await message.answer(term_block(
            "PRESENTE",
            ">> uso: <code>/royalpresentear @user 100</code>\n"
            ">> ou: reply na msg + <code>/royalpresentear 100</code>",
            status="USO", status_color="AMBER"))
        await auto_delete_after(ack, delay=12.0)
        return
    if target_uid == sender:
        ack = await message.answer(term_block(
            "PRESENTE",
            ">> nao da pra se presentear, nobre 🙃",
            status="NEGADO", status_color="HOT"))
        await auto_delete_after(ack, delay=8.0)
        return
    if amount < GIFT_MIN or amount > GIFT_MAX:
        ack = await message.answer(term_block(
            "PRESENTE",
            f">> valor entre <b>{GIFT_MIN}</b> e <b>{GIFT_MAX}</b> florins",
            status="LIMITE", status_color="AMBER"))
        await auto_delete_after(ack, delay=8.0)
        return

    # Bots nunca recebem presente nem viram jogadores. Cobre o alvo via @user
    # (linha legada no `users`) e qualquer alvo bot. Checagem POSITIVA via
    # Telegram: so bloqueia ao confirmar is_bot; erro de API segue (nao quebra
    # presente legitimo se o alvo, p.ex., saiu do grupo).
    try:
        tgt_member = await bot.get_chat_member(chat_id, target_uid)
        if getattr(tgt_member.user, "is_bot", False):
            ack = await message.answer(term_block(
                "PRESENTE",
                ">> nao da pra presentear um bot 🤖",
                status="NEGADO", status_color="HOT"))
            await auto_delete_after(ack, delay=8.0)
            return
    except Exception:
        logger.exception("presentear: get_chat_member falhou alvo=%s", target_uid)

    p = ensure_player(chat_id, sender)
    if int(p["gold"] or 0) < amount:
        ack = await message.answer(term_block(
            "PRESENTE",
            f">> saldo insuficiente. voce tem "
            f"<b>{int(p['gold'] or 0)}🪙</b>",
            status="SEM_FUNDOS", status_color="HOT"))
        await auto_delete_after(ack, delay=10.0)
        return

    # transacao atomica: -sender, +target, ledger
    ensure_player(chat_id, target_uid)
    cur.execute("UPDATE players SET gold=gold-? "
                "WHERE chat_id=? AND user_id=? AND gold>=?",
                (amount, chat_id, sender, amount))
    if cur.rowcount == 0:
        await message.answer(term_block(
            "PRESENTE", ">> falha — tente de novo",
            status="ERRO", status_color="HOT"))
        return
    cur.execute("UPDATE players SET gold=gold+? "
                "WHERE chat_id=? AND user_id=?",
                (amount, chat_id, target_uid))
    cur.execute(
        "INSERT INTO gifts (chat_id, from_user, to_user, amount, sent_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (chat_id, sender, target_uid, amount, utc_iso()))
    db.commit()
    unlock_achievement(chat_id, sender, "generoso")
    logger.info("[GIFT] %s -> %s amount=%s chat=%s",
                sender, target_uid, amount, chat_id)

    sender_name = display_name(message)
    body = (
        f">> {mention(sender, sender_name)} presenteou "
        f"<b>{format_br(amount)}🪙</b>\n"
        f">> para {mention(target_uid, target_name)}\n"
        f"<i>// que generosidade real</i>"
    )
    await message.answer(
        term_block("PRESENTE", body, status="ENTREGUE",
                   status_color="ACID", stamp="🎁"),
        **effect_kw(message.chat.type, EFFECT_HEART))


