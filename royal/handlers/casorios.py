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

from royal.config import (BOSS_SPAWN_HOUR, XP_VOTE_LIKE, logger)
from royal.core import (EFFECT_HEART, EFFECT_PARTY, GROUP_ONLY_MSG, award_xp_immediate, bot, cap1024, cur, current_season_code, current_season_label, db, display_name, dp, effect_kw, ensure_chat, get_anon_name, get_player, get_votes, is_admin, is_group, react_to, resolve_dm_chat, safe_typing, schedule_next_palavra, send_couple, term_block, term_pre, upsert_user, utc_iso, vote_keyboard, with_close)

router = Router()

@router.message(Command("royalativar", "noivado"))
async def royal_ativar(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    ensure_chat(message.chat.id, message.chat.title)
    cur.execute(
        "INSERT INTO chats_rpg (chat_id, current_season) VALUES (?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET current_season=excluded.current_season",
        (message.chat.id, current_season_code()))
    schedule_next_palavra(message.chat.id)
    db.commit()
    rows = [
        ("CASORIOS", "3x / dia"),
        ("PALAVRA", "60 min"),
        ("BOSS", f"dom {BOSS_SPAWN_HOUR}h"),
        ("TEMPORADA", current_season_label()),
        ("CHAT ID", str(message.chat.id)),
    ]
    body = (
        "<b>// SISTEMA ATIVADO NESTE GRUPO</b>\n"
        f"{term_pre(rows)}"
        "<i>Que comecem os feitos, nobre.</i>"
    )
    ativar_uid = message.from_user.id if message.from_user else None
    await message.answer(term_block(
        "ROYAL", body, status="ONLINE",
        stamp=current_season_label()),
        reply_markup=with_close(None, ativar_uid),
        **effect_kw(message.chat.type, EFFECT_PARTY))


@router.message(Command("royalcasar", "querocasar"))
async def royal_casar(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    if not await is_admin(message):
        await message.answer("🚫 Só admin pode invocar um casório agora.")
        return
    ensure_chat(message.chat.id, message.chat.title)
    db.commit()
    await send_couple(message.chat.id, source="manual")


@router.message(Command("royalencalhar", "encalhado"))
async def royal_encalhar(message: Message):
    if not message.from_user:
        return
    chat_id = message.chat.id
    upsert_user(chat_id, message.from_user.id, display_name(message), message.from_user.username)
    cur.execute("UPDATE users SET opt_out=1 WHERE chat_id=? AND user_id=?",
                (chat_id, message.from_user.id))
    db.commit()
    await react_to(message.chat.id, message.message_id, "🚫")
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        await safe_typing(message.chat.id, "upload_photo")
        p = get_player(chat_id, message.from_user.id) or {}
        rid = p.get("royal_id") or "RYL-????"
        data = ShipperData(
            royal_id=rid,
            name=get_anon_name(chat_id, message.from_user.id),
            avatar_slug=p.get("avatar_slug"),
            opted_out=True,
            owner_uid=int(message.from_user.id),
        )
        png = await asyncio.to_thread(render_shipper_card, data)
        if png and bot is not None:
            caption = ("<i>🚫💔 modo encalhado ativado — "
                       "sem casórios automáticos.</i>")
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename="encalhar.jpg"),
                caption=cap1024(caption))
            return
    except Exception:
        logger.exception("[ROYALENCALHAR] card render failed; fallback texto")
    await message.answer(term_block(
        "SHIPPER",
        "🚫💔 <i>Modo encalhado(a) ativado.</i>\n"
        ">> sistema não vai mais te colocar em casórios.",
        status="OPT_OUT", status_color="AMBER"))


@router.message(Command("royaldesencalhar", "desencalhar"))
async def royal_desencalhar(message: Message):
    if not message.from_user:
        return
    chat_id = message.chat.id
    upsert_user(chat_id, message.from_user.id, display_name(message), message.from_user.username)
    cur.execute("UPDATE users SET opt_out=0 WHERE chat_id=? AND user_id=?",
                (chat_id, message.from_user.id))
    db.commit()
    await react_to(message.chat.id, message.message_id, "💘")
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        await safe_typing(message.chat.id, "upload_photo")
        p = get_player(chat_id, message.from_user.id) or {}
        rid = p.get("royal_id") or "RYL-????"
        data = ShipperData(
            royal_id=rid,
            name=get_anon_name(chat_id, message.from_user.id),
            avatar_slug=p.get("avatar_slug"),
            opted_out=False,
            owner_uid=int(message.from_user.id),
        )
        png = await asyncio.to_thread(render_shipper_card, data)
        if png and bot is not None:
            caption = "<i>💘🔄 de volta ao jogo dos casórios!</i>"
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename="desencalhar.jpg"),
                caption=cap1024(caption),
                **effect_kw(message.chat.type, EFFECT_HEART))
            return
    except Exception:
        logger.exception("[ROYALDESENCALHAR] card render failed; fallback texto")
    await message.answer(term_block(
        "SHIPPER",
        "🔄💘 <i>De volta ao jogo dos casórios!</i>",
        status="OPT_IN"),
        **effect_kw(message.chat.type, EFFECT_HEART))


@router.message(Command("royalmeuscasorios", "meusdivorcios"))
async def royal_meus(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    chat_id = await resolve_dm_chat(message, action_hint="ver meus casorios")
    if chat_id is None:
        return
    await react_to(message.chat.id, message.message_id, "👀")
    await safe_typing(message.chat.id, "upload_photo")
    cur.execute("SELECT COUNT(*) AS total FROM couples WHERE chat_id=? AND (user1=? OR user2=?)",
                (chat_id, uid, uid))
    total = cur.fetchone()["total"]
    cur.execute(
        """
        SELECT CASE WHEN user1=? THEN user2 ELSE user1 END AS partner, COUNT(*) AS total
        FROM couples WHERE chat_id=? AND (user1=? OR user2=?)
        GROUP BY partner ORDER BY total DESC LIMIT 5
        """, (uid, chat_id, uid, uid))
    rows = cur.fetchall()
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        self_p = get_player(chat_id, uid) or {}
        self_rid = self_p.get("royal_id") or "RYL-????"
        self_name = get_anon_name(chat_id, uid)
        self_slug = self_p.get("avatar_slug")
        partners_data: list[PartnerMini] = []
        for r in rows:
            pp = get_player(chat_id, r["partner"]) or {}
            partners_data.append(PartnerMini(
                royal_id=pp.get("royal_id") or "RYL-????",
                name=get_anon_name(chat_id, r["partner"]),
                avatar_slug=pp.get("avatar_slug"),
                total=int(r["total"]),
            ))
        data = MeusCasoriosData(
            self_royal_id=self_rid,
            self_name=self_name,
            self_avatar_slug=self_slug,
            total_casorios=int(total),
            top_partners=tuple(partners_data),
            season_label=current_season_label(),
            owner_uid=int(uid),
        )
        png = await asyncio.to_thread(render_meuscasorios_card, data)
        if png and bot is not None:
            caption = (f"<i>Seus casórios no reino — "
                       f"<b>{total}</b> ao todo.</i>")
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename=f"meuscasorios-{self_rid}.jpg"),
                caption=cap1024(caption),
                parse_mode="HTML",
            )
            return
    except Exception:
        logger.exception("render_meuscasorios_card path failed; fallback texto")
    # === Fallback texto (mantido como salvaguarda) ===
    body = f"<i>Você já participou de <b>{total}</b> casórios. 😳</i>"
    if rows:
        body += "\n\n<b>>> TOP PARES</b>\n" + "\n".join(
            f"• {html.escape(get_anon_name(chat_id, row['partner']))} — "
            f"<code>{row['total']}x</code>"
            for row in rows
        )
    await message.answer(term_block(
        "CASORIOS", body, status="HISTORICO", status_color="CYAN"))


@router.message(Command("royalcasorios", "divorcios"))
async def royal_casorios(message: Message):
    chat_id = message.chat.id
    cas_uid = message.from_user.id if message.from_user else None
    if message.from_user:
        await react_to(message.chat.id, message.message_id, "👀")
    cur.execute(
        """
        SELECT user1, user2, COUNT(*) AS total FROM couples
        WHERE chat_id=? GROUP BY user1, user2 ORDER BY total DESC LIMIT 10
        """, (chat_id,))
    rows = cur.fetchall()
    if not rows:
        await message.answer(term_block(
            "CASORIOS",
            "<i>Ainda não existem casórios suficientes pra ranking.</i>",
            status="VAZIO", status_color="AMBER"),
            reply_markup=with_close(None, cas_uid))
        return
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        await safe_typing(message.chat.id, "upload_photo")
        entries: list[CouplePodiumEntry] = []
        for i, row in enumerate(rows, start=1):
            p1 = get_player(chat_id, row["user1"]) or {}
            p2 = get_player(chat_id, row["user2"]) or {}
            entries.append(CouplePodiumEntry(
                rank=i,
                p1_royal_id=p1.get("royal_id") or "RYL-????",
                p1_name=get_anon_name(chat_id, row["user1"]),
                p1_avatar_slug=p1.get("avatar_slug"),
                p2_royal_id=p2.get("royal_id") or "RYL-????",
                p2_name=get_anon_name(chat_id, row["user2"]),
                p2_avatar_slug=p2.get("avatar_slug"),
                total=int(row["total"]),
            ))
        season = current_season_label()
        png = await asyncio.to_thread(
            render_casorios_ranking_card, season, tuple(entries))
        if png and bot is not None:
            top = entries[0]
            caption = (
                f"<b>👑 ROYAL COUPLES // {html.escape(season)}</b>\n"
                f"<i>1º lugar: {html.escape(top.p1_name)} ♥ "
                f"{html.escape(top.p2_name)} — "
                f"<b>{top.total}x</b></i>"
            )
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename="royal_casorios.jpg"),
                caption=cap1024(caption),
                parse_mode="HTML",
                reply_markup=with_close(None, cas_uid),
            )
            return
    except Exception:
        logger.exception("render_casorios_ranking_card path failed; fallback texto")
    # === Fallback texto ===
    medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
    lines = []
    for i, row in enumerate(rows, start=1):
        lines.append(
            f"{medals[i-1]} <b>{i}.</b> "
            f"{html.escape(get_anon_name(chat_id, row['user1']))} ❤️ "
            f"{html.escape(get_anon_name(chat_id, row['user2']))} — "
            f"<code>{row['total']}x</code>")
    await message.answer(term_block(
        "CASORIOS", "\n".join(lines),
        status="RANKING", stamp=current_season_label()),
        reply_markup=with_close(None, cas_uid))


@router.message(F.text == "📊 Meus casórios")
async def btn_meus(message: Message):
    await royal_meus(message)


@router.callback_query(F.data.startswith(("ship_like:", "ship_dislike:")))
async def vote_cb(cb: CallbackQuery):
    if not cb.data:
        return
    action, couple_id_raw = cb.data.split(":", 1)
    couple_id = int(couple_id_raw)
    vote_type = "like" if action == "ship_like" else "dislike"
    try:
        cur.execute(
            "INSERT INTO votes (couple_id, voter_id, type, created_at) VALUES (?, ?, ?, ?)",
            (couple_id, cb.from_user.id, vote_type, utc_iso()))
        db.commit()
    except sqlite3.IntegrityError:
        await cb.answer("Você já votou nesse casório 😶")
        return
    likes, dislikes = get_votes(couple_id)
    # XP pro casal por like
    if vote_type == "like":
        cur.execute("SELECT chat_id, user1, user2 FROM couples WHERE id=?", (couple_id,))
        crow = cur.fetchone()
        if crow:
            for uid in (crow["user1"], crow["user2"]):
                award_xp_immediate(crow["chat_id"], uid, XP_VOTE_LIKE, reason="vote_like")
    try:
        await cb.message.edit_reply_markup(reply_markup=vote_keyboard(couple_id, likes, dislikes))
    except TelegramBadRequest:
        pass
    await cb.answer("Voto registrado 👑")


