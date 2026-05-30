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

from royal.core import (STYLE_INFO, STYLE_NO, bot, cur, db, dp, ensure_player, get_dm_active_chat, get_player, group_picker_kb, ikb, is_group, is_test_chat, list_user_groups, set_dm_active_chat, term_block, term_pre)

router = Router()

@router.message(Command("royalprivacidade"))
async def royal_priv(message: Message):
    if not message.from_user:
        return
    if is_group(message):
        kb = None
        try:
            me = await bot.me()
            if me and me.username:
                kb = InlineKeyboardMarkup(inline_keyboard=[[ikb(
                    "🔒 Abrir na DM", style=STYLE_INFO,
                    url=f"https://t.me/{me.username}?start=priv")]])
        except Exception:
            kb = None
        await message.answer(
            "🔒 Use /royalprivacidade no chat privado comigo para "
            "configurar suas opções.", reply_markup=kb)
        return
    cur.execute("SELECT chat_id FROM players WHERE user_id=? LIMIT 1", (message.from_user.id,))
    row = cur.fetchone()
    if not row:
        await message.answer("Você ainda não tem perfil no Reino.")
        return
    chat_id = row["chat_id"]
    p = ensure_player(chat_id, message.from_user.id)
    db.commit()
    hide_rank = bool(p["privacy_hide_ranking"])
    hide_stats = bool(p["privacy_hide_stats"])
    rank_btn = ("🙈 Estou oculto do ranking" if hide_rank
                else "🏆 Estou no ranking")
    stats_btn = ("🙈 Stats detalhados ocultos" if hide_stats
                 else "📊 Stats detalhados visíveis")
    rows = [
        ("RANKING", "OCULTO" if hide_rank else "VISIVEL"),
        ("STATS DETALHADOS", "OCULTOS" if hide_stats else "VISIVEIS"),
    ]
    body = f"{term_pre(rows)}<i>Toca nos botões pra alternar.</i>"
    await message.answer(
        term_block("PRIVACIDADE", body,
                   status="CONFIG", status_color="CYAN",
                   stamp="dados sob seu controle"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [ikb(rank_btn, callback_data="r:priv:rank", style=STYLE_INFO)],
            [ikb(stats_btn, callback_data="r:priv:stats", style=STYLE_INFO)],
        ]))


@router.message(Command("royaldados"))
async def royal_dados(message: Message):
    if not message.from_user:
        return
    if is_group(message):
        await message.answer(term_block(
            "DADOS",
            "<i>Use /royaldados no chat privado comigo.</i>",
            status="DM_ONLY", status_color="AMBER"))
        return
    body = (
        "<i>Você controla seus dados no reino:</i>\n"
        "<blockquote>"
        "📥 <b>Exportar</b> — JSON com seus perfis e registros\n"
        "🗑️ <b>Apagar</b> — remove tudo (irrecuperável)"
        "</blockquote>"
    )
    await message.answer(
        term_block("DADOS", body, status="CONFIG", status_color="CYAN",
                   stamp="LGPD · dados sob seu controle"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [ikb("📥 Exportar JSON", callback_data="r:dados:export", style=STYLE_INFO)],
            [ikb("🗑️ Apagar tudo", callback_data="r:dados:wipe", style=STYLE_NO)],
        ]))


@router.message(Command("royalgrupo"))
async def royal_grupo(message: Message):
    if not message.from_user:
        return
    if is_group(message):
        await message.answer(term_block(
            "REINO",
            "<i>Esse comando configura o grupo ativo da sua DM.\n"
            "Roda /royalgrupo no chat privado comigo.</i>",
            status="DM_ONLY", status_color="AMBER"))
        return
    uid = message.from_user.id
    groups = list_user_groups(uid)
    if not groups:
        await message.answer(term_block(
            "REINO",
            "<i>Voce ainda nao tem perfil em nenhum grupo Royal.</i>",
            status="SEM_REINO", status_color="AMBER"))
        return
    active = get_dm_active_chat(uid)
    active_title = next((t for c, t in groups if c == active), None)
    rows = [
        ("ATIVO", active_title or "—"),
        ("GRUPOS", str(len(groups))),
    ]
    body = (
        f"{term_pre(rows)}"
        "<i>Escolhe abaixo qual grupo passa a ser o ativo na DM:</i>"
    )
    await message.answer(
        term_block("REINO", body, status="CONFIG", status_color="CYAN",
                   stamp="trocavel a qualquer hora"),
        reply_markup=group_picker_kb(groups))


@router.callback_query(F.data.startswith("g:pick:"))
async def cb_group_pick(cb: CallbackQuery):
    if not cb.from_user or not cb.data:
        await cb.answer()
        return
    try:
        chat_id = int(cb.data.split(":", 2)[2])
    except (ValueError, IndexError):
        await cb.answer("ID invalido", show_alert=False)
        return
    uid = cb.from_user.id
    if is_test_chat(chat_id) or not get_player(chat_id, uid):
        await cb.answer("Grupo indisponivel.", show_alert=True)
        return
    set_dm_active_chat(uid, chat_id)
    cur.execute("SELECT title FROM chats WHERE chat_id=?", (chat_id,))
    r = cur.fetchone()
    title = (r["title"] if r and r["title"] else f"Grupo {chat_id}")
    await cb.answer(f"✅ Ativo: {title}", show_alert=False)
    try:
        if cb.message:
            await cb.message.edit_text(term_block(
                "REINO",
                f"<b>>> ATIVO:</b> <i>{html.escape(title)}</i>\n"
                "<i>// Pronto. Agora /royalperfil, /royalranking, /royalficha "
                "e configs usam esse grupo por padrao.</i>",
                status="OK", status_color="ACID",
                stamp="troca via /royalgrupo"))
    except TelegramBadRequest:
        pass


@router.callback_query(F.data.startswith("r:priv:"))
async def priv_cb(cb: CallbackQuery):
    if not cb.data or not cb.from_user:
        return
    parts = cb.data.split(":")
    if len(parts) < 3:
        await cb.answer()
        return
    sub = parts[2]
    cur.execute("SELECT chat_id FROM players WHERE user_id=? LIMIT 1", (cb.from_user.id,))
    row = cur.fetchone()
    if not row:
        await cb.answer("Sem perfil", show_alert=True)
        return
    chat_id = row["chat_id"]
    if sub == "rank":
        cur.execute(
            "UPDATE players SET privacy_hide_ranking=1-privacy_hide_ranking "
            "WHERE chat_id=? AND user_id=?", (chat_id, cb.from_user.id))
    elif sub == "stats":
        cur.execute(
            "UPDATE players SET privacy_hide_stats=1-privacy_hide_stats "
            "WHERE chat_id=? AND user_id=?", (chat_id, cb.from_user.id))
    db.commit()
    await cb.answer("Atualizado ✓")


@router.callback_query(F.data.startswith("r:dados:"))
async def dados_cb(cb: CallbackQuery):
    assert bot is not None
    if not cb.data or not cb.from_user:
        return
    parts = cb.data.split(":")
    sub = parts[2] if len(parts) > 2 else ""
    uid = cb.from_user.id
    if sub == "export":
        cur.execute("SELECT * FROM players WHERE user_id=?", (uid,))
        players = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM users WHERE user_id=?", (uid,))
        users = [dict(r) for r in cur.fetchall()]
        import json
        payload = json.dumps({"players": players, "users": users}, indent=2, default=str)
        if len(payload) < 3500:
            await bot.send_message(cb.from_user.id, f"<pre>{html.escape(payload)}</pre>")
        else:
            await bot.send_message(cb.from_user.id, "📦 Dados muito grandes. Resumo:\n" +
                                   f"Perfis: {len(players)}\nRegistros user: {len(users)}")
        await cb.answer("Enviado por DM")
        return
    if sub == "wipe":
        cur.execute("DELETE FROM players WHERE user_id=?", (uid,))
        cur.execute("DELETE FROM inventory WHERE user_id=?", (uid,))
        cur.execute("UPDATE users SET opt_out=1 WHERE user_id=?", (uid,))
        db.commit()
        await cb.answer("Dados apagados ✓", show_alert=True)
        return
    await cb.answer()


