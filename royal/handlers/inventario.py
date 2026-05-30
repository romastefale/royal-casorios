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
from royal.core import (ITEMS, assert_owner, award_xp_immediate, bot, cap1024, consume_consumable, cur, db, dp, ensure_player, format_br, get_anon_name, get_player, inv_keyboard, react_to, register_owner, resolve_dm_chat, safe_typing, term_block)

router = Router()

@router.message(Command("royalinventario"))
async def royal_inv(message: Message):
    if not message.from_user:
        return
    chat_id = await resolve_dm_chat(message, action_hint="ver inventario")
    if chat_id is None:
        return
    uid = message.from_user.id
    ensure_player(chat_id, uid)
    db.commit()
    await react_to(message.chat.id, message.message_id, "🎒")
    cur.execute(
        "SELECT item_id, qty, equipped FROM inventory WHERE chat_id=? AND user_id=? AND qty>0 ORDER BY equipped DESC, item_id",
        (chat_id, uid))
    rows = cur.fetchall()
    if not rows:
        await message.answer(term_block(
            "INVENTARIO",
            "<i>Cofre vazio. Visita a /royalloja pra comprar itens.</i>",
            status="VAZIO", status_color="AMBER"))
        return
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        await safe_typing(message.chat.id, "upload_photo")
        self_p = get_player(chat_id, uid) or {}
        self_rid = self_p.get("royal_id") or "RYL-????"
        self_name = get_anon_name(chat_id, uid)
        self_slug = self_p.get("avatar_slug")
        saldo = int(self_p.get("gold") or 0)
        slots: list[InventarioSlot] = []
        for r in rows:
            item = ITEMS.get(r["item_id"])
            if not item:
                continue
            slots.append(InventarioSlot(
                item_id=r["item_id"],
                emoji=item.get("emoji", "📦"),
                name=item.get("name", r["item_id"]),
                qty=int(r["qty"]),
                equipped=bool(r["equipped"]),
                item_type=item.get("type", "item"),
            ))
        data = InventarioData(
            self_royal_id=self_rid,
            self_name=self_name,
            self_avatar_slug=self_slug,
            saldo=saldo,
            slots=tuple(slots[:8]),
            owner_uid=int(message.from_user.id),
        )
        png = await asyncio.to_thread(render_inventario_card, data)
        if png and bot is not None:
            n_eq = sum(1 for s in slots if s.equipped)
            caption = (f"<b>🎒 MOCHILA // ROY {html.escape(self_rid)}</b>\n"
                       f"<i>{len(slots)} itens · {n_eq} equipados · "
                       f"<b>{format_br(saldo)}</b> 🪙</i>")
            sent = await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename=f"inventario-{self_rid}.jpg"),
                caption=cap1024(caption),
                parse_mode="HTML",
                reply_markup=inv_keyboard(chat_id, uid, snapshot=rows),
            )
            register_owner(sent, uid, auto_delete_secs=60.0)
            return
    except Exception:
        logger.exception("render_inventario_card path failed; fallback texto")
    # === Fallback texto ===
    parts = []
    for r in rows:
        item = ITEMS.get(r["item_id"])
        if not item:
            continue
        eq = "✅ " if r["equipped"] else "  "
        parts.append(
            f"{eq}{item['emoji']} <b>{item['name']}</b> "
            f"<code>x{r['qty']}</code>\n   <i>{item['desc']}</i>"
        )
    body = "\n\n".join(parts)
    sent = await message.answer(
        term_block("INVENTARIO", body, status="LOADED"),
        reply_markup=inv_keyboard(chat_id, uid))
    register_owner(sent, uid, auto_delete_secs=60.0)


@router.callback_query(F.data.startswith("r:inv:"))
async def inv_cb(cb: CallbackQuery):
    if not cb.data or not cb.from_user or not cb.message:
        return
    parts = cb.data.split(":")
    if len(parts) < 4:
        await cb.answer()
        return
    if not await assert_owner(cb):
        return
    sub = parts[2]
    iid = parts[3]
    chat_id = cb.message.chat.id
    uid = cb.from_user.id

    if sub == "eq":
        cur.execute("SELECT equipped FROM inventory WHERE chat_id=? AND user_id=? AND item_id=?",
                    (chat_id, uid, iid))
        row = cur.fetchone()
        if not row:
            await cb.answer()
            return
        new_eq = 0 if row["equipped"] else 1
        if new_eq == 1:
            # desequipa outros do mesmo stat
            item = ITEMS.get(iid, {})
            if item.get("stat"):
                cur.execute(
                    "SELECT inventory.item_id FROM inventory WHERE chat_id=? AND user_id=? AND equipped=1",
                    (chat_id, uid))
                for r in cur.fetchall():
                    other = ITEMS.get(r["item_id"])
                    if other and other.get("stat") == item.get("stat"):
                        cur.execute(
                            "UPDATE inventory SET equipped=0 WHERE chat_id=? AND user_id=? AND item_id=?",
                            (chat_id, uid, r["item_id"]))
        cur.execute("UPDATE inventory SET equipped=? WHERE chat_id=? AND user_id=? AND item_id=?",
                    (new_eq, chat_id, uid, iid))
        db.commit()
        await cb.answer("Equipado ✓" if new_eq else "Desequipado")
        return

    if sub == "use":
        item = ITEMS.get(iid, {})
        if item.get("type") != "consumable":
            await cb.answer()
            return
        effect = item.get("effect")
        if effect == "xp100":
            # consome ANTES de conceder: o guard atomico fecha o exploit de
            # clicar o botao persistente depois do item zerar (ver
            # consume_consumable em core).
            if not consume_consumable(chat_id, uid, iid):
                await cb.answer("Você não tem mais esse item.", show_alert=False)
                return
            award_xp_immediate(chat_id, uid, 100, reason="tomo")
            await cb.answer("+100 XP ✓")
            return
        if effect == "heal":
            if not consume_consumable(chat_id, uid, iid):
                await cb.answer("Você não tem mais esse item.", show_alert=False)
                return
            await cb.answer("HP restaurado ✓")
            return
        await cb.answer()
        return

    await cb.answer()


