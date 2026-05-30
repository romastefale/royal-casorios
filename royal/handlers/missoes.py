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

from royal.config import (DAILY_QUESTS_BY_ID, TZ_NAME, logger)
from royal.core import (_quests_render, award_xp_immediate, bot, cap1024, cur, db, dp, ensure_player, get_anon_name, get_dm_active_chat, get_quest_state, is_player_out, resolve_dm_chat, safe_typing, term_block, today_key, with_close)

router = Router()

@router.message(Command("royalmissoes"))
async def royal_missoes(message: Message):
    """M05: lista as missoes diarias do user no chat ativo + claim."""
    if not message.from_user:
        return
    owner_chat = await resolve_dm_chat(message, action_hint="ver missoes")
    if owner_chat is None:
        return
    uid = message.from_user.id
    body, kb = _quests_render(owner_chat, uid)
    # === CARD VISUAL (card-first com text fallback) ===
    try:
        p = ensure_player(owner_chat, uid)
        db.commit()
        rid = p.get("royal_id") or "RYL-????"
        qstate = get_quest_state(owner_chat, uid)
        qrows = tuple(
            MissaoRow(
                desc=re.sub(r"^\W+", "", q["desc"]).strip(),
                progress=int(q["progress"]),
                target=int(q["target"]),
                xp=int(q["xp"]), gold=int(q["gold"]),
                state=("claimed" if q["claimed"]
                       else "ready" if q["done"] else "progress"),
            ) for q in qstate
        )
        data = MissoesData(
            self_royal_id=rid,
            self_name=get_anon_name(owner_chat, uid),
            self_avatar_slug=p.get("avatar_slug"),
            owner_uid=int(uid),
            quests=qrows,
        )
        await safe_typing(message.chat.id, "upload_photo")
        png = await asyncio.to_thread(render_missoes_card, data)
        if png and bot is not None:
            caption = f"<i>Missões diárias — reset 00:00 {TZ_NAME}.</i>"
            await bot.send_photo(
                message.chat.id,
                BufferedInputFile(png, filename=f"missoes-{rid}.jpg"),
                caption=cap1024(caption), parse_mode="HTML",
                reply_markup=with_close(kb, uid))
            return
    except Exception:
        logger.exception("render_missoes_card path failed; fallback texto")
    # === Fallback texto ===
    await message.answer(
        term_block("MISSOES", body, status="DIARIAS",
                   status_color="CYAN",
                   stamp="reset 00:00 " + TZ_NAME),
        reply_markup=with_close(kb, uid))


@router.callback_query(F.data.startswith("r:quest:"))
async def quest_claim_cb(cb: CallbackQuery):
    if not cb.data or not cb.from_user or not cb.message:
        return
    qid = cb.data.split(":", 2)[2]
    q = DAILY_QUESTS_BY_ID.get(qid)
    if not q:
        await cb.answer()
        return
    # resolve grupo dono da mensagem (DM ou grupo)
    if cb.message.chat.type in {"group", "supergroup"}:
        chat_id = cb.message.chat.id
    else:
        chat_id = get_dm_active_chat(cb.from_user.id) or 0
    if not chat_id:
        await cb.answer("Sem reino ativo.", show_alert=True)
        return
    uid = cb.from_user.id
    if is_player_out(chat_id, uid):  # Etapa 4: fora do jogo nao resgata XP/florins
        await cb.answer("Você está fora do jogo. Use /royalvoltar primeiro.",
                        show_alert=True)
        return
    day = today_key()
    # garante linha em players antes de creditar gold (evita UPDATE no-op)
    ensure_player(chat_id, uid)
    # claim atomico: so se progress>=target E ainda nao claimed
    cur.execute(
        "UPDATE quest_progress SET claimed=1 "
        "WHERE chat_id=? AND user_id=? AND day=? AND quest_id=? "
        "AND progress>=? AND claimed=0",
        (chat_id, uid, day, qid, q["target"]))
    if cur.rowcount == 0:
        db.commit()
        await cb.answer("Missao nao concluida ou ja resgatada.",
                        show_alert=True)
        return
    cur.execute("UPDATE players SET gold=gold+? WHERE chat_id=? AND user_id=?",
                (q["gold"], chat_id, uid))
    db.commit()
    award_xp_immediate(chat_id, uid, q["xp"], reason="quest")
    logger.info("[M05] claim uid=%s chat=%s quest=%s +%sXP +%s gold",
                uid, chat_id, qid, q["xp"], q["gold"])
    body, kb = _quests_render(chat_id, uid)
    kb = with_close(kb, uid)
    try:
        await cb.message.edit_text(
            term_block("MISSOES", body, status="DIARIAS",
                       status_color="CYAN",
                       stamp="reset 00:00 " + TZ_NAME),
            reply_markup=kb)
    except TelegramBadRequest:
        # A mensagem pode ser uma FOTO (card de missoes): edit_text falha
        # em mensagens-foto. Atualiza so o teclado (remove o botao
        # resgatado) — mantem o fluxo in-place sem re-renderizar o card.
        try:
            await cb.message.edit_reply_markup(reply_markup=kb)
        except TelegramBadRequest:
            pass
    except Exception:
        logger.exception("[M05] quest_claim_cb edit falhou uid=%s chat=%s",
                         uid, chat_id)
    await cb.answer(f"🎁 +{q['xp']}XP +{q['gold']}🪙!", show_alert=False)


