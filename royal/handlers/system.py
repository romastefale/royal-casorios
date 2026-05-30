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

from royal.config import (LUCKY_DAILY_CAP, LUCKY_EMOJI, MUSIC_BOT_ID, MUSIC_BOT_REACTION, MUSIC_BOT_XP_MULTIPLIER, OWNER_USER_ID, REACTION_XP, REACTION_XP_DAILY_CAP, RECENT_WINDOW_SECONDS, XP_PER_MESSAGE, logger, lucky_reward_for)
from royal.core import (REJOIN_DEDUP_SEC, _extract_text_mentioned_users, _rejoin_welcome_ts, activity_buffer, auto_delete_after, award_xp_immediate, award_xp_message, bot, cur, db, display_name, dp, ensure_chat, ensure_player, format_br, get_active_challenge, get_name, get_player, handle_palavra_attempt, is_chat_muted, local_now, mention, migrate_chat_data, normalize_pair, pair_buffer, quest_bump, react_to, recent_messages, safe_send, send_profile_card, term_block, today_key, upsert_user, utc_now)

router = Router()

@router.message(
    F.chat.type.in_({"group", "supergroup"}),
    F.from_user.id == MUSIC_BOT_ID,
)
async def handle_music_bot_post(message: Message):
    """Bot de musica publicou faixa marcando um user → pontua o user
    marcado com 3x XP_PER_MESSAGE e reage ✨ na publicacao."""
    if MUSIC_BOT_ID == 0:
        return
    chat_id = message.chat.id
    sc = message.sender_chat
    logger.info(
        "[MUSIC_BOT] recebido chat=%d mid=%d ctype=%s from_user_id=%s sender_chat_id=%s "
        "has_text=%s has_caption=%s entities=%d caption_entities=%d",
        chat_id, message.message_id, message.content_type,
        (message.from_user.id if message.from_user else None),
        (sc.id if sc else None),
        bool(message.text), bool(message.caption),
        len(message.entities or []), len(message.caption_entities or []),
    )
    try:
        ensure_chat(chat_id, message.chat.title)
    except Exception:
        logger.exception("ensure_chat failed for music bot post chat=%d", chat_id)

    mentioned = _extract_text_mentioned_users(message)
    if not mentioned:
        logger.info("[MUSIC_BOT] nenhum text_mention valido chat=%d mid=%d",
                    chat_id, message.message_id)
        return

    xp_amount = XP_PER_MESSAGE * MUSIC_BOT_XP_MULTIPLIER
    success_count = 0
    for uid, name in mentioned:
        try:
            upsert_user(chat_id, uid, name, None)
            ensure_player(chat_id, uid)
            award_xp_immediate(chat_id, uid, xp_amount, reason="music_mention")
            success_count += 1
            logger.info("music_mention xp+%d uid=%d chat=%d mid=%d",
                        xp_amount, uid, chat_id, message.message_id)
        except Exception:
            logger.exception("music_mention award failed uid=%d chat=%d", uid, chat_id)

    if success_count > 0:
        try:
            await react_to(chat_id, message.message_id, MUSIC_BOT_REACTION)
        except Exception:
            logger.exception("music_mention reaction failed chat=%d mid=%d",
                             chat_id, message.message_id)


@router.chat_member(ChatMemberUpdatedFilter(member_status_changed=JOIN_TRANSITION))
async def on_member_rejoin(event: ChatMemberUpdated):
    """Reentrada de membro no grupo. Se quem entrou JA tem progresso salvo
    (linha em players deste chat), da boas-vindas marcando a pessoa com uma
    foto = ficha. O progresso NUNCA e apagado quando alguem sai, entao
    'restaurar' e automatico — aqui so anunciamos a volta.

    JOIN_TRANSITION = (LEFT|KICKED|RESTRICTED-fora) >> (MEMBER|ADMIN|CREATOR|
    RESTRICTED-dentro): o lado ANTIGO e sempre um estado FORA do grupo, entao
    e sempre uma reentrada real (nao dispara em quem so trocou de restricao
    estando dentro). Dedup curto extra blinda contra churn de status.

    ⚠️ Requer o bot ADMIN no grupo: o Telegram so entrega chat_member updates
    a bots administradores. O tipo `chat_member` entra em allowed_updates
    automaticamente via dp.resolve_used_update_types() (handler registrado)."""
    if not event.chat or event.chat.type not in {"group", "supergroup"}:
        return
    u = event.new_chat_member.user if event.new_chat_member else None
    if u is None or u.is_bot:
        return
    chat_id = event.chat.id
    existing = get_player(chat_id, u.id)
    if not existing:
        return  # membro genuinamente novo — nada de progresso a restaurar
    # dedup: nao re-saudar o mesmo (chat,user) dentro da janela.
    key = (chat_id, u.id)
    nowm = asyncio.get_running_loop().time()
    if nowm - _rejoin_welcome_ts.get(key, 0.0) < REJOIN_DEDUP_SEC:
        return
    _rejoin_welcome_ts[key] = nowm
    if len(_rejoin_welcome_ts) > 1000:  # prune leve, evita crescer sem limite
        cutoff = nowm - REJOIN_DEDUP_SEC
        for k in [k for k, t in _rejoin_welcome_ts.items() if t < cutoff]:
            _rejoin_welcome_ts.pop(k, None)
    ensure_chat(chat_id, event.chat.title)
    nome = u.full_name or (f"@{u.username}" if u.username else "")
    try:
        upsert_user(chat_id, u.id, nome, u.username)
    except Exception:
        logger.exception("[REENTRY] upsert_user falhou uid=%s", u.id)
    who = mention(u.id, nome or "nobre")
    welcome = term_block(
        "REENTRADA",
        f">> {who} voltou ao reino\n"
        "// progresso restaurado: nivel, classe, XP, saldo, inventario\n"
        "<i>nada se perdeu — tudo continua de onde parou.</i>",
        status="CONECTADO", status_color="ACID",
        stamp=local_now().strftime("%d/%m/%Y %H:%M"))
    try:
        await send_profile_card(chat_id, chat_id, u.id, caption_override=welcome)
        logger.info("[REENTRY] welcome-back uid=%s chat=%s royal=%s",
                    u.id, chat_id, existing.get("royal_id"))
    except Exception:
        logger.exception("[REENTRY] envio ficha falhou uid=%s chat=%s",
                         u.id, chat_id)


@router.message(F.migrate_to_chat_id | F.migrate_from_chat_id)
async def on_chat_migration(message: Message):
    """Grupo -> supergrupo: o Telegram emite uma mensagem de servico e troca o
    chat_id. Sem migrar os dados, todo o progresso (XP, saldo, casorios,
    conquistas...) ficaria orfao sob o id antigo. Handler registrado ANTES do
    catch-all `track` (que casaria a msg de servico sem texto e a consumiria).
    """
    if message.migrate_to_chat_id:
        old_id, new_id = message.chat.id, message.migrate_to_chat_id
    elif message.migrate_from_chat_id:
        old_id, new_id = message.migrate_from_chat_id, message.chat.id
    else:
        return

    try:
        moved = migrate_chat_data(old_id, new_id)
    except Exception:
        logger.exception("[MIGRATE] handler falhou %d -> %d", old_id, new_id)
        return
    if not moved:
        return

    total = sum(moved.values())
    try:
        await safe_send(
            new_id,
            term_block(
                "MIGRACAO",
                ">> grupo promovido a <b>supergrupo</b>.\n"
                f"// progresso preservado: <b>{total}</b> registros migrados.\n"
                "<i>Nada foi perdido — XP, saldo, casorios e conquistas "
                "seguem aqui.</i>",
                status="RESTAURADO", status_color="ACID"),
        )
    except Exception:
        logger.exception("[MIGRATE] aviso no grupo falhou chat=%d", new_id)

    if OWNER_USER_ID and bot is not None:
        try:
            await bot.send_message(
                OWNER_USER_ID,
                term_block(
                    "MIGRACAO",
                    f">> <code>{old_id}</code> -> <code>{new_id}</code>\n"
                    f"// {total} registros: {html.escape(str(moved))}",
                    status="OK", status_color="CYAN"))
        except Exception:
            logger.exception("[MIGRATE] DM owner falhou")


@router.message(F.dice)
async def lucky_emoji_handler(message: Message):
    """🎰 Emoji da Sorte: quando um USER manda o slot 🎰 no grupo e tira uma
    TRINCA (3 iguais), premia XP+florins (jackpot 7️⃣7️⃣7️⃣ em dobro) e o bot
    reage com 🎉. Cap diário LUCKY_DAILY_CAP/user/chat (só vitórias contam,
    anti-farm). Espera ~2s (animação do slot parar) antes de comemorar p/ não
    dar spoiler. NÃO floda: vitória = só a reaction 🎉; jackpot = reaction + 1
    ack efêmero (auto-delete). O 🎰 COSMÉTICO que o BOT manda no baú não
    dispara isto (bot não recebe os próprios updates; user.is_bot é filtrado
    por garantia). Registrado ANTES do catch-all `track` — senão o track
    (que casa msg sem texto) engoliria o dice e pararia a propagação."""
    try:
        if message.chat.type not in {"group", "supergroup"}:
            return
        d = message.dice
        if not d or d.emoji != LUCKY_EMOJI:
            return
        user = message.from_user
        if not user or user.is_bot:
            return
        chat_id = message.chat.id
        uid = user.id
        if is_chat_muted(chat_id):
            return
        xp, gold, jackpot = lucky_reward_for(d.value)
        if xp <= 0:
            return  # não foi trinca — silencioso (sem flood)
        # cap diário anti-farm: SELECT+INSERT sem await no meio (single-thread
        # asyncio → sem corrida, mesmo padrão de on_message_reaction).
        day = today_key()
        row = cur.execute(
            "SELECT count FROM lucky_emoji_daily "
            "WHERE chat_id=? AND user_id=? AND day=?",
            (chat_id, uid, day)).fetchone()
        if (row["count"] if row else 0) >= LUCKY_DAILY_CAP:
            return
        ensure_player(chat_id, uid)
        cur.execute(
            "INSERT INTO lucky_emoji_daily (chat_id, user_id, day, count) "
            "VALUES (?, ?, ?, 1) "
            "ON CONFLICT(chat_id, user_id, day) DO UPDATE SET count=count+1",
            (chat_id, uid, day))
        cur.execute(
            "UPDATE players SET gold=gold+? WHERE chat_id=? AND user_id=?",
            (gold, chat_id, uid))
        db.commit()
        award_xp_immediate(chat_id, uid, xp, reason="lucky")
        # espera a animação do slot parar (~2s) antes de comemorar (sem spoiler)
        await asyncio.sleep(2.0)
        await react_to(chat_id, message.message_id, "🎉")
        if jackpot:
            ack = await safe_send(
                chat_id,
                term_block(
                    "JACKPOT",
                    f">> {mention(uid, get_name(chat_id, uid))} tirou "
                    f"<b>7️⃣7️⃣7️⃣</b> no 🎰!\n"
                    f"// prêmio: <b>+{xp} XP +{format_br(gold)}🪙</b>",
                    status="SORTE GRANDE", status_color="GOLD"))
            if ack:
                asyncio.create_task(auto_delete_after(ack, 30.0))
    except Exception:
        logger.exception("lucky_emoji_handler failed")


@router.message(
    F.chat.type.in_({"group", "supergroup"}),
    # NAO casar comandos: este catch-all roda ANTES de alguns
    # @router.message(Command(...)) (royalpresentear/conquistas/
    # missoes/evento). Em aiogram o 1o handler que casa vence e PARA a
    # propagacao — sem este filtro, esses comandos nunca rodavam em grupo.
    ~(F.text & F.text.startswith("/")),
)
async def track(message: Message):
    if not message.from_user:
        return
    # Bots NUNCA viram jogadores. Admins anonimos chegam como @GroupAnonymousBot
    # (is_bot=True) e ate aqui eram cadastrados na corte por engano. Posts de
    # canal/bot tambem caem aqui. Filtra na entrada do catch-all.
    if message.from_user.is_bot:
        return
    if message.text and message.text.startswith("/"):
        return
    chat_id = message.chat.id
    uid = message.from_user.id
    name = display_name(message)
    username = message.from_user.username
    ensure_chat(chat_id, message.chat.title)
    upsert_user(chat_id, uid, name, username)
    activity_buffer[(chat_id, uid, today_key())] += 1

    # RPG: garante player + XP por mensagem
    try:
        ensure_player(chat_id, uid)
        is_reply = bool(
            message.reply_to_message and message.reply_to_message.from_user
            and not message.reply_to_message.from_user.is_bot
            and message.reply_to_message.from_user.id != uid
        )
        award_xp_message(chat_id, uid, is_reply=is_reply)
        # M05: quest diaria de mensagens
        quest_bump(chat_id, uid, "message")
    except Exception:
        logger.exception("RPG XP failed")

    # PALAVRA: checa desafio ativo
    try:
        ch = get_active_challenge(chat_id)
        if ch and message.text:
            await handle_palavra_attempt(message, ch)
    except Exception:
        logger.exception("palavra attempt failed")

    # SHIPPER: afinidade
    now_ts = utc_now().timestamp()
    if message.reply_to_message and message.reply_to_message.from_user and not message.reply_to_message.from_user.is_bot:
        target = message.reply_to_message.from_user.id
        if target != uid:
            u1, u2 = normalize_pair(uid, target)
            pair_buffer[(chat_id, u1, u2)] += 6

    if message.entities:
        for entity in message.entities:
            if entity.type == "text_mention" and entity.user and entity.user.id != uid and not entity.user.is_bot:
                u1, u2 = normalize_pair(uid, entity.user.id)
                pair_buffer[(chat_id, u1, u2)] += 4

    for previous_uid, previous_ts in list(recent_messages[chat_id]):
        if previous_uid != uid and now_ts - previous_ts <= RECENT_WINDOW_SECONDS:
            u1, u2 = normalize_pair(uid, previous_uid)
            pair_buffer[(chat_id, u1, u2)] += 1
    recent_messages[chat_id].append((uid, now_ts))


@router.message_reaction()
async def on_message_reaction(event: MessageReactionUpdated):
    """Premia XP quando o user ADICIONA uma reaction numa msg de grupo.
    Cap diario REACTION_XP_DAILY_CAP por user/chat. Conta tambem pra
    quest 'social' (M05)."""
    try:
        if event.chat.type not in {"group", "supergroup"}:
            return
        user = event.user
        if not user or user.is_bot:
            return
        # so premia quando ADICIONA reaction (new > old)
        if len(event.new_reaction or []) <= len(event.old_reaction or []):
            return
        chat_id = event.chat.id
        uid = user.id
        if is_chat_muted(chat_id):
            return
        day = today_key()
        row = cur.execute(
            "SELECT count FROM reaction_xp_daily "
            "WHERE chat_id=? AND user_id=? AND day=?",
            (chat_id, uid, day)).fetchone()
        cnt = row["count"] if row else 0
        if cnt >= REACTION_XP_DAILY_CAP:
            return
        ensure_player(chat_id, uid)
        cur.execute(
            "INSERT INTO reaction_xp_daily (chat_id, user_id, day, count) "
            "VALUES (?, ?, ?, 1) "
            "ON CONFLICT(chat_id, user_id, day) "
            "DO UPDATE SET count=count+1",
            (chat_id, uid, day))
        db.commit()
        award_xp_immediate(chat_id, uid, REACTION_XP, reason="reaction")
        quest_bump(chat_id, uid, "reaction")
    except Exception:
        logger.exception("on_message_reaction failed")


