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

from royal.config import (OWNER_USER_ID, STASH_CHAT_ID, _log_ring, logger)
from royal.core import (BACKUP_DIR, BACKUP_RETENTION_DAYS, GROUP_ONLY_MSG, auto_delete_after, bot, cur, current_season_label, dp, dump_logs_to_gist, dump_logs_to_file, get_active_challenge, is_chat_muted, is_group, run_backup, safe_typing, set_chat_muted, spawn_palavra, term_block)

router = Router()

@router.message(Command("royalpalavratest"))
async def royal_palavra_test(message: Message):
    if not is_group(message):
        await message.answer(GROUP_ONLY_MSG)
        return
    uid = message.from_user.id if message.from_user else 0
    if OWNER_USER_ID is None or uid != OWNER_USER_ID:
        return
    existing = get_active_challenge(message.chat.id)
    if existing:
        ack = await message.answer(term_block(
            "PALAVRA", ">> <b>já existe desafio ativo</b>\n"
            f"<i>// /royalpalavra mostra o atual (id={existing['id']}).</i>",
            status="OCUPADO", status_color="AMBER"))
        if ack:
            await auto_delete_after(ack, delay=8.0)
        return
    await safe_typing(message.chat.id, "upload_photo")
    await spawn_palavra(message.chat.id)


@router.message(Command("royalbackup"))
async def royal_backup(message: Message):
    """Backup manual do DB. Owner-only, off-menu, qualquer chat.
    Gera VACUUM INTO + sobe pro STASH e reporta ultimo backup."""
    uid = message.from_user.id if message.from_user else 0
    if OWNER_USER_ID is None or uid != OWNER_USER_ID:
        return
    in_group = is_group(message)
    res = await run_backup(upload=True)
    if not res:
        ack = await message.answer(term_block(
            "BACKUP", "!! falha ao gerar backup — ver logs.",
            status="ERRO", status_color="HOT"))
        if ack and in_group:
            await auto_delete_after(ack, delay=12.0)
        return
    dest, size = res
    # lista backups existentes p/ visao de retencao
    try:
        files = sorted(fn for fn in os.listdir(BACKUP_DIR)
                       if fn.endswith(".sqlite3"))
    except OSError:
        files = []
    lines = [
        f">> arquivo: <code>{os.path.basename(dest)}</code>",
        f">> tamanho: <b>{size // 1024} KB</b>",
        f">> stash: <b>{'ok' if STASH_CHAT_ID else 'off (sem STASH)'}</b>",
        f"// retencao: {len(files)} backups ({BACKUP_RETENTION_DAYS}d)",
    ]
    ack = await message.answer(term_block(
        "BACKUP", "\n".join(lines), status="OK", status_color="ACID",
        stamp=current_season_label()))
    if ack and in_group:
        await auto_delete_after(ack, delay=12.0)


@router.message(Command("royallog"))
async def royal_log(message: Message):
    """Dump imediato dos logs. Owner-only, qualquer chat, off-menu.
    Grava o snapshot em backup/logs/ (NAO manda mais o log na DM) + atualiza
    gist se GH_TOKEN setado. Alertas de erro relevante chegam por DM separada."""
    uid = message.from_user.id if message.from_user else 0
    if OWNER_USER_ID is None or uid != OWNER_USER_ID:
        return
    path = dump_logs_to_file()
    gist_url = await dump_logs_to_gist()
    in_group = is_group(message)
    fname = os.path.basename(path) if path else None
    lines = [
        f">> arquivo: <b>{fname or 'vazio/falhou'}</b>",
        f">> gist: <b>{'ok' if gist_url else 'off (sem GH_TOKEN)'}</b>",
        f"// buffer: {len(_log_ring.buffer)} linhas",
    ]
    # NUNCA expor URL do gist em grupo (blast radius — qualquer membro veria
    # o link nos 12s antes do auto-delete). URL só no DM 1:1 do owner.
    if gist_url and not in_group:
        lines.append(f'// <a href="{gist_url}">abrir gist</a>')
    ack = await message.answer(term_block(
        "LOG.SYS", "\n".join(lines), status="DUMP", status_color="CYAN"))
    if ack and in_group:
        await auto_delete_after(ack, delay=12.0)
        # Manda URL privadamente pro owner pra ele acessar sem expor no grupo.
        if gist_url and bot is not None:
            try:
                await bot.send_message(
                    OWNER_USER_ID,
                    term_block("LOG.SYS",
                               f'// <a href="{gist_url}">abrir gist</a>',
                               status="LINK", status_color="CYAN"),
                    disable_notification=True)
            except Exception:
                logger.exception("[LOGS] envio do gist url pro DM falhou")


@router.message(Command("royalmudo"))
async def royal_mudo(message: Message):
    """Toggle mute. Owner-only, NAO aparece no menu.

    - Em GRUPO: toggle do chat atual.
    - Em DM do owner: master switch — se algum grupo estiver ON-AIR,
      silencia TODOS; se todos ja estiverem silenciados, religa TODOS.

    Quando ON: bot pula auto-casorios, auto-palavra, auto-boss, auto-chest
    e anuncios one-shot. Comandos manuais continuam respondendo."""
    uid = message.from_user.id if message.from_user else 0
    if OWNER_USER_ID is None or uid != OWNER_USER_ID:
        return

    # === DM owner: aplica em TODOS os grupos conhecidos ===
    if message.chat.type == "private":
        cur.execute("SELECT chat_id, COALESCE(muted, 0) AS muted FROM chats_rpg")
        rows = cur.fetchall()
        if not rows:
            await message.answer(term_block(
                "MUDO.SYS",
                ">> <b>SEM GRUPOS</b>\n<i>// nenhum chat ativo no DB.</i>",
                status="VAZIO", status_color="AMBER"))
            return
        any_unmuted = any(int(r["muted"] or 0) == 0 for r in rows)
        target_state = True if any_unmuted else False  # ON se sobrar algum ativo
        changed = 0
        for r in rows:
            chat_id = int(r["chat_id"])
            if bool(int(r["muted"] or 0)) != target_state:
                set_chat_muted(chat_id, target_state)
                changed += 1
        logger.info("[MUDO] BROADCAST actor=%d new_state=%s total=%d changed=%d",
                    uid, "ON" if target_state else "OFF", len(rows), changed)
        if target_state:
            body = (f">> <b>SILENCIADO GLOBAL</b>\n"
                    f"// {len(rows)} grupos atingidos ({changed} mudados)\n"
                    f"<i>// auto-posts pausados em todos os grupos.</i>\n"
                    f"<i>// /royalmudo de novo na DM pra religar todos.</i>")
            status, color = "MUDO-ALL", "AMBER"
        else:
            body = (f">> <b>NORMALIZADO GLOBAL</b>\n"
                    f"// {len(rows)} grupos religados ({changed} mudados)\n"
                    f"<i>// auto-posts religados em todos os grupos.</i>")
            status, color = "ON-AIR-ALL", "ACID"
        await message.answer(term_block("MUDO.SYS", body,
                                        status=status, status_color=color))
        return

    # === GRUPO: toggle local ===
    if not is_group(message):
        return
    chat_id = message.chat.id
    new_state = not is_chat_muted(chat_id)
    set_chat_muted(chat_id, new_state)
    logger.info("[MUDO] chat=%d new_state=%s actor=%d",
                chat_id, "ON" if new_state else "OFF", uid)
    if new_state:
        body = (">> <b>SILENCIADO</b>\n"
                "<i>// auto-posts pausados (casorio/palavra/boss/chest/anuncios).</i>\n"
                "<i>// comandos manuais continuam ativos.</i>\n"
                "<i>// /royalmudo de novo pra desligar.</i>")
        status, color = "MUDO", "AMBER"
    else:
        body = (">> <b>NORMALIZADO</b>\n"
                "<i>// auto-posts religados.</i>\n"
                "<i>// proxima palavra/casorio segue o agendamento normal.</i>")
        status, color = "ON-AIR", "ACID"
    ack = await message.answer(term_block("MUDO.SYS", body,
                                          status=status, status_color=color))
    if ack:
        await auto_delete_after(ack, delay=12.0)


