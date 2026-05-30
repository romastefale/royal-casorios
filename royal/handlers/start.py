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

from royal.core import (EFFECT_PARTY, ROYAL_HELP, current_season_label, dp, effect_kw, private_menu, safe_typing, send_tutorial, term_block, with_close)

router = Router()

@router.message(CommandStart())
async def start_cmd(message: Message):
    if message.chat.type != "private":
        return
    body = (
        "<b>// SISTEMA ROYAL INICIALIZADO</b>\n"
        "<i>A corte te aguardava, nobre. Bem-vindo ao </i>"
        "<b>RPG - Royal para Geeks</b><i>.</i>\n"
        "<blockquote expandable>"
        ">> ROTAS PESSOAIS (aqui na DM)\n"
        "• 👤 /royalperfil — cartao de identidade\n"
        "• 📜 /royalficha — atalho pro perfil\n"
        "• 🏆 /royalranking — top 10 da temporada\n"
        "• 🎒 /royalinventario — seus itens\n"
        "• 💰 /royalsaldo — florins na arca 🪙\n"
        "• 💰 /royalloja — loja de florins 🪙\n"
        "• 📊 /royalmeuscasorios — seu historico\n"
        "• 👑 /royalavatar — escolher avatar\n"
        "• 🏰 /royalgrupo — trocar grupo ativo\n"
        "• 🔒 /royalprivacidade — controles\n"
        "• 📦 /royaldados — exportar / apagar\n"
        "• 🗺️ /royalmissoes — missoes diarias (XP + florins)\n"
        "• 🎉 /royalevento — evento de XP ativo agora\n"
        "• 🏅 /royalconquistas — suas medalhas\n"
        "• ⚙️ /royalconfig — preferencias (silenciar, esconder)\n"
        "• 📖 /royaltutorial — aprender a jogar\n"
        "• ❓ /royalajuda — manual completo"
        "</blockquote>"
        "<blockquote expandable>"
        ">> INLINE — usa em QUALQUER chat\n"
        "Digita <code>@nome_do_bot</code> em qualquer conversa\n"
        "e envia seu cartao de perfil onde quiser ✨"
        "</blockquote>"
        "<i>>> Se voce participa de mais de um grupo Royal,\n"
        "use /royalgrupo pra escolher qual eh o ativo na DM.</i>"
    )
    await message.answer(
        term_block("BOOT", body, status="CONECTADO",
                   stamp=current_season_label()),
        reply_markup=private_menu,
        **effect_kw(message.chat.type, EFFECT_PARTY),
    )
    # Tutorial completo logo em seguida (primeira impressão = ensina o jogo)
    await send_tutorial(message)


@router.message(Command("royalajuda"))
async def royal_ajuda(message: Message):
    # Tutorial primeiro (ensina), depois manual de comandos (referência)
    await safe_typing(message.chat.id)
    await send_tutorial(message)
    uid = message.from_user.id if message.from_user else None
    await message.answer(ROYAL_HELP, reply_markup=with_close(None, uid))


@router.message(Command("help"))
async def help_cmd(message: Message):
    await safe_typing(message.chat.id)
    await send_tutorial(message)
    uid = message.from_user.id if message.from_user else None
    await message.answer(ROYAL_HELP, reply_markup=with_close(None, uid))


@router.message(Command("royaltutorial"))
async def royal_tutorial_cmd(message: Message):
    await send_tutorial(message)


@router.message(F.text == "📖 Tutorial")
async def btn_tutorial(message: Message):
    await send_tutorial(message)


@router.message(F.text == "❓ Como funciona")
async def btn_como_funciona(message: Message):
    body = (
        "<i>O bot observa interações no grupo (respostas, menções, conversa).</i>\n"
        "<blockquote expandable>"
        "💍 <b>CASORIOS</b> — 3x/dia, votação ❤️/🤮\n"
        "🎯 <b>PALAVRA</b> — 60min, primeiro a acertar leva XP+🪙\n"
        "🐉 <b>BOSS</b> — domingo 20h, todos atacam juntos\n"
        "🧠 <b>QUIZ</b> — admin abre /rquiz, grupo responde em enquetes\n"
        "⭐ <b>XP</b> — ganho ao interagir, sobe nível, distribui atributos\n"
        "🏆 <b>TEMPORADAS</b> — seguem as estações do ano"
        "</blockquote>"
        "<i>Use /royal pra acessar o menu completo.</i>"
    )
    await message.answer(term_block(
        "MANUAL", body, status="DOC", status_color="CYAN",
        stamp=current_season_label()))


