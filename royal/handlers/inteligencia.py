"""Captura das respostas da @Mira no grupo-ponte (inteligência royal).

A @Mira é OUTRO bot; suas mensagens são `is_bot=True`. O catch-all `track` (em
system.py) DESCARTA mensagens de bot — por isso este router precisa ser incluído
ANTES de `system` em main.py (o 1º handler que casa vence e PARA a propagação).

Só casa no grupo-ponte (IA_BRIDGE_CHAT_ID) e só quando a ponte está ligada
(_BRIDGE_ID=0 = off → o filtro nunca casa um chat real). NÃO cadastra a @Mira
como jogador: apenas resolve o pedido pendente via mira.on_mira_reply().
"""
from aiogram import F, Router
from aiogram.types import Message

from royal.config import IA_BRIDGE_CHAT_ID, MIRA_ENABLED, logger
from royal.mira import on_mira_reply

router = Router()

# Sentinela: ponte off → 0 (nunca casa um chat real do Telegram).
_BRIDGE_ID = IA_BRIDGE_CHAT_ID if (MIRA_ENABLED and IA_BRIDGE_CHAT_ID is not None) else 0


@router.message(F.chat.id == _BRIDGE_ID, F.from_user.is_bot)
async def capture_mira_message(message: Message):
    try:
        on_mira_reply(message)
    except Exception:
        logger.exception("[MIRA] capture falhou")
