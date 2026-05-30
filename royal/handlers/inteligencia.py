"""Captura das respostas da @Mira no grupo-ponte (inteligência royal).

A @Mira é OUTRO bot; suas mensagens são `is_bot=True`. O catch-all `track` (em
system.py) DESCARTA mensagens de bot — por isso este router precisa ser incluído
ANTES de `system` em main.py (o 1º handler que casa vence e PARA a propagação).

Só casa no grupo-ponte (IA_BRIDGE_CHAT_ID) e só quando a ponte está ligada
(_BRIDGE_ID=0 = off → o filtro nunca casa um chat real). NÃO cadastra a @Mira
como jogador: apenas resolve o pedido pendente via mira.on_mira_reply().
"""
from aiogram import F, Router
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.types import Message

from royal.config import IA_BRIDGE_CHAT_ID, MIRA_ENABLED, logger
from royal.mira import on_mira_reply

router = Router()

# Sentinela: ponte off → 0 (nunca casa um chat real do Telegram).
_BRIDGE_ID = IA_BRIDGE_CHAT_ID if (MIRA_ENABLED and IA_BRIDGE_CHAT_ID is not None) else 0


@router.message(F.chat.id == _BRIDGE_ID)
async def capture_mira_message(message: Message):
    # Casa QUALQUER msg no grupo-ponte (não só is_bot): a @Mira pode responder
    # como bot (is_bot=True) OU como userbot (is_bot=False). O match real fica
    # por conta de on_mira_reply (id/username). Log de diagnóstico p/ enxergar o
    # que chega (id/username/is_bot) — chave p/ saber se o Bot-to-Bot Mode está
    # entregando e qual é o id REAL da @Mira.
    #
    # SÓ consome (para a propagação) quando on_mira_reply casa a @Mira. Senão,
    # raise SkipHandler → a msg segue p/ os handlers downstream (comandos do dono
    # no grupo-ponte, ex. /royallog, continuam funcionando).
    consumed = False
    try:
        u = message.from_user
        text = (getattr(message, "text", None)
                or getattr(message, "caption", None) or "")
        logger.info(
            "[MIRA] bridge msg: id=%s username=%s is_bot=%s reply_to=%s text=%r",
            getattr(u, "id", None), getattr(u, "username", None),
            getattr(u, "is_bot", None),
            getattr(getattr(message, "reply_to_message", None),
                    "message_id", None),
            text[:60],
        )
        consumed = on_mira_reply(message)
        if not consumed:
            logger.info("[MIRA] bridge msg NÃO consumida (sem pedido pendente "
                        "ou remetente ≠ @Mira) → segue p/ downstream")
    except Exception:
        logger.exception("[MIRA] capture falhou")
    if not consumed:
        raise SkipHandler
