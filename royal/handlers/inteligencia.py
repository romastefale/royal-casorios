"""Captura das respostas da @Mira no grupo-ponte (inteligência royal).

A @Mira responde como `is_bot=True`. DOIS filtros descartariam isso antes daqui:
(1) a outer-middleware `_require_user_for_commands` (core.py) dropa `is_bot` na
ENTRADA — por isso o grupo-ponte está na ALLOWLIST dela; (2) o catch-all `track`
(system.py) também dropa bot — por isso este router é incluído ANTES de `system`
em main.py (o 1º handler que casa vence e PARA a propagação).

Modelo TR3: casa QUALQUER msg no grupo-ponte (IA_BRIDGE_CHAT_ID) e deixa
on_mira_reply() decidir pelo REPLY ao nosso pedido — SEM checar quem enviou. Só
ativo com a ponte ligada (_BRIDGE_ID=0 = off → o filtro nunca casa um chat real).
NÃO cadastra ninguém como jogador: apenas resolve o pedido pendente.
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
    # Casa QUALQUER msg no grupo-ponte (modelo TR3): o match real é o REPLY ao
    # nosso pedido, feito em on_mira_reply — SEM checar quem enviou. Log de
    # diagnóstico p/ enxergar o que chega (id/username/is_bot/reply_to) — chave
    # p/ confirmar que a resposta está sendo ENTREGUE (passou a allowlist da
    # middleware) e que o reply_to bate com o nosso pedido.
    #
    # SÓ consome (para a propagação) quando on_mira_reply casa o reply. Senão,
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
                        "ou reply_to ≠ nosso pedido) → segue p/ downstream")
    except Exception:
        logger.exception("[MIRA] capture falhou")
    if not consumed:
        raise SkipHandler
