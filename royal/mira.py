"""Inteligência royal — ponte bot↔bot com a IA @Mira.

A @Mira é um bot SEPARADO que o dono mantém: a IA (cara/paga ou não) roda do
LADO DELA. O jogo (este bot) apenas SOLICITA conteúdo pela ponte e INGERE a
resposta → custo zero, nenhuma IA paga dentro do jogo. Requer o "Bot-to-Bot
Communication Mode" LIGADO no @BotFather (ação do dono) — sem isso o Telegram
não entrega as mensagens da @Mira ao jogo.

Objetivo 1: "palavras do dia" — 1x/dia o jogo pede N palavras à @Mira no
grupo-ponte; a resposta é parseada e gravada no banco dinâmico (palavra_pool,
em core.pool_ingest_words) que enriquece o mini-game Palavra da Hora.

Protocolo (matriz de captura): ask_mira() manda "@Mira <pedido>" no grupo-ponte
e cria um Future; o handler de captura (royal/handlers/inteligencia.py) chama
on_mira_reply() quando chega a msg da @Mira e resolve o Future. parse_words()
é TOLERANTE ao formato (vírgula / linha / numeração / frase).

Grafo de deps: config ← core ← mira ← {jobs, handlers}. mira NÃO é importada por
core (evita ciclo).
"""
import asyncio
import re

from royal.config import (IA_BRIDGE_CHAT_ID, MIRA_ENABLED, MIRA_PALAVRAS_COUNT,
                          MIRA_REQUEST_TIMEOUT_SEC, MIRA_USERNAME, logger)
from royal.core import bot, normalize_word, pool_ingest_words, safe_send

# 1 pedido por vez por grupo-ponte: bridge_chat_id -> {future, request_mid}
_pending: dict = {}


async def ask_mira(prompt: str, timeout: float | None = None) -> str | None:
    """Manda um pedido à @Mira no grupo-ponte e espera a resposta dela (msg de
    bot capturada por on_mira_reply). Retorna o texto da resposta ou None (ponte
    off / sem resposta no tempo / erro)."""
    if not MIRA_ENABLED or bot is None or IA_BRIDGE_CHAT_ID is None:
        return None
    if timeout is None:
        timeout = MIRA_REQUEST_TIMEOUT_SEC
    # cancela pedido anterior pendente (evita travar a fila)
    old = _pending.pop(IA_BRIDGE_CHAT_ID, None)
    if old and not old["future"].done():
        old["future"].cancel()
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    text = f"@{MIRA_USERNAME} {prompt}".strip()
    try:
        msg = await safe_send(IA_BRIDGE_CHAT_ID, text)
    except Exception:
        logger.exception("[MIRA] falha ao enviar pedido")
        return None
    if msg is None:
        logger.warning("[MIRA] envio do pedido não retornou mensagem")
        return None
    _pending[IA_BRIDGE_CHAT_ID] = {
        "future": fut,
        "request_mid": getattr(msg, "message_id", None),
    }
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("[MIRA] timeout (%ss) esperando resposta", timeout)
        return None
    except asyncio.CancelledError:
        return None
    finally:
        cur_p = _pending.get(IA_BRIDGE_CHAT_ID)
        if cur_p and cur_p["future"] is fut:
            _pending.pop(IA_BRIDGE_CHAT_ID, None)


def on_mira_reply(message) -> bool:
    """Chamado pelo handler de captura quando chega uma msg de BOT no
    grupo-ponte. Se for a @Mira (username confere quando configurado) e houver
    pedido pendente, resolve o Future com o texto. Retorna True se consumiu.
    NÃO cadastra a @Mira como jogador (o handler para a propagação aqui)."""
    if IA_BRIDGE_CHAT_ID is None:
        return False
    if not message or not message.from_user or not message.from_user.is_bot:
        return False
    if message.chat is None or message.chat.id != IA_BRIDGE_CHAT_ID:
        return False
    uname = message.from_user.username or ""
    if MIRA_USERNAME and uname.lower() != MIRA_USERNAME.lower():
        return False
    pend = _pending.get(IA_BRIDGE_CHAT_ID)
    if not pend or pend["future"].done():
        return False
    # Correlação: se a @Mira respondeu CITANDO uma msg (reply_to), ela tem que
    # ser o NOSSO pedido — assim uma msg solta dela (ou reply a outra coisa) não
    # resolve o Future errado. Se ela não citar nada (só posta a resposta),
    # aceitamos (fallback: no grupo-ponte dedicado, com 1 pedido por vez, a única
    # msg dela enquanto há pedido pendente é a resposta).
    req_mid = pend.get("request_mid")
    reply_to = getattr(message, "reply_to_message", None)
    if reply_to is not None and req_mid is not None:
        if getattr(reply_to, "message_id", None) != req_mid:
            return False
    text = (getattr(message, "text", None) or getattr(message, "caption", None) or "")
    if not text.strip():
        return False
    pend["future"].set_result(text)
    return True


# Tokens só-letras (3-16, aceita acento), usados na extração tolerante.
_WORD_RE = re.compile(r"[A-Za-zÀ-ÿ]{3,16}")
# Itens de lista (vírgula, ponto-e-vírgula, quebra de linha, bullets).
_ITEM_SPLIT = re.compile(r"[,\n;•\u2022|/]+")
# Lixo no início do item (numeração "1.", "-", "*", espaços).
_LEAD_JUNK = re.compile(r"^[\W\d_]+", re.UNICODE)
# Um item válido = exatamente UMA palavra só-letras.
_SINGLE_WORD = re.compile(r"^[A-Za-zÀ-ÿ]{3,16}$")
# Stopwords PT (forma normalizada) p/ o fallback por espaços — tira frases tipo
# "aqui estão as palavras do dia".
_STOPWORDS_NORM = {
    normalize_word(w) for w in (
        "aqui", "estao", "está", "estão", "segue", "seguem", "lista", "as",
        "os", "uma", "uns", "umas", "palavra", "palavras", "dia", "hoje", "para",
        "por", "com", "sem", "que", "dos", "das", "del", "the", "and", "claro",
        "aqui", "voce", "você", "sao", "são",
    )
}


def parse_words(text: str, limit: int = 300) -> list[str]:
    """Extrai palavras candidatas da resposta livre da @Mira. TOLERANTE ao
    formato: 1º tenta itens separados por vírgula/linha/numeração (cada item =
    1 palavra); se vier pouco (resposta em frase/só espaços), cai num fallback
    por tokens filtrando stopwords. Dedup por forma normalizada, ordem
    preservada. Devolve as palavras RAW (com acento/caixa originais)."""
    if not text:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for chunk in _ITEM_SPLIT.split(text):
        item = _LEAD_JUNK.sub("", chunk.strip()).strip(" .:-–—\t")
        if not _SINGLE_WORD.match(item):
            continue
        norm = normalize_word(item)
        if len(norm) < 3 or norm in seen:
            continue
        seen.add(norm)
        out.append(item)
        if len(out) >= limit:
            return out
    if len(out) < 5:
        for tok in _WORD_RE.findall(text):
            norm = normalize_word(tok)
            if len(norm) < 3 or norm in seen or norm in _STOPWORDS_NORM:
                continue
            seen.add(norm)
            out.append(tok)
            if len(out) >= limit:
                break
    return out


async def fetch_and_store_palavras(count: int | None = None) -> int:
    """Pede as "palavras do dia" à @Mira, parseia e grava no banco dinâmico.
    Retorna quantas palavras VÁLIDAS foram recebidas (0 se a ponte está off, a
    @Mira não respondeu ou nada válido veio). A dedup contra o banco/lista fixa
    é feita em pool_ingest_words (logado à parte)."""
    if not MIRA_ENABLED:
        return 0
    if count is None:
        count = MIRA_PALAVRAS_COUNT
    prompt = (
        f"PALAVRAS DO DIA: envie {count} palavras em português do Brasil para um "
        f"jogo de adivinhação (substantivos comuns, 3 a 14 letras, sem nomes "
        f"próprios). Responda APENAS as palavras separadas por vírgula, sem "
        f"numeração e sem frases."
    )
    reply = await ask_mira(prompt)
    if not reply:
        logger.warning("[MIRA] sem resposta para 'palavras do dia'")
        return 0
    words = parse_words(reply, limit=max(count * 2, 50))
    if not words:
        logger.warning("[MIRA] resposta sem palavras válidas: %r", reply[:200])
        return 0
    n_new = pool_ingest_words(words, source="mira")
    logger.info("[MIRA] palavras do dia: %d recebidas, %d novas no pool",
                len(words), n_new)
    return len(words)
