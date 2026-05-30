"""Testes da Inteligência royal (ponte @Mira + banco dinâmico de palavras).

Cobrem só lógica pura/DB (sem rede): parse_words tolerante a formato,
pool_ingest_words (dedup vs lista fixa + idempotência) e pick_palavra_word
(anti-repetição recente). A ponte de rede em si (ask_mira/on_mira_reply) exige
runtime do aiogram e não é exercitada aqui.
"""
import asyncio

import main
import royal.mira as mira
from royal_words import PALAVRAS


class _FakeUser:
    def __init__(self, is_bot=True, username=None):
        self.is_bot = is_bot
        self.username = username


class _FakeChat:
    def __init__(self, cid):
        self.id = cid


class _FakeMsg:
    def __init__(self, chat_id, *, is_bot=True, username=None, text="a, b, c",
                 reply_to=None):
        self.from_user = _FakeUser(is_bot, username)
        self.chat = _FakeChat(chat_id)
        self.text = text
        self.caption = None
        self.reply_to_message = reply_to


class _FakeReply:
    def __init__(self, mid):
        self.message_id = mid


def test_parse_words_lista_por_virgula():
    out = main.parse_words("casa, rosa, gato, livro")
    assert out == ["casa", "rosa", "gato", "livro"]


def test_parse_words_lista_numerada():
    txt = "1. casa\n2. rosa\n3. gato"
    assert main.parse_words(txt) == ["casa", "rosa", "gato"]


def test_parse_words_bullets_e_traco():
    txt = "- mesa\n- cadeira\n* janela"
    assert main.parse_words(txt) == ["mesa", "cadeira", "janela"]


def test_parse_words_frase_com_stopwords_fallback():
    # Sem vírgulas: cai no fallback por tokens, filtrando stopwords PT.
    txt = "Aqui estão as palavras do dia: cavalo girassol martelo"
    out = main.parse_words(txt)
    assert "cavalo" in out and "girassol" in out and "martelo" in out
    # stopwords não entram
    for junk in ("aqui", "estao", "estão", "palavras", "dia"):
        assert junk not in [w.lower() for w in out]


def test_parse_words_dedup_e_acento_preservado():
    out = main.parse_words("maçã, MAÇÃ, maca, maçã")
    # "maçã" e "MAÇÃ" e "maca" normalizam igual → 1 só; raw preservado.
    assert out == ["maçã"]


def test_parse_words_vazio():
    assert main.parse_words("") == []
    assert main.parse_words(None) == []


def test_pool_ingest_dedup_contra_lista_fixa_e_idempotente():
    fixa = PALAVRAS[0]  # palavra que já está na lista estática
    novas = ["zlorptn", "qxvbwmn", fixa]
    n1 = main.pool_ingest_words(novas, source="test")
    assert n1 == 2  # a 'fixa' é ignorada (já existe em PALAVRAS)
    # idempotente: reinserir as mesmas → 0 novas
    n2 = main.pool_ingest_words(novas, source="test")
    assert n2 == 0
    rows = main.cur.execute(
        "SELECT word_norm FROM palavra_pool WHERE source='test'").fetchall()
    norms = {r["word_norm"] for r in rows}
    assert "zlorptn" in norms and "qxvbwmn" in norms
    assert main.normalize_word(fixa) not in norms


def test_pick_palavra_word_evita_recente():
    chat_id = -999111222
    # injeta uma palavra única no pool e marca-a como recém-usada nesse chat
    main.pool_ingest_words(["florpusxq"], source="test")
    norm = main.normalize_word("florpusxq")
    main.cur.execute(
        "INSERT INTO challenges (chat_id, word, status) VALUES (?, ?, 'open')",
        (chat_id, norm))
    main.db.commit()
    # como há muitas alternativas (PALAVRAS), a recente NUNCA deve ser sorteada
    picks = {main.normalize_word(main.pick_palavra_word(chat_id)) for _ in range(200)}
    assert norm not in picks
    # e sempre devolve algo válido
    assert all(isinstance(main.pick_palavra_word(chat_id), str) for _ in range(5))


def test_on_mira_reply_correlacao_por_reply_to():
    """Se a @Mira responde CITANDO uma msg, tem que ser o nosso pedido — uma
    msg dela citando outra coisa NÃO resolve o Future errado."""
    bridge = mira.IA_BRIDGE_CHAT_ID
    assert bridge is not None
    loop = asyncio.new_event_loop()
    try:
        fut = loop.create_future()
        mira._pending[bridge] = {"future": fut, "request_mid": 555}
        # reply citando OUTRA msg → ignorado
        assert mira.on_mira_reply(
            _FakeMsg(bridge, reply_to=_FakeReply(999))) is False
        assert not fut.done()
        # reply citando o NOSSO pedido → resolve
        assert mira.on_mira_reply(
            _FakeMsg(bridge, reply_to=_FakeReply(555), text="casa, rosa")) is True
        assert fut.result() == "casa, rosa"
    finally:
        mira._pending.pop(bridge, None)
        loop.close()


def test_on_mira_reply_aceita_sem_reply_to_e_filtra_humano():
    """Sem reply_to (a @Mira só posta a resposta) → aceita. Msg de humano
    (is_bot=False) ou de outro chat → ignorada."""
    bridge = mira.IA_BRIDGE_CHAT_ID
    loop = asyncio.new_event_loop()
    try:
        # humano não resolve
        fut = loop.create_future()
        mira._pending[bridge] = {"future": fut, "request_mid": 1}
        assert mira.on_mira_reply(
            _FakeMsg(bridge, is_bot=False, text="oi")) is False
        assert not fut.done()
        # outro chat não resolve
        assert mira.on_mira_reply(_FakeMsg(bridge + 1, text="x, y")) is False
        assert not fut.done()
        # bot no chat-ponte, sem reply_to → resolve
        assert mira.on_mira_reply(_FakeMsg(bridge, text="gato, mesa")) is True
        assert fut.result() == "gato, mesa"
    finally:
        mira._pending.pop(bridge, None)
        loop.close()
