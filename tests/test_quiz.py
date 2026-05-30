"""Testes do Quiz Real (/rquiz) — só lógica pura/sem rede.

Cobrem o parser tolerante das perguntas vindas da @Mira (parse_quiz_questions)
e o cálculo do top-N final (quiz_top). A ponte de rede (fetch_quiz_questions) e
o fluxo de enquetes exigem runtime do aiogram/Telegram e não são exercitados.
"""
import asyncio

import main  # noqa: F401  (conftest aponta DATABASE_PATH p/ tmp antes do import)
from aiogram.types import PollAnswer, User
from royal.mira import QuizQuestion, parse_quiz_questions
from royal.handlers import quiz as quizmod
from royal.handlers.quiz import quiz_top


_GOOD = """\
1) Qual a capital do Brasil?
A) São Paulo
B) Rio de Janeiro
C) Brasília
D) Salvador
GABARITO: C

2) Quanto é 2 + 2?
A) 3
B) 4
C) 5
D) 22
GABARITO: B
"""


def test_parse_formato_bom():
    qs = parse_quiz_questions(_GOOD, want=10)
    assert len(qs) == 2
    assert isinstance(qs[0], QuizQuestion)
    assert qs[0].question == "Qual a capital do Brasil?"
    assert qs[0].options == ("São Paulo", "Rio de Janeiro", "Brasília", "Salvador")
    assert qs[0].correct == 2          # C
    assert qs[1].correct == 1          # B
    assert qs[1].options[qs[1].correct] == "4"


def test_parse_tolerante_variacoes():
    text = (
        "Pergunta 1 - Cor do céu?\n"
        "a. Verde\n"
        "b. Azul\n"
        "Resposta: B\n"
        "P: Animal que late?\n"
        "(A) Gato\n"
        "(B) Cachorro\n"
        "(C) Peixe\n"
        "correta - b\n"
    )
    qs = parse_quiz_questions(text, want=10)
    assert len(qs) == 2
    assert qs[0].options == ("Verde", "Azul")
    assert qs[0].correct == 1
    assert qs[1].question == "Animal que late?"
    assert qs[1].options[qs[1].correct] == "Cachorro"


def test_parse_descarta_bloco_sem_gabarito():
    text = (
        "1) Sem gabarito aqui?\n"
        "A) um\n"
        "B) dois\n"
        "C) tres\n"
        "2) Com gabarito?\n"
        "A) sim\n"
        "B) nao\n"
        "GABARITO: A\n"
    )
    qs = parse_quiz_questions(text, want=10)
    assert len(qs) == 1
    assert qs[0].question == "Com gabarito?"
    assert qs[0].correct == 0


def test_parse_respeita_want_e_vazio():
    assert parse_quiz_questions("", want=5) == []
    assert parse_quiz_questions("texto solto sem formato", want=5) == []
    qs = parse_quiz_questions(_GOOD, want=1)
    assert len(qs) == 1


def test_parse_gabarito_fora_do_range_e_truncagem():
    # Gabarito 'E' não casa A-D → bloco descartado (não cria índice inválido).
    bad = "1) X?\nA) a\nB) b\nGABARITO: E\n"
    assert parse_quiz_questions(bad, want=5) == []
    # Pergunta gigante é truncada pro limite do Telegram (300).
    long_q = "1) " + ("z" * 500) + "\nA) a\nB) b\nGABARITO: A\n"
    qs = parse_quiz_questions(long_q, want=5)
    assert len(qs) == 1
    assert len(qs[0].question) <= 300


def test_quiz_top_ordena_e_desempata():
    scores = {10: 3, 20: 5, 30: 3, 40: 0}
    top = quiz_top(scores, n=3)
    assert top == [(20, 5), (10, 3), (30, 3)]  # pts desc, depois uid asc
    assert quiz_top({}, n=5) == []
    assert len(quiz_top(scores, n=2)) == 2


def _answer(poll_id: str, uid: int, options: list[int]) -> PollAnswer:
    return PollAnswer(
        poll_id=poll_id,
        user=User(id=uid, is_bot=False, first_name=f"U{uid}"),
        option_ids=options,
        option_persistent_ids=[str(o) for o in options],
    )


def _run_answer(pa: PollAnswer) -> None:
    asyncio.run(quizmod.rq_poll_answer(pa))


def test_poll_answer_idempotente_e_so_inscritos(monkeypatch):
    chat_id, poll_id = -1000, "pollX"
    sess = quizmod.QuizSession(
        chat_id=chat_id, admin_id=1, theme="t", count=5, state="running")
    sess.participants = {100, 200}
    monkeypatch.setitem(quizmod._sessions, chat_id, sess)
    monkeypatch.setitem(
        quizmod._polls, poll_id,
        {"chat_id": chat_id, "correct": 1, "answered": set()})

    # Inscrito acerta → +1. Telegram reenvia o MESMO voto → continua 1.
    _run_answer(_answer(poll_id, 100, [1]))
    _run_answer(_answer(poll_id, 100, [1]))
    assert sess.scores.get(100) == 1

    # Inscrito erra → não pontua (mas fica marcado como respondido).
    _run_answer(_answer(poll_id, 200, [0]))
    assert sess.scores.get(200, 0) == 0
    # Mesmo trocando depois p/ a correta, não reabre a pontuação da pergunta.
    _run_answer(_answer(poll_id, 200, [1]))
    assert sess.scores.get(200, 0) == 0

    # Não-inscrito é ignorado.
    _run_answer(_answer(poll_id, 999, [1]))
    assert 999 not in sess.scores


def test_cleanup_apaga_enquetes_e_lobby(monkeypatch):
    deleted: list[tuple[int, int]] = []

    class _FakeBot:
        async def delete_message(self, chat_id, message_id):
            deleted.append((chat_id, message_id))

    sess = quizmod.QuizSession(
        chat_id=-77, admin_id=1, theme="t", count=5, state="done")
    sess.poll_mids = [11, 12, 13]
    sess.lobby_mid = 10
    monkeypatch.setattr(quizmod, "bot", _FakeBot())
    asyncio.run(quizmod._cleanup_quiz_messages(sess))
    assert deleted == [(-77, 11), (-77, 12), (-77, 13), (-77, 10)]


def test_cleanup_tolera_falha_e_bot_none(monkeypatch):
    class _BoomBot:
        async def delete_message(self, chat_id, message_id):
            raise RuntimeError("message can't be deleted")

    sess = quizmod.QuizSession(
        chat_id=-88, admin_id=1, theme="t", count=5, state="done")
    sess.poll_mids = [1, 2]
    sess.lobby_mid = None
    monkeypatch.setattr(quizmod, "bot", _BoomBot())
    asyncio.run(quizmod._cleanup_quiz_messages(sess))   # não levanta

    monkeypatch.setattr(quizmod, "bot", None)
    asyncio.run(quizmod._cleanup_quiz_messages(sess))   # no-op


def test_poll_answer_ignora_quiz_nao_rodando(monkeypatch):
    chat_id, poll_id = -2000, "pollY"
    sess = quizmod.QuizSession(
        chat_id=chat_id, admin_id=1, theme="t", count=5, state="done")
    sess.participants = {100}
    monkeypatch.setitem(quizmod._sessions, chat_id, sess)
    monkeypatch.setitem(
        quizmod._polls, poll_id,
        {"chat_id": chat_id, "correct": 0, "answered": set()})
    _run_answer(_answer(poll_id, 100, [0]))
    assert sess.scores == {}
    # poll_id desconhecido → no-op silencioso.
    _run_answer(_answer("inexistente", 100, [0]))
    assert sess.scores == {}
