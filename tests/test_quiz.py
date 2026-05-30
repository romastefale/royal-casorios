"""Testes do Quiz Real (/rquiz) — só lógica pura/sem rede.

Cobrem o parser tolerante das perguntas vindas da @Mira (parse_quiz_questions)
e o cálculo do top-N final (quiz_top). A ponte de rede (fetch_quiz_questions) e
o fluxo de enquetes exigem runtime do aiogram/Telegram e não são exercitados.
"""
import main  # noqa: F401  (conftest aponta DATABASE_PATH p/ tmp antes do import)
from royal.mira import QuizQuestion, parse_quiz_questions
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
