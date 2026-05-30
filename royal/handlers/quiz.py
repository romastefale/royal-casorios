"""Quiz Real — /rquiz (Objetivo 2 da inteligência royal).

Admin inicia um quiz no grupo; as perguntas são geradas pela @Mira pela ponte
bot↔bot (royal/mira.py). O quiz roda como ENQUETES NATIVAS do Telegram (quiz
poll, 4 alternativas, 1 correta); cada inscrito que acerta pontua; no fim sai um
Card (Pillow) com o top-5.

Fluxo:
  /rquiz <tema>  → escolha 5/10 → janela de inscrição (gerando em paralelo) →
  admin "Começar agora" → N quiz polls → Card top-5.
  /rquiz (sem tema) → ForceReply pedindo o tema (admin responde em reply).

UX no grupo (regra do dono): edita SEMPRE a mesma mensagem da janela
(in-place), acks efêmeros via cb.answer, nunca floodar.

ORDEM (CRÍTICO): este router é incluído ANTES de `system` em main.py —
  - o reply do tema é texto comum (não-comando) e o catch-all `track` casaria
    primeiro; o filtro `_is_theme_reply` é PRECISO (só casa o reply pendente),
    então replies não-relacionados seguem pro `track` normalmente;
  - `@router.poll_answer` faz `dp.resolve_used_update_types()` incluir o update
    "poll_answer" automaticamente (só vem porque `is_anonymous=False`).

Estado em memória (worker single-thread asyncio, 1 quiz por grupo):
  _sessions[chat_id] = QuizSession · _polls[poll_id] = {chat_id, correct}.
Sem persistência entre restarts (quiz é efêmero, fora de escopo persistir).
"""
import asyncio
import html
from dataclasses import dataclass, field

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ForceReply,
    InlineKeyboardMarkup,
    InputPollOption,
    Message,
    PollAnswer,
)

from royal.config import MIRA_ENABLED, QUIZ_XP_PER_POINT, logger
from royal.core import (
    BTN_GO,
    BTN_NO,
    BTN_OK,
    STYLE_INFO,
    STYLE_NO,
    STYLE_OK,
    anonize,
    auto_delete_after,
    award_xp_immediate,
    bot,
    cap1024,
    card_safe_name,
    ensure_player,
    get_name,
    ikb,
    is_admin,
    royal_avatars,
    safe_send,
    safe_typing,
    term_block,
)
from royal.mira import fetch_quiz_questions
from royal_render import QuizCardEntry, render_quiz_card

router = Router()

QUIZ_COUNT_OPTIONS = (5, 10)     # quantidades de perguntas oferecidas
QUIZ_POLL_SECONDS = 30           # open_period de cada enquete (Telegram: 5-600)
QUIZ_LOBBY_TIMEOUT = 600         # auto-cancela a janela se ninguém começar
QUIZ_THEME_MAX = 80              # corta o tema do usuário
QUIZ_PODIUM_TTL = 900            # apaga o pódio 15min após o fim do quiz


@dataclass
class QuizSession:
    chat_id: int
    admin_id: int
    theme: str
    count: int
    lobby_mid: int | None = None
    participants: set = field(default_factory=set)   # user_ids inscritos
    scores: dict = field(default_factory=dict)       # user_id -> pontos
    questions: list = field(default_factory=list)
    poll_mids: list = field(default_factory=list)    # message_ids das enquetes
    ready: bool = False
    failed: bool = False
    state: str = "lobby"   # lobby | running | done | cancelled
    gen_task: asyncio.Task | None = None
    run_task: asyncio.Task | None = None


_sessions: dict[int, QuizSession] = {}      # chat_id -> sessão ativa
_polls: dict[str, dict] = {}                # poll_id -> {chat_id, correct, answered:set}
_pending_theme: dict[int, dict] = {}        # chat_id -> {admin_id, mid}
_pending_count: dict[int, dict] = {}        # chat_id -> {admin_id, theme}


def quiz_top(scores: dict, n: int = 5) -> list:
    """Top-N por pontos (desc), desempate estável por user_id (asc)."""
    return sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))[:n]


# ---------------------------------------------------------------------------
# Comando + entradas (tema inline ou via ForceReply)
# ---------------------------------------------------------------------------

@router.message(Command("rquiz"), F.chat.type.in_({"group", "supergroup"}))
async def rquiz_cmd(message: Message, command: CommandObject):
    if not message.from_user:
        return
    if not await is_admin(message):
        m = await message.reply("👑 Só administradores podem iniciar o quiz.")
        await auto_delete_after(m, 8.0)
        return
    if not MIRA_ENABLED:
        m = await message.reply(
            "⚠️ A inteligência royal (@Mira) está desligada — quiz indisponível.")
        await auto_delete_after(m, 10.0)
        return
    chat_id = message.chat.id
    if chat_id in _sessions or chat_id in _pending_count or chat_id in _pending_theme:
        m = await message.reply("⚠️ Já existe um quiz sendo montado neste grupo.")
        await auto_delete_after(m, 8.0)
        return
    admin_id = message.from_user.id
    theme = (command.args or "").strip()
    if theme:
        await _offer_count(message, theme[:QUIZ_THEME_MAX], admin_id)
    else:
        prompt = await message.reply(
            "🧠 <b>Quiz Real</b> — responda <b>a esta mensagem</b> com o "
            "<b>tema</b> do quiz.",
            reply_markup=ForceReply(selective=True))
        _pending_theme[chat_id] = {"admin_id": admin_id, "mid": prompt.message_id}


def _is_theme_reply(message: Message) -> bool:
    """Filtro PRECISO: só casa o reply (do admin que iniciou) à mensagem de
    ForceReply pendente. Assim replies não-relacionados seguem pro `track`."""
    if message.chat is None or message.from_user is None:
        return False
    pend = _pending_theme.get(message.chat.id)
    if not pend:
        return False
    rt = message.reply_to_message
    if rt is None or rt.message_id != pend["mid"]:
        return False
    return message.from_user.id == pend["admin_id"]


@router.message(_is_theme_reply)
async def rquiz_theme_reply(message: Message):
    chat_id = message.chat.id
    pend = _pending_theme.pop(chat_id, None)
    if not pend:
        return
    theme = (message.text or "").strip()
    if not theme:
        m = await message.reply("⚠️ Tema vazio. Tente /rquiz <tema>.")
        await auto_delete_after(m, 8.0)
        return
    if chat_id in _sessions or chat_id in _pending_count:
        return
    await _offer_count(message, theme[:QUIZ_THEME_MAX], pend["admin_id"])


async def _offer_count(message: Message, theme: str, admin_id: int) -> None:
    rows = [[
        ikb(f"{n} perguntas", callback_data=f"rq:n:{n}:{admin_id}",
            style=STYLE_INFO) for n in QUIZ_COUNT_OPTIONS
    ], [ikb(f"{BTN_NO} Cancelar", callback_data=f"rq:x:{admin_id}",
            style=STYLE_NO)]]
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.reply(
        f"🧠 <b>Quiz Real</b> — tema: <b>{html.escape(theme)}</b>\n"
        f"Quantas perguntas?", reply_markup=kb)
    _pending_count[message.chat.id] = {"admin_id": admin_id, "theme": theme}


# ---------------------------------------------------------------------------
# Callbacks: escolha de quantidade, inscrição, começar, cancelar
# ---------------------------------------------------------------------------

@router.callback_query(F.data.startswith("rq:n:"))
async def rq_count_cb(cb: CallbackQuery):
    parts = cb.data.split(":")           # rq : n : <count> : <admin_id>
    count, admin_id = int(parts[2]), int(parts[3])
    if cb.from_user.id != admin_id:
        await cb.answer("Só quem iniciou escolhe.")
        return
    if cb.message is None:
        await cb.answer()
        return
    chat_id = cb.message.chat.id
    pend = _pending_count.pop(chat_id, None)
    if not pend or chat_id in _sessions:
        await cb.answer()
        return
    sess = QuizSession(chat_id=chat_id, admin_id=admin_id,
                       theme=pend["theme"], count=count,
                       lobby_mid=cb.message.message_id)
    _sessions[chat_id] = sess
    await cb.answer()
    await _render_lobby(sess)
    sess.gen_task = asyncio.create_task(_generate(sess))
    asyncio.create_task(_lobby_watchdog(sess))


@router.callback_query(F.data == "rq:j")
async def rq_join_cb(cb: CallbackQuery):
    if cb.message is None:
        await cb.answer()
        return
    sess = _sessions.get(cb.message.chat.id)
    if not sess or sess.state != "lobby":
        await cb.answer("Inscrições encerradas.")
        return
    uid = cb.from_user.id
    if uid in sess.participants:
        await cb.answer("Você já está inscrito! ⚔️")
        return
    ensure_player(sess.chat_id, uid)
    sess.participants.add(uid)
    await cb.answer("Inscrito! 🎮")
    await _render_lobby(sess)


@router.callback_query(F.data.startswith("rq:go:"))
async def rq_start_cb(cb: CallbackQuery):
    admin_id = int(cb.data.split(":")[2])
    if cb.message is None:
        await cb.answer()
        return
    sess = _sessions.get(cb.message.chat.id)
    if not sess or sess.state != "lobby":
        await cb.answer()
        return
    if cb.from_user.id != admin_id:
        await cb.answer("Só quem iniciou pode começar.")
        return
    if sess.failed:
        await cb.answer("A geração falhou. Cancele e tente de novo.",
                        show_alert=True)
        return
    if not sess.ready:
        await cb.answer("⏳ Ainda estou gerando as perguntas…", show_alert=True)
        return
    if not sess.participants:
        await cb.answer("Ninguém se inscreveu ainda!", show_alert=True)
        return
    sess.state = "running"
    await cb.answer("Começando! 🎬")
    sess.run_task = asyncio.create_task(_run_quiz(sess))


@router.callback_query(F.data.startswith("rq:x:"))
async def rq_cancel_cb(cb: CallbackQuery):
    admin_id = int(cb.data.split(":")[2])
    if cb.from_user.id != admin_id:
        await cb.answer("Só quem iniciou pode cancelar.")
        return
    if cb.message is None:
        await cb.answer()
        return
    chat_id = cb.message.chat.id
    _pending_count.pop(chat_id, None)
    sess = _sessions.pop(chat_id, None)
    if sess:
        sess.state = "cancelled"
        if sess.gen_task:
            sess.gen_task.cancel()
    await cb.answer("Quiz cancelado.")
    try:
        await cb.message.edit_text("🚫 Quiz cancelado.")
    except TelegramBadRequest:
        pass
    await auto_delete_after(cb.message, 6.0)


# ---------------------------------------------------------------------------
# Janela de inscrição (editada in-place) + geração via @Mira + watchdog
# ---------------------------------------------------------------------------

async def _render_lobby(sess: QuizSession) -> None:
    if sess.lobby_mid is None or bot is None:
        return
    if sess.failed:
        status = "❌ Falha ao gerar as perguntas. Cancele e tente de novo."
    elif sess.ready:
        status = (f"✅ {len(sess.questions)} perguntas prontas! "
                  f"O admin pode começar.")
    else:
        status = "⏳ Gerando as perguntas com a inteligência royal…"
    text = (
        f"🧠 <b>Quiz Real</b> — tema: <b>{html.escape(sess.theme)}</b>\n"
        f"Perguntas: <b>{sess.count}</b>\n\n"
        f"{status}\n\n"
        f"🎮 Inscritos: <b>{len(sess.participants)}</b>\n"
        f"<i>Toque em Entrar pra competir.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(f"{BTN_OK} Entrar", callback_data="rq:j", style=STYLE_OK)],
        [ikb(f"{BTN_GO} Começar agora", callback_data=f"rq:go:{sess.admin_id}",
             style=STYLE_INFO),
         ikb(f"{BTN_NO} Cancelar", callback_data=f"rq:x:{sess.admin_id}",
             style=STYLE_NO)],
    ])
    try:
        await bot.edit_message_text(text, sess.chat_id, sess.lobby_mid,
                                    reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning("[QUIZ] edit lobby falhou: %s", e)


async def _generate(sess: QuizSession) -> None:
    try:
        qs = await fetch_quiz_questions(sess.theme, sess.count)
    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("[QUIZ] geração falhou")
        qs = []
    if sess.state == "cancelled":
        return
    if not qs:
        sess.failed = True
    else:
        sess.questions = qs
        sess.ready = True
    await _render_lobby(sess)


async def _lobby_watchdog(sess: QuizSession) -> None:
    """Auto-cancela a janela se o admin nunca começar (evita sessão zumbi)."""
    await asyncio.sleep(QUIZ_LOBBY_TIMEOUT)
    if _sessions.get(sess.chat_id) is not sess or sess.state != "lobby":
        return
    sess.state = "cancelled"
    _sessions.pop(sess.chat_id, None)
    if sess.gen_task:
        sess.gen_task.cancel()
    if bot is not None and sess.lobby_mid is not None:
        try:
            await bot.edit_message_text(
                "⌛ Quiz expirado (ninguém começou a tempo).",
                sess.chat_id, sess.lobby_mid)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Rodar o quiz (enquetes nativas) + pontuação (poll_answer) + card final
# ---------------------------------------------------------------------------

async def _run_quiz(sess: QuizSession) -> None:
    chat_id = sess.chat_id
    try:
        if bot is not None and sess.lobby_mid is not None:
            try:
                await bot.edit_message_text(
                    f"🧠 <b>Quiz Real</b> — tema: <b>{html.escape(sess.theme)}"
                    f"</b>\n🎬 Começou! {len(sess.participants)} jogadores · "
                    f"{len(sess.questions)} perguntas.",
                    chat_id, sess.lobby_mid)
            except Exception:
                pass
        total = len(sess.questions)
        for idx, q in enumerate(sess.questions, 1):
            if sess.state != "running" or bot is None:
                break
            question = f"[{idx}/{total}] {q.question}"[:300]
            try:
                msg = await bot.send_poll(
                    chat_id,
                    question=question,
                    options=[InputPollOption(text=o) for o in q.options],
                    type="quiz",
                    correct_option_id=q.correct,
                    is_anonymous=False,
                    open_period=QUIZ_POLL_SECONDS,
                )
            except Exception:
                logger.exception("[QUIZ] send_poll falhou")
                continue
            sess.poll_mids.append(msg.message_id)
            if msg.poll:
                _polls[msg.poll.id] = {
                    "chat_id": chat_id, "correct": q.correct, "answered": set(),
                }
            await asyncio.sleep(QUIZ_POLL_SECONDS + 2)
            if msg.poll:
                _polls.pop(msg.poll.id, None)
        if sess.state == "running":
            sess.state = "done"
            await _send_results(sess)
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("[QUIZ] run falhou")
    finally:
        if _sessions.get(chat_id) is sess:
            _sessions.pop(chat_id, None)


@router.poll_answer()
async def rq_poll_answer(poll_answer: PollAnswer):
    info = _polls.get(poll_answer.poll_id)
    if not info:
        return
    sess = _sessions.get(info["chat_id"])
    if not sess or sess.state != "running":
        return
    user = poll_answer.user
    if user is None:
        return
    uid = user.id
    # Default (documentado): só INSCRITOS pontuam.
    if uid not in sess.participants:
        return
    # Idempotência por pergunta: o Telegram pode emitir vários `poll_answer`
    # pro mesmo (poll, user) à medida que o voto muda — pontuar no MÁXIMO 1x.
    answered = info["answered"]
    if uid in answered:
        return
    answered.add(uid)
    if list(poll_answer.option_ids) == [info["correct"]]:
        sess.scores[uid] = sess.scores.get(uid, 0) + 1


async def _cleanup_quiz_messages(sess: QuizSession) -> None:
    """Após o pódio: apaga as enquetes respondidas + a msg de lobby/início,
    deixando SÓ a mensagem do pódio no chat (regra do dono: não floodar)."""
    if bot is None:
        return
    mids = list(sess.poll_mids)
    if sess.lobby_mid is not None:
        mids.append(sess.lobby_mid)
    for mid in mids:
        try:
            await bot.delete_message(sess.chat_id, mid)
        except Exception:
            pass


async def _send_results(sess: QuizSession) -> None:
    chat_id = sess.chat_id
    ranked = quiz_top(sess.scores, n=10)
    if not ranked:
        m = await safe_send(chat_id, term_block(
            "QUIZ", "<i>Ninguém pontuou neste quiz. 🦗</i>",
            status="VAZIO", status_color="AMBER", stamp=sess.theme[:24]))
        await _cleanup_quiz_messages(sess)
        if m is not None:
            await auto_delete_after(m, QUIZ_PODIUM_TTL)
        return
    # XP em LOTE no fim (regra do dono: 1 concessão por jogador → no máximo 1
    # anúncio de level-up por pessoa, sem floodar). Todo acerto vira XP; bônus
    # de classe/casamento/evento são aplicados dentro de award_xp_immediate.
    for uid, pts in sess.scores.items():
        if pts > 0:
            award_xp_immediate(chat_id, uid, pts * QUIZ_XP_PER_POINT,
                               reason="quiz")
    medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
    entries: list[QuizCardEntry] = []
    lines: list[str] = []
    for i, (uid, pts) in enumerate(ranked, 1):
        prow = ensure_player(chat_id, uid)
        rid = prow.get("royal_id") or "RYL-????"
        name = anonize(get_name(chat_id, uid), rid)
        if i <= 5:
            entries.append(QuizCardEntry(
                rank=i, royal_id=rid,
                name=card_safe_name(chat_id, uid, name, rid),
                points=pts,
                avatar_slug=royal_avatars.resolve_slug(
                    prow.get("avatar_slug"), rid)))
        lines.append(f"{medals[i - 1]} <b>{i}.</b> {html.escape(name)} "
                     f"<code>{rid}</code> — {pts} pts")
    body = "\n".join(lines[:10]) + (
        f"\n\n<i>// +{QUIZ_XP_PER_POINT} XP por acerto "
        f"(bônus de classe/casamento contam). Veja /royalperfil.</i>")
    caption = term_block("QUIZ", body,
                         status="FIM", status_color="ACID",
                         stamp=sess.theme[:24])
    await safe_typing(chat_id, "upload_photo")
    podium = None
    try:
        png = await asyncio.to_thread(
            render_quiz_card, sess.theme[:40], tuple(entries))
        if png and bot is not None:
            podium = await bot.send_photo(
                chat_id,
                BufferedInputFile(png, filename="royal_quiz.jpg"),
                caption=cap1024(caption))
    except Exception:
        logger.exception("[QUIZ] card falhou; fallback texto")
    if podium is None:
        podium = await safe_send(chat_id, caption)
    # Limpa as enquetes/lobby (deixa só o pódio) e agenda o pódio p/ sumir.
    await _cleanup_quiz_messages(sess)
    if podium is not None:
        await auto_delete_after(podium, QUIZ_PODIUM_TTL)
