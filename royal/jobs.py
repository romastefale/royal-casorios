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

from royal import core
from royal.config import (AUTO_HOURS, FLUSH_INTERVAL_SECONDS, LOG_DUMP_ENABLED, LOG_DUMP_INTERVAL_SEC, MIRA_ENABLED, MIRA_PALAVRAS_HOUR, MUSIC_BOT_ID, STASH_CHAT_ID, TEST_CHAT_IDS, logger)
from royal.core import (BACKUP_ENABLED, BACKUP_HOUR, _compute_next_palavra_at, _identity_card_hash, activity_buffer, admin_cache, attempt_cooldowns, boss_attack_cooldowns, bot, bot_meta_get, bot_meta_set, check_season_change, cur, db, dp, dump_logs_to_gist, dump_logs_to_file, ensure_identity_card_async, expire_old_chests, finalize_expired_challenges, flush_buffers_once, get_active_challenge, is_chat_muted, local_now, pair_buffer, photo_cache, run_backup, schedule_next_palavra, send_couple, spawn_boss_if_due, spawn_chest, spawn_palavra, term_block, typewriter_animate, utc_iso, utc_now)

async def log_dump_job() -> None:
    """Background: dump dos logs a cada LOG_DUMP_INTERVAL_SEC."""
    if not LOG_DUMP_ENABLED:
        logger.info("[LOGS] dump job desabilitado (LOG_DUMP_ENABLED=0)")
        return
    await asyncio.sleep(LOG_DUMP_INTERVAL_SEC)  # 1a janela espera intervalo
    while True:
        try:
            path = dump_logs_to_file()
            gist_url = await dump_logs_to_gist()
            if path or gist_url:
                logger.info("[LOGS] tick file=%s gist=%s",
                            "ok" if path else "-",
                            "ok" if gist_url else "-")
        except Exception:
            logger.exception("[LOGS] tick failed")
        await asyncio.sleep(LOG_DUMP_INTERVAL_SEC)


async def backup_job() -> None:
    """Background: backup diario gated em BACKUP_HOUR local (1x/dia) +
    confirmacao no DM do owner. Na 1a implementacao desta versao roda 2
    backups de verificacao (one-time, flag em bot_meta) p/ comprovar que
    o pipeline funciona."""
    if not BACKUP_ENABLED:
        logger.info("[BACKUP] desabilitado (BACKUP_ENABLED=0)")
        return
    # ONE-TIME: double backup de verificacao na 1a subida desta versao.
    # Flag persistida em bot_meta (sobrevive a restarts; no volume persiste
    # de vez) -> roda 1x na vida do deploy, nao a cada boot. Blindado: erro
    # transiente (ex: bot_meta_set) nao deve impedir o backup diario abaixo.
    try:
        if bot_meta_get("initial_double_backup") != "done":
            await asyncio.sleep(15)  # deixa polling/bot estabilizar
            logger.info("[BACKUP] 1a implementacao: rodando DOUBLE BACKUP de verificacao")
            ok1 = await run_backup(upload=True, notify_owner=True,
                                   label="verificacao 1/2", tag="init1")
            await asyncio.sleep(3)
            ok2 = await run_backup(upload=True, notify_owner=True,
                                   label="verificacao 2/2", tag="init2")
            if ok1 and ok2:
                bot_meta_set("initial_double_backup", "done")
                logger.info("[BACKUP] DOUBLE BACKUP inicial OK — flag setada (nao repete)")
            else:
                logger.warning("[BACKUP] DOUBLE BACKUP inicial incompleto — "
                               "retry no proximo boot")
    except Exception:
        logger.exception("[BACKUP] DOUBLE BACKUP inicial erro — retry no proximo boot")
    last_day = None
    while True:
        await asyncio.sleep(300)  # checa a cada 5min
        # blindagem: nenhum erro transiente pode matar o loop diario.
        try:
            now_local = local_now()
            day = now_local.date().isoformat()
            if now_local.hour == BACKUP_HOUR and day != last_day:
                # so marca o dia APOS sucesso — falha (lock/IO transiente)
                # permite retry no proximo tick (5min) ainda no mesmo dia.
                if await run_backup(upload=True, notify_owner=True,
                                    label="diario meio-dia"):
                    last_day = day
        except Exception:
            logger.exception("[BACKUP] loop diario erro — segue no proximo tick")


async def flush_buffers():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        try:
            flush_buffers_once()
        except Exception:
            logger.exception("buffer flush failed")


async def cleanup_job():
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff_pairs = (utc_now() - timedelta(days=3)).isoformat()
            cutoff_activity = (local_now() - timedelta(days=7)).date().isoformat()
            cur.execute("DELETE FROM pair_scores WHERE last_seen < ?", (cutoff_pairs,))
            cur.execute("DELETE FROM daily_activity WHERE day < ?", (cutoff_activity,))

            now = utc_now().timestamp()
            expired_admin = [k for k, (_, exp) in admin_cache.items() if exp <= now]
            for k in expired_admin:
                admin_cache.pop(k, None)
            expired_photo = [k for k, (_, exp) in photo_cache.items() if exp <= now]
            for k in expired_photo:
                photo_cache.pop(k, None)

            # limpa cooldowns antigos (>1h)
            cutoff_ts = now - 3600
            for k in list(attempt_cooldowns.keys()):
                if attempt_cooldowns[k] < cutoff_ts:
                    attempt_cooldowns.pop(k, None)
            for k in list(boss_attack_cooldowns.keys()):
                if boss_attack_cooldowns[k] < cutoff_ts:
                    boss_attack_cooldowns.pop(k, None)

            db.commit()
            logger.info("cleanup_job ok (admin=%d photo=%d)",
                        len(expired_admin), len(expired_photo))
        except Exception:
            logger.exception("cleanup_job failed")


_identity_sweep_last_day: str | None = None


async def identity_card_sweep_job():
    """Sweep diario: percorre todos os players e regenera o identity card
    de quem teve mudanca de nome ou avatar desde a ultima geracao. Roda
    1x por dia local (~03:30 horario local pra evitar pico). Silencioso —
    upload vai pro STASH_CHAT_ID sem notificacao."""
    global _identity_sweep_last_day
    while True:
        await asyncio.sleep(600)  # checa a cada 10 min
        try:
            now_local = local_now()
            today = now_local.date().isoformat()
            # Roda 1x por dia, em qualquer momento depois das 03:00
            # local. Se o bot estava down as 03h, pega assim que voltar
            # online no mesmo dia (catch-up resiliente).
            if (_identity_sweep_last_day == today
                    or now_local.hour < 3):
                continue
            if not STASH_CHAT_ID:
                _identity_sweep_last_day = today
                logger.info("identity sweep: STASH_CHAT_ID nao configurado, "
                            "skip do dia")
                continue
            cur.execute(
                "SELECT p.chat_id, p.user_id, p.avatar_slug, p.royal_id, "
                "       p.inline_card_hash, u.display_name, u.username "
                "FROM players p "
                "LEFT JOIN users u ON u.chat_id=p.chat_id AND u.user_id=p.user_id"
            )
            rows = cur.fetchall()
            refreshed = 0
            checked = 0
            for r in rows:
                checked += 1
                raw_name = (r["display_name"] or "").strip()
                username = (r["username"] or "").strip() or None
                name = raw_name or (f"@{username}" if username
                                    else str(r["user_id"]))
                target = _identity_card_hash(name, r["avatar_slug"],
                                             username, r["royal_id"])
                if r["inline_card_hash"] == target:
                    continue
                fid = await ensure_identity_card_async(
                    r["chat_id"], r["user_id"])
                if fid:
                    refreshed += 1
                # Throttle leve pra nao spammar Telegram (~1 upload/s)
                await asyncio.sleep(1.2)
            _identity_sweep_last_day = today
            logger.info("identity sweep ok: checked=%d refreshed=%d",
                        checked, refreshed)
        except Exception:
            logger.exception("identity_card_sweep failed")


async def scheduler():
    """Loop unificado: casórios, palavra, boss, temporadas."""
    last_minute = -1
    while True:
        await asyncio.sleep(20)
        try:
            now_local = local_now()

            # === SHIPPER: casórios automáticos por hora ===
            if now_local.hour in AUTO_HOURS:
                marker = now_local.strftime("%Y-%m-%d-%H")
                cur.execute("SELECT chat_id, last_auto_post FROM chats WHERE enabled=1")
                for row in cur.fetchall():
                    if row["last_auto_post"] == marker:
                        continue
                    if is_chat_muted(row["chat_id"]):
                        continue
                    ok = await send_couple(row["chat_id"], source="auto")
                    if ok:
                        cur.execute("UPDATE chats SET last_auto_post=? WHERE chat_id=?",
                                    (marker, row["chat_id"]))
                        db.commit()

            # === PALAVRA DA HORA ===
            cur.execute("SELECT chat_id FROM chats WHERE enabled=1")
            enabled = [r["chat_id"] for r in cur.fetchall()]
            for chat_id in enabled:
                cur.execute("SELECT next_palavra_at FROM chats_rpg WHERE chat_id=?", (chat_id,))
                r = cur.fetchone()
                if not r or not r["next_palavra_at"]:
                    schedule_next_palavra(chat_id)
                    continue
                try:
                    nxt = datetime.fromisoformat(r["next_palavra_at"])
                except Exception:
                    schedule_next_palavra(chat_id)
                    continue
                if now_local >= nxt:
                    # Mute: nao spawna nem reagenda — fica pendente ate
                    # desmutar (entao spawn vira na proxima tick).
                    if is_chat_muted(chat_id):
                        continue
                    # CLAIM ATOMICO anti-duplicacao (multi-instancia): so a
                    # instancia que conseguir AVANCAR next_palavra_at (do valor
                    # 'nxt' antigo p/ o proximo) prossegue ao spawn. Se 2
                    # processos rodarem em overlap (ex: redeploy), a 2a instancia
                    # le o valor ja avancado → WHERE nao casa → rowcount 0 →
                    # pula. WAL serializa os writers, garantindo 1 vencedor.
                    cur.execute(
                        "UPDATE chats_rpg SET next_palavra_at=? "
                        "WHERE chat_id=? AND next_palavra_at=?",
                        (_compute_next_palavra_at().isoformat(),
                         chat_id, r["next_palavra_at"]),
                    )
                    db.commit()
                    if cur.rowcount != 1:
                        continue  # outra instancia ja pegou este slot
                    # Spawna se nao tem um ativo
                    if not get_active_challenge(chat_id):
                        await spawn_palavra(chat_id)

            # === FINALIZA DESAFIOS EXPIRADOS ===
            await finalize_expired_challenges()

            # === BAUS REAIS (spawn pendentes + expira abertos) ===
            try:
                cur.execute(
                    "SELECT id, chat_id FROM chests "
                    "WHERE status='pending' AND spawn_at <= ?",
                    (utc_iso(),),
                )
                pending = [dict(r) for r in cur.fetchall()]
                for ch in pending:
                    if is_chat_muted(ch["chat_id"]):
                        continue  # adia spawn ate desmutar
                    await spawn_chest(ch["chat_id"], ch["id"])
                await expire_old_chests()
            except Exception:
                logger.exception("chest scheduler tick failed")

            # === BOSS SEMANAL ===
            await spawn_boss_if_due()

            # === TEMPORADAS (check a cada minuto, no segundo 0-20) ===
            if now_local.minute != last_minute:
                last_minute = now_local.minute
                await check_season_change()

        except Exception:
            logger.exception("scheduler failed")


async def healthcheck():
    while True:
        await asyncio.sleep(300)
        try:
            logger.info(
                "healthcheck activity_buf=%d pair_buf=%d admin_cache=%d photo_cache=%d",
                len(activity_buffer), len(pair_buffer),
                len(admin_cache), len(photo_cache))
        except Exception:
            logger.exception("healthcheck failed")


STALE_UPDATE_SEC = int(os.getenv("HEALTH_STALE_SEC", "600"))


_health_runner = None  # type: ignore[var-annotated]


async def _health_handler(request):
    from aiohttp import web
    checks: dict[str, object] = {}
    ok = True
    # (a) DB ping
    try:
        cur.execute("SELECT 1")
        cur.fetchone()
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"fail:{type(e).__name__}"
        ok = False
    # (b) staleness das updates
    idle = time.monotonic() - core._last_update_monotonic
    checks["last_update_age_sec"] = round(idle, 1)
    if idle > STALE_UPDATE_SEC:
        checks["updates"] = "stale"
        ok = False
    else:
        checks["updates"] = "ok"
    checks["schema_version"] = cur.execute(
        "PRAGMA user_version").fetchone()[0]
    return web.json_response(
        {"status": "ok" if ok else "degraded", "checks": checks},
        status=200 if ok else 503)


async def start_health_server():
    """Sobe o server de health na $PORT (no-op se PORT ausente)."""
    global _health_runner
    port_raw = os.getenv("PORT")
    if not port_raw:
        logger.info("[HEALTH] PORT ausente — health server desativado")
        return
    try:
        from aiohttp import web
    except Exception:
        logger.warning("[HEALTH] aiohttp indisponivel — health server pulado")
        return
    try:
        port = int(port_raw)
    except ValueError:
        logger.warning("[HEALTH] PORT invalido: %r", port_raw)
        return
    app = web.Application()
    app.router.add_get("/health", _health_handler)
    app.router.add_get("/", _health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    _health_runner = runner
    logger.info("[HEALTH] server ON em 0.0.0.0:%d /health", port)


@dp.shutdown()
async def _on_shutdown():
    logger.info("[SHUTDOWN] iniciando teardown gracioso")
    try:
        flush_buffers_once()  # sincrona — NAO await
    except Exception:
        logger.exception("[SHUTDOWN] flush_buffers_once falhou")
    if _health_runner is not None:
        try:
            await _health_runner.cleanup()
            logger.info("[SHUTDOWN] health server fechado")
        except Exception:
            logger.exception("[SHUTDOWN] health cleanup falhou")
    try:
        db.commit()
        db.close()
        logger.info("[SHUTDOWN] DB fechado limpo")
    except Exception:
        logger.exception("[SHUTDOWN] db.close falhou")


async def mira_palavras_job():
    """Inteligência royal (Objetivo 1): 1x/dia (a partir de MIRA_PALAVRAS_HOUR
    local) pede as "palavras do dia" à @Mira pela ponte e grava no banco
    dinâmico. Persiste o dia em bot_meta (sobrevive a restart → não re-pede no
    mesmo dia). Se a @Mira não responder, NÃO marca o dia → retry no próximo
    tick, com throttle de 30min p/ não nagar."""
    from royal.mira import fetch_and_store_palavras
    if not MIRA_ENABLED:
        logger.info("[MIRA] ponte desligada (sem MIRA_USERNAME/IA_BRIDGE_CHAT_ID) "
                    "— job de palavras do dia não roda")
        return
    last_attempt = 0.0
    while True:
        await asyncio.sleep(300)
        try:
            now_local = local_now()
            day = now_local.date().isoformat()
            if now_local.hour < MIRA_PALAVRAS_HOUR:
                continue
            if bot_meta_get("mira_palavras_day") == day:
                continue
            now_ts = utc_now().timestamp()
            if now_ts - last_attempt < 1800:
                continue  # throttle: no máx 1 tentativa / 30min em caso de falha
            last_attempt = now_ts
            got = await fetch_and_store_palavras()
            if got > 0:
                bot_meta_set("mira_palavras_day", day)
                logger.info("[MIRA] palavras do dia OK (%s): %d palavras", day, got)
            else:
                logger.warning("[MIRA] palavras do dia falhou (%s) — retry depois", day)
        except Exception:
            logger.exception("[MIRA] mira_palavras_job tick falhou")


async def register_bot_commands():
    assert bot is not None
    group_cmds = [
        BotCommand(command="royal",             description="👑 Menu principal"),
        BotCommand(command="royalperfil",       description="📜 Ver perfil"),
        BotCommand(command="royalavatar",       description="👑 Escolher avatar"),
        BotCommand(command="royalup",           description="⬆️ Distribuir pontos"),
        BotCommand(command="royalclasse",       description="🎭 Escolher classe"),
        BotCommand(command="royalinventario",   description="🎒 Ver inventário"),
        BotCommand(command="royalloja",         description="🪙 Loja"),
        BotCommand(command="royalsaldo",        description="💰 Saldo"),
        BotCommand(command="royalranking",      description="🏆 Ranking da temporada"),
        BotCommand(command="royalpalavra",      description="🎯 Palavra da hora"),
        BotCommand(command="royalboss",         description="🐉 Boss da semana"),
        BotCommand(command="royalcasorios",     description="💍 Ranking dos casais"),
        BotCommand(command="royalmeuscasorios", description="📊 Meus casórios"),
        BotCommand(command="royalencalhar",     description="🚫 Sair dos casórios"),
        BotCommand(command="royaldesencalhar",  description="💘 Voltar pros casórios"),
        BotCommand(command="royalpresentear",   description="🎁 Presentear florins"),
        BotCommand(command="royalmissoes",      description="🗺️ Missões diárias"),
        BotCommand(command="royalevento",       description="🎉 Evento ativo"),
        BotCommand(command="royalconquistas",   description="🏅 Conquistas"),
        BotCommand(command="royalcasar",        description="💍 (admin) Forçar casório"),
        BotCommand(command="royalativar",       description="🔧 (admin) Ativar bot"),
        BotCommand(command="royaltutorial",     description="📖 Como jogar"),
        BotCommand(command="royalajuda",        description="❓ Ajuda completa"),
    ]
    private_cmds = [
        BotCommand(command="royal",             description="👑 Menu principal"),
        BotCommand(command="royalperfil",       description="📜 Meu perfil"),
        BotCommand(command="royalavatar",       description="👑 Escolher avatar"),
        BotCommand(command="royalranking",      description="🏆 Ranking"),
        BotCommand(command="royalinventario",   description="🎒 Inventário"),
        BotCommand(command="royalsaldo",        description="💰 Saldo"),
        BotCommand(command="royalmeuscasorios", description="📊 Meus casórios"),
        BotCommand(command="royalmissoes",      description="🗺️ Missões diárias"),
        BotCommand(command="royalevento",       description="🎉 Evento ativo"),
        BotCommand(command="royalconquistas",   description="🏅 Conquistas"),
        BotCommand(command="royalconfig",       description="⚙️ Preferências"),
        BotCommand(command="royalgrupo",        description="🏰 Trocar grupo ativo"),
        BotCommand(command="royaltutorial",     description="📖 Como jogar"),
        BotCommand(command="royalajuda",        description="❓ Ajuda"),
        BotCommand(command="royalprivacidade",  description="🔒 Privacidade"),
        BotCommand(command="royaldados",        description="📦 Meus dados"),
    ]
    # IMPORTANTE: scopes do Telegram sao resolvidos por SUBSTITUICAO
    # (mais especifico vence), nao merge. AllChatAdministrators tem
    # prioridade > AllGroupChats pra admins — entao se mandar so o
    # comando admin, admins PERDEM os 19 comandos normais no menu /.
    # Fix: admin_cmds = group_cmds + extras de admin.
    # /royalpalavratest eh restrito ao OWNER_USER_ID e NAO aparece no menu.
    admin_cmds = list(group_cmds)

    try:
        await bot.set_my_commands(group_cmds, scope=BotCommandScopeAllGroupChats())
        await bot.set_my_commands(private_cmds, scope=BotCommandScopeAllPrivateChats())
        await bot.set_my_commands(admin_cmds, scope=BotCommandScopeAllChatAdministrators())
    except Exception:
        logger.exception("set_my_commands failed")


BOOT_ANNOUNCE_KEY = "boot_announce_typewriter_v1"


async def announce_typewriter_feature() -> None:
    """One-shot: anuncia o novo feedback de cadastro do /royal nos grupos
    RPG ativos. Flag persistente em bot_meta — roda 1x apos o deploy."""
    if bot_meta_get(BOOT_ANNOUNCE_KEY):
        return
    # Espera o polling estabilizar antes de mandar mensagens
    await asyncio.sleep(5.0)
    try:
        cur.execute(
            "SELECT DISTINCT p.chat_id AS chat_id, COUNT(*) AS n "
            "FROM players p "
            "INNER JOIN chats_rpg c ON c.chat_id = p.chat_id "
            "GROUP BY p.chat_id")
        chats = [(r["chat_id"], r["n"]) for r in cur.fetchall()]
    except Exception:
        logger.exception("announce_typewriter: query chats falhou")
        return
    if not chats:
        bot_meta_set(BOOT_ANNOUNCE_KEY, "no_chats")
        return
    sent_ok = 0
    for chat_id, n_players in chats:
        if TEST_CHAT_IDS and chat_id in TEST_CHAT_IDS:
            # Pula grupos de teste — anuncio so em prod
            continue
        if is_chat_muted(chat_id):
            # /royalmudo ON — pula anuncio neste grupo
            continue
        try:
            cur.execute(
                "SELECT royal_id FROM players WHERE chat_id=? "
                "AND royal_id IS NOT NULL "
                "ORDER BY joined_at DESC LIMIT 8", (chat_id,))
            rids = [r["royal_id"] for r in cur.fetchall()]
        except Exception:
            rids = []
        rid_block = "\n".join(f"  >> {r}" for r in rids) or "  >> --"
        intro = (
            f"> ROYAL.SYS // UPDATE_APLICADO\n"
            f">> /royal agora confirma cadastro\n"
            f">> feedback letra-por-letra ativo\n"
            f"// nobres sincronizados: {n_players}\n"
            f"// ultimos cadastros:\n"
            f"{rid_block}\n"
            f"// digita /royal pra testar"
        )
        try:
            await typewriter_animate(chat_id, intro, chunk=6, delay=0.32)
            sent_ok += 1
        except Exception:
            logger.exception("announce_typewriter: send falhou chat=%s",
                             chat_id)
        await asyncio.sleep(2.0)  # respeita rate-limit cross-chat
    bot_meta_set(BOOT_ANNOUNCE_KEY,
                 f"sent={sent_ok}/{len(chats)} at={utc_iso()}")
    logger.info("announce_typewriter: ok sent=%d/%d",
                sent_ok, len(chats))


_LEGACY_BOT_USER_IDS = {1087968824, 136817688, MUSIC_BOT_ID}


BOOT_CLEANUP_BOTS_KEY = "boot_cleanup_bot_players_v1"


_USER_REF_COLS = ("user_id", "user1", "user2", "from_user", "to_user",
                  "winner_user_id", "voter_id")


def cleanup_legacy_bot_players() -> dict[str, int]:
    """One-shot: remove do banco os bots conhecidos que foram cadastrados como
    jogadores ANTES do filtro is_bot. Varre TODA tabela e apaga linhas onde
    qualquer coluna de referencia de usuario (_USER_REF_COLS, descoberta via
    PRAGMA — robusto a schema novo) aponte p/ um bot conhecido. Transacional
    (1 commit; rollback em erro) + idempotente (flag em bot_meta + re-rodar nao
    acha nada). Silencioso: so loga, nao posta no grupo."""
    if bot_meta_get(BOOT_CLEANUP_BOTS_KEY):
        return {}
    bot_ids = sorted(i for i in _LEGACY_BOT_USER_IDS if i and i > 0)
    if not bot_ids:
        bot_meta_set(BOOT_CLEANUP_BOTS_KEY, "no_ids")
        return {}
    ph = ",".join("?" for _ in bot_ids)
    deleted: dict[str, int] = {}
    try:
        tabelas = [r["name"] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        for t in tabelas:
            cols = [c["name"] for c in cur.execute(
                f'PRAGMA table_info("{t}")').fetchall()]
            ref = [c for c in cols if c in _USER_REF_COLS]
            if not ref:
                continue
            # DELETE onde QUALQUER coluna de usuario for um bot conhecido.
            where = " OR ".join(f'"{c}" IN ({ph})' for c in ref)
            args = tuple(bot_ids) * len(ref)
            cur.execute(f'DELETE FROM "{t}" WHERE {where}', args)
            if cur.rowcount:
                deleted[t] = cur.rowcount
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("cleanup_legacy_bot_players: falhou — rollback")
        return {}
    total = sum(deleted.values())
    bot_meta_set(BOOT_CLEANUP_BOTS_KEY,
                 f"deleted={total} tables={len(deleted)} at={utc_iso()}")
    if total:
        logger.info("cleanup_legacy_bot_players: removidas %d linhas de bots "
                    "legados em %s", total, deleted)
    else:
        logger.info("cleanup_legacy_bot_players: nada a limpar")
    return deleted


BOOT_ANNOUNCE_NOBOTS_KEY = "boot_announce_nobots_v1"


async def announce_no_bots_feature() -> None:
    """One-shot: anuncia nos grupos RPG ativos que bots nao entram mais no
    jogo (admins anonimos = @GroupAnonymousBot deixam de ser cadastrados).
    Mensagem-log com a confissao pedida pelo dono. Flag persistente em
    bot_meta — roda 1x apos o deploy."""
    if bot_meta_get(BOOT_ANNOUNCE_NOBOTS_KEY):
        return
    await asyncio.sleep(8.0)  # deixa o polling estabilizar (apos o outro anuncio)
    try:
        cur.execute(
            "SELECT DISTINCT p.chat_id AS chat_id "
            "FROM players p "
            "INNER JOIN chats_rpg c ON c.chat_id = p.chat_id")
        chats = [r["chat_id"] for r in cur.fetchall()]
    except Exception:
        logger.exception("announce_no_bots: query chats falhou")
        return
    if not chats:
        bot_meta_set(BOOT_ANNOUNCE_NOBOTS_KEY, "no_chats")
        return
    body = (
        ">> bots agora ficam de fora da corte\n"
        "// confesso: me confundi. parecia MUITO\n"
        "// sedutor alistar no jogo os bots que sao\n"
        "// administradores — mas agora estou mais\n"
        "// consciente do trabalho que realizo.\n"
        "// daqui pra frente, so nobres de verdade."
    )
    msg = term_block("ATUALIZACAO", body, status="APLICADO", status_color="ACID")
    eligible = 0
    sent_ok = 0
    for chat_id in chats:
        if TEST_CHAT_IDS and chat_id in TEST_CHAT_IDS:
            continue
        if is_chat_muted(chat_id):
            continue
        eligible += 1
        try:
            await bot.send_message(chat_id, msg)
            sent_ok += 1
        except Exception:
            logger.exception("announce_no_bots: send falhou chat=%s", chat_id)
        await asyncio.sleep(2.0)  # respeita rate-limit cross-chat
    # So persiste a flag se nao havia elegiveis (nada a fazer) OU se ao menos 1
    # enviou. Se havia elegiveis e TODOS falharam (transiente), NAO marca →
    # tenta de novo no proximo boot, sem double-post nos que ja receberam.
    if eligible == 0 or sent_ok > 0:
        bot_meta_set(BOOT_ANNOUNCE_NOBOTS_KEY,
                     f"sent={sent_ok}/{eligible} at={utc_iso()}")
        logger.info("announce_no_bots: ok sent=%d/%d", sent_ok, eligible)
    else:
        logger.warning("announce_no_bots: 0/%d enviados — sem flag, retry no "
                       "proximo boot", eligible)


