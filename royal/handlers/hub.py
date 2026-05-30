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

from royal.config import (BOSS_SPAWN_HOUR, logger)
from royal.core import (CLASSES, EFFECT_PARTY, ITEMS, ROYAL_HELP, ROYAL_TUTORIAL_PARTS, _HUB_DEDICATED_PREFIXES, _apply_avatar, _avatar_reveal_payload, _tutorial_block, _tutorial_kb, assert_owner, boss_keyboard, bot, cap1024, classe_keyboard, cur, current_season_label, db, delete_msg_safe, display_name, dp, effect_kw, ensure_player, format_boss_text, format_challenge_text, get_active_boss, get_active_challenge, get_anon_name, get_name, get_or_create_player, get_player, handle_boss_attack, handle_chest_claim, hub_keyboard_main, hub_text, inv_keyboard, is_group_chat, loja_keyboard, rate_limited, react_to, refresh_user_identity, register_owner, resolve_owner_chat, safe_typing, send_profile_card, send_ranking, term_block, typewriter_animate, up_keyboard, with_close)

router = Router()

@router.message(Command("royal"))
async def royal_hub(message: Message):
    # Bot reage ao comando antes de responder — feedback instantaneo
    if message.from_user:
        await react_to(message.chat.id, message.message_id, "👀")
    # Cadastra player se for novo (so em grupo — DM nao tem contexto de
    # chat_id de grupo). Se for o 1o /royal do nobre, animacao de boas
    # vindas letra por letra antes do hub.
    is_new = False
    rid = "RYL-????"
    if (message.chat.type in ("group", "supergroup")
            and message.from_user):
        try:
            row, is_new = get_or_create_player(
                message.chat.id, message.from_user.id)
            rid = row.get("royal_id") or rid
        except Exception:
            logger.exception("royal_hub: get_or_create_player falhou")
    if is_new:
        intro = (
            f"> ROYAL.SYS // CADASTRADO\n"
            f">> {rid} sincronizado\n"
            f"// bem-vindo ao reino, nobre"
        )
        await typewriter_animate(message.chat.id, intro,
                                 chunk=4, delay=0.35)
        await asyncio.sleep(0.4)
    await safe_typing(message.chat.id)
    extra = (effect_kw(message.chat.type, EFFECT_PARTY)
             if is_new else {})
    hub_uid = message.from_user.id if message.from_user else None
    await message.answer(hub_text(),
                         reply_markup=with_close(hub_keyboard_main(), hub_uid),
                         **extra)
    # Nudge pos-cadastro: lembra o nobre de escolher classe. Roda
    # tambem pra players ja existentes que ainda nao escolheram.
    if (message.chat.type in ("group", "supergroup")
            and message.from_user):
        try:
            p = get_player(message.chat.id, message.from_user.id)
        except Exception:
            p = None
        if p and not (p.get("class_id") or "").strip():
            await asyncio.sleep(0.6)
            nudge = (
                ">> PROXIMO_PASSO\n"
                "// classe nao definida\n"
                "// usa /royalclasse pra escolher a sua"
            )
            await typewriter_animate(message.chat.id, nudge,
                                     chunk=5, delay=0.32)


@router.message(F.text == "👑 Reino")
async def royal_hub_btn(message: Message):
    await message.answer(hub_text(), reply_markup=hub_keyboard_main())


@router.callback_query(F.data.startswith("r:") & ~F.data.startswith(_HUB_DEDICATED_PREFIXES))
async def hub_cb(cb: CallbackQuery):
    if not cb.data or not cb.message or not cb.from_user:
        return
    parts = cb.data.split(":")
    if len(parts) < 2:
        await cb.answer()
        return
    action = parts[1]

    # Botão universal ❌ Fechar — apaga a mensagem do próprio dono. Não
    # precisa de contexto de chat. r:close:{uid} (cards) checa o id embutido;
    # r:close puro (menus) cai no owner-lock via assert_owner.
    if action == "close":
        owner_id = None
        if len(parts) >= 3:
            try:
                owner_id = int(parts[2])
            except ValueError:
                owner_id = None
        if owner_id is not None:
            if cb.from_user.id != owner_id:
                await cb.answer("❌ Esse menu não é seu.", show_alert=False)
                return
        elif not await assert_owner(cb):
            return
        await delete_msg_safe(cb.message)
        try:
            await cb.answer()
        except Exception:
            pass
        return

    # Tutorial navegável (◀/▶) — edita a MESMA msg, sem floodar.
    if action == "tut":
        try:
            idx = int(parts[2])
        except (IndexError, ValueError):
            await cb.answer()
            return
        # uid embutido (parts[3]) → lock durável; assert_owner como reforço.
        owner_id = (int(parts[3]) if len(parts) > 3
                    and parts[3].lstrip("-").isdigit() else None)
        if owner_id is not None:
            if cb.from_user.id != owner_id:
                await cb.answer("❌ Esse tutorial não é seu.",
                                show_alert=False)
                return
        elif not await assert_owner(cb):
            return
        idx = max(0, min(idx, len(ROYAL_TUTORIAL_PARTS) - 1))
        try:
            await cb.message.edit_text(
                _tutorial_block(idx),
                reply_markup=_tutorial_kb(idx, cb.from_user.id))
        except Exception:
            pass
        await cb.answer()
        return

    # chat_id efetivo: em grupo, o proprio chat; em DM, o reino mais ativo do user
    fallback = cb.message.chat.id if is_group_chat(cb.message) else None
    chat_id = resolve_owner_chat(cb.from_user.id, fallback)

    # Escolha de avatar por botão (grid 1..36) — fonte única _apply_avatar.
    if action == "av":
        try:
            n = int(parts[2])
        except (IndexError, ValueError):
            await cb.answer()
            return
        # uid embutido (parts[3]) → lock durável; assert_owner como reforço.
        owner_id = (int(parts[3]) if len(parts) > 3
                    and parts[3].lstrip("-").isdigit() else None)
        if owner_id is not None:
            if cb.from_user.id != owner_id:
                await cb.answer("❌ Esse menu não é seu.", show_alert=False)
                return
        elif not await assert_owner(cb):
            return
        if chat_id is None:
            await cb.answer("😶 Você ainda não tem perfil no Reino.",
                            show_alert=True)
            return
        status, payload = _apply_avatar(chat_id, cb.from_user.id, n)
        if status == "noprofile":
            await cb.answer("😶 Você ainda não tem perfil no Reino.",
                            show_alert=True)
            return
        if status == "locked":
            cur_slug = payload
            await cb.answer(
                f"❌ Você já escolheu seu avatar nesta temporada "
                f"(#{royal_avatars.number_of(cur_slug):02d} "
                f"{royal_avatars.display_name(cur_slug)}).",
                show_alert=True)
            return
        if status != "ok":
            await cb.answer()
            return
        slug = payload
        name = royal_avatars.display_name(slug)
        await cb.answer(f"Avatar #{n:02d} {name} ✓")
        # Acao terminal: apaga o mosaico (poluicao zero) e revela o avatar.
        await delete_msg_safe(cb.message)
        png, caption = _avatar_reveal_payload(chat_id, cb.from_user.id,
                                              slug, n)
        try:
            if png and bot is not None:
                await bot.send_photo(
                    cb.message.chat.id,
                    BufferedInputFile(png, filename=f"avatar-{slug}.png"),
                    caption=cap1024(caption),
                    **effect_kw(cb.message.chat.type, EFFECT_PARTY))
            elif bot is not None:
                await bot.send_message(
                    cb.message.chat.id, caption,
                    **effect_kw(cb.message.chat.type, EFFECT_PARTY))
        except Exception:
            logger.exception("avatar reveal (cb) falhou")
        return

    # Acoes que requerem contexto de chat Royal:
    needs_chat = {"rank", "pal", "boss", "loja", "cas", "up", "cls", "buy",
                  "atk", "inv"}
    if action in needs_chat and chat_id is None:
        await cb.answer("🏰 Esse atalho precisa de um grupo do Reino. Volta pra lá pra usar.",
                        show_alert=True)
        return

    # Baú: callback vem do próprio chat onde foi postado; ignora needs_chat
    if action == "chest":
        if len(parts) < 3:
            await cb.answer()
            return
        try:
            chest_id = int(parts[2])
        except ValueError:
            await cb.answer()
            return
        await handle_chest_claim(cb, chest_id)
        return

    try:
        if action == "perfil":
            target_chat = chat_id or (cb.message.chat.id if is_group_chat(cb.message) else None)
            if target_chat is None:
                await cb.answer("Você ainda não tem perfil. Mande mensagem no grupo Royal.",
                                show_alert=True)
                return
            # Refresca nome vivo do Telegram p/ evitar ANON-X no proprio dono
            live = (cb.from_user.full_name or cb.from_user.first_name
                    or (f"@{cb.from_user.username}" if cb.from_user.username else ""))
            refresh_user_identity(target_chat, cb.from_user.id, live, cb.from_user.username)
            db.commit()
            await send_profile_card(cb.message.chat.id, target_chat, cb.from_user.id,
                                    close_uid=cb.from_user.id)
            await cb.answer()
            return

        if action == "rank":
            wait = rate_limited(cb.from_user.id, "RANKING", cooldown=15.0)
            if wait > 0:
                await cb.answer(f"⏳ Aguarde {wait}s", show_alert=False)
                return
            await cb.answer()
            await send_ranking(chat_id, target_chat_id=cb.message.chat.id,
                               owner_uid=cb.from_user.id)
            return

        if action == "pal":
            ch = get_active_challenge(chat_id)
            if ch:
                await cb.message.answer(format_challenge_text(ch))
            else:
                await cb.message.answer(term_block(
                    "PALAVRA",
                    "<i>Sem transmissão ativa agora.</i>",
                    status="STANDBY", status_color="AMBER"))
            await cb.answer()
            return

        if action == "boss":
            boss = get_active_boss(chat_id)
            if boss:
                await cb.message.answer(format_boss_text(boss),
                                        reply_markup=boss_keyboard(boss["id"]))
            else:
                await cb.message.answer(term_block(
                    "BOSS",
                    f"<i>Nenhuma anomalia detectada.</i>\n"
                    f">> próximo spawn: <b>domingo {BOSS_SPAWN_HOUR}h</b>",
                    status="OFFLINE", status_color="AMBER"))
            await cb.answer()
            return

        if action == "help":
            await cb.message.answer(ROYAL_HELP)
            await cb.answer()
            return

        if action == "loja":
            p = ensure_player(chat_id, cb.from_user.id)
            db.commit()
            gold_br = f"{int(p['gold']):,}".replace(",", ".")
            parts_b = [f">> SALDO: <b><code>{gold_br}</code></b> 🪙\n"]
            for iid, item in ITEMS.items():
                parts_b.append(
                    f"{item['emoji']} <b>{item['name']}</b> "
                    f"— <code>{item['price']}</code>🪙\n   <i>{item['desc']}</i>")
            sent = await cb.message.answer(
                term_block("LOJA", "\n\n".join(parts_b),
                           status="OPEN", status_color="ACID"),
                reply_markup=loja_keyboard())
            register_owner(sent, cb.from_user.id, auto_delete_secs=60.0)
            await cb.answer()
            return

        if action == "inv":
            ensure_player(chat_id, cb.from_user.id)
            db.commit()
            cur.execute(
                "SELECT item_id, qty, equipped FROM inventory "
                "WHERE chat_id=? AND user_id=? AND qty>0 ORDER BY equipped DESC",
                (chat_id, cb.from_user.id))
            rows = cur.fetchall()
            if not rows:
                await cb.message.answer(term_block(
                    "INVENTARIO",
                    "<i>Cofre vazio. Visita a /royalloja.</i>",
                    status="VAZIO", status_color="AMBER"))
            else:
                parts_b = []
                for r in rows:
                    it = ITEMS.get(r["item_id"])
                    if not it:
                        continue
                    eq = "✅ " if r["equipped"] else "  "
                    parts_b.append(
                        f"{eq}{it['emoji']} <b>{it['name']}</b> "
                        f"<code>x{r['qty']}</code>\n   <i>{it['desc']}</i>")
                sent = await cb.message.answer(
                    term_block("INVENTARIO", "\n\n".join(parts_b),
                               status="LOADED"),
                    reply_markup=inv_keyboard(chat_id, cb.from_user.id))
                register_owner(sent, cb.from_user.id, auto_delete_secs=60.0)
            await cb.answer()
            return

        if action == "cas":
            cur.execute(
                "SELECT user1, user2, COUNT(*) AS total FROM couples "
                "WHERE chat_id=? GROUP BY user1, user2 ORDER BY total DESC LIMIT 10",
                (chat_id,))
            rows = cur.fetchall()
            if not rows:
                await cb.message.answer(term_block(
                    "CASORIOS",
                    "<i>Sem casórios suficientes ainda.</i>",
                    status="VAZIO", status_color="AMBER"))
            else:
                medals = ["🥇", "🥈", "🥉"] + ["🏅"] * 7
                lines = []
                for i, r in enumerate(rows, 1):
                    lines.append(
                        f"{medals[i-1]} <b>{i}.</b> "
                        f"{html.escape(get_anon_name(chat_id, r['user1']))} ❤️ "
                        f"{html.escape(get_anon_name(chat_id, r['user2']))} — "
                        f"<code>{r['total']}x</code>")
                await cb.message.answer(term_block(
                    "CASORIOS", "\n".join(lines),
                    status="RANKING", stamp=current_season_label()))
            await cb.answer()
            return

        if action == "up":
            sub = parts[2] if len(parts) > 2 else "menu"
            if sub == "menu":
                p = ensure_player(chat_id, cb.from_user.id)
                db.commit()
                sent = await cb.message.answer(
                    term_block("ATRIBUTOS",
                               f"<b>{p['pts_available']}</b> ponto(s) disponíveis.\n"
                               f"<i>Escolha um atributo abaixo:</i>",
                               status="READY"),
                    reply_markup=up_keyboard())
                register_owner(sent, cb.from_user.id, auto_delete_secs=60.0)
                await cb.answer()
                return
            attr = sub
            if attr not in {"for", "des", "vit", "car"}:
                await cb.answer()
                return
            if not await assert_owner(cb):
                return
            p = ensure_player(chat_id, cb.from_user.id)
            if p["pts_available"] <= 0:
                await cb.answer("Sem pontos disponíveis.", show_alert=True)
                return
            cur.execute(
                f"UPDATE players SET attr_{attr}=attr_{attr}+1, pts_available=pts_available-1 "
                f"WHERE chat_id=? AND user_id=? AND pts_available>0",
                (chat_id, cb.from_user.id))
            db.commit()
            p = ensure_player(chat_id, cb.from_user.id)
            await cb.answer(f"+1 {attr.upper()} ({p['pts_available']} pts restantes)")
            return

        if action == "cls":
            sub = parts[2] if len(parts) > 2 else "menu"
            if sub == "menu":
                lines = []
                for cid, info in CLASSES.items():
                    lines.append(
                        f"{info['emoji']} <b>{info['name']}</b> — "
                        f"<i>{info['bonus']}</i>")
                sent = await cb.message.answer(
                    term_block("CLASSE",
                               "<i>Escolha sua identidade no reino:</i>\n\n"
                               + "\n".join(lines),
                               status="SELECAO", status_color="CYAN"),
                    reply_markup=classe_keyboard())
                register_owner(sent, cb.from_user.id, auto_delete_secs=60.0)
                await cb.answer()
                return
            if sub == "set" and len(parts) > 3:
                cid = parts[3]
                if cid not in CLASSES:
                    await cb.answer()
                    return
                if not await assert_owner(cb):
                    return
                p = ensure_player(chat_id, cb.from_user.id)
                # Detecta se eh a PRIMEIRA escolha (sem class_id ainda) — so
                # renderiza card na 1a vez pra nao spammar troca de classe.
                first_time = not (p.get("class_id"))
                cur.execute("UPDATE players SET class_id=? WHERE chat_id=? AND user_id=?",
                            (cid, chat_id, cb.from_user.id))
                db.commit()
                info = CLASSES[cid]
                await cb.answer(f"Classe: {info['name']} ✓")
                if first_time:
                    try:
                        live = display_name(
                            Message.model_construct(
                                chat=cb.message.chat,
                                from_user=cb.from_user)
                        ) if False else (
                            (cb.from_user.full_name or "").strip()
                            or get_name(chat_id, cb.from_user.id))
                        data = ClasseCardData(
                            royal_id=p.get("royal_id") or "RYL-????",
                            name=live,
                            class_emoji=info.get("emoji", "🎭"),
                            class_name=info.get("name", cid.upper()),
                            class_bonus=info.get("bonus", ""),
                            avatar_slug=p.get("avatar_slug"),
                            season_label=current_season_label(),
                        )
                        card = await asyncio.to_thread(render_classe_card, data)
                        if card:
                            caption = (
                                f"{info['emoji']} <b>{html.escape(info['name'])}</b> "
                                f"// <i>{html.escape(info['bonus'])}</i>"
                            )
                            if len(caption) > 1024:
                                caption = caption[:1020] + "..."
                            await bot.send_photo(
                                chat_id,
                                photo=BufferedInputFile(
                                    card, filename=f"classe-{cid}.jpg"),
                                caption=cap1024(caption),
                                parse_mode="HTML",
                            )
                    except Exception:
                        logger.exception("render_classe_card path failed")
                # Acao terminal: deleta o menu de classes (poluicao zero)
                await delete_msg_safe(cb.message)
                return
            await cb.answer()
            return

        if action == "buy":
            if len(parts) < 3:
                await cb.answer()
                return
            if not await assert_owner(cb):
                return
            iid = parts[2]
            item = ITEMS.get(iid)
            if not item:
                await cb.answer("Item inexistente", show_alert=True)
                return
            p = ensure_player(chat_id, cb.from_user.id)
            if p["gold"] < item["price"]:
                await cb.answer("🪙 Florins insuficientes", show_alert=True)
                return
            # debita atomicamente
            cur.execute(
                "UPDATE players SET gold=gold-? "
                "WHERE chat_id=? AND user_id=? AND gold>=?",
                (item["price"], chat_id, cb.from_user.id, item["price"]))
            if cur.rowcount == 0:
                await cb.answer("🪙 Saldo insuficiente.", show_alert=True)
                return
            cur.execute(
                "INSERT INTO inventory (chat_id, user_id, item_id, qty) VALUES (?, ?, ?, 1) "
                "ON CONFLICT(chat_id, user_id, item_id) DO UPDATE SET qty=qty+1",
                (chat_id, cb.from_user.id, iid))
            db.commit()
            await cb.answer(f"Comprou {item['name']} ✓")
            # Card de drop pra itens raros (>=250) ou lendarios (>=500)
            price = int(item.get("price", 0))
            rarity = None
            if price >= 500:
                rarity = "LENDARIO"
            elif price >= 250:
                rarity = "RARO"
            if rarity:
                try:
                    buyer = ((cb.from_user.full_name or "").strip()
                             or get_name(chat_id, cb.from_user.id))
                    data = LojaDropData(
                        royal_id=p.get("royal_id") or "RYL-????",
                        buyer_name=buyer,
                        item_emoji=item.get("emoji", "📦"),
                        item_name=item.get("name", iid),
                        item_desc=item.get("desc", ""),
                        price=price,
                        rarity=rarity,
                    )
                    card = await asyncio.to_thread(render_loja_drop_card, data)
                    if card:
                        rar_mark = "🟪" if rarity == "LENDARIO" else "🟧"
                        caption = (
                            f"{rar_mark} <b>{html.escape(rarity)}</b> // "
                            f"{html.escape(item.get('name', iid))} "
                            f"— <code>-{price}</code>🪙"
                        )
                        if len(caption) > 1024:
                            caption = caption[:1020] + "..."
                        await bot.send_photo(
                            chat_id,
                            photo=BufferedInputFile(
                                card, filename=f"drop-{iid}.jpg"),
                            caption=cap1024(caption),
                            parse_mode="HTML",
                        )
                except Exception:
                    logger.exception("render_loja_drop_card path failed")
            return

        if action == "atk":
            if len(parts) < 3:
                await cb.answer()
                return
            await handle_boss_attack(cb, int(parts[2]))
            return

        await cb.answer()
    except Exception:
        logger.exception("hub_cb failed for data=%s", cb.data)
        try:
            await cb.answer("Erro interno", show_alert=False)
        except Exception:
            pass


