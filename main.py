"""
Royal RPG — Telegram bot (aiogram 3.28)
========================================

Bot completo de RPG do grupo Royal:
- Shipper original (casamentos automaticos) preservado
- Sistema RPG: Royal ID, perfil, XP, atributos, classes, economia
- Mini-game Palavra da Hora
- Boss semanal cooperativo
- Temporadas com Hall da Fama

Roda como worker (sem frontend) em Railway com volume SQLite alocado.

Este arquivo e o ENTRYPOINT + facade: o codigo real mora no pacote `royal/`
(config / core / jobs / handlers.*). Mantemos os re-exports abaixo pra que os
testes (que fazem `import main`) e qualquer integracao externa continuem
enxergando os simbolos publicos pelo namespace `main`.
"""
import asyncio

import royal_avatars  # re-export p/ testes (main.royal_avatars)
from royal.config import *  # noqa: F401,F403
from royal.core import *  # noqa: F401,F403
from royal.core import (  # noqa: F401  nomes "privados" usados em testes/entrypoint
    bot,
    dp,
    logger,
    _LEVEL_TABLE,
    _identity_card_hash,
    _require_user_for_commands,
    _block_bot_callbacks,
    _CHAT_MIGRATE_PK_TABLES,
    _CHAT_MIGRATE_FK_TABLES,
    _tutorial_kb,
    _avatar_kb,
)
from royal.config import BOT_TOKEN  # noqa: F401
from royal.jobs import (  # noqa: F401
    register_bot_commands,
    start_health_server,
    flush_buffers,
    cleanup_job,
    scheduler,
    healthcheck,
    identity_card_sweep_job,
    cleanup_legacy_bot_players,
    announce_typewriter_feature,
    announce_no_bots_feature,
    announce_update_greeting,
    log_dump_job,
    backup_job,
    mira_palavras_job,
)
from royal.mira import *  # noqa: F401,F403  re-export p/ testes (main.parse_words etc.)

# Importa as features (cada uma expoe um aiogram Router) e registra na ordem.
# Em aiogram o 1o router (na ordem de include) cujo handler casa VENCE e PARA a
# propagacao. Duas restricoes de ORDEM precisam ser respeitadas (o resto e
# disjunto: comandos/botoes/prefixos de callback unicos):
#   1. `system` POR ULTIMO — o catch-all `track` (+ migracao/dice/posts de bot
#      de musica) so deve vencer depois que os handlers especificos tentarem.
#   2. `hub_cb` e um catch-all `r:` & ~_HUB_DEDICATED_PREFIXES. Como o 1o router
#      que casa VENCE e PARA a propagacao (sem SkipHandler), os prefixos com
#      handler DEDICADO precisam estar em _HUB_DEDICATED_PREFIXES, senao o hub_cb
#      os engole e o botao morre em silencio (`await cb.answer()` no fim do
#      hub_cb, sem acao). Hoje a lista cobre "r:inv:","r:priv:","r:dados:",
#      "r:quest:" (Resgatar de missao) e "r:cfg:" (toggles do /royalconfig).
#      ⚠️ Ao criar um handler dedicado p/ um novo prefixo `r:...`, ADICIONE-O em
#      _HUB_DEDICATED_PREFIXES (royal/core.py) ou o hub_cb o intercepta.
from royal.handlers import (  # noqa: E402
    start, hub, perfil, avatar, classe, inventario, loja, economia, ranking,
    palavra, boss, casorios, privacidade, inline, missoes, eventos,
    conquistas, cfg, admin, inteligencia, quiz, system,
)

_FEATURE_ROUTERS = (
    start, hub, perfil, avatar, classe, inventario, loja, economia, ranking,
    palavra, boss, casorios, privacidade, inline, missoes, eventos,
    conquistas, cfg, admin, inteligencia, quiz, system,
)
for _feature in _FEATURE_ROUTERS:
    dp.include_router(_feature.router)


async def main():
    if not BOT_TOKEN or bot is None:
        raise RuntimeError("BOT_TOKEN environment variable is required")
    logger.info("Royal RPG starting")
    await register_bot_commands()
    await start_health_server()  # F13: /health na $PORT (no-op sem PORT)
    asyncio.create_task(flush_buffers())
    asyncio.create_task(cleanup_job())
    asyncio.create_task(scheduler())
    asyncio.create_task(healthcheck())
    asyncio.create_task(identity_card_sweep_job())
    cleanup_legacy_bot_players()  # one-shot: remove bots cadastrados antes do filtro
    asyncio.create_task(announce_typewriter_feature())
    asyncio.create_task(announce_no_bots_feature())
    asyncio.create_task(announce_update_greeting())  # saudacao de atualizacao (1x, fixa no grupo)
    asyncio.create_task(log_dump_job())
    asyncio.create_task(backup_job())  # F10: backup diario do DB
    asyncio.create_task(mira_palavras_job())  # Inteligencia: palavras do dia (@Mira)
    # allowed_updates resolvido dos handlers registrados — inclui
    # automaticamente "message_reaction" (M06) pq ha @router.message_reaction.
    allowed = dp.resolve_used_update_types()
    logger.info("polling allowed_updates=%s", allowed)
    await dp.start_polling(bot, allowed_updates=allowed)


if __name__ == "__main__":
    asyncio.run(main())
