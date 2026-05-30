# `backup/` — logs e alertas do bot

Esta pasta substitui o antigo dump de log na **DM do dono a cada 5 min**. Agora:

- **`backup/logs/`** — snapshots periódicos do ring buffer (`royal-rpg-<ts>.log`),
  gravados pelo `log_dump_job` a cada `LOG_DUMP_INTERVAL_SEC` (300s). Rotação mantém
  os últimos `LOG_BACKUP_KEEP` (50) arquivos. É o histórico completo, sem floodar a DM.
- **`backup/alerts/`** — um arquivo por **erro relevante** detectado (`<ts>-<id>.txt`),
  com caso, assinatura, traceback e como corrigir. Gerado pelo `OwnerAlertHandler`.

> O repositório é privado, então **logs com user_id/chat_id podem ficar aqui** sem
> problema de PII vazar. Mesmo assim, os arquivos de runtime (`*.log`, `*.txt`) são
> ignorados pelo `.gitignore` — só a estrutura da pasta + a documentação são versionadas
> (no Railway os arquivos são gerados em disco, não voltam pro git automaticamente).

## Onde os arquivos ficam (persistência)

`LOG_BACKUP_DIR` (default `backup`) define a raiz. O container do Railway é **efêmero**
(ver `replit.md` § Persistência) — pra os logs sobreviverem a redeploys, aponte pro
volume: `LOG_BACKUP_DIR=/data/backup`.

## Sistema de alerta (só DM quando importa)

O dono **não recebe mais o log inteiro na DM**. Em vez disso, um classificador
(`royal/alerts.py`) olha todo erro de nível `ERROR`/`CRITICAL` (inclusive as exceções
não tratadas que o aiogram loga) e **só manda DM quando o erro é de verdade relevante**
(bug de código, falha de DB, import quebrado, falha de boot). Erros transitórios/esperados
do Telegram (rate-limit, callback expirado, msg não modificada, bot bloqueado, rede)
**não geram DM** — ficam só nos arquivos/stdout.

Há **dedupe por assinatura + TTL** (`OWNER_ALERT_TTL_SEC`, default 30 min): o mesmo erro
não dispara DM repetida dentro da janela.

O mapa completo dos casos está em [`ERROR_CATALOG.md`](ERROR_CATALOG.md) — espelho em
documentação do `ERROR_CATALOG` de `royal/alerts.py` (mantenha os dois em sincronia).

## Env vars relacionadas

| Var | Default | Para que serve |
|---|---|---|
| `LOG_BACKUP_DIR` | `backup` | Raiz dos logs/alertas. Use `/data/backup` no Railway p/ persistir. |
| `LOG_BACKUP_KEEP` | `50` | Quantos snapshots de log manter (rotação). |
| `OWNER_ALERTS_ENABLED` | `1` | `0` desliga as DMs de alerta (mantém os arquivos). |
| `OWNER_ALERT_TTL_SEC` | `1800` | Janela de dedupe por assinatura de erro (segundos). |
| `LOG_DUMP_INTERVAL_SEC` | `300` | Intervalo dos snapshots em `backup/logs/`. |
| `LOG_DUMP_ENABLED` | `1` | `0` desliga o snapshot periódico (mantém `/royallog`). |
