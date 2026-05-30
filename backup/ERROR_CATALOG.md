# Catálogo de erros — mapa dos casos importantes

Fonte-de-verdade em código: `ERROR_CATALOG` em **`royal/alerts.py`**. Este arquivo é o
espelho legível — **mantenha os dois em sincronia** ao adicionar/editar casos.

Cada erro de nível `ERROR`/`CRITICAL` (nossos `logger.exception` + as exceções não
tratadas que o aiogram loga) passa pelo classificador. A 1ª entrada que casar (por tipo
de exceção ou substring na msg/traceback) decide o veredito. Se nada casar: é
**relevante** quando há traceback (exceção real) ou é `CRITICAL`; senão é ruído.

`relevante = SIM` → grava em `backup/alerts/` **e** manda DM pro dono (com dedupe/TTL).
`relevante = NÃO` → fica só no stdout/ring/`backup/logs` (sem DM).

---

## 🟢 Transitórios / esperados — NÃO mandam DM

| id | caso | casa por | como agir |
|---|---|---|---|
| `flood_control` | Flood control / `TelegramRetryAfter` (rate-limit) | `TelegramRetryAfter`; "flood control exceeded", "too many requests", "retry after" | Já tratado (espera + retry). Só agir se for **constante**: espaçar uploads (identity sweep) ou reduzir msgs/s. |
| `callback_expired` | Callback clicado, `cb.answer()` tarde demais | "query is too old", "query id is invalid", "response timeout expired" | Benigno (usuário só não vê o spinner sumir). Só agir se a lentidão for crônica. |
| `message_edit_noop` | Editar/apagar msg que mudou/sumiu | "message is not modified", "message to edit not found", "message to delete not found", "message_id_invalid" | Esperado no fluxo de edição in-place. Nada a corrigir. |
| `user_unreachable` | Bot bloqueado / sem permissão / chat sumiu | `TelegramForbidden`; "bot was blocked by the user", "chat not found", "not enough rights" | Esperado. Não dá pra corrigir no código. |
| `network_transient` | Rede/servidor do Telegram instável | `TelegramNetworkError`, `TelegramServerError`, `TimeoutError`; "bad gateway", "service is unavailable" | O polling reconecta sozinho. Só agir se persistir. |

## 🔴 Relevantes — mandam DM pro dono

| id | caso | casa por | como corrigir |
|---|---|---|---|
| `code_bug` | Bug de código (exceção não tratada) | `NameError`, `AttributeError`, `KeyError`, `IndexError`, `TypeError`, `ValueError`, `UnboundLocalError`, `AssertionError` | Olhar `arquivo:linha` no traceback, reproduzir e corrigir. `ruff check --select F` pega nomes indefinidos. |
| `import_error` | Import/dependência quebrada | `ImportError`, `ModuleNotFoundError`; "no module named" | Conferir `requirements.txt` e os imports do módulo do trace. |
| `db_error` | Erro de banco (SQLite) | `OperationalError`, `IntegrityError`, `DatabaseError`; "database is locked", "no such column/table", "unique constraint", "malformed" | Conferir migrations, `integrity_check` e se o volume `/data` está montado. "database is locked" = transação presa. |
| `boot_migration` | Falha de boot / migration | "migration", "bootstrap", "integrity_check", "schema" | Falha crítica de subida — sem isso o bot não serve. Conferir logs de boot e o volume. |
| `uncategorized` | Exceção com traceback fora do catálogo | (fallback) | Investigar o trace; se recorrente, **adicionar entrada** aqui e em `royal/alerts.py`. |

---

## Casos reais que originaram este sistema (logs Railway, 28/mai)

1. **`NameError: name 'TZ' is not defined`** em `send_couple` (monólito antigo) →
   `code_bug`. **Já corrigido** no código modular: usa `datetime.now(ZoneInfo(TZ_NAME))`.
2. **`TelegramRetryAfter: Flood control` em `SendPhoto`** (identity card → STASH) →
   `flood_control`. **Corrigido**: `ensure_identity_card_async` agora trata o RetryAfter
   (espera `retry_after` e tenta 1x de novo) em vez de cair no `except` genérico.
3. **`TelegramBadRequest: query is too old`** em `hub_cb` → `callback_expired`.
   **Corrigido**: o `except` do `hub_cb` usa `is_benign_telegram_error()` e não loga
   mais isso como `ERROR` (some o ruído; não dispara alerta).
