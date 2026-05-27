# Royal Casorios

Telegram bot construído com aiogram (Python 3.12). Roda como worker (sem frontend), persistindo dados em SQLite local.

**Deploy:** Railway (config em `railway.json`). No Replit fazemos apenas o código — não é necessário rodar/configurar workflow aqui.

## Estrutura
- `main.py` — código principal do bot
- `requirements.txt` — dependências Python
- Requer secret `BOT_TOKEN`

## User preferences
- Sempre usar as versões mais atualizadas das APIs:
  - **Telegram Bot API: 10**
  - **aiogram: última versão estável** (atualmente 3.28.2, lançada em 10/05/2026)
