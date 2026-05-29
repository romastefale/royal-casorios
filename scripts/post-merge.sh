#!/bin/bash
# Post-merge setup para RPG - Royal para Geeks.
# Idempotente, non-interactive, fail-fast. Roda no Replit (deploy real e no Railway).
set -e

echo "[post-merge] instalando dependencias (runtime + dev)"
# Usa o wrapper 'pip' do Replit (instala em .pythonlibs); 'python -m pip' eh
# bloqueado pelo Nix (externally-managed-environment).
pip install -q --no-input -r requirements.txt -r requirements-dev.txt

echo "[post-merge] sanity compile dos modulos do bot"
python -m py_compile main.py royal_render.py royal_words.py

echo "[post-merge] ok"
