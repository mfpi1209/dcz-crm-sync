"""Troca de senha pelo próprio usuário e troca obrigatória (26/11).

- `validar_nova_senha`: regras da nova senha (tamanho, confirmação, diferente da atual).
- `bloqueio`: chamado no `require_auth`. Com `session["precisa_trocar_senha"]`
  ligado, página vai para `/trocar-senha` e `/api/*` recebe 403 em JSON.
- `garantir_coluna`: cria `app_users.precisa_trocar_senha` no boot. Só cria a
  coluna (FALSE para todos); ninguém é marcado automaticamente. Para obrigar um
  usuário a trocar: `UPDATE app_users SET precisa_trocar_senha = TRUE WHERE id = ...`.
"""
from __future__ import annotations

import logging

from flask import jsonify, redirect, request, session

logger = logging.getLogger(__name__)

URL = "/trocar-senha"
TAMANHO_MINIMO = 8
TAMANHO_MAXIMO = 128
CHAVE_SESSAO = "precisa_trocar_senha"

# Com a flag ligada, só isto passa: a própria troca, sair, entrar, voltar ao admin
# (Entrar como) e o que não é do usuário.
_LIVRES = {URL, "/logout", "/login", "/health", "/voltar-admin"}

SQL_COLUNA = """
    ALTER TABLE app_users
    ADD COLUMN IF NOT EXISTS precisa_trocar_senha BOOLEAN NOT NULL DEFAULT FALSE
"""


def validar_nova_senha(atual: str, nova: str, confirmacao: str) -> str | None:
    """Devolve a mensagem de erro, ou None se a nova senha pode ser gravada."""
    if not atual:
        return "Informe a senha atual."
    if not nova:
        return "Informe a nova senha."
    if len(nova) < TAMANHO_MINIMO:
        return f"A nova senha precisa ter pelo menos {TAMANHO_MINIMO} caracteres."
    if len(nova) > TAMANHO_MAXIMO:
        return f"A nova senha pode ter no máximo {TAMANHO_MAXIMO} caracteres."
    if nova != confirmacao:
        return "A nova senha e a confirmação não conferem."
    if nova == atual:
        return "A nova senha precisa ser diferente da atual."
    return None


def bloqueio():
    """Resposta que barra a navegação enquanto a troca é obrigatória (ou None)."""
    if not session.get(CHAVE_SESSAO):
        return None
    caminho = request.path
    if caminho in _LIVRES or caminho.startswith("/static/"):
        return None
    if caminho.startswith("/api/"):
        return jsonify({"error": "Troque a sua senha antes de continuar.", "redirect": URL}), 403
    return redirect(URL)


def garantir_coluna(get_conn) -> None:
    """Cria a coluna da flag. Falha só avisa no log: não derruba o boot."""
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(SQL_COLUNA)
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Não consegui criar app_users.precisa_trocar_senha: %s", e)
