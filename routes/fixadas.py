"""Telas fixadas por usuário, guardadas no banco (Fase 4, etapa 3, 27/11/2026).

- `GET /api/me/fixadas`: `{"fixadas": [...], "mais_usadas": [...]}`. As fixadas vêm em
  ordem, já filtradas por `Menu.visivel` (permissão tirada some sozinha). As mais
  usadas são o top 3 de `page_views` do usuário nos últimos 30 dias.
- `PUT /api/me/fixadas` com `{"paginas": [...]}`: substitui a lista (ordem = posição).

Rota do próprio usuário: cada um só mexe nas suas (ver `permissoes.ROTAS_DO_PROPRIO_USUARIO`).
O login de emergência (user_id 0) não tem linha em `app_users`: não guarda fixadas.
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request, session

import navegacao
from db import get_conn

logger = logging.getLogger(__name__)

fixadas_bp = Blueprint("fixadas", __name__)

MAX_FIXADAS = 8
MAX_MAIS_USADAS = 3
MSG_USUARIO_ZERO = "Este login não guarda telas fixadas."

# Contagem por tela nos últimos 30 dias (dia contado no fuso de São Paulo).
# O corte em 3 é feito no Python, depois de filtrar o que o menu esconde.
_SQL_MAIS_USADAS = """
    SELECT page, COUNT(*) FROM page_views
    WHERE user_id = %s
      AND (ts AT TIME ZONE 'America/Sao_Paulo') >= (NOW() AT TIME ZONE 'America/Sao_Paulo') - INTERVAL '30 days'
    GROUP BY page ORDER BY COUNT(*) DESC, page
"""


def _erro(msg: str):
    return jsonify({"error": msg}), 400


def _user_id() -> int | None:
    try:
        return int(session.get("user_id"))
    except (TypeError, ValueError):
        return None


@fixadas_bp.route("/api/me/fixadas", methods=["GET"])
def listar():
    uid = _user_id()
    vazio = {"fixadas": [], "mais_usadas": []}
    if not uid:  # None (sem usuário) ou 0 (login de emergência)
        return jsonify(vazio)
    menu = navegacao.menu_do_usuario()
    conhecidas = set(navegacao.CONHECIDAS)
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT page FROM nav_fixadas WHERE user_id = %s ORDER BY ordem, criado_em, page",
                    (uid,),
                )
                fixadas = [r[0] for r in cur.fetchall()]
                cur.execute(_SQL_MAIS_USADAS, (uid,))
                usadas = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as e:
        # O menu não pode quebrar por causa disto: sem fixadas, a seção some.
        logger.warning("Fixadas: não consegui ler do banco: %s", e)
        return jsonify(vazio)
    fixadas = [p for p in fixadas if p in conhecidas and menu.visivel(p)]
    mais_usadas = [p for p in usadas if p in conhecidas and menu.visivel(p)][:MAX_MAIS_USADAS]
    return jsonify({"fixadas": fixadas, "mais_usadas": mais_usadas})


@fixadas_bp.route("/api/me/fixadas", methods=["PUT"])
def salvar():
    uid = _user_id()
    if uid == 0:
        return _erro(MSG_USUARIO_ZERO)
    if not uid:
        return jsonify({"error": "Não autenticado."}), 401
    corpo = request.get_json(silent=True)
    paginas = corpo.get("paginas") if isinstance(corpo, dict) else None
    if not isinstance(paginas, list) or not all(isinstance(p, str) for p in paginas):
        return _erro("Envie 'paginas' como uma lista de textos.")
    if len(set(paginas)) != len(paginas):
        return _erro("A lista tem telas repetidas.")
    if len(paginas) > MAX_FIXADAS:
        return _erro(f"Dá para fixar no máximo {MAX_FIXADAS} telas.")
    menu = navegacao.menu_do_usuario()
    conhecidas = set(navegacao.CONHECIDAS)
    for p in paginas:
        if p not in conhecidas:
            return _erro(f"Tela desconhecida: {p}.")
        if not menu.visivel(p):
            return _erro(f"Sem acesso à tela: {p}.")
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM nav_fixadas WHERE user_id = %s", (uid,))
                for ordem, p in enumerate(paginas):
                    cur.execute(
                        "INSERT INTO nav_fixadas (user_id, page, ordem) VALUES (%s, %s, %s)",
                        (uid, p, ordem),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Fixadas: não consegui salvar: %s", e)
        return jsonify({"error": "Não consegui salvar as telas fixadas. Tente de novo."}), 500
    return jsonify({"fixadas": paginas})
