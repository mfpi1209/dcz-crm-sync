"""Tracking de page views do dashboard.

POST /api/track-page-view          -> registra que o usuario logado entrou em uma pagina
GET  /api/page-views/stats         -> agregados (ranking de paginas, por usuario, evolucao)
GET  /api/page-views/timeline      -> serie diaria por pagina (top N) para grafico
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, session

from db import DB_DSN
from helpers import ALL_PAGES

logger = logging.getLogger(__name__)
page_views_bp = Blueprint("page_views_bp", __name__)


def _conn():
    return psycopg2.connect(**DB_DSN)


def _resolve_period(start: str | None, end: str | None) -> tuple[str, str]:
    """Defaults: ultimos 30 dias (BRT)."""
    today = datetime.now(timezone(timedelta(hours=-3))).date()
    try:
        if start:
            datetime.strptime(start, "%Y-%m-%d")
        else:
            start = (today - timedelta(days=29)).strftime("%Y-%m-%d")
        if end:
            datetime.strptime(end, "%Y-%m-%d")
        else:
            end = today.strftime("%Y-%m-%d")
    except ValueError:
        start = (today - timedelta(days=29)).strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
    return start, end


@page_views_bp.route("/api/track-page-view", methods=["POST"])
def track_page_view():
    """Registra um acesso a pagina (fire-and-forget)."""
    if not session.get("user_id"):
        # Sem sessao logada: ignora silenciosamente
        return ("", 204)
    data = request.get_json(silent=True) or {}
    page = (data.get("page") or "").strip().lower()
    if not page or len(page) > 100:
        return jsonify({"ok": False, "error": "page invalido"}), 400
    if page not in ALL_PAGES and page != "profile":
        # Aceita 'profile' (nao esta em ALL_PAGES) mas barra valores aleatorios
        return jsonify({"ok": False, "error": "page desconhecido"}), 400
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO page_views (user_id, username, role, page) VALUES (%s, %s, %s, %s)",
                (
                    session.get("user_id"),
                    session.get("username"),
                    session.get("role"),
                    page,
                ),
            )
        conn.commit()
        conn.close()
        return ("", 204)
    except Exception as e:
        logger.warning("track_page_view: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


def _admin_only():
    if session.get("role") != "admin":
        return jsonify({"error": "Acesso negado"}), 403
    return None


@page_views_bp.route("/api/page-views/stats")
def page_views_stats():
    """Agregados para o dashboard de uso. Somente admin."""
    deny = _admin_only()
    if deny:
        return deny
    start, end = _resolve_period(
        (request.args.get("start_date") or "").strip(),
        (request.args.get("end_date") or "").strip(),
    )
    try:
        conn = _conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT page, COUNT(*) AS hits, COUNT(DISTINCT user_id) AS users
                FROM page_views
                WHERE (ts AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN %s::date AND %s::date
                GROUP BY page
                ORDER BY hits DESC
                """,
                (start, end),
            )
            por_pagina = [dict(r) for r in cur.fetchall()]

            # Paginas com 0 acessos: lista usuarios com permissao
            cur.execute(
                """
                SELECT u.username, u.role, up.page
                FROM app_users u
                LEFT JOIN user_permissions up ON up.user_id = u.id
                WHERE u.role = 'admin' OR up.page IS NOT NULL
                """
            )
            # Constroi mapa pagina -> [usuarios com acesso]
            from collections import defaultdict
            admins = []
            page_to_users: dict = defaultdict(list)
            seen_admins = set()
            for row in cur.fetchall():
                username = row["username"]
                role = row["role"]
                page = row["page"]
                if role == "admin":
                    if username not in seen_admins:
                        admins.append({"username": username, "role": role})
                        seen_admins.add(username)
                elif page:
                    page_to_users[page].append({"username": username, "role": role})

            paginas_acessadas = {r["page"] for r in por_pagina}
            nao_acessadas = []
            for p in ALL_PAGES:
                if p in paginas_acessadas:
                    continue
                # Lista: todos os admins + usuarios com permissao explicita
                permitidos = list(admins) + page_to_users.get(p, [])
                # dedupe por username
                seen = set(); dedup = []
                for u in permitidos:
                    if u["username"] not in seen:
                        seen.add(u["username"])
                        dedup.append(u)
                nao_acessadas.append({
                    "page": p,
                    "users_with_access": sorted(dedup, key=lambda x: (x["role"] != "admin", x["username"] or "")),
                })
            # Ordena: paginas com menos permissoes primeiro (mais "esquecidas")
            nao_acessadas.sort(key=lambda x: (len(x["users_with_access"]), x["page"]))

            cur.execute(
                """
                SELECT
                    COUNT(*)               AS total_hits,
                    COUNT(DISTINCT user_id) AS total_users,
                    COUNT(DISTINCT page)   AS total_pages,
                    COUNT(DISTINCT (ts AT TIME ZONE 'America/Sao_Paulo')::date) AS dias_ativos
                FROM page_views
                WHERE (ts AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN %s::date AND %s::date
                """,
                (start, end),
            )
            totais = dict(cur.fetchone() or {})

        conn.close()
        return jsonify({
            "ok": True,
            "start_date": start,
            "end_date": end,
            "totais": totais,
            "por_pagina": por_pagina,
            "nao_acessadas": nao_acessadas,
        })
    except Exception as e:
        logger.exception("page_views_stats")
        return jsonify({"ok": False, "error": str(e)}), 500


@page_views_bp.route("/api/page-views/timeline")
def page_views_timeline():
    """Serie diaria: hits por dia para as N paginas mais acessadas. Somente admin."""
    deny = _admin_only()
    if deny:
        return deny
    start, end = _resolve_period(
        (request.args.get("start_date") or "").strip(),
        (request.args.get("end_date") or "").strip(),
    )
    try:
        top_n = max(1, min(int(request.args.get("top", "8")), 20))
    except Exception:
        top_n = 8
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT page
                FROM page_views
                WHERE (ts AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN %s::date AND %s::date
                GROUP BY page
                ORDER BY COUNT(*) DESC
                LIMIT %s
                """,
                (start, end, top_n),
            )
            top_pages = [r[0] for r in cur.fetchall()]

            cur.execute(
                """
                SELECT (ts AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                       page,
                       COUNT(*) AS hits
                FROM page_views
                WHERE (ts AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN %s::date AND %s::date
                  AND page = ANY(%s)
                GROUP BY 1, 2
                ORDER BY 1
                """,
                (start, end, top_pages),
            )
            rows = cur.fetchall()
        conn.close()

        # Monta serie [{dia, total, page1, page2, ...}]
        from collections import defaultdict
        by_day: dict = defaultdict(lambda: {p: 0 for p in top_pages})
        for dia, page, hits in rows:
            by_day[dia][page] = int(hits)
        dias = sorted(by_day.keys())
        serie = [
            {"dia": d.strftime("%Y-%m-%d"), **{p: by_day[d].get(p, 0) for p in top_pages}}
            for d in dias
        ]
        return jsonify({
            "ok": True,
            "start_date": start,
            "end_date": end,
            "top_pages": top_pages,
            "serie": serie,
        })
    except Exception as e:
        logger.exception("page_views_timeline")
        return jsonify({"ok": False, "error": str(e)}), 500
