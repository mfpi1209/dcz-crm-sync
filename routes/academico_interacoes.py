"""Interações Acadêmicas: resumo da tabela academico_interacoes.

A tabela fica em um projeto Supabase SEPARADO do principal, configurado via:
    SUPABASE_ACADEMICO_URL / SUPABASE_ACADEMICO_KEY  (.env)

GET /api/academico-interacoes/resumo
    Retorna contagens de interagiu true/false e a lista de telefones
    dos registros com interagiu=false (exceto os de telefone vazio,
    que entram apenas na contagem 'sem_telefone').

GET  /api/academico-interacoes/atender-preview?telefone=
POST /api/academico-interacoes/atender  {telefone}
    Casa o usuário logado no painel com o operador da org no CRM EduIT
    (nome/e-mail) e, no POST, atribui o lead + abre a URL do funil.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.parse
import urllib.request

from flask import Blueprint, jsonify, request, session

from db import get_conn
from helpers import display_name_from_login
from services.eduit_crm import (
    EduitCrmError,
    assign_lead_to_user,
    configured as eduit_crm_configured,
    lookup_lead,
    resolve_crm_user,
)

logger = logging.getLogger(__name__)
academico_interacoes_bp = Blueprint("academico_interacoes_bp", __name__)

_TABLE = "academico_interacoes"
_PAGE_SIZE = 1000


def _fetch_all_rows(de: str = "", ate: str = "") -> list[dict]:
    base = os.getenv("SUPABASE_ACADEMICO_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_ACADEMICO_KEY", "")
    if not base or not key:
        raise RuntimeError("SUPABASE_ACADEMICO_URL/SUPABASE_ACADEMICO_KEY não configurados no .env")

    url = f"{base}/rest/v1/{_TABLE}?select=id,data,telefone,rgm,interagiu,ligacao_feita&order=id.asc"
    if de:
        url += f"&data=gte.{de}T00:00:00Z"
    if ate:
        url += f"&data=lte.{ate}T23:59:59Z"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Range-Unit": "items",
        "User-Agent": "dcz-crm-sync/1.0",
    }
    rows: list[dict] = []
    offset = 0
    while True:
        req = urllib.request.Request(
            url, headers={**headers, "Range": f"{offset}-{offset + _PAGE_SIZE - 1}"}
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            batch = json.loads(r.read().decode("utf-8"))
        rows.extend(batch)
        if len(batch) < _PAGE_SIZE:
            break
        offset += _PAGE_SIZE
    return rows


@academico_interacoes_bp.route("/api/academico-interacoes/resumo", methods=["GET"])
def resumo():
    if not session.get("role"):
        return jsonify({"error": "Não autenticado"}), 401
    de = (request.args.get("de") or "").strip()
    ate = (request.args.get("ate") or "").strip()
    try:
        rows = _fetch_all_rows(de, ate)
    except Exception as e:
        logger.exception("academico_interacoes: falha ao consultar Supabase")
        return jsonify({"error": str(e)}), 502

    interagiram = 0
    sem_telefone = 0
    contatos: dict[str, dict] = {}
    for row in rows:
        if row.get("interagiu") is True:
            interagiram += 1
            continue
        tel = (row.get("telefone") or "").strip()
        if not tel:
            sem_telefone += 1
            continue
        # dedupe por telefone, mantendo o registro mais recente
        if tel not in contatos or (row.get("data") or "") > (contatos[tel].get("data") or ""):
            contatos[tel] = {
                "telefone": tel,
                "rgm": row.get("rgm"),
                "data": row.get("data"),
                "ligacao_feita": row.get("ligacao_feita"),
            }

    lista = sorted(contatos.values(), key=lambda c: c.get("data") or "", reverse=True)
    return jsonify({
        "total": len(rows),
        "interagiram": interagiram,
        "nao_interagiram": len(rows) - interagiram,
        "sem_telefone": sem_telefone,
        "contatos": lista,
    })


_STATUS_VALIDOS = ("atendido", "não atendido")


@academico_interacoes_bp.route("/api/academico-interacoes/ligacao", methods=["POST"])
def registrar_ligacao():
    if not session.get("role"):
        return jsonify({"error": "Não autenticado"}), 401
    body = request.get_json(silent=True) or {}
    telefone = (body.get("telefone") or "").strip()
    status = (body.get("status") or "").strip()
    if not telefone or status not in _STATUS_VALIDOS:
        return jsonify({"error": "telefone e status ('atendido'/'não atendido') são obrigatórios"}), 400

    base = os.getenv("SUPABASE_ACADEMICO_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_ACADEMICO_KEY", "")
    if not base or not key:
        return jsonify({"error": "SUPABASE_ACADEMICO_URL/SUPABASE_ACADEMICO_KEY não configurados no .env"}), 500

    # marca TODAS as linhas desse telefone (a lista é deduplicada por telefone)
    url = f"{base}/rest/v1/{_TABLE}?telefone=eq.{urllib.parse.quote(telefone, safe='')}"
    req = urllib.request.Request(
        url,
        data=json.dumps({"ligacao_feita": status}).encode("utf-8"),
        method="PATCH",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
            "User-Agent": "dcz-crm-sync/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30):
            pass
    except Exception as e:
        logger.exception("academico_interacoes: falha ao registrar ligação")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "telefone": telefone, "ligacao_feita": status})


def _session_dashboard_user() -> dict:
    """Login do painel (app_users) usado para casar com o operador do CRM."""
    uid = session.get("user_id") or 0
    username = (session.get("username") or "").strip()
    email = ""
    categoria = None
    if uid and uid != 0:
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT username, email_cruzeiro, categoria FROM app_users WHERE id = %s",
                    (uid,),
                )
                row = cur.fetchone()
            conn.close()
            if row:
                username = (row[0] or username or "").strip()
                email = (row[1] or "").strip()
                categoria = row[2]
        except Exception:
            logger.exception("academico_interacoes: falha ao ler app_users")
    display = display_name_from_login(username, email)
    return {
        "user_id": uid,
        "username": username,
        "email": email,
        "categoria": categoria,
        "display_name": display,
    }


def _crm_user_for_session(dash: dict) -> dict:
    return resolve_crm_user(dash.get("username") or "", dash.get("email") or "")


@academico_interacoes_bp.route("/api/academico-interacoes/atender-preview", methods=["GET"])
def atender_preview():
    if not session.get("role"):
        return jsonify({"error": "Não autenticado"}), 401
    telefone = (request.args.get("telefone") or "").strip()
    if not telefone:
        return jsonify({"error": "telefone é obrigatório"}), 400
    if not eduit_crm_configured():
        return jsonify({"error": "EDUIT_CRM_TOKEN não configurado no .env"}), 503

    dash = _session_dashboard_user()
    try:
        crm_user = _crm_user_for_session(dash)
        lead = lookup_lead(telefone)
    except EduitCrmError as e:
        status = e.status if e.status in (400, 404, 409, 503) else 502
        return jsonify({
            "error": str(e),
            "dashboard_user": dash,
        }), status

    return jsonify({
        "ok": True,
        "dashboard_user": dash,
        "crm_user": crm_user,
        "lead": lead,
    })


@academico_interacoes_bp.route("/api/academico-interacoes/atender", methods=["POST"])
def atender():
    if not session.get("role"):
        return jsonify({"error": "Não autenticado"}), 401
    body = request.get_json(silent=True) or {}
    telefone = (body.get("telefone") or "").strip()
    if not telefone:
        return jsonify({"error": "telefone é obrigatório"}), 400
    if not eduit_crm_configured():
        return jsonify({"error": "EDUIT_CRM_TOKEN não configurado no .env"}), 503

    dash = _session_dashboard_user()
    try:
        crm_user = _crm_user_for_session(dash)
        result = assign_lead_to_user(telefone, crm_user)
    except EduitCrmError as e:
        status = e.status if e.status in (400, 404, 409, 503) else 502
        return jsonify({
            "error": str(e),
            "dashboard_user": dash,
        }), status
    except Exception:
        logger.exception("academico_interacoes: falha ao atribuir lead no CRM")
        return jsonify({"error": "Falha ao atribuir o lead no CRM EduIT"}), 502

    return jsonify({
        "ok": True,
        "dashboard_user": dash,
        "crm_user": crm_user,
        "lead": result,
        "crm_url": result.get("crm_url"),
    })
