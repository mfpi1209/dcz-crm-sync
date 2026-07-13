"""
eduit. — Disparador WhatsApp (proxy Flask para tool_whatsapp_alunos).

Reembala as rotas /api/templates e /api/send-message do app externo
(hospedado em WHATSAPP_TOOL_BASE_URL, default banco-disparador-whatsapp.6tqx2r.easypanel.host)
sob /api/disparador_whatsapp/*, herdando a sessão e o controle de
permissões do dcz-crm-sync — só usuários com a página "disparador_whatsapp"
liberada conseguem disparar.

A APP_API_KEY (opcional) do app externo vive em WHATSAPP_TOOL_API_KEY no
.env e é injetada server-side; nunca vaza pro browser.
"""

import os
import logging
from pathlib import Path

import requests as _requests
import psycopg2
from dotenv import load_dotenv
from flask import Blueprint, jsonify, request, session

from db import DB_DSN

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logger = logging.getLogger(__name__)

disparador_whatsapp_bp = Blueprint("disparador_whatsapp_bp", __name__)

TOOL_BASE_URL = os.getenv(
    "WHATSAPP_TOOL_BASE_URL",
    "https://banco-disparador-whatsapp.6tqx2r.easypanel.host",
).rstrip("/")
TOOL_API_KEY = os.getenv("WHATSAPP_TOOL_API_KEY", "").strip()
TOOL_TIMEOUT = int(os.getenv("WHATSAPP_TOOL_TIMEOUT_S", "30"))

PERM_PAGE = "disparador_whatsapp"


def _user_has_permission():
    """Valida server-side se o usuário logado pode usar o disparador.

    Admin sempre pode. Demais usuários precisam ter PERM_PAGE em user_permissions.
    Retorna (ok: bool, status: int, payload: dict|None).
    """
    if not session.get("authenticated"):
        return False, 401, {"error": "Não autenticado"}
    role = (session.get("role") or "").strip().lower()
    if role == "admin":
        return True, 200, None
    uid = session.get("user_id")
    if uid is None or uid == 0:
        return False, 403, {"error": "Usuário sem permissão para esta página"}
    try:
        conn = psycopg2.connect(**DB_DSN)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM user_permissions WHERE user_id = %s AND page = %s LIMIT 1",
                (uid, PERM_PAGE),
            )
            ok = cur.fetchone() is not None
        conn.close()
    except Exception as e:
        logger.warning("Falha verificando permissão %s para uid=%s: %s", PERM_PAGE, uid, e)
        return False, 500, {"error": "Erro interno verificando permissão"}
    if not ok:
        return False, 403, {"error": "Usuário sem permissão para esta página"}
    return True, 200, None


def _proxy_headers():
    h = {"Accept": "application/json"}
    if TOOL_API_KEY:
        h["x-api-key"] = TOOL_API_KEY
        h["Authorization"] = f"Bearer {TOOL_API_KEY}"
    return h


def _log_consultor(action, extra=None):
    uid = session.get("user_id")
    uname = session.get("username") or ""
    msg = f"[disparador_whatsapp] {action} user_id={uid} username={uname}"
    if extra:
        msg += f" {extra}"
    logger.info(msg)


@disparador_whatsapp_bp.route("/api/disparador_whatsapp/templates", methods=["GET"])
def list_templates():
    ok, status, payload = _user_has_permission()
    if not ok:
        return jsonify(payload), status
    try:
        r = _requests.get(
            f"{TOOL_BASE_URL}/api/templates",
            headers=_proxy_headers(),
            timeout=TOOL_TIMEOUT,
        )
    except _requests.RequestException as e:
        logger.error("Falha contatando tool_whatsapp_alunos /api/templates: %s", e)
        return jsonify({"error": "Não foi possível contatar o serviço externo"}), 502
    if r.status_code >= 400:
        logger.warning("tool_whatsapp /api/templates %d: %s", r.status_code, r.text[:200])
    try:
        return jsonify(r.json()), r.status_code
    except ValueError:
        return jsonify({"error": "Resposta inválida do serviço externo"}), 502


@disparador_whatsapp_bp.route("/api/disparador_whatsapp/send-message", methods=["POST"])
def send_message():
    ok, status, payload = _user_has_permission()
    if not ok:
        return jsonify(payload), status

    body = request.get_json(silent=True) or {}
    phone = (body.get("phone") or "").strip()
    template_name = (body.get("templateName") or "").strip()
    if not phone or not template_name:
        return jsonify({"error": "phone e templateName são obrigatórios"}), 400

    forwarded = {
        "phone": phone,
        "templateName": template_name,
        "language": body.get("language") or "pt_BR",
    }
    if isinstance(body.get("variables"), dict):
        forwarded["variables"] = body["variables"]

    _log_consultor("send-message", extra=f"phone={phone} template={template_name}")

    try:
        r = _requests.post(
            f"{TOOL_BASE_URL}/api/send-message",
            json=forwarded,
            headers={**_proxy_headers(), "Content-Type": "application/json"},
            timeout=TOOL_TIMEOUT,
        )
    except _requests.RequestException as e:
        logger.error("Falha contatando tool_whatsapp_alunos /api/send-message: %s", e)
        return jsonify({"success": False, "phone": phone, "error": "Falha de conexão com o serviço externo"}), 502
    try:
        data = r.json()
    except ValueError:
        return jsonify({"success": False, "phone": phone, "error": "Resposta inválida do serviço externo"}), 502
    return jsonify(data), r.status_code


@disparador_whatsapp_bp.route("/api/disparador_whatsapp/consultores-academicos", methods=["GET"])
def api_consultores_academicos():
    """Catálogo de consultores acadêmicos (app_users) para Metas / Meu Painel."""
    ok, status, payload = _user_has_permission()
    if not ok:
        return jsonify(payload), status
    from helpers import list_consultores_academicos, user_has_disparador_full_access
    role, categoria = _nav_load_user_data_from_session()
    if not user_has_disparador_full_access(role, categoria):
        return jsonify({"error": "Apenas admin ou Supervisor Acadêmico."}), 403
    return jsonify({"ok": True, "consultores": list_consultores_academicos()})


def _nav_load_user_data_from_session():
    """Mini helper — categoria do usuário logado."""
    categoria = ""
    uid = session.get("user_id")
    if uid:
        try:
            conn = psycopg2.connect(**DB_DSN)
            with conn.cursor() as cur:
                cur.execute("SELECT categoria FROM app_users WHERE id = %s", (uid,))
                r = cur.fetchone()
                if r:
                    categoria = r[0] or ""
            conn.close()
        except Exception:
            pass
    return session.get("role", ""), categoria


@disparador_whatsapp_bp.route("/api/disparador_whatsapp/health", methods=["GET"])
def health():
    ok, status, payload = _user_has_permission()
    if not ok:
        return jsonify(payload), status
    try:
        r = _requests.get(
            f"{TOOL_BASE_URL}/api/health",
            headers=_proxy_headers(),
            timeout=10,
        )
        return jsonify({"base_url": TOOL_BASE_URL, "upstream": r.json()}), r.status_code
    except _requests.RequestException as e:
        return jsonify({"base_url": TOOL_BASE_URL, "error": str(e)}), 502
