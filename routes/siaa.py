"""SIAA: scraping direto ao SIAA + persistência no Supabase.

GET /api/siaa/aluno/<rgm>?refresh=0|1
    Consulta dados do aluno no sistema SIAA.
    - refresh=1  força nova captura ao vivo via HTTP (pode demorar ~60 s)
    - refresh=0  retorna ultima captura em cache do Supabase (default)

Env vars usadas:
    SUPABASE_URL, SUPABASE_KEY     — obrigatórias para leitura/escrita
    SIAA_SESSION_URL               — usado pelo siaa_http_client para buscar cookies
    SIAA_SESSION_TOKEN             — token de autenticação do servidor de sessão
"""
from __future__ import annotations

import logging
import os
import re

import requests
from flask import Blueprint, jsonify, request, session

from siaa.parser import process_siaa_http_rgm
from siaa.insights import (
    build_insights, fetch_latest_captura, fetch_documentos,
    fetch_titulos, supabase_configured, normalize_rgm,
)

logger = logging.getLogger(__name__)
siaa_bp = Blueprint("siaa_bp", __name__)

SIAA_URL = (os.environ.get("SIAA_SESSION_URL", "") or "").rstrip("/")
SIAA_TOKEN = os.environ.get("SIAA_SESSION_TOKEN", "") or ""


@siaa_bp.route("/api/siaa/aluno/<rgm>", methods=["GET"])
def api_siaa_aluno(rgm):
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "forbidden"}), 403
    rgm_norm = normalize_rgm(rgm)
    if not rgm_norm:
        return jsonify({"ok": False, "error": "rgm invalido"}), 400
    if not supabase_configured():
        return jsonify({"ok": False, "error": "SUPABASE_URL e SUPABASE_KEY nao configurados"}), 500

    do_refresh = request.args.get("refresh", "0") == "1"
    refresh_status = None
    refresh_message = None
    if do_refresh:
        try:
            process_siaa_http_rgm(rgm_norm, save_supabase=True, debug=False)
            refresh_status = "ok"
            refresh_message = "Consulta online ao SIAA realizada automaticamente."
        except Exception as exc:
            logger.warning("api_siaa_aluno refresh falhou rgm=%s: %s", rgm_norm, exc)
            refresh_status = "fail"
            refresh_message = "Nao foi possivel consultar o SIAA agora. Exibindo a ultima captura disponivel."

    captura, err = fetch_latest_captura(rgm_norm)
    if err:
        return jsonify({"ok": False, "error": err, "rgm": rgm_norm}), 502

    if not captura:
        return jsonify({
            "ok": True,
            "found": False,
            "rgm": rgm_norm,
            "refresh_status": refresh_status,
            "refresh_message": refresh_message,
        })

    captura_id = captura.get("id")
    documentos, doc_err = fetch_documentos(captura_id)
    titulos, tit_err = fetch_titulos(captura_id)
    if doc_err or tit_err:
        return jsonify({
            "ok": False,
            "error": " | ".join([e for e in (doc_err, tit_err) if e]),
            "rgm": rgm_norm,
        }), 502

    return jsonify({
        "ok": True,
        "found": True,
        "rgm": rgm_norm,
        "refresh_status": refresh_status,
        "refresh_message": refresh_message,
        "insights": build_insights(captura, documentos, titulos),
    })


# ---------------------------------------------------------------------------
# Sessao SIAA — salvar / testar cookies (admin-only)
# ---------------------------------------------------------------------------

def _siaa_sessao_forbidden():
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return None


def _validar_sessao_payload(data):
    module = (data.get("module") or "").strip().lower()
    cookie = (data.get("cookie") or "").strip()
    if module not in ("academico", "financeiro"):
        return None, None, (jsonify({"ok": False, "error": "module invalido"}), 400)
    if not cookie:
        return None, None, (jsonify({"ok": False, "error": "cookie ausente"}), 400)
    return module, cookie, None


@siaa_bp.route("/siaa-sessao/salvar", methods=["POST"])
def siaa_sessao_salvar():
    forbidden = _siaa_sessao_forbidden()
    if forbidden is not None:
        return forbidden

    if not SIAA_URL or not SIAA_TOKEN:
        return jsonify({"ok": False, "error": "siaa sessao nao configurado"}), 500

    data = request.get_json(silent=True) or {}
    module, cookie, err = _validar_sessao_payload(data)
    if err is not None:
        return err

    try:
        r = requests.post(
            f"{SIAA_URL}/session/{module}",
            headers={"Authorization": f"Bearer {SIAA_TOKEN}"},
            json={"cookie": cookie},
            timeout=60,
        )
    except requests.RequestException as e:
        logger.error("siaa_sessao_salvar: erro de rede module=%s", module)
        return jsonify({"ok": False, "error": str(e)}), 502

    logger.info("siaa_sessao_salvar: module=%s len=%d status=%d", module, len(cookie), r.status_code)

    try:
        resp_data = r.json()
    except ValueError:
        resp_data = {"ok": r.status_code < 400, "raw": r.text[:300]}

    return jsonify(resp_data), r.status_code


@siaa_bp.route("/siaa-sessao/testar", methods=["POST"])
def siaa_sessao_testar():
    forbidden = _siaa_sessao_forbidden()
    if forbidden is not None:
        return forbidden

    if not SIAA_URL or not SIAA_TOKEN:
        return jsonify({"ok": False, "error": "siaa sessao nao configurado"}), 500

    data = request.get_json(silent=True) or {}
    module, cookie, err = _validar_sessao_payload(data)
    if err is not None:
        return err

    try:
        r = requests.post(
            f"{SIAA_URL}/session/{module}/test",
            headers={"Authorization": f"Bearer {SIAA_TOKEN}"},
            json={"cookie": cookie},
            timeout=60,
        )
    except requests.RequestException as e:
        logger.error("siaa_sessao_testar: erro de rede module=%s", module)
        return jsonify({"ok": False, "error": str(e)}), 502

    logger.info("siaa_sessao_testar: module=%s len=%d status=%d", module, len(cookie), r.status_code)

    try:
        resp_data = r.json()
    except ValueError:
        resp_data = {"ok": r.status_code < 400, "raw": r.text[:300]}

    return jsonify(resp_data), r.status_code
