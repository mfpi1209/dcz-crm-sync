"""
Rota para merge de leads duplicados no Kommo.

Endpoints:
  POST /api/kommo/merge          merge de dois leads
  GET  /api/kommo/merge/status   verifica status da sessão com Kommo_chat
"""

import logging
import threading
from flask import Blueprint, request, jsonify, session as flask_session

from kommo_merge import (
    merge_lead_pair,
    get_session_cookies,
    fetch_lead_full,
)

log = logging.getLogger("kommo_merge_route")

kommo_merge_bp = Blueprint("kommo_merge", __name__)

_merge_jobs = {}


def _require_admin():
    if flask_session.get("role") != "admin":
        return jsonify({"error": "Acesso negado"}), 403
    return None


@kommo_merge_bp.route("/api/kommo/merge", methods=["POST"])
def api_merge():
    """
    Merge de dois leads no Kommo.

    Body JSON:
      {
        "keep_id": 15815745,
        "remove_id": 20387845
      }
    """
    check = _require_admin()
    if check:
        return check

    data = request.get_json(silent=True) or {}
    keep_id = data.get("keep_id")
    remove_id = data.get("remove_id")

    if not keep_id or not remove_id:
        return jsonify({"error": "keep_id e remove_id são obrigatórios"}), 400

    if str(keep_id) == str(remove_id):
        return jsonify({"error": "keep_id e remove_id não podem ser iguais"}), 400

    job_key = f"{keep_id}_{remove_id}"
    if job_key in _merge_jobs and _merge_jobs[job_key].get("running"):
        return jsonify({"error": "Merge já em andamento para esses leads"}), 409

    _merge_jobs[job_key] = {"running": True, "result": None}

    def _run():
        try:
            result = merge_lead_pair(int(keep_id), int(remove_id))
            _merge_jobs[job_key] = {"running": False, "result": result}
        except Exception as e:
            log.exception("Erro no merge %s → %s", keep_id, remove_id)
            _merge_jobs[job_key] = {"running": False, "result": {"ok": False, "error": str(e)}}

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return jsonify({
        "status": "accepted",
        "detail": f"Merge iniciado: manter={keep_id}, remover={remove_id}",
        "job_key": job_key,
    }), 202


@kommo_merge_bp.route("/api/kommo/merge/job/<job_key>", methods=["GET"])
def api_merge_job_status(job_key):
    """Verifica status de um job de merge."""
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401

    job = _merge_jobs.get(job_key)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404

    return jsonify({
        "running": job["running"],
        "result": job["result"],
    })


@kommo_merge_bp.route("/api/kommo/merge/session-status", methods=["GET"])
def api_merge_session_status():
    """Verifica se a sessão com Kommo_chat está disponível."""
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401

    cookies = get_session_cookies()
    if cookies:
        return jsonify({"status": "ok", "detail": "Sessão válida"})
    return jsonify({"status": "error", "detail": "Sessão indisponível"}), 503


@kommo_merge_bp.route("/api/kommo/merge/preview", methods=["POST"])
def api_merge_preview():
    """
    Preview dos dados dos dois leads antes do merge.

    Body JSON:
      {
        "keep_id": 15815745,
        "remove_id": 20387845
      }
    """
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401

    data = request.get_json(silent=True) or {}
    keep_id = data.get("keep_id")
    remove_id = data.get("remove_id")

    if not keep_id or not remove_id:
        return jsonify({"error": "keep_id e remove_id são obrigatórios"}), 400

    keep = fetch_lead_full(int(keep_id))
    remove = fetch_lead_full(int(remove_id))

    if not keep:
        return jsonify({"error": f"Lead {keep_id} não encontrado"}), 404
    if not remove:
        return jsonify({"error": f"Lead {remove_id} não encontrado"}), 404

    return jsonify({
        "keep": _summarize_lead(keep),
        "remove": _summarize_lead(remove),
    })


def _summarize_lead(lead):
    """Resume dados do lead para preview."""
    cf_values = {}
    for cf in (lead.get("custom_fields_values") or []):
        name = cf.get("field_name", cf.get("field_id"))
        vals = cf.get("values", [])
        if vals:
            cf_values[str(name)] = vals[0].get("value", "") if len(vals) == 1 else [v.get("value", "") for v in vals]

    contacts = []
    for c in lead.get("_embedded", {}).get("contacts", []):
        contacts.append({"id": c["id"], "is_main": c.get("is_main", False)})

    return {
        "id": lead["id"],
        "name": lead.get("name", ""),
        "status_id": lead.get("status_id"),
        "pipeline_id": lead.get("pipeline_id"),
        "responsible_user_id": lead.get("responsible_user_id"),
        "price": lead.get("price"),
        "created_at": lead.get("created_at"),
        "custom_fields": cf_values,
        "contacts": contacts,
    }
