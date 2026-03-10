"""
Rota para merge de leads duplicados no Kommo.

Endpoints:
  POST /api/kommo/merge          merge de dois leads
  POST /api/kommo/merge/preview  preview de N leads para selecao
  GET  /api/kommo/merge/status   verifica status da sessão com Kommo_chat
"""

import logging
import threading
import time
from flask import Blueprint, request, jsonify, session as flask_session

import psycopg2
import psycopg2.extras

from kommo_merge import (
    merge_lead_pair,
    get_session_cookies,
    fetch_lead_full,
)
from match_merge_lib import KOMMO_DB_DSN

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
        return jsonify({"error": "Job não encontrado", "status": "not_found"}), 404

    if job["running"]:
        return jsonify({"status": "running"})

    result = job.get("result") or {}
    if result.get("ok"):
        return jsonify({"status": "done", "result": result})
    else:
        return jsonify({"status": "error", "error": result.get("error", "Falha desconhecida"), "result": result})


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
    Preview de N leads para selecao de merge.

    Body JSON:
      { "lead_ids": [15815745, 20387845] }
    """
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401

    data = request.get_json(silent=True) or {}
    lead_ids = data.get("lead_ids") or []

    if not lead_ids or len(lead_ids) < 2:
        return jsonify({"error": "lead_ids deve conter pelo menos 2 IDs"}), 400

    status_map = _load_pipeline_statuses()
    leads_out = []
    for lid in lead_ids:
        lead_data = fetch_lead_full(int(lid))
        if lead_data:
            leads_out.append(_summarize_lead(lead_data, status_map))

    return jsonify({"leads": leads_out})


def _load_pipeline_statuses():
    """Carrega mapa status_id -> (status_name, pipeline_name) do banco kommo_sync."""
    try:
        conn = psycopg2.connect(**KOMMO_DB_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ps.id AS status_id, ps.name AS status_name,
                       p.name AS pipeline_name
                FROM pipeline_statuses ps
                LEFT JOIN pipelines p ON p.id::text = ps.pipeline_id::text
            """)
            rows = cur.fetchall()
        conn.close()
        return {r["status_id"]: (r["status_name"], r["pipeline_name"]) for r in rows}
    except Exception as e:
        log.warning("Falha ao carregar pipeline_statuses: %s", e)
        return {}


def _summarize_lead(lead, status_map=None):
    """Resume dados do lead para preview."""
    cf_values = {}
    for cf in (lead.get("custom_fields_values") or []):
        name = cf.get("field_name", cf.get("field_id"))
        vals = cf.get("values", [])
        if vals:
            cf_values[str(name)] = vals[0].get("value", "") if len(vals) == 1 else ", ".join(str(v.get("value", "")) for v in vals)

    status_id = lead.get("status_id")
    status_name = ""
    pipeline_name = ""
    if status_map and status_id in status_map:
        status_name, pipeline_name = status_map[status_id]

    created_at = lead.get("created_at")
    if isinstance(created_at, (int, float)):
        created_at = time.strftime("%Y-%m-%d %H:%M", time.localtime(created_at))

    return {
        "id": lead["id"],
        "name": lead.get("name", ""),
        "status_id": status_id,
        "status_name": status_name,
        "pipeline_name": pipeline_name,
        "pipeline_id": lead.get("pipeline_id"),
        "price": lead.get("price"),
        "created_at": created_at,
        "custom_fields": cf_values,
    }
