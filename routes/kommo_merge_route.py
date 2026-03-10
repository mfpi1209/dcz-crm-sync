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
    Preview de N leads para selecao de merge (busca do banco local kommo_sync).

    Body JSON:
      { "lead_ids": [15815745, 20387845] }
    """
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401

    data = request.get_json(silent=True) or {}
    lead_ids = data.get("lead_ids") or []

    if not lead_ids or len(lead_ids) < 2:
        return jsonify({"error": "lead_ids deve conter pelo menos 2 IDs"}), 400

    try:
        conn = psycopg2.connect(**KOMMO_DB_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT l.id, l.name, l.status_id, l.pipeline_id, l.price,
                       l.created_at, l.custom_fields_json,
                       ps.name AS status_name,
                       p.name AS pipeline_name
                FROM leads l
                LEFT JOIN pipeline_statuses ps ON ps.id = l.status_id
                LEFT JOIN pipelines p ON p.id::text = l.pipeline_id::text
                WHERE l.id = ANY(%s)
            """, ([int(x) for x in lead_ids],))
            rows = cur.fetchall()
        conn.close()
    except Exception as e:
        log.error("Erro ao buscar leads para preview: %s", e)
        return jsonify({"error": f"Erro no banco: {e}"}), 500

    leads_out = []
    for row in rows:
        leads_out.append(_summarize_lead_from_db(row))

    return jsonify({"leads": leads_out})


def _summarize_lead_from_db(row):
    """Resume dados do lead a partir de uma row do banco kommo_sync."""
    import json as _json

    cf_values = {}
    cf_raw = row.get("custom_fields_json")
    if cf_raw:
        try:
            cf_list = _json.loads(cf_raw) if isinstance(cf_raw, str) else cf_raw
            if isinstance(cf_list, list):
                for cf in cf_list:
                    name = cf.get("field_name", str(cf.get("field_id", "")))
                    vals = cf.get("values", [])
                    if vals:
                        cf_values[name] = vals[0].get("value", "") if len(vals) == 1 else ", ".join(str(v.get("value", "")) for v in vals)
        except Exception:
            pass

    created_at = row.get("created_at")
    if isinstance(created_at, (int, float)) and created_at > 0:
        created_at = time.strftime("%Y-%m-%d %H:%M", time.localtime(created_at))

    return {
        "id": row["id"],
        "name": row.get("name") or "",
        "status_id": row.get("status_id"),
        "status_name": row.get("status_name") or "",
        "pipeline_name": row.get("pipeline_name") or "",
        "pipeline_id": row.get("pipeline_id"),
        "price": row.get("price"),
        "created_at": created_at,
        "custom_fields": cf_values,
    }
