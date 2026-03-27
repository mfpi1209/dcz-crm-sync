"""
Rota para merge de leads duplicados no Kommo.

Endpoints:
  POST /api/kommo/merge                           merge de dois leads
  POST /api/kommo/merge/preview                   preview de N leads para selecao
  GET  /api/kommo/merge/session-status            verifica sessão Kommo_chat
  GET  /api/kommo/merge/lost-duplicates           detecta duplicatas em Perdido
  POST /api/kommo/merge/lost-duplicates/execute   merge em massa
  GET  /api/kommo/merge/lost-duplicates/status/<job_id>
"""

import json as _json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from flask import Blueprint, request, jsonify, session as flask_session

import psycopg2
import psycopg2.extras

from kommo_merge import (
    merge_lead_pair,
    get_session_cookies,
    set_manual_cookies,
    get_manual_cookies,
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

    manual = get_manual_cookies()
    if manual:
        return jsonify({"status": "ok", "source": "manual", "keys": list(manual.keys())})

    cookies = get_session_cookies()
    if cookies:
        return jsonify({"status": "ok", "source": "dispatcher"})
    return jsonify({"status": "error", "detail": "Sessão indisponível"}), 503


@kommo_merge_bp.route("/api/kommo/merge/manual-cookies", methods=["POST"])
def api_set_manual_cookies():
    """
    Set manual Kommo session cookies.

    Body JSON: { "cookie_string": "session_id=abc; ...", ... }
      OR:      { "cookies": { "session_id": "abc", ... } }
    """
    check = _require_admin()
    if check:
        return check

    data = request.get_json(silent=True) or {}

    if data.get("cookies") and isinstance(data["cookies"], dict):
        cookies = data["cookies"]
    elif data.get("cookie_string"):
        cookies = _parse_cookie_string(data["cookie_string"])
    else:
        return jsonify({"error": "Envie 'cookie_string' ou 'cookies' dict"}), 400

    if not cookies.get("session_id"):
        return jsonify({"error": "session_id ausente nos cookies"}), 400

    set_manual_cookies(cookies)
    return jsonify({"ok": True, "keys": list(cookies.keys()), "count": len(cookies)})


@kommo_merge_bp.route("/api/kommo/merge/manual-cookies", methods=["DELETE"])
def api_clear_manual_cookies():
    """Clear manual cookies."""
    check = _require_admin()
    if check:
        return check
    set_manual_cookies(None)
    return jsonify({"ok": True})


def _parse_cookie_string(raw):
    """Parse 'key=value; key2=value2' into dict."""
    cookies = {}
    for part in raw.split(";"):
        part = part.strip()
        if "=" in part:
            k, v = part.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


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


# ── Sanitizar leads perdidos duplicados ──────────────────────────────────

LOST_STATUS = 143
MAIN_PIPELINE = 5481944


def _mark_lead_deleted(lead_id):
    """Marca lead como is_deleted=true no banco local após merge."""
    try:
        conn = psycopg2.connect(**KOMMO_DB_DSN)
        with conn.cursor() as cur:
            cur.execute("UPDATE leads SET is_deleted = true WHERE id = %s", (lead_id,))
        conn.commit()
        conn.close()
        log.debug("Lead %s marcado como deletado no banco local", lead_id)
    except Exception as e:
        log.warning("Falha ao marcar lead %s como deletado: %s", lead_id, e)

_lost_dup_jobs = {}


def _detect_lost_duplicates():
    """
    Detect duplicate lost leads grouped by phone number.

    Groups leads in Perdido (143) / main pipeline by the normalized phone
    of their linked contact.  Keeps the newest lead per phone group.
    """
    conn = psycopg2.connect(**KOMMO_DB_DSN)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            WITH lost_phones AS (
                SELECT DISTINCT ON (l.id, clean_phone)
                       l.id          AS lead_id,
                       l.name        AS lead_name,
                       l.created_at,
                       c.name        AS contact_name,
                       (je->>'id')::int AS contact_id,
                       REGEXP_REPLACE(v->>'value', '[^0-9]', '', 'g') AS clean_phone
                FROM leads l
                     JOIN LATERAL jsonb_array_elements(l.contacts_json) je ON true
                     JOIN contacts c ON c.id = (je->>'id')::int
                     JOIN LATERAL jsonb_array_elements(c.custom_fields_json) cf ON true
                     JOIN LATERAL jsonb_array_elements(cf->'values') v ON true
                WHERE l.status_id  = %s
                  AND l.pipeline_id = %s
                  AND NOT COALESCE(l.is_deleted, false)
                  AND l.contacts_json IS NOT NULL
                  AND jsonb_array_length(l.contacts_json) > 0
                  AND (cf->>'field_code') = 'PHONE'
                  AND LENGTH(REGEXP_REPLACE(v->>'value', '[^0-9]', '', 'g')) >= 8
            )
            SELECT clean_phone,
                   MIN(contact_name)  AS contact_name,
                   array_agg(lead_id    ORDER BY created_at DESC) AS lead_ids,
                   array_agg(lead_name  ORDER BY created_at DESC) AS lead_names,
                   array_agg(created_at ORDER BY created_at DESC) AS created_ats,
                   array_agg(contact_id ORDER BY created_at DESC) AS contact_ids,
                   COUNT(*)           AS lead_count
            FROM lost_phones
            GROUP BY clean_phone
            HAVING COUNT(*) >= 2
            ORDER BY COUNT(*) DESC
        """, (LOST_STATUS, MAIN_PIPELINE))
        rows = cur.fetchall()
    conn.close()

    groups = []
    total_removable = 0
    for r in rows:
        lead_ids = list(r["lead_ids"])
        lead_names = list(r["lead_names"])
        created_ats = list(r["created_ats"])
        contact_ids = list(r["contact_ids"])

        leads = []
        for i, lid in enumerate(lead_ids):
            ca = created_ats[i]
            if isinstance(ca, (int, float)) and ca > 0:
                ca = time.strftime("%Y-%m-%d %H:%M", time.localtime(ca))
            elif hasattr(ca, "isoformat"):
                ca = ca.isoformat()
            leads.append({
                "id": lid,
                "name": lead_names[i] or "",
                "created_at": ca,
                "contact_id": contact_ids[i],
            })

        keep_id = lead_ids[0]
        remove_ids = lead_ids[1:]
        total_removable += len(remove_ids)

        phone_raw = r["clean_phone"] or ""
        phone_fmt = phone_raw
        if len(phone_raw) >= 10:
            phone_fmt = f"({phone_raw[-11:-9]}) {phone_raw[-9:-4]}-{phone_raw[-4:]}" if len(phone_raw) >= 11 else f"({phone_raw[-10:-8]}) {phone_raw[-8:-4]}-{phone_raw[-4:]}"

        groups.append({
            "phone": phone_fmt,
            "phone_raw": phone_raw,
            "contact_name": r["contact_name"] or "",
            "lead_count": r["lead_count"],
            "leads": leads,
            "keep_id": keep_id,
            "remove_ids": remove_ids,
        })

    return groups, total_removable


@kommo_merge_bp.route("/api/kommo/merge/lost-duplicates")
def api_lost_duplicates_detect():
    """Detect lost leads sharing the same phone number."""
    check = _require_admin()
    if check:
        return check

    try:
        groups, total_removable = _detect_lost_duplicates()
        return jsonify({
            "ok": True,
            "groups_count": len(groups),
            "total_leads": sum(g["lead_count"] for g in groups),
            "total_removable": total_removable,
            "groups": groups,
        })
    except Exception as e:
        log.exception("lost-duplicates detect error")
        return jsonify({"ok": False, "error": str(e)}), 500


@kommo_merge_bp.route("/api/kommo/merge/lost-duplicates/execute", methods=["POST"])
def api_lost_duplicates_execute():
    """Execute mass merge of lost duplicate leads."""
    check = _require_admin()
    if check:
        return check

    data = request.get_json(silent=True) or {}
    dry_run = data.get("dry_run", False)
    limit = data.get("limit")

    for jid, j in list(_lost_dup_jobs.items()):
        if j.get("running") and not j.get("cancelled"):
            started = j.get("started_at", "")
            try:
                t = datetime.fromisoformat(started)
                age_min = (datetime.now(timezone(timedelta(hours=-3))) - t).total_seconds() / 60
            except Exception:
                age_min = 999
            if age_min > 60:
                j["running"] = False
                j["cancelled"] = True
                continue
            return jsonify({"error": "Já existe um merge em massa em andamento", "job_id": jid}), 409

    MAX_CONSECUTIVE_ERRORS = 10

    job_id = str(uuid.uuid4())[:8]
    _lost_dup_jobs[job_id] = {
        "running": True,
        "cancelled": False,
        "dry_run": dry_run,
        "progress": 0,
        "total": 0,
        "processed": 0,
        "success": 0,
        "errors": 0,
        "log": [],
        "started_at": datetime.now(timezone(timedelta(hours=-3))).isoformat(),
    }

    def _run():
        job = _lost_dup_jobs[job_id]
        try:
            groups, _ = _detect_lost_duplicates()
            if limit:
                groups = groups[:int(limit)]

            pairs = []
            for g in groups:
                label = g.get("contact_name") or g.get("phone") or "?"
                for rid in g["remove_ids"]:
                    pairs.append((g["keep_id"], rid, label, g.get("phone", "")))

            job["total"] = len(pairs)

            if dry_run:
                for keep_id, remove_id, label, phone in pairs:
                    job["log"].append({
                        "action": "dry_run",
                        "keep": keep_id,
                        "remove": remove_id,
                        "contact": label,
                        "phone": phone,
                    })
                job["processed"] = len(pairs)
                job["success"] = len(pairs)
                job["progress"] = 100
                job["running"] = False
                return

            consecutive_errors = 0
            for i, (keep_id, remove_id, label, phone) in enumerate(pairs):
                if job["cancelled"]:
                    job["log"].append({"action": "fatal", "error": "Cancelado pelo usuario"})
                    break

                try:
                    result = merge_lead_pair(keep_id, remove_id)
                    ok = result.get("ok", False)
                    status = result.get("status", "")
                    detail = result.get("response", {})
                    job["log"].append({
                        "action": "merge",
                        "keep": keep_id,
                        "remove": remove_id,
                        "contact": label,
                        "phone": phone,
                        "ok": ok,
                        "error": result.get("error") if not ok else None,
                        "status": status,
                        "detail": str(detail)[:200] if detail else "",
                    })
                    if ok:
                        job["success"] += 1
                        consecutive_errors = 0
                        _mark_lead_deleted(remove_id)
                    else:
                        job["errors"] += 1
                        consecutive_errors += 1
                except Exception as e:
                    log.exception("Merge pair %s->%s failed", keep_id, remove_id)
                    job["log"].append({
                        "action": "merge",
                        "keep": keep_id,
                        "remove": remove_id,
                        "contact": label,
                        "phone": phone,
                        "ok": False,
                        "error": str(e),
                    })
                    job["errors"] += 1
                    consecutive_errors += 1

                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    job["log"].append({
                        "action": "fatal",
                        "error": f"Auto-stop: {MAX_CONSECUTIVE_ERRORS} erros consecutivos",
                    })
                    break

                job["processed"] = i + 1
                job["progress"] = int((i + 1) / len(pairs) * 100) if pairs else 100
                time.sleep(0.5)

        except Exception as e:
            log.exception("lost-duplicates execute error")
            job["log"].append({"action": "fatal", "error": str(e)})
        finally:
            job["running"] = False
            job["progress"] = 100

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "job_id": job_id, "dry_run": dry_run}), 202


@kommo_merge_bp.route("/api/kommo/merge/lost-duplicates/cancel/<job_id>", methods=["POST"])
def api_lost_duplicates_cancel(job_id):
    """Signal a running job to stop."""
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401
    job = _lost_dup_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404
    job["cancelled"] = True
    return jsonify({"ok": True})


@kommo_merge_bp.route("/api/kommo/merge/lost-duplicates/force-clear", methods=["POST"])
def api_lost_duplicates_force_clear():
    """Kill all jobs and clear the registry."""
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401
    count = len(_lost_dup_jobs)
    for j in _lost_dup_jobs.values():
        j["cancelled"] = True
        j["running"] = False
    _lost_dup_jobs.clear()
    return jsonify({"ok": True, "cleared": count})


@kommo_merge_bp.route("/api/kommo/merge/lost-duplicates/active-job")
def api_lost_duplicates_active_job():
    """Return the most recent job (running or finished) so the UI can reconnect."""
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401

    if not _lost_dup_jobs:
        return jsonify({"has_job": False})

    latest_id = max(_lost_dup_jobs, key=lambda k: _lost_dup_jobs[k].get("started_at", ""))
    job = _lost_dup_jobs[latest_id]
    return jsonify({
        "has_job": True,
        "job_id": latest_id,
        "running": job["running"],
        "dry_run": job.get("dry_run", False),
        "progress": job["progress"],
        "total": job["total"],
        "processed": job["processed"],
        "success": job["success"],
        "errors": job["errors"],
        "log_total": len(job.get("log", [])),
    })


@kommo_merge_bp.route("/api/kommo/merge/lost-duplicates/status/<job_id>")
def api_lost_duplicates_status(job_id):
    """Poll progress of a mass merge job."""
    if not flask_session.get("authenticated"):
        return jsonify({"error": "Não autenticado"}), 401

    job = _lost_dup_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job não encontrado"}), 404

    out = {
        "running": job["running"],
        "dry_run": job.get("dry_run", False),
        "progress": job["progress"],
        "total": job["total"],
        "processed": job["processed"],
        "success": job["success"],
        "errors": job["errors"],
    }
    log_entries = job.get("log", [])
    since = request.args.get("since", 0, type=int)
    out["log"] = log_entries[since:since + 100]
    out["log_since"] = since
    out["log_total"] = len(log_entries)
    return jsonify(out)
