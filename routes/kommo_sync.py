"""
eduit. — Sync Comercial (Kommo CRM).
Integração com o projeto Kommo_Update para sincronização de leads/contatos.
"""

import os
import sys
import uuid
import logging
import threading
import subprocess
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)

kommo_bp = Blueprint("kommo_bp", __name__)

KOMMO_DIR = str(Path(__file__).resolve().parent.parent / "Kommo_Update")

PG_KOMMO = {
    "host": os.getenv("KOMMO_PG_HOST", "31.97.91.47"),
    "port": int(os.getenv("KOMMO_PG_PORT", "5432")),
    "dbname": os.getenv("KOMMO_PG_DB", "kommo_sync"),
    "user": os.getenv("KOMMO_PG_USER", "adm_eduit"),
    "password": os.getenv("KOMMO_PG_PASS", "IaDm24Sx3HxrYoqT"),
}

_tasks = {}


def _pg():
    return psycopg2.connect(**PG_KOMMO)


# ── Status da sincronização ──────────────────────────────────────────────

@kommo_bp.route("/api/kommo/status")
def api_kommo_status():
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT * FROM sync_metadata ORDER BY entity_type")
        entities = [dict(r) for r in cur.fetchall()]

        cur.execute("SELECT COUNT(*) AS cnt FROM leads")
        leads = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) AS cnt FROM contacts")
        contacts = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT entity_type, last_sync_at, records_synced, status
            FROM sync_metadata ORDER BY last_sync_at DESC LIMIT 5
        """)
        history = [dict(r) for r in cur.fetchall()]

        conn.close()
        return jsonify({
            "ok": True,
            "data": {
                "entities": entities,
                "leads_count": leads,
                "contacts_count": contacts,
                "history": history,
            }
        })
    except Exception as e:
        logger.error("kommo status: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Leads por pipeline/stage ─────────────────────────────────────────────

@kommo_bp.route("/api/kommo/leads-by-stage")
def api_kommo_leads_by_stage():
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT p.name AS pipeline_name, ps.name AS stage_name,
                   ps.id AS stage_id, COUNT(l.id) AS total
            FROM leads l
            JOIN pipeline_statuses ps ON ps.id = l.status_id
            JOIN pipelines p ON p.id = l.pipeline_id
            WHERE l.is_deleted = false
            GROUP BY p.name, ps.name, ps.id, ps.sort, p.sort
            ORDER BY p.sort, ps.sort
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        logger.error("kommo leads-by-stage: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Mudanças recentes ────────────────────────────────────────────────────

@kommo_bp.route("/api/kommo/recent-changes")
def api_kommo_recent_changes():
    import time as _time
    hours = request.args.get("hours", 24, type=int)
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("""
            SELECT p.name AS pipeline_name, ps.name AS stage_name, COUNT(*) AS total
            FROM leads l
            JOIN pipeline_statuses ps ON ps.id = l.status_id
            JOIN pipelines p ON p.id = l.pipeline_id
            WHERE l.synced_at >= (NOW() - INTERVAL '%s hours')::text
            GROUP BY p.name, ps.name, ps.sort, p.sort
            ORDER BY p.sort, ps.sort
        """, (hours,))
        by_stage = [dict(r) for r in cur.fetchall()]

        cur.execute("SELECT COUNT(*) AS t FROM leads WHERE synced_at >= (NOW() - INTERVAL '%s hours')::text", (hours,))
        leads_upd = cur.fetchone()["t"]

        cur.execute("SELECT COUNT(*) AS t FROM contacts WHERE synced_at >= (NOW() - INTERVAL '%s hours')::text", (hours,))
        contacts_upd = cur.fetchone()["t"]

        since_ts = int(_time.time()) - (hours * 3600)
        cur.execute("SELECT COUNT(*) AS t FROM leads WHERE created_at >= %s AND is_deleted = false", (since_ts,))
        new_leads = cur.fetchone()["t"]

        cur.execute("SELECT COUNT(*) AS t FROM leads WHERE status_id = 142 AND synced_at >= (NOW() - INTERVAL '%s hours')::text", (hours,))
        won = cur.fetchone()["t"]

        conn.close()
        return jsonify({"ok": True, "data": {
            "hours": hours,
            "leads_updated": leads_upd,
            "contacts_updated": contacts_upd,
            "new_leads": new_leads,
            "won_leads": won,
            "updated_by_stage": by_stage,
        }})
    except Exception as e:
        logger.error("kommo recent-changes: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Pipeline statuses ────────────────────────────────────────────────────

@kommo_bp.route("/api/kommo/pipelines")
def api_kommo_pipelines():
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT ps.id, ps.name AS stage_name, ps.pipeline_id,
                   p.name AS pipeline_name, ps.sort
            FROM pipeline_statuses ps
            JOIN pipelines p ON p.id = ps.pipeline_id
            ORDER BY p.sort, ps.sort
        """)
        data = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        logger.error("kommo pipelines: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Trigger sync ─────────────────────────────────────────────────────────

@kommo_bp.route("/api/kommo/sync", methods=["POST"])
def api_kommo_sync():
    for t in _tasks.values():
        if t.get("type") == "sync" and t.get("status") == "running":
            return jsonify({"ok": False, "error": "Sincronização já em andamento."}), 409

    body = request.json or {}
    mode = body.get("mode", "delta")
    task_id = str(uuid.uuid4())[:8]

    _tasks[task_id] = {
        "type": "sync",
        "status": "running",
        "progress": 0,
        "message": "Iniciando sincronização Kommo...",
        "started_at": datetime.now().isoformat(),
        "log": [],
    }

    def _run():
        ts = lambda: datetime.now().strftime("%H:%M:%S")
        try:
            cmd = [sys.executable, "main.py"]
            if mode == "full":
                cmd.append("--full")

            _tasks[task_id]["log"].append({"time": ts(), "msg": f"Executando: {' '.join(cmd)}"})
            _tasks[task_id]["progress"] = 10

            proc = subprocess.run(
                cmd, cwd=KOMMO_DIR, capture_output=True, text=True, timeout=900,
            )

            output_lines = (proc.stdout or "").strip().split("\n")[-30:]
            for line in output_lines:
                if line.strip():
                    _tasks[task_id]["log"].append({"time": ts(), "msg": line.strip()})

            if proc.returncode == 0:
                _tasks[task_id]["progress"] = 85
                _tasks[task_id]["log"].append({"time": ts(), "msg": "Sync concluído. Migrando para PostgreSQL..."})

                mig_cmd = [sys.executable, "migrate_to_postgres.py", "--light"]
                mig = subprocess.run(mig_cmd, cwd=KOMMO_DIR, capture_output=True, text=True, timeout=300)
                if mig.returncode == 0:
                    _tasks[task_id]["log"].append({"time": ts(), "msg": "PostgreSQL atualizado!"})
                else:
                    _tasks[task_id]["log"].append({"time": ts(), "msg": f"Aviso PG: {(mig.stderr or '')[-200:]}"})

                _tasks[task_id]["progress"] = 100
                _tasks[task_id]["status"] = "completed"
                _tasks[task_id]["message"] = "Sincronização concluída com sucesso!"
            else:
                _tasks[task_id]["status"] = "error"
                err_tail = (proc.stderr or "")[-300:]
                _tasks[task_id]["message"] = f"Erro: {err_tail}"
                _tasks[task_id]["log"].append({"time": ts(), "msg": f"ERRO: {err_tail}"})

        except subprocess.TimeoutExpired:
            _tasks[task_id]["status"] = "error"
            _tasks[task_id]["message"] = "Timeout (15 min)"
        except Exception as e:
            _tasks[task_id]["status"] = "error"
            _tasks[task_id]["message"] = f"Erro: {e}"
            _tasks[task_id]["log"].append({"time": ts(), "msg": f"Exceção: {e}"})

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return jsonify({"ok": True, "task_id": task_id})


# ── Task progress ────────────────────────────────────────────────────────

@kommo_bp.route("/api/kommo/task/<task_id>")
def api_kommo_task(task_id):
    task = _tasks.get(task_id)
    if not task:
        return jsonify({"ok": False, "error": "Tarefa não encontrada"}), 404
    t = dict(task)
    if "log" in t and len(t["log"]) > 30:
        t["log"] = t["log"][-30:]
    return jsonify({"ok": True, "data": t})
