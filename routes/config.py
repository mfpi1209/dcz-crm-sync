"""
eduit. — Blueprint de configuração (turmas, ciclos, distribuição, schedules, debug).
"""

import os
import sys
import json
import logging
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests as _requests
from flask import Blueprint, request, jsonify
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from db import get_conn
from helpers import BRT, to_brt, BASE_DIR, SYNC_SCRIPT, LOG_DIR

config_bp = Blueprint("config_bp", __name__)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scheduler reference (set via init_scheduler from app.py)
# ---------------------------------------------------------------------------

scheduler = None


def init_scheduler(sched):
    global scheduler
    scheduler = sched


# ---------------------------------------------------------------------------
# N8N Webhooks — Distribuição
# ---------------------------------------------------------------------------

N8N_DIST_GET = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/api/distribuicao"
N8N_DIST_SAVE = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/api/atualizar-distribuicao"

# ---------------------------------------------------------------------------
# Constantes — Turmas
# ---------------------------------------------------------------------------

GRAD_MONTHS = [2, 3, 4, 5, 8, 9, 10, 11]
POS_MONTHS = list(range(1, 13))
MONTH_NAMES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

DAY_MAP = {"0": "mon", "1": "tue", "2": "wed", "3": "thu", "4": "fri", "5": "sat", "6": "sun"}


def _turma_defaults(nivel, ano):
    """Gera ranges padrão de turmas para um nível/ano."""
    import calendar
    months = GRAD_MONTHS if nivel == "Graduação" else POS_MONTHS
    rows = []
    for m in months:
        last_day = calendar.monthrange(ano, m)[1]
        rows.append({
            "nivel": nivel,
            "nome": f"{MONTH_NAMES[m]} {ano}",
            "dt_inicio": f"{ano}-{m:02d}-01",
            "dt_fim": f"{ano}-{m:02d}-{last_day:02d}",
            "ano": ano,
        })
    return rows


# ---------------------------------------------------------------------------
# Rotas — Turmas CRUD
# ---------------------------------------------------------------------------

@config_bp.route("/api/turmas")
def api_turmas_list():
    nivel = request.args.get("nivel", "")
    ano = request.args.get("ano", "")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            q = "SELECT * FROM turmas WHERE 1=1"
            params = []
            if nivel:
                q += " AND nivel = %s"
                params.append(nivel)
            if ano:
                q += " AND ano = %s"
                params.append(int(ano))
            q += " ORDER BY ano, dt_inicio"
            cur.execute(q, params)
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat() if v else None
                rows.append(row)
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/turmas", methods=["POST"])
def api_turmas_create():
    body = request.json or {}
    required = ("nivel", "nome", "dt_inicio", "dt_fim", "ano")
    if not all(body.get(k) for k in required):
        return jsonify({"error": "Campos obrigatórios: " + ", ".join(required)}), 400
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO turmas (nivel, nome, dt_inicio, dt_fim, ano) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (nivel, nome) DO UPDATE SET dt_inicio=EXCLUDED.dt_inicio, dt_fim=EXCLUDED.dt_fim, ano=EXCLUDED.ano "
                "RETURNING id",
                (body["nivel"], body["nome"], body["dt_inicio"], body["dt_fim"], int(body["ano"])),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/turmas/<int:tid>", methods=["PUT"])
def api_turmas_update(tid):
    body = request.json or {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE turmas SET nivel=COALESCE(%s,nivel), nome=COALESCE(%s,nome), "
                "dt_inicio=COALESCE(%s,dt_inicio), dt_fim=COALESCE(%s,dt_fim), ano=COALESCE(%s,ano) "
                "WHERE id=%s",
                (body.get("nivel"), body.get("nome"), body.get("dt_inicio"), body.get("dt_fim"),
                 int(body["ano"]) if body.get("ano") else None, tid),
            )
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/turmas/<int:tid>", methods=["DELETE"])
def api_turmas_delete(tid):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM turmas WHERE id=%s", (tid,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/turmas/seed", methods=["POST"])
def api_turmas_seed():
    body = request.json or {}
    ano = int(body.get("ano", datetime.now().year))
    conn = get_conn()
    try:
        created = 0
        with conn.cursor() as cur:
            for nivel in ("Graduação", "Pós-Graduação"):
                for t in _turma_defaults(nivel, ano):
                    cur.execute(
                        "INSERT INTO turmas (nivel, nome, dt_inicio, dt_fim, ano) "
                        "VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (nivel, nome) DO NOTHING",
                        (t["nivel"], t["nome"], t["dt_inicio"], t["dt_fim"], t["ano"]),
                    )
                    created += cur.rowcount
        conn.commit()
        return jsonify({"ok": True, "created": created, "ano": ano})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Ciclos CRUD
# ---------------------------------------------------------------------------

@config_bp.route("/api/ciclos")
def api_ciclos_list():
    nivel = request.args.get("nivel", "")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            q = "SELECT * FROM ciclos WHERE 1=1"
            params = []
            if nivel:
                q += " AND nivel = %s"
                params.append(nivel)
            q += " ORDER BY dt_inicio"
            cur.execute(q, params)
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat() if v else None
                rows.append(row)
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/ciclos", methods=["POST"])
def api_ciclos_create():
    body = request.json or {}
    required = ("nivel", "nome", "dt_inicio", "dt_fim")
    if not all(body.get(k) for k in required):
        return jsonify({"error": "Campos obrigatórios: " + ", ".join(required)}), 400
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ciclos (nivel, nome, dt_inicio, dt_fim) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (nivel, nome) DO UPDATE SET dt_inicio=EXCLUDED.dt_inicio, dt_fim=EXCLUDED.dt_fim "
                "RETURNING id",
                (body["nivel"], body["nome"], body["dt_inicio"], body["dt_fim"]),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/ciclos/<int:cid>", methods=["PUT"])
def api_ciclos_update(cid):
    body = request.json or {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ciclos SET nivel=COALESCE(%s,nivel), nome=COALESCE(%s,nome), "
                "dt_inicio=COALESCE(%s,dt_inicio), dt_fim=COALESCE(%s,dt_fim) "
                "WHERE id=%s",
                (body.get("nivel"), body.get("nome"), body.get("dt_inicio"), body.get("dt_fim"), cid),
            )
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/ciclos/<int:cid>", methods=["DELETE"])
def api_ciclos_delete(cid):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ciclos WHERE id=%s", (cid,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/ciclos/seed", methods=["POST"])
def api_ciclos_seed():
    """Gera ciclos padrão: Graduação semestral, Pós-Graduação semestral."""
    body = request.json or {}
    ano = int(body.get("ano", datetime.now().year))
    conn = get_conn()
    try:
        created = 0
        defaults = [
            ("Graduação", f"{ano}.1", f"{ano-1}-11-16", f"{ano}-05-15"),
            ("Graduação", f"{ano}.2", f"{ano}-05-16", f"{ano}-11-15"),
            ("Pós-Graduação", f"{ano}.1", f"{ano-1}-11-16", f"{ano}-05-15"),
            ("Pós-Graduação", f"{ano}.2", f"{ano}-05-16", f"{ano}-11-15"),
        ]
        with conn.cursor() as cur:
            for nivel, nome, dt_ini, dt_fim in defaults:
                cur.execute(
                    "INSERT INTO ciclos (nivel, nome, dt_inicio, dt_fim) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (nivel, nome) DO NOTHING",
                    (nivel, nome, dt_ini, dt_fim),
                )
                created += cur.rowcount
        conn.commit()
        return jsonify({"ok": True, "created": created, "ano": ano})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Distribuição
# ---------------------------------------------------------------------------

@config_bp.route("/api/distribuicao", methods=["GET"])
def api_distribuicao_get():
    try:
        r = _requests.get(N8N_DIST_GET, timeout=15)
        payload = r.json()
        if isinstance(payload, list):
            payload = payload[0] if payload else {}
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@config_bp.route("/api/distribuicao", methods=["POST"])
def api_distribuicao_save():
    try:
        data = request.json
        r = _requests.post(N8N_DIST_SAVE, json=data, timeout=15,
                           headers={"Content-Type": "application/json"})
        if r.ok:
            return jsonify({"ok": True})
        return jsonify({"ok": False, "error": f"n8n respondeu {r.status_code}"}), 502
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502


# ---------------------------------------------------------------------------
# Rotas — Agendamento (Schedules)
# ---------------------------------------------------------------------------

@config_bp.route("/api/schedules")
def api_schedules_list():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM schedules ORDER BY created_at")
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                rows.append(row)

        for row in rows:
            job = scheduler.get_job(row["id"])
            if job and job.next_run_time:
                row["next_run"] = to_brt(job.next_run_time)
            else:
                row["next_run"] = None

        return jsonify({"schedules": rows})
    except Exception as e:
        return jsonify({"schedules": [], "error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/schedules", methods=["POST"])
def api_schedules_save():
    data = request.json
    if not data:
        return jsonify({"error": "Dados inválidos."}), 400

    job_type = data.get("job_type", "")
    if job_type not in ("sync_delta", "sync_full"):
        return jsonify({"error": "Tipo inválido. Use 'sync_delta' ou 'sync_full'."}), 400

    cron_days = data.get("cron_days", "*")
    cron_hour = int(data.get("cron_hour", 2))
    cron_minute = int(data.get("cron_minute", 0))
    enabled = bool(data.get("enabled", True))
    schedule_id = data.get("id") or f"{job_type}_{cron_hour:02d}{cron_minute:02d}"

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO schedules (id, job_type, cron_days, cron_hour, cron_minute, enabled)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    job_type = EXCLUDED.job_type,
                    cron_days = EXCLUDED.cron_days,
                    cron_hour = EXCLUDED.cron_hour,
                    cron_minute = EXCLUDED.cron_minute,
                    enabled = EXCLUDED.enabled
            """, (schedule_id, job_type, cron_days, cron_hour, cron_minute, enabled))
        conn.commit()

        _register_schedule_job(schedule_id, job_type, cron_days, cron_hour, cron_minute, enabled)

        return jsonify({"ok": True, "id": schedule_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/schedules/<schedule_id>", methods=["DELETE"])
def api_schedules_delete(schedule_id):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedules WHERE id = %s", (schedule_id,))
        conn.commit()

        try:
            scheduler.remove_job(schedule_id)
        except Exception:
            pass

        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@config_bp.route("/api/schedules/<schedule_id>/toggle", methods=["POST"])
def api_schedules_toggle(schedule_id):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("UPDATE schedules SET enabled = NOT enabled WHERE id = %s RETURNING *", (schedule_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Agendamento não encontrado."}), 404
        conn.commit()

        _register_schedule_job(
            row["id"], row["job_type"], row["cron_days"],
            row["cron_hour"], row["cron_minute"], row["enabled"],
        )

        return jsonify({"ok": True, "enabled": row["enabled"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Debug
# ---------------------------------------------------------------------------

@config_bp.route("/api/debug")
def api_debug():
    import app as _app
    return jsonify({
        "sync_running": _app._sync_running,
        "sync_proc_alive": _app._sync_proc is not None and _app._sync_proc.poll() is None if _app._sync_proc else False,
        "sync_log_count": len(_app._sync_logs),
        "sync_logs_last5": list(_app._sync_logs)[-5:] if _app._sync_logs else [],
        "update_running": _app._update_running,
        "update_log_count": len(_app._update_logs),
        "python": sys.executable,
        "sync_script": SYNC_SCRIPT,
        "sync_script_exists": Path(SYNC_SCRIPT).exists(),
        "cwd": str(BASE_DIR),
    })


# ---------------------------------------------------------------------------
# APScheduler — funções auxiliares
# ---------------------------------------------------------------------------

def _run_scheduled_sync(job_type):
    """Executa sync agendado (roda no thread do scheduler)."""
    import app as _app

    if _app._sync_running:
        logger.info("Scheduled %s skipped — sync already running", job_type)
        return

    mode = "full" if job_type == "sync_full" else "delta"
    _app._sync_running = True
    _app._sync_logs.clear()

    try:
        cmd = [sys.executable, SYNC_SCRIPT]
        if mode == "full":
            cmd.append("--full")

        _app._add_sync_log(f"[AGENDADO] Sincronização {mode.upper()} iniciada automaticamente")

        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
        )
        _app._sync_proc = proc

        for line in proc.stdout:
            _app._add_sync_log(line)

        proc.wait()

        if proc.returncode == 0:
            _app._add_sync_log("[FIM] Sincronização agendada concluída com sucesso")
        else:
            _app._add_sync_log(f"[ERRO] Sincronização agendada falhou (exit code {proc.returncode})")

        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("UPDATE schedules SET last_run_at = NOW() WHERE job_type = %s", (job_type,))
            conn.commit()
            conn.close()
        except Exception:
            pass

    except Exception as e:
        _app._add_sync_log(f"[ERRO] {e}")
    finally:
        _app._sync_proc = None
        _app._sync_running = False


def _register_schedule_job(schedule_id, job_type, cron_days, cron_hour, cron_minute, enabled):
    """Register or update a scheduler job."""
    try:
        scheduler.remove_job(schedule_id)
    except Exception:
        pass

    if not enabled:
        return

    if cron_days == "*":
        day_of_week = "*"
    else:
        parts = [d.strip() for d in cron_days.split(",")]
        day_of_week = ",".join(DAY_MAP.get(p, p) for p in parts)

    trigger = CronTrigger(
        day_of_week=day_of_week,
        hour=cron_hour,
        minute=cron_minute,
        timezone="America/Sao_Paulo",
    )

    scheduler.add_job(
        _run_scheduled_sync,
        trigger=trigger,
        args=[job_type],
        id=schedule_id,
        replace_existing=True,
        misfire_grace_time=300,
    )


def _load_schedules_from_db():
    """Load all schedules from DB and register them in APScheduler."""
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM schedules")
            for row in cur.fetchall():
                _register_schedule_job(
                    row["id"], row["job_type"], row["cron_days"],
                    row["cron_hour"], row["cron_minute"], row["enabled"],
                )
        conn.close()
        logger.info("Schedules loaded from DB")
    except Exception as e:
        logger.warning("Could not load schedules: %s", e)


DELTA_INTERVAL_MINUTES = int(os.getenv("KOMMO_DELTA_INTERVAL", "5"))
ACEITE_RECONCILE_MINUTES = int(os.getenv("ACEITE_RECONCILE_INTERVAL", "10"))


def register_delta_interval(sched):
    """Register a sync_delta job that runs every N minutes (default 5).
    Skips if another sync is already running (handled inside _run_scheduled_sync)."""
    try:
        sched.remove_job("sync_delta_interval")
    except Exception:
        pass

    sched.add_job(
        _run_scheduled_sync,
        trigger=IntervalTrigger(minutes=DELTA_INTERVAL_MINUTES),
        args=["sync_delta"],
        id="sync_delta_interval",
        replace_existing=True,
        misfire_grace_time=120,
        max_instances=1,
    )
    logger.info("Sync delta interval registered: every %d minutes", DELTA_INTERVAL_MINUTES)


def _run_aceite_reconcile():
    """Run Kommo aceite reconciliation in scheduler thread."""
    try:
        from routes.kommo_sync import reconcile_aceite_leads
        result = reconcile_aceite_leads()
        logger.info("Aceite reconcile result: %s", result)
    except Exception as e:
        logger.error("Aceite reconcile error: %s", e)


def register_aceite_reconcile(sched):
    """Register periodic reconciliation of aceite leads (default every 10 min)."""
    try:
        sched.remove_job("aceite_reconcile")
    except Exception:
        pass

    sched.add_job(
        _run_aceite_reconcile,
        trigger=IntervalTrigger(minutes=ACEITE_RECONCILE_MINUTES),
        args=[],
        id="aceite_reconcile",
        replace_existing=True,
        misfire_grace_time=300,
        max_instances=1,
    )
    logger.info("Aceite reconcile registered: every %d minutes", ACEITE_RECONCILE_MINUTES)
