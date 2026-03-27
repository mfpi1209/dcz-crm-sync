"""
eduit. — Sync Comercial (Kommo CRM).
Integração com o projeto Kommo_Update para sincronização de leads/contatos.
"""

import os
import sys
import json
import uuid
import logging
import threading
import subprocess
import time as _time
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

import requests as _requests
import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)

kommo_bp = Blueprint("kommo_bp", __name__)

_kommo_lib = Path(__file__).resolve().parent.parent / "kommo_lib"
_kommo_ext = Path(__file__).resolve().parent.parent / "Kommo_Update"
KOMMO_DIR = str(_kommo_ext) if _kommo_ext.is_dir() else (str(_kommo_lib) if _kommo_lib.is_dir() else None)

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

        import time as _time
        today_start = int(_time.mktime(
            datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timetuple()
        ))
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM leads WHERE created_at >= %s AND is_deleted = false",
            (today_start,)
        )
        new_today = cur.fetchone()["cnt"]

        conn.close()
        return jsonify({
            "ok": True,
            "data": {
                "entities": entities,
                "leads_count": leads,
                "contacts_count": contacts,
                "history": history,
                "new_today": new_today,
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
    if not KOMMO_DIR:
        return jsonify({
            "ok": False,
            "error": "Sync indisponível neste ambiente. A pasta Kommo_Update não está presente. "
                     "Execute a sincronização pelo servidor local (Windows).",
        }), 400

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

    def _log(msg, progress=None):
        t = datetime.now().strftime("%H:%M:%S")
        _tasks[task_id]["log"].append({"time": t, "msg": msg})
        _tasks[task_id]["message"] = msg
        if progress is not None:
            _tasks[task_id]["progress"] = progress

    def _stream(proc, label, base_pct, end_pct):
        """Lê stdout linha a linha e atualiza o log em tempo real."""
        lines_read = 0
        for raw in iter(proc.stdout.readline, ""):
            line = raw.strip()
            if not line:
                continue
            lines_read += 1
            _log(line)
            if lines_read % 5 == 0:
                pct = min(base_pct + int((end_pct - base_pct) * 0.8), end_pct - 1)
                _tasks[task_id]["progress"] = pct
        proc.stdout.close()
        proc.wait()
        return proc.returncode

    def _run():
        try:
            env = {**os.environ}
            cmd = [sys.executable, "-u", "main.py"]
            if mode == "full":
                cmd.append("--full")

            _log(f"Executando: {' '.join(cmd)}", 5)

            proc = subprocess.Popen(
                cmd, cwd=KOMMO_DIR,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, env=env,
            )

            rc = _stream(proc, "sync", 5, 80)

            if rc == 0:
                _log("Sync concluído. Migrando para PostgreSQL...", 82)

                mig = subprocess.Popen(
                    [sys.executable, "-u", "migrate_to_postgres.py", "--light"],
                    cwd=KOMMO_DIR,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, env=env,
                )
                mig_rc = _stream(mig, "migrate", 82, 98)

                if mig_rc == 0:
                    _log("PostgreSQL atualizado!", 99)
                else:
                    _log(f"Aviso PG: retorno {mig_rc}", 99)

                _tasks[task_id]["progress"] = 100
                _tasks[task_id]["status"] = "completed"
                _log("Sincronização concluída com sucesso!", 100)
            else:
                _tasks[task_id]["status"] = "error"
                _log(f"Sync falhou (código {rc})")

        except Exception as e:
            _tasks[task_id]["status"] = "error"
            _log(f"Exceção: {e}")

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


# ── Funnel LIVE (Kommo API v4) ────────────────────────────────────────────

KOMMO_API_BASE = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")

FUNNEL_PIPELINE = 5481944
FUNNEL_STAGES_DEF = [
    {"key": "incoming",              "id": 48539237, "label": "Incoming"},
    {"key": "contato_inicial",       "id": 48539240, "label": "Contato Inicial"},
    {"key": "sem_resposta",          "id": 48539243, "label": "Sem Resposta"},
    {"key": "em_atendimento",        "id": 48539246, "label": "Em Atendimento"},
    {"key": "aguardando_resposta",   "id": 74941508, "label": "Aguardando Resposta"},
    {"key": "aguardando_inscricao",  "id": 99045180, "label": "Aguardando Inscrição"},
    {"key": "inscricao",             "id": 48539249, "label": "Inscrição"},
    {"key": "processo_seletivo",     "id": 48566195, "label": "Processo Seletivo"},
    {"key": "em_processo",           "id": 48566198, "label": "Em Processo"},
    {"key": "aprovado_reprovado",    "id": 48566201, "label": "Aprovados/Reprovados"},
    {"key": "boleto_enviado",        "id": 48566204, "label": "Boleto Enviado"},
    {"key": "aceite",                "id": 48566207, "label": "Aceite"},
    {"key": "qualificacao",          "id": 53917599, "label": "Qualificação"},
    {"key": "pagamento_confirmado",  "id": 77728584, "label": "Pagamento Confirmado"},
]

_STAGE_ID_TO_DEF = {s["id"]: s for s in FUNNEL_STAGES_DEF}

FUNNEL_HIGHLIGHT = [
    "aguardando_inscricao", "inscricao", "processo_seletivo",
    "em_processo", "aprovado_reprovado", "aceite",
]

_funnel_cache = {"data": None, "ts": 0}
_FUNNEL_CACHE_TTL = 120
_bg_fetch_lock = threading.Lock()
_bg_fetch_running = False


def _kommo_get(path, params=None):
    base = KOMMO_API_BASE.rstrip("/")
    if "/api/v4" not in base:
        url = f"{base}/api/v4{path}"
    else:
        url = f"{base}{path}"
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
    return _requests.get(url, headers=headers, params=params, timeout=30)


# ── PG-based instant funnel counts ───────────────────────────────────────

def _funnel_from_pg():
    """Instant funnel counts from the locally-synced leads table."""
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT status_id, COUNT(*) AS cnt
            FROM leads
            WHERE pipeline_id = %s AND NOT COALESCE(is_deleted, false)
            GROUP BY status_id
        """, (FUNNEL_PIPELINE,))
        rows = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) FROM leads
            WHERE created_at >= %s AND NOT COALESCE(is_deleted, false)
        """, (int(datetime.now(timezone(timedelta(hours=-3)))
                   .replace(hour=0, minute=0, second=0, microsecond=0)
                   .timestamp()),))
        new_today = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        logger.error("_funnel_from_pg error: %s", e)
        return None

    counts = {r[0]: r[1] for r in rows}
    stages = []
    total = 0
    for sdef in FUNNEL_STAGES_DEF:
        c = counts.get(sdef["id"], 0)
        total += c
        stages.append({
            "key": sdef["key"], "id": sdef["id"], "label": sdef["label"],
            "count": c, "highlight": sdef["key"] in FUNNEL_HIGHLIGHT,
        })
    for s in stages:
        s["pct"] = round(s["count"] / total * 100, 1) if total > 0 else 0

    return {
        "stages": stages, "total": total, "new_today": new_today,
        "leads_fetched": total, "pages": 0,
    }


# ── Kommo API live fetch ─────────────────────────────────────────────────

def _count_new_leads_today():
    BRT = timezone(timedelta(hours=-3))
    today_start = int(datetime.now(BRT).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    count, seen, page = 0, set(), 1
    while True:
        try:
            r = _kommo_get("/leads", {"filter[created_at][from]": today_start, "limit": 250, "page": page})
        except Exception as e:
            logger.error("count_new_leads API error: %s", e)
            break
        if r.status_code != 200:
            break
        data = r.json()
        leads = data.get("_embedded", {}).get("leads", [])
        if not leads:
            break
        for lead in leads:
            lid = lead.get("id")
            if lid and lid not in seen:
                seen.add(lid)
                count += 1
        if "next" not in data.get("_links", {}):
            break
        page += 1
        _time.sleep(0.05)
    return count


def _fetch_funnel_live():
    """Fetch all leads in the funnel pipeline from Kommo API v4."""
    stage_ids = [s["id"] for s in FUNNEL_STAGES_DEF]
    all_leads, seen_ids, page = [], set(), 1
    while True:
        params = {"limit": 250, "page": page}
        for i, sid in enumerate(stage_ids):
            params[f"filter[statuses][{i}][pipeline_id]"] = FUNNEL_PIPELINE
            params[f"filter[statuses][{i}][status_id]"] = sid
        try:
            r = _kommo_get("/leads", params)
        except Exception as e:
            logger.error("Kommo API error: %s", e)
            break
        if r.status_code != 200:
            break
        data = r.json()
        leads = data.get("_embedded", {}).get("leads", [])
        if not leads:
            break
        for lead in leads:
            lid = lead.get("id")
            if lid and lid not in seen_ids:
                seen_ids.add(lid)
                all_leads.append(lead)
        if "_links" not in data or "next" not in data["_links"]:
            break
        page += 1
        _time.sleep(0.05)

    counts = {}
    for lead in all_leads:
        counts[lead.get("status_id")] = counts.get(lead.get("status_id"), 0) + 1

    stages, total = [], 0
    for sdef in FUNNEL_STAGES_DEF:
        c = counts.get(sdef["id"], 0)
        total += c
        stages.append({
            "key": sdef["key"], "id": sdef["id"], "label": sdef["label"],
            "count": c, "highlight": sdef["key"] in FUNNEL_HIGHLIGHT,
        })
    for s in stages:
        s["pct"] = round(s["count"] / total * 100, 1) if total > 0 else 0

    return {"stages": stages, "total": total, "leads_fetched": len(all_leads), "pages": page}


# ── DB snapshot (replaces JSON file) ─────────────────────────────────────

def _save_funnel_to_db(result):
    """Persist a funnel snapshot into kommo_funnel_log (dcz_sync)."""
    from db import get_conn
    BRT = timezone(timedelta(hours=-3))
    today = datetime.now(BRT).date()
    stages_json = {s["key"]: s["count"] for s in result["stages"]}
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO kommo_funnel_log (captured_date, source, total, new_today, stages)
                VALUES (%s, 'live', %s, %s, %s)
            """, (today, result["total"], result.get("new_today", 0), json.dumps(stages_json)))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("_save_funnel_to_db: %s", e)


def _get_d0_from_db():
    """Get today's D0 and yesterday's last snapshot from the DB."""
    from db import get_conn
    BRT = timezone(timedelta(hours=-3))
    today = datetime.now(BRT).date()
    yesterday = today - timedelta(days=1)
    d0, yd = {}, None
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT stages, total FROM kommo_funnel_log
                WHERE captured_date = %s ORDER BY captured_at ASC LIMIT 1
            """, (today,))
            row = cur.fetchone()
            if row:
                d0 = row["stages"] if isinstance(row["stages"], dict) else json.loads(row["stages"])
                d0["_total"] = row["total"]

            cur.execute("""
                SELECT stages, total FROM kommo_funnel_log
                WHERE captured_date = %s ORDER BY captured_at DESC LIMIT 1
            """, (yesterday,))
            row = cur.fetchone()
            if row:
                yd = row["stages"] if isinstance(row["stages"], dict) else json.loads(row["stages"])
                yd["_total"] = row["total"]
        conn.close()
    except Exception as e:
        logger.error("_get_d0_from_db: %s", e)
    return d0, yd


def _enrich_with_d0(result):
    """Add D0 / yesterday delta info to each stage."""
    d0, yesterday = _get_d0_from_db()
    BRT = timezone(timedelta(hours=-3))

    if not d0:
        _save_funnel_to_db(result)
        d0 = {s["key"]: s["count"] for s in result["stages"]}

    for s in result["stages"]:
        d0_val = d0.get(s["key"], s["count"])
        s["d0"] = d0_val
        delta = s["count"] - d0_val
        s["delta"] = delta
        s["delta_pct"] = round(delta / d0_val * 100, 1) if d0_val > 0 else 0
        if yesterday:
            yd = yesterday.get(s["key"], 0)
            s["yesterday"] = yd
            s["delta_yesterday"] = s["count"] - yd
        else:
            s["yesterday"] = None
            s["delta_yesterday"] = None

    result["d0_date"] = datetime.now(BRT).date().isoformat()
    result["fetched_at"] = datetime.now(BRT).strftime("%H:%M:%S")
    return result


# ── Background live refresh ──────────────────────────────────────────────

def _bg_live_refresh():
    """Run in a background thread: fetch live data, save to DB, update cache."""
    global _bg_fetch_running
    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_funnel = pool.submit(_fetch_funnel_live)
            fut_count = pool.submit(_count_new_leads_today)

        result = fut_funnel.result()
        try:
            result["new_today"] = fut_count.result()
        except Exception:
            result["new_today"] = 0

        _save_funnel_to_db(result)
        result = _enrich_with_d0(result)
        result["source"] = "live"

        _funnel_cache["data"] = result
        _funnel_cache["ts"] = _time.time()
        logger.info("bg_live_refresh complete: %d leads", result["total"])
    except Exception as e:
        logger.error("bg_live_refresh error: %s", e)
    finally:
        with _bg_fetch_lock:
            _bg_fetch_running = False


# ── API endpoint ─────────────────────────────────────────────────────────

@kommo_bp.route("/api/kommo/funnel-live")
def api_kommo_funnel_live():
    """
    Returns funnel data instantly (PG or cache), triggers live refresh in background.
    ?force=1  — ignore cache, wait for PG
    ?poll=1   — return live data only if available (for progressive loading)
    """
    global _bg_fetch_running
    force = request.args.get("force", "0") == "1"
    poll = request.args.get("poll", "0") == "1"
    now = _time.time()
    cache_valid = _funnel_cache["data"] and (now - _funnel_cache["ts"]) < _FUNNEL_CACHE_TTL

    if poll:
        if _funnel_cache["data"] and _funnel_cache["data"].get("source") == "live" and cache_valid:
            return jsonify({"ok": True, "data": _funnel_cache["data"], "source": "live"})
        return jsonify({"ok": True, "data": None, "source": "pending"})

    if not force and cache_valid:
        return jsonify({"ok": True, "data": _funnel_cache["data"],
                        "source": _funnel_cache["data"].get("source", "cache"), "cached": True})

    pg_result = _funnel_from_pg()
    if pg_result:
        pg_result = _enrich_with_d0(pg_result)
        pg_result["source"] = "pg"

    if not force and pg_result:
        if not _funnel_cache["data"] or (now - _funnel_cache["ts"]) > _FUNNEL_CACHE_TTL:
            _funnel_cache["data"] = pg_result
            _funnel_cache["ts"] = now

    should_bg = KOMMO_TOKEN and (force or not cache_valid)
    if should_bg:
        with _bg_fetch_lock:
            if not _bg_fetch_running:
                _bg_fetch_running = True
                threading.Thread(target=_bg_live_refresh, daemon=True).start()

    data = pg_result or _funnel_cache.get("data")
    if data:
        return jsonify({"ok": True, "data": data, "source": data.get("source", "pg")})

    return jsonify({"ok": False, "error": "Sem dados disponíveis"}), 503


# ── Funnel history endpoint ──────────────────────────────────────────────

@kommo_bp.route("/api/kommo/funnel-history")
def api_kommo_funnel_history():
    """Return the last snapshot of each day for the past N days."""
    from db import get_conn
    days = request.args.get("days", 7, type=int)
    BRT = timezone(timedelta(hours=-3))
    since = (datetime.now(BRT).date() - timedelta(days=days)).isoformat()
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (captured_date)
                    captured_date, captured_at, total, new_today, stages
                FROM kommo_funnel_log
                WHERE captured_date >= %s
                ORDER BY captured_date, captured_at DESC
            """, (since,))
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                r["captured_date"] = r["captured_date"].isoformat()
                r["captured_at"] = r["captured_at"].isoformat()
                if isinstance(r["stages"], str):
                    r["stages"] = json.loads(r["stages"])
        conn.close()
        return jsonify({"ok": True, "data": rows})
    except Exception as e:
        logger.error("funnel-history error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500
