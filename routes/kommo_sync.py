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
    {"key": "pagamento_confirmado",  "id": 77728584, "label": "Pagamento Confirmado"},
]

FUNNEL_HIGHLIGHT = [
    "aguardando_inscricao", "inscricao", "processo_seletivo",
    "em_processo", "aprovado_reprovado", "aceite",
]

_funnel_cache = {"data": None, "ts": 0}
_FUNNEL_CACHE_TTL = 300

SNAPSHOT_FILE = Path(__file__).resolve().parent.parent / "data" / "funnel_snapshot.json"


def _kommo_get(path, params=None):
    base = KOMMO_API_BASE.rstrip("/")
    if "/api/v4" not in base:
        url = f"{base}/api/v4{path}"
    else:
        url = f"{base}{path}"
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
    return _requests.get(url, headers=headers, params=params, timeout=30)


def _fetch_funnel_live():
    """Fetch all leads in the funnel pipeline from Kommo API v4, count by status."""
    stage_ids = [s["id"] for s in FUNNEL_STAGES_DEF]
    all_leads = []
    seen_ids = set()
    page = 1

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
            logger.warning("Kommo API %d: %s", r.status_code, r.text[:200])
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
        _time.sleep(0.2)

    BRT = timezone(timedelta(hours=-3))
    today_brt = datetime.now(BRT).replace(hour=0, minute=0, second=0, microsecond=0)
    today_start = int(today_brt.timestamp())

    counts = {}
    new_today = 0
    new_by_stage = {}
    wrong_pipeline = 0
    for lead in all_leads:
        sid = lead.get("status_id")
        pid = lead.get("pipeline_id")
        counts[sid] = counts.get(sid, 0) + 1
        if pid != FUNNEL_PIPELINE:
            wrong_pipeline += 1
        if lead.get("created_at", 0) >= today_start:
            new_today += 1
            new_by_stage[sid] = new_by_stage.get(sid, 0) + 1

    if wrong_pipeline:
        logger.warning("funnel-live: %d leads from wrong pipeline!", wrong_pipeline)

    stage_key_map = {s["id"]: s["key"] for s in FUNNEL_STAGES_DEF}

    stages = []
    total = 0
    for sdef in FUNNEL_STAGES_DEF:
        c = counts.get(sdef["id"], 0)
        total += c
        stages.append({
            "key": sdef["key"],
            "id": sdef["id"],
            "label": sdef["label"],
            "count": c,
            "new_today": new_by_stage.get(sdef["id"], 0),
            "highlight": sdef["key"] in FUNNEL_HIGHLIGHT,
        })

    for s in stages:
        s["pct"] = round(s["count"] / total * 100, 1) if total > 0 else 0

    logger.info("funnel-live: total=%d new_today=%d wrong_pipeline=%d breakdown=%s",
                total, new_today, wrong_pipeline,
                {stage_key_map.get(k, k): v for k, v in new_by_stage.items()})

    return {
        "stages": stages,
        "total": total,
        "new_today": new_today,
        "leads_fetched": len(all_leads),
        "pages": page,
        "debug": {
            "wrong_pipeline": wrong_pipeline,
            "new_by_stage": {stage_key_map.get(k, str(k)): v for k, v in new_by_stage.items()},
            "today_start_ts": today_start,
            "today_start_brt": today_brt.isoformat(),
        },
    }


def _load_snapshot():
    try:
        if SNAPSHOT_FILE.exists():
            with open(SNAPSHOT_FILE, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_snapshot(snapshots):
    SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshots, f, indent=2, ensure_ascii=False)


def _get_snapshot_d0(current_stages):
    """Get or create today's D0 snapshot. Returns yesterday's snapshot for delta."""
    BRT = timezone(timedelta(hours=-3))
    today = datetime.now(BRT).date().isoformat()
    snapshots = _load_snapshot()

    if today not in snapshots:
        snapshots[today] = {s["key"]: s["count"] for s in current_stages}
        snapshots[today]["_total"] = sum(s["count"] for s in current_stages)
        old = sorted(k for k in snapshots if k != today)
        for k in old[:-7]:
            del snapshots[k]
        _save_snapshot(snapshots)

    d0 = snapshots.get(today, {})

    dates_sorted = sorted(snapshots.keys())
    yesterday = None
    for dt in dates_sorted:
        if dt < today:
            yesterday = snapshots[dt]

    return d0, yesterday


@kommo_bp.route("/api/kommo/funnel-live")
def api_kommo_funnel_live():
    """Fetch real-time funnel data from Kommo API v4."""
    force = request.args.get("force", "0") == "1"
    now = _time.time()

    if not force and _funnel_cache["data"] and (now - _funnel_cache["ts"]) < _FUNNEL_CACHE_TTL:
        return jsonify({"ok": True, "data": _funnel_cache["data"], "cached": True})

    if not KOMMO_TOKEN:
        return jsonify({"ok": False, "error": "KOMMO_TOKEN não configurado"}), 500

    try:
        result = _fetch_funnel_live()
        d0, yesterday = _get_snapshot_d0(result["stages"])

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

        BRT = timezone(timedelta(hours=-3))
        result["d0_date"] = datetime.now(BRT).date().isoformat()
        result["fetched_at"] = datetime.now(BRT).strftime("%H:%M:%S")

        _funnel_cache["data"] = result
        _funnel_cache["ts"] = now

        return jsonify({"ok": True, "data": result})
    except Exception as e:
        logger.error("funnel-live error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500
