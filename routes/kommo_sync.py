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
# Antes: preferia Kommo_Update e o painel rodava cópia antiga (sem fixes do repo).
# Agora: kommo_lib versionado primeiro; Kommo_Update só fallback; ou KOMMO_SYNC_DIR.
_env_kommo = os.getenv("KOMMO_SYNC_DIR", "").strip()
if _env_kommo and Path(_env_kommo).is_dir():
    KOMMO_DIR = _env_kommo
elif _kommo_lib.is_dir():
    KOMMO_DIR = str(_kommo_lib)
elif _kommo_ext.is_dir():
    KOMMO_DIR = str(_kommo_ext)
else:
    KOMMO_DIR = None

PG_KOMMO = {
    "host": os.getenv("KOMMO_PG_HOST", "31.97.91.47"),
    "port": int(os.getenv("KOMMO_PG_PORT", "5432")),
    "dbname": os.getenv("KOMMO_PG_DB", "kommo_sync"),
    "user": os.getenv("KOMMO_PG_USER", os.getenv("DB_USER", "adm_eduit")),
    "password": os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS", "IaDm24Sx3HxrYoqT")),
}

_tasks = {}


def _pg():
    return psycopg2.connect(**PG_KOMMO)


# ── Status da sincronização ──────────────────────────────────────────────

@kommo_bp.route("/api/kommo/connection-test", methods=["GET"])
def api_kommo_connection_test():
    """Testa token Kommo sem rodar sync completo."""
    token = os.getenv("KOMMO_TOKEN", "") or ""
    base = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com").strip().rstrip("/")
    if not token:
        return jsonify({"ok": False, "error": "KOMMO_TOKEN não configurado no servidor."}), 500
    url = base if base.endswith("/api/v4") else f"{base}/api/v4/account"
    try:
        r = _requests.get(
            url,
            headers={
                **{
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                },
                "Authorization": f"Bearer {token}",
            },
            timeout=15,
        )
        if r.status_code == 200:
            d = r.json()
            return jsonify({
                "ok": True,
                "account": d.get("name"),
                "subdomain": d.get("subdomain"),
                "token_length": len(token),
            })
        return jsonify({
            "ok": False,
            "http_status": r.status_code,
            "error": (r.text or "")[:200],
        }), 502
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@kommo_bp.route("/api/_diag/kommo")
def api_diag_kommo():
    """Diagnostico de conectividade Kommo a partir do container.

    Retorna num JSON unico:
      - file_mtime: timestamp da ultima modificacao deste arquivo (proxy de versao do deploy)
      - has_default_headers: True se _KOMMO_DEFAULT_HEADERS existe (codigo novo)
      - env: snapshot das envs relevantes
      - egress_ip: IP publico de saida do container
      - request: headers que o Flask manda
      - response: status, headers, primeiros 500 chars do body
    """
    diag = {"ok": True}

    try:
        diag["file_mtime"] = datetime.fromtimestamp(
            Path(__file__).stat().st_mtime, _BRT
        ).isoformat()
    except Exception as e:
        diag["file_mtime_error"] = str(e)

    diag["has_default_headers"] = "_KOMMO_DEFAULT_HEADERS" in globals()

    diag["env"] = {
        "KOMMO_BASE_URL": os.getenv("KOMMO_BASE_URL", "<unset>"),
        "KOMMO_TOKEN_length": len(os.getenv("KOMMO_TOKEN", "") or ""),
        "KOMMO_TOKEN_prefix": (os.getenv("KOMMO_TOKEN", "") or "")[:20],
    }

    try:
        ip_r = _requests.get("https://api.ipify.org?format=json", timeout=8)
        diag["egress_ip"] = ip_r.json().get("ip") if ip_r.status_code == 200 else f"http {ip_r.status_code}"
    except Exception as e:
        diag["egress_ip"] = f"err: {e}"

    token = os.getenv("KOMMO_TOKEN", "") or ""
    base = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com").strip().rstrip("/")
    url = base if base.endswith("/api/v4") else f"{base}/api/v4/account"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Authorization": f"Bearer {token}",
    }
    diag["request"] = {"url": url, "headers_sent": {k: v for k, v in headers.items() if k != "Authorization"}}

    try:
        r = _requests.get(url, headers=headers, timeout=15)
        diag["response"] = {
            "status": r.status_code,
            "headers": dict(r.headers),
            "body_preview": (r.text or "")[:500],
        }
    except Exception as e:
        diag["response"] = {"error": str(e)}

    return jsonify(diag)


@kommo_bp.route("/api/_diag/kommo/probe")
def api_diag_kommo_probe():
    """Tenta varias combinacoes de headers em sequencia, retorna qual passa.

    Util pra descobrir o que o WAF da Kommo exige quando bloqueia IPs de
    datacenter (alem de User-Agent realista, pode exigir Sec-Fetch-*,
    Sec-Ch-Ua-*, Origin, Referer, ou nenhum desses).
    """
    token = os.getenv("KOMMO_TOKEN", "") or ""
    base = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com").strip().rstrip("/")
    url = base if base.endswith("/api/v4") else f"{base}/api/v4/account"

    UA_CHROME = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    UA_CURL = "curl/8.4.0"
    UA_PYTHON = "python-requests/2.32.0"

    common = {"Authorization": f"Bearer {token}"}

    variants = [
        ("01_only_auth", {**common}),
        ("02_ua_curl", {**common, "User-Agent": UA_CURL, "Accept": "*/*"}),
        ("03_ua_python", {**common, "User-Agent": UA_PYTHON, "Accept": "*/*"}),
        ("04_ua_chrome_basic", {**common, "User-Agent": UA_CHROME, "Accept": "application/json"}),
        ("05_ua_chrome_full", {
            **common,
            "User-Agent": UA_CHROME,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }),
        ("06_ua_chrome_with_origin_referer", {
            **common,
            "User-Agent": UA_CHROME,
            "Accept": "application/json, text/plain, */*",
            "Origin": base,
            "Referer": f"{base}/",
        }),
    ]

    results = []
    for name, headers in variants:
        try:
            r = _requests.get(url, headers=headers, timeout=12)
            results.append({
                "variant": name,
                "status": r.status_code,
                "server_header": r.headers.get("Server"),
                "x_error": r.headers.get("X-Error"),
                "body_first_120": (r.text or "")[:120],
            })
        except Exception as e:
            results.append({"variant": name, "error": str(e)})

    return jsonify({
        "ok": True,
        "url": url,
        "egress_hint": "veja /api/_diag/kommo p/ ver IP de saida",
        "results": results,
    })


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

        _lead_synced_since = (
            "NULLIF(trim(l.synced_at), '')::timestamptz >= NOW() - (%s * INTERVAL '1 hour')"
        )
        _synced_since = (
            "NULLIF(trim(synced_at), '')::timestamptz >= NOW() - (%s * INTERVAL '1 hour')"
        )
        cur.execute(f"""
            SELECT p.name AS pipeline_name, ps.name AS stage_name, COUNT(*) AS total
            FROM leads l
            JOIN pipeline_statuses ps ON ps.id = l.status_id
            JOIN pipelines p ON p.id = l.pipeline_id
            WHERE {_lead_synced_since}
            GROUP BY p.name, ps.name, ps.sort, p.sort
            ORDER BY p.sort, ps.sort
        """, (hours,))
        by_stage = [dict(r) for r in cur.fetchall()]

        cur.execute(f"SELECT COUNT(*) AS t FROM leads WHERE {_synced_since}", (hours,))
        leads_upd = cur.fetchone()["t"]

        cur.execute(f"SELECT COUNT(*) AS t FROM contacts WHERE {_synced_since}", (hours,))
        contacts_upd = cur.fetchone()["t"]

        since_ts = int(_time.time()) - (hours * 3600)
        cur.execute("SELECT COUNT(*) AS t FROM leads WHERE created_at >= %s AND is_deleted = false", (since_ts,))
        new_leads = cur.fetchone()["t"]

        cur.execute(
            f"SELECT COUNT(*) AS t FROM leads WHERE status_id = 142 AND {_synced_since}",
            (hours,),
        )
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
            "error": "Sync indisponível: pasta kommo_lib (ou KOMMO_SYNC_DIR) não encontrada no servidor.",
        }), 400

    for t in _tasks.values():
        if t.get("type") == "sync" and t.get("status") == "running":
            return jsonify({"ok": False, "error": "Sincronização já em andamento."}), 409

    body = request.json or {}
    mode = body.get("mode", "delta")
    task_id = str(uuid.uuid4())[:8]

    _t0 = datetime.now().strftime("%H:%M:%S")
    _mode_label = "INCREMENTAL" if mode != "full" else "FULL"
    _tasks[task_id] = {
        "type": "sync",
        "mode": mode,
        "status": "running",
        "progress": 1,
        "message": f"Iniciando sync {_mode_label}...",
        "started_at": datetime.now().isoformat(),
        "log": [
            {"time": _t0, "msg": f"Modo solicitado: {_mode_label} (botão {'Incremental' if mode != 'full' else 'Full'})"},
            {"time": _t0, "msg": "Tarefa aceita. Subindo processo (pode levar alguns segundos até o primeiro log)..."},
        ],
    }

    def _log(msg, progress=None):
        t = datetime.now().strftime("%H:%M:%S")
        _tasks[task_id]["log"].append({"time": t, "msg": msg})
        _tasks[task_id]["message"] = msg
        if progress is not None:
            _tasks[task_id]["progress"] = progress

    def _stream(proc, label, base_pct, end_pct):
        """Lê stdout linha a linha e atualiza o log em tempo real.

        Lê em modo binário e decodifica UTF-8 no Flask — no Windows, text=True no Popen
        ainda pode usar cp1252 em alguns builds e quebrar em bytes como 0x8d.
        """
        lines_read = 0
        while True:
            chunk = proc.stdout.readline()
            if not chunk:
                break
            line = chunk.decode("utf-8", errors="replace").strip()
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

    def _dual_write_on():
        v = os.getenv("KOMMO_DUAL_WRITE_PG", "1").strip().lower()
        return v in ("1", "true", "yes", "on")

    def _run():
        try:
            # Windows: sem PYTHONUNBUFFERED o stdout de main.py pode ficar preso em buffer (UI em 0% por minutos).
            env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
            # Full sync: grava só no SQLite local (rápido); migração em massa para PG no fim.
            # Incremental: dual-write durante o sync (poucos registros) e pula migrate redundante.
            if mode == "full":
                env["KOMMO_DUAL_WRITE_PG"] = "0"
                _log("Full sync: espelho PG desligado durante download (migração em lote no fim).", 3)
            elif _dual_write_on():
                _log("Incremental: espelho PG ativo durante o sync.", 3)

            cmd = [sys.executable, "-u", "main.py"]
            if mode == "full":
                cmd.append("--full")

            _log(f"Pasta do sync: {KOMMO_DIR}", 4)
            _log(f"Executando: {' '.join(cmd)}", 5)

            proc = subprocess.Popen(
                cmd, cwd=KOMMO_DIR,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                bufsize=0, env=env,
            )
            _tasks[task_id]["proc"] = proc

            rc = _stream(proc, "sync", 5, 80)

            if _tasks[task_id].get("cancelled"):
                _tasks[task_id]["status"] = "cancelled"
                _log("[CANCELADO] Sincronização interrompida pelo usuário.")
                return

            if rc == 0:
                skip_migrate = mode != "full" and _dual_write_on()
                if skip_migrate:
                    _log("Sync concluído. PostgreSQL já atualizado (dual-write).", 90)
                    mig_rc = 0
                else:
                    # Full sync: migração completa (inclui lead_custom_field_values em massa).
                    # Delta sem dual-write: --light + cf_values.
                    mig_args = [sys.executable, "-u", "migrate_to_postgres.py"]
                    if mode != "full":
                        mig_args.append("--light")
                    _log(
                        "Sync concluído. Migrando para PostgreSQL (%s)..."
                        % ("FULL" if mode == "full" else "LIGHT+cf_values"),
                        82,
                    )

                    mig = subprocess.Popen(
                        mig_args,
                        cwd=KOMMO_DIR,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        bufsize=0, env=env,
                    )
                    _tasks[task_id]["proc"] = mig
                    mig_rc = _stream(mig, "migrate", 82, 98)

                    if mig_rc == 0:
                        _log("PostgreSQL atualizado!", 99)
                    else:
                        _log(f"Aviso PG: retorno {mig_rc}", 99)

                _tasks[task_id]["progress"] = 100
                _tasks[task_id]["status"] = "completed"
                _log("Sincronização concluída com sucesso!", 100)
            else:
                if not _tasks[task_id].get("cancelled"):
                    _tasks[task_id]["status"] = "error"
                    _log(f"Sync falhou (código {rc})")

        except Exception as e:
            _tasks[task_id]["status"] = "error"
            _log(f"Exceção: {e}")
        finally:
            _tasks[task_id].pop("proc", None)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return jsonify({"ok": True, "task_id": task_id})


@kommo_bp.route("/api/kommo/sync/cancel", methods=["POST"])
def api_kommo_sync_cancel():
    """Cancela o sync em andamento matando o subprocess."""
    body = request.json or {}
    task_id = body.get("task_id")

    task = _tasks.get(task_id) if task_id else None
    if not task:
        # Tenta achar qualquer task de sync rodando
        task = next((t for t in _tasks.values() if t.get("type") == "sync" and t.get("status") == "running"), None)

    if not task:
        return jsonify({"ok": False, "error": "Nenhum sync em andamento."}), 404

    task["cancelled"] = True
    proc = task.get("proc")
    if proc:
        try:
            proc.kill()
        except Exception:
            pass

    task["status"] = "cancelled"
    task["message"] = "Cancelado pelo usuário."
    return jsonify({"ok": True})


# ── Task progress ────────────────────────────────────────────────────────

@kommo_bp.route("/api/kommo/task/<task_id>")
def api_kommo_task(task_id):
    task = _tasks.get(task_id)
    if not task:
        return jsonify({"ok": False, "error": "Tarefa não encontrada"}), 404
    t = {k: v for k, v in task.items() if k != "proc"}  # Popen não é JSON-serializável
    if "log" in t and len(t["log"]) > 30:
        t["log"] = t["log"][-30:]
    return jsonify({"ok": True, "data": t})


# ── Funnel LIVE (Kommo API v4) ────────────────────────────────────────────

KOMMO_API_BASE = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")


def _kommo_token():
    """Lê token no momento da chamada (.env ou app_config do painel Config)."""
    t = (os.getenv("KOMMO_TOKEN", "") or KOMMO_TOKEN or "").strip()
    if t:
        return t
    try:
        from kommo_lib.config import KOMMO_TOKEN as _cfg_tok
        return (_cfg_tok or "").strip()
    except Exception:
        return ""

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
    {"key": "qualificacao",          "id": 53917599, "label": "ROBÔ"},
    {"key": "pagamento_confirmado",  "id": 77728584, "label": "Pagamento Confirmado"},
]

FUNNEL_HIGHLIGHT = [
    "em_atendimento",
    "aguardando_inscricao", "inscricao", "processo_seletivo",
    "em_processo", "aprovado_reprovado", "aceite",
]

_funnel_cache = {"data": None, "ts": 0}
_FUNNEL_CACHE_TTL = 300
# Easypanel/nginx costuma cortar em ~60s; live Kommo pode levar 40s+.
_FUNNEL_LIVE_TIMEOUT_S = int(os.getenv("FUNNEL_LIVE_TIMEOUT_S", "25"))
_FUNNEL_LIVE_FORCE_TIMEOUT_S = int(os.getenv("FUNNEL_LIVE_FORCE_TIMEOUT_S", "45"))

SNAPSHOT_FILE = Path(__file__).resolve().parent.parent / "data" / "funnel_snapshot.json"


# Headers padrao para chamadas Kommo. User-Agent realista evita bloqueio
# por WAF/nginx em datacenters (o default 'python-requests/X.Y' costuma
# cair em blacklist de bots, retornando 403 nginx em produ��o).
_KOMMO_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


def _kommo_get(path, params=None):
    base = KOMMO_API_BASE.rstrip("/")
    if "/api/v4" not in base:
        url = f"{base}/api/v4{path}"
    else:
        url = f"{base}{path}"
    headers = dict(_KOMMO_DEFAULT_HEADERS)
    headers["Authorization"] = f"Bearer {_kommo_token()}"
    return _requests.get(url, headers=headers, params=params, timeout=30)


_BRT = timezone(timedelta(hours=-3))


def _day_bounds_brt(d: date):
    """Unix timestamps [início, fim] do dia civil em BRT."""
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=_BRT)
    end = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=_BRT)
    return int(start.timestamp()), int(end.timestamp())


def _count_new_leads_between(from_ts: int, to_ts: int | None = None, pipeline_id: int | None = None):
    """Count leads created in [from_ts, to_ts]. pipeline_id=None = todos; default funil = widget Kommo."""
    count = 0
    seen = set()
    page = 1
    params_base = {"filter[created_at][from]": from_ts, "limit": 250}
    if to_ts is not None:
        params_base["filter[created_at][to]"] = to_ts
    if pipeline_id is not None:
        params_base["filter[pipeline_id]"] = pipeline_id

    while True:
        try:
            params = dict(params_base)
            params["page"] = page
            r = _kommo_get("/leads", params)
        except Exception as e:
            logger.error("count_new_leads API error: %s", e)
            if page == 1:
                raise
            break

        if r.status_code != 200:
            logger.warning("count_new_leads API %d: %s", r.status_code, r.text[:200])
            if page == 1:
                raise RuntimeError(f"Kommo API {r.status_code}")
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


def _count_new_leads_today():
    """Leads criados hoje no funil principal (mesmo recorte do widget +NOVO do Kommo)."""
    today = datetime.now(_BRT).date()
    from_ts, to_ts = _day_bounds_brt(today)
    count = _count_new_leads_between(from_ts, to_ts, pipeline_id=FUNNEL_PIPELINE)
    logger.info("count_new_leads_today: %d leads (pipeline=%s)", count, FUNNEL_PIPELINE)
    return count


def _count_leads_day_pg(d: date, pipeline_id: int | None = None) -> int:
    """Leads criados no dia (Postgres kommo_sync — mesmo espelho do sync)."""
    ep_ini, ep_fim = _day_bounds_brt(d)
    conn = None
    try:
        conn = _pg()
        cur = conn.cursor()
        if pipeline_id is not None:
            cur.execute(
                """
                SELECT COUNT(*) FROM leads
                WHERE created_at >= %s AND created_at <= %s AND NOT is_deleted
                  AND pipeline_id = %s
                """,
                (ep_ini, ep_fim, pipeline_id),
            )
        else:
            cur.execute(
                """
                SELECT COUNT(*) FROM leads
                WHERE created_at >= %s AND created_at <= %s AND NOT is_deleted
                """,
                (ep_ini, ep_fim),
            )
        return int(cur.fetchone()[0] or 0)
    except Exception as e:
        logger.warning("count_leads_day_pg %s: %s", d, e)
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _count_new_leads_today_best():
    """Captação do dia: API Kommo (intraday, ~1s); espelho Postgres só se API falhar."""
    today = datetime.now(_BRT).date()
    if _kommo_token():
        try:
            return _count_new_leads_today()
        except Exception as e:
            logger.warning("new_today kommo: %s", e)
    pg_n = _count_leads_day_pg(today, pipeline_id=FUNNEL_PIPELINE)
    logger.info("new_today pg fallback: %d (pipeline=%s)", pg_n, FUNNEL_PIPELINE)
    return pg_n


_new_today_cache = {"count": None, "ts": 0, "source": None}
_NEW_TODAY_CACHE_TTL = 60


def _get_new_leads_today_payload(force=False):
    """KPI leve de novos leads hoje — usado pelo dashboard sem esperar o funil inteiro."""
    now = _time.time()
    if (
        not force
        and _new_today_cache["count"] is not None
        and (now - _new_today_cache["ts"]) < _NEW_TODAY_CACHE_TTL
    ):
        return {
            "count": _new_today_cache["count"],
            "source": _new_today_cache["source"],
            "cached": True,
        }

    source = "kommo"
    try:
        if not _kommo_token():
            source = "db"
            count = _count_leads_day_pg(datetime.now(_BRT).date(), pipeline_id=FUNNEL_PIPELINE)
        else:
            count = _count_new_leads_today_best()
            if count <= 0:
                pg_n = _count_leads_day_pg(datetime.now(_BRT).date(), pipeline_id=FUNNEL_PIPELINE)
                if pg_n > count:
                    count = pg_n
                    source = "db"
    except Exception as e:
        logger.warning("new_leads_today payload: %s", e)
        source = "db"
        count = _count_leads_day_pg(datetime.now(_BRT).date(), pipeline_id=FUNNEL_PIPELINE)

    _new_today_cache["count"] = count
    _new_today_cache["source"] = source
    _new_today_cache["ts"] = now
    return {"count": count, "source": source, "cached": False}


def _vendas_comercial_dia(d: date) -> int:
    """Matrículas EM CURSO no dia — mesma fonte do gráfico do Dash Comercial."""
    try:
        from routes.comercial_rgm import comercial_periodo_vendas_resumo
        d_str = d.isoformat()
        resumo = comercial_periodo_vendas_resumo(dt_ini=d_str, dt_fim=d_str)
        by_day = resumo.get("mat_by_date") or {}
        n = by_day.get(d)
        if n is None:
            n = resumo.get("vendas_liquidas", 0)
        return int(n or 0)
    except Exception as e:
        logger.warning("vendas_comercial_dia %s: %s", d, e)
        return 0


def _ganhos_kommo_dia(d: date) -> int:
    """Fechados ganhos (status 142) por closed_at — fallback se comercial vier vazio."""
    ep_ini, ep_fim = _day_bounds_brt(d)
    conn = None
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM leads
            WHERE status_id = 142 AND NOT is_deleted
              AND closed_at IS NOT NULL AND closed_at > 0
              AND closed_at >= %s AND closed_at <= %s
            """,
            (ep_ini, ep_fim),
        )
        return int(cur.fetchone()[0] or 0)
    except Exception as e:
        logger.warning("ganhos_kommo_dia %s: %s", d, e)
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _count_leads_day_kommo(d: date, pipeline_id: int | None = FUNNEL_PIPELINE) -> int:
    """Leads criados no dia via API Kommo. Default: funil principal. PG só fallback sem filtro."""
    ep_ini, ep_fim = _day_bounds_brt(d)
    try:
        n = _count_new_leads_between(ep_ini, ep_fim, pipeline_id=pipeline_id)
        if n > 0:
            return n
    except Exception as e:
        logger.warning("count_leads_day_kommo %s: %s", d, e)
    if pipeline_id is None:
        return _count_leads_day_pg(d)
    return 0


def _build_yesterday_summary():
    """
    Vendas (EM CURSO) de ontem via comercial_rgm — mesma fonte do gráfico do Dash Comercial.
    Leads criados ontem vs anteontem (API Kommo) para tendência de captação.
    """
    from concurrent.futures import ThreadPoolExecutor

    today = datetime.now(_BRT).date()
    yesterday = today - timedelta(days=1)
    day_before = today - timedelta(days=2)
    y_str = yesterday.isoformat()

    vendas = _vendas_comercial_dia(yesterday)
    if vendas <= 0:
        vendas = _ganhos_kommo_dia(yesterday)

    y_from, y_to = _day_bounds_brt(yesterday)
    p_from, p_to = _day_bounds_brt(day_before)
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_y = pool.submit(_count_new_leads_between, y_from, y_to, FUNNEL_PIPELINE)
        fut_p = pool.submit(_count_new_leads_between, p_from, p_to, FUNNEL_PIPELINE)
        leads = fut_y.result()
        leads_prev = fut_p.result()

    if leads <= 0:
        leads = _count_leads_day_kommo(yesterday)
    if leads_prev <= 0:
        leads_prev = _count_leads_day_kommo(day_before)

    if leads_prev > 0:
        leads_delta_pct = round((leads - leads_prev) / leads_prev * 100, 1)
    elif leads > 0:
        leads_delta_pct = 100.0
    else:
        leads_delta_pct = 0.0

    summary = {
        "date": y_str,
        "vendas": vendas,
        "leads": leads,
        "leads_prev": leads_prev,
        "leads_delta_pct": leads_delta_pct,
    }
    logger.info("yesterday_summary: %s", summary)
    return summary


_yesterday_cache = {"data": None, "ts": 0, "version": 0}
_YESTERDAY_CACHE_TTL = 600  # 10 min — independente do cache do funil
_YESTERDAY_CACHE_VERSION = 2  # v2 = leads só funil principal (644, não 659)


def _yesterday_summary_has_signal(data: dict) -> bool:
    return bool(data.get("vendas") or data.get("leads") or data.get("leads_prev"))


def _get_yesterday_summary_cached(force=False):
    """Cache próprio: respostas cacheadas do funil antigas não traziam yesterday_summary."""
    now = _time.time()
    expected_date = (datetime.now(_BRT).date() - timedelta(days=1)).isoformat()
    cached = _yesterday_cache.get("data")
    if (
        not force
        and cached
        and cached.get("date") == expected_date
        and cached.get("_cache_v") == _YESTERDAY_CACHE_VERSION
        and _yesterday_summary_has_signal(cached)
        and (now - _yesterday_cache["ts"]) < _YESTERDAY_CACHE_TTL
    ):
        return cached
    data = _build_yesterday_summary()
    data["_cache_v"] = _YESTERDAY_CACHE_VERSION
    if _yesterday_summary_has_signal(data):
        _yesterday_cache["data"] = data
        _yesterday_cache["ts"] = now
        _yesterday_cache["version"] = _YESTERDAY_CACHE_VERSION
    return data


def _get_yesterday_summary_light():
    """Resumo de ontem sem bater na API Kommo — evita estourar timeout do proxy no funil."""
    expected_date = (datetime.now(_BRT).date() - timedelta(days=1)).isoformat()
    cached = _yesterday_cache.get("data")
    if (
        cached
        and cached.get("date") == expected_date
        and _yesterday_summary_has_signal(cached)
    ):
        return cached
    yesterday = datetime.now(_BRT).date() - timedelta(days=1)
    return {
        "date": yesterday.isoformat(),
        "vendas": _vendas_comercial_dia(yesterday),
        "leads": 0,
        "leads_prev": 0,
        "leads_delta_pct": 0,
    }


def _count_leads_in_stage(status_id: int) -> tuple[int, str | None]:
    """Conta leads em uma fila — paginação limit=250 (API Kommo não expõe _total_items em /leads)."""
    count = 0
    page = 1
    rate_retries = 0
    while True:
        try:
            r = _kommo_get("/leads", {
                "limit": 250,
                "page": page,
                "filter[statuses][0][pipeline_id]": FUNNEL_PIPELINE,
                "filter[statuses][0][status_id]": status_id,
            })
        except Exception as e:
            return (count, str(e)) if page > 1 else (0, str(e))
        if r.status_code == 204:
            break
        if r.status_code == 429:
            rate_retries += 1
            if rate_retries > 8:
                return count, "Kommo API 429: rate limit"
            _time.sleep(0.5 * rate_retries)
            continue
        rate_retries = 0
        if r.status_code != 200:
            return (count, f"Kommo API {r.status_code}: {r.text[:200]}") if page > 1 else (
                0, f"Kommo API {r.status_code}: {r.text[:200]}"
            )
        data = r.json()
        leads = data.get("_embedded", {}).get("leads", [])
        if not leads:
            break
        count += len(leads)
        if "next" not in data.get("_links", {}):
            break
        page += 1
        _time.sleep(0.08)
    return count, None


def _fetch_funnel_live_counts():
    """Contagem ao vivo por fila — sequencial (evita 429 no Easypanel)."""
    counts = {}
    for sdef in FUNNEL_STAGES_DEF:
        n, err = _count_leads_in_stage(sdef["id"])
        if err:
            return None, err
        counts[sdef["id"]] = n
        _time.sleep(0.12)

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
            "highlight": sdef["key"] in FUNNEL_HIGHLIGHT,
        })

    for s in stages:
        s["pct"] = round(s["count"] / total * 100, 1) if total > 0 else 0

    out = {
        "stages": stages,
        "total": total,
        "leads_fetched": total,
        "pages": 0,
        "source": "live",
        "funnel_api_version": 5,
    }
    return out, None


def _fetch_funnel_live_parallel():
    """Alias — mantido para compat interna; usa contagem sequencial."""
    result, err = _fetch_funnel_live_counts()
    if err:
        return {"stages": [], "total": 0, "source": "live", "api_error": err}
    return result


def _fetch_funnel_live():
    """Fetch all leads in the funnel pipeline from Kommo API v4, count by status."""
    stage_ids = [s["id"] for s in FUNNEL_STAGES_DEF]
    all_leads = []
    seen_ids = set()
    page = 1
    api_error = None

    while True:
        params = {"limit": 250, "page": page}
        for i, sid in enumerate(stage_ids):
            params[f"filter[statuses][{i}][pipeline_id]"] = FUNNEL_PIPELINE
            params[f"filter[statuses][{i}][status_id]"] = sid

        try:
            r = _kommo_get("/leads", params)
        except Exception as e:
            logger.error("Kommo API error: %s", e)
            api_error = str(e)
            break

        if r.status_code != 200:
            api_error = f"Kommo API {r.status_code}: {r.text[:200]}"
            logger.warning("Kommo funnel %s", api_error)
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
        sid = lead.get("status_id")
        counts[sid] = counts.get(sid, 0) + 1

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
            "highlight": sdef["key"] in FUNNEL_HIGHLIGHT,
        })

    for s in stages:
        s["pct"] = round(s["count"] / total * 100, 1) if total > 0 else 0

    out = {
        "stages": stages,
        "total": total,
        "leads_fetched": len(all_leads),
        "pages": page,
    }
    if api_error:
        out["api_error"] = api_error
    return out


def _fetch_funnel_from_db():
    """Contagem por etapa a partir do espelho Postgres (kommo_sync) — rápido, sem timeout."""
    stage_ids = [s["id"] for s in FUNNEL_STAGES_DEF]
    conn = _pg()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT status_id, COUNT(*) AS total
                FROM leads
                WHERE pipeline_id = %s AND NOT is_deleted
                  AND status_id = ANY(%s)
                GROUP BY status_id
                """,
                (FUNNEL_PIPELINE, stage_ids),
            )
            counts = {int(r["status_id"]): int(r["total"]) for r in cur.fetchall()}
            last_sync = None
            try:
                cur.execute(
                    """
                    SELECT MAX(NULLIF(trim(synced_at), '')::timestamptz) AS last_sync
                    FROM leads
                    WHERE pipeline_id = %s AND NOT is_deleted
                    """,
                    (FUNNEL_PIPELINE,),
                )
                last_sync = cur.fetchone().get("last_sync")
            except Exception as e:
                logger.warning("funnel db last_sync: %s", e)
    finally:
        conn.close()

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
            "highlight": sdef["key"] in FUNNEL_HIGHLIGHT,
        })

    for s in stages:
        s["pct"] = round(s["count"] / total * 100, 1) if total > 0 else 0

    out = {
        "stages": stages,
        "total": total,
        "leads_fetched": total,
        "pages": 0,
        "source": "db",
    }
    if last_sync:
        if hasattr(last_sync, "astimezone"):
            last_sync = last_sync.astimezone(_BRT)
        out["synced_at"] = last_sync.strftime("%d/%m %H:%M")
    return out


def _enrich_funnel_result(result, new_today=None, yesterday_summary=None, live_error=None):
    """Aplica D0, yesterday deltas e metadados comuns ao payload do funil."""
    if new_today is not None:
        result["new_today"] = new_today
    if yesterday_summary is not None:
        result["yesterday_summary"] = yesterday_summary
    if live_error:
        result["live_error"] = live_error

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

    now_brt = datetime.now(_BRT)
    result["d0_date"] = now_brt.date().isoformat()
    if result.get("source") != "db":
        result["fetched_at"] = now_brt.strftime("%H:%M:%S")
    return result


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

    current_total = sum(s["count"] for s in current_stages)
    existing = snapshots.get(today)
    needs_create = existing is None
    if existing and existing.get("_total", 0) < 100 and current_total > 100:
        logger.warning("D0 snapshot for %s looks invalid (_total=%s), recreating", today, existing.get("_total"))
        needs_create = True

    if needs_create:
        snapshots[today] = {s["key"]: s["count"] for s in current_stages}
        snapshots[today]["_total"] = current_total
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


# ── Reconciliação de leads em Aceite ──────────────────────────────────────

_reconcile_lock = threading.Lock()
_last_reconcile_ts = 0
RECONCILE_COOLDOWN = 120  # seconds

def reconcile_aceite_leads():
    """Sync aceite leads between Kommo API and our DB.
    - Upserts leads currently in aceite (updates status_id for existing, inserts missing)
    - Marks stale leads (no longer in aceite on Kommo) as is_deleted=True
    Safety: aborts without changes if API returns errors with zero leads."""
    global _last_reconcile_ts
    now = _time.time()
    if now - _last_reconcile_ts < RECONCILE_COOLDOWN:
        return {"skipped": True, "reason": "cooldown"}
    if not _reconcile_lock.acquire(blocking=False):
        return {"skipped": True, "reason": "already running"}
    try:
        _last_reconcile_ts = now
        if not KOMMO_TOKEN:
            return {"error": "KOMMO_TOKEN not set"}

        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT id, pipeline_id FROM pipeline_statuses WHERE LOWER(name) LIKE '%aceite%'")
        aceite_statuses = cur.fetchall()
        if not aceite_statuses:
            cur.close(); conn.close()
            return {"error": "no aceite statuses found"}

        ace_ids = [r[0] for r in aceite_statuses]
        ace_ph = ",".join(["%s"] * len(ace_ids))

        cur.execute(
            f"SELECT id FROM leads WHERE status_id IN ({ace_ph}) AND NOT is_deleted",
            ace_ids,
        )
        db_aceite_ids = {r[0] for r in cur.fetchall()}
        logger.info("Reconcile aceites: %d active leads in DB with aceite status", len(db_aceite_ids))

        api_leads = []
        api_lead_ids = set()
        api_error = False
        for status_id, pipeline_id in aceite_statuses:
            page = 1
            while True:
                params = {
                    "filter[statuses][0][pipeline_id]": pipeline_id,
                    "filter[statuses][0][status_id]": status_id,
                    "limit": 250,
                    "page": page,
                }
                try:
                    r = _kommo_get("/leads", params)
                except Exception as e:
                    logger.error("Reconcile API error: %s", e)
                    api_error = True
                    break
                if r.status_code == 204:
                    break
                if r.status_code != 200:
                    logger.warning("Reconcile API %d: %s", r.status_code, r.text[:200])
                    api_error = True
                    break
                data = r.json()
                leads = data.get("_embedded", {}).get("leads", [])
                if not leads:
                    break
                for ld in leads:
                    api_lead_ids.add(ld["id"])
                    api_leads.append(ld)
                if "next" not in data.get("_links", {}):
                    break
                page += 1
                _time.sleep(0.05)

        if api_error and not api_lead_ids:
            logger.warning("Reconcile aceites: API returned errors and zero leads — aborting to prevent data loss")
            cur.close(); conn.close()
            return {"aborted": True, "reason": "API error with zero leads, refusing to mark all as deleted"}

        if not api_lead_ids and db_aceite_ids:
            logger.warning("Reconcile aceites: API returned 0 leads but DB has %d — aborting (likely API issue)", len(db_aceite_ids))
            cur.close(); conn.close()
            return {"aborted": True, "reason": f"API returned 0 leads but DB has {len(db_aceite_ids)} — refusing to delete all"}

        upserted = 0
        inserted = 0
        for ld in api_leads:
            lid = ld["id"]
            cur.execute("SELECT id FROM leads WHERE id = %s", (lid,))
            row = cur.fetchone()
            if row:
                cur.execute(
                    "UPDATE leads SET status_id = %s, pipeline_id = %s, responsible_user_id = %s, "
                    "updated_at = %s, is_deleted = false WHERE id = %s",
                    (ld.get("status_id"), ld.get("pipeline_id"), ld.get("responsible_user_id"),
                     ld.get("updated_at"), lid),
                )
                upserted += 1
            else:
                cur.execute(
                    "INSERT INTO leads (id, name, status_id, pipeline_id, responsible_user_id, "
                    "created_at, updated_at, is_deleted) VALUES (%s,%s,%s,%s,%s,%s,%s,false)",
                    (lid, ld.get("name", ""), ld.get("status_id"), ld.get("pipeline_id"),
                     ld.get("responsible_user_id"), ld.get("created_at"), ld.get("updated_at")),
                )
                inserted += 1

        # "Stale": leads que estavam em Aceite no DB mas nao retornaram no filtro de Aceite da API.
        # NAO marcar como deletado cegamente — o lead pode ter mudado de etapa (Ganho, Em Atendimento, etc).
        # Faz GET individual: se vivo, atualiza status real e mantem is_deleted=FALSE; se 404/204, marca deletado.
        stale_ids = db_aceite_ids - api_lead_ids
        stale_deleted = 0
        stale_status_changed = 0
        stale_check_errors = 0
        STALE_BATCH_LIMIT = 500  # protecao: se passar disso, algo absurdo aconteceu, aborta o stale check

        if stale_ids:
            if len(stale_ids) > STALE_BATCH_LIMIT:
                logger.warning(
                    "Reconcile aceites: %d stale leads ultrapassam limite de %d — pulando stale check (anomalia)",
                    len(stale_ids), STALE_BATCH_LIMIT,
                )
            else:
                logger.info("Reconcile aceites: verificando %d leads stale individualmente", len(stale_ids))
                for lid in stale_ids:
                    try:
                        r = _kommo_get(f"/leads/{lid}")
                    except Exception as e:
                        stale_check_errors += 1
                        logger.warning("Reconcile aceites: erro GET lead %s: %s", lid, e)
                        continue

                    if r.status_code in (204, 404):
                        cur.execute(
                            "UPDATE leads SET is_deleted = true WHERE id = %s",
                            (lid,),
                        )
                        stale_deleted += 1
                    elif r.status_code == 200:
                        try:
                            ld = r.json() or {}
                        except Exception:
                            stale_check_errors += 1
                            continue
                        cur.execute(
                            "UPDATE leads SET status_id = %s, pipeline_id = %s, "
                            "responsible_user_id = %s, updated_at = %s, is_deleted = false "
                            "WHERE id = %s",
                            (
                                ld.get("status_id"),
                                ld.get("pipeline_id"),
                                ld.get("responsible_user_id"),
                                ld.get("updated_at"),
                                lid,
                            ),
                        )
                        stale_status_changed += 1
                    else:
                        stale_check_errors += 1
                        logger.warning("Reconcile aceites: lead %s status %d", lid, r.status_code)

                    _time.sleep(0.05)

                logger.info(
                    "Reconcile aceites stale: %d com status atualizado, %d marcados deletados, %d erros",
                    stale_status_changed, stale_deleted, stale_check_errors,
                )

        conn.commit()
        logger.info("Reconcile aceites: api=%d, upserted=%d, inserted=%d, stale_changed=%d, stale_deleted=%d",
                     len(api_lead_ids), upserted, inserted, stale_status_changed, stale_deleted)

        cur.close()
        conn.close()
        return {
            "db_aceites_before": len(db_aceite_ids),
            "api_aceites": len(api_lead_ids),
            "status_updated": upserted,
            "new_inserted": inserted,
            "stale_status_changed": stale_status_changed,
            "stale_marked_deleted": stale_deleted,
            "stale_check_errors": stale_check_errors,
        }
    except Exception as e:
        logger.error("Reconcile aceites error: %s", e)
        return {"error": str(e)}
    finally:
        _reconcile_lock.release()


@kommo_bp.route("/api/kommo/reconcile-aceites", methods=["POST"])
def api_kommo_reconcile_aceites():
    result = reconcile_aceite_leads()
    return jsonify({"ok": True, "data": result})


@kommo_bp.route("/api/kommo/new-leads-today")
def api_kommo_new_leads_today():
    """KPI intraday de novos leads — endpoint leve (~1s), independente do funil."""
    force = request.args.get("force", "0") == "1"
    try:
        data = _get_new_leads_today_payload(force=force)
        return jsonify({"ok": True, **data})
    except Exception as e:
        logger.exception("new-leads-today: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@kommo_bp.route("/api/kommo/yesterday-summary")
def api_kommo_yesterday_summary():
    """Resumo de ontem (vendas + leads) — endpoint leve, sem buscar funil inteiro."""
    force = request.args.get("force", "0") == "1"
    try:
        data = _get_yesterday_summary_cached(force=force)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        logger.exception("yesterday-summary: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@kommo_bp.route("/api/kommo/funnel-live")
def api_kommo_funnel_live():
    """Funil: conta leads por fila na API Kommo (somente ao vivo, sem espelho PG)."""
    force = request.args.get("force", "0") == "1"
    now = _time.time()

    cached = _funnel_cache.get("data")
    if (
        not force
        and cached
        and cached.get("source") == "live"
        and cached.get("funnel_api_version", 0) >= 5
        and (now - _funnel_cache["ts"]) < _FUNNEL_CACHE_TTL
    ):
        data = dict(cached)
        try:
            nt = _get_new_leads_today_payload(force=force)
            data["new_today"] = nt["count"]
            data["new_today_source"] = nt.get("source")
        except Exception as e:
            logger.warning("new_today cache refresh: %s", e)
        data["yesterday_summary"] = _get_yesterday_summary_light()
        return jsonify({"ok": True, "data": data, "cached": True})

    def _finalize(
        result,
        live_error=None,
        new_today=None,
        yesterday_summary=None,
        heavy_yesterday=False,
    ):
        if new_today is None:
            try:
                nt = _get_new_leads_today_payload(force=force)
                new_today = nt["count"]
                result["new_today_source"] = nt.get("source")
            except Exception as e:
                logger.warning("new_today: %s", e)
                new_today = 0
        if yesterday_summary is None:
            if heavy_yesterday:
                try:
                    yesterday_summary = _get_yesterday_summary_cached(force=force)
                except Exception as e:
                    logger.exception("yesterday_summary failed: %s", e)
                    yesterday_summary = _get_yesterday_summary_light()
            else:
                yesterday_summary = _get_yesterday_summary_light()
        result = _enrich_funnel_result(
            result,
            new_today=new_today,
            yesterday_summary=yesterday_summary,
            live_error=live_error,
        )
        if result.get("source") == "live":
            _funnel_cache["data"] = result
            _funnel_cache["ts"] = now
        return jsonify({"ok": True, "data": result})

    def _normalize_live_error(live_error: str) -> str:
        err = live_error or ""
        if "401" in err:
            return "Token Kommo inválido ou expirado (401)."
        if "403" in err:
            return "Kommo bloqueou a requisição (403 WAF)."
        if "429" in err:
            return "Kommo limitou requisições (429). Tente Atualizar em 1 min."
        return err or "Falha ao buscar funil no Kommo."

    def _live_path():
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_funnel = pool.submit(_fetch_funnel_live_counts)
            fut_new = pool.submit(_get_new_leads_today_payload, force)
            new_today_pre = None
            try:
                nt = fut_new.result(timeout=15)
                new_today_pre = nt["count"]
            except Exception as e:
                logger.warning("new_today parallel: %s", e)
            try:
                result, err = fut_funnel.result(timeout=120)
            except FutTimeout as e:
                raise RuntimeError("Kommo excedeu 120s ao contar filas") from e
        if err:
            raise RuntimeError(err)
        return _finalize(
            result,
            new_today=new_today_pre,
            heavy_yesterday=force,
        )

    if not _kommo_token():
        return jsonify({"ok": False, "error": "KOMMO_TOKEN não configurado no servidor."}), 500

    try:
        return _live_path()
    except Exception as e:
        logger.warning("funnel live failed: %s", e)
        return jsonify({"ok": False, "error": _normalize_live_error(str(e))}), 502


# ---------------------------------------------------------------------------
# Histórico de responsável — lead_responsible_history
# ---------------------------------------------------------------------------
# Sincroniza eventos do tipo "lead_responsible_changed" da API Kommo e salva
# em lead_responsible_history.  Chamada pelo scheduler diário e pelo backfill
# manual via endpoint /api/kommo/sync-responsible-history.
# ---------------------------------------------------------------------------

_RESPONSIBLE_SYNC_META_KEY = "lead_responsible_history"


def _sync_responsible_history(days_back: int = 1) -> dict:
    """Busca eventos 'lead_responsible_changed' da API Kommo e persiste em
    lead_responsible_history.

    Args:
        days_back: quantos dias para trás buscar (1 = incremental, 90 = backfill).

    Returns:
        dict com campos inserted, skipped, pages, errors.
    """
    if not KOMMO_TOKEN:
        return {"error": "KOMMO_TOKEN not set"}

    BRT = timezone(timedelta(hours=-3))
    from_ts = int((datetime.now(BRT) - timedelta(days=days_back)).timestamp())

    inserted = 0
    skipped  = 0
    pages    = 0
    errors   = []

    conn = _pg()
    cur  = conn.cursor()

    page = 1
    while True:
        try:
            # Sem filter[entity]: no Kommo, filter[entity]=lead costuma zerar o resultado;
            # o backfill (run_backfill.py) só usa type + created_at e filtra lead no Python.
            resp = _kommo_get("/events", {
                "filter[type]":             "entity_responsible_changed",
                "filter[created_at][from]": from_ts,
                "limit":                    250,
                "page":                     page,
            })
        except Exception as exc:
            errors.append(f"page {page}: {exc}")
            break

        if resp.status_code == 204 or resp.status_code == 404:
            break
        if resp.status_code != 200:
            errors.append(f"page {page}: HTTP {resp.status_code}")
            break

        try:
            data = resp.json()
        except Exception as exc:
            errors.append(f"page {page}: json parse error {exc}")
            break

        items = (data.get("_embedded") or {}).get("events") or []
        if not items:
            break

        pages += 1
        for ev in items:
            try:
                entity_id   = ev.get("entity_id")
                entity_type = ev.get("entity_type", "")
                # Só processa eventos de lead (filtra contatos que podem vir junto)
                if str(entity_type).lower() not in ("lead", "leads"):
                    skipped += 1
                    continue

                created_at_ts = ev.get("created_at")
                if not created_at_ts:
                    skipped += 1
                    continue
                changed_at = datetime.fromtimestamp(created_at_ts, tz=timezone.utc)

                # value_after / value_before: lista com dict {responsible_user: {id: X}}
                value_after  = ev.get("value_after")  or []
                value_before = ev.get("value_before") or []
                if isinstance(value_after,  list): value_after  = value_after[0]  if value_after  else {}
                if isinstance(value_before, list): value_before = value_before[0] if value_before else {}

                to_user_id   = (value_after.get("responsible_user")  or {}).get("id") if isinstance(value_after,  dict) else None
                from_user_id = (value_before.get("responsible_user") or {}).get("id") if isinstance(value_before, dict) else None

                # Ignora quando to_user_id = 0 (sem responsável definido)
                if not to_user_id or to_user_id == 0 or not entity_id:
                    skipped += 1
                    continue

                cur.execute("""
                    INSERT INTO lead_responsible_history (lead_id, changed_at, from_user_id, to_user_id, source)
                    VALUES (%s, %s, %s, %s, 'kommo_events')
                    ON CONFLICT (lead_id, changed_at) DO NOTHING
                """, (entity_id, changed_at, from_user_id, to_user_id))

                if cur.rowcount:
                    inserted += 1
                else:
                    skipped += 1

            except Exception as exc:
                errors.append(f"event parse: {exc}")
                skipped += 1
                continue

        conn.commit()

        # Verifica se há próxima página
        links = data.get("_links") or {}
        if not links.get("next"):
            break
        page += 1

    # Atualiza metadado de última sync
    try:
        cur.execute("""
            INSERT INTO sync_metadata (entity_type, last_sync_at, records_synced, status)
            VALUES (%s, NOW(), %s, 'ok')
            ON CONFLICT (entity_type) DO UPDATE
               SET last_sync_at   = EXCLUDED.last_sync_at,
                   records_synced = EXCLUDED.records_synced,
                   status         = EXCLUDED.status
        """, (_RESPONSIBLE_SYNC_META_KEY, inserted))
        conn.commit()
    except Exception:
        pass

    cur.close()
    conn.close()

    result = {"inserted": inserted, "skipped": skipped, "pages": pages, "errors": errors[:10]}
    logger.info("responsible_history sync: %s", result)
    return result


def run_responsible_history_daily():
    """Job diário chamado pelo scheduler: puxa últimas 2 horas com margem."""
    try:
        # 1.5 dias de margem para não perder nada no caso de atraso do scheduler
        result = _sync_responsible_history(days_back=2)
        logger.info("responsible_history daily job: %s", result)
    except Exception as exc:
        logger.exception("responsible_history daily job failed: %s", exc)


@kommo_bp.route("/api/kommo/sync-responsible-history")
def api_sync_responsible_history():
    """Endpoint manual: dispara backfill ou sync incremental.

    Query params:
        days_back (int, default 1): quantos dias para trás buscar.
    """
    try:
        days_back = int(request.args.get("days_back", 1))
    except ValueError:
        days_back = 1

    days_back = max(1, min(days_back, 365))
    result = _sync_responsible_history(days_back=days_back)
    return jsonify({"ok": True, **result})
