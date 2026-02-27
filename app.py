"""
DataCrazy CRM Sync — Interface Web (Flask).

Uso:
    python app.py
    Acesse http://localhost:5001
"""

import os
import sys
import json
import subprocess
import threading
import time
import re
from datetime import datetime, timezone, timedelta

BRT = timezone(timedelta(hours=-3))
from pathlib import Path
from collections import deque

from flask import (
    Flask, render_template, request, jsonify,
    redirect, url_for, session, send_file,
)
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")


def to_brt(dt):
    """Convert a datetime to BRT (UTC-3) string."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BRT).strftime("%d/%m/%Y %H:%M:%S")
    return str(dt)


app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dcz-sync-default-key-change-me")

# ---------------------------------------------------------------------------
# Autenticação por sessão
# ---------------------------------------------------------------------------

APP_USER = os.getenv("APP_USER", "admin")
APP_PASS = os.getenv("APP_PASS", "")


@app.before_request
def require_auth():
    if not APP_PASS:
        return
    if request.path in ("/login",):
        return
    if request.path.startswith("/static/"):
        return
    if not session.get("authenticated"):
        if request.path.startswith("/api/"):
            return jsonify({"error": "Não autenticado"}), 401
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = request.form.get("username", "")
        pwd = request.form.get("password", "")
        if user == APP_USER and pwd == APP_PASS:
            session["authenticated"] = True
            return redirect(url_for("index"))
        app.logger.warning("Login falhou: user=%r (esperado %r), pass_len=%d (esperado %d)",
                           user, APP_USER, len(pwd), len(APP_PASS))
        error = "Usuário ou senha incorretos."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)

BASE_DIR = Path(__file__).parent
SYNC_SCRIPT = str(BASE_DIR / "sync.py")
UPDATE_SCRIPT = str(BASE_DIR / "update_crm.py")
SANITIZE_SCRIPT = str(BASE_DIR / "sanitize_crm.py")
PIPELINE_SCRIPT = str(BASE_DIR / "pipeline_crm.py")
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

MAX_LOG_LINES = 2000

# ---------------------------------------------------------------------------
# Estado global
# ---------------------------------------------------------------------------

_sync_running = False
_sync_proc = None
_sync_logs: deque = deque(maxlen=MAX_LOG_LINES)

_update_running = False
_update_proc = None
_update_logs: deque = deque(maxlen=MAX_LOG_LINES)

_sanitize_running = False
_sanitize_proc = None
_sanitize_logs: deque = deque(maxlen=MAX_LOG_LINES)

_pipeline_running = False
_pipeline_proc = None
_pipeline_logs: deque = deque(maxlen=MAX_LOG_LINES)


def _add_sync_log(line: str):
    _sync_logs.append(line.rstrip())


def _add_update_log(line: str):
    _update_logs.append(line.rstrip())


def _add_sanitize_log(line: str):
    _sanitize_logs.append(line.rstrip())


def _add_pipeline_log(line: str):
    _pipeline_logs.append(line.rstrip())

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_DSN)


FIELD_RGM = "2ac4e30f-cfd7-435f-b688-fbce27f76c38"

SEARCH_QUERY = """
SELECT
    l.id                                  AS lead_id,
    l.data->>'name'                       AS lead_nome,
    l.data->>'phone'                      AS lead_telefone,
    l.data->>'rawPhone'                   AS lead_telefone_raw,
    l.data->>'email'                      AS lead_email,
    l.data->>'source'                     AS lead_origem,
    l.data->>'taxId'                      AS lead_cpf,
    l.data->'address'->>'city'            AS lead_cidade,
    l.data->'address'->>'state'           AS lead_estado,
    l.data->>'createdAt'                  AS lead_criado_em,

    b.id                                  AS negocio_id,
    b.data->>'code'                       AS negocio_codigo,
    b.data->>'status'                     AS negocio_status,
    b.data->>'total'                      AS negocio_valor,
    b.data->>'createdAt'                  AS negocio_criado_em,
    b.data->>'lastMovedAt'                AS negocio_movido_em,

    p.data->>'name'                       AS pipeline_nome,
    ps.data->>'name'                      AS etapa_nome,
    ps.data->>'color'                     AS etapa_cor,

    b.data->'attendant'->>'name'          AS atendente,

    biz_cf.campos                         AS campos_negocio,
    lead_cf.campos                        AS campos_lead

FROM businesses b
LEFT JOIN leads l            ON l.id  = b.data->>'leadId'
LEFT JOIN pipeline_stages ps ON ps.id = b.data->>'stageId'
LEFT JOIN pipelines p        ON p.id  = ps.pipeline_id
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(
        elem->'additionalField'->>'name',
        COALESCE(elem->>'value', '')
    ) AS campos
    FROM jsonb_array_elements(b.data->'additionalFields') elem
    WHERE elem->'additionalField'->>'name' IS NOT NULL
) biz_cf ON true
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(
        elem->'additionalField'->>'name',
        COALESCE(elem->>'value', '')
    ) AS campos
    FROM jsonb_array_elements(l.data->'additionalFields') elem
    WHERE elem->'additionalField'->>'name' IS NOT NULL
) lead_cf ON true
WHERE (
    (%(cpf)s != '' AND REPLACE(REPLACE(l.data->>'taxId', '.', ''), '-', '') LIKE '%%' || REPLACE(REPLACE(%(cpf)s, '.', ''), '-', '') || '%%')
    OR (%(rgm)s != '' AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(b.data->'additionalFields') e
        WHERE e->'additionalField'->>'id' = '2ac4e30f-cfd7-435f-b688-fbce27f76c38'
          AND e->>'value' LIKE '%%' || %(rgm)s || '%%'
    ))
    OR (%(telefone)s != '' AND (
        l.data->>'rawPhone' LIKE '%%' || %(telefone)s || '%%'
        OR REPLACE(REPLACE(REPLACE(REPLACE(l.data->>'phone', ' ', ''), '(', ''), ')', ''), '-', '') LIKE '%%' || %(telefone)s || '%%'
    ))
)
ORDER BY b.data->>'lastMovedAt' DESC NULLS LAST
LIMIT 50;
"""

RECENT_BIZ_UPDATES_QUERY = """
SELECT
    'negocio' AS tipo,
    b.id,
    b.data->'lead'->>'name' AS nome_lead,
    b.data->>'status' AS status,
    p.data->>'name' AS pipeline,
    ps.data->>'name' AS etapa,
    b.synced_at
FROM businesses b
LEFT JOIN pipeline_stages ps ON ps.id = b.data->>'stageId'
LEFT JOIN pipelines p ON p.id = ps.pipeline_id
WHERE b.synced_at = (SELECT MAX(synced_at) FROM businesses)
ORDER BY b.synced_at DESC
LIMIT 10;
"""

SYNC_STATE_QUERY = """
SELECT entity_type, last_sync_at, last_full_sync_at, run_count
FROM sync_state ORDER BY entity_type;
"""

# ---------------------------------------------------------------------------
# Rotas — Páginas
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    resp = app.make_response(render_template("index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


# ---------------------------------------------------------------------------
# Rotas — Dashboard
# ---------------------------------------------------------------------------

@app.route("/api/dashboard")
def api_dashboard():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS total FROM leads")
            total_leads = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) AS total FROM businesses")
            total_biz = cur.fetchone()["total"]

            cur.execute(SYNC_STATE_QUERY)
            states = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                states.append(row)

            cur.execute(RECENT_BIZ_UPDATES_QUERY)
            recent = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                recent.append(row)

            cur.execute("SELECT COUNT(*) AS total FROM pipelines")
            total_pipelines = cur.fetchone()["total"]

            # Schedules
            try:
                cur.execute("SELECT * FROM schedules ORDER BY created_at")
                schedules = [dict(r) for r in cur.fetchall()]
                for s in schedules:
                    for k, v in s.items():
                        if isinstance(v, datetime):
                            s[k] = to_brt(v)
            except Exception:
                schedules = []

        return jsonify({
            "total_leads": total_leads,
            "total_businesses": total_biz,
            "total_pipelines": total_pipelines,
            "sync_states": states,
            "recent_updates": recent,
            "schedules": schedules,
            "sync_running": _sync_running,
            "update_running": _update_running,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Busca
# ---------------------------------------------------------------------------

@app.route("/api/search")
def api_search():
    cpf = request.args.get("cpf", "").strip()
    rgm = request.args.get("rgm", "").strip()
    telefone = request.args.get("telefone", "").strip()

    if not cpf and not rgm and not telefone:
        return jsonify({"results": [], "error": "Informe pelo menos um critério de busca."})

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SEARCH_QUERY, {"cpf": cpf, "rgm": rgm, "telefone": telefone})
            rows = cur.fetchall()
            results = []
            for r in rows:
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                results.append(row)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"results": [], "error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/sync-state")
def api_sync_state():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SYNC_STATE_QUERY)
            states = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                states.append(row)

            cur.execute(RECENT_BIZ_UPDATES_QUERY)
            recent = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                recent.append(row)

        return jsonify({"states": states, "recent_updates": recent})
    except Exception as e:
        return jsonify({"states": [], "recent_updates": [], "error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Sync
# ---------------------------------------------------------------------------

@app.route("/api/sync/<mode>", methods=["POST"])
def api_sync(mode):
    global _sync_running

    if mode not in ("delta", "full"):
        return jsonify({"error": "Modo inválido. Use 'delta' ou 'full'."}), 400

    if _sync_running:
        return jsonify({"error": "Sincronização já em andamento."}), 409

    _sync_running = True
    _sync_logs.clear()

    def run():
        global _sync_running, _sync_proc
        try:
            cmd = [sys.executable, SYNC_SCRIPT]
            if mode == "full":
                cmd.append("--full")

            _add_sync_log(f"[INÍCIO] Sincronização {mode.upper()} iniciada")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(BASE_DIR),
                env=env,
            )
            _sync_proc = proc

            for line in proc.stdout:
                _add_sync_log(line)

            proc.wait()

            if proc.returncode == 0:
                _add_sync_log("[FIM] Sincronização concluída com sucesso")
            elif proc.returncode < 0:
                _add_sync_log("[PARADO] Sincronização interrompida.")
            else:
                _add_sync_log(f"[ERRO] Sincronização falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_sync_log(f"[ERRO] {e}")
            _add_sync_log(traceback.format_exc())
        finally:
            _sync_proc = None
            _sync_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/sync/logs")
def api_sync_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_sync_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _sync_running})


@app.route("/api/sync/status")
def api_sync_status():
    global _sync_running
    if _sync_running and (_sync_proc is None or _sync_proc.poll() is not None):
        _sync_running = False
    return jsonify({"running": _sync_running})


@app.route("/api/sync/stop", methods=["POST"])
def api_sync_stop():
    global _sync_running
    if _sync_proc is not None:
        try:
            _sync_proc.terminate()
        except Exception:
            pass
    _sync_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Update CRM
# ---------------------------------------------------------------------------

@app.route("/api/update/<mode>", methods=["POST"])
def api_update(mode):
    global _update_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _update_running:
        return jsonify({"error": "Atualização já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")
    rate = body.get("rate")

    _update_running = True
    _update_logs.clear()

    def run():
        global _update_running, _update_proc
        try:
            cmd = [sys.executable, UPDATE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])
            if rate is not None:
                cmd.extend(["--rate", str(int(rate))])

            _add_update_log(f"[INÍCIO] Update CRM — modo {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(BASE_DIR),
                env=env,
            )
            _update_proc = proc

            for line in proc.stdout:
                _add_update_log(line)

            proc.wait()

            if proc.returncode == 0:
                _add_update_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_update_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_update_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_update_log(f"[ERRO] {e}")
            _add_update_log(traceback.format_exc())
        finally:
            _update_proc = None
            _update_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/update/logs")
def api_update_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_update_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _update_running})


@app.route("/api/update/status")
def api_update_status():
    global _update_running
    if _update_running and (_update_proc is None or _update_proc.poll() is not None):
        _update_running = False
    return jsonify({"running": _update_running})


@app.route("/api/update/stop", methods=["POST"])
def api_update_stop():
    global _update_running
    if _update_proc is not None:
        try:
            _update_proc.terminate()
        except Exception:
            pass
    _update_running = False
    return jsonify({"ok": True})


@app.route("/api/update/preview")
def api_update_preview():
    preview_path = REPORTS_DIR / "update_preview.csv"
    if not preview_path.exists():
        return jsonify({"rows": [], "error": "Rode dry-run primeiro para gerar o preview."})

    import csv as csv_mod
    rows = []
    with open(preview_path, "r", encoding="utf-8-sig") as f:
        reader = csv_mod.DictReader(f, delimiter=";")
        for i, row in enumerate(reader):
            if i >= 500:
                break
            rows.append(dict(row))
    return jsonify({"rows": rows, "total": len(rows)})


# ---------------------------------------------------------------------------
# Rotas — Saneamento
# ---------------------------------------------------------------------------

@app.route("/api/sanitize/<mode>", methods=["POST"])
def api_sanitize(mode):
    global _sanitize_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _sanitize_running:
        return jsonify({"error": "Saneamento já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")

    _sanitize_running = True
    _sanitize_logs.clear()

    def run():
        global _sanitize_running, _sanitize_proc
        try:
            cmd = [sys.executable, SANITIZE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])

            _add_sanitize_log(f"[INÍCIO] Saneamento — modo {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
            )
            _sanitize_proc = proc
            for line in proc.stdout:
                _add_sanitize_log(line)
            proc.wait()

            if proc.returncode == 0:
                _add_sanitize_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_sanitize_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_sanitize_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_sanitize_log(f"[ERRO] {e}")
            _add_sanitize_log(traceback.format_exc())
        finally:
            _sanitize_proc = None
            _sanitize_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/sanitize/logs")
def api_sanitize_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_sanitize_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _sanitize_running})


@app.route("/api/sanitize/status")
def api_sanitize_status():
    global _sanitize_running
    if _sanitize_running and (_sanitize_proc is None or _sanitize_proc.poll() is not None):
        _sanitize_running = False
    return jsonify({"running": _sanitize_running})


@app.route("/api/sanitize/stop", methods=["POST"])
def api_sanitize_stop():
    global _sanitize_running
    if _sanitize_proc is not None:
        try:
            _sanitize_proc.terminate()
        except Exception:
            pass
    _sanitize_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Pipeline
# ---------------------------------------------------------------------------

@app.route("/api/pipeline/<mode>", methods=["POST"])
def api_pipeline(mode):
    global _pipeline_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _pipeline_running:
        return jsonify({"error": "Pipeline já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")

    _pipeline_running = True
    _pipeline_logs.clear()

    def run():
        global _pipeline_running, _pipeline_proc
        try:
            cmd = [sys.executable, PIPELINE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])

            _add_pipeline_log(f"[INÍCIO] Pipeline — modo {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
            )
            _pipeline_proc = proc
            for line in proc.stdout:
                _add_pipeline_log(line)
            proc.wait()

            if proc.returncode == 0:
                _add_pipeline_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_pipeline_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_pipeline_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_pipeline_log(f"[ERRO] {e}")
            _add_pipeline_log(traceback.format_exc())
        finally:
            _pipeline_proc = None
            _pipeline_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/pipeline/logs")
def api_pipeline_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_pipeline_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _pipeline_running})


@app.route("/api/pipeline/status")
def api_pipeline_status():
    global _pipeline_running
    if _pipeline_running and (_pipeline_proc is None or _pipeline_proc.poll() is not None):
        _pipeline_running = False
    return jsonify({"running": _pipeline_running})


@app.route("/api/pipeline/stop", methods=["POST"])
def api_pipeline_stop():
    global _pipeline_running
    if _pipeline_proc is not None:
        try:
            _pipeline_proc.terminate()
        except Exception:
            pass
    _pipeline_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Upload
# ---------------------------------------------------------------------------

UPLOAD_DIR = BASE_DIR


def _find_xlsx():
    for f in UPLOAD_DIR.iterdir():
        if f.suffix.lower() == ".xlsx" and "matriculados" in f.name.lower():
            stat = f.stat()
            return {
                "name": f.name,
                "size": stat.st_size,
                "modified": to_brt(datetime.fromtimestamp(stat.st_mtime, tz=BRT)),
            }
    return None


@app.route("/api/upload/info")
def api_upload_info():
    return jsonify({"file": _find_xlsx()})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    if "file" not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado."}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Nenhum arquivo selecionado."}), 400

    if not f.filename.lower().endswith(".xlsx"):
        return jsonify({"error": "Apenas arquivos .xlsx são aceitos."}), 400

    for old in UPLOAD_DIR.iterdir():
        if old.suffix.lower() == ".xlsx" and "matriculados" in old.name.lower():
            old.unlink()

    safe_name = f.filename
    if "matriculados" not in safe_name.lower():
        safe_name = "Relação de matriculados por polo.xlsx"

    dest = UPLOAD_DIR / safe_name
    f.save(str(dest))

    stat = dest.stat()
    return jsonify({
        "ok": True,
        "file": {
            "name": dest.name,
            "size": stat.st_size,
            "modified": to_brt(datetime.fromtimestamp(stat.st_mtime, tz=BRT)),
        },
    })


# ---------------------------------------------------------------------------
# Rotas — Explorador de Logs
# ---------------------------------------------------------------------------

SAFE_LOG_DIRS = [LOG_DIR, REPORTS_DIR]


def _list_log_files():
    files = []
    for d in SAFE_LOG_DIRS:
        if not d.exists():
            continue
        for f in d.iterdir():
            if f.is_file() and f.suffix.lower() in (".csv", ".log", ".txt"):
                stat = f.stat()
                files.append({
                    "name": f.name,
                    "dir": d.name,
                    "path": f"{d.name}/{f.name}",
                    "size": stat.st_size,
                    "modified": to_brt(datetime.fromtimestamp(stat.st_mtime, tz=BRT)),
                })
    files.sort(key=lambda x: x["modified"], reverse=True)
    return files


def _resolve_log_path(filepath):
    """Resolve and validate a log file path, preventing directory traversal."""
    filepath = filepath.replace("\\", "/")
    if ".." in filepath:
        return None
    for d in SAFE_LOG_DIRS:
        candidate = d.parent / filepath
        try:
            candidate = candidate.resolve()
            if candidate.is_file() and any(str(candidate).startswith(str(sd.resolve())) for sd in SAFE_LOG_DIRS):
                return candidate
        except Exception:
            pass
    return None


@app.route("/api/logs")
def api_logs_list():
    return jsonify({"files": _list_log_files()})


@app.route("/api/logs/view/<path:filepath>")
def api_logs_view(filepath):
    fpath = _resolve_log_path(filepath)
    if not fpath:
        return jsonify({"error": "Arquivo não encontrado."}), 404

    tail = int(request.args.get("tail", 200))

    try:
        with open(fpath, "r", encoding="utf-8-sig", errors="replace") as f:
            lines = f.readlines()

        total = len(lines)
        if tail and tail < total:
            lines = lines[-tail:]

        return jsonify({
            "name": fpath.name,
            "total_lines": total,
            "showing": len(lines),
            "lines": [l.rstrip() for l in lines],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs/download/<path:filepath>")
def api_logs_download(filepath):
    fpath = _resolve_log_path(filepath)
    if not fpath:
        return jsonify({"error": "Arquivo não encontrado."}), 404
    return send_file(str(fpath), as_attachment=True)


# ---------------------------------------------------------------------------
# Rotas — Agendamento (Schedules)
# ---------------------------------------------------------------------------

@app.route("/api/schedules")
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

        # Add next run info from scheduler
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


@app.route("/api/schedules", methods=["POST"])
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


@app.route("/api/schedules/<schedule_id>", methods=["DELETE"])
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


@app.route("/api/schedules/<schedule_id>/toggle", methods=["POST"])
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

@app.route("/api/debug")
def api_debug():
    return jsonify({
        "sync_running": _sync_running,
        "sync_proc_alive": _sync_proc is not None and _sync_proc.poll() is None if _sync_proc else False,
        "sync_log_count": len(_sync_logs),
        "sync_logs_last5": list(_sync_logs)[-5:] if _sync_logs else [],
        "update_running": _update_running,
        "update_log_count": len(_update_logs),
        "python": sys.executable,
        "sync_script": SYNC_SCRIPT,
        "sync_script_exists": Path(SYNC_SCRIPT).exists(),
        "cwd": str(BASE_DIR),
    })


# ---------------------------------------------------------------------------
# APScheduler
# ---------------------------------------------------------------------------

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")

DAY_MAP = {"0": "mon", "1": "tue", "2": "wed", "3": "thu", "4": "fri", "5": "sat", "6": "sun"}


def _run_scheduled_sync(job_type):
    """Executa sync agendado (roda no thread do scheduler)."""
    global _sync_running, _sync_proc

    if _sync_running:
        app.logger.info("Scheduled %s skipped — sync already running", job_type)
        return

    mode = "full" if job_type == "sync_full" else "delta"
    _sync_running = True
    _sync_logs.clear()

    try:
        cmd = [sys.executable, SYNC_SCRIPT]
        if mode == "full":
            cmd.append("--full")

        _add_sync_log(f"[AGENDADO] Sincronização {mode.upper()} iniciada automaticamente")

        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
        )
        _sync_proc = proc

        for line in proc.stdout:
            _add_sync_log(line)

        proc.wait()

        if proc.returncode == 0:
            _add_sync_log("[FIM] Sincronização agendada concluída com sucesso")
        else:
            _add_sync_log(f"[ERRO] Sincronização agendada falhou (exit code {proc.returncode})")

        # Update last_run_at
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("UPDATE schedules SET last_run_at = NOW() WHERE job_type = %s", (job_type,))
            conn.commit()
            conn.close()
        except Exception:
            pass

    except Exception as e:
        _add_sync_log(f"[ERRO] {e}")
    finally:
        _sync_proc = None
        _sync_running = False


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
        app.logger.info("Schedules loaded from DB")
    except Exception as e:
        app.logger.warning("Could not load schedules: %s", e)


def _ensure_schedules_table():
    """Create the schedules table if it doesn't exist yet."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schedules (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    cron_days TEXT NOT NULL DEFAULT '*',
                    cron_hour INTEGER NOT NULL DEFAULT 2,
                    cron_minute INTEGER NOT NULL DEFAULT 0,
                    enabled BOOLEAN DEFAULT TRUE,
                    last_run_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        app.logger.warning("Could not ensure schedules table: %s", e)


# Start scheduler
_ensure_schedules_table()
scheduler.start()
_load_schedules_from_db()


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001, threaded=True, use_reloader=False)
