"""
DataCrazy CRM Sync — Interface Web (Flask).

Uso:
    python app.py
    Acesse http://localhost:5000
"""

import os
import sys
import json
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from flask import (
    Flask, render_template, request, jsonify,
    redirect, url_for, session,
)
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

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

SYNC_SCRIPT = str(Path(__file__).parent / "sync.py")
UPDATE_SCRIPT = str(Path(__file__).parent / "update_crm.py")

# ---------------------------------------------------------------------------
# Estado global (para controlar execuções concorrentes)
# ---------------------------------------------------------------------------

_sync_running = False
_sync_proc = None
_sync_logs: list[str] = []

_update_running = False
_update_proc = None
_update_logs: list[str] = []


def _add_sync_log(line: str):
    _sync_logs.append(line.rstrip())


def _add_update_log(line: str):
    _update_logs.append(line.rstrip())

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_DSN)


# IDs dos campos personalizados dos negócios
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
    -- CPF
    (%(cpf)s != '' AND REPLACE(REPLACE(l.data->>'taxId', '.', ''), '-', '') LIKE '%%' || REPLACE(REPLACE(%(cpf)s, '.', ''), '-', '') || '%%')
    -- RGM
    OR (%(rgm)s != '' AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(b.data->'additionalFields') e
        WHERE e->'additionalField'->>'id' = '2ac4e30f-cfd7-435f-b688-fbce27f76c38'
          AND e->>'value' LIKE '%%' || %(rgm)s || '%%'
    ))
    -- Telefone
    OR (%(telefone)s != '' AND (
        l.data->>'rawPhone' LIKE '%%' || %(telefone)s || '%%'
        OR REPLACE(REPLACE(REPLACE(REPLACE(l.data->>'phone', ' ', ''), '(', ''), ')', ''), '-', '') LIKE '%%' || %(telefone)s || '%%'
    ))
)
ORDER BY b.data->>'lastMovedAt' DESC NULLS LAST
LIMIT 50;
"""

RECENT_UPDATES_QUERY = """
SELECT
    'lead' AS tipo,
    l.id,
    l.data->>'name' AS nome,
    l.synced_at
FROM leads l
WHERE l.synced_at = (SELECT MAX(synced_at) FROM leads)
ORDER BY l.synced_at DESC
LIMIT 10;
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
# Rotas
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    resp = app.make_response(render_template("index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


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
                        row[k] = v.isoformat()
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
                        row[k] = v.isoformat()
                states.append(row)

            cur.execute(RECENT_BIZ_UPDATES_QUERY)
            recent = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = v.isoformat()
                recent.append(row)

        return jsonify({"states": states, "recent_updates": recent})
    except Exception as e:
        return jsonify({"states": [], "recent_updates": [], "error": str(e)}), 500
    finally:
        conn.close()


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
            _add_sync_log(f"CMD: {' '.join(cmd)}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(Path(__file__).parent),
                env=env,
            )
            _sync_proc = proc

            for line in proc.stdout:
                _add_sync_log(line)

            proc.wait()

            if proc.returncode == 0:
                _add_sync_log("[FIM] Sincronização concluída com sucesso")
            elif proc.returncode < 0 or proc.returncode == 1:
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


@app.route("/api/debug")
def api_debug():
    return jsonify({
        "sync_running": _sync_running,
        "sync_proc_alive": _sync_proc is not None and _sync_proc.poll() is None if _sync_proc else False,
        "sync_log_count": len(_sync_logs),
        "sync_logs_last5": _sync_logs[-5:] if _sync_logs else [],
        "update_running": _update_running,
        "update_log_count": len(_update_logs),
        "python": sys.executable,
        "sync_script": SYNC_SCRIPT,
        "sync_script_exists": Path(SYNC_SCRIPT).exists(),
        "cwd": str(Path(__file__).parent),
    })


@app.route("/api/sync/logs")
def api_sync_logs():
    since = int(request.args.get("since", 0))
    lines = _sync_logs[since:]
    return jsonify({"lines": lines, "total": len(_sync_logs), "running": _sync_running})


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
# Update CRM routes
# ---------------------------------------------------------------------------

@app.route("/api/update/<mode>", methods=["POST"])
def api_update(mode):
    global _update_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _update_running:
        return jsonify({"error": "Atualização já em andamento."}), 409

    limit = request.json.get("limit") if request.is_json else None

    _update_running = True
    _update_logs.clear()

    def run():
        global _update_running, _update_proc
        try:
            cmd = [sys.executable, UPDATE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])

            _add_update_log(f"[INÍCIO] Update CRM — modo {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(Path(__file__).parent),
                env=env,
            )
            _update_proc = proc

            for line in proc.stdout:
                _add_update_log(line)

            proc.wait()

            if proc.returncode == 0:
                _add_update_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0 or proc.returncode == 1:
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
    lines = _update_logs[since:]
    return jsonify({"lines": lines, "total": len(_update_logs), "running": _update_running})


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
    preview_path = Path(__file__).parent / "reports" / "update_preview.csv"
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
# Upload da planilha de matriculados
# ---------------------------------------------------------------------------

UPLOAD_DIR = Path(__file__).parent

def _find_xlsx():
    """Retorna info do .xlsx de matriculados atual, se existir."""
    for f in UPLOAD_DIR.iterdir():
        if f.suffix.lower() == ".xlsx" and "matriculados" in f.name.lower():
            stat = f.stat()
            return {
                "name": f.name,
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
    return None


@app.route("/api/upload/info")
def api_upload_info():
    info = _find_xlsx()
    if info:
        return jsonify({"file": info})
    return jsonify({"file": None})


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
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        },
    })


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001, threaded=True, use_reloader=False)
