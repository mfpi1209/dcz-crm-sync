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
import queue
import time
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, render_template, request, jsonify, Response, stream_with_context
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

app = Flask(__name__)

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

_sync_lock = threading.Lock()
_sync_running = False
_sync_log_queues: list[queue.Queue] = []

_update_running = False
_update_log_queues: list[queue.Queue] = []
_update_proc = None

_sync_proc = None


def _broadcast_log(line: str):
    for q in _sync_log_queues:
        q.put(line)


def _broadcast_done():
    for q in _sync_log_queues:
        q.put(None)


def _broadcast_update_log(line: str):
    for q in _update_log_queues:
        q.put(line)


def _broadcast_update_done():
    for q in _update_log_queues:
        q.put(None)

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
    return render_template("index.html")


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

    def run():
        global _sync_running, _sync_proc
        try:
            cmd = [sys.executable, SYNC_SCRIPT]
            if mode == "full":
                cmd.append("--full")

            _broadcast_log(f"[INÍCIO] Sincronização {mode.upper()} iniciada\n")

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(Path(__file__).parent),
            )
            _sync_proc = proc

            for line in proc.stdout:
                _broadcast_log(line)

            proc.wait()

            if proc.returncode == 0:
                _broadcast_log(f"\n[FIM] Sincronização concluída com sucesso (exit code 0)\n")
            elif proc.returncode < 0 or proc.returncode == 1:
                _broadcast_log(f"\n[PARADO] Sincronização interrompida.\n")
            else:
                _broadcast_log(f"\n[ERRO] Sincronização falhou (exit code {proc.returncode})\n")
        except Exception as e:
            _broadcast_log(f"\n[ERRO] {e}\n")
        finally:
            _sync_proc = None
            _sync_running = False
            _broadcast_done()

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/sync/stream")
def api_sync_stream():
    q = queue.Queue()
    _sync_log_queues.append(q)

    def generate():
        try:
            while True:
                try:
                    msg = q.get(timeout=60)
                except queue.Empty:
                    yield "data: \n\n"
                    continue

                if msg is None:
                    yield "data: [DONE]\n\n"
                    break

                for line in msg.splitlines():
                    yield f"data: {line}\n\n"
        finally:
            if q in _sync_log_queues:
                _sync_log_queues.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/sync/status")
def api_sync_status():
    return jsonify({"running": _sync_running})


@app.route("/api/sync/stop", methods=["POST"])
def api_sync_stop():
    if not _sync_running or _sync_proc is None:
        return jsonify({"error": "Nenhuma sincronização em andamento."}), 400
    try:
        _sync_proc.terminate()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

    def run():
        global _update_running, _update_proc
        try:
            cmd = [sys.executable, UPDATE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])

            _broadcast_update_log(f"[INÍCIO] Update CRM — modo {mode.upper()}\n")

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(Path(__file__).parent),
            )
            _update_proc = proc

            for line in proc.stdout:
                _broadcast_update_log(line)

            proc.wait()

            if proc.returncode == 0:
                _broadcast_update_log(f"\n[FIM] Concluído com sucesso (exit code 0)\n")
            elif proc.returncode < 0 or proc.returncode == 1:
                _broadcast_update_log(f"\n[PARADO] Processo interrompido pelo usuário.\n")
            else:
                _broadcast_update_log(f"\n[ERRO] Falhou (exit code {proc.returncode})\n")
        except Exception as e:
            _broadcast_update_log(f"\n[ERRO] {e}\n")
        finally:
            _update_proc = None
            _update_running = False
            _broadcast_update_done()

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/update/stream")
def api_update_stream():
    q = queue.Queue()
    _update_log_queues.append(q)

    def generate():
        try:
            while True:
                try:
                    msg = q.get(timeout=60)
                except queue.Empty:
                    yield "data: \n\n"
                    continue

                if msg is None:
                    yield "data: [DONE]\n\n"
                    break

                for line in msg.splitlines():
                    yield f"data: {line}\n\n"
        finally:
            if q in _update_log_queues:
                _update_log_queues.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/update/status")
def api_update_status():
    return jsonify({"running": _update_running})


@app.route("/api/update/stop", methods=["POST"])
def api_update_stop():
    if not _update_running or _update_proc is None:
        return jsonify({"error": "Nenhuma atualização em andamento."}), 400
    try:
        _update_proc.terminate()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
