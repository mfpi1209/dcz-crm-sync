import sys
import os
import re
import subprocess
import threading
import time
import json
from datetime import datetime
from pathlib import Path
from collections import deque

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

from db import get_conn
from helpers import (
    BRT, to_brt, SEARCH_QUERY, SYNC_STATE_QUERY, RECENT_BIZ_UPDATES_QUERY,
    FIELD_RGM, _normalize_digits, BASE_DIR,
    SYNC_SCRIPT, UPDATE_SCRIPT, SANITIZE_SCRIPT, PIPELINE_SCRIPT,
    ENRICH_SCRIPT, MERGE_SCRIPT, INADIMPLENTES_SCRIPT, CONCLUINTES_SCRIPT,
    LOG_DIR, REPORTS_DIR, MAX_LOG_LINES,
)

crm_bp = Blueprint("crm", __name__)

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

_enrich_running = False
_enrich_proc = None
_enrich_logs: deque = deque(maxlen=MAX_LOG_LINES)

_merge_running = False
_merge_proc = None
_merge_logs: deque = deque(maxlen=MAX_LOG_LINES)

_inadimplentes_running = False
_inadimplentes_proc = None
_inadimplentes_logs: deque = deque(maxlen=MAX_LOG_LINES)

_concluintes_running = False
_concluintes_proc = None
_concluintes_logs: deque = deque(maxlen=MAX_LOG_LINES)


def _add_sync_log(line: str):
    _sync_logs.append(line.rstrip())


def _add_update_log(line: str):
    _update_logs.append(line.rstrip())


def _add_sanitize_log(line: str):
    _sanitize_logs.append(line.rstrip())


def _add_pipeline_log(line: str):
    _pipeline_logs.append(line.rstrip())


def _add_enrich_log(line: str):
    _enrich_logs.append(line.rstrip())


def _add_merge_log(line: str):
    _merge_logs.append(line.rstrip())


def _add_inadimplentes_log(line: str):
    _inadimplentes_logs.append(line.rstrip())


def _add_concluintes_log(line: str):
    _concluintes_logs.append(line.rstrip())


# ---------------------------------------------------------------------------
# Rotas — Debug
# ---------------------------------------------------------------------------

@crm_bp.route("/api/debug/address")
def api_debug_address():
    """Compara address no banco local vs API direta para diagnosticar sync."""
    import requests as req
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, data->>'name' AS nome, data->'address' AS local_address
                FROM leads
                WHERE data->'address' IS NOT NULL
                   AND data->'address' != 'null'::jsonb
                   AND data->'address' != '{}'::jsonb
                LIMIT 3
            """)
            with_addr = cur.fetchall()

            cur.execute("""
                SELECT id, data->>'name' AS nome, data->'address' AS local_address
                FROM leads
                WHERE data->'address' IS NULL
                   OR data->'address' = 'null'::jsonb
                   OR data->'address' = '{}'::jsonb
                LIMIT 3
            """)
            without_addr = cur.fetchall()

            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE data->'address' IS NOT NULL
                        AND data->'address' != 'null'::jsonb
                        AND data->'address' != '{}'::jsonb) AS com_address,
                    COUNT(*) FILTER (WHERE data->'address' IS NULL
                        OR data->'address' = 'null'::jsonb
                        OR data->'address' = '{}'::jsonb) AS sem_address,
                    COUNT(*) AS total
                FROM leads
            """)
            stats = dict(cur.fetchone())

        token = os.getenv("DATACRAZY_API_TOKEN", "")
        headers = {"Authorization": f"Bearer {token}"}
        api_base = "https://api.g1.datacrazy.io/api/v1"

        api_samples = []
        sample_ids = [r["id"] for r in (with_addr[:1] + without_addr[:1])]
        for lid in sample_ids:
            try:
                r = req.get(f"{api_base}/leads/{lid}", headers=headers, timeout=15)
                if r.ok:
                    d = r.json()
                    api_samples.append({
                        "id": lid,
                        "nome": d.get("name"),
                        "api_address": d.get("address"),
                    })
            except Exception as e:
                api_samples.append({"id": lid, "error": str(e)})

        list_sample = []
        try:
            r = req.get(f"{api_base}/leads", headers=headers, params={
                "take": 2,
                "complete[additionalFields]": "true",
            }, timeout=15)
            if r.ok:
                for lead in r.json().get("data", [])[:2]:
                    list_sample.append({
                        "id": lead.get("id"),
                        "nome": lead.get("name"),
                        "list_address": lead.get("address"),
                        "has_address_key": "address" in lead,
                    })
        except Exception as e:
            list_sample = [{"error": str(e)}]

        return jsonify({
            "stats": stats,
            "local_with_addr_samples": [dict(r) for r in with_addr],
            "local_without_addr_samples": [dict(r) for r in without_addr],
            "api_individual_samples": api_samples,
            "api_list_samples": list_sample,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Busca
# ---------------------------------------------------------------------------

@crm_bp.route("/api/search")
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


@crm_bp.route("/api/search-xl")
def api_search_xl():
    cpf = _normalize_digits(request.args.get("cpf", ""))
    rgm = _normalize_digits(request.args.get("rgm", ""))
    telefone = _normalize_digits(request.args.get("telefone", ""))
    snapshot_id = request.args.get("snapshot_id", "")
    tipo = request.args.get("tipo", "").strip().lower()

    if not cpf and not rgm and not telefone:
        return jsonify({"results": [], "snapshot": None})

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if snapshot_id:
                cur.execute("SELECT id, tipo, filename, row_count, uploaded_at FROM xl_snapshots WHERE id = %s", (snapshot_id,))
            elif tipo:
                cur.execute("SELECT id, tipo, filename, row_count, uploaded_at FROM xl_snapshots WHERE tipo = %s ORDER BY id DESC LIMIT 1", (tipo,))
            else:
                cur.execute("SELECT id, tipo, filename, row_count, uploaded_at FROM xl_snapshots ORDER BY id DESC LIMIT 1")
            snap = cur.fetchone()
            if not snap:
                return jsonify({"results": [], "snapshot": None})

            snap_info = {
                "id": snap["id"],
                "tipo": snap["tipo"],
                "filename": snap["filename"],
                "row_count": snap["row_count"],
                "uploaded_at": to_brt(snap["uploaded_at"]),
            }
            sid = snap["id"]

            conditions = []
            params_list = [sid]

            if cpf:
                conditions.append("data->>'cpf_digits' LIKE '%%' || %s || '%%'")
                params_list.append(cpf)
            if rgm:
                conditions.append("data->>'rgm' LIKE '%%' || %s || '%%'")
                params_list.append(rgm)
            if telefone:
                conditions.append("""(
                    EXISTS (SELECT 1 FROM jsonb_array_elements_text(data->'phones_digits') ph WHERE ph LIKE '%%' || %s || '%%')
                )""")
                params_list.append(telefone)

            where = " OR ".join(conditions)
            cur.execute(
                f"SELECT data FROM xl_rows WHERE snapshot_id = %s AND ({where}) LIMIT 20",
                params_list,
            )
            rows = cur.fetchall()

        results = []
        for r in rows:
            d = r["data"]
            results.append({k: v for k, v in d.items()
                            if k not in ("cpf_digits", "rgm_digits", "phones_digits")})

        return jsonify({"results": results, "snapshot": snap_info})
    except Exception as e:
        return jsonify({"results": [], "snapshot": None, "error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Sync State
# ---------------------------------------------------------------------------

@crm_bp.route("/api/sync-state")
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

@crm_bp.route("/api/sync/<mode>", methods=["POST"])
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


@crm_bp.route("/api/sync/logs")
def api_sync_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_sync_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _sync_running})


@crm_bp.route("/api/sync/status")
def api_sync_status():
    global _sync_running
    if _sync_running and (_sync_proc is None or _sync_proc.poll() is not None):
        _sync_running = False
    return jsonify({"running": _sync_running})


@crm_bp.route("/api/sync/stop", methods=["POST"])
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

@crm_bp.route("/api/update/<mode>", methods=["POST"])
def api_update(mode):
    global _update_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _update_running:
        return jsonify({"error": "Atualização já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")
    rate = body.get("rate")
    with_address = body.get("withAddress", False)

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
            if with_address:
                cmd.append("--with-address")

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


@crm_bp.route("/api/update/logs")
def api_update_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_update_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _update_running})


@crm_bp.route("/api/update/status")
def api_update_status():
    global _update_running
    if _update_running and (_update_proc is None or _update_proc.poll() is not None):
        _update_running = False
    return jsonify({"running": _update_running})


@crm_bp.route("/api/update/stop", methods=["POST"])
def api_update_stop():
    global _update_running
    if _update_proc is not None:
        try:
            _update_proc.terminate()
        except Exception:
            pass
    _update_running = False
    return jsonify({"ok": True})


@crm_bp.route("/api/update/preview")
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

@crm_bp.route("/api/sanitize/<mode>", methods=["POST"])
def api_sanitize(mode):
    global _sanitize_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _sanitize_running:
        return jsonify({"error": "Saneamento já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")
    rate = body.get("rate", 60)

    _sanitize_running = True
    _sanitize_logs.clear()

    def run():
        global _sanitize_running, _sanitize_proc
        try:
            cmd = [sys.executable, SANITIZE_SCRIPT, f"--{mode}",
                   "--rate", str(int(rate))]
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


@crm_bp.route("/api/sanitize/logs")
def api_sanitize_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_sanitize_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _sanitize_running})


@crm_bp.route("/api/sanitize/status")
def api_sanitize_status():
    global _sanitize_running
    if _sanitize_running and (_sanitize_proc is None or _sanitize_proc.poll() is not None):
        _sanitize_running = False
    return jsonify({"running": _sanitize_running})


@crm_bp.route("/api/sanitize/stop", methods=["POST"])
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
# Rotas — Enriquecimento de duplicatas
# ---------------------------------------------------------------------------

@crm_bp.route("/api/enrich/start", methods=["POST"])
def api_enrich_start():
    global _enrich_running
    if _enrich_running:
        return jsonify({"error": "Enriquecimento já em andamento."}), 409

    body = request.json if request.is_json else {}
    rate = body.get("rate", 60)

    _enrich_running = True
    _enrich_logs.clear()

    def run():
        global _enrich_running, _enrich_proc
        try:
            cmd = [sys.executable, ENRICH_SCRIPT, "--rate", str(int(rate))]
            _add_enrich_log("[INÍCIO] Enriquecimento de duplicatas entre leads")
            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
            )
            _enrich_proc = proc
            for line in proc.stdout:
                _add_enrich_log(line)
            proc.wait()
            if proc.returncode == 0:
                _add_enrich_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_enrich_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_enrich_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_enrich_log(f"[ERRO] {e}")
            _add_enrich_log(traceback.format_exc())
        finally:
            _enrich_proc = None
            _enrich_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True})


@crm_bp.route("/api/enrich/logs")
def api_enrich_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_enrich_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _enrich_running})


@crm_bp.route("/api/enrich/stop", methods=["POST"])
def api_enrich_stop():
    global _enrich_running
    if _enrich_proc is not None:
        try:
            _enrich_proc.terminate()
        except Exception:
            pass
    _enrich_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Merge
# ---------------------------------------------------------------------------

@crm_bp.route("/api/merge/start", methods=["POST"])
def api_merge_start():
    global _merge_running
    if _merge_running:
        return jsonify({"error": "Merge já em andamento."}), 409

    body = request.json if request.is_json else {}
    mode = body.get("mode", "dry-run")
    fase = body.get("fase")
    limit = body.get("limit")
    rate = body.get("rate", 60)

    _merge_running = True
    _merge_logs.clear()

    def run():
        global _merge_running, _merge_proc
        try:
            cmd = [sys.executable, MERGE_SCRIPT, f"--{mode}"]
            if fase:
                cmd += ["--fase", str(int(fase))]
            if limit:
                cmd += ["--limit", str(int(limit))]
            cmd += ["--rate", str(int(rate))]

            _add_merge_log(f"[INÍCIO] Merge de leads — {mode}" +
                           (f" (fase {fase})" if fase else "") +
                           (f" (limit {limit})" if limit else ""))
            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
            )
            _merge_proc = proc
            for line in proc.stdout:
                _add_merge_log(line)
            proc.wait()
            if proc.returncode == 0:
                _add_merge_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_merge_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_merge_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_merge_log(f"[ERRO] {e}")
            _add_merge_log(traceback.format_exc())
        finally:
            _merge_proc = None
            _merge_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True})


@crm_bp.route("/api/merge/logs")
def api_merge_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_merge_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _merge_running})


@crm_bp.route("/api/merge/stop", methods=["POST"])
def api_merge_stop():
    global _merge_running
    if _merge_proc is not None:
        try:
            _merge_proc.terminate()
        except Exception:
            pass
    _merge_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Pipeline
# ---------------------------------------------------------------------------

@crm_bp.route("/api/pipeline/<mode>", methods=["POST"])
def api_pipeline(mode):
    global _pipeline_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _pipeline_running:
        return jsonify({"error": "Pipeline já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")
    rate = body.get("rate")

    _pipeline_running = True
    _pipeline_logs.clear()

    def run():
        global _pipeline_running, _pipeline_proc
        try:
            cmd = [sys.executable, PIPELINE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])
            if rate:
                cmd.extend(["--rate", str(int(rate))])

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


@crm_bp.route("/api/pipeline/logs")
def api_pipeline_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_pipeline_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _pipeline_running})


@crm_bp.route("/api/pipeline/status")
def api_pipeline_status():
    global _pipeline_running
    if _pipeline_running and (_pipeline_proc is None or _pipeline_proc.poll() is not None):
        _pipeline_running = False
    return jsonify({"running": _pipeline_running})


@crm_bp.route("/api/pipeline/stop", methods=["POST"])
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
# Rotas — Inadimplentes
# ---------------------------------------------------------------------------

@crm_bp.route("/api/inadimplentes/<mode>", methods=["POST"])
def api_inadimplentes(mode):
    global _inadimplentes_running

    if mode not in ("dry-run", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run' ou 'execute'."}), 400

    if _inadimplentes_running:
        return jsonify({"error": "Atualização de inadimplentes já em andamento."}), 409

    body = request.json if request.is_json else {}
    rate = body.get("rate")

    _inadimplentes_running = True
    _inadimplentes_logs.clear()

    def run():
        global _inadimplentes_running, _inadimplentes_proc
        try:
            cmd = [sys.executable, INADIMPLENTES_SCRIPT, f"--{mode}"]
            if rate:
                cmd.extend(["--rate", str(int(rate))])

            _add_inadimplentes_log(f"[INÍCIO] Inadimplentes — {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
            )
            _inadimplentes_proc = proc
            for line in proc.stdout:
                _add_inadimplentes_log(line)
            proc.wait()

            if proc.returncode == 0:
                _add_inadimplentes_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_inadimplentes_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_inadimplentes_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_inadimplentes_log(f"[ERRO] {e}")
            _add_inadimplentes_log(traceback.format_exc())
        finally:
            _inadimplentes_proc = None
            _inadimplentes_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@crm_bp.route("/api/inadimplentes/logs")
def api_inadimplentes_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_inadimplentes_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _inadimplentes_running})


@crm_bp.route("/api/inadimplentes/status")
def api_inadimplentes_status():
    global _inadimplentes_running
    if _inadimplentes_running and (_inadimplentes_proc is None or _inadimplentes_proc.poll() is not None):
        _inadimplentes_running = False
    return jsonify({"running": _inadimplentes_running})


@crm_bp.route("/api/inadimplentes/stop", methods=["POST"])
def api_inadimplentes_stop():
    global _inadimplentes_running
    if _inadimplentes_proc is not None:
        try:
            _inadimplentes_proc.terminate()
        except Exception:
            pass
    _inadimplentes_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Concluintes
# ---------------------------------------------------------------------------

@crm_bp.route("/api/concluintes/<mode>", methods=["POST"])
def api_concluintes(mode):
    global _concluintes_running

    if mode not in ("dry-run", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run' ou 'execute'."}), 400

    if _concluintes_running:
        return jsonify({"error": "Atualização de concluintes já em andamento."}), 409

    body = request.json if request.is_json else {}
    rate = body.get("rate")

    _concluintes_running = True
    _concluintes_logs.clear()

    def run():
        global _concluintes_running, _concluintes_proc
        try:
            cmd = [sys.executable, CONCLUINTES_SCRIPT, f"--{mode}"]
            if rate:
                cmd.extend(["--rate", str(int(rate))])

            _add_concluintes_log(f"[INÍCIO] Concluintes — {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
            )
            _concluintes_proc = proc
            for line in proc.stdout:
                _add_concluintes_log(line)
            proc.wait()

            if proc.returncode == 0:
                _add_concluintes_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_concluintes_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_concluintes_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_concluintes_log(f"[ERRO] {e}")
            _add_concluintes_log(traceback.format_exc())
        finally:
            _concluintes_proc = None
            _concluintes_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@crm_bp.route("/api/concluintes/logs")
def api_concluintes_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_concluintes_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _concluintes_running})


@crm_bp.route("/api/concluintes/status")
def api_concluintes_status():
    global _concluintes_running
    if _concluintes_running and (_concluintes_proc is None or _concluintes_proc.poll() is not None):
        _concluintes_running = False
    return jsonify({"running": _concluintes_running})


@crm_bp.route("/api/concluintes/stop", methods=["POST"])
def api_concluintes_stop():
    global _concluintes_running
    if _concluintes_proc is not None:
        try:
            _concluintes_proc.terminate()
        except Exception:
            pass
    _concluintes_running = False
    return jsonify({"ok": True})
