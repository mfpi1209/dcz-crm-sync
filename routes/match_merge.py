"""
Match & Merge SIAA — Flask Blueprint.

Endpoints:
  POST /api/match-merge/upload       upload .xlsm/.xlsx (candidatos/matriculados)
  POST /api/match-merge/process      trigger full pipeline in background
  GET  /api/match-merge/status       processing state
  GET  /api/match-merge/preview      action preview (after processing)
  POST /api/match-merge/execute      execute Kommo updates
  GET  /api/match-merge/logs?since=N polling logs
"""

import os
import json
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import deque

from flask import Blueprint, request, jsonify

from helpers import BASE_DIR, MAX_LOG_LINES

match_merge_bp = Blueprint("match_merge", __name__)

BRT = timezone(timedelta(hours=-3))

UPLOAD_DIR = BASE_DIR / "uploads" / "match_merge"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

_running = False
_logs: deque = deque(maxlen=MAX_LOG_LINES)
_result = None
_exec_running = False
_exec_logs: deque = deque(maxlen=MAX_LOG_LINES)
_exec_result = None
_unif_running = False
_unif_logs: deque = deque(maxlen=MAX_LOG_LINES)
_unif_result = None
_uploaded = {"candidatos": [], "matriculados": []}


def _ts():
    return datetime.now(BRT).strftime("%H:%M:%S")


def _add_log(line: str):
    _logs.append(f"{_ts()} {line.rstrip()}")


def _add_exec_log(line: str):
    _exec_logs.append(f"{_ts()} {line.rstrip()}")


# ── Upload ──────────────────────────────────────────────────────

@match_merge_bp.route("/api/match-merge/upload", methods=["POST"])
def mm_upload():
    """Upload multiple files for a given tipo (candidatos or matriculados)."""
    tipo = request.form.get("tipo", "candidatos")
    if tipo not in ("candidatos", "matriculados"):
        return jsonify({"error": "tipo deve ser 'candidatos' ou 'matriculados'"}), 400

    nivel = request.form.get("nivel", "grad")
    dest = UPLOAD_DIR / nivel / tipo
    dest.mkdir(parents=True, exist_ok=True)

    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    saved = []
    for f in files:
        if not f.filename:
            continue
        ext = os.path.splitext(f.filename)[1].lower()
        if ext not in (".xlsx", ".xlsm", ".xls"):
            continue
        path = dest / f.filename
        f.save(str(path))
        saved.append(str(path))

    _uploaded.setdefault(tipo, []).extend(saved)

    return jsonify({
        "ok": True,
        "tipo": tipo,
        "nivel": nivel,
        "saved": len(saved),
        "filenames": [os.path.basename(p) for p in saved],
    })


@match_merge_bp.route("/api/match-merge/clear-uploads", methods=["POST"])
def mm_clear_uploads():
    """Clear uploaded files and state."""
    global _result, _exec_result
    import shutil
    for sub in UPLOAD_DIR.iterdir():
        if sub.is_dir():
            shutil.rmtree(sub, ignore_errors=True)
    _uploaded.clear()
    _uploaded["candidatos"] = []
    _uploaded["matriculados"] = []
    _result = None
    _exec_result = None
    _logs.clear()
    _exec_logs.clear()
    return jsonify({"ok": True})


@match_merge_bp.route("/api/match-merge/upload-info", methods=["GET"])
def mm_upload_info():
    """List currently uploaded files per tipo."""
    info = {}
    for tipo in ("candidatos", "matriculados"):
        files = []
        for nivel_dir in UPLOAD_DIR.iterdir():
            if not nivel_dir.is_dir():
                continue
            tipo_dir = nivel_dir / tipo
            if tipo_dir.is_dir():
                for f in tipo_dir.iterdir():
                    if f.is_file():
                        files.append({
                            "name": f.name,
                            "size": f.stat().st_size,
                            "nivel": nivel_dir.name,
                        })
        info[tipo] = files
    return jsonify(info)


# ── Process (pipeline) ──────────────────────────────────────────

@match_merge_bp.route("/api/match-merge/process", methods=["POST"])
def mm_process():
    """Start the full pipeline in a background thread."""
    global _running, _result
    if _running:
        return jsonify({"error": "Pipeline já está em execução."}), 409

    nivel = request.json.get("nivel", "grad") if request.is_json else "grad"

    cand_dir = UPLOAD_DIR / nivel / "candidatos"
    mat_dir = UPLOAD_DIR / nivel / "matriculados"

    cand_files = sorted(str(f) for f in cand_dir.iterdir() if f.is_file()) if cand_dir.is_dir() else []
    mat_files = sorted(str(f) for f in mat_dir.iterdir() if f.is_file()) if mat_dir.is_dir() else []

    if not cand_files and not mat_files:
        return jsonify({"error": "Nenhum arquivo uploaded. Faça upload primeiro."}), 400

    _logs.clear()
    _result = None

    def _run():
        global _running, _result
        _running = True
        try:
            from match_merge_lib import run_pipeline
            _result = run_pipeline(
                candidatos_files=cand_files,
                matriculados_files=mat_files,
                nivel=nivel,
                log_callback=_add_log,
            )
        except Exception as e:
            import traceback
            _add_log(f"ERRO FATAL: {e}")
            _add_log(traceback.format_exc())
            _result = {"error": str(e)}
        finally:
            _running = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return jsonify({"ok": True, "msg": "Pipeline iniciado.", "nivel": nivel,
                    "candidatos": len(cand_files), "matriculados": len(mat_files)})


@match_merge_bp.route("/api/match-merge/status", methods=["GET"])
def mm_status():
    return jsonify({
        "running": _running,
        "exec_running": _exec_running,
        "unif_running": _unif_running,
        "has_result": _result is not None,
        "has_exec_result": _exec_result is not None,
        "has_unif_result": _unif_result is not None,
    })


@match_merge_bp.route("/api/match-merge/logs", methods=["GET"])
def mm_logs():
    since = int(request.args.get("since", 0))
    lines = list(_logs)
    return jsonify({"lines": lines[since:], "total": len(lines)})


# ── Preview ─────────────────────────────────────────────────────

@match_merge_bp.route("/api/match-merge/preview", methods=["GET"])
def mm_preview():
    """Return pipeline results (stats + actions) for review."""
    if _running:
        return jsonify({"running": True, "msg": "Pipeline em execução..."})
    if _result is None:
        return jsonify({"error": "Nenhum resultado. Execute o pipeline primeiro."}), 400
    if "error" in _result:
        return jsonify({"error": _result["error"]}), 500

    acoes = _result.get("acoes", [])
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))
    filtro = request.args.get("filtro", "")

    if filtro:
        acoes = [a for a in acoes if a["acao"] == filtro.upper()]

    total = len(acoes)
    start = (page - 1) * per_page
    paginated = acoes[start:start + per_page]

    return jsonify({
        "stats": {
            "inscritos": _result.get("inscritos", 0),
            "matriculados": _result.get("matriculados", 0),
            "cruzamento": _result.get("cruzamento", {}),
            "match": _result.get("match", {}),
            "elapsed": _result.get("elapsed", 0),
        },
        "acoes_total": total,
        "acoes_por_tipo": {
            "NOVO": sum(1 for a in _result.get("acoes", []) if a["acao"] == "NOVO"),
            "ATUALIZAR": sum(1 for a in _result.get("acoes", []) if a["acao"] == "ATUALIZAR"),
            "MATRICULADO": sum(1 for a in _result.get("acoes", []) if a["acao"] == "MATRICULADO"),
            "MOVER_PERDIDO": sum(1 for a in _result.get("acoes", []) if a["acao"] == "MOVER_PERDIDO"),
            "RESTAURAR": sum(1 for a in _result.get("acoes", []) if a["acao"] == "RESTAURAR"),
            "UNIFICAR": sum(1 for a in _result.get("acoes", []) if a["acao"] == "UNIFICAR"),
            "UNIFICAR_AUTO": sum(1 for a in _result.get("acoes", []) if a["acao"] == "UNIFICAR" and a.get("auto_decided")),
            "UNIFICAR_MANUAL": sum(1 for a in _result.get("acoes", []) if a["acao"] == "UNIFICAR" and not a.get("auto_decided")),
        },
        "acoes": paginated,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if per_page else 1,
    })


# ── Execute ─────────────────────────────────────────────────────

@match_merge_bp.route("/api/match-merge/execute", methods=["POST"])
def mm_execute():
    """Execute Kommo updates for generated actions."""
    global _exec_running, _exec_result
    if _exec_running:
        return jsonify({"error": "Execução já em andamento."}), 409
    if _result is None or "error" in (_result or {}):
        return jsonify({"error": "Execute o pipeline primeiro."}), 400

    acoes = _result.get("acoes", [])
    if not acoes:
        return jsonify({"error": "Nenhuma ação para executar."}), 400

    data = request.json or {}
    limit = data.get("limit")
    filtro = data.get("filtro", "")

    to_exec = acoes
    if filtro:
        to_exec = [a for a in to_exec if a["acao"] == filtro.upper()]
    if limit:
        to_exec = to_exec[:int(limit)]

    _exec_logs.clear()
    _exec_result = None

    def _run():
        global _exec_running, _exec_result
        _exec_running = True
        try:
            from match_merge_lib import executar_acoes
            _exec_result = executar_acoes(to_exec, log_callback=_add_exec_log)
        except Exception as e:
            import traceback
            _add_exec_log(f"ERRO: {e}")
            _add_exec_log(traceback.format_exc())
            _exec_result = {"error": str(e)}
        finally:
            _exec_running = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return jsonify({"ok": True, "msg": f"Executando {len(to_exec)} ações...",
                    "total": len(to_exec)})


@match_merge_bp.route("/api/match-merge/exec-status", methods=["GET"])
def mm_exec_status():
    since = int(request.args.get("since", 0))
    lines = list(_exec_logs)
    return jsonify({
        "running": _exec_running,
        "result": _exec_result,
        "lines": lines[since:],
        "total": len(lines),
    })


# ── Execute UNIFICAR em lote ─────────────────────────────────────

def _add_unif_log(line: str):
    _unif_logs.append(f"{_ts()} {line.rstrip()}")


@match_merge_bp.route("/api/match-merge/execute-unificar-lote", methods=["POST"])
def mm_execute_unificar_lote():
    """Execute auto-decided UNIFICAR merges in batch."""
    global _unif_running, _unif_result
    if _unif_running:
        return jsonify({"error": "Unificação em lote já em andamento."}), 409
    if _result is None or "error" in (_result or {}):
        return jsonify({"error": "Execute o pipeline primeiro."}), 400

    acoes = _result.get("acoes", [])
    auto_unif = [a for a in acoes if a["acao"] == "UNIFICAR" and a.get("auto_decided")]

    if not auto_unif:
        return jsonify({"error": "Nenhuma unificação automática disponível."}), 400

    _unif_logs.clear()
    _unif_result = None

    def _run():
        global _unif_running, _unif_result
        _unif_running = True
        ok_count = 0
        err_count = 0
        try:
            from kommo_merge import merge_lead_pair
            total = len(auto_unif)
            _add_unif_log(f"Iniciando unificação em lote: {total} pares")
            for i, a in enumerate(auto_unif, 1):
                keep = a["auto_keep_id"]
                remove = a["auto_remove_id"]
                nome = a.get("nome", "?")
                reason = a.get("auto_reason", "")
                _add_unif_log(f"[{i}/{total}] {nome} — manter {keep}, remover {remove} ({reason})")
                try:
                    result = merge_lead_pair(keep, remove)
                    if result.get("ok"):
                        ok_count += 1
                        _add_unif_log(f"  OK — job {result.get('job_id', '?')}")
                    else:
                        err_count += 1
                        _add_unif_log(f"  ERRO: {result.get('error', 'desconhecido')}")
                except Exception as e:
                    err_count += 1
                    _add_unif_log(f"  ERRO: {e}")
                time.sleep(1)
            _add_unif_log(f"Concluído: {ok_count} ok, {err_count} erros de {total}")
            _unif_result = {"ok": ok_count, "error": err_count, "total": total}
        except Exception as e:
            import traceback
            _add_unif_log(f"ERRO FATAL: {e}")
            _add_unif_log(traceback.format_exc())
            _unif_result = {"error": str(e)}
        finally:
            _unif_running = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return jsonify({"ok": True, "msg": f"Unificando {len(auto_unif)} pares...",
                    "total": len(auto_unif)})


@match_merge_bp.route("/api/match-merge/unif-status", methods=["GET"])
def mm_unif_status():
    since = int(request.args.get("since", 0))
    lines = list(_unif_logs)
    return jsonify({
        "running": _unif_running,
        "result": _unif_result,
        "lines": lines[since:],
        "total": len(lines),
    })
