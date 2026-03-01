"""
eduit. — Blueprint de visualização de logs.
"""

import os
from datetime import datetime
from pathlib import Path

from flask import Blueprint, request, jsonify, send_file

from helpers import BRT, to_brt, LOG_DIR, REPORTS_DIR

logs_bp = Blueprint("logs_bp", __name__)

# ---------------------------------------------------------------------------
# Constantes e utilitários locais
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


# ---------------------------------------------------------------------------
# Rotas — Logs
# ---------------------------------------------------------------------------

@logs_bp.route("/api/logs")
def api_logs_list():
    return jsonify({"files": _list_log_files()})


@logs_bp.route("/api/logs/view/<path:filepath>")
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


@logs_bp.route("/api/logs/download/<path:filepath>")
def api_logs_download(filepath):
    fpath = _resolve_log_path(filepath)
    if not fpath:
        return jsonify({"error": "Arquivo não encontrado."}), 404
    return send_file(str(fpath), as_attachment=True)
