"""Tracker de Tarefas - TI: CRUD na tabela public.tracker_tarefas_ti.

Mesma conexão Supabase das Inscrições (SUPABASE_INSCRICOES_URL/KEY).

GET    /api/tracker-tarefas          lista todas (order id)
POST   /api/tracker-tarefas          cria {data_inicio, responsavel, descricao, mes, data_final, status}
PATCH  /api/tracker-tarefas/<id>     atualiza campos enviados
DELETE /api/tracker-tarefas/<id>     remove
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

from flask import Blueprint, jsonify, request, session

logger = logging.getLogger(__name__)
tracker_tarefas_bp = Blueprint("tracker_tarefas_bp", __name__)

_TABLE = "tracker_tarefas_ti"
_STATUS_VALIDOS = ("Pendente", "Em andamento", "Concluído")
_CAMPOS_EDITAVEIS = ("data_inicio", "responsavel", "descricao", "mes", "data_final", "status")


def _cfg() -> tuple[str, str]:
    base = (os.getenv("SUPABASE_INSCRICOES_URL") or "").rstrip("/")
    key = os.getenv("SUPABASE_INSCRICOES_KEY") or ""
    if not base or not key:
        raise RuntimeError("SUPABASE_INSCRICOES_URL/SUPABASE_INSCRICOES_KEY não configurados no .env")
    return base, key


def _sb(method: str, path: str, body: dict | None = None, prefer: str | None = None):
    base, key = _cfg()
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "dcz-crm-sync/1.0",
    }
    if prefer:
        headers["Prefer"] = prefer
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        f"{base}/rest/v1/{path}", data=payload, headers=headers, method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else None


def _forbidden():
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return None


def _s(v) -> str:
    return str(v).strip() if v is not None else ""


@tracker_tarefas_bp.route("/api/tracker-tarefas", methods=["GET"])
def api_list():
    fb = _forbidden()
    if fb:
        return fb
    try:
        rows = _sb("GET", f"{_TABLE}?select=*&order=id.desc")
        return jsonify({"ok": True, "rows": rows or []})
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:400]
        logger.exception("tracker_tarefas GET HTTP %s", e.code)
        return jsonify({"ok": False, "error": f"Supabase HTTP {e.code}: {body}"}), 502
    except Exception as e:
        logger.exception("tracker_tarefas GET")
        return jsonify({"ok": False, "error": str(e)}), 500


@tracker_tarefas_bp.route("/api/tracker-tarefas", methods=["POST"])
def api_create():
    fb = _forbidden()
    if fb:
        return fb
    body = request.get_json(silent=True) or {}
    descricao = _s(body.get("descricao"))
    if not descricao:
        return jsonify({"ok": False, "error": "informe a descrição"}), 400
    status = _s(body.get("status")) or "Pendente"
    if status not in _STATUS_VALIDOS:
        return jsonify({"ok": False, "error": "status inválido"}), 400
    row = {
        "data_inicio": _s(body.get("data_inicio")),
        "responsavel": _s(body.get("responsavel")),
        "descricao": descricao,
        "mes": _s(body.get("mes")),
        "data_final": _s(body.get("data_final")),
        "status": status,
    }
    try:
        data = _sb("POST", _TABLE, body=row, prefer="return=representation")
        created = data[0] if isinstance(data, list) and data else data
        return jsonify({"ok": True, "row": created})
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", "replace")[:400]
        logger.exception("tracker_tarefas POST HTTP %s", e.code)
        return jsonify({"ok": False, "error": f"Supabase HTTP {e.code}: {body_txt}"}), 502
    except Exception as e:
        logger.exception("tracker_tarefas POST")
        return jsonify({"ok": False, "error": str(e)}), 500


@tracker_tarefas_bp.route("/api/tracker-tarefas/<int:tid>", methods=["PATCH"])
def api_update(tid: int):
    fb = _forbidden()
    if fb:
        return fb
    body = request.get_json(silent=True) or {}
    patch = {}
    for campo in _CAMPOS_EDITAVEIS:
        if campo in body:
            patch[campo] = _s(body.get(campo))
    if "status" in patch and patch["status"] not in _STATUS_VALIDOS:
        return jsonify({"ok": False, "error": "status inválido"}), 400
    if "descricao" in patch and not patch["descricao"]:
        return jsonify({"ok": False, "error": "descrição não pode ficar vazia"}), 400
    if not patch:
        return jsonify({"ok": False, "error": "nada para atualizar"}), 400
    try:
        data = _sb("PATCH", f"{_TABLE}?id=eq.{tid}", body=patch, prefer="return=representation")
        updated = data[0] if isinstance(data, list) and data else None
        if not updated:
            return jsonify({"ok": False, "error": "tarefa não encontrada"}), 404
        return jsonify({"ok": True, "row": updated})
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", "replace")[:400]
        logger.exception("tracker_tarefas PATCH HTTP %s", e.code)
        return jsonify({"ok": False, "error": f"Supabase HTTP {e.code}: {body_txt}"}), 502
    except Exception as e:
        logger.exception("tracker_tarefas PATCH")
        return jsonify({"ok": False, "error": str(e)}), 500


@tracker_tarefas_bp.route("/api/tracker-tarefas/<int:tid>", methods=["DELETE"])
def api_delete(tid: int):
    fb = _forbidden()
    if fb:
        return fb
    try:
        _sb("DELETE", f"{_TABLE}?id=eq.{tid}", prefer="return=minimal")
        return jsonify({"ok": True})
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode("utf-8", "replace")[:400]
        logger.exception("tracker_tarefas DELETE HTTP %s", e.code)
        return jsonify({"ok": False, "error": f"Supabase HTTP {e.code}: {body_txt}"}), 502
    except Exception as e:
        logger.exception("tracker_tarefas DELETE")
        return jsonify({"ok": False, "error": str(e)}), 500
