"""Solicitações TI — formulário que grava chamados no Google Sheets via Apps Script.

Página `solicitacoes_ti` (sidebar → seção TI). Qualquer usuário autenticado pode
abrir um chamado. O Flask atua APENAS como proxy: a gravação real é feita pelo
Google Apps Script (webhook publicado como App da Web), evitando CORS no browser
e dispensando OAuth do usuário final.

Endpoints
---------
POST /api/solicitacoes_ti/submit
    Recebe o ticket do front, repassa para o Apps Script webhook configurado
    em `TI_APPS_SCRIPT_URL`. Retorna o JSON do Apps Script (status/message).

GET  /api/solicitacoes_ti/config
    Devolve metadados públicos (sheet_id mascarado, sheet_name, se há webhook
    configurado). Não vaza a URL completa do webhook.

Variáveis de ambiente
---------------------
TI_APPS_SCRIPT_URL    URL do webhook do Apps Script (termina em /exec). REQUIRED.
TI_SHEET_ID           Spreadsheet ID. Default = ID da planilha de teste do zip.
TI_SHEET_NAME         Nome da aba. Default = "Solicitações".
TI_REQUEST_TIMEOUT_S  Timeout (segundos) da chamada HTTP. Default = 30.
"""
from __future__ import annotations

import json
import logging
import os
import random
from datetime import datetime
from typing import Any

import requests
from flask import Blueprint, jsonify, request, session

logger = logging.getLogger(__name__)
solicitacoes_ti_bp = Blueprint("solicitacoes_ti_bp", __name__)


DEFAULT_SHEET_ID = "1FvpQRTpb5I3-Pwtmf7LK3Zmk4nkSjQuOTqy06RWN5I0"
DEFAULT_SHEET_NAME = "Solicitações"


def _apps_script_url() -> str:
    return (os.getenv("TI_APPS_SCRIPT_URL") or "").strip()


def _sheet_id() -> str:
    return (os.getenv("TI_SHEET_ID") or DEFAULT_SHEET_ID).strip()


def _sheet_name() -> str:
    return (os.getenv("TI_SHEET_NAME") or DEFAULT_SHEET_NAME).strip()


def _timeout_s() -> float:
    raw = (os.getenv("TI_REQUEST_TIMEOUT_S") or "30").strip()
    try:
        return max(5.0, float(raw))
    except ValueError:
        return 30.0


def _require_auth():
    if not session.get("authenticated"):
        return jsonify({"status": "error", "message": "Não autenticado"}), 401
    return None


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/config", methods=["GET"])
def get_config():
    deny = _require_auth()
    if deny:
        return deny
    sid = _sheet_id()
    masked = (sid[:6] + "…" + sid[-4:]) if len(sid) > 12 else sid
    return jsonify({
        "ok": True,
        "sheet_id_masked": masked,
        "sheet_name": _sheet_name(),
        "webhook_configured": bool(_apps_script_url()),
        "default_solicitante": session.get("username", "") or "",
    })


def _build_ticket_payload(body: dict[str, Any]) -> dict[str, Any]:
    """Normaliza o payload do front pro formato esperado pelo Apps Script."""
    urgencia = (body.get("urgencia") or "Média").strip()
    if urgencia not in {"Baixa", "Média", "Alta", "Crítica"}:
        urgencia = "Média"
    ticket_id = f"CH-{random.randint(1000, 9999)}"
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return {
        "id": ticket_id,
        "timestamp": timestamp,
        "solicitante": (body.get("solicitante") or "").strip(),
        "setor": (body.get("setor") or "").strip(),
        "categoria": (body.get("categoria") or "").strip(),
        "urgencia": urgencia,
        "titulo": (body.get("titulo") or "").strip(),
        "descricao": (body.get("descricao") or "").strip(),
        "observacoes": (body.get("observacoes") or "").strip(),
        "status": "Pendente",
        "spreadsheetId": _sheet_id(),
        "sheetName": _sheet_name(),
    }


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/submit", methods=["POST"])
def submit_ticket():
    deny = _require_auth()
    if deny:
        return deny

    body = request.get_json(silent=True) or {}
    required = ["solicitante", "setor", "categoria", "titulo", "descricao"]
    missing = [f for f in required if not (body.get(f) or "").strip()]
    if missing:
        return jsonify({
            "status": "error",
            "message": f"Preencha os campos obrigatórios: {', '.join(missing)}",
        }), 400

    webhook = _apps_script_url()
    if not webhook:
        return jsonify({
            "status": "error",
            "message": "TI_APPS_SCRIPT_URL não configurado no .env. Avise o admin para colar a URL do Apps Script (termina em /exec).",
        }), 503

    ticket = _build_ticket_payload(body)

    try:
        resp = requests.post(
            webhook,
            json=ticket,
            timeout=_timeout_s(),
            allow_redirects=True,
            headers={"Content-Type": "application/json"},
        )
    except requests.exceptions.Timeout:
        logger.warning("solicitacoes_ti: timeout chamando Apps Script")
        return jsonify({"status": "error", "message": "Timeout ao chamar o Google Apps Script."}), 504
    except requests.exceptions.RequestException as e:
        logger.exception("solicitacoes_ti: erro de rede")
        return jsonify({"status": "error", "message": f"Erro de rede: {e}"}), 502

    raw = resp.text or ""
    is_html = raw.strip().startswith("<!DOCTYPE html>") or "<html" in raw or "Page Not Found" in raw or "Sorry, unable to open the file" in raw
    if is_html:
        return jsonify({
            "status": "error",
            "message": (
                "O Apps Script retornou uma página de erro do Google (provavelmente URL "
                "inválida, ID da planilha errado ou implantação antiga). Confira a URL "
                "publicada (termina em /exec) e o TI_SHEET_ID no .env."
            ),
        }), 400

    try:
        data = resp.json()
    except (ValueError, json.JSONDecodeError):
        data = {"status": "success_raw", "message": raw[:500]}

    # Devolve o ticket gerado pra UI mostrar/armazenar localmente
    data["ticket"] = {
        "id": ticket["id"],
        "timestamp": ticket["timestamp"],
        "solicitante": ticket["solicitante"],
        "setor": ticket["setor"],
        "categoria": ticket["categoria"],
        "urgencia": ticket["urgencia"],
        "titulo": ticket["titulo"],
        "descricao": ticket["descricao"],
        "observacoes": ticket["observacoes"],
        "status": ticket["status"],
    }

    status_code = resp.status_code if 200 <= resp.status_code < 600 else 200
    return jsonify(data), status_code
