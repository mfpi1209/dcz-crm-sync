"""Captacao Externa: persistencia dos leads no Supabase.

POST /api/captacao/lead
    Recebe o payload do formulario (mesmo que vai pro webhook n8n) e
    insere uma linha em public.captacao_leads via Supabase REST.

A tabela esta definida em sql/captacao_leads.sql e deve ser criada
manualmente no SQL Editor do Supabase antes do primeiro uso.
"""
from __future__ import annotations

import logging
import os

import requests
from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)
captacao_bp = Blueprint("captacao_bp", __name__)

SUPA_URL = (os.environ.get("SUPABASE_URL", "") or "").rstrip("/")
SUPA_KEY = os.environ.get("SUPABASE_KEY", "") or ""

ALLOWED_FIELDS = {
    "nome", "contato", "email", "nivel", "curso", "grau", "modalidade",
    "ingresso", "tipo", "usuario_logado", "promotor",
    "ensino_medio", "ano_em",
}
NULLABLE_PLACEHOLDERS = {"email", "nivel", "curso", "grau", "modalidade",
                         "ingresso", "usuario_logado", "promotor"}


def _normalize(data: dict) -> dict:
    row = {k: v for k, v in data.items() if k in ALLOWED_FIELDS}

    em = row.get("ensino_medio")
    if em is True or em == "sim":
        row["ensino_medio"] = True
    elif em is False or em == "nao":
        row["ensino_medio"] = False
    else:
        row["ensino_medio"] = None

    ano = row.get("ano_em")
    try:
        ano_int = int(ano) if ano not in (None, "", "null") else None
    except (TypeError, ValueError):
        ano_int = None
    row["ano_em"] = ano_int if ano_int in (1, 2, 3) else None

    for k in NULLABLE_PLACEHOLDERS:
        v = row.get(k)
        if v in ("---", "", None):
            row[k] = None
        elif isinstance(v, str):
            row[k] = v.strip() or None

    for k in ("nome", "contato"):
        v = row.get(k)
        row[k] = v.strip() if isinstance(v, str) else v

    return row


@captacao_bp.route("/api/captacao/lead", methods=["POST"])
def captacao_lead():
    if not SUPA_URL or not SUPA_KEY:
        logger.warning("captacao_lead: SUPABASE_URL/SUPABASE_KEY nao configurados")
        return jsonify({"ok": False, "error": "supabase nao configurado"}), 500

    data = request.get_json(silent=True) or {}
    nome = (data.get("nome") or "").strip()
    contato = (data.get("contato") or "").strip()
    tipo = data.get("tipo")
    if not nome or not contato or tipo not in ("promotor", "candidato"):
        return jsonify({"ok": False, "error": "payload invalido"}), 400

    row = _normalize(data)

    try:
        r = requests.post(
            f"{SUPA_URL}/rest/v1/captacao_leads",
            headers={
                "apikey": SUPA_KEY,
                "Authorization": f"Bearer {SUPA_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            json=row,
            timeout=10,
        )
    except requests.RequestException as e:
        logger.exception("captacao_lead: erro de rede")
        return jsonify({"ok": False, "error": str(e)}), 502

    if r.status_code >= 300:
        body = (r.text or "")[:300]
        logger.warning("captacao_lead: supabase %s -> %s", r.status_code, body)
        return jsonify({"ok": False, "error": body, "status": r.status_code}), 502

    return ("", 204)
