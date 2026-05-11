"""Painel "Meus Atendimentos" — agrega feedback de consultores (webhook n8n).

Mesma hierarquia da Minha Performance / Distribuição por Consultor:
  - Admin → vê tudo.
  - Supervisor Acadêmico → vê tudo (pode filtrar pelo consultor).
  - Demais → só veem seus próprios dados.

A página reaproveita o webhook `feedback` do n8n (mesma fonte da página
Feedback Comercial), que devolve métricas por consultor: total_atendimentos,
nota_media, tempo_medio_resposta_min, tempo_medio_atendimento_min, serie_dia.
"""

import os
import logging

import requests
from flask import Blueprint, request, jsonify, session

from routes.comercial_rgm import (
    _dist_consultor_kommo_uid_for_session,
    _dist_consultor_name_for_kommo_uid,
)

logger = logging.getLogger(__name__)

meus_atendimentos_bp = Blueprint("meus_atendimentos", __name__)

FB_WEBHOOK_URL = os.getenv(
    "FB_WEBHOOK_URL",
    "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/feedback",
)


def _ma_categoria() -> str:
    return (session.get("categoria") or "").strip().lower()


def _ma_is_supervisor_academico() -> bool:
    return _ma_categoria() in ("supervisor acadêmico", "supervisor academico")


def _ma_is_privileged() -> bool:
    """Admin global OU Supervisor Acadêmico vê dados de todos os consultores."""
    return session.get("role") == "admin" or _ma_is_supervisor_academico()


def _ma_session_identity() -> dict:
    """Identidade do usuário logado para o painel."""
    privileged = _ma_is_privileged()
    uid = _dist_consultor_kommo_uid_for_session()
    nome = None if privileged else _dist_consultor_name_for_kommo_uid(uid)
    return {
        "is_admin": privileged,
        "kommo_user_id": uid,
        "consultor_nome": nome,
        "categoria": session.get("categoria") or None,
    }


@meus_atendimentos_bp.route("/api/meus-atendimentos/me")
def meus_atendimentos_me():
    """Identidade do usuário para o painel (mesma forma do /api/dist-consultor/me)."""
    info = _ma_session_identity()
    return jsonify({
        "ok": True,
        "is_admin": info["is_admin"],
        "kommo_user_id": info["kommo_user_id"],
        "consultor_nome": info["consultor_nome"],
        "categoria": info["categoria"],
    })


@meus_atendimentos_bp.route("/api/meus-atendimentos")
def meus_atendimentos_data():
    """Proxy do webhook n8n /feedback com ACL.

    Query params suportados (repassa para o webhook):
        start, end, consultor, tipo, topN
    """
    info = _ma_session_identity()
    consultor_arg = (request.args.get("consultor") or "").strip()

    if info["is_admin"]:
        consultor = consultor_arg or None
    else:
        nome = info["consultor_nome"]
        if not nome:
            return jsonify({
                "ok": True,
                "forced": True,
                "consultor": None,
                "global": {},
                "consultores": [],
                "consultor_detalhe": None,
                "_acl_message": "Usuário sem kommo_user_id mapeado.",
            })
        consultor = nome

    params: dict = {}
    for k in ("start", "end", "tipo"):
        v = (request.args.get(k) or "").strip()
        if v:
            params[k] = v
    params["topN"] = request.args.get("topN") or 5
    if consultor:
        params["consultor"] = consultor

    last_exc: Exception | None = None
    for attempt in (1, 2):
        try:
            # 1ª tentativa: 60s; 2ª: 90s (timeouts do n8n são frequentes em janelas grandes).
            timeout = 60 if attempt == 1 else 90
            r = requests.get(FB_WEBHOOK_URL, params=params, timeout=timeout, verify=False)
            r.raise_for_status()
            try:
                data = r.json()
            except ValueError:
                data = {"raw": r.text}
            if not isinstance(data, dict):
                data = {"raw": data}
            data.setdefault("ok", True)
            data["_acl_consultor"] = consultor
            data["_acl_forced"] = not info["is_admin"]
            data["_attempt"] = attempt
            return jsonify(data)
        except requests.Timeout as e:
            last_exc = e
            logger.warning("meus-atendimentos webhook timeout (try %d/2): %s", attempt, e)
            continue
        except requests.RequestException as e:
            last_exc = e
            logger.warning("meus-atendimentos webhook proxy: %s", e)
            break

    msg = str(last_exc) if last_exc else "Falha no webhook"
    return jsonify({
        "ok": False,
        "error": msg,
        "hint": "O webhook do n8n demorou para responder. Tente um período menor (até 7 dias) ou filtre por consultor.",
    }), 502
