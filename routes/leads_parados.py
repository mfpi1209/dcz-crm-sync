"""
eduit. — Leads Parados: leads em atendimento sem interação há mais de 1 hora.

Consulta a API do Kommo em tempo real (não depende de sync local).
"""

import os
import time as _time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests as _requests
from dotenv import load_dotenv
from flask import Blueprint, jsonify, request

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logger = logging.getLogger(__name__)

leads_parados_bp = Blueprint("leads_parados_bp", __name__)

KOMMO_BASE_URL = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")

PIPELINE_ID = 5481944
STATUS_ID = 48539246
THRESHOLD_SECONDS = 3600


def _kommo_get(path, params=None):
    base = KOMMO_BASE_URL.rstrip("/")
    if "/api/v4" not in base:
        url = f"{base}/api/v4{path}"
    else:
        url = f"{base}{path}"
    headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
    return _requests.get(url, headers=headers, params=params, timeout=30)


def _fetch_leads_em_atendimento():
    """Fetch all leads in 'Em Atendimento' from the Kommo API v4."""
    all_leads = []
    seen_ids = set()
    page = 1

    while True:
        params = {
            "limit": 250,
            "page": page,
            "with": "contacts",
            "filter[statuses][0][pipeline_id]": PIPELINE_ID,
            "filter[statuses][0][status_id]": STATUS_ID,
        }
        try:
            r = _kommo_get("/leads", params)
        except Exception as e:
            logger.error("Kommo API error: %s", e)
            break

        if r.status_code != 200:
            logger.warning("Kommo API %d: %s", r.status_code, r.text[:200])
            break

        data = r.json()
        leads = data.get("_embedded", {}).get("leads", [])
        if not leads:
            break

        for lead in leads:
            lid = lead.get("id")
            if lid and lid not in seen_ids:
                seen_ids.add(lid)
                all_leads.append(lead)

        if "next" not in data.get("_links", {}):
            break
        page += 1
        _time.sleep(0.05)

    return all_leads


def _fetch_user_names(user_ids):
    """Fetch user names from the Kommo API."""
    if not user_ids or not KOMMO_TOKEN:
        return {}
    try:
        r = _kommo_get("/users", {"limit": 250})
        if r.status_code == 200:
            users = r.json().get("_embedded", {}).get("users", [])
            return {u["id"]: u["name"] for u in users if u["id"] in user_ids}
    except Exception as e:
        logger.warning("Failed to fetch Kommo users: %s", e)
    return {}


@leads_parados_bp.route("/api/leads-parados")
def api_leads_parados():
    if not KOMMO_TOKEN:
        return jsonify({"error": "KOMMO_TOKEN não configurado"}), 500

    try:
        raw_leads = _fetch_leads_em_atendimento()
    except Exception as e:
        logger.exception("Erro ao buscar leads da API Kommo")
        return jsonify({"error": str(e)}), 500

    horas = request.args.get("horas", 1, type=int)
    threshold = max(horas, 1) * 3600

    now_ts = int(_time.time())
    parados = []

    for lead in raw_leads:
        updated_at = lead.get("updated_at", 0)
        diff = now_ts - updated_at
        if diff < threshold:
            continue
        contacts = lead.get("_embedded", {}).get("contacts", [])
        contact_id = contacts[0]["id"] if contacts else None
        parados.append({
            "id": lead.get("id"),
            "name": lead.get("name") or "(sem nome)",
            "responsible_user_id": lead.get("responsible_user_id"),
            "contact_id": contact_id,
            "updated_at_ts": updated_at,
            "segundos_parado": diff,
        })

    user_ids = set(l["responsible_user_id"] for l in parados if l["responsible_user_id"])
    user_names = _fetch_user_names(user_ids)

    BRT = timezone(timedelta(hours=-3))
    result = []
    for lead in sorted(parados, key=lambda x: x["segundos_parado"], reverse=True):
        seg = lead["segundos_parado"]
        horas = seg // 3600
        minutos = (seg % 3600) // 60

        dt_brt = datetime.fromtimestamp(lead["updated_at_ts"], tz=BRT)

        result.append({
            "id": lead["id"],
            "name": lead["name"],
            "responsible_user_id": lead["responsible_user_id"],
            "contact_id": lead["contact_id"],
            "consultor": user_names.get(lead["responsible_user_id"], "—"),
            "updated_at": dt_brt.strftime("%d/%m/%Y %H:%M"),
            "tempo_parado": f"{horas}h {minutos}min",
            "segundos_parado": seg,
        })

    return jsonify({"leads": result, "total": len(result)})


DISTRIBUIR_WEBHOOK = "https://banco-dev-n8n-eduit.6tqx2r.easypanel.host/webhook/distribuir_leads"


@leads_parados_bp.route("/api/leads-parados/distribuir", methods=["POST"])
def api_distribuir_leads():
    try:
        body = request.get_json(silent=True) or {}
        horas = body.get("horas", 1)
        threshold = max(int(horas), 1) * 3600

        raw_leads = _fetch_leads_em_atendimento()
        now_ts = int(_time.time())
        leads_data = []
        for l in raw_leads:
            if now_ts - l.get("updated_at", 0) >= threshold:
                contacts = l.get("_embedded", {}).get("contacts", [])
                leads_data.append({
                    "lead_id": l["id"],
                    "contact_id": contacts[0]["id"] if contacts else None,
                    "responsible_user_id": l.get("responsible_user_id"),
                })

        r = _requests.post(DISTRIBUIR_WEBHOOK, json={"leads": leads_data}, timeout=600)
        r.raise_for_status()
        n8n_body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {"message": r.text}
        return jsonify({"ok": True, "total": len(leads_data), "n8n": n8n_body})
    except Exception as e:
        logger.exception("Erro ao chamar webhook de distribuição")
        return jsonify({"error": str(e)}), 502
