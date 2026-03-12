"""
Merge de leads duplicados via endpoint interno do Kommo.

Usa cookies de sessão obtidos do serviço Kommo_chat (Playwright)
para chamar o endpoint AJAX /ajax/leads/double/leads/save.

Fluxo:
  1. get_session_cookies()  → obtém cookies do Kommo_chat
  2. fetch_lead_full()      → busca dados completos do lead via API v4
  3. build_merge_payload()  → monta o form-data no formato interno
  4. merge_leads()          → POST no endpoint de merge
  5. poll_merge_status()    → aguarda conclusão do job assíncrono
"""

import os
import time
import json
import logging
from urllib.parse import urlencode

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("kommo_merge")

KOMMO_DB_DSN = dict(
    host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
    port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
    user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
    password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
    dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
)

KOMMO_CHAT_URL = os.getenv("KOMMO_CHAT_URL", "http://banco-kommo-dispatcher:8000")
KOMMO_WEB_URL = os.getenv("KOMMO_WEB_URL", "https://admamoeduitcombr.kommo.com")
KOMMO_BASE_URL = os.getenv("KOMMO_BASE_URL", "https://eduitbr.kommo.com")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")

MERGE_ENDPOINT = "/ajax/leads/double/leads/save"
STATUS_ENDPOINT = "/ajax/v1/multiactions/status"

POLL_INTERVAL = 2
POLL_MAX_ATTEMPTS = 30


# ---------------------------------------------------------------------------
# 1. Session cookies from Kommo_chat
# ---------------------------------------------------------------------------

def get_session_cookies(force_renew=False):
    """Obtém cookies de sessão web do Kommo via serviço Kommo_chat."""
    url = f"{KOMMO_CHAT_URL}/api/kommo/session"
    if force_renew:
        url = f"{KOMMO_CHAT_URL}/api/kommo/session/renew"

    try:
        method = "post" if force_renew else "get"
        resp = requests.request(method, url, timeout=120 if force_renew else 15)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("is_valid"):
            if not force_renew:
                log.warning("Sessão inválida, forçando renovação via Playwright...")
                return get_session_cookies(force_renew=True)
            log.error("Sessão continua inválida após renovação")
            return None

        cookies = data.get("cookies", {})
        if not cookies.get("session_id"):
            log.error("session_id ausente nos cookies")
            return None

        log.info("Cookies obtidos: %d cookies, session_id presente", len(cookies))
        return cookies

    except requests.RequestException as e:
        log.error("Erro ao obter cookies do Kommo_chat: %s", e)
        return None


# ---------------------------------------------------------------------------
# 2. Fetch lead data via Kommo API v4
# ---------------------------------------------------------------------------

def _api_v4_get(path, params=None):
    """GET na API v4 do Kommo (OAuth)."""
    url = f"{KOMMO_BASE_URL.rstrip('/')}{path}"
    headers = {
        "Authorization": f"Bearer {KOMMO_TOKEN}",
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code >= 400:
        return None
    return resp.json()


def fetch_lead_full(lead_id):
    """Busca lead com custom_fields e contacts via API v4, fallback para banco local."""
    data = _api_v4_get(f"/api/v4/leads/{lead_id}", {"with": "contacts"})
    if data:
        return data
    log.warning("API v4 falhou para lead %s, tentando banco local", lead_id)
    return _fetch_lead_from_db(lead_id)


def _fetch_lead_from_db(lead_id):
    """Busca lead do banco kommo_sync e converte para formato API v4."""
    try:
        conn = psycopg2.connect(**KOMMO_DB_DSN, connect_timeout=15)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, name, price, responsible_user_id, status_id, pipeline_id,
                   created_at, custom_fields_json, tags_json, contacts_json
            FROM leads WHERE id = %s
        """, (lead_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            log.error("Lead %s não encontrado no banco local", lead_id)
            return None

        cf_json = row.get("custom_fields_json")
        if isinstance(cf_json, str):
            cf_json = json.loads(cf_json)

        cf_values = []
        if isinstance(cf_json, list):
            for cf in cf_json:
                fid = cf.get("field_id")
                vals = cf.get("values", [])
                cf_values.append({
                    "field_id": fid,
                    "field_name": cf.get("field_name", ""),
                    "values": vals,
                })

        tags_json = row.get("tags_json")
        if isinstance(tags_json, str):
            tags_json = json.loads(tags_json)
        tags = tags_json if isinstance(tags_json, list) else []

        contacts_json = row.get("contacts_json")
        if isinstance(contacts_json, str):
            contacts_json = json.loads(contacts_json)
        contacts = contacts_json if isinstance(contacts_json, list) else []

        created_at = row.get("created_at")
        if isinstance(created_at, int):
            from datetime import datetime
            created_at = datetime.utcfromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")

        lead = {
            "id": row["id"],
            "name": row.get("name", ""),
            "price": row.get("price", 0),
            "responsible_user_id": row.get("responsible_user_id"),
            "status_id": row.get("status_id"),
            "pipeline_id": row.get("pipeline_id"),
            "created_at": created_at,
            "custom_fields_values": cf_values,
            "_embedded": {
                "tags": tags,
                "contacts": contacts,
            },
        }
        log.info("Lead %s carregado do banco local", lead_id)
        return lead
    except Exception as e:
        log.error("Erro ao buscar lead %s do banco: %s", lead_id, e)
        return None


def fetch_contact_full(contact_id):
    """Busca contato com custom_fields via API v4, fallback para banco local."""
    data = _api_v4_get(f"/api/v4/contacts/{contact_id}")
    if data:
        return data
    log.warning("API v4 falhou para contato %s, tentando banco local", contact_id)
    return _fetch_contact_from_db(contact_id)


def _fetch_contact_from_db(contact_id):
    """Busca contato do banco kommo_sync e converte para formato API v4."""
    try:
        conn = psycopg2.connect(**KOMMO_DB_DSN, connect_timeout=15)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, name, first_name, last_name, custom_fields_json
            FROM contacts WHERE id = %s
        """, (contact_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return None

        cf_json = row.get("custom_fields_json")
        if isinstance(cf_json, str):
            cf_json = json.loads(cf_json)

        cf_values = []
        if isinstance(cf_json, list):
            for cf in cf_json:
                cf_values.append({
                    "field_id": cf.get("field_id"),
                    "field_name": cf.get("field_name", ""),
                    "values": cf.get("values", []),
                })

        return {
            "id": row["id"],
            "name": row.get("name", ""),
            "first_name": row.get("first_name", ""),
            "last_name": row.get("last_name", ""),
            "custom_fields_values": cf_values,
        }
    except Exception as e:
        log.error("Erro ao buscar contato %s do banco: %s", contact_id, e)
        return None


# ---------------------------------------------------------------------------
# 3. Build merge payload
# ---------------------------------------------------------------------------

def _pick_best_value(val_keep, val_remove):
    """Escolhe o melhor valor entre os dois leads (prefere preenchido)."""
    if val_keep and str(val_keep).strip():
        return val_keep
    return val_remove


def _extract_cf_values(lead_data):
    """Extrai custom_fields_values do lead num dict {field_id: value}."""
    result = {}
    for cf in (lead_data.get("custom_fields_values") or []):
        fid = cf.get("field_id")
        vals = cf.get("values", [])
        if vals:
            if len(vals) == 1:
                result[fid] = vals[0].get("value", "")
            else:
                result[fid] = [v.get("value", "") for v in vals]
    return result


def _extract_contact_cf(contact_data):
    """Extrai custom_fields do contato no formato do merge payload."""
    result = {}
    for cf in (contact_data.get("custom_fields_values") or []):
        fid = cf.get("field_id")
        vals = cf.get("values", [])
        entries = []
        for v in vals:
            entries.append({
                "DESCRIPTION": v.get("enum_code", "WORK"),
                "VALUE": v.get("value", ""),
            })
        if entries:
            result[fid] = entries
    return result


def build_merge_payload(keep_lead, remove_lead, keep_contacts=None, remove_contacts=None):
    """
    Monta o payload form-encoded para o endpoint de merge.

    keep_lead / remove_lead: dados completos do lead (API v4 response).
    keep_contacts / remove_contacts: dados dos contatos associados.

    Retorna dict pronto para urlencode.
    """
    keep_id = keep_lead["id"]
    remove_id = remove_lead["id"]

    keep_cf = _extract_cf_values(keep_lead)
    remove_cf = _extract_cf_values(remove_lead)

    all_field_ids = set(keep_cf.keys()) | set(remove_cf.keys())
    merged_cf = {}
    for fid in all_field_ids:
        merged_cf[fid] = _pick_best_value(keep_cf.get(fid), remove_cf.get(fid))

    pairs = []

    pairs.append(("id[]", str(keep_id)))
    pairs.append(("id[]", str(remove_id)))

    pairs.append(("result_element[ID]", str(keep_id)))
    pairs.append(("result_element[DATE_CREATE]", keep_lead.get("created_at", "")))
    pairs.append(("result_element[MAIN_USER_ID]", str(keep_lead.get("responsible_user_id", ""))))
    pairs.append(("result_element[PRICE]", str(keep_lead.get("price", 0))))
    pairs.append(("result_element[STATUS]", str(keep_lead.get("status_id", ""))))
    pairs.append(("result_element[PIPELINE_ID]", str(keep_lead.get("pipeline_id", ""))))
    pairs.append(("result_element[NAME]", keep_lead.get("name", "")))

    tags = keep_lead.get("_embedded", {}).get("tags", [])
    for tag in tags:
        pairs.append(("result_element[TAGS][]", str(tag.get("id", ""))))

    for fid, val in merged_cf.items():
        if isinstance(val, list):
            for v in val:
                pairs.append((f"result_element[CFV][{fid}][]", str(v)))
        else:
            pairs.append((f"result_element[CFV][{fid}]", str(val)))

    if keep_contacts and remove_contacts:
        _add_contact_merge(pairs, keep_contacts, remove_contacts)

    return pairs


def _add_contact_merge(pairs, keep_contacts, remove_contacts):
    """Adiciona seção double[] ao payload para merge de contatos."""
    keep_ids = [c["id"] for c in (keep_contacts if isinstance(keep_contacts, list) else [keep_contacts])]
    remove_ids = [c["id"] for c in (remove_contacts if isinstance(remove_contacts, list) else [remove_contacts])]

    all_contact_ids = keep_ids + remove_ids
    if len(all_contact_ids) < 2:
        return

    main_contact = keep_contacts[0] if isinstance(keep_contacts, list) else keep_contacts
    contact_key = main_contact["id"]

    for cid in all_contact_ids:
        pairs.append((f"double[{contact_key}][id][]", str(cid)))

    pairs.append((f"double[{contact_key}][result_element][COMPANY_UID]", "0"))
    pairs.append((f"double[{contact_key}][result_element][NAME]",
                   main_contact.get("name", "")))

    keep_cf = _extract_contact_cf(main_contact)
    for fid, entries in keep_cf.items():
        for i, entry in enumerate(entries):
            pairs.append((f"double[{contact_key}][result_element][CFV][{fid}][{i}][DESCRIPTION]",
                           entry["DESCRIPTION"]))
            pairs.append((f"double[{contact_key}][result_element][CFV][{fid}][{i}][VALUE]",
                           entry["VALUE"]))


# ---------------------------------------------------------------------------
# 4. Execute merge
# ---------------------------------------------------------------------------

def merge_leads(payload_pairs, cookies):
    """
    Envia o merge ao Kommo via endpoint AJAX interno.

    payload_pairs: lista de tuplas [(key, value), ...] para form-encode.
    cookies: dict de cookies de sessão.

    Retorna dict com resultado.
    """
    url = f"{KOMMO_WEB_URL}{MERGE_ENDPOINT}"
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": KOMMO_WEB_URL,
    }

    keep_id = None
    for k, v in payload_pairs:
        if k == "result_element[ID]":
            keep_id = v
            break
    if keep_id:
        headers["Referer"] = f"{KOMMO_WEB_URL}/leads/detail/{keep_id}"

    body = urlencode(payload_pairs)

    log.info("POST %s (%d bytes)", MERGE_ENDPOINT, len(body))
    try:
        resp = requests.post(url, data=body, headers=headers, cookies=cookies, timeout=30)
    except requests.RequestException as e:
        return {"ok": False, "error": str(e)}

    log.info("Response: %d", resp.status_code)

    if resp.status_code == 401:
        log.warning("401 — sessão expirada, tentando renovar...")
        new_cookies = get_session_cookies(force_renew=True)
        if not new_cookies:
            return {"ok": False, "error": "Sessão expirada e renovação falhou"}
        resp = requests.post(url, data=body, headers=headers, cookies=new_cookies, timeout=30)
        log.info("Retry response: %d", resp.status_code)
        if resp.status_code >= 400:
            return {"ok": False, "error": f"HTTP {resp.status_code}", "body": resp.text[:500]}
        cookies = new_cookies

    if resp.status_code == 202:
        try:
            resp_data = resp.json()
        except Exception:
            resp_data = {}

        job_id = resp_data.get("job_id") or _extract_job_id(resp_data)

        if job_id:
            log.info("Merge aceito — job_id=%s, aguardando conclusão...", job_id)
            status = poll_merge_status(job_id, cookies)
            return {"ok": True, "job_id": job_id, "status": status}
        else:
            log.info("Merge aceito (202) sem job_id no response")
            return {"ok": True, "response": resp_data}

    if resp.status_code < 400:
        return {"ok": True, "response": resp.json() if resp.text.strip() else {}}

    return {"ok": False, "error": f"HTTP {resp.status_code}", "body": resp.text[:500]}


def _extract_job_id(data):
    """Tenta extrair job_id de formatos variados de resposta."""
    if isinstance(data, dict):
        for key in ("job_id", "jobId", "id"):
            if key in data:
                return data[key]
        multiactions = data.get("multiactions", {})
        if isinstance(multiactions, dict):
            for k, v in multiactions.items():
                if isinstance(v, dict) and "job_id" in v:
                    return v["job_id"]
    return None


# ---------------------------------------------------------------------------
# 5. Poll merge status
# ---------------------------------------------------------------------------

def poll_merge_status(job_id, cookies):
    """
    Faz polling do status do job de merge até concluir ou timeout.
    """
    url = f"{KOMMO_WEB_URL}{STATUS_ENDPOINT}"
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    params = {
        "request[multiactions][status][0][job_id]": str(job_id),
    }

    for attempt in range(POLL_MAX_ATTEMPTS):
        time.sleep(POLL_INTERVAL)
        try:
            resp = requests.get(url, params=params, headers=headers,
                                cookies=cookies, timeout=15)
            if resp.status_code != 200:
                log.warning("Poll attempt %d: HTTP %d", attempt + 1, resp.status_code)
                continue

            data = resp.json()
            log.debug("Poll attempt %d: %s", attempt + 1, data)

            status = _check_job_status(data)
            if status in ("completed", "done", "success"):
                log.info("Merge concluído após %d polls", attempt + 1)
                return "completed"
            if status in ("error", "failed"):
                log.error("Merge falhou: %s", data)
                return "failed"

        except requests.RequestException as e:
            log.warning("Poll attempt %d erro: %s", attempt + 1, e)

    log.warning("Timeout após %d polls", POLL_MAX_ATTEMPTS)
    return "timeout"


def _check_job_status(data):
    """Interpreta o response de status do multiactions."""
    if isinstance(data, dict):
        multiactions = data.get("response", data).get("multiactions", {})
        status_info = multiactions.get("status", [])
        if isinstance(status_info, list):
            for item in status_info:
                if isinstance(item, dict):
                    s = item.get("status", "")
                    if s:
                        return s
                    if item.get("complete") or item.get("done"):
                        return "completed"
    return "pending"


# ---------------------------------------------------------------------------
# 6. High-level merge function
# ---------------------------------------------------------------------------

def merge_lead_pair(keep_id, remove_id):
    """
    Merge completo de dois leads: busca dados, monta payload, executa.

    keep_id: ID do lead que sobrevive.
    remove_id: ID do lead que será absorvido.

    Retorna dict com resultado.
    """
    log.info("=== Merge de leads: manter=%s, remover=%s ===", keep_id, remove_id)

    cookies = get_session_cookies()
    if not cookies:
        return {"ok": False, "error": "Não foi possível obter cookies de sessão"}

    keep_lead = fetch_lead_full(keep_id)
    if not keep_lead:
        return {"ok": False, "error": f"Lead {keep_id} não encontrado"}

    remove_lead = fetch_lead_full(remove_id)
    if not remove_lead:
        return {"ok": False, "error": f"Lead {remove_id} não encontrado"}

    keep_contacts = _get_lead_contacts(keep_lead)
    remove_contacts = _get_lead_contacts(remove_lead)

    if keep_contacts:
        keep_contacts = [fetch_contact_full(c["id"]) for c in keep_contacts]
        keep_contacts = [c for c in keep_contacts if c]
    if remove_contacts:
        remove_contacts = [fetch_contact_full(c["id"]) for c in remove_contacts]
        remove_contacts = [c for c in remove_contacts if c]

    payload = build_merge_payload(
        keep_lead, remove_lead,
        keep_contacts=keep_contacts,
        remove_contacts=remove_contacts,
    )

    log.info("Payload: %d pares", len(payload))
    result = merge_leads(payload, cookies)

    if result.get("ok"):
        log.info("Merge concluído com sucesso")
    else:
        log.error("Merge falhou: %s", result.get("error", ""))

    return result


def _get_lead_contacts(lead_data):
    """Extrai lista de contatos do _embedded do lead."""
    embedded = lead_data.get("_embedded", {})
    return embedded.get("contacts", [])
