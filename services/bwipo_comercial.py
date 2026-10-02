"""Cliente HTTP do CRM comercial Bwipo (org Comercial Cruzeiro).

Não confundir com o CRM acadêmico (cruzeiro-ead / tool_whatsapp_alunos).
Auth: Bearer `BWIPO_COMERCIAL_API_TOKEN` (prefixo eduit_).
API: https://integrations.bwipo.com
Front: https://comercialcruzeiro.bwipo.com
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DEFAULT_BASE = "https://integrations.bwipo.com"
_DEFAULT_WEB = "https://comercialcruzeiro.bwipo.com"
_TIMEOUT = 25

# Nomes que o dashboard comercial / Match&Merge já leem no Kommo.
# Inclui slugs reais do dealPanelFields da org Comercial Cruzeiro.
# A lista da API manda {customFieldId, value} sem nome. Estes ids são da org Comercial Cruzeiro.
_FIELD_IDS = {
    "cmu2wz3qt0eqrqn01gv5p1vcr": "rgm",
    "cmu2wwmld0burqn01gu4w1sxm": "cpf",
    "cmu2wyu8l0efdqn01kfew46tt": "data_matricula",
    "cmub9ys7k2wxhld01zvujuuiu": "situacao",
    "cmu2ww7lk0be9qn01q3re0hem": "curso",
    "cmub9yel82wunld01uru5t6b0": "polo",
    "cmu2wx03m0cajqn01k714l0id": "email",
    "cmu2wzdv80fb3qn01e3xaii0b": "email",
    "cmu2wrsvf069zqn01ur6wmqjv": "nro_inscricao",
    "cmu2wxfv80cu3qn01jzzi19lj": "telefone",
}

_FIELD_ALIASES = {
    "rgm": ("rgm",),
    "cpf": ("cpf",),
    "data_matricula": ("data matricula", "data de matricula", "data_de_matricula", "matricula", "matrícula"),
    "situacao": ("situacao", "situação"),
    "curso": ("curso_de_inscricao", "curso de inscricao", "curso_siaa", "curso"),
    "polo": ("polo_de_inscricao", "polo de inscricao", "polo"),
    "origem": ("origem",),
    "email": ("e_mail_academico", "e-mail academico", "email academico", "e_mail", "e-mail", "email"),
    "telefone": ("telefone_da_inscricao", "telefone inscricao", "telefone inscrição", "telefone"),
    "marca": ("marca",),
    "modalidade": ("modalidade_curso1", "modalidade", "modalidade_siaa"),
    "grau": ("grau_new", "grau", "grau_siaa"),
    "nro_inscricao": ("nro_da_inscricao", "nro. da inscricao", "nro da inscricao", "inscricao", "inscrição"),
}


class BwipoComercialError(RuntimeError):
    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


def api_base() -> str:
    return (os.getenv("BWIPO_COMERCIAL_API_BASE_URL") or _DEFAULT_BASE).rstrip("/")


def web_base() -> str:
    return (os.getenv("BWIPO_COMERCIAL_WEB_URL") or _DEFAULT_WEB).rstrip("/")


def api_token() -> str:
    return (os.getenv("BWIPO_COMERCIAL_API_TOKEN") or "").strip()


def configured() -> bool:
    return bool(api_token())


def page_sleep() -> float:
    try:
        return max(0.2, min(2.0, float(os.getenv("BWIPO_COMERCIAL_PAGE_SLEEP", "0.45"))))
    except (TypeError, ValueError):
        return 0.45


def digits_only(raw: Any) -> str:
    return re.sub(r"\D+", "", str(raw or ""))


def phone_norm(raw: Any) -> Optional[str]:
    d = digits_only(raw)
    if not d:
        return None
    if d.startswith("55") and len(d) >= 12:
        d = d[2:]
    if len(d) >= 10:
        return d[-11:]
    return d or None


def email_norm(raw: Any) -> Optional[str]:
    v = str(raw or "").strip().lower()
    return v or None


def cpf_norm(raw: Any) -> Optional[str]:
    d = digits_only(raw)
    if not d:
        return None
    if 9 <= len(d) <= 10:
        d = d.zfill(11)
    return d if len(d) == 11 else None


def rgm_norm(raw: Any) -> Optional[str]:
    d = digits_only(raw)
    return d if len(d) == 8 else (d or None)


def _fold(s: str) -> str:
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def request(method: str, path: str, body: Any = None, timeout: int = _TIMEOUT) -> Any:
    token = api_token()
    if not token:
        raise BwipoComercialError("BWIPO_COMERCIAL_API_TOKEN não configurado no .env", 503)
    url = api_base() + path
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "dcz-crm-sync/bwipo-comercial",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        msg = raw
        try:
            parsed = json.loads(raw)
            msg = parsed.get("message") or parsed.get("error") or raw
        except Exception:
            pass
        logger.warning("bwipo_comercial %s %s → %s %s", method, path, e.code, str(msg)[:300])
        raise BwipoComercialError(str(msg) or f"CRM HTTP {e.code}", 502 if e.code >= 500 else e.code)
    except urllib.error.URLError as e:
        raise BwipoComercialError(f"Falha ao conectar no Bwipo: {e.reason}", 502) from e


def _items(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        val = payload.get("items")
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
    return []


def list_page(path: str, page: int = 1, per_page: int = 100, extra: Optional[dict] = None) -> dict:
    params = {"page": str(page), "perPage": str(per_page)}
    if extra:
        for k, v in extra.items():
            if v is not None and v != "":
                params[str(k)] = str(v)
    qs = urllib.parse.urlencode(params)
    raw = request("GET", f"{path}?{qs}")
    if not isinstance(raw, dict):
        items = _items(raw)
        return {"items": items, "total": len(items), "page": page, "perPage": per_page, "totalPages": 1}
    items = _items(raw)
    return {
        "items": items,
        "total": int(raw.get("total") or len(items) or 0),
        "page": int(raw.get("page") or page),
        "perPage": int(raw.get("perPage") or per_page),
        "totalPages": int(raw["totalPages"]) if raw.get("totalPages") is not None else None,
    }


def list_pipelines() -> list[dict]:
    raw = request("GET", "/api/pipelines")
    return _items(raw) if not isinstance(raw, list) else [x for x in raw if isinstance(x, dict)]


def _cf_iter(payload: Any):
    if isinstance(payload, dict):
        for k, v in payload.items():
            if isinstance(v, dict):
                yield {
                    "id": v.get("id") or v.get("fieldId") or k,
                    "name": v.get("name") or v.get("label") or k,
                    "value": v.get("value") if "value" in v else v,
                }
            else:
                yield {"id": k, "name": k, "value": v}
    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            yield {
                "id": item.get("id") or item.get("fieldId") or item.get("customFieldId"),
                "name": item.get("name") or item.get("label") or item.get("slug") or "",
                "value": item.get("value") if "value" in item else item.get("values"),
            }


def extract_known_fields(custom_fields: Any, extra: Any = None) -> dict:
    """Achata custom fields / dealPanelFields nos nomes que o painel já usa."""
    out: dict[str, Any] = {}
    bags = [custom_fields]
    if extra is not None:
        bags.append(extra)
    for bag in bags:
        for item in _cf_iter(bag):
            name = _fold(str(item.get("name") or ""))
            val = item.get("value")
            if isinstance(val, list) and val:
                first = val[0]
                val = first.get("value") if isinstance(first, dict) else first
            if val is None or val == "":
                continue
            text = str(val).strip()
            if not text:
                continue
            fid = str(item.get("id") or "")
            mapped = _FIELD_IDS.get(fid)
            if mapped and mapped not in out:
                out[mapped] = text
            if not name:
                continue
            folded_space = name.replace("_", " ")
            matched = False
            for key, aliases in _FIELD_ALIASES.items():
                if key in out:
                    continue
                if any(name == a or folded_space == a for a in aliases):
                    out[key] = text
                    matched = True
                    break
            if matched:
                continue
            for key, aliases in _FIELD_ALIASES.items():
                if key in out:
                    continue
                if any(len(a) >= 4 and a in name for a in aliases):
                    out[key] = text
                    break
    if out.get("rgm"):
        out["rgm_norm"] = rgm_norm(out["rgm"])
    if out.get("cpf"):
        out["cpf_norm"] = cpf_norm(out["cpf"])
    return out


def flatten_deal(deal: dict) -> dict:
    contact = deal.get("contact") if isinstance(deal.get("contact"), dict) else {}
    stage = deal.get("stage") if isinstance(deal.get("stage"), dict) else {}
    pipeline = stage.get("pipeline") if isinstance(stage.get("pipeline"), dict) else {}
    owner = deal.get("owner") if isinstance(deal.get("owner"), dict) else {}
    known = extract_known_fields(
        deal.get("customFields") or deal.get("custom_fields"),
        deal.get("dealPanelFields") or deal.get("deal_panel_fields"),
    )
    phone = contact.get("phone") or known.get("telefone")
    email = contact.get("email") or known.get("email")
    return {
        "id": deal.get("id"),
        "number": deal.get("number"),
        "title": deal.get("title") or "",
        "value": deal.get("value"),
        "status": deal.get("status") or "OPEN",
        "deal_role": deal.get("dealRole") or deal.get("deal_role"),
        "contact_id": deal.get("contactId") or contact.get("id"),
        "stage_id": deal.get("stageId") or stage.get("id"),
        "stage_name": stage.get("name"),
        "stage_slug": stage.get("slug"),
        "pipeline_id": pipeline.get("id") or stage.get("pipelineId"),
        "pipeline_name": pipeline.get("name"),
        "owner_id": deal.get("ownerId") or owner.get("id"),
        "owner_name": owner.get("name") or owner.get("email"),
        "owner_email": owner.get("email"),
        "lost_reason": deal.get("lostReason"),
        "contact_name": contact.get("name"),
        "contact_phone": phone,
        "contact_email": email,
        "phone_norm": phone_norm(phone),
        "email_norm": email_norm(email),
        "rgm_norm": known.get("rgm_norm"),
        "cpf_norm": known.get("cpf_norm"),
        "data_matricula": known.get("data_matricula"),
        "situacao": known.get("situacao"),
        "curso": known.get("curso"),
        "polo": known.get("polo"),
        "origem": known.get("origem"),
        "known_fields": known,
        "custom_fields": deal.get("customFields") or deal.get("custom_fields") or [],
        "created_at": deal.get("createdAt") or deal.get("created_at"),
        "updated_at": deal.get("updatedAt") or deal.get("updated_at"),
        "closed_at": deal.get("closedAt") or deal.get("closed_at"),
        "raw": deal,
    }


def flatten_contact(contact: dict) -> dict:
    assigned = contact.get("assignedTo") if isinstance(contact.get("assignedTo"), dict) else {}
    known = extract_known_fields(contact.get("customFields") or contact.get("custom_fields"))
    phone = contact.get("phone") or known.get("telefone")
    email = contact.get("email") or known.get("email")
    return {
        "id": contact.get("id"),
        "number": contact.get("number"),
        "name": contact.get("name") or "",
        "email": email,
        "phone": phone,
        "phone_norm": phone_norm(phone),
        "email_norm": email_norm(email),
        "source": contact.get("source"),
        "assigned_to_id": contact.get("assignedToId") or assigned.get("id"),
        "assigned_to_name": assigned.get("name") or assigned.get("email"),
        "rgm_norm": known.get("rgm_norm"),
        "cpf_norm": known.get("cpf_norm"),
        "known_fields": known,
        "custom_fields": contact.get("customFields") or contact.get("custom_fields") or {},
        "created_at": contact.get("createdAt") or contact.get("created_at"),
        "updated_at": contact.get("updatedAt") or contact.get("updated_at"),
        "raw": contact,
    }


def sleep_page() -> None:
    time.sleep(page_sleep())


def delta_since_iso(last_sync_at, slack_s: int = 300, lookback_days: int = 7) -> str:
    """Janela do Incremental, no mesmo espírito do Kommo (folga + teto de dias)."""
    if isinstance(last_sync_at, str):
        dt = datetime.fromisoformat(last_sync_at.replace("Z", "+00:00"))
    else:
        dt = last_sync_at
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        slack = max(0, int(os.getenv("BWIPO_DELTA_SLACK_S", str(slack_s))))
    except (TypeError, ValueError):
        slack = slack_s
    try:
        lookback = int(os.getenv("BWIPO_DELTA_LOOKBACK_DAYS", str(lookback_days)))
    except (TypeError, ValueError):
        lookback = lookback_days
    dt = dt - timedelta(seconds=slack)
    if lookback > 0:
        floor = datetime.now(timezone.utc) - timedelta(days=lookback)
        if dt < floor:
            dt = floor
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def get_deal(deal_id: str) -> dict:
    raw = request("GET", f"/api/deals/{deal_id}")
    if isinstance(raw, dict) and isinstance(raw.get("deal"), dict):
        return raw["deal"]
    return raw if isinstance(raw, dict) else {}


def get_contact(contact_id: str) -> dict:
    raw = request("GET", f"/api/contacts/{contact_id}")
    if isinstance(raw, dict) and isinstance(raw.get("contact"), dict):
        return raw["contact"]
    return raw if isinstance(raw, dict) else {}


# Pipeline Principal do comercial. O funil da home conta só estas etapas.
PIPELINE_PRINCIPAL_ID = "cmu2tx2f600h1qo018c7r0alx"

# Ordem do Kanban do Pipeline Principal. Ganho e Perdido ficam fora do total de ativos.
_FUNNEL_ROWS = (
    ("sem resposta", "sem_resposta", "Sem Resposta", True),
    ("em atendimento", "em_atendimento", "Em Atendimento", True),
    ("aguardando resposta", "aguardando_resposta", "Aguardando Resposta", True),
    ("aguardando inscricao", "aguardando_inscricao", "Aguardando Inscrição", True),
    ("inscricao", "inscricao", "Inscrição", True),
    ("processo seletivo", "processo_seletivo", "Processo Seletivo", True),
    ("em processo", "em_processo", "Em Processo", True),
    ("aprovado/reprovado", "aprovado_reprovado", "Aprovados/Reprovados", True),
    ("boleto enviado", "boleto_enviado", "Boleto Enviado", True),
    ("pagamento confirmado", "pagamento_confirmado", "Pagamento Confirmado", True),
    ("aceite", "aceite", "Aceite", True),
)


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def deal_total(extra: Optional[dict] = None) -> int:
    page = list_page("/api/deals", page=1, per_page=1, extra=extra)
    return int(page.get("total") or 0)


def _principal_stage_ids() -> dict[str, str]:
    """Nome dobrado → id da etapa no Pipeline Principal (espelho local)."""
    from db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name, id FROM bwipo_stages
            WHERE pipeline_id = %s
            """,
            (PIPELINE_PRINCIPAL_ID,),
        )
        return {_fold(name): sid for name, sid in cur.fetchall()}
    finally:
        conn.close()


def _funnel_payload(counts: dict[str, int], *, source: str) -> dict:
    stages = []
    active = 0
    for _fold_name, key, label, highlight in _FUNNEL_ROWS:
        count = int(counts.get(key) or 0)
        if key != "perdido":
            active += count
        stages.append({
            "key": key,
            "id": key,
            "label": label,
            "count": count,
            "highlight": highlight,
        })
    for stage in stages:
        if stage["key"] == "perdido" or active <= 0:
            stage["pct"] = None
        else:
            stage["pct"] = round(stage["count"] / active * 100, 1)
    return {
        "stages": stages,
        "total": active,
        "leads_fetched": active,
        "pages": 0,
        "source": source,
        "crm": "bwipo",
        "dashboard_only": True,
    }


def dashboard_funnel_live() -> dict:
    """Estoque ao vivo de cada etapa do funil. Uma chamada por etapa (o total vem na página)."""
    ids = _principal_stage_ids()
    counts = {}
    for fold_name, key, _label, _highlight in _FUNNEL_ROWS:
        sid = ids.get(fold_name)
        if not sid:
            counts[key] = 0
            continue
        counts[key] = deal_total({"pipelineId": PIPELINE_PRINCIPAL_ID, "stageId": sid})
    return _funnel_payload(counts, source="live")


def dashboard_funnel_mirror() -> dict:
    """Mesmas etapas, lidas do espelho, quando a API não responde."""
    from db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT s.name, COUNT(d.id)
            FROM bwipo_stages s
            LEFT JOIN bwipo_deals d ON d.stage_id = s.id AND NOT d.is_deleted
            WHERE s.pipeline_id = %s
            GROUP BY s.name
            """,
            (PIPELINE_PRINCIPAL_ID,),
        )
        by_name = {_fold(name): int(n or 0) for name, n in cur.fetchall()}
        synced_at = None
        cur.execute(
            """
            SELECT last_sync_at FROM bwipo_sync_metadata
            WHERE entity_type = 'deals'
            """
        )
        row = cur.fetchone()
        if row:
            synced_at = row[0]
    finally:
        conn.close()
    counts = {key: by_name.get(fold_name, 0) for fold_name, key, _l, _h in _FUNNEL_ROWS}
    out = _funnel_payload(counts, source="db")
    if synced_at is not None:
        if hasattr(synced_at, "astimezone"):
            synced_at = synced_at.astimezone(timezone(timedelta(hours=-3)))
            out["synced_at"] = synced_at.strftime("%d/%m %H:%M")
        else:
            out["synced_at"] = str(synced_at)[:16]
    return out


def count_created_between(start: datetime, end: datetime, *, max_pages: int = 40) -> int:
    """Negócios criados no intervalo. A API ignora createdSince; filtra createdAt em updatedSince."""
    since = _iso_z(start)
    until = _iso_z(end)
    extra = {"pipelineId": PIPELINE_PRINCIPAL_ID, "updatedSince": since}
    found = 0
    for page_n in range(1, max_pages + 1):
        page = list_page("/api/deals", page=page_n, per_page=100, extra=extra)
        if page_n == 1 and int(page.get("total") or 0) > max_pages * 100:
            return count_created_mirror(start, end)
        items = page.get("items") or []
        for deal in items:
            created = deal.get("createdAt") or ""
            if since <= created < until:
                found += 1
        if len(items) < 100:
            break
    return found


def count_created_mirror(start: datetime, end: datetime) -> int:
    from db import get_conn

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM bwipo_deals
            WHERE NOT is_deleted
              AND pipeline_id = %s
              AND created_at >= %s AND created_at < %s
            """,
            (PIPELINE_PRINCIPAL_ID, start, end),
        )
        return int(cur.fetchone()[0] or 0)
    finally:
        conn.close()


def count_won_closed_between(start: datetime, end: datetime, *, max_pages: int = 10) -> int:
    """Ganhos cujo closedAt cai no intervalo. updatedSince limita a página."""
    since = _iso_z(start)
    until = _iso_z(end)
    extra = {"pipelineId": PIPELINE_PRINCIPAL_ID, "status": "WON", "updatedSince": since}
    found = 0
    for page_n in range(1, max_pages + 1):
        page = list_page("/api/deals", page=page_n, per_page=100, extra=extra)
        items = page.get("items") or []
        for deal in items:
            closed = deal.get("closedAt") or ""
            if since <= closed < until:
                found += 1
        if len(items) < 100:
            break
    return found
