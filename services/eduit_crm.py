"""Cliente HTTP do CRM EduIT (backend_crm1) para Interações Acadêmicas.

Auth: Bearer `EDUIT_CRM_TOKEN` (prefixo eduit_). API default: https://crm.eduit.com.br
Front (deep-link): https://frontend-front.v74knz.easypanel.host
O GET /api/users exige sessão NextAuth (não aceita Bearer). O match do operador
usa a busca de conversas, que indexa assignedTo.name e assignedTo.email.

PUT /api/deals/:id {ownerId} propaga responsável para contato e conversas
(propagateOwnerToContactAndChat no backend do CRM).
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
from typing import Any, Optional

from helpers import display_name_from_login, fold_name

logger = logging.getLogger(__name__)

_DEFAULT_BASE = "https://crm.eduit.com.br"
_DEFAULT_WEB = "https://frontend-front.v74knz.easypanel.host"
_USER_CACHE: dict[str, tuple[float, dict]] = {}
_USER_CACHE_TTL = 300.0
_TIMEOUT = 25


class EduitCrmError(RuntimeError):
    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


def _base() -> str:
    return (os.getenv("EDUIT_CRM_BASE_URL") or _DEFAULT_BASE).rstrip("/")


def _web() -> str:
    web = (os.getenv("EDUIT_CRM_WEB_URL") or "").strip().rstrip("/")
    # Front real do time é o EasyPanel; crm.eduit.com.br é o SaaS/marketing.
    if not web or "crm.eduit.com.br" in web.lower():
        return _DEFAULT_WEB
    return web


def _token() -> str:
    return (os.getenv("EDUIT_CRM_TOKEN") or "").strip()


def configured() -> bool:
    return bool(_token())


def _request(method: str, path: str, body: Any = None) -> Any:
    token = _token()
    if not token:
        raise EduitCrmError("EDUIT_CRM_TOKEN não configurado no .env", 503)
    url = _base() + path
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "dcz-crm-sync/academico-interacoes",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
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
        logger.warning("eduit_crm %s %s → %s %s", method, path, e.code, msg[:300])
        raise EduitCrmError(str(msg) or f"CRM HTTP {e.code}", 502 if e.code >= 500 else e.code)
    except urllib.error.URLError as e:
        raise EduitCrmError(f"Falha ao conectar no CRM EduIT: {e.reason}", 502) from e


def phone_candidates(raw: str) -> list[str]:
    digits = re.sub(r"\D+", "", raw or "")
    if not digits:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(v: str) -> None:
        if v and v not in seen:
            seen.add(v)
            out.append(v)

    add(digits)
    add("+" + digits)
    if digits.startswith("55") and len(digits) >= 12:
        local = digits[2:]
        add(local)
        add("55" + local)
        add("+55" + local)
    else:
        add("55" + digits)
        add("+55" + digits)
    return out


def _items(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("items", "users", "data"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def find_contact_by_phone(telefone: str) -> Optional[dict]:
    last_err: Optional[Exception] = None
    for cand in phone_candidates(telefone):
        try:
            payload = _request("GET", f"/api/contacts?phone={urllib.parse.quote(cand)}&perPage=5")
        except EduitCrmError as e:
            last_err = e
            continue
        items = _items(payload)
        if items:
            return items[0]
    digits = re.sub(r"\D+", "", telefone or "")
    if len(digits) >= 8:
        try:
            payload = _request(
                "GET",
                f"/api/conversations?search={urllib.parse.quote(digits[-11:])}&perPage=5",
            )
            for conv in _items(payload):
                contact = conv.get("contact") or {}
                if contact.get("id"):
                    return contact
        except EduitCrmError as e:
            last_err = e
    if last_err and not phone_candidates(telefone):
        raise last_err
    return None


def find_open_deal(contact_id: str, telefone: str = "") -> Optional[dict]:
    if contact_id:
        payload = _request(
            "GET",
            f"/api/deals?contactId={urllib.parse.quote(contact_id)}&status=OPEN&perPage=10",
        )
        items = _items(payload)
        if items:
            return items[0]
        payload = _request(
            "GET",
            f"/api/deals?contactId={urllib.parse.quote(contact_id)}&perPage=5",
        )
        items = _items(payload)
        if items:
            return items[0]
    for cand in phone_candidates(telefone)[:4]:
        payload = _request(
            "GET",
            f"/api/deals?contactPhone={urllib.parse.quote(cand)}&status=OPEN&perPage=5",
        )
        items = _items(payload)
        if items:
            return items[0]
    return None


def find_conversation(contact_id: str, telefone: str = "") -> Optional[dict]:
    if contact_id:
        payload = _request(
            "GET",
            f"/api/conversations?contactId={urllib.parse.quote(contact_id)}&perPage=10",
        )
        items = _items(payload)
        open_ones = [c for c in items if (c.get("status") or "").upper() == "OPEN"]
        if open_ones:
            return open_ones[0]
        if items:
            return items[0]
    digits = re.sub(r"\D+", "", telefone or "")
    if len(digits) >= 8:
        payload = _request(
            "GET",
            f"/api/conversations?search={urllib.parse.quote(digits[-11:])}&perPage=5",
        )
        items = _items(payload)
        if items:
            return items[0]
    return None


def _queries_for_dashboard_user(username: str, email: str, display_name: str) -> list[str]:
    qs: list[str] = []
    seen: set[str] = set()

    def add(v: str) -> None:
        v = (v or "").strip()
        if v and v.lower() not in seen:
            seen.add(v.lower())
            qs.append(v)

    add(email)
    if username and "@" in username:
        add(username)
    add(display_name)
    if username and "@" not in username:
        add(username)
    return qs


def _assigned_matches(assignee: dict, emails: set[str], name_fold: str) -> bool:
    if not assignee.get("id"):
        return False
    aemail = (assignee.get("email") or "").strip().lower()
    aname = fold_name(assignee.get("name") or "")
    if aemail and aemail in emails:
        return True
    if name_fold and aname == name_fold:
        return True
    return False


def resolve_crm_user(username: str, email: str = "") -> dict:
    """Resolve o operador da org Cruzeiro EaD a partir do login do dashboard."""
    username = (username or "").strip()
    email = (email or "").strip()
    display = display_name_from_login(username, email)
    cache_key = f"{username}|{email}".lower()
    now = time.time()
    hit = _USER_CACHE.get(cache_key)
    if hit and now - hit[0] < _USER_CACHE_TTL:
        return hit[1]

    emails = {e.lower() for e in (email, username) if e and "@" in e}
    name_fold = fold_name(display)
    last_assignees: list[dict] = []
    for q in _queries_for_dashboard_user(username, email, display):
        payload = _request(
            "GET",
            f"/api/conversations?search={urllib.parse.quote(q)}&perPage=20",
        )
        for conv in _items(payload):
            a = conv.get("assignedTo") or {}
            if a.get("id"):
                last_assignees.append(a)
            if _assigned_matches(a, emails, name_fold):
                user = {
                    "id": a["id"],
                    "name": a.get("name") or display,
                    "email": a.get("email") or email,
                }
                _USER_CACHE[cache_key] = (now, user)
                return user

    # último recurso: se a busca pelo nome devolveu um único assignedTo distinto
    unique: dict[str, dict] = {}
    for a in last_assignees:
        unique[a["id"]] = a
    if len(unique) == 1:
        a = next(iter(unique.values()))
        user = {
            "id": a["id"],
            "name": a.get("name") or display,
            "email": a.get("email") or email,
        }
        _USER_CACHE[cache_key] = (now, user)
        return user

    raise EduitCrmError(
        "Não achei seu usuário no CRM EduIT. O login do painel precisa casar "
        "com o nome ou o e-mail da org (ex.: Wesley Guerreiro / "
        "wesley.guerreiro@cruzeiroead.com.br).",
        404,
    )


def lead_url(deal: Optional[dict], conversation: Optional[dict]) -> str:
    web = _web()
    if deal:
        num = deal.get("number")
        if num is not None:
            return f"{web}/pipeline?deal={urllib.parse.quote(str(num))}"
        if deal.get("id"):
            return f"{web}/pipeline?deal={urllib.parse.quote(str(deal['id']))}"
    if conversation:
        num = conversation.get("number")
        if num is not None:
            return f"{web}/inbox?c={urllib.parse.quote(str(num))}"
        if conversation.get("id"):
            return f"{web}/inbox?c={urllib.parse.quote(str(conversation['id']))}"
    return f"{web}/inbox"


def lookup_lead(telefone: str) -> dict:
    contact = find_contact_by_phone(telefone)
    if not contact:
        raise EduitCrmError(
            "Não encontrei este telefone no CRM EduIT. Confira se o número está no contato.",
            404,
        )
    deal = find_open_deal(contact.get("id") or "", telefone)
    conversation = find_conversation(contact.get("id") or "", telefone)
    if not deal and not conversation:
        raise EduitCrmError(
            "Contato existe no CRM, mas não há negócio nem conversa para abrir.",
            404,
        )
    return {
        "contact": {
            "id": contact.get("id"),
            "name": contact.get("name"),
            "phone": contact.get("phone"),
        },
        "deal": {
            "id": deal.get("id"),
            "number": deal.get("number"),
            "title": deal.get("title"),
            "ownerId": deal.get("ownerId"),
            "ownerName": (deal.get("owner") or {}).get("name"),
        } if deal else None,
        "conversation": {
            "id": conversation.get("id"),
            "number": conversation.get("number"),
            "status": conversation.get("status"),
            "assignedToId": conversation.get("assignedToId"),
            "assignedToName": (conversation.get("assignedTo") or {}).get("name"),
        } if conversation else None,
        "crm_url": lead_url(deal, conversation),
    }


def assign_lead_to_user(telefone: str, crm_user: dict) -> dict:
    found = lookup_lead(telefone)
    deal = found.get("deal")
    if not deal or not deal.get("id"):
        raise EduitCrmError(
            "Este telefone não tem negócio (lead) no CRM para atribuir. "
            "Abra a conversa manualmente.",
            409,
        )
    uid = crm_user["id"]
    if deal.get("ownerId") != uid:
        _request("PUT", f"/api/deals/{urllib.parse.quote(deal['id'])}", {"ownerId": uid})
        found = lookup_lead(telefone)
    found["assigned"] = True
    found["crm_user"] = crm_user
    found["crm_url"] = lead_url(found.get("deal"), found.get("conversation"))
    return found
