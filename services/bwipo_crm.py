"""Cliente LOCAL do CRM comercial Bwipo (Upload Comercial).

NÃO mexe no Kommo a menos que UPLOAD_COMERCIAL_CRM=bwipo.

Auth: Bearer `BWIPO_CRM_TOKEN` (prefixo eduit_).
API pública: https://integrations.bwipo.com  (tokens NÃO são aceitos em comercialcruzeiro.bwipo.com)
Front: https://comercialcruzeiro.bwipo.com

Escrita de custom fields (confirmado no deal #5189):
    PUT /api/deals/{id}/custom-fields
    {"values": [{"fieldId": "<id>", "value": "<str>"}]}
Flag do Upload Comercial: UPLOAD_COMERCIAL_CRM=bwipo (default continua Kommo).
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import re
import threading
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DEFAULT_API = "https://integrations.bwipo.com"
_DEFAULT_WEB = "https://comercialcruzeiro.bwipo.com"
_TIMEOUT = 90

# Pipeline Principal — ids observados em 2026-09-21 (org comercial cruzeiro).
PIPELINE_PRINCIPAL_ID = "cmu2tx2f600h1qo018c7r0alx"
PIPELINE_ATIVACOES_ID = "cmu5vitqo01tlmq01qx3jlav0"

STAGES = {
    "lead_de_entrada": "cmu2xmns00l6vq6019s508e9j",
    "contato_inicial": "cmu2tx2fg00h3qo01vaik3tk4",
    "sem_resposta": "cmu2tx2fg00h4qo019aikj9cm",
    "em_atendimento": "cmu2tx2fg00h5qo01q27vy7bw",
    "aguardando_resposta": "cmu2tx2fg00h6qo01a4d7d3hz",
    "aguardando_inscricao": "cmu2tx2fg00h7qo01ehk7ctkw",
    "inscricao": "cmu2x2z4n0jjtqn01huz8l1cz",
    "processo_seletivo": "cmu2x2zc40jk7qn0123tej7cb",
    "aprovado_reprovado": "cmu2x2zin0jklqn01wzm2e9e5",
    "boleto_enviado": "cmu2x301m0jl3qn0168rbvcqi",
    "pagamento_confirmado": "cmu2x30960jlhqn01ay55bzh6",
    "aceite": "cmu2x30f30jlnqn01wqcb2wlf",
    "robo": "cmu2x5ojg0l3hqn0136dooym6",
    "ganho": "cmu2tx2fg00h8qo01sigh62wz",
    "perdido": "cmu2tx2fg00h9qo01wbou1ldv",
}

# Dono padrão dos NOVO no Kommo era 8261837 (adm@eduit.com.br).
# GET /api/users é 401 com o token de API; o id abaixo veio de um negócio
# já atribuído a Admin Sistema (resolvido por e-mail).
ADMIN_SISTEMA_ID = os.getenv(
    "BWIPO_NOVO_OWNER_ID", "cmu30un170001uawgopfset4j"
)
ADMIN_SISTEMA_EMAIL = "adm@eduit.com.br"
ADMIN_SISTEMA_NAME = "Admin Sistema"

# Equivalência das etapas que o executar_acoes usa hoje no Kommo.
KOMMO_STAGE_TO_BWIPO = {
    "processo seletivo": STAGES["processo_seletivo"],
    "aprovad": STAGES["aprovado_reprovado"],
    "venda ganha": STAGES["ganho"],
    "matriculado": STAGES["ganho"],
    "aceite": STAGES["aceite"],
    "142": STAGES["ganho"],
    "143": STAGES["perdido"],
}

# Campos custom do deal (name da API Bwipo).
# Kommo field_name → Bwipo custom field name (None = não existe definição).
KOMMO_TO_BWIPO_FIELD = {
    "Situação": "situacao",
    "Curso_SIAA": "curso_de_inscricao",
    "Curso": "curso_de_inscricao",
    "Modalidade_SIAA": "modalidade_curso1",
    "Modalidade": "modalidade_curso1",
    "Grau_SIAA": "grau_new",
    "Grau": "grau_new",
    "Polo": "polo",  # TEXT livre. polo_de_inscricao é SELECT com lista fechada.
    "Marca": None,
    "CPF": "cpf",
    "Telefone Inscricao": "telefone_da_inscricao",
    "Telefone Comercial": None,  # nativo contact.phone
    "Chave_SIAA": None,
    "Email Acadêmico": "e_mail_academico",
    "E-mail": "e_mail",
    "Preço_SIAA": "valor_curso1",
    "Duração_SIAA": "duracao",
    "Nro. da Inscrição": "nro_da_inscricao",
    "Turma de Ingresso": None,
    "Nome": "nome_completo",
    "CEP": "cep",
    "RG": "rg",
    "Origem": "origem_do_lead",
    "RGM": "rgm",
    "Matrícula": "data_de_matricula",
    "Inscrição": None,  # data da inscrição: não há campo DATE equivalente
    "Situação (perda)": "motivo_da_perda",
}

# fieldId confirmados no deal #5189 (2026-09-21). Listagem devolve
# customFields como {customFieldId, value} — este mapa resolve o name.
DEAL_FIELD_IDS = {
    "cpf": "cmu2wwmld0burqn01gu4w1sxm",
    "rgm": "cmu2wz3qt0eqrqn01gv5p1vcr",
    "situacao": "cmub9ys7k2wxhld01zvujuuiu",
    "polo": "cmub9yel82wunld01uru5t6b0",
    "polo_de_inscricao": "cmu2wvvc90az3qn01zptwmn7f",
    "curso_de_inscricao": "cmu2ww7lk0be9qn01q3re0hem",
    "nome_completo": "cmu2wwfah0bn5qn01ao6klixz",
    "e_mail": "cmu2wx03m0cajqn01k714l0id",
    "e_mail_academico": "cmu2wzdv80fb3qn01e3xaii0b",
    "telefone_da_inscricao": "cmu2wxfv80cu3qn01jzzi19lj",
    "cep": "cmu2wx59q0chxqn01gov51hq6",
    "rg": "cmu2wwrvl0c0jqn01eyt5ux5o",
    "origem_do_lead": "cmu2x0o7p0gxzqn01aey739bm",
    "nro_da_inscricao": "cmu2wrsvf069zqn01ur6wmqjv",
    "data_de_matricula": "cmu2wyu8l0efdqn01kfew46tt",
    "data_de_nascimento": "cmu2wy0w30dhrqn01771f7d0n",
    "grau_new": "cmu5r0cise7094117ddc51108",
    "modalidade_curso1": "cmu5r0cy8059a696e1beb8014",
    "valor_curso1": "cmu5r0d6c9d1dc130f9171d21",
    "duracao": "cmu5r0d2j34c1cf5ff6fc2b7c",
    "motivo_da_perda": "cmu5r0cmpb1aa681eb7aff470",
}
FIELD_ID_TO_NAME = {v: k for k, v in DEAL_FIELD_IDS.items()}

# Status/pipeline sintéticos = mesmos ints que gerar_acoes já compara no Kommo.
SYNTHETIC_PIPE_VENDAS = 5481944
SYNTHETIC_GANHO = 142
SYNTHETIC_PERDIDO = 143
SYNTHETIC_ACEITE = 48566207
SYNTHETIC_ROBO = 53917599
SYNTHETIC_ATIVO = 1

DATE_FIELDS = {"data_de_matricula", "data_de_nascimento"}
LOSS_REASON_DUPLICADO = "Matriculado/Concorrente"

_INDEX = None
_INDEX_LOCK = threading.Lock()

# Campos Kommo sem definição no Bwipo (definição, não só valor vazio).
MISSING_FIELD_DEFINITIONS = [
    "Marca",
    "Chave_SIAA",
    "Turma de Ingresso",
    "Inscrição (data)",
    "Telefone Comercial (custom; existe phone nativo no contato)",
    "Curso_SIAA vs Curso (Bwipo tem um só: curso_de_inscricao)",
]

# Tipos observados — CPF/RGM/RG/CEP/telefone como NUMBER perdem formatação.
NUMBER_FIELDS = {
    "cpf", "rgm", "rg", "cep", "nro_da_inscricao", "telefone_da_inscricao",
}

SELECT_FIELDS = {
    "grau_new": ["Graduação", "Pós-Graduação"],
    "polo_de_inscricao": [
        "barra funda", "vila ema", "freguesia", "taboao centro", "mituzi",
        "vila prudente 2", "santana 2", "ouro verde", "capivari", "ibirapuera",
        "itapira centro", "vila mariana", "polo mais próximo",
    ],
    "tipo_de_inscricao": [
        "ENEM", "Segunda Graduação", "Transferência", "Pós ou MBA",
        "Vestibular Múltipla Escolha", "Vestibular Redação", "Regresso",
    ],
    "motivo_da_perda": [
        "Matriculado/Concorrente", "Análise curricular", "Contato Inválido",
        "Interesse Futuro", "Localização", "Não possui o curso", "Pesquisa",
        "Preço", "Presencial/Técnico", "Sem resposta", "Possui débito",
        "Sem interesse", "Black List", "Abandono", "1ª Mensalidade",
    ],
}


class BwipoCrmError(RuntimeError):
    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


def _api() -> str:
    return (os.getenv("BWIPO_CRM_BASE_URL") or _DEFAULT_API).rstrip("/")


def _web() -> str:
    return (os.getenv("BWIPO_CRM_WEB_URL") or _DEFAULT_WEB).rstrip("/")


def _token() -> str:
    return (os.getenv("BWIPO_CRM_TOKEN") or "").strip()


def configured() -> bool:
    return bool(_token())


_REQ_LOCK = threading.Lock()
_LAST_REQ_AT = 0.0


def _max_rps() -> float:
    raw = os.getenv("BWIPO_MAX_RPS", "0") or "0"
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


def _throttle() -> None:
    """Teto duro de req/s. 0 = sem limite (Upload Comercial). Backfill usa 3."""
    rps = _max_rps()
    if rps <= 0:
        return
    min_interval = 1.0 / rps
    global _LAST_REQ_AT
    with _REQ_LOCK:
        now = time.monotonic()
        wait = _LAST_REQ_AT + min_interval - now
        if wait > 0:
            time.sleep(wait)
        _LAST_REQ_AT = time.monotonic()


def _http_timeout() -> float:
    raw = os.getenv("BWIPO_HTTP_TIMEOUT", "") or str(_TIMEOUT)
    try:
        return max(30.0, float(raw))
    except ValueError:
        return float(_TIMEOUT)


def _request(method: str, path: str, body: Any = None) -> Any:
    token = _token()
    if not token:
        raise BwipoCrmError("BWIPO_CRM_TOKEN não configurado no .env", 503)
    url = _api() + path
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "dcz-crm-sync/upload-comercial-bwipo-local",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    timeout = _http_timeout()
    attempts = 4
    last_exc: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        _throttle()
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
            if e.code != 429 and e.code < 500:
                logger.warning("bwipo_crm %s %s → %s %s", method, path, e.code, str(msg)[:300])
                raise BwipoCrmError(str(msg) or f"CRM HTTP {e.code}", e.code) from e
            last_exc = e
            logger.warning(
                "bwipo_crm %s %s → HTTP %s, tentativa %s/%s",
                method, path, e.code, attempt, attempts,
            )
        except (TimeoutError, urllib.error.URLError, ConnectionError) as e:
            last_exc = e
            logger.warning(
                "bwipo_crm %s %s timeout/rede, tentativa %s/%s: %s",
                method, path, attempt, attempts, e,
            )
        if attempt < attempts:
            time.sleep(min(2 ** attempt, 15))
    raise BwipoCrmError(
        f"Falha ao conectar no CRM Bwipo após {attempts} tentativas: {last_exc}",
        502,
    ) from last_exc


def _items(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("items", "deals", "contacts", "data"):
            val = payload.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def get_pipelines() -> list[dict]:
    payload = _request("GET", "/api/pipelines")
    return payload if isinstance(payload, list) else _items(payload)


def get_stages() -> list[dict]:
    payload = _request("GET", "/api/stages")
    return payload if isinstance(payload, list) else _items(payload)


def get_deal(deal_id: str) -> dict:
    return _request("GET", f"/api/deals/{urllib.parse.quote(deal_id)}")


def get_deal_custom_fields(deal_id: str) -> list[dict]:
    payload = _request("GET", f"/api/deals/{urllib.parse.quote(deal_id)}/custom-fields")
    return payload if isinstance(payload, list) else []


def search_contacts(*, phone: str = "", email: str = "", query: str = "", per_page: int = 5) -> list[dict]:
    params: list[tuple[str, str]] = [("perPage", str(per_page))]
    if phone:
        params.append(("phone", phone))
    if email:
        params.append(("email", email))
    if query:
        params.append(("search", query))
    qs = urllib.parse.urlencode(params)
    return _items(_request("GET", f"/api/contacts?{qs}"))


def search_deals(*, query: str = "", contact_id: str = "", stage_id: str = "",
                 status: str = "", per_page: int = 20) -> dict:
    params: list[tuple[str, str]] = [("perPage", str(per_page))]
    if query:
        params.append(("search", query))
    if contact_id:
        params.append(("contactId", contact_id))
    if stage_id:
        params.append(("stageId", stage_id))
    if status:
        params.append(("status", status))
    qs = urllib.parse.urlencode(params)
    payload = _request("GET", f"/api/deals?{qs}")
    if isinstance(payload, dict):
        return payload
    return {"items": _items(payload), "total": None}


def find_deals_by_phone(telefone: str) -> list[dict]:
    """Match operacional possível hoje: telefone nativo do contato (não CPF)."""
    contacts = search_contacts(phone=telefone, per_page=5)
    if not contacts:
        digits = "".join(ch for ch in (telefone or "") if ch.isdigit())
        if len(digits) >= 8:
            contacts = search_contacts(query=digits[-11:], per_page=5)
    deals: list[dict] = []
    seen: set[str] = set()
    for c in contacts:
        cid = c.get("id")
        if not cid:
            continue
        for d in _items(search_deals(contact_id=cid, per_page=10)):
            did = d.get("id")
            if did and did not in seen:
                seen.add(did)
                deals.append(d)
    return deals


def deal_url(deal: Optional[dict]) -> str:
    web = _web()
    if deal and deal.get("number") is not None:
        return f"{web}/pipeline?deal={urllib.parse.quote(str(deal['number']))}"
    if deal and deal.get("id"):
        return f"{web}/pipeline?deal={urllib.parse.quote(str(deal['id']))}"
    return f"{web}/pipeline"


def _digits(val: Any) -> str:
    return re.sub(r"[^0-9]", "", str(val or ""))


def _date_for_bwipo(val: Any) -> str:
    s = str(val or "").strip()
    if not s:
        return ""
    if "T" in s:
        s = s[:10]
    if len(s) >= 10 and s[4] == "-":
        y, m, d = s[:10].split("-")
        return f"{d}/{m}/{y}"
    return s


def _cf_map_from_list(deal: dict) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in deal.get("customFields") or []:
        if not isinstance(item, dict):
            continue
        fid = item.get("customFieldId") or item.get("fieldId")
        name = FIELD_ID_TO_NAME.get(fid or "")
        if name and item.get("value") not in (None, "", [], {}):
            out[name] = item.get("value")
    return out


def synthetic_status(stage_id: Optional[str]) -> int:
    if stage_id == STAGES["ganho"]:
        return SYNTHETIC_GANHO
    if stage_id == STAGES["perdido"]:
        return SYNTHETIC_PERDIDO
    if stage_id == STAGES["aceite"]:
        return SYNTHETIC_ACEITE
    if stage_id == STAGES["robo"]:
        return SYNTHETIC_ROBO
    return SYNTHETIC_ATIVO


def synthetic_pipeline(deal: dict) -> int:
    stage = deal.get("stage") or {}
    pipe = (stage.get("pipeline") or {}).get("id") or deal.get("pipelineId")
    if pipe == PIPELINE_PRINCIPAL_ID:
        return SYNTHETIC_PIPE_VENDAS
    return 0


def stage_sort(deal: dict) -> int:
    stage = deal.get("stage") or {}
    try:
        return int(stage.get("position") or 0)
    except (TypeError, ValueError):
        return 0


_INDEX_PROGRESS = None


def set_index_progress(callback) -> None:
    """Aviso na tela do Upload Comercial enquanto o índice pagina a base."""
    global _INDEX_PROGRESS
    _INDEX_PROGRESS = callback


def iter_deals(*, per_page: int = 100):
    page = 1
    loaded = 0
    while True:
        payload = _request("GET", f"/api/deals?perPage={per_page}&page={page}")
        items = _items(payload) if isinstance(payload, dict) else []
        if not items:
            break
        yield from items
        loaded += len(items)
        total = payload.get("total") if isinstance(payload, dict) else None
        if page == 1 or page % 25 == 0:
            msg = f"Índice Novo CRM: {loaded}" + (f"/{total}" if total else "") + " negócios"
            logger.info(msg)
            if _INDEX_PROGRESS:
                _INDEX_PROGRESS(msg)
        if total is not None and loaded >= int(total):
            break
        if len(items) < per_page:
            break
        page += 1


def _normalize_deal(raw: dict) -> dict:
    contact = raw.get("contact") or {}
    cfs = _cf_map_from_list(raw)
    phone = contact.get("phone") or cfs.get("telefone_da_inscricao") or ""
    email = (contact.get("email") or cfs.get("e_mail") or "").strip().lower()
    cpf = _digits(cfs.get("cpf")).zfill(11) if cfs.get("cpf") else ""
    if cpf in ("00000000000", "00000000009") or set(cpf) == {"0"}:
        cpf = ""
    rgm = _digits(cfs.get("rgm"))
    stage = raw.get("stage") or {}
    closed = raw.get("closedAt")
    closed_date = str(closed)[:10] if closed else None
    status_id = synthetic_status(raw.get("stageId") or stage.get("id"))
    return {
        "id": raw.get("id"),
        "number": raw.get("number"),
        "name": contact.get("name") or raw.get("title") or "",
        "contact_id": raw.get("contactId") or contact.get("id"),
        "stage_id": raw.get("stageId") or stage.get("id"),
        "stage_name": stage.get("name") or "",
        "status_id": status_id,
        "pipeline_id": synthetic_pipeline(raw),
        "owner_id": raw.get("ownerId"),
        "closed_at": closed_date,
        "situacao": cfs.get("situacao") or "",
        "cpf": cpf,
        "rgm": rgm,
        "rg": _digits(cfs.get("rg")),
        "phone": _digits(phone),
        "email": email,
        "email_academico": (cfs.get("e_mail_academico") or "").strip().lower(),
        "cfs": cfs,
        "lead_fechado": status_id in (SYNTHETIC_GANHO, SYNTHETIC_PERDIDO),
        "sort": stage_sort(raw),
    }


def build_lead_index() -> dict:
    deals: dict[str, dict] = {}
    idx_cpf: dict[str, set] = {}
    idx_rgm: dict[str, set] = {}
    idx_tel: dict[str, set] = {}
    idx_email: dict[str, set] = {}
    idx_rg: dict[str, set] = {}
    lid_cpf: dict[str, str] = {}
    lead_pipe: dict[str, tuple] = {}
    n_raw = 0
    for raw in iter_deals():
        n_raw += 1
        d = _normalize_deal(raw)
        did = d.get("id")
        if not did:
            continue
        deals[did] = d
        lead_pipe[did] = (d["pipeline_id"], d["status_id"], d["sort"])
        if d["cpf"] and len(d["cpf"]) == 11:
            lid_cpf[did] = d["cpf"]
            idx_cpf.setdefault(d["cpf"], set()).add(did)
        if d["rgm"]:
            idx_rgm.setdefault(d["rgm"].lower(), set()).add(did)
        tel = d["phone"]
        if len(tel) >= 10:
            idx_tel.setdefault(tel[-11:], set()).add(did)
            idx_tel.setdefault(tel[-10:], set()).add(did)
        for em in (d["email"], d["email_academico"]):
            if em:
                idx_email.setdefault(em, set()).add(did)
        if len(d["rg"]) >= 5:
            idx_rg.setdefault(d["rg"], set()).add(did)
    logger.info(
        "bwipo index: %d deals brutos / %d indexados / cpf=%d rgm=%d tel=%d email=%d",
        n_raw, len(deals), len(idx_cpf), len(idx_rgm), len(idx_tel), len(idx_email),
    )
    return {
        "deals": deals,
        "idx_cpf": idx_cpf,
        "idx_rgm": idx_rgm,
        "idx_tel": idx_tel,
        "idx_email": idx_email,
        "idx_rg": idx_rg,
        "lid_cpf": lid_cpf,
        "lead_pipe": lead_pipe,
        "lead_deleted": set(),
    }


def clear_lead_index() -> None:
    global _INDEX
    with _INDEX_LOCK:
        _INDEX = None


def get_lead_index(*, refresh: bool = False) -> dict:
    global _INDEX
    with _INDEX_LOCK:
        if _INDEX is None or refresh:
            _INDEX = build_lead_index()
        return _INDEX


def put_deal(deal_id: str, body: dict) -> Any:
    return _request("PUT", f"/api/deals/{urllib.parse.quote(deal_id)}", body)


def put_custom_fields(deal_id: str, values_by_name: dict[str, Any]) -> list:
    values = []
    for name, val in values_by_name.items():
        fid = DEAL_FIELD_IDS.get(name)
        if not fid or val in (None, ""):
            continue
        if name in NUMBER_FIELDS:
            val = _digits(val)
            if not val:
                continue
        elif name in DATE_FIELDS:
            val = _date_for_bwipo(val)
            if not val:
                continue
        values.append({"fieldId": fid, "value": str(val)})
    if not values:
        return []
    payload = _request(
        "PUT",
        f"/api/deals/{urllib.parse.quote(deal_id)}/custom-fields",
        {"values": values},
    )
    return payload if isinstance(payload, list) else []


def create_contact(*, name: str, email: str = "", phone: str = "") -> dict:
    body: dict[str, Any] = {"name": name}
    if email:
        body["email"] = email
    if phone:
            digits = _digits(phone)
            if digits:
                if digits.startswith("55") and len(digits) >= 12:
                    body["phone"] = "+" + digits
                else:
                    body["phone"] = "+55" + digits
    return _request("POST", "/api/contacts", body)


def create_deal(*, title: str, contact_id: str, stage_id: str, owner_id: str = "") -> dict:
    body: dict[str, Any] = {
        "title": title,
        "contactId": contact_id,
        "stageId": stage_id,
    }
    if owner_id:
        body["ownerId"] = owner_id
    return _request("POST", "/api/deals", body)


def move_deal(deal_id: str, stage_id: str, *, lost_reason: str = "") -> Any:
    body: dict[str, Any] = {"stageId": stage_id}
    if lost_reason:
        body["lostReason"] = lost_reason
    return put_deal(deal_id, body)
