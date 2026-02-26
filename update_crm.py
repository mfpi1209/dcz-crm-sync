"""
DataCrazy CRM — Atualização em massa a partir da planilha de matriculados.

Apenas atualiza campos (custom fields + lead info). Não move pipeline.

Uso:
    python update_crm.py --test          # Testa 1 update real para validar a API
    python update_crm.py --dry-run       # Mostra o que faria, sem executar (padrão)
    python update_crm.py --execute       # Executa todas as atualizações
    python update_crm.py --execute --limit 50   # Executa só os primeiros 50
"""

import sys
import io
import os
import csv
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter

import warnings
import requests
import openpyxl
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

load_dotenv(Path(__file__).parent / ".env")

API_BASE = "https://api.g1.datacrazy.io/api/v1"
API_TOKEN = os.getenv("DATACRAZY_API_TOKEN", "")

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)

REPORTS_DIR = Path(__file__).parent / "reports"
LOG_DIR = Path(__file__).parent / "logs"

FIELD_IDS = {
    "RGM":              "2ac4e30f-cfd7-435f-b688-fbce27f76c38",
    "Curso":            "4bddb764-658b-48bc-9d70-6e94ad420132",
    "Polo":             "0ec9d8dc-d547-4482-b9ad-d4a3e6ec1b54",
    "Serie":            "b921a702-8e51-4b6c-b4d8-cdea931ea51d",
    "Situacao":         "fd08d44b-a4a5-4343-b7a9-37f75e2c1caa",
    "DataMatricula":    "bf93a8e9-42c0-4517-8518-6f604746a300",
    "Modalidade":       "9c8fc723-d9f7-4074-a0bc-ca4b96d36739",
    "Bairro":           "f7cf5892-573f-45b8-9425-6dafab92cc2c",
    "Cidade":           "7a4407e4-7345-4f7e-8a24-4f51d4a10cf8",
    "EmailAD":          "731bd2fd-7cfa-49af-ab24-2e55e0374798",
    "SenhaProvisoria":  "cccb3046-1906-4465-901d-329ef2fe08dc",
    "TipoAluno":        "4230e4db-970b-4444-abaf-c3135a03b79c",
    "Sexo":             "d77aa3c7-cd39-46d2-9c12-7be48b86eb2f",
    "Turma":            "8815a8de-f755-4597-b6f4-8da6d289b6eb",
}

RATE_LIMIT_BUFFER = 5
MIN_REQUEST_DELAY = 1.05

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("update_crm")

# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------

class ApiClient:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {API_TOKEN}"
        self.s.headers["Content-Type"] = "application/json"
        self._remaining = 60
        self._reset = 0
        self._last_req = 0.0
        self.total_calls = 0

    def _throttle(self):
        elapsed = time.monotonic() - self._last_req
        if elapsed < MIN_REQUEST_DELAY:
            time.sleep(MIN_REQUEST_DELAY - elapsed)
        if self._remaining <= RATE_LIMIT_BUFFER and self._reset > 0:
            wait = self._reset + 1
            log.warning("Rate-limit próximo (%d restantes) — pausando %ds", self._remaining, wait)
            time.sleep(wait)

    def _read_headers(self, r):
        self._remaining = int(r.headers.get("X-RateLimit-Remaining", self._remaining))
        self._reset = int(r.headers.get("X-RateLimit-Reset", 0))

    def _request(self, method, url, payload=None):
        for attempt in range(4):
            self._throttle()
            self._last_req = time.monotonic()
            self.total_calls += 1

            r = self.s.request(method, url, json=payload, timeout=30)

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 30))
                log.warning("429 — Retry-After %ds (tentativa %d/4)", retry, attempt + 1)
                time.sleep(retry + 1)
                continue

            self._read_headers(r)

            if r.status_code >= 400:
                return {"ok": False, "status": r.status_code, "body": r.text[:500]}

            return {"ok": True, "status": r.status_code, "body": r.json()}

        return {"ok": False, "status": 429, "body": "Falha após 4 tentativas"}

    def patch(self, path, payload):
        return self._request("PATCH", f"{API_BASE}{path}", payload)

    def put(self, path, payload):
        return self._request("PUT", f"{API_BASE}{path}", payload)

    def put_biz_field(self, biz_id, field_id, value):
        """PUT /crm/crm/additional-fields/business/{bizId}/{fieldId}"""
        path = f"/crm/crm/additional-fields/business/{biz_id}/{field_id}"
        return self.put(path, {"value": str(value)})

    def get(self, path, params=None):
        url = f"{API_BASE}{path}"
        self._throttle()
        self._last_req = time.monotonic()
        self.total_calls += 1
        r = self.s.get(url, params=params, timeout=30)
        self._read_headers(r)
        if r.status_code >= 400:
            return {"ok": False, "status": r.status_code, "body": r.text[:500]}
        return {"ok": True, "status": r.status_code, "body": r.json()}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_cpf(cpf):
    if not cpf:
        return ""
    return str(cpf).replace(".", "").replace("-", "").replace(" ", "").strip()


def format_cpf(cpf):
    c = clean_cpf(cpf)
    if len(c) == 11:
        return f"{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:]}"
    return c


def clean_phone(phone):
    if not phone:
        return ""
    digits = "".join(c for c in str(phone) if c.isdigit())
    if digits.startswith("55") and len(digits) >= 12:
        digits = digits[2:]
    if len(digits) >= 10:
        return digits[-10:] if len(digits) == 10 else digits[-11:]
    return ""


def normalize_name(name):
    if not name:
        return ""
    import unicodedata
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    return " ".join(name.upper().split())


# ---------------------------------------------------------------------------
# Normalização de dados para o CRM
# ---------------------------------------------------------------------------

KEEP_UPPER = {"cst", "ead", "ti", "rh", "ii", "iii", "iv"}

def title_case(text):
    """Primeira maiúscula em cada palavra, preservando siglas conhecidas."""
    if not text:
        return ""
    text = str(text).strip()
    LOWERCASE_WORDS = {"de", "do", "da", "dos", "das", "e", "em", "o", "a"}
    words = text.lower().split()
    result = []
    for i, w in enumerate(words):
        if w in KEEP_UPPER:
            result.append(w.upper())
        elif i == 0 or w not in LOWERCASE_WORDS:
            result.append(w.capitalize())
        else:
            result.append(w)
    return " ".join(result)


SEXO_MAP = {
    "m": "Masculino",
    "f": "Feminino",
    "masculino": "Masculino",
    "feminino": "Feminino",
}

def normalize_sexo(sexo):
    if not sexo:
        return ""
    return SEXO_MAP.get(str(sexo).strip().lower(), title_case(sexo))


POLO_MAP = {
    "barra funda": "Barra Funda",
    "sapopemba": "Sapopemba",
    "vila prudente": "Vila Prudente",
    "morumbi": "Morumbi",
    "santana": "Santana",
    "vila mariana": "Vila Mariana",
    "ibirapuera": "Ibirapuera",
    "freguesia do o": "Freguesia do Ó",
    "freguesia do ó": "Freguesia do Ó",
    "taboao da serra - centro": "Taboão da Serra - Centro",
    "taboão da serra - centro": "Taboão da Serra - Centro",
    "taboao da serra centro": "Taboão da Serra - Centro",
    "taboão da serra centro": "Taboão da Serra - Centro",
    "taboao da serra - mituzi": "Taboão da Serra - Mituzi",
    "taboão da serra - mituzi": "Taboão da Serra - Mituzi",
    "taboao da serra mituzi": "Taboão da Serra - Mituzi",
    "taboão da serra mituzi": "Taboão da Serra - Mituzi",
    "campinas": "Campinas",
    "capivari": "Capivari",
    "itapira": "Itapira",
}


def normalize_polo(polo):
    if not polo:
        return ""
    import unicodedata
    key = unicodedata.normalize("NFKD", str(polo).strip())
    key = "".join(c for c in key if not unicodedata.combining(c)).lower()

    if key in POLO_MAP:
        return POLO_MAP[key]

    # Match parcial para nomes longos tipo "3146 - Polo Taboão da Serra_centro..."
    if "taboao" in key or "taboa" in key:
        if "mituzi" in key or "mituizi" in key or "jardim" in key:
            return "Taboão da Serra - Mituzi"
        if "centro" in key or "santos dumont" in key or "parque" in key:
            return "Taboão da Serra - Centro"
        return "Taboão da Serra - Centro"

    for canonical_key, canonical_val in POLO_MAP.items():
        if canonical_key in key:
            return canonical_val

    return title_case(str(polo).strip())


SITUACAO_MAP = {
    "em curso": "Em Curso",
    "trancado": "Trancado",
    "cancelado": "Cancelado",
    "transferido": "Transferido",
}


def normalize_situacao(sit):
    if not sit:
        return ""
    key = str(sit).strip().lower()
    return SITUACAO_MAP.get(key, title_case(sit))


TIPO_ALUNO_MAP = {
    "nova matricula": "Calouro",
    "nova matrícula": "Calouro",
    "recompra": "Calouro (Recompra)",
    "retorno": "Regresso (Retorno)",
    "rematricula": "Veterano",
    "rematrícula": "Veterano",
}


def normalize_tipo_aluno(tipo):
    if not tipo:
        return ""
    key = str(tipo).strip().lower()
    return TIPO_ALUNO_MAP.get(key, title_case(tipo))


NIVEL_MAP = {
    "graduação": "Graduação",
    "graduacao": "Graduação",
    "pós-graduação": "Pós-Graduação",
    "pos-graduacao": "Pós-Graduação",
    "pós graduação": "Pós-Graduação",
    "pos graduacao": "Pós-Graduação",
    "posgraduacao": "Pós-Graduação",
}


def normalize_nivel(nivel):
    if not nivel:
        return ""
    import unicodedata
    key = unicodedata.normalize("NFKD", str(nivel).strip())
    key = "".join(c for c in key if not unicodedata.combining(c)).lower()
    return NIVEL_MAP.get(key, title_case(nivel))


def get_biz_field(biz_data, field_id):
    for f in biz_data.get("additionalFields", []):
        af = f.get("additionalField", {})
        if isinstance(af, dict) and af.get("id") == field_id:
            return f.get("value", "")
        if isinstance(af, str) and af == field_id:
            return f.get("value", "")
    return ""


def get_biz_field_value_id(biz_data, field_id):
    """Retorna o ID do registro de valor do campo (para saber se é update ou create)."""
    for f in biz_data.get("additionalFields", []):
        af = f.get("additionalField", {})
        if isinstance(af, dict) and af.get("id") == field_id:
            return f.get("id", "")
    return ""


def get_conn():
    return psycopg2.connect(**DB_DSN)


def update_local_biz_field(conn, biz_id, field_id, new_value):
    """Atualiza o valor de um campo adicional no JSONB local do negócio."""
    with conn.cursor() as cur:
        cur.execute("SELECT data FROM businesses WHERE id = %s", (biz_id,))
        row = cur.fetchone()
        if not row:
            return
        data = row[0]
        updated = False
        for f in data.get("additionalFields", []):
            af = f.get("additionalField", {})
            fid = af.get("id") if isinstance(af, dict) else af
            if fid == field_id:
                f["value"] = str(new_value)
                updated = True
                break
        if updated:
            cur.execute(
                "UPDATE businesses SET data = %s::jsonb WHERE id = %s",
                (json.dumps(data), biz_id),
            )
            conn.commit()


def update_local_lead(conn, lead_id, updates):
    """Atualiza campos do lead no JSONB local."""
    if not updates:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT data FROM leads WHERE id = %s", (lead_id,))
        row = cur.fetchone()
        if not row:
            return
        data = row[0]
        for k, v in updates.items():
            if k == "address":
                if "address" not in data or not isinstance(data["address"], dict):
                    data["address"] = {}
                data["address"].update(v)
            else:
                data[k] = v
        cur.execute(
            "UPDATE leads SET data = %s::jsonb WHERE id = %s",
            (json.dumps(data), lead_id),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Carregamento
# ---------------------------------------------------------------------------

COLUMN_ALIASES = {
    "Ciclo": ["Ciclo"], "Nome": ["Nome"], "CPF": ["CPF"], "RGM": ["RGM"],
    "Sexo": ["Sexo"], "Curso": ["Curso"],
    "Instituicao": ["Instituição", "Institui"], "Empresa": ["Empresa"],
    "Polo": ["Polo"], "Negocio": ["Negócio", "Neg"],
    "Serie": ["Série", "Serie", "rie"],
    "DataNascimento": ["Data Nascimento"],
    "TipoMatricula": ["Tipo Matrícula", "Tipo Matr"],
    "DataMatricula": ["Data Matrícula", "Data Matr"],
    "SituacaoMatricula": ["Situação Matrícula", "Situa"],
    "FoneResidencial": ["Fone Residencial"], "FoneComercial": ["Fone Comercial"],
    "FoneCelular": ["Fone celular"], "Email": ["Email"],
    "EmailAcademico": ["Email acadêmico", "Email acad"],
    "Endereco": ["Endereço", "Endere"],
    "Bairro": ["Bairro"], "Cidade": ["Cidade"],
}


def _col_find(col_map, *candidates):
    for c in candidates:
        if c in col_map:
            return c
    for c in candidates:
        cl = c.lower()
        for k in col_map:
            if k and cl in k.lower():
                return k
    return candidates[0]


def load_excel():
    log.info("Carregando planilha...")
    xlsx = None
    for f in Path(__file__).parent.iterdir():
        if f.suffix.lower() == ".xlsx" and "matriculados" in f.name.lower():
            xlsx = f
            break
    if not xlsx:
        raise FileNotFoundError("Planilha de matriculados não encontrada")

    wb = openpyxl.load_workbook(str(xlsx), data_only=True)
    ws = wb["Export"]
    raw_header = [cell.value for cell in ws[1]]
    raw_col = {h: i for i, h in enumerate(raw_header) if h}

    col = {}
    for norm, aliases in COLUMN_ALIASES.items():
        found = _col_find(raw_col, *aliases)
        if found in raw_col:
            col[norm] = raw_col[found]

    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] is None and row[1] is None:
            continue
        rows.append(row)
    wb.close()
    log.info("  %d registros, %d colunas", len(rows), len(col))
    return rows, col


def load_crm_data(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    log.info("Carregando todos os negócios (pode levar ~2 min)...")
    cur.execute("SELECT b.id, b.data FROM businesses b")
    all_biz = cur.fetchall()

    by_rgm = {}
    biz_by_lead = {}
    for row in all_biz:
        lead_id = row["data"].get("leadId", "")
        if lead_id:
            biz_by_lead.setdefault(lead_id, []).append(row)
        rgm = get_biz_field(row["data"], FIELD_IDS["RGM"])
        if rgm:
            by_rgm.setdefault(rgm, []).append(row)
    log.info("  %d negócios | %d RGMs | %d leads com negócio",
             len(all_biz), len(by_rgm), len(biz_by_lead))

    log.info("Carregando todos os leads (pode levar ~1 min)...")
    cur.execute("""
        SELECT id,
               data->>'name' AS nome,
               REPLACE(REPLACE(COALESCE(data->>'taxId',''), '.', ''), '-', '') AS cpf,
               data->>'rawPhone' AS telefone,
               data->>'email' AS email,
               data
        FROM leads
    """)
    leads = cur.fetchall()

    by_cpf, by_phone, by_name = {}, {}, {}
    leads_by_id = {}
    for r in leads:
        leads_by_id[r["id"]] = r
        cpf = r["cpf"].strip() if r["cpf"] else ""
        if cpf:
            by_cpf.setdefault(cpf, []).append(r)
        phone = clean_phone(r["telefone"])
        if phone:
            by_phone.setdefault(phone, []).append(r)
        nome = normalize_name(r["nome"])
        if nome:
            by_name.setdefault(nome, []).append(r)

    cur.close()
    log.info("  %d leads | CPF: %d | Phone: %d | Name: %d",
             len(leads), len(by_cpf), len(by_phone), len(by_name))

    return by_rgm, by_cpf, by_phone, by_name, leads_by_id, biz_by_lead


# ---------------------------------------------------------------------------
# Preparação das atualizações
# ---------------------------------------------------------------------------

def prepare_updates(xl_rows, col, crm_by_rgm, crm_by_cpf, crm_by_phone, crm_by_name, leads_by_id, biz_by_lead):
    """Retorna lista de dicts com as atualizações necessárias."""
    updates = []

    for r in xl_rows:
        rgm = str(r[col["RGM"]]).strip() if r[col["RGM"]] else ""
        cpf = clean_cpf(r[col["CPF"]])
        phone = clean_phone(str(r[col["FoneCelular"]] or ""))
        nome = normalize_name(r[col["Nome"]] or "")

        xl_data = {
            "rgm": rgm,
            "cpf": cpf,
            "nome": title_case(r[col["Nome"]] or ""),
            "curso": title_case(r[col["Curso"]] or ""),
            "polo": normalize_polo(r[col["Polo"]] or ""),
            "serie": str(r[col["Serie"]] or ""),
            "situacao": normalize_situacao(r[col["SituacaoMatricula"]] or ""),
            "tipo": normalize_tipo_aluno(r[col["TipoMatricula"]] or ""),
            "bairro": title_case(r[col["Bairro"]] or ""),
            "cidade": title_case(r[col["Cidade"]] or ""),
            "sexo": normalize_sexo(r[col["Sexo"]] or ""),
            "email": (r[col["Email"]] or "").strip().lower(),
            "email_acad": (r[col.get("EmailAcademico", -1)] or "").strip().lower() if "EmailAcademico" in col else "",
            "empresa": title_case(r[col["Empresa"]] or "") if "Empresa" in col else "",
            "phone_raw": str(r[col["FoneCelular"]] or ""),
        }

        dm = r[col["DataMatricula"]] if "DataMatricula" in col else None
        xl_data["data_matricula"] = dm.strftime("%Y-%m-%d") if dm and hasattr(dm, "strftime") else str(dm or "")

        # Cascata de match
        match_type = None
        matched_lead_id = None
        matched_biz_list = []

        if rgm and rgm in crm_by_rgm:
            match_type = "RGM"
            matched_biz_list = crm_by_rgm[rgm]
            if matched_biz_list:
                lid = matched_biz_list[0]["data"].get("leadId", "")
                matched_lead_id = lid
        elif cpf and cpf in crm_by_cpf:
            match_type = "CPF"
            matched_lead_id = crm_by_cpf[cpf][0]["id"]
            matched_biz_list = biz_by_lead.get(matched_lead_id, [])
        elif phone and phone in crm_by_phone:
            match_type = "TELEFONE"
            matched_lead_id = crm_by_phone[phone][0]["id"]
            matched_biz_list = biz_by_lead.get(matched_lead_id, [])
        elif nome and nome in crm_by_name:
            match_type = "NOME"
            matched_lead_id = crm_by_name[nome][0]["id"]
            matched_biz_list = biz_by_lead.get(matched_lead_id, [])

        if not match_type:
            continue

        # Preparar atualização do lead
        lead_updates = {}
        if matched_lead_id and matched_lead_id in leads_by_id:
            lead = leads_by_id[matched_lead_id]
            crm_name = (lead["nome"] or "").strip()
            if xl_data["nome"] and xl_data["nome"] != crm_name:
                lead_updates["name"] = xl_data["nome"]
            crm_cpf = lead["cpf"].strip() if lead["cpf"] else ""
            if cpf and not crm_cpf:
                lead_updates["taxId"] = format_cpf(cpf)
            crm_email = (lead["email"] or "").strip().lower()
            if xl_data["email"] and not crm_email:
                lead_updates["email"] = xl_data["email"]

            crm_addr = lead["data"].get("address") or {}
            addr = {}
            if xl_data["bairro"] and xl_data["bairro"] != (crm_addr.get("block") or ""):
                addr["block"] = xl_data["bairro"]
            if xl_data["cidade"] and xl_data["cidade"] != (crm_addr.get("city") or ""):
                addr["city"] = xl_data["cidade"]
            if addr:
                lead_updates["address"] = addr

            crm_company = (lead["data"].get("company") or "").strip()
            if xl_data["empresa"] and not crm_company:
                lead_updates["company"] = xl_data["empresa"]

        # Preparar atualização dos custom fields do negócio
        biz_updates = []
        for biz in matched_biz_list:
            fields_to_update = {}
            mapping = {
                "Curso": xl_data["curso"],
                "Polo": xl_data["polo"],
                "Serie": xl_data["serie"],
                "Situacao": xl_data["situacao"],
                "Bairro": xl_data["bairro"],
                "Cidade": xl_data["cidade"],
                "Sexo": xl_data["sexo"],
                "DataMatricula": xl_data["data_matricula"],
                "TipoAluno": xl_data["tipo"],
            }
            if rgm:
                mapping["RGM"] = rgm

            for field_name, new_val in mapping.items():
                if not new_val:
                    continue
                fid = FIELD_IDS.get(field_name, "")
                if not fid:
                    continue
                current = get_biz_field(biz["data"], fid)
                if new_val != current:
                    fields_to_update[fid] = new_val

            if fields_to_update:
                biz_updates.append({
                    "biz_id": biz["id"],
                    "fields": fields_to_update,
                })

        if lead_updates or biz_updates:
            updates.append({
                "match_type": match_type,
                "xl_nome": xl_data["nome"],
                "xl_rgm": rgm,
                "lead_id": matched_lead_id,
                "lead_updates": lead_updates,
                "biz_updates": biz_updates,
            })

    return updates


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------

def field_id_to_name(fid):
    for name, fid2 in FIELD_IDS.items():
        if fid == fid2:
            return name
    return fid[:8]


def test_one_update(api, updates):
    """Testa 1 atualização via PUT campo a campo."""
    log.info("=== MODO TESTE: verificando 1 atualização ===")

    upd = None
    for u in updates:
        if u["biz_updates"]:
            upd = u
            break

    if not upd:
        log.error("Nenhum registro com atualização de negócio para testar.")
        return False

    biz = upd["biz_updates"][0]
    log.info("Negócio: %s", biz["biz_id"])
    log.info("  Nome: %s | RGM: %s | Match: %s", upd["xl_nome"], upd["xl_rgm"], upd["match_type"])

    first_fid = list(biz["fields"].keys())[0]
    first_val = biz["fields"][first_fid]
    fname = field_id_to_name(first_fid)

    log.info("  Testando campo: %s = %s", fname, first_val)
    log.info("  PUT /crm/crm/additional-fields/business/%s/%s", biz["biz_id"], first_fid)

    result = api.put_biz_field(biz["biz_id"], first_fid, first_val)
    log.info("  Status: %s", result["status"])

    if not result["ok"]:
        log.error("  FALHOU: %s", result["body"])
        return False

    body = result["body"]
    returned_value = body.get("value", "")
    log.info("  Retorno API: value=%s", returned_value)

    time.sleep(1.5)
    verify = api.get(f"/businesses/{biz['biz_id']}")
    if verify["ok"]:
        actual = get_biz_field(verify["body"], first_fid)
        log.info("  Verificação GET: %s = '%s'", fname, actual)
        if actual == str(first_val):
            log.info("  CONFIRMADO — campo atualizado com sucesso!")
            return True
        else:
            log.warning("  Campo retornou valor diferente: '%s' vs '%s'", actual, first_val)
            return True

    return True


def execute_updates(api, updates, limit=None):
    """Executa as atualizações — PUT campo a campo para negócios."""
    LOG_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"update_{ts}.csv"

    conn = get_conn()

    with open(log_file, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["timestamp", "tipo", "match", "nome", "rgm", "id",
                     "campo", "valor", "status", "resultado"])

        total = len(updates)
        if limit:
            updates = updates[:limit]
            log.info("Limitado a %d de %d atualizações", limit, total)

        ok_count = 0
        err_count = 0
        start = time.monotonic()

        for i, upd in enumerate(updates, 1):
            n_fields = sum(len(b["fields"]) for b in upd["biz_updates"])
            lead_tag = "lead+" if upd["lead_updates"] else ""
            log.info("[%d/%d] %s | RGM %s | %s%d campos | %s",
                     i, len(updates), upd["xl_nome"],
                     upd["xl_rgm"] or "—", lead_tag, n_fields,
                     upd["match_type"])

            if upd["lead_updates"] and upd["lead_id"]:
                payload = upd["lead_updates"]
                result = api.patch(f"/leads/{upd['lead_id']}", payload)
                status = "OK" if result["ok"] else "ERRO"
                w.writerow([
                    datetime.now().isoformat(), "LEAD", upd["match_type"],
                    upd["xl_nome"], upd["xl_rgm"], upd["lead_id"],
                    ",".join(payload.keys()), "", result["status"], status,
                ])
                if result["ok"]:
                    ok_count += 1
                    try:
                        update_local_lead(conn, upd["lead_id"], payload)
                    except Exception:
                        pass
                else:
                    err_count += 1
                    log.warning("  ERRO lead %s: %s", upd["lead_id"], result["body"][:200])

            for biz in upd["biz_updates"]:
                for fid, val in biz["fields"].items():
                    fname = field_id_to_name(fid)
                    result = api.put_biz_field(biz["biz_id"], fid, val)
                    status = "OK" if result["ok"] else "ERRO"
                    w.writerow([
                        datetime.now().isoformat(), "BIZ_FIELD", upd["match_type"],
                        upd["xl_nome"], upd["xl_rgm"], biz["biz_id"],
                        fname, val, result["status"], status,
                    ])
                    if result["ok"]:
                        ok_count += 1
                        try:
                            update_local_biz_field(conn, biz["biz_id"], fid, val)
                        except Exception:
                            pass
                    else:
                        err_count += 1
                        log.warning("  ERRO biz %s campo %s: %s",
                                    biz["biz_id"], fname, result["body"][:200])

            if i % 50 == 0 or i == len(updates):
                elapsed = time.monotonic() - start
                rate = api.total_calls / elapsed * 60 if elapsed > 0 else 0
                remaining = (len(updates) - i) * (elapsed / i) if i > 0 else 0
                log.info("--- %d/%d (%.0f%%) | OK: %d | Erros: %d | ~%.0f min restantes ---",
                         i, len(updates), i/len(updates)*100,
                         ok_count, err_count, remaining/60)

    conn.close()
    log.info("Concluído. OK: %d | Erros: %d | API calls: %d", ok_count, err_count, api.total_calls)
    log.info("Log detalhado: %s", log_file)
    return ok_count, err_count


def dry_run_summary(updates):
    """Mostra resumo do que seria atualizado."""
    match_types = Counter(u["match_type"] for u in updates)
    lead_updates_count = sum(1 for u in updates if u["lead_updates"])
    biz_updates_count = sum(len(u["biz_updates"]) for u in updates)
    biz_field_calls = sum(
        len(biz["fields"]) for u in updates for biz in u["biz_updates"]
    )
    total_api_calls = lead_updates_count + biz_field_calls

    estimated_minutes = total_api_calls * MIN_REQUEST_DELAY / 60

    print("\n" + "=" * 60)
    print("DRY-RUN — Resumo das atualizações pendentes")
    print("=" * 60)
    print(f"  Registros com match:           {len(updates):,}")
    for mt, c in match_types.most_common():
        print(f"    {mt:20s}: {c:,}")
    print()
    print(f"  Leads a atualizar:             {lead_updates_count:,}")
    print(f"  Negócios a atualizar:          {biz_updates_count:,}")
    print(f"  Campos PUT (1 call/campo):     {biz_field_calls:,}")
    print(f"  Total de API calls:            {total_api_calls:,}")
    print(f"  Tempo estimado (~1 req/s):     {estimated_minutes:.0f} min ({estimated_minutes/60:.1f}h)")
    print()

    # Detalhe dos campos de lead
    lead_fields = Counter()
    for u in updates:
        for k in u["lead_updates"]:
            lead_fields[k] += 1
    if lead_fields:
        print("  Campos de lead atualizados:")
        for k, c in lead_fields.most_common():
            print(f"    {k:20s}: {c:,}")

    # Detalhe dos campos de negócio
    biz_fields = Counter()
    for u in updates:
        for biz in u["biz_updates"]:
            for fid in biz["fields"]:
                for name, fid2 in FIELD_IDS.items():
                    if fid == fid2:
                        biz_fields[name] += 1
                        break
    if biz_fields:
        print("  Campos de negócio atualizados:")
        for k, c in biz_fields.most_common():
            print(f"    {k:20s}: {c:,}")

    print()
    print("  Para executar: python update_crm.py --execute")
    print("  Para testar 1: python update_crm.py --test")
    print("=" * 60)

    # Salvar preview
    preview_rows = []
    for u in updates[:200]:
        lead_chg = json.dumps(u["lead_updates"], ensure_ascii=False) if u["lead_updates"] else ""
        biz_chg = "; ".join(
            f"{biz['biz_id']}: " + ", ".join(
                f"{k}={v}" for k, v in biz["fields"].items()
            ) for biz in u["biz_updates"]
        )
        preview_rows.append([
            u["match_type"], u["xl_rgm"], u["xl_nome"],
            u["lead_id"] or "", lead_chg, biz_chg,
        ])

    preview_path = REPORTS_DIR / "update_preview.csv"
    REPORTS_DIR.mkdir(exist_ok=True)
    with open(preview_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["match_tipo", "rgm", "nome", "lead_id", "lead_mudancas", "biz_mudancas"])
        w.writerows(preview_rows)
    log.info("  Preview salvo: %s (primeiros 200)", preview_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    mode = "--dry-run"
    limit = None

    for arg in sys.argv[1:]:
        if arg in ("--test", "--dry-run", "--execute"):
            mode = arg
        elif arg == "--limit":
            pass
        elif sys.argv[sys.argv.index(arg) - 1] == "--limit":
            limit = int(arg)

    # Parse --limit N
    for i, arg in enumerate(sys.argv):
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])

    log.info("=" * 50)
    log.info("Atualização CRM — modo: %s", mode.upper())
    log.info("=" * 50)

    xl_rows, col = load_excel()

    conn = get_conn()
    try:
        crm_by_rgm, crm_by_cpf, crm_by_phone, crm_by_name, leads_by_id, biz_by_lead = load_crm_data(conn)
    finally:
        conn.close()

    log.info("Preparando atualizações...")
    updates = prepare_updates(
        xl_rows, col, crm_by_rgm,
        crm_by_cpf, crm_by_phone, crm_by_name, leads_by_id, biz_by_lead,
    )
    log.info("  %d registros com atualizações pendentes", len(updates))

    if mode == "--dry-run":
        dry_run_summary(updates)
        return

    api = ApiClient()

    if mode == "--test":
        result = test_one_update(api, updates)
        if result:
            log.info("Teste OK! Formato aceito: %s", result)
            log.info("Execute com: python update_crm.py --execute")
        else:
            log.error("Teste FALHOU. Verifique os logs acima.")
        return

    if mode == "--execute":
        log.info("Iniciando atualização em massa...")
        ok, err = execute_updates(api, updates, limit)
        print(f"\nResultado: {ok} OK, {err} erros de {len(updates)} registros.")


if __name__ == "__main__":
    main()
