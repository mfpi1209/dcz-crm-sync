"""
eduit. — Atualização de dados de concluintes nos negócios do CRM.

Lê o snapshot mais recente de 'concluintes' do banco de dados,
cruza por RGM com os negócios locais, e marca o negócio como Won
ou move para etapa de "Concluinte" se configurada.

Uso:
    python update_concluintes.py --dry-run       # (padrão) mostra o que faria
    python update_concluintes.py --execute       # executa as atualizações
    python update_concluintes.py --execute --rate 150
"""

import sys
import io
import os
import csv
import json
import time
import hashlib
import logging
import unicodedata
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

BRT = timezone(timedelta(hours=-3))

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

BIZ_FIELD_IDS = {
    "RGM":              "2ac4e30f-cfd7-435f-b688-fbce27f76c38",
    "Curso":            "4bddb764-658b-48bc-9d70-6e94ad420132",
    "Polo":             "0ec9d8dc-d547-4482-b9ad-d4a3e6ec1b54",
    "Serie":            "b921a702-8e51-4b6c-b4d8-cdea931ea51d",
    "Situacao":         "fd08d44b-a4a5-4343-b7a9-37f75e2c1caa",
    "DataMatricula":    "bf93a8e9-42c0-4517-8518-6f604746a300",
    "Modalidade":       "9c8fc723-d9f7-4074-a0bc-ca4b96d36739",
    "EmailAD":          "731bd2fd-7cfa-49af-ab24-2e55e0374798",
    "TipoAluno":        "4230e4db-970b-4444-abaf-c3135a03b79c",
    "Ciclo":            "b9dce12b-30b7-4a0f-a764-298031f5b84e",
    "Nivel":            "233fcf6f-0bed-49d7-89a1-d1cd54fb9c12",
}

STAGE_NAMES_CONCLUINTE = [
    "Concluinte", "Concluintes", "CONCLUINTE",
    "Concluínte", "Concluíntes", "CONCLUÍNTE",
]

API_RATE_LIMIT = 240
DEFAULT_TARGET_RATE = 120
CRITICAL_REMAINING = 20
BATCH_SIZE = 50


class _BRTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=BRT)
        return dt.strftime(datefmt or "%H:%M:%S")


logging.basicConfig(level=logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(_BRTFormatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"))
logging.root.handlers = [_handler]
log = logging.getLogger("update_concluintes")


class ApiClient:
    def __init__(self, target_rate=None):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {API_TOKEN}"
        self.s.headers["Content-Type"] = "application/json"
        self._remaining = API_RATE_LIMIT
        self._reset = 0
        self._last_req = 0.0
        self.total_calls = 0
        self._window_start = time.monotonic()
        self._window_calls = 0
        self.target_rate = max(1, min(target_rate or DEFAULT_TARGET_RATE, API_RATE_LIMIT))
        self.base_delay = 60.0 / self.target_rate
        log.info("Rate-limit: %d req/min", self.target_rate)

    def _throttle(self):
        now = time.monotonic()
        if now - self._window_start >= 60:
            self._window_start = now
            self._window_calls = 0
        if self._remaining <= CRITICAL_REMAINING and self._reset > 0:
            wait = self._reset + 1
            log.warning("Rate-limit crítico (%d restantes) — pausando %ds", self._remaining, wait)
            time.sleep(wait)
            self._window_start = time.monotonic()
            self._window_calls = 0
            return
        ratio = self._remaining / API_RATE_LIMIT
        delay = self.base_delay if ratio > 0.5 else self.base_delay * 1.5 if ratio > 0.25 else self.base_delay * 3.0
        if self._window_calls >= self.target_rate:
            remaining_window = 60 - (now - self._window_start)
            if remaining_window > 0:
                time.sleep(remaining_window + 0.5)
                self._window_start = time.monotonic()
                self._window_calls = 0
                return
        elapsed = now - self._last_req
        if elapsed < delay:
            time.sleep(delay - elapsed)

    def _read_headers(self, r):
        self._remaining = int(r.headers.get("X-RateLimit-Remaining", self._remaining))
        self._reset = int(r.headers.get("X-RateLimit-Reset", 0))

    def _request(self, method, url, payload=None):
        for attempt in range(4):
            self._throttle()
            self._last_req = time.monotonic()
            self.total_calls += 1
            self._window_calls += 1
            try:
                r = self.s.request(method, url, json=payload, timeout=30)
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
                wait = min(5 * (2 ** attempt), 60)
                log.warning("Timeout (tentativa %d/4) — retry em %ds: %s", attempt + 1, wait, str(exc)[:120])
                time.sleep(wait)
                continue
            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 30))
                log.warning("429 — Retry-After %ds", retry)
                time.sleep(retry + 1)
                continue
            self._read_headers(r)
            if r.status_code >= 400:
                return {"ok": False, "status": r.status_code, "body": r.text[:500]}
            body = r.json() if r.text.strip() else {}
            return {"ok": True, "status": r.status_code, "body": body}
        return {"ok": False, "status": 429, "body": "Falha após 4 tentativas"}

    def post(self, path, payload):
        return self._request("POST", f"{API_BASE}{path}", payload)

    def put(self, path, payload):
        return self._request("PUT", f"{API_BASE}{path}", payload)

    def put_biz_field(self, biz_id, field_id, value):
        path = f"/crm/crm/additional-fields/business/{biz_id}/{field_id}"
        return self.put(path, {"value": str(value)})

    def move_businesses(self, ids, destination_stage_id):
        return self.post("/businesses/actions/move", {"ids": ids, "destinationStageId": destination_stage_id})

    def win_businesses(self, ids):
        return self.post("/businesses/actions/win", {"ids": ids})

    def restore_businesses(self, ids):
        return self.post("/businesses/actions/restore", {"ids": ids})

    def create_lead(self, payload):
        return self._request("POST", f"{API_BASE}/leads", payload)

    def create_business(self, lead_id, stage_id):
        return self.post("/businesses", {"leadId": lead_id, "stageId": stage_id})

    def patch_lead(self, lead_id, payload):
        return self._request("PATCH", f"{API_BASE}/leads/{lead_id}", payload)


def get_conn():
    return psycopg2.connect(**DB_DSN)


def get_biz_field(biz_data, field_id):
    for f in biz_data.get("additionalFields", []):
        af = f.get("additionalField", {})
        if isinstance(af, dict) and af.get("id") == field_id:
            return f.get("value", "")
        if isinstance(af, str) and af == field_id:
            return f.get("value", "")
    return ""


def lead_name(biz_data):
    lead = biz_data.get("lead")
    if isinstance(lead, dict):
        return lead.get("name", "")
    return ""


def load_concluintes_snapshot(conn):
    """Carrega snapshot mais recente de concluintes → {rgm: row_data}."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, row_count, uploaded_at FROM xl_snapshots WHERE tipo='concluintes' ORDER BY id DESC LIMIT 1")
    snap = cur.fetchone()
    if not snap:
        cur.close()
        return None, {}

    log.info("Snapshot concluintes: id=%d, %d registros, %s", snap["id"], snap["row_count"], snap["uploaded_at"])
    cur.execute("SELECT data FROM xl_rows WHERE snapshot_id=%s AND data->>'rgm_digits' != ''", (snap["id"],))
    rows = cur.fetchall()
    cur.close()

    by_rgm = {}
    for r in rows:
        d = r["data"] if isinstance(r["data"], dict) else json.loads(r["data"])
        rgm = d.get("rgm_digits", "")
        if rgm:
            by_rgm[rgm] = d
    log.info("  %d RGMs concluintes", len(by_rgm))
    return snap, by_rgm


def load_crm_businesses(conn):
    log.info("Carregando negócios do banco local...")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, data FROM businesses")
    rows = cur.fetchall()
    cur.close()

    by_rgm = {}
    for row in rows:
        rgm = get_biz_field(row["data"], BIZ_FIELD_IDS["RGM"])
        if rgm and rgm.strip():
            by_rgm.setdefault(rgm.strip(), []).append(row)
    log.info("  %d negócios | %d RGMs únicos", len(rows), len(by_rgm))
    return by_rgm


def load_pipeline_stages(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT ps.id, ps.data->>'name' AS nome
        FROM pipeline_stages ps
        ORDER BY ps.data->>'order'
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def find_concluinte_stage(stages):
    normalized_variants = {_strip_accents(n.lower().strip()) for n in STAGE_NAMES_CONCLUINTE}
    for stage in stages:
        sname = (stage["nome"] or "").strip()
        if _strip_accents(sname.lower()) in normalized_variants:
            log.info("  Etapa 'Concluinte' encontrada: %s (%s)", sname, stage["id"][:12])
            return stage["id"]
    log.warning("  Etapa 'Concluinte' NÃO encontrada no banco local.")
    log.warning("  Etapas disponíveis:")
    for s in stages:
        log.warning("    - %s (%s)", s["nome"], s["id"][:12])
    return None


def _normalize_phone(raw):
    """Normaliza telefone para formato aceito pela API."""
    if not raw:
        return ""
    digits = "".join(c for c in str(raw) if c.isdigit())
    if len(digits) >= 10 and not digits.startswith("55"):
        digits = "55" + digits
    return digits


def _build_lead_payload(xl_data):
    """Constrói payload para POST /leads a partir de dados do snapshot."""
    payload = {}
    nome = xl_data.get("nome", "").strip()
    if nome:
        payload["name"] = nome
    cpf = xl_data.get("cpf_digits", "") or xl_data.get("cpf", "")
    if cpf:
        payload["taxId"] = str(cpf).strip()
    phones = xl_data.get("phones_digits", [])
    if phones:
        payload["phone"] = str(phones[0])
    elif xl_data.get("fone_cel"):
        payload["phone"] = _normalize_phone(xl_data["fone_cel"])
    email = xl_data.get("email", "").strip()
    if email:
        payload["email"] = email
    empresa = xl_data.get("empresa", "").strip()
    if empresa:
        payload["company"] = empresa
    bairro = xl_data.get("bairro", "").strip() if xl_data.get("bairro") else ""
    cidade = xl_data.get("cidade", "").strip() if xl_data.get("cidade") else ""
    if bairro or cidade:
        addr = {}
        if bairro:
            addr["block"] = bairro
        if cidade:
            addr["city"] = cidade
        payload["address"] = addr
    if not payload.get("name"):
        payload["name"] = f"Concluinte RGM {xl_data.get('rgm_digits', '?')}"
    return payload


def _build_biz_fields(xl_data):
    """Retorna lista de (field_id, value) para preencher campos adicionais do negócio."""
    fields = []
    rgm = xl_data.get("rgm_digits", "") or xl_data.get("rgm", "")
    if rgm:
        fields.append((BIZ_FIELD_IDS["RGM"], str(rgm)))
    curso = xl_data.get("curso", "")
    if curso:
        fields.append((BIZ_FIELD_IDS["Curso"], str(curso).strip()))
    polo = xl_data.get("polo", "")
    if polo:
        fields.append((BIZ_FIELD_IDS["Polo"], str(polo).strip()))
    serie = xl_data.get("serie", "")
    if serie:
        fields.append((BIZ_FIELD_IDS["Serie"], str(serie).strip()))
    sit = xl_data.get("situacao", "")
    if sit:
        fields.append((BIZ_FIELD_IDS["Situacao"], str(sit).strip()))
    data_mat = xl_data.get("data_mat", "")
    if data_mat:
        fields.append((BIZ_FIELD_IDS["DataMatricula"], str(data_mat).strip()))
    modalidade = xl_data.get("modalidade", "")
    if modalidade:
        fields.append((BIZ_FIELD_IDS["Modalidade"], str(modalidade).strip()))
    email_acad = xl_data.get("email_acad", "")
    if email_acad:
        fields.append((BIZ_FIELD_IDS["EmailAD"], str(email_acad).strip()))
    tipo = xl_data.get("tipo_matricula", "")
    if tipo:
        fields.append((BIZ_FIELD_IDS["TipoAluno"], str(tipo).strip()))
    ciclo = xl_data.get("ciclo", "")
    if ciclo:
        fields.append((BIZ_FIELD_IDS["Ciclo"], str(ciclo).strip()))
    nivel = xl_data.get("negocio", "")
    if nivel:
        fields.append((BIZ_FIELD_IDS["Nivel"], str(nivel).strip()))
    return fields


def analyze(concluintes_by_rgm, crm_by_rgm, concluinte_stage_id):
    """Determina quais negócios devem ser restaurados, movidos, criados e marcados como won."""
    to_restore = []
    to_move = []
    to_win = []
    to_create = []
    stats = Counter()

    for rgm, xl_data in concluintes_by_rgm.items():
        biz_list = crm_by_rgm.get(rgm, [])
        if not biz_list:
            to_create.append({"rgm": rgm, "xl_data": xl_data, "nome": xl_data.get("nome", rgm)})
            stats["criar_lead_negocio"] += 1
            continue

        for biz in biz_list:
            biz_id = biz["id"]
            biz_data = biz["data"]
            crm_status = biz_data.get("status", "")
            crm_stage_id = biz_data.get("stageId", "")
            nome = lead_name(biz_data) or rgm

            if crm_status == "won":
                if concluinte_stage_id and crm_stage_id != concluinte_stage_id:
                    to_move.append({"biz_id": biz_id, "rgm": rgm, "nome": nome, "motivo": "Won mas fora da etapa Concluinte"})
                    stats["move_concluinte"] += 1
                else:
                    stats["ja_won_correto"] += 1
                continue

            if crm_status == "lost":
                to_restore.append({"biz_id": biz_id, "rgm": rgm, "nome": nome})
                stats["restore"] += 1

            if concluinte_stage_id and crm_stage_id != concluinte_stage_id:
                to_move.append({"biz_id": biz_id, "rgm": rgm, "nome": nome, "motivo": "Concluinte (mover)"})
                stats["move_concluinte"] += 1

            to_win.append({"biz_id": biz_id, "rgm": rgm, "nome": nome})
            stats["marcar_won"] += 1

    return to_restore, to_move, to_win, to_create, stats


def _batch(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def main():
    mode = "--dry-run"
    rate_limit = None

    for arg in sys.argv[1:]:
        if arg in ("--dry-run", "--execute"):
            mode = arg
    for i, arg in enumerate(sys.argv):
        if arg == "--rate" and i + 1 < len(sys.argv):
            rate_limit = int(sys.argv[i + 1])

    log.info("=" * 50)
    log.info("ATUALIZAÇÃO DE CONCLUINTES — %s", mode.upper())
    log.info("=" * 50)

    conn = get_conn()
    try:
        snap, concluintes_by_rgm = load_concluintes_snapshot(conn)
        if not snap:
            log.error("Nenhum snapshot de concluintes encontrado. Faça upload primeiro.")
            return

        crm_by_rgm = load_crm_businesses(conn)
        stages = load_pipeline_stages(conn)
        concluinte_stage_id = find_concluinte_stage(stages)
    finally:
        conn.close()

    if not concluinte_stage_id:
        log.error("Etapa 'Concluinte' obrigatória. Crie no CRM e rode sync antes de executar.")
        return

    to_restore, to_move, to_win, to_create, stats = analyze(concluintes_by_rgm, crm_by_rgm, concluinte_stage_id)

    log.info("Resumo:")
    for k, v in sorted(stats.items()):
        log.info("  %s: %d", k, v)
    log.info("  Restaurar: %d | Mover: %d | Marcar Won: %d | Criar: %d",
             len(to_restore), len(to_move), len(to_win), len(to_create))

    if mode == "--dry-run":
        REPORTS_DIR.mkdir(exist_ok=True)
        preview = REPORTS_DIR / "concluintes_preview.csv"
        with open(preview, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["acao", "biz_id", "rgm", "nome", "motivo"])
            for item in to_restore:
                w.writerow(["RESTORE", item["biz_id"], item["rgm"], item["nome"], "Lost → Restaurar para mover"])
            for item in to_move:
                w.writerow(["MOVE", item["biz_id"], item["rgm"], item["nome"], item["motivo"]])
            for item in to_win:
                w.writerow(["WIN", item["biz_id"], item["rgm"], item["nome"], "Concluinte"])
            for item in to_create:
                w.writerow(["CREATE", "-", item["rgm"], item["nome"], "Lead + Negócio + Campos + Won"])
        log.info("Preview: %s", preview)
        log.info("Para executar: python update_concluintes.py --execute")
        return

    if not to_restore and not to_move and not to_win and not to_create:
        log.info("Nada a atualizar.")
        return

    api = ApiClient(target_rate=rate_limit)

    LOG_DIR.mkdir(exist_ok=True)
    ts = datetime.now(BRT).strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"concluintes_{ts}.csv"

    ok_count = 0
    err_count = 0
    start = time.monotonic()

    with open(log_file, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["timestamp", "acao", "batch_n", "batch_size", "resultado", "detalhe"])

        if to_restore:
            ids = [i["biz_id"] for i in to_restore]
            log.info("=== RESTORE: %d negócios (lost → in_process) ===", len(ids))
            for bn, batch in enumerate(_batch(ids, BATCH_SIZE), 1):
                log.info("  Restore batch %d (%d IDs)...", bn, len(batch))
                r = api.restore_businesses(batch)
                status = "OK" if r["ok"] else "ERRO"
                w.writerow([datetime.now(BRT).strftime("%H:%M:%S"), "RESTORE", bn, len(batch), status, ";".join(batch[:3])])
                if r["ok"]:
                    ok_count += len(batch)
                else:
                    err_count += len(batch)
                    log.warning("  ERRO restore batch %d: %s", bn, r["body"][:200])

        if to_move:
            ids = [i["biz_id"] for i in to_move]
            log.info("=== MOVE → Concluinte: %d negócios ===", len(ids))
            for bn, batch in enumerate(_batch(ids, BATCH_SIZE), 1):
                log.info("  Move batch %d (%d IDs)...", bn, len(batch))
                r = api.move_businesses(batch, concluinte_stage_id)
                status = "OK" if r["ok"] else "ERRO"
                w.writerow([datetime.now(BRT).strftime("%H:%M:%S"), "MOVE", bn, len(batch), status, ";".join(batch[:3])])
                if r["ok"]:
                    ok_count += len(batch)
                else:
                    err_count += len(batch)
                    log.warning("  ERRO move batch %d: %s", bn, r["body"][:200])

        if to_win:
            ids = [i["biz_id"] for i in to_win]
            log.info("=== WIN: %d negócios ===", len(ids))
            for bn, batch in enumerate(_batch(ids, BATCH_SIZE), 1):
                log.info("  Win batch %d (%d IDs)...", bn, len(batch))
                r = api.win_businesses(batch)
                status = "OK" if r["ok"] else "ERRO"
                w.writerow([datetime.now(BRT).strftime("%H:%M:%S"), "WIN", bn, len(batch), status, ";".join(batch[:3])])
                if r["ok"]:
                    ok_count += len(batch)
                else:
                    err_count += len(batch)
                    log.warning("  ERRO win batch %d: %s", bn, r["body"][:200])

        if to_create:
            log.info("=== CREATE: %d leads + negócios (RGMs sem negócio no CRM) ===", len(to_create))
            created_biz_ids = []
            for idx, item in enumerate(to_create, 1):
                rgm = item["rgm"]
                xl = item["xl_data"]
                nome = item["nome"]

                lead_payload = _build_lead_payload(xl)
                log.info("  [%d/%d] RGM %s — Criando lead '%s'...", idx, len(to_create), rgm, nome)
                r_lead = api.create_lead(lead_payload)
                if not r_lead["ok"]:
                    err_count += 1
                    log.warning("    ERRO criar lead RGM %s: %s", rgm, str(r_lead["body"])[:200])
                    w.writerow([datetime.now(BRT).strftime("%H:%M:%S"), "CREATE_LEAD", idx, 1, "ERRO", f"RGM {rgm}: {str(r_lead['body'])[:100]}"])
                    continue

                lead_body = r_lead["body"]
                lead_id = lead_body.get("id") if isinstance(lead_body, dict) else None
                if not lead_id:
                    err_count += 1
                    log.warning("    ERRO: resposta sem ID de lead para RGM %s", rgm)
                    w.writerow([datetime.now(BRT).strftime("%H:%M:%S"), "CREATE_LEAD", idx, 1, "ERRO", f"RGM {rgm}: sem ID na resposta"])
                    continue

                log.info("    Lead criado: %s", lead_id[:12])

                r_biz = api.create_business(lead_id, concluinte_stage_id)
                if not r_biz["ok"]:
                    err_count += 1
                    log.warning("    ERRO criar negócio RGM %s: %s", rgm, str(r_biz["body"])[:200])
                    w.writerow([datetime.now(BRT).strftime("%H:%M:%S"), "CREATE_BIZ", idx, 1, "ERRO", f"RGM {rgm}: {str(r_biz['body'])[:100]}"])
                    continue

                biz_body = r_biz["body"]
                biz_id = biz_body.get("id") if isinstance(biz_body, dict) else None
                if not biz_id:
                    err_count += 1
                    log.warning("    ERRO: resposta sem ID de negócio para RGM %s", rgm)
                    continue

                log.info("    Negócio criado: %s", biz_id[:12])

                biz_fields = _build_biz_fields(xl)
                field_ok = 0
                for field_id, value in biz_fields:
                    rf = api.put_biz_field(biz_id, field_id, value)
                    if rf["ok"]:
                        field_ok += 1
                    else:
                        log.warning("    ERRO campo %s RGM %s: %s", field_id[:8], rgm, str(rf["body"])[:100])

                log.info("    %d/%d campos preenchidos", field_ok, len(biz_fields))
                created_biz_ids.append(biz_id)
                ok_count += 1
                w.writerow([datetime.now(BRT).strftime("%H:%M:%S"), "CREATE_FULL", idx, 1, "OK",
                            f"RGM {rgm} lead={lead_id[:12]} biz={biz_id[:12]} campos={field_ok}/{len(biz_fields)}"])

            if created_biz_ids:
                log.info("=== WIN (novos): %d negócios criados ===", len(created_biz_ids))
                for bn, batch in enumerate(_batch(created_biz_ids, BATCH_SIZE), 1):
                    r = api.win_businesses(batch)
                    status = "OK" if r["ok"] else "ERRO"
                    w.writerow([datetime.now(BRT).strftime("%H:%M:%S"), "WIN_NEW", bn, len(batch), status, ";".join(batch[:3])])
                    if not r["ok"]:
                        log.warning("  ERRO win novos batch %d: %s", bn, r["body"][:200])

    elapsed = time.monotonic() - start
    log.info("Concluído em %.1f min. OK: %d | Erros: %d | API calls: %d", elapsed / 60, ok_count, err_count, api.total_calls)
    log.info("Log: %s", log_file)


if __name__ == "__main__":
    main()
