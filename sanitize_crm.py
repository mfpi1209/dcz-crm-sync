"""
DataCrazy CRM — Saneamento de negócios duplicados.

Fase 1 do pipeline: limpar duplicatas ANTES de movimentar etapas.

Detecta e remove:
  1. Negócios com mesmo RGM no mesmo lead (mantém o mais completo)
  2. Negócios sem RGM em leads que já possuem negócios com RGM

Gera relatório (sem ação) para:
  3. Mesmo RGM em leads DIFERENTES (revisão manual)

Uso:
    python sanitize_crm.py --dry-run          # (padrão) mostra resumo
    python sanitize_crm.py --test             # exclui 1 negócio para validar
    python sanitize_crm.py --execute          # executa todos os DELETEs
    python sanitize_crm.py --execute --limit 50
"""

import sys
import io
import os
import csv
import json
import time
import logging
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
    "RGM":           "2ac4e30f-cfd7-435f-b688-fbce27f76c38",
    "Curso":         "4bddb764-658b-48bc-9d70-6e94ad420132",
    "Polo":          "0ec9d8dc-d547-4482-b9ad-d4a3e6ec1b54",
    "Serie":         "b921a702-8e51-4b6c-b4d8-cdea931ea51d",
    "Situacao":      "fd08d44b-a4a5-4343-b7a9-37f75e2c1caa",
    "DataMatricula": "bf93a8e9-42c0-4517-8518-6f604746a300",
    "Modalidade":    "9c8fc723-d9f7-4074-a0bc-ca4b96d36739",
    "Bairro":        "f7cf5892-573f-45b8-9425-6dafab92cc2c",
    "Cidade":        "7a4407e4-7345-4f7e-8a24-4f51d4a10cf8",
    "EmailAD":       "731bd2fd-7cfa-49af-ab24-2e55e0374798",
    "SenhaProvisoria": "cccb3046-1906-4465-901d-329ef2fe08dc",
    "TipoAluno":     "4230e4db-970b-4444-abaf-c3135a03b79c",
    "Turma":         "8815a8de-f755-4597-b6f4-8da6d289b6eb",
}

DEFAULT_RATE_LIMIT = 60

class _BRTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=BRT)
        return dt.strftime(datefmt or "%H:%M:%S")

logging.basicConfig(level=logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(_BRTFormatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"))
logging.root.handlers = [_handler]
log = logging.getLogger("sanitize")


# ---------------------------------------------------------------------------
# API Client (simplificado — só precisa de DELETE e GET)
# ---------------------------------------------------------------------------

class ApiClient:
    def __init__(self, rate_limit=None):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {API_TOKEN}"
        self.s.headers["Content-Type"] = "application/json"
        self.rate_limit = rate_limit or DEFAULT_RATE_LIMIT
        self._remaining = self.rate_limit
        self._reset = 0
        self._last_req = 0.0
        self.total_calls = 0
        self.base_delay = 60.0 / self.rate_limit

    def _throttle(self):
        if self._remaining <= 5 and self._reset > 0:
            wait = self._reset + 1
            log.warning("Rate-limit crítico (%d restantes) — pausando %ds",
                        self._remaining, wait)
            time.sleep(wait)
            return

        elapsed = time.monotonic() - self._last_req
        if elapsed < self.base_delay:
            time.sleep(self.base_delay - elapsed)

    def _read_headers(self, r):
        self._remaining = int(r.headers.get("X-RateLimit-Remaining", self._remaining))
        self._reset = int(r.headers.get("X-RateLimit-Reset", 0))

    def delete(self, biz_id):
        """DELETE /api/v1/businesses/{id}"""
        for attempt in range(4):
            self._throttle()
            self._last_req = time.monotonic()
            self.total_calls += 1

            url = f"{API_BASE}/businesses/{biz_id}"
            r = self.s.delete(url, timeout=30)

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 30))
                log.warning("429 — Retry-After %ds (tentativa %d/4)", retry, attempt + 1)
                time.sleep(retry + 1)
                continue

            self._read_headers(r)

            if r.status_code >= 400:
                return {"ok": False, "status": r.status_code, "body": r.text[:500]}

            return {"ok": True, "status": r.status_code}

        return {"ok": False, "status": 429, "body": "Falha após 4 tentativas"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def biz_score(biz_data):
    """Pontua um negócio para decidir qual manter em caso de duplicata."""
    score = 0
    for fid in BIZ_FIELD_IDS.values():
        val = get_biz_field(biz_data, fid)
        if val and str(val).strip():
            score += 1
    if biz_data.get("status") == "in_process":
        score += 2
    return score


def biz_sort_key(biz):
    """Ordena negócios: maior score primeiro, depois mais recente por lastMovedAt."""
    data = biz["data"]
    sc = biz_score(data)
    moved = data.get("lastMovedAt", "") or ""
    created = data.get("createdAt", "") or ""
    return (-sc, moved if moved else "", created if created else "")


# ---------------------------------------------------------------------------
# Carregamento
# ---------------------------------------------------------------------------

def load_all_businesses(conn):
    """Carrega todos os negócios do banco local."""
    log.info("Carregando negócios do banco local...")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, data FROM businesses")
    rows = cur.fetchall()
    cur.close()
    log.info("  %d negócios carregados", len(rows))
    return rows


def load_lead_info(conn):
    """Carrega nome e CPF dos leads para relatórios."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id,
               data->>'name' AS nome,
               REPLACE(REPLACE(COALESCE(data->>'taxId',''), '.', ''), '-', '') AS cpf
        FROM leads
    """)
    rows = cur.fetchall()
    cur.close()
    return {r["id"]: r for r in rows}


# ---------------------------------------------------------------------------
# Análise de duplicatas
# ---------------------------------------------------------------------------

def analyze(businesses, leads_info):
    """Analisa e classifica todos os negócios.

    Retorna:
        to_delete:  lista de dicts {biz_id, lead_id, lead_nome, rgm, motivo, score}
        cross_lead: lista de dicts para relatório de RGMs em leads diferentes
        stats:      dicionário com contadores
    """
    by_lead = {}
    for biz in businesses:
        lead_id = biz["data"].get("leadId", "")
        if lead_id:
            by_lead.setdefault(lead_id, []).append(biz)

    to_delete = []
    cross_lead_rgms = {}
    stats = Counter()

    rgm_global = {}
    for biz in businesses:
        rgm = get_biz_field(biz["data"], BIZ_FIELD_IDS["RGM"])
        if rgm and rgm.strip():
            rgm = rgm.strip()
            rgm_global.setdefault(rgm, []).append(biz)

    # --- Tipo 3: mesmo RGM em leads diferentes (só relatório) ---
    cross_lead = []
    for rgm, biz_list in rgm_global.items():
        lead_ids = set(b["data"].get("leadId", "") for b in biz_list)
        if len(lead_ids) <= 1:
            continue
        stats["rgm_cross_lead"] += 1
        for b in biz_list:
            lid = b["data"].get("leadId", "")
            li = leads_info.get(lid, {})
            cross_lead.append({
                "rgm": rgm,
                "biz_id": b["id"],
                "lead_id": lid,
                "lead_nome": li.get("nome", lead_name(b["data"])),
                "lead_cpf": li.get("cpf", ""),
                "curso": get_biz_field(b["data"], BIZ_FIELD_IDS["Curso"]),
                "situacao": get_biz_field(b["data"], BIZ_FIELD_IDS["Situacao"]),
                "status": b["data"].get("status", ""),
                "score": biz_score(b["data"]),
                "total_leads": len(lead_ids),
            })

    # --- Tipo 1 e 2: por lead ---
    for lead_id, biz_list in by_lead.items():
        by_rgm = {}
        sem_rgm = []
        tem_rgm = False

        for biz in biz_list:
            rgm = get_biz_field(biz["data"], BIZ_FIELD_IDS["RGM"])
            if rgm and rgm.strip():
                rgm = rgm.strip()
                by_rgm.setdefault(rgm, []).append(biz)
                tem_rgm = True
            else:
                sem_rgm.append(biz)

        li = leads_info.get(lead_id, {})
        lnome = li.get("nome", "")

        # Tipo 1: mesmo RGM no mesmo lead
        for rgm, dupes in by_rgm.items():
            if len(dupes) <= 1:
                continue
            sorted_dupes = sorted(dupes, key=biz_sort_key)
            keeper = sorted_dupes[0]
            for discard in sorted_dupes[1:]:
                stats["dup_same_lead"] += 1
                to_delete.append({
                    "biz_id": discard["id"],
                    "lead_id": lead_id,
                    "lead_nome": lnome or lead_name(discard["data"]),
                    "rgm": rgm,
                    "motivo": f"DUP_MESMO_LEAD (manter {keeper['id'][:8]}…, score {biz_score(keeper['data'])})",
                    "score": biz_score(discard["data"]),
                    "curso": get_biz_field(discard["data"], BIZ_FIELD_IDS["Curso"]),
                    "status": discard["data"].get("status", ""),
                })

        # Tipo 2: sem RGM em lead que tem negócios com RGM
        if tem_rgm:
            for biz in sem_rgm:
                if biz["data"].get("status") == "won":
                    stats["sem_rgm_won_skip"] += 1
                    continue
                stats["sem_rgm_delete"] += 1
                to_delete.append({
                    "biz_id": biz["id"],
                    "lead_id": lead_id,
                    "lead_nome": lnome or lead_name(biz["data"]),
                    "rgm": "",
                    "motivo": "SEM_RGM (lead já tem negócios com RGM)",
                    "score": biz_score(biz["data"]),
                    "curso": get_biz_field(biz["data"], BIZ_FIELD_IDS["Curso"]),
                    "status": biz["data"].get("status", ""),
                })

    stats["total_to_delete"] = len(to_delete)
    stats["total_cross_lead"] = len(cross_lead)
    return to_delete, cross_lead, stats


# ---------------------------------------------------------------------------
# Relatórios
# ---------------------------------------------------------------------------

def write_cross_lead_report(cross_lead):
    REPORTS_DIR.mkdir(exist_ok=True)
    path = REPORTS_DIR / "duplicatas_entre_leads.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["RGM", "negocio_id", "lead_id", "lead_nome", "lead_cpf",
                     "curso", "situacao", "status_crm", "score", "total_leads_diferentes",
                     "observacao"])
        for r in sorted(cross_lead, key=lambda x: x["rgm"]):
            obs = "Possível familiar/duplicidade" if r["total_leads"] == 2 else f"{r['total_leads']} leads"
            w.writerow([
                r["rgm"], r["biz_id"], r["lead_id"], r["lead_nome"],
                r["lead_cpf"], r["curso"], r.get("situacao", ""),
                r["status"], r["score"], r["total_leads"], obs,
            ])
    log.info("Relatório para análise manual: %s (%d linhas)", path, len(cross_lead))
    return path


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def dry_run_summary(to_delete, cross_lead, stats, rate_limit=DEFAULT_RATE_LIMIT):
    motivos = Counter(d["motivo"].split(" (")[0] for d in to_delete)

    log.info("Negócios a excluir: %s", f"{stats['total_to_delete']:,}")
    for m, c in motivos.most_common():
        log.info("  %s: %s", m, f"{c:,}")
    if stats.get("sem_rgm_won_skip"):
        log.info("Sem RGM ignorados (won): %s", f"{stats['sem_rgm_won_skip']:,}")

    log.info("RGMs em leads diferentes: %s RGMs (%s negócios) — somente relatório",
             f"{stats.get('rgm_cross_lead', 0):,}", f"{stats['total_cross_lead']:,}")

    if to_delete:
        est_time = len(to_delete) * (60.0 / rate_limit) / 60
        log.info("Tempo estimado (~%d req/min): %.1f min", rate_limit, est_time)

        preview_path = REPORTS_DIR / "sanitize_preview.csv"
        REPORTS_DIR.mkdir(exist_ok=True)
        with open(preview_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["biz_id", "lead_id", "lead_nome", "rgm", "curso", "status", "score", "motivo"])
            for d in to_delete[:500]:
                w.writerow([d["biz_id"], d["lead_id"], d["lead_nome"],
                            d["rgm"], d["curso"], d["status"], d["score"], d["motivo"]])
        log.info("Preview salvo: %s (primeiros 500)", preview_path)

    log.info("Resultado: %s para excluir, %s RGMs entre leads (relatório).",
             f"{stats['total_to_delete']:,}", f"{stats.get('rgm_cross_lead', 0):,}")


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------

def test_one_delete(api, to_delete):
    """Testa 1 exclusão para validar o endpoint."""
    if not to_delete:
        log.error("Nenhum negócio para excluir.")
        return False

    d = to_delete[0]
    log.info("Teste: excluindo 1 negócio")
    log.info("  Negócio: %s", d["biz_id"])
    log.info("  Lead:    %s (%s)", d["lead_nome"], d["lead_id"])
    log.info("  RGM:     %s | Motivo: %s", d["rgm"] or "(vazio)", d["motivo"])

    result = api.delete(d["biz_id"])
    log.info("  Status:  %s", result["status"])

    if result["ok"]:
            log.info("DELETE OK — endpoint validado.")
            return True
        else:
            log.error("FALHOU: %s", result.get("body", ""))
            return False


def execute_deletes(api, to_delete, conn, limit=None):
    """Executa exclusões em massa."""
    LOG_DIR.mkdir(exist_ok=True)
    ts = datetime.now(BRT).strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"sanitize_{ts}.csv"

    total = len(to_delete)
    if limit:
        to_delete = to_delete[:limit]
        log.info("Limitado a %d de %d exclusões", limit, total)

    ok_count = 0
    err_count = 0
    start = time.monotonic()

    with open(log_file, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["timestamp", "acao", "biz_id", "lead_id", "lead_nome",
                     "rgm", "motivo", "score", "status_http", "resultado"])

        for i, d in enumerate(to_delete, 1):
            log.info("[%d/%d] %s | RGM %s | %s",
                     i, len(to_delete),
                     d["lead_nome"], d["rgm"] or "—", d["motivo"].split(" (")[0])

            result = api.delete(d["biz_id"])
            status = "OK" if result["ok"] else "ERRO"

            w.writerow([
                datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S"),
                "DELETE", d["biz_id"], d["lead_id"], d["lead_nome"],
                d["rgm"], d["motivo"], d["score"],
                result["status"], status,
            ])

            if result["ok"]:
                ok_count += 1
                try:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM businesses WHERE id = %s", (d["biz_id"],))
                    conn.commit()
                except Exception:
                    pass
            else:
                err_count += 1
                log.warning("  ERRO: %s", result.get("body", "")[:200])

            if i % 25 == 0 or i == len(to_delete):
                elapsed = time.monotonic() - start
                rate = api.total_calls / elapsed * 60 if elapsed > 0 else 0
                remaining = (len(to_delete) - i) * (elapsed / i) if i > 0 else 0
                log.info("--- %d/%d (%.0f%%) | OK: %d | Erros: %d | %.0f req/min | ~%.0f min restantes ---",
                         i, len(to_delete), i / len(to_delete) * 100,
                         ok_count, err_count, rate, remaining / 60)

    log.info("Concluído. OK: %d | Erros: %d | API calls: %d", ok_count, err_count, api.total_calls)
    log.info("Log detalhado: %s", log_file)
    return ok_count, err_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    mode = "--dry-run"
    limit = None
    rate_limit = DEFAULT_RATE_LIMIT

    for arg in sys.argv[1:]:
        if arg in ("--test", "--dry-run", "--execute"):
            mode = arg
    for i, arg in enumerate(sys.argv):
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
        if arg == "--rate" and i + 1 < len(sys.argv):
            rate_limit = int(sys.argv[i + 1])

    log.info("Saneamento CRM — modo: %s", mode.upper())
    log.info("Rate-limit: %d req/min", rate_limit)

    conn = get_conn()
    try:
        businesses = load_all_businesses(conn)
        leads_info = load_lead_info(conn)

        log.info("Analisando duplicatas...")
        to_delete, cross_lead, stats = analyze(businesses, leads_info)
        log.info("%d para excluir | %d RGMs em leads diferentes (relatório)",
                 stats["total_to_delete"], stats.get("rgm_cross_lead", 0))

        write_cross_lead_report(cross_lead)

        if mode == "--dry-run":
            dry_run_summary(to_delete, cross_lead, stats, rate_limit)
            return

        api = ApiClient(rate_limit)

        if mode == "--test":
            test_one_delete(api, to_delete)
            return

        if mode == "--execute":
            log.info("Iniciando exclusão em massa...")
            ok, err = execute_deletes(api, to_delete, conn, limit)
            log.info("Resultado: %d excluídos, %d erros de %d negócios.",
                     ok, err, len(to_delete))

    finally:
        conn.close()


if __name__ == "__main__":
    main()
