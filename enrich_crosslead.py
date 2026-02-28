"""
Enriquece o relatório de RGMs em leads diferentes com dados de conversas e atividades.

Para cada lead no relatório duplicatas_entre_leads.csv:
  1. Busca histórico do lead (última atividade no CRM)
  2. Cruza conversas pré-carregadas por telefone do lead

Gera CSV enriquecido agrupado por RGM para decisão manual.

Uso:
    python enrich_crosslead.py
    python enrich_crosslead.py --limit 100
    python enrich_crosslead.py --rate 30
"""

import sys
import io
import os
import csv
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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
INPUT_FILE = REPORTS_DIR / "duplicatas_entre_leads.csv"

DEFAULT_RATE = 60


class _BRTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=BRT)
        return dt.strftime(datefmt or "%H:%M:%S")


logging.basicConfig(level=logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(_BRTFormatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"))
logging.root.handlers = [_handler]
log = logging.getLogger("enrich")


class ApiClient:
    def __init__(self, rate_limit=DEFAULT_RATE):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {API_TOKEN}"
        self.s.headers["Content-Type"] = "application/json"
        self.rate_limit = rate_limit
        self.base_delay = 60.0 / rate_limit
        self._last_req = 0.0
        self._remaining = rate_limit
        self._reset = 0
        self.total_calls = 0
        self._lock = threading.Lock()

    def _throttle(self):
        with self._lock:
            if self._remaining <= 5 and self._reset > 0:
                wait = self._reset + 1
                self._lock.release()
                time.sleep(wait)
                self._lock.acquire()
                return
            elapsed = time.monotonic() - self._last_req
            if elapsed < self.base_delay:
                gap = self.base_delay - elapsed
                self._lock.release()
                time.sleep(gap)
                self._lock.acquire()
            self._last_req = time.monotonic()
            self.total_calls += 1

    def _read_headers(self, r):
        self._remaining = int(r.headers.get("X-RateLimit-Remaining", self._remaining))
        self._reset = int(r.headers.get("X-RateLimit-Reset", 0))

    def get(self, path, params=None):
        for attempt in range(4):
            self._throttle()
            try:
                r = self.s.get(f"{API_BASE}{path}", params=params, timeout=30)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ReadTimeout) as e:
                wait = 5 * (2 ** attempt)
                log.warning("Conexão falhou (%s) — retry em %ds (%d/4)",
                            type(e).__name__, wait, attempt + 1)
                time.sleep(wait)
                continue

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 30))
                log.warning("429 — Retry-After %ds (%d/4)", retry, attempt + 1)
                time.sleep(retry + 1)
                continue

            with self._lock:
                self._read_headers(r)
            if r.status_code >= 400:
                return None
            return r.json()
        return None

    def paginate(self, path, params=None, label="registros"):
        all_items = []
        skip = 0
        take = 1000
        last_logged = 0
        while True:
            p = {**(params or {}), "skip": skip, "take": take}
            data = self.get(path, p)
            if not data:
                break
            items = data.get("data", [])
            all_items.extend(items)
            if len(all_items) - last_logged >= 1000 or len(items) < take:
                log.info("  ... %d %s carregados", len(all_items), label)
                last_logged = len(all_items)
            if len(items) < take:
                break
            skip += take
        return all_items


def load_lead_phones(conn, lead_ids):
    """Carrega telefones dos leads do banco local."""
    if not lead_ids:
        return {}
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id,
               data->>'name' AS nome,
               COALESCE(data->'phone'->>'phoneNumber', data->>'phone', '') AS telefone,
               data->>'email' AS email,
               data->>'createdAt' AS criado_em
        FROM leads
        WHERE id = ANY(%s)
    """, (list(lead_ids),))
    rows = cur.fetchall()
    cur.close()
    return {r["id"]: r for r in rows}


def normalize_phone(phone):
    """Extrai apenas dígitos de um telefone."""
    if not phone:
        return ""
    return "".join(c for c in str(phone) if c.isdigit())


def load_conversations(api):
    """Pré-carrega todas as conversas (abertas e fechadas) para cruzamento por telefone."""
    log.info("Carregando conversas da API...")
    convs = api.paginate("/conversations", {"filter[opened]": "false"}, label="conversas")
    log.info("  %d conversas carregadas", len(convs))

    by_phone = {}
    for c in convs:
        contact = c.get("contact") or {}
        cid = normalize_phone(contact.get("contactId", ""))
        if not cid:
            continue
        last_msg = c.get("lastMessageDate") or c.get("lastReceivedMessageDate") or ""
        last_recv = c.get("lastReceivedMessageDate") or ""
        last_sent = c.get("lastSendedMessageDate") or ""
        finished = c.get("finished", False)

        existing = by_phone.get(cid)
        if not existing or (last_msg and last_msg > (existing.get("lastMessageDate") or "")):
            by_phone[cid] = {
                "conv_id": c.get("id", ""),
                "lastMessageDate": last_msg,
                "lastReceivedMessageDate": last_recv,
                "lastSendedMessageDate": last_sent,
                "finished": finished,
                "conv_name": c.get("name", ""),
            }

    log.info("  %d telefones únicos com conversa", len(by_phone))
    return by_phone


def get_lead_activity_from_db(conn, lead_ids):
    """Busca atividade recente dos leads via dados locais (negócios)."""
    if not lead_ids:
        return {}
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT
            b.data->>'leadId' AS lead_id,
            MAX(b.data->>'lastMovedAt') AS last_moved,
            MAX(b.data->>'createdAt') AS last_created,
            MAX(b.data->'attendant'->>'name') AS attendant
        FROM businesses b
        WHERE b.data->>'leadId' = ANY(%s)
        GROUP BY b.data->>'leadId'
    """, (list(lead_ids),))
    rows = cur.fetchall()
    cur.close()

    result = {}
    for r in rows:
        lid = r["lead_id"]
        last = r["last_moved"] or r["last_created"] or ""
        result[lid] = {
            "last_history_date": last,
            "last_history_type": "business_move" if r["last_moved"] else "business_create",
            "last_history_text": "",
            "last_history_attendant": r["attendant"] or "",
            "source": "db",
        }
    return result


def get_lead_history(api, lead_id):
    """Busca a última atividade/anotação do lead via API."""
    data = api.get(f"/leads/{lead_id}/history", {"take": 1})
    if not data:
        return None
    items = data.get("data", [])
    if not items:
        return None
    item = items[0]
    return {
        "last_history_date": item.get("createdAt", ""),
        "last_history_type": item.get("type", ""),
        "last_history_text": (item.get("history") or "")[:80],
        "last_history_attendant": (item.get("attendant") or {}).get("name", ""),
        "source": "api",
    }


def fetch_histories_concurrent(api, lead_ids, max_workers=4):
    """Busca histórico de múltiplos leads em paralelo."""
    results = {}
    done_count = [0]
    total = len(lead_ids)
    lock = threading.Lock()

    def _fetch_one(lid):
        hist = get_lead_history(api, lid)
        with lock:
            results[lid] = hist or {}
            done_count[0] += 1
            c = done_count[0]
            if c % 100 == 0 or c == total:
                pct = c / total * 100
                log.info("  [%d/%d] %.0f%% — %d API calls", c, total, pct, api.total_calls)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, lid): lid for lid in lead_ids}
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                lid = futures[f]
                log.warning("Erro ao buscar histórico de %s: %s", lid[:12], e)
                results[lid] = {}

    return results


def format_date(iso_str):
    """Converte ISO date para formato legível BRT."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.astimezone(BRT).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return iso_str[:19]


def main():
    rate_limit = DEFAULT_RATE
    limit = None

    for i, arg in enumerate(sys.argv):
        if arg == "--rate" and i + 1 < len(sys.argv):
            rate_limit = int(sys.argv[i + 1])
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])

    log.info("Enriquecimento de duplicatas entre leads")
    log.info("Rate-limit: %d req/min", rate_limit)

    if not INPUT_FILE.exists():
        log.error("Arquivo não encontrado: %s", INPUT_FILE)
        log.error("Rode o saneamento (dry-run) primeiro para gerar o relatório.")
        return

    rows = []
    with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for r in reader:
            rows.append(r)
    log.info("  %d linhas no relatório", len(rows))

    lead_ids = set(r["lead_id"] for r in rows if r.get("lead_id"))
    log.info("  %d leads únicos para enriquecer", len(lead_ids))

    if limit:
        lead_ids = set(list(lead_ids)[:limit])
        log.info("  Limitado a %d leads", len(lead_ids))

    conn = psycopg2.connect(**DB_DSN)
    try:
        lead_info = load_lead_phones(conn, lead_ids)
        log.info("  %d leads com dados locais", len(lead_info))

        log.info("Buscando atividade dos leads no banco local...")
        db_activity = get_lead_activity_from_db(conn, lead_ids)
        log.info("  %d leads com atividade no banco local", len(db_activity))
    finally:
        conn.close()

    api = ApiClient(rate_limit)

    convs_by_phone = load_conversations(api)

    needs_api = sorted(lid for lid in lead_ids if lid not in db_activity)
    has_db = len(lead_ids) - len(needs_api)
    log.info("Histórico: %d já resolvidos via DB, %d precisam da API", has_db, len(needs_api))

    workers = max(1, min(4, rate_limit // 60))
    if needs_api:
        est_min = len(needs_api) * (60.0 / rate_limit) / 60 / workers
        log.info("Consultando histórico de %d leads via API (%d workers, estimativa: %.1f min)",
                 len(needs_api), workers, est_min)
        api_history = fetch_histories_concurrent(api, needs_api, max_workers=workers)
    else:
        api_history = {}

    lead_history = {**db_activity, **api_history}
    for lid in lead_ids:
        if lid not in lead_history:
            lead_history[lid] = {}

    REPORTS_DIR.mkdir(exist_ok=True)
    output_path = REPORTS_DIR / "duplicatas_enriquecido.csv"

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([
            "RGM", "total_leads",
            "lead_id", "lead_nome", "lead_cpf", "lead_telefone", "lead_email",
            "curso", "situacao", "status_crm", "score",
            "ultima_atividade_crm", "tipo_atividade", "atendente", "resumo_atividade",
            "tem_conversa", "ultima_msg", "ultima_msg_recebida", "ultima_msg_enviada",
            "conversa_finalizada",
            "recomendacao",
        ])

        by_rgm = defaultdict(list)
        for r in rows:
            by_rgm[r["RGM"]].append(r)

        for rgm in sorted(by_rgm.keys()):
            group = by_rgm[rgm]
            enriched = []

            for r in group:
                lid = r["lead_id"]
                info = lead_info.get(lid, {})
                hist = lead_history.get(lid, {})

                phone_raw = info.get("telefone", "")
                phone_norm = normalize_phone(phone_raw)

                conv = None
                if phone_norm:
                    for suffix_len in [len(phone_norm), 11, 13]:
                        key = phone_norm[-suffix_len:] if len(phone_norm) >= suffix_len else phone_norm
                        if key in convs_by_phone:
                            conv = convs_by_phone[key]
                            break
                    if not conv:
                        for k, v in convs_by_phone.items():
                            if phone_norm[-10:] and k.endswith(phone_norm[-10:]):
                                conv = v
                                break

                last_activity = hist.get("last_history_date", "")
                last_msg = (conv or {}).get("lastMessageDate", "")

                most_recent = max(last_activity, last_msg) if last_activity or last_msg else ""

                enriched.append({
                    "row": r,
                    "info": info,
                    "hist": hist,
                    "conv": conv,
                    "most_recent": most_recent,
                })

            enriched.sort(key=lambda x: x["most_recent"] or "", reverse=True)

            for idx, e in enumerate(enriched):
                r = e["row"]
                info = e["info"]
                hist = e["hist"]
                conv = e["conv"] or {}

                if len(enriched) > 1 and idx == 0 and e["most_recent"]:
                    rec = "MANTER (mais recente)"
                elif len(enriched) > 1 and idx > 0:
                    if not e["most_recent"]:
                        rec = "CANDIDATO A MERGE (sem atividade)"
                    else:
                        rec = "AVALIAR (atividade antiga)"
                else:
                    rec = ""

                w.writerow([
                    r.get("RGM", ""),
                    r.get("total_leads_diferentes", ""),
                    r.get("lead_id", ""),
                    r.get("lead_nome", ""),
                    r.get("lead_cpf", ""),
                    info.get("telefone", ""),
                    info.get("email", ""),
                    r.get("curso", ""),
                    r.get("situacao", ""),
                    r.get("status_crm", ""),
                    r.get("score", ""),
                    format_date(hist.get("last_history_date", "")),
                    hist.get("last_history_type", ""),
                    hist.get("last_history_attendant", ""),
                    hist.get("last_history_text", ""),
                    "Sim" if conv else "Não",
                    format_date(conv.get("lastMessageDate", "")),
                    format_date(conv.get("lastReceivedMessageDate", "")),
                    format_date(conv.get("lastSendedMessageDate", "")),
                    "Sim" if conv.get("finished") else ("Não" if conv else ""),
                    rec,
                ])

    log.info("Relatório enriquecido salvo: %s", output_path)
    log.info("Total API calls: %d", api.total_calls)

    manter = sum(1 for g in by_rgm.values() for e in g if True)
    log.info("RGMs analisados: %d | Leads: %d", len(by_rgm), len(lead_ids))


if __name__ == "__main__":
    main()
