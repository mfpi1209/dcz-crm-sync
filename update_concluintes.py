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
    "RGM":      "2ac4e30f-cfd7-435f-b688-fbce27f76c38",
    "Situacao": "fd08d44b-a4a5-4343-b7a9-37f75e2c1caa",
}

STAGE_NAMES_CONCLUINTE = ["Concluinte", "Concluintes", "CONCLUINTE"]

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
            return {"ok": True, "status": r.status_code, "body": r.json()}
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
    """Carrega snapshot mais recente de concluintes → {rgm: data}."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, row_count, uploaded_at FROM xl_snapshots WHERE tipo='concluintes' ORDER BY id DESC LIMIT 1")
    snap = cur.fetchone()
    if not snap:
        cur.close()
        return None, set()

    log.info("Snapshot concluintes: id=%d, %d registros, %s", snap["id"], snap["row_count"], snap["uploaded_at"])
    cur.execute("SELECT data->>'rgm_digits' AS rgm FROM xl_rows WHERE snapshot_id=%s AND data->>'rgm_digits' != ''", (snap["id"],))
    rgms = {r["rgm"] for r in cur.fetchall()}
    cur.close()
    log.info("  %d RGMs concluintes", len(rgms))
    return snap, rgms


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


def find_concluinte_stage(stages):
    lower_variants = [n.lower().strip() for n in STAGE_NAMES_CONCLUINTE]
    for stage in stages:
        sname = (stage["nome"] or "").strip()
        if sname.lower() in lower_variants:
            log.info("  Etapa 'Concluinte' encontrada: %s (%s)", sname, stage["id"][:12])
            return stage["id"]
    log.warning("  Etapa 'Concluinte' NÃO encontrada. Negócios serão apenas marcados como Won.")
    return None


def analyze(concluintes_rgms, crm_by_rgm, concluinte_stage_id):
    """Determina quais negócios devem ser movidos/marcados como won."""
    to_move = []
    to_win = []
    stats = Counter()

    for rgm in concluintes_rgms:
        biz_list = crm_by_rgm.get(rgm, [])
        if not biz_list:
            stats["rgm_sem_negocio"] += 1
            continue

        for biz in biz_list:
            biz_id = biz["id"]
            biz_data = biz["data"]
            crm_status = biz_data.get("status", "")
            crm_stage_id = biz_data.get("stageId", "")
            crm_sit = get_biz_field(biz_data, BIZ_FIELD_IDS["Situacao"])
            nome = lead_name(biz_data) or rgm

            if crm_status == "won":
                if concluinte_stage_id and crm_stage_id != concluinte_stage_id:
                    to_move.append({"biz_id": biz_id, "rgm": rgm, "nome": nome, "motivo": "Won mas fora da etapa Concluinte"})
                    stats["move_concluinte"] += 1
                else:
                    stats["ja_won_correto"] += 1
                continue

            if concluinte_stage_id and crm_stage_id != concluinte_stage_id:
                to_move.append({"biz_id": biz_id, "rgm": rgm, "nome": nome, "motivo": "Concluinte (mover)"})
                stats["move_concluinte"] += 1

            if crm_status != "won":
                to_win.append({"biz_id": biz_id, "rgm": rgm, "nome": nome})
                stats["marcar_won"] += 1

    return to_move, to_win, stats


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
        snap, concluintes_rgms = load_concluintes_snapshot(conn)
        if not snap:
            log.error("Nenhum snapshot de concluintes encontrado. Faça upload primeiro.")
            return

        crm_by_rgm = load_crm_businesses(conn)
        stages = load_pipeline_stages(conn)
        concluinte_stage_id = find_concluinte_stage(stages)
    finally:
        conn.close()

    to_move, to_win, stats = analyze(concluintes_rgms, crm_by_rgm, concluinte_stage_id)

    log.info("Resumo:")
    for k, v in sorted(stats.items()):
        log.info("  %s: %d", k, v)
    log.info("  Mover: %d | Marcar Won: %d", len(to_move), len(to_win))

    if mode == "--dry-run":
        REPORTS_DIR.mkdir(exist_ok=True)
        preview = REPORTS_DIR / "concluintes_preview.csv"
        with open(preview, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["acao", "biz_id", "rgm", "nome", "motivo"])
            for item in to_move:
                w.writerow(["MOVE", item["biz_id"], item["rgm"], item["nome"], item["motivo"]])
            for item in to_win:
                w.writerow(["WIN", item["biz_id"], item["rgm"], item["nome"], "Concluinte"])
        log.info("Preview: %s", preview)
        log.info("Para executar: python update_concluintes.py --execute")
        return

    if not to_move and not to_win:
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
        w.writerow(["timestamp", "acao", "batch_n", "batch_size", "resultado", "ids_amostra"])

        if to_move and concluinte_stage_id:
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

    elapsed = time.monotonic() - start
    log.info("Concluído em %.1f min. OK: %d | Erros: %d | API calls: %d", elapsed / 60, ok_count, err_count, api.total_calls)
    log.info("Log: %s", log_file)


if __name__ == "__main__":
    main()
