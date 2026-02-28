"""
eduit. — Atualização de dados de inadimplência nos negócios do CRM.

Lê o snapshot mais recente de 'inadimplentes' do banco de dados,
cruza por RGM com os negócios locais, e atualiza campos personalizados
no CRM (valor em aberto, dias de atraso, tipo de título).

Uso:
    python update_inadimplentes.py --dry-run       # (padrão) mostra o que faria
    python update_inadimplentes.py --execute       # executa as atualizações
    python update_inadimplentes.py --execute --rate 150   # com rate limit
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
    "RGM":              "2ac4e30f-cfd7-435f-b688-fbce27f76c38",
    "ValorAberto":      None,
    "DiasAtraso":       None,
    "QtdTitulos":       None,
    "StatusFinanceiro": None,
}

API_RATE_LIMIT = 240
DEFAULT_TARGET_RATE = 120
CRITICAL_REMAINING = 20


class _BRTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=BRT)
        return dt.strftime(datefmt or "%H:%M:%S")


logging.basicConfig(level=logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(_BRTFormatter("%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"))
logging.root.handlers = [_handler]
log = logging.getLogger("update_inadimplentes")


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

    def put(self, path, payload):
        return self._request("PUT", f"{API_BASE}{path}", payload)

    def put_biz_field(self, biz_id, field_id, value):
        path = f"/crm/crm/additional-fields/business/{biz_id}/{field_id}"
        return self.put(path, {"value": str(value)})


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


def _data_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()


def update_local_biz_field(conn, biz_id, field_id, new_value):
    with conn.cursor() as cur:
        cur.execute("SELECT data FROM businesses WHERE id = %s", (biz_id,))
        row = cur.fetchone()
        if not row:
            return
        data = row[0]
        found = False
        for f in data.get("additionalFields", []):
            af = f.get("additionalField", {})
            fid = af.get("id") if isinstance(af, dict) else af
            if fid == field_id:
                f["value"] = str(new_value)
                found = True
                break
        if not found:
            data.setdefault("additionalFields", []).append({
                "additionalField": {"id": field_id},
                "value": str(new_value),
            })
        jdata = json.dumps(data)
        cur.execute(
            "UPDATE businesses SET data = %s::jsonb, data_hash = %s WHERE id = %s",
            (jdata, _data_hash(data), biz_id),
        )
        conn.commit()


def load_inadimplentes_snapshot(conn):
    """Carrega o snapshot mais recente de inadimplentes, retorna dict {rgm: {...}}."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, row_count, uploaded_at FROM xl_snapshots WHERE tipo='inadimplentes' ORDER BY id DESC LIMIT 1")
    snap = cur.fetchone()
    if not snap:
        cur.close()
        return None, {}

    log.info("Snapshot inadimplentes: id=%d, %d registros, %s", snap["id"], snap["row_count"], snap["uploaded_at"])
    cur.execute("SELECT data FROM xl_rows WHERE snapshot_id = %s", (snap["id"],))
    rows = cur.fetchall()
    cur.close()

    by_rgm = {}
    for r in rows:
        d = r["data"]
        rgm = str(d.get("rgm_digits", d.get("rgm", ""))).strip()
        if rgm:
            existing = by_rgm.get(rgm)
            if existing:
                v_old = float(existing.get("valor_titulo", 0) or 0)
                v_new = float(d.get("valor_titulo", 0) or 0)
                existing["valor_titulo"] = str(v_old + v_new)
                a_old = int(existing.get("dias_atraso", 0) or 0)
                a_new = int(d.get("dias_atraso", 0) or 0)
                existing["dias_atraso"] = str(max(a_old, a_new))
                existing["qtd_titulos"] = str(int(existing.get("qtd_titulos", 1) or 1) + 1)
            else:
                by_rgm[rgm] = {**d, "qtd_titulos": "1"}

    log.info("  %d RGMs inadimplentes únicos", len(by_rgm))
    return snap, by_rgm


def load_crm_businesses(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, data FROM businesses")
    rows = cur.fetchall()
    cur.close()

    by_rgm = {}
    for row in rows:
        rgm = get_biz_field(row["data"], BIZ_FIELD_IDS["RGM"])
        if rgm and rgm.strip():
            by_rgm.setdefault(rgm.strip(), []).append(row)

    log.info("  %d negócios CRM com RGM (%d RGMs únicos)", sum(len(v) for v in by_rgm.values()), len(by_rgm))
    return by_rgm


def _discover_field_ids(conn):
    """Descobre IDs de campos personalizados de inadimplência nos negócios existentes."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT data FROM businesses LIMIT 200")
    rows = cur.fetchall()
    cur.close()

    field_names_map = {
        "valor aberto": "ValorAberto",
        "valor em aberto": "ValorAberto",
        "dias atraso": "DiasAtraso",
        "dias de atraso": "DiasAtraso",
        "qtd titulos": "QtdTitulos",
        "quantidade titulos": "QtdTitulos",
        "status financeiro": "StatusFinanceiro",
    }

    for row in rows:
        for f in row["data"].get("additionalFields", []):
            af = f.get("additionalField", {})
            if isinstance(af, dict):
                name = (af.get("name") or "").strip().lower()
                for needle, key in field_names_map.items():
                    if needle in name and BIZ_FIELD_IDS[key] is None:
                        BIZ_FIELD_IDS[key] = af["id"]
                        log.info("  Campo '%s' → %s (%s)", af.get("name"), key, af["id"][:12])

    missing = [k for k, v in BIZ_FIELD_IDS.items() if v is None and k != "RGM"]
    if missing:
        log.warning("  Campos não encontrados no CRM: %s (serão ignorados)", ", ".join(missing))


def prepare_updates(inad_by_rgm, crm_by_rgm):
    """Cruza dados e prepara lista de atualizações."""
    updates = []
    stats = Counter()

    for rgm, inad_data in inad_by_rgm.items():
        biz_list = crm_by_rgm.get(rgm, [])
        if not biz_list:
            stats["rgm_sem_negocio"] += 1
            continue

        valor = inad_data.get("valor_titulo", "0")
        dias = inad_data.get("dias_atraso", "0")
        qtd = inad_data.get("qtd_titulos", "1")

        for biz in biz_list:
            biz_id = biz["id"]
            fields_to_update = {}

            if BIZ_FIELD_IDS.get("ValorAberto"):
                crm_val = get_biz_field(biz["data"], BIZ_FIELD_IDS["ValorAberto"])
                if str(crm_val).strip() != str(valor).strip():
                    fields_to_update["ValorAberto"] = valor

            if BIZ_FIELD_IDS.get("DiasAtraso"):
                crm_val = get_biz_field(biz["data"], BIZ_FIELD_IDS["DiasAtraso"])
                if str(crm_val).strip() != str(dias).strip():
                    fields_to_update["DiasAtraso"] = dias

            if BIZ_FIELD_IDS.get("QtdTitulos"):
                crm_val = get_biz_field(biz["data"], BIZ_FIELD_IDS["QtdTitulos"])
                if str(crm_val).strip() != str(qtd).strip():
                    fields_to_update["QtdTitulos"] = qtd

            if BIZ_FIELD_IDS.get("StatusFinanceiro"):
                crm_val = get_biz_field(biz["data"], BIZ_FIELD_IDS["StatusFinanceiro"])
                if str(crm_val).strip().lower() != "inadimplente":
                    fields_to_update["StatusFinanceiro"] = "Inadimplente"

            if fields_to_update:
                lead = biz["data"].get("lead", {})
                nome = lead.get("name", "") if isinstance(lead, dict) else ""
                updates.append({
                    "biz_id": biz_id,
                    "rgm": rgm,
                    "nome": nome or rgm,
                    "fields": fields_to_update,
                })
                stats["a_atualizar"] += 1
            else:
                stats["sem_alteracao"] += 1

    return updates, stats


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
    log.info("ATUALIZAÇÃO DE INADIMPLENTES — %s", mode.upper())
    log.info("=" * 50)

    conn = get_conn()
    try:
        _discover_field_ids(conn)
        snap, inad_by_rgm = load_inadimplentes_snapshot(conn)
        if not snap:
            log.error("Nenhum snapshot de inadimplentes encontrado. Faça upload primeiro.")
            return

        crm_by_rgm = load_crm_businesses(conn)
    finally:
        conn.close()

    updates, stats = prepare_updates(inad_by_rgm, crm_by_rgm)

    log.info("Resumo:")
    for k, v in sorted(stats.items()):
        log.info("  %s: %d", k, v)
    log.info("  Total a atualizar: %d negócios", len(updates))

    if mode == "--dry-run":
        REPORTS_DIR.mkdir(exist_ok=True)
        preview = REPORTS_DIR / "inadimplentes_preview.csv"
        with open(preview, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["biz_id", "rgm", "nome", "campos"])
            for u in updates[:200]:
                w.writerow([u["biz_id"], u["rgm"], u["nome"], json.dumps(u["fields"], ensure_ascii=False)])
        log.info("Preview salvo: %s (%d primeiros)", preview, min(len(updates), 200))
        log.info("Para executar: python update_inadimplentes.py --execute")
        return

    if not updates:
        log.info("Nada a atualizar.")
        return

    api = ApiClient(target_rate=rate_limit)
    conn = get_conn()

    LOG_DIR.mkdir(exist_ok=True)
    ts = datetime.now(BRT).strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"inadimplentes_{ts}.csv"

    ok_count = 0
    err_count = 0
    start = time.monotonic()

    with open(log_file, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["timestamp", "biz_id", "rgm", "nome", "campo", "valor", "resultado"])

        for idx, upd in enumerate(updates, 1):
            biz_id = upd["biz_id"]

            for field_key, value in upd["fields"].items():
                field_id = BIZ_FIELD_IDS.get(field_key)
                if not field_id:
                    continue

                r = api.put_biz_field(biz_id, field_id, value)
                ts_now = datetime.now(BRT).strftime("%H:%M:%S")

                if r["ok"]:
                    ok_count += 1
                    update_local_biz_field(conn, biz_id, field_id, value)
                    w.writerow([ts_now, biz_id[:12], upd["rgm"], upd["nome"], field_key, value, "OK"])
                else:
                    err_count += 1
                    w.writerow([ts_now, biz_id[:12], upd["rgm"], upd["nome"], field_key, value, f"ERRO:{r['status']}"])
                    log.warning("[%d/%d] ERRO %s campo %s: %s", idx, len(updates), upd["nome"], field_key, r["body"][:120])

            if idx % 100 == 0 or idx == len(updates):
                elapsed = time.monotonic() - start
                log.info("[%d/%d] OK:%d ERR:%d API:%d (%.1f min)",
                         idx, len(updates), ok_count, err_count, api.total_calls, elapsed / 60)

    conn.close()
    elapsed = time.monotonic() - start
    log.info("Concluído em %.1f min. OK: %d | Erros: %d | API calls: %d", elapsed / 60, ok_count, err_count, api.total_calls)
    log.info("Log: %s", log_file)


if __name__ == "__main__":
    main()
