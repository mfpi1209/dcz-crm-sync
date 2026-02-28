"""
DataCrazy CRM Sync — Sincronização delta para PostgreSQL (JSONB).

Uso:
    python sync.py              # sync incremental
    python sync.py --full       # força full sync de todas as entidades
"""

import os
import sys
import json
import hashlib
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

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

PAGE_SIZE = 1000
FULL_SYNC_EVERY = 10        # full re-scan de leads a cada N execuções
RATE_LIMIT_BUFFER = 5       # margem de segurança antes de pausar
MIN_REQUEST_DELAY = 0.55    # delay mínimo entre requests (seg) — ~109 req/min max

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dcz")

# ---------------------------------------------------------------------------
# Schema SQL (embarcado para auto-criação)
# ---------------------------------------------------------------------------

_SCHEMA = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")

# ---------------------------------------------------------------------------
# API Client com prevenção de 429
# ---------------------------------------------------------------------------

class ApiClient:
    """Cliente HTTP com rate-limit proativo."""

    def __init__(self, base: str, token: str):
        self.base = base
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {token}"
        self._remaining = 60
        self._reset = 0
        self._last_req = 0.0

    # -- throttle ----------------------------------------------------------

    def _throttle(self):
        """Garante delay mínimo entre requests e pausa proativa se perto do limite."""
        elapsed = time.monotonic() - self._last_req
        if elapsed < MIN_REQUEST_DELAY:
            time.sleep(MIN_REQUEST_DELAY - elapsed)

        if self._remaining <= RATE_LIMIT_BUFFER and self._reset > 0:
            wait = self._reset + 1
            log.warning("Rate-limit próximo (%d restantes) — pausando %ds", self._remaining, wait)
            time.sleep(wait)

    def _read_headers(self, r: requests.Response):
        self._remaining = int(r.headers.get("X-RateLimit-Remaining", self._remaining))
        self._reset = int(r.headers.get("X-RateLimit-Reset", 0))

    # -- GET com retry -----------------------------------------------------

    def get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base}{path}"
        for attempt in range(4):
            self._throttle()
            self._last_req = time.monotonic()

            r = self.s.get(url, params=params, timeout=30)

            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 30))
                log.warning("429 — Retry-After %ds (tentativa %d/4)", retry, attempt + 1)
                time.sleep(retry + 1)
                continue

            self._read_headers(r)
            r.raise_for_status()
            return r.json()

        raise RuntimeError(f"Falha após 4 tentativas: GET {path}")

    # -- paginação ---------------------------------------------------------

    def paginate(self, path: str, params: dict | None = None) -> list:
        """Percorre todas as páginas e retorna lista completa."""
        all_items: list[dict] = []
        skip = 0
        last_pct = -1

        while True:
            p = {"skip": skip, "take": PAGE_SIZE}
            if params:
                p.update(params)

            body = self.get(path, p)
            data = body.get("data", [])
            total = body.get("count", 0)

            all_items.extend(data)
            skip += PAGE_SIZE

            pct = int(len(all_items) / total * 100) if total else 100
            if pct >= 100 or pct >= last_pct + 10:
                log.info("  %s  %d%%  (%d/%d)", path, pct, len(all_items), total)
                last_pct = pct

            if skip >= total:
                break

        return all_items

# ---------------------------------------------------------------------------
# Banco de dados
# ---------------------------------------------------------------------------

def connect():
    return psycopg2.connect(**DB_DSN)


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(_SCHEMA)
    conn.commit()
    log.info("Schema OK")


def md5(data: dict) -> str:
    return hashlib.md5(
        json.dumps(data, sort_keys=True, default=str).encode()
    ).hexdigest()

# ---------------------------------------------------------------------------
# sync_state helpers
# ---------------------------------------------------------------------------

def get_state(conn, entity: str) -> dict:
    with conn.cursor() as c:
        c.execute(
            "SELECT last_sync_at, last_full_sync_at, run_count "
            "FROM sync_state WHERE entity_type = %s",
            (entity,),
        )
        row = c.fetchone()
    if row:
        return {"last_sync_at": row[0], "last_full_sync_at": row[1], "runs": row[2] or 0}
    return {"last_sync_at": None, "last_full_sync_at": None, "runs": 0}


def set_state(conn, entity: str, now: datetime, is_full: bool):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO sync_state (entity_type, last_sync_at, last_full_sync_at, run_count)
            VALUES (%s, %s, %s, 1)
            ON CONFLICT (entity_type) DO UPDATE SET
                last_sync_at      = EXCLUDED.last_sync_at,
                last_full_sync_at = CASE WHEN %s THEN %s ELSE sync_state.last_full_sync_at END,
                run_count         = sync_state.run_count + 1
            """,
            (entity, now, now if is_full else None, is_full, now),
        )
    conn.commit()

# ---------------------------------------------------------------------------
# Upsert genérico JSONB (com hash para detectar mudanças reais)
# ---------------------------------------------------------------------------

_PRESERVE_KEYS = ("address", "additionalFields", "birthDate", "taxId")


def _merge_preserve(api_rec: dict, local_rec: dict) -> dict:
    """Mescla dados da API com dados locais, preservando campos que a API pode não retornar."""
    merged = dict(api_rec)
    for key in _PRESERVE_KEYS:
        api_val = api_rec.get(key)
        local_val = local_rec.get(key)

        if key == "additionalFields":
            api_fields = api_val if isinstance(api_val, list) else []
            local_fields = local_val if isinstance(local_val, list) else []
            if not local_fields:
                continue
            api_field_ids = set()
            for f in api_fields:
                af = f.get("additionalField", {})
                fid = af.get("id") if isinstance(af, dict) else af
                if fid:
                    api_field_ids.add(fid)
            for lf in local_fields:
                af = lf.get("additionalField", {})
                fid = af.get("id") if isinstance(af, dict) else af
                if fid and fid not in api_field_ids:
                    api_fields.append(lf)
            merged["additionalFields"] = api_fields

        elif key == "address":
            if isinstance(local_val, dict) and local_val:
                if not api_val or not isinstance(api_val, dict):
                    merged["address"] = local_val
                else:
                    addr = dict(local_val)
                    addr.update({k: v for k, v in api_val.items() if v})
                    merged["address"] = addr

        else:
            if not api_val and local_val:
                merged[key] = local_val

    return merged


def upsert(conn, table: str, records: list[dict], now: datetime, *, use_hash: bool = True):
    """Batch upsert otimizado — reduz round-trips ao DB remoto.
    Para updates, mescla campos locais que a API pode não retornar."""
    ins = upd = unch = 0
    if not records:
        return ins, upd, unch

    BATCH = 500

    if use_hash:
        prepared = []
        for rec in records:
            rid = rec.get("id")
            if not rid:
                continue
            prepared.append((rid, rec, md5(rec), rec.get("createdAt")))

        with conn.cursor() as c:
            for i in range(0, len(prepared), BATCH):
                batch = prepared[i:i + BATCH]
                batch_ids = [r[0] for r in batch]

                c.execute(
                    f"SELECT id, data_hash, data FROM {table} WHERE id = ANY(%s)",
                    (batch_ids,),
                )
                existing = {}
                existing_data = {}
                for row in c.fetchall():
                    existing[row[0]] = row[1]
                    existing_data[row[0]] = row[2] if isinstance(row[2], dict) else json.loads(row[2]) if row[2] else {}

                to_insert = []
                to_update = []
                for rid, rec, h, created in batch:
                    if rid not in existing:
                        to_insert.append((rid, json.dumps(rec, default=str), h, now, created))
                        ins += 1
                    elif existing[rid] != h:
                        merged = _merge_preserve(rec, existing_data.get(rid, {}))
                        merged_h = md5(merged)
                        if merged_h != existing[rid]:
                            to_update.append((json.dumps(merged, default=str), merged_h, now, rid))
                            upd += 1
                        else:
                            unch += 1
                    else:
                        unch += 1

                if to_insert:
                    from psycopg2.extras import execute_values
                    execute_values(
                        c,
                        f"INSERT INTO {table} (id, data, data_hash, synced_at, created_at) VALUES %s "
                        f"ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data, data_hash=EXCLUDED.data_hash, synced_at=EXCLUDED.synced_at",
                        to_insert,
                    )

                if to_update:
                    from psycopg2.extras import execute_batch
                    execute_batch(
                        c,
                        f"UPDATE {table} SET data=%s, data_hash=%s, synced_at=%s WHERE id=%s",
                        to_update,
                    )

        conn.commit()
    else:
        from psycopg2.extras import execute_values
        vals = []
        for rec in records:
            rid = rec.get("id")
            if not rid:
                continue
            vals.append((rid, json.dumps(rec, default=str), now))
            ins += 1

        with conn.cursor() as c:
            for i in range(0, len(vals), BATCH):
                execute_values(
                    c,
                    f"INSERT INTO {table} (id, data, synced_at) VALUES %s "
                    f"ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data, synced_at=EXCLUDED.synced_at",
                    vals[i:i + BATCH],
                )
        conn.commit()

    return ins, upd, unch

# ---------------------------------------------------------------------------
# Sync: Pipelines + Stages
# ---------------------------------------------------------------------------

def sync_pipelines(api: ApiClient, conn, now: datetime):
    log.info("--- Pipelines ---")
    body = api.get("/pipelines")
    pipes = body.get("data", body) if isinstance(body, dict) else body

    upsert(conn, "pipelines", pipes, now, use_hash=False)
    log.info("  %d pipelines", len(pipes))

    for p in pipes:
        pid = p["id"]
        body = api.get(f"/pipelines/{pid}/stages")
        stages = body.get("data", []) if isinstance(body, dict) else body

        with conn.cursor() as c:
            for st in stages:
                jdata = json.dumps(st, default=str)
                c.execute(
                    "INSERT INTO pipeline_stages (id, pipeline_id, data, synced_at) "
                    "VALUES (%s,%s,%s,%s) "
                    "ON CONFLICT (id) DO UPDATE SET "
                    "  pipeline_id=EXCLUDED.pipeline_id, data=EXCLUDED.data, synced_at=EXCLUDED.synced_at",
                    (st["id"], pid, jdata, now),
                )
        conn.commit()
        log.info("  Pipeline '%s': %d etapas", p.get("name", pid), len(stages))

    set_state(conn, "pipelines", now, True)

# ---------------------------------------------------------------------------
# Sync: Tags
# ---------------------------------------------------------------------------

def sync_tags(api: ApiClient, conn, now: datetime):
    log.info("--- Tags ---")
    body = api.get("/tags")
    tags = body if isinstance(body, list) else body.get("data", [body] if "id" in body else [])

    upsert(conn, "tags", tags, now, use_hash=False)
    log.info("  %d tags", len(tags))
    set_state(conn, "tags", now, True)

# ---------------------------------------------------------------------------
# Sync: Leads (delta + full periódico)
# ---------------------------------------------------------------------------

def sync_leads(api: ApiClient, conn, now: datetime, *, force_full: bool = False):
    log.info("--- Leads ---")
    state = get_state(conn, "leads")
    first_run = state["last_sync_at"] is None
    do_full = force_full or first_run or (state["runs"] % FULL_SYNC_EVERY == 0)

    base_params = {"complete[additionalFields]": "true"}

    if do_full:
        reason = "primeira execução" if first_run else f"a cada {FULL_SYNC_EVERY} runs"
        if force_full:
            reason = "--full"
        log.info("  FULL SYNC (%s)", reason)
        leads = api.paginate("/leads", base_params)
    else:
        since = state["last_sync_at"].isoformat()
        log.info("  DELTA — novos desde %s", since)
        base_params["filter[createdAtGreaterOrEqual]"] = since
        leads = api.paginate("/leads", base_params)

    log.info("  Gravando %d leads no banco...", len(leads))
    ins, upd, unch = upsert(conn, "leads", leads, now)
    log.info("  Leads: +%d novos, ~%d atualizados, =%d inalterados", ins, upd, unch)
    set_state(conn, "leads", now, do_full)

# ---------------------------------------------------------------------------
# Sync: Negócios (delta real via lastMovedAfter + createdAt)
# ---------------------------------------------------------------------------

def sync_businesses(api: ApiClient, conn, now: datetime, *, force_full: bool = False):
    log.info("--- Negócios ---")
    state = get_state(conn, "businesses")
    first_run = state["last_sync_at"] is None

    base_params = {"complete[additionalFields]": "true"}

    if force_full or first_run:
        reason = "primeira execução" if first_run else "--full"
        log.info("  FULL SYNC (%s)", reason)
        biz = api.paginate("/businesses", base_params)
    else:
        since = state["last_sync_at"].isoformat()
        log.info("  DELTA — desde %s", since)

        p1 = {**base_params, "filter[createdAtGreaterOrEqual]": since}
        p2 = {**base_params, "filter[lastMovedAfter]": since}
        new_biz = api.paginate("/businesses", p1)
        moved_biz = api.paginate("/businesses", p2)

        seen: dict[str, dict] = {}
        for b in new_biz + moved_biz:
            seen[b["id"]] = b
        biz = list(seen.values())
        log.info("  Delta: %d novos + %d movidos = %d únicos", len(new_biz), len(moved_biz), len(biz))

    log.info("  Gravando %d negócios no banco...", len(biz))
    ins, upd, unch = upsert(conn, "businesses", biz, now)
    log.info("  Negócios: +%d novos, ~%d atualizados, =%d inalterados", ins, upd, unch)
    set_state(conn, "businesses", now, force_full or first_run)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    force_full = "--full" in sys.argv

    log.info("=" * 50)
    log.info("DataCrazy CRM Sync")
    log.info("=" * 50)

    if not API_TOKEN:
        log.error("DATACRAZY_API_TOKEN ausente no .env")
        sys.exit(1)

    api = ApiClient(API_BASE, API_TOKEN)
    conn = connect()

    try:
        ensure_schema(conn)
        now = datetime.now(timezone.utc)

        sync_pipelines(api, conn, now)
        sync_tags(api, conn, now)
        sync_leads(api, conn, now, force_full=force_full)
        sync_businesses(api, conn, now, force_full=force_full)

        log.info("=" * 50)
        log.info("Sincronização concluída com sucesso")
        log.info("=" * 50)

    except Exception:
        log.exception("Erro durante sincronização")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
