"""
Sincroniza lead(s) da API Kommo -> SQLite + PostgreSQL (kommo_sync).

O sync INCREMENTAL do painel só traz leads alterados desde o último cursor;
não atualiza em massa leads antigos no PG. Este script faz GET por ID (igual
foi feito manualmente para 20871179): API -> upsert SQLite -> upsert PostgreSQL.

Uso:
  python sync_one_lead.py 20871179
  python sync_one_lead.py 111 222 333
  python sync_one_lead.py --file ids.txt
  python sync_one_lead.py --file ids.txt --sleep 0.2
"""

import argparse
import sys
import os
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api_client import KommoAPIClient
from database import init_database, upsert_leads_batch, upsert_lead_postgres

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _extract_lead(data: dict) -> dict | None:
    if not data:
        return None
    emb = data.get("_embedded") or {}
    leads = emb.get("leads") or []
    if leads:
        return leads[0]
    if data.get("id") is not None and "name" in data:
        return data
    return None


def sync_one_lead(client: KommoAPIClient, lid: int) -> bool:
    data = client.get(f"leads/{lid}", params={"with": "contacts"})
    lead = _extract_lead(data)
    if not lead:
        logger.error("Lead %s: API não retornou dados.", lid)
        return False
    upsert_leads_batch([lead])
    upsert_lead_postgres(lead)
    logger.info(
        "OK %s | status_id=%s | pipeline_id=%s",
        lead.get("id"),
        lead.get("status_id"),
        lead.get("pipeline_id"),
    )
    return True


def _load_ids_from_file(path: str) -> list[int]:
    out: list[int] = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.isdigit():
                out.append(int(line))
    return out


def main():
    ap = argparse.ArgumentParser(description="Sync lead(s) Kommo -> SQLite + PostgreSQL")
    ap.add_argument("ids", nargs="*", type=int, help="IDs de leads (um ou vários)")
    ap.add_argument(
        "-f", "--file",
        help="Arquivo com um ID de lead por linha (# comentário permitido)",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.15,
        help="Segundos entre cada requisição à API (rate limit). Padrão: 0.15",
    )
    args = ap.parse_args()

    ids: list[int] = list(args.ids)
    if args.file:
        ids.extend(_load_ids_from_file(args.file))
    # únicos, ordem preservada
    seen = set()
    uniq: list[int] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            uniq.append(i)
    ids = uniq

    if not ids:
        ap.print_help()
        sys.exit(1)

    init_database()
    client = KommoAPIClient()
    ok, fail = 0, 0
    for n, lid in enumerate(ids, 1):
        logger.info("[%d/%d] Sincronizando lead %s...", n, len(ids), lid)
        if sync_one_lead(client, lid):
            ok += 1
        else:
            fail += 1
        if n < len(ids) and args.sleep > 0:
            time.sleep(args.sleep)

    logger.info("Concluído: %d OK, %d falha(s), total %d.", ok, fail, len(ids))
    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
