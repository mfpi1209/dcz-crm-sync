"""
Prova objetiva: SQLite (staging do main.py) vs PostgreSQL kommo_sync (pós migrate).

Rode ANTES de decidir outro Full Sync — mostra se os dois bancos batem.
Rode DEPOIS de Full Sync + migrate — se contagens e max(synced_at) alinharem, o pipeline funcionou.

Uso:
  cd kommo_lib
  python verify_sync_mirror.py
  python verify_sync_mirror.py --sample 15

Não chama a API Kommo; só lê SQLite + PostgreSQL (.env).
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2

# Mesmos defaults que migrate_to_postgres / config
SQLITE_PATH = os.getenv("KOMMO_DB_PATH", os.path.join(os.path.dirname(__file__), "kommo_sync.db"))
PG = {
    "host": os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
    "port": int(os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432"))),
    "dbname": os.getenv("KOMMO_PG_DB", "kommo_sync"),
    "user": os.getenv("KOMMO_PG_USER", os.getenv("DB_USER", "")),
    "password": os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS", "")),
}


def _norm_ts(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def main():
    ap = argparse.ArgumentParser(description="Compara SQLite vs PostgreSQL (leads)")
    ap.add_argument("--sample", type=int, default=10, help="Quantos exemplos de divergência mostrar")
    args = ap.parse_args()

    if not os.path.isfile(SQLITE_PATH):
        print(f"ERRO: SQLite não encontrado: {SQLITE_PATH}")
        sys.exit(1)

    sq = sqlite3.connect(SQLITE_PATH)
    sq.row_factory = sqlite3.Row
    cur_s = sq.cursor()

    cur_s.execute("SELECT COUNT(*), MAX(synced_at), MIN(synced_at) FROM leads")
    n_s, max_s, min_s = cur_s.fetchone()

    try:
        pg = psycopg2.connect(**PG)
        cur_p = pg.cursor()
        cur_p.execute("SELECT COUNT(*), MAX(synced_at), MIN(synced_at) FROM leads")
        n_p, max_p, min_p = cur_p.fetchone()

        cur_p.execute(
            """
            SELECT entity_type, last_sync_at, last_full_sync_at, records_synced, status
            FROM sync_metadata WHERE entity_type = 'leads'
            """
        )
        meta = cur_p.fetchone()
    except Exception as e:
        print(f"ERRO PostgreSQL: {e}")
        sq.close()
        sys.exit(1)

    print("=" * 60)
    print("VERIFICACAO SQLite <-> PostgreSQL (tabela leads)")
    print("=" * 60)
    print(f"SQLite path:  {SQLITE_PATH}")
    print(f"PostgreSQL:   {PG['user']}@{PG['host']}:{PG['port']}/{PG['dbname']}")
    print()
    print(f"{'':20} {'SQLite':>24} {'PG':>24}")
    print(f"{'COUNT(leads)':20} {n_s:>24,} {n_p:>24,}")
    print(f"{'MAX(synced_at)':20} {str(max_s or '-')[:24]:>24} {str(max_p or '-')[:24]:>24}")
    print(f"{'MIN(synced_at)':20} {str(min_s or '-')[:24]:>24} {str(min_p or '-')[:24]:>24}")
    print()
    if meta:
        print("sync_metadata (entity_type=leads):")
        print(f"  last_sync_at = {meta[1]}")
        print(f"  last_full_sync_at = {meta[2]}")
        print(f"  records_synced = {meta[3]}")
        print(f"  status = {meta[4]}")
    else:
        print("sync_metadata: sem linha para entity_type='leads'")
    print()

    # Divergências: SQLite mais novo que PG (migração não atualizou a linha)
    cur_s.execute("SELECT id, synced_at FROM leads")
    stale = []
    missing_pg = 0
    checked = 0
    cur_p.execute("SELECT id, synced_at FROM leads")
    pg_map = {row[0]: row[1] for row in cur_p.fetchall()}

    for row in cur_s:
        checked += 1
        lid, st = int(row[0]), _norm_ts(row[1])
        pt = pg_map.get(lid)
        if pt is None:
            missing_pg += 1
            if len(stale) < args.sample * 3:
                stale.append((lid, "FALTA_NO_PG", st, None))
            continue
        pt = _norm_ts(pt)
        if st > pt:
            stale.append((lid, "SQLITE_MAIS_NOVO", st, pt))

    stale_show = [x for x in stale if x[1] == "SQLITE_MAIS_NOVO"][: args.sample]
    missing_show = [x for x in stale if x[1] == "FALTA_NO_PG"][: args.sample]

    sqlite_newer = sum(1 for x in stale if x[1] == "SQLITE_MAIS_NOVO")

    print("-" * 60)
    print(f"Leads so no SQLite (faltam no PG):     {missing_pg:,}")
    print(f"Leads com SQLite.synced_at > PG:       {sqlite_newer:,}")
    print("-" * 60)

    if sqlite_newer == 0 and missing_pg == 0 and n_s == n_p:
        print("OK: contagens iguais e nenhum lead com staging mais novo que o PG.")
        print("    O espelho PostgreSQL esta alinhado ao SQLite local.")
    elif n_s != n_p:
        print(f"ATENCAO: COUNT difere ({n_s} vs {n_p}). Migracao incompleta ou bases diferentes.")
    if sqlite_newer > 0:
        print(f"ATENCAO: {sqlite_newer:,} leads com SQLite.synced_at mais recente que no PG.")
        print("         Rode: python migrate_to_postgres.py  (sem --light, apos full sync)")
    if missing_pg > 0:
        print(f"ATENCAO: {missing_pg:,} leads so no SQLite (nao existem no PG).")

    if missing_show:
        print()
        print(f"Exemplos (faltam no PG), ate {args.sample}:")
        for lid, _k, st, pt in missing_show:
            print(f"  id={lid}  sqlite.synced_at={st}")

    if stale_show:
        print()
        print(f"Exemplos (SQLite mais novo que PG), ate {args.sample}:")
        for lid, _k, st, pt in stale_show:
            print(f"  id={lid}  sqlite={st[:19]}...  pg={str(pt)[:19]}...")

    print("=" * 60)
    print("Proximo passo se houver divergencia:")
    print("  1) Sync no painel (full ou incremental) ate SQLite estar atualizado")
    print("  2) python migrate_to_postgres.py   (sem --light = copia tudo para o PG)")
    print("  3) Rodar este script de novo.")
    print("=" * 60)

    sq.close()
    cur_p.close()
    pg.close()

    if n_s == n_p and sqlite_newer == 0 and missing_pg == 0:
        sys.exit(0)
    sys.exit(2)


if __name__ == "__main__":
    main()
