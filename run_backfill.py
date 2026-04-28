"""
Backfill de 90 dias por janelas de 7 dias (evita bug de paginacao do Kommo).
Cada janela usa filter[created_at][from] + filter[created_at][to] e esgota
as paginas dentro daquele intervalo.
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

import psycopg2
from datetime import datetime, timezone, timedelta
from routes.kommo_sync import _kommo_get

BRT      = timezone(timedelta(hours=-3))
DAYS     = 90
WINDOW   = 7   # dias por janela

PG = dict(
    host=os.getenv('KOMMO_PG_HOST', os.getenv('DB_HOST')),
    port=os.getenv('KOMMO_PG_PORT', os.getenv('DB_PORT', '5432')),
    user=os.getenv('KOMMO_PG_USER', os.getenv('DB_USER')),
    password=os.getenv('KOMMO_PG_PASS', os.getenv('DB_PASS')),
    dbname=os.getenv('KOMMO_PG_DB', 'kommo_sync'),
)
conn = psycopg2.connect(**PG)
cur  = conn.cursor()

now    = datetime.now(BRT)
total_inserted = 0
total_skipped  = 0

# Divide em janelas do passado para o presente
windows = []
end_dt = now
for _ in range(DAYS // WINDOW + 1):
    start_dt = end_dt - timedelta(days=WINDOW)
    windows.append((start_dt, end_dt))
    end_dt = start_dt

windows.reverse()  # processa do mais antigo para o mais recente

for w_start, w_end in windows:
    from_ts = int(w_start.timestamp())
    to_ts   = int(w_end.timestamp())
    label   = f"{w_start.strftime('%d/%m')} → {w_end.strftime('%d/%m')}"

    page     = 1
    win_ins  = 0
    win_skip = 0

    while True:
        resp = _kommo_get("/events", {
            "filter[type]":            "entity_responsible_changed",
            "filter[created_at][from]": from_ts,
            "filter[created_at][to]":   to_ts,
            "limit": 250,
            "page":  page,
        })
        if resp.status_code == 204:
            break
        if resp.status_code != 200:
            print(f"  [{label}] ERRO HTTP {resp.status_code}")
            break

        data  = resp.json()
        items = (data.get("_embedded") or {}).get("events") or []
        if not items:
            break

        for ev in items:
            if ev.get("entity_type") != "lead":
                win_skip += 1
                continue
            entity_id  = ev.get("entity_id")
            created_ts = ev.get("created_at")
            if not entity_id or not created_ts:
                win_skip += 1
                continue
            changed_at = datetime.fromtimestamp(created_ts, tz=timezone.utc)
            va = (ev.get("value_after")  or [{}])[0] if ev.get("value_after")  else {}
            vb = (ev.get("value_before") or [{}])[0] if ev.get("value_before") else {}
            to_uid   = (va.get("responsible_user") or {}).get("id") if isinstance(va, dict) else None
            from_uid = (vb.get("responsible_user") or {}).get("id") if isinstance(vb, dict) else None
            if not to_uid:
                win_skip += 1
                continue
            cur.execute("""
                INSERT INTO lead_responsible_history (lead_id, changed_at, from_user_id, to_user_id, source)
                VALUES (%s, %s, %s, %s, 'kommo_events')
                ON CONFLICT (lead_id, changed_at) DO NOTHING
            """, (entity_id, changed_at, from_uid, to_uid))
            win_ins += cur.rowcount

        conn.commit()

        links = data.get("_links") or {}
        if not links.get("next"):
            break
        page += 1

    total_inserted += win_ins
    total_skipped  += win_skip
    if win_ins > 0 or page > 1:
        print(f"  [{label}] pages={page} inseridos={win_ins} ignorados={win_skip}", flush=True)

print(f"\nBackfill concluido! total_inseridos={total_inserted} total_ignorados={total_skipped}")

# Verifica lead 20300621
cur.execute("""
    SELECT lrh.changed_at AT TIME ZONE 'America/Sao_Paulo',
           uf.name, ut.name
    FROM lead_responsible_history lrh
    LEFT JOIN users uf ON uf.id = lrh.from_user_id
    LEFT JOIN users ut ON ut.id = lrh.to_user_id
    WHERE lrh.lead_id = 20300621
    ORDER BY lrh.changed_at
""")
rows = cur.fetchall()
print(f"\nHistorico do lead 20300621 (Karine Gama) — {len(rows)} registro(s):")
for r in rows:
    print(f"  {str(r[0])[:16]}  de: {r[1] or '?'}  para: {r[2] or '?'}")

cur.execute("SELECT COUNT(*) FROM lead_responsible_history")
print(f"\nTotal na tabela: {cur.fetchone()[0]}")
cur.close(); conn.close()
