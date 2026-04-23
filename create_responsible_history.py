import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2

dsn = dict(
    host=os.getenv('KOMMO_PG_HOST', os.getenv('DB_HOST')),
    port=os.getenv('KOMMO_PG_PORT', os.getenv('DB_PORT', '5432')),
    user=os.getenv('KOMMO_PG_USER', os.getenv('DB_USER')),
    password=os.getenv('KOMMO_PG_PASS', os.getenv('DB_PASS')),
    dbname=os.getenv('KOMMO_PG_DB', 'kommo_sync'),
)
conn = psycopg2.connect(**dsn)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS lead_responsible_history (
        lead_id       bigint      NOT NULL,
        changed_at    timestamptz NOT NULL,
        from_user_id  integer,
        to_user_id    integer     NOT NULL,
        source        text        NOT NULL DEFAULT 'kommo_events',
        PRIMARY KEY (lead_id, changed_at)
    )
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_lrh_lead ON lead_responsible_history (lead_id, changed_at DESC)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_lrh_user ON lead_responsible_history (to_user_id, changed_at DESC)")
conn.commit()
cur.execute("SELECT COUNT(*) FROM lead_responsible_history")
print("Table created OK. Rows:", cur.fetchone()[0])
cur.close()
conn.close()
