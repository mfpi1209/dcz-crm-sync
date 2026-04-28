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

print("=== Colunas da tabela leads ===")
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name='leads' AND table_schema='public'
    ORDER BY ordinal_position
""")
for r in cur.fetchall():
    print(f"  {r[0]:<30}  {r[1]}")

print("\n=== Dados do lead 20300621 ===")
cur.execute("SELECT * FROM leads WHERE id = 20300621")
cols = [d[0] for d in cur.description]
row = cur.fetchone()
if row:
    for c, v in zip(cols, row):
        if isinstance(v, (list, dict)):
            v = str(v)[:200]
        print(f"  {c:<30}  {v}")
else:
    print("  (lead nao encontrado)")

cur.close()
conn.close()
