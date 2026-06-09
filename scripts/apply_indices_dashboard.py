"""
Aplica os indices definidos em sql/indices_dashboard_comercial.sql.

Executa cada CREATE INDEX em transacao propria (autocommit), de forma que
um IF NOT EXISTS sobre indice ja existente nao aborta os seguintes.

Usa as mesmas credenciais que `routes/comercial_rgm.py` (.env carregado por
helpers.py). Cada arquivo de saida indica:
  - quais indices foram criados, ja existentes, ou falharam
  - em qual banco (principal vs kommo)
"""
import os
import sys
import time

# Garante que rodemos a partir da raiz do projeto
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
os.chdir(_ROOT)

from dotenv import load_dotenv
load_dotenv()

import psycopg2

# DSNs (mesmo padrao de routes/comercial_rgm.py)
DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)
KOMMO_DB_DSN = dict(
    host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
    port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
    user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
    password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
    dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
)

# Mapeia cada indice ao banco em que ele deve ser criado
INDICES_PRINCIPAL = [
    ("idx_crgm_rgm",
     "CREATE INDEX IF NOT EXISTS idx_crgm_rgm ON comercial_rgm (rgm)"),
    ("idx_xl_rows_snapshot_id",
     "CREATE INDEX IF NOT EXISTS idx_xl_rows_snapshot_id ON xl_rows (snapshot_id)"),
    ("idx_xl_rows_data_rgm",
     "CREATE INDEX IF NOT EXISTS idx_xl_rows_data_rgm ON xl_rows ((data->>'rgm'))"),
    ("idx_xl_snapshots_tipo_id",
     "CREATE INDEX IF NOT EXISTS idx_xl_snapshots_tipo_id ON xl_snapshots (tipo, id DESC)"),
]

INDICES_KOMMO = [
    ("idx_leads_resp_created",
     "CREATE INDEX IF NOT EXISTS idx_leads_resp_created ON leads (responsible_user_id, created_at) WHERE is_deleted = false"),
    ("idx_leads_resp_status_closed",
     "CREATE INDEX IF NOT EXISTS idx_leads_resp_status_closed ON leads (responsible_user_id, status_id, closed_at) WHERE is_deleted = false"),
    ("idx_leads_status_deleted_id",
     "CREATE INDEX IF NOT EXISTS idx_leads_status_deleted_id ON leads (status_id, is_deleted, id)"),
    ("idx_lcfv_field_lead",
     "CREATE INDEX IF NOT EXISTS idx_lcfv_field_lead ON lead_custom_field_values (lead_id) WHERE lower(field_name) = 'rgm'"),
]


def _run(dsn, indices, label):
    print(f"\n=== {label} ({dsn.get('host')}:{dsn.get('port')}/{dsn.get('dbname')}) ===")
    try:
        conn = psycopg2.connect(**dsn)
    except Exception as e:
        print(f"  ERRO conexao: {e}")
        return
    conn.autocommit = True
    cur = conn.cursor()
    for name, sql in indices:
        t0 = time.perf_counter()
        try:
            cur.execute(sql)
            dt = time.perf_counter() - t0
            print(f"  OK  {name:32s} ({dt:.2f}s)")
        except Exception as e:
            print(f"  ERR {name:32s} -> {e}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    _run(DB_DSN, INDICES_PRINCIPAL, "Banco PRINCIPAL")
    _run(KOMMO_DB_DSN, INDICES_KOMMO, "Banco KOMMO")
    print("\nConcluido.")
