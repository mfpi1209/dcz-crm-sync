"""
Aplica sql/motor_ativacoes.sql (tabelas motor_ do Motor de Ativações) no dcz_sync.

Idempotente. Só cria tabelas e índices com prefixo motor_; não toca em mais nada.
Usa as credenciais DB_* do .env (as mesmas do painel).

Uso:
    python scripts/apply_motor_sql.py            # mostra o alvo e pede confirmação
    python scripts/apply_motor_sql.py --sim      # aplica sem perguntar
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
os.chdir(_ROOT)

from dotenv import load_dotenv

load_dotenv()

import psycopg2


def main() -> int:
    host = os.getenv("DB_HOST", "localhost")
    dbname = os.getenv("DB_NAME", "dcz_sync")
    print(f"Alvo: banco '{dbname}' em '{host}'. Só cria tabelas motor_.")
    if "--sim" not in sys.argv:
        if input("Aplicar? [s/N] ").strip().lower() not in ("s", "sim"):
            print("Nada foi aplicado.")
            return 1
    with open(os.path.join(_ROOT, "sql", "motor_ativacoes.sql"), encoding="utf-8") as f:
        ddl = f.read()
    conn = psycopg2.connect(
        host=host,
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        dbname=dbname,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = current_schema() AND table_name LIKE 'motor\\_%' ORDER BY 1"
            )
            tabelas = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    print("Tabelas motor_ presentes:", ", ".join(tabelas))
    return 0


if __name__ == "__main__":
    sys.exit(main())
