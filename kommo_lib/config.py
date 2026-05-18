"""
Configurações do sync Kommo — lê de variáveis de ambiente.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    # .env na raiz do projeto (Flask); fallback cwd ao rodar python main.py em kommo_lib/
    _root_env = Path(__file__).resolve().parent.parent / ".env"
    if _root_env.is_file():
        load_dotenv(_root_env)
    load_dotenv()
except ImportError:
    pass

def _load_from_app_config(chave, fallback=""):
    """Read a config value from app_config table (shared DB) when env var is empty."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            dbname=os.getenv("DB_NAME", "dcz_sync"),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT valor FROM app_config WHERE chave = %s", (chave,))
            row = cur.fetchone()
        conn.close()
        return row[0] if row else fallback
    except Exception:
        return fallback

_raw_base = os.getenv("KOMMO_BASE_URL", "") or _load_from_app_config("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com/api/v4")
KOMMO_BASE_URL = _raw_base if _raw_base.rstrip("/").endswith("/api/v4") else _raw_base.rstrip("/") + "/api/v4"
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "") or _load_from_app_config("KOMMO_TOKEN", "")

DB_PATH = os.getenv("KOMMO_DB_PATH", os.path.join(os.path.dirname(__file__), "kommo_sync.db"))

RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "120"))
RATE_LIMIT_PERIOD_SECONDS = int(os.getenv("RATE_LIMIT_PERIOD_SECONDS", "60"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("KOMMO_LOG_FILE", os.path.join(os.path.dirname(__file__), "kommo_sync.log"))

PAGE_SIZE = 250

# Lote SQLite/PG por página da API (25 = mais leve no PC; 100+ = sync mais rápido)
BATCH_SIZE = int(os.getenv("KOMMO_BATCH_SIZE", "100"))
# Pausa entre páginas (0 = mais rápido; 0.12 = menos pico de CPU)
SLEEP_BETWEEN_PAGES = float(os.getenv("KOMMO_SLEEP_PAGES", "0"))

# Delta sync: filter[updated_at][from] usa o último sync, mas isso deixa leads antigos no PG
# se o Kommo não os devolveu no intervalo. Com N>0, o "from" nunca é mais recente que (agora − N dias),
# re-buscando alterações dos últimos N dias a cada incremental (mesma gravação que sync_one_lead).
# 0 = desliga (comportamento antigo: só desde last_sync_at − 5 min).
# Incremental: no máximo N dias de alterações (1 = dia a dia mais rápido; 7 = mais seguro)
KOMMO_DELTA_LOOKBACK_DAYS = int(os.getenv("KOMMO_DELTA_LOOKBACK_DAYS", "1"))

PIPELINES = {
    "licenciado": {
        "id": 9994596,
        "stages": {
            "robo": 76715668,
            "ativacao": 77202008,
        }
    },
    "funil_de_vendas": {
        "id": None,
        "stages": {
            "perdido": 143,
            "ganho": 142,
            "aceite": 48566207,
            "pagamento_confirmado": 77728584,
            "boleto_enviado": 48566204,
            "aprovado_reprovado": 48566201,
            "em_processo": 48566198,
            "processo_seletivo": 48566195,
            "inscricao": 48539249,
            "aguardando_inscricao": 99045180,
            "aguardando_resposta": 74941508,
            "em_atendimento": 48539246,
            "sem_resposta": 48539243,
            "contato_inicial": 48539240,
        }
    }
}

ALL_STAGE_IDS = set()
for pipeline_data in PIPELINES.values():
    for stage_id in pipeline_data["stages"].values():
        ALL_STAGE_IDS.add(stage_id)

ALL_PIPELINE_IDS = {
    p["id"] for p in PIPELINES.values() if p["id"] is not None
}
