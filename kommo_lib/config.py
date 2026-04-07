"""
Configurações do sync Kommo — lê de variáveis de ambiente.
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

KOMMO_BASE_URL = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com/api/v4")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")

DB_PATH = os.getenv("KOMMO_DB_PATH", os.path.join(os.path.dirname(__file__), "kommo_sync.db"))

RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "120"))
RATE_LIMIT_PERIOD_SECONDS = int(os.getenv("RATE_LIMIT_PERIOD_SECONDS", "60"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("KOMMO_LOG_FILE", os.path.join(os.path.dirname(__file__), "kommo_sync.log"))

PAGE_SIZE = 250

# Otimização: batches menores = transações menores (menos travamento)
BATCH_SIZE = int(os.getenv("KOMMO_BATCH_SIZE", "25"))
# Pausa entre páginas da API (segundos) para não saturar CPU/disco
SLEEP_BETWEEN_PAGES = float(os.getenv("KOMMO_SLEEP_PAGES", "0.12"))

# Delta sync: filter[updated_at][from] usa o último sync, mas isso deixa leads antigos no PG
# se o Kommo não os devolveu no intervalo. Com N>0, o "from" nunca é mais recente que (agora − N dias),
# re-buscando alterações dos últimos N dias a cada incremental (mesma gravação que sync_one_lead).
# 0 = desliga (comportamento antigo: só desde last_sync_at − 5 min).
KOMMO_DELTA_LOOKBACK_DAYS = int(os.getenv("KOMMO_DELTA_LOOKBACK_DAYS", "7"))

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
