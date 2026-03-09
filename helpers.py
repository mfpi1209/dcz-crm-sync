"""
eduit. — Constantes e utilitários compartilhados.
"""

import os
import re
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ---------------------------------------------------------------------------
# Timezone
# ---------------------------------------------------------------------------

BRT = timezone(timedelta(hours=-3))


def to_brt(dt):
    """Convert a datetime to BRT (UTC-3) string."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BRT).strftime("%d/%m/%Y %H:%M:%S")
    return str(dt)


# ---------------------------------------------------------------------------
# Autenticação — constantes
# ---------------------------------------------------------------------------

ALL_PAGES = [
    "dashboard", "search", "sync", "kommo_sync", "update", "pipeline", "match_merge",
    "comercial_rgm", "logs", "distribuicao", "ativacoes", "intelligence", "inadimplencia",
    "feedback", "config", "schedule", "inscricao",
]

APP_USER_FALLBACK = os.getenv("APP_USER", "admin")
APP_PASS_FALLBACK = os.getenv("APP_PASS", "")


def _hash_pw(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
SYNC_SCRIPT = str(BASE_DIR / "sync.py")
UPDATE_SCRIPT = str(BASE_DIR / "update_crm.py")
SANITIZE_SCRIPT = str(BASE_DIR / "sanitize_crm.py")
PIPELINE_SCRIPT = str(BASE_DIR / "pipeline_crm.py")
ENRICH_SCRIPT = str(BASE_DIR / "enrich_crosslead.py")
MERGE_SCRIPT = str(BASE_DIR / "merge_leads.py")
INADIMPLENTES_SCRIPT = str(BASE_DIR / "update_inadimplentes.py")
CONCLUINTES_SCRIPT = str(BASE_DIR / "update_concluintes.py")
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

MAX_LOG_LINES = 2000

# ---------------------------------------------------------------------------
# Field IDs (custom fields do CRM)
# ---------------------------------------------------------------------------

FIELD_RGM = "2ac4e30f-cfd7-435f-b688-fbce27f76c38"

TIPO_ALUNO_FIELD = "4230e4db-970b-4444-abaf-c3135a03b79c"
DATA_MATRICULA_FIELD = "bf93a8e9-42c0-4517-8518-6f604746a300"
SITUACAO_FIELD = "fd08d44b-a4a5-4343-b7a9-37f75e2c1caa"
NIVEL_FIELD = "233fcf6f-0bed-49d7-89a1-d1cd54fb9c12"
POLO_FIELD = "0ec9d8dc-d547-4482-b9ad-d4a3e6ec1b54"
TURMA_FIELD = "8815a8de-f755-4597-b6f4-8da6d289b6eb"

# ---------------------------------------------------------------------------
# SQL Queries
# ---------------------------------------------------------------------------

SEARCH_QUERY = """
SELECT
    l.id                                  AS lead_id,
    l.data->>'name'                       AS lead_nome,
    l.data->>'phone'                      AS lead_telefone,
    l.data->>'rawPhone'                   AS lead_telefone_raw,
    l.data->>'email'                      AS lead_email,
    l.data->>'source'                     AS lead_origem,
    l.data->>'taxId'                      AS lead_cpf,
    l.data->'address'->>'city'            AS lead_cidade,
    l.data->'address'->>'state'           AS lead_estado,
    l.data->>'createdAt'                  AS lead_criado_em,

    b.id                                  AS negocio_id,
    b.data->>'code'                       AS negocio_codigo,
    b.data->>'status'                     AS negocio_status,
    b.data->>'total'                      AS negocio_valor,
    b.data->>'createdAt'                  AS negocio_criado_em,
    b.data->>'lastMovedAt'                AS negocio_movido_em,

    p.data->>'name'                       AS pipeline_nome,
    ps.data->>'name'                      AS etapa_nome,
    ps.data->>'color'                     AS etapa_cor,

    b.data->'attendant'->>'name'          AS atendente,

    biz_cf.campos                         AS campos_negocio,
    lead_cf.campos                        AS campos_lead

FROM businesses b
LEFT JOIN leads l            ON l.id  = b.data->>'leadId'
LEFT JOIN pipeline_stages ps ON ps.id = b.data->>'stageId'
LEFT JOIN pipelines p        ON p.id  = ps.pipeline_id
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(
        elem->'additionalField'->>'name',
        COALESCE(elem->>'value', '')
    ) AS campos
    FROM jsonb_array_elements(b.data->'additionalFields') elem
    WHERE elem->'additionalField'->>'name' IS NOT NULL
) biz_cf ON true
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(
        elem->'additionalField'->>'name',
        COALESCE(elem->>'value', '')
    ) AS campos
    FROM jsonb_array_elements(l.data->'additionalFields') elem
    WHERE elem->'additionalField'->>'name' IS NOT NULL
) lead_cf ON true
WHERE (
    (%(cpf)s != '' AND REPLACE(REPLACE(l.data->>'taxId', '.', ''), '-', '') LIKE '%%' || REPLACE(REPLACE(%(cpf)s, '.', ''), '-', '') || '%%')
    OR (%(rgm)s != '' AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(b.data->'additionalFields') e
        WHERE e->'additionalField'->>'id' = '2ac4e30f-cfd7-435f-b688-fbce27f76c38'
          AND e->>'value' LIKE '%%' || %(rgm)s || '%%'
    ))
    OR (%(telefone)s != '' AND (
        l.data->>'rawPhone' LIKE '%%' || %(telefone)s || '%%'
        OR REPLACE(REPLACE(REPLACE(REPLACE(l.data->>'phone', ' ', ''), '(', ''), ')', ''), '-', '') LIKE '%%' || %(telefone)s || '%%'
    ))
)
ORDER BY b.data->>'lastMovedAt' DESC NULLS LAST
LIMIT 50;
"""

RECENT_BIZ_UPDATES_QUERY = """
SELECT
    'negocio' AS tipo,
    b.id,
    b.data->'lead'->>'name' AS nome_lead,
    b.data->>'status' AS status,
    p.data->>'name' AS pipeline,
    ps.data->>'name' AS etapa,
    b.synced_at
FROM businesses b
LEFT JOIN pipeline_stages ps ON ps.id = b.data->>'stageId'
LEFT JOIN pipelines p ON p.id = ps.pipeline_id
WHERE b.synced_at = (SELECT MAX(synced_at) FROM businesses)
ORDER BY b.synced_at DESC
LIMIT 10;
"""

SYNC_STATE_QUERY = """
SELECT entity_type, last_sync_at, last_full_sync_at, run_count
FROM sync_state ORDER BY entity_type;
"""

# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------


def _normalize_digits(s):
    """Remove tudo exceto dígitos, tratando floats do Excel (46901353.0 → 46901353)."""
    if not s:
        return ""
    if isinstance(s, float) and s == int(s):
        s = int(s)
    raw = str(s).strip()
    if re.match(r"^\d+\.0+$", raw):
        raw = raw.split(".")[0]
    return re.sub(r"\D", "", raw)


# ---------------------------------------------------------------------------
# Tipos de planilha
# ---------------------------------------------------------------------------

XL_TIPOS = ["matriculados", "inadimplentes", "concluintes", "acesso_ava", "sem_rematricula"]
