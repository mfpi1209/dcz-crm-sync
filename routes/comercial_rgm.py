"""
eduit. â€” Dashboard Comercial.

Upload de CSV de matrículas (Power BI), integração com dados do Match & Merge,
ranking de agentes comerciais via Kommo, e dashboard com KPIs e comparativos.

Endpoints:
  POST /api/comercial-rgm/upload        upload CSV e importa para o banco
  GET  /api/comercial-rgm/data          dados filtrados (KPIs + evolução + ranking)
  GET  /api/comercial-rgm/filters       listas de polos, níveis e agentes
  GET  /api/comercial-rgm/snapshot-info info do último upload
  POST /api/comercial-rgm/sync-users    sincroniza usuários do Kommo
"""

import os
import csv
from collections import defaultdict
import io
import logging
import re
import time
import requests
from datetime import datetime, date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)

comercial_rgm_bp = Blueprint("comercial_rgm", __name__)

MM_TIPO_MAT_VALIDOS = (
    'INGRESSANTE', 'NOVA MATRICULA', 'NOVA MATRÃCULA', 'RETORNO', 'RECOMPRA'
)

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

KOMMO_BASE_URL = os.getenv("KOMMO_BASE_URL", "https://eduitbr.kommo.com").rstrip("/")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")
# Campo RGM no Kommo (custom field id) â€” usado na busca por RGM na API
KOMMO_RGM_FIELD_ID = int(os.getenv("KOMMO_RGM_FIELD_ID", "31776"))


def _kommo_api_v4() -> str:
    b = KOMMO_BASE_URL.rstrip("/")
    return b if b.endswith("/api/v4") else f"{b}/api/v4"


def _kommo_uid_int(v):
    """IDs de usuário Kommo: o ranking usa int; o PG pode devolver tipos mistos."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _kommo_upsert_lead_postgres(lead: dict) -> None:
    """Grava/atualiza um lead e campos custom direto no PostgreSQL kommo_sync."""
    from psycopg2.extras import Json

    now = datetime.utcnow().isoformat()
    cfs = lead.get("custom_fields_values") or []
    emb = lead.get("_embedded") or {}
    tags = emb.get("tags") or []
    contacts = emb.get("contacts") or []
    k = _pg_kommo()
    c = k.cursor()
    c.execute(
        """
        INSERT INTO leads (
            id, name, price, responsible_user_id, group_id, status_id, pipeline_id,
            loss_reason_id, source_id, created_by, updated_by, closed_at, created_at,
            updated_at, closest_task_at, is_deleted, score, account_id, labor_cost,
            is_price_modified, custom_fields_json, tags_json, contacts_json, raw_json, synced_at
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, price = EXCLUDED.price,
            responsible_user_id = EXCLUDED.responsible_user_id, group_id = EXCLUDED.group_id,
            status_id = EXCLUDED.status_id, pipeline_id = EXCLUDED.pipeline_id,
            loss_reason_id = EXCLUDED.loss_reason_id, source_id = EXCLUDED.source_id,
            created_by = EXCLUDED.created_by, updated_by = EXCLUDED.updated_by,
            closed_at = EXCLUDED.closed_at, created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at, closest_task_at = EXCLUDED.closest_task_at,
            is_deleted = EXCLUDED.is_deleted, score = EXCLUDED.score,
            account_id = EXCLUDED.account_id, labor_cost = EXCLUDED.labor_cost,
            is_price_modified = EXCLUDED.is_price_modified,
            custom_fields_json = EXCLUDED.custom_fields_json, tags_json = EXCLUDED.tags_json,
            contacts_json = EXCLUDED.contacts_json, synced_at = EXCLUDED.synced_at
        """,
        (
            lead["id"],
            lead.get("name"),
            int(lead.get("price") or 0),
            lead.get("responsible_user_id"),
            lead.get("group_id"),
            lead.get("status_id"),
            lead.get("pipeline_id"),
            lead.get("loss_reason_id"),
            lead.get("source_id"),
            lead.get("created_by"),
            lead.get("updated_by"),
            lead.get("closed_at"),
            lead.get("created_at"),
            lead.get("updated_at"),
            lead.get("closest_task_at"),
            bool(lead.get("is_deleted")),
            lead.get("score"),
            lead.get("account_id"),
            lead.get("labor_cost"),
            bool(lead.get("is_price_modified_by_robot")),
            Json(cfs) if cfs else None,
            Json(tags) if tags else None,
            Json(contacts) if contacts else None,
            None,
            now,
        ),
    )
    for cf in cfs:
        c.execute(
            """
            INSERT INTO lead_custom_field_values
            (lead_id, field_id, field_name, field_code, field_type, values_json, synced_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (lead_id, field_id) DO UPDATE SET
                field_name = EXCLUDED.field_name, field_code = EXCLUDED.field_code,
                field_type = EXCLUDED.field_type, values_json = EXCLUDED.values_json,
                synced_at = EXCLUDED.synced_at
            """,
            (
                lead["id"],
                cf.get("field_id"),
                cf.get("field_name"),
                cf.get("field_code"),
                cf.get("field_type"),
                Json(cf.get("values") or []),
                now,
            ),
        )
    k.commit()
    c.close()
    k.close()


def _kommo_resolve_lead_id_by_rgm(rgm_clean: str) -> tuple[list[int], str | None]:
    """Retorna (lista de lead_ids, None) ou ([], mensagem_erro)."""
    ids: list[int] = []
    try:
        kc = _pg_kommo()
        cur = kc.cursor()
        cur.execute(
            """
            SELECT DISTINCT id FROM (
                SELECT l.id FROM leads l
                JOIN lead_custom_field_values lcf ON lcf.lead_id = l.id
                  AND lower(lcf.field_name) = 'rgm'
                WHERE length(regexp_replace(COALESCE((lcf.values_json->0)->>'value',''), '[^0-9]', '', 'g')) = 8
                  AND regexp_replace((lcf.values_json->0)->>'value', '[^0-9]', '', 'g') = %s
                UNION
                SELECT l.id FROM leads l,
                     LATERAL jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) x
                WHERE lower(x->>'field_name') = 'rgm'
                  AND length(regexp_replace(COALESCE(x->'values'->0->>'value',''), '[^0-9]', '', 'g')) = 8
                  AND regexp_replace(x->'values'->0->>'value', '[^0-9]', '', 'g') = %s
            ) t ORDER BY id DESC LIMIT 15
            """,
            (rgm_clean, rgm_clean),
        )
        ids = [r[0] for r in cur.fetchall()]
        cur.close()
        kc.close()
    except Exception as e:
        logger.warning("kommo PG busca RGM: %s", e)
    if ids:
        return ids, None
    if not KOMMO_TOKEN:
        return [], "RGM não encontrado na base local. Configure KOMMO_TOKEN para buscar na API."
    api = _kommo_api_v4()
    headers = {
        "Authorization": f"Bearer {KOMMO_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    try:
        r = requests.post(
            f"{api}/leads/list",
            headers=headers,
            json={
                "limit": 50,
                "filter": {
                    "custom_fields_values": [
                        {"field_id": KOMMO_RGM_FIELD_ID, "values": [{"value": rgm_clean}]}
                    ]
                },
            },
            timeout=45,
        )
        if r.status_code == 200:
            emb = r.json().get("_embedded", {})
            for L in emb.get("leads", []):
                if L.get("id"):
                    ids.append(int(L["id"]))
            ids = list(dict.fromkeys(ids))
            if ids:
                return ids, None
    except Exception as e:
        logger.warning("Kommo leads/list RGM: %s", e)
    for page in range(1, 9):
        try:
            time.sleep(0.1)
            r = requests.get(
                f"{api}/leads",
                headers={"Authorization": f"Bearer {KOMMO_TOKEN}", "Accept": "application/json"},
                params={"limit": 250, "page": page, "query": rgm_clean},
                timeout=30,
            )
            if r.status_code != 200:
                break
            data = r.json()
            for L in data.get("_embedded", {}).get("leads", []):
                for cf in L.get("custom_fields_values") or []:
                    if str(cf.get("field_name", "")).lower() != "rgm":
                        continue
                    v = re.sub(r"[^0-9]", "", str((cf.get("values") or [{}])[0].get("value", "")))
                    if v == rgm_clean and L.get("id"):
                        ids.append(int(L["id"]))
            ids = list(dict.fromkeys(ids))
            if ids:
                return ids, None
            if "next" not in data.get("_links", {}):
                break
        except Exception as e:
            logger.warning("Kommo leads query page %s: %s", page, e)
            break
    return [], (
        "Não achamos esse RGM na base nem nas primeiras páginas da API. "
        "Use o ID do lead (número após # na URL do Kommo)."
    )


def _kommo_fetch_lead_full(lead_id: int) -> dict | None:
    api = _kommo_api_v4()
    r = requests.get(
        f"{api}/leads/{lead_id}",
        headers={"Authorization": f"Bearer {KOMMO_TOKEN}", "Accept": "application/json"},
        params={"with": "contacts"},
        timeout=45,
    )
    if r.status_code != 200:
        logger.warning("GET lead %s -> %s %s", lead_id, r.status_code, r.text[:200])
        return None
    data = r.json()
    if isinstance(data.get("id"), int):
        return data
    emb = data.get("_embedded", {})
    if emb.get("leads"):
        return emb["leads"][0]
    return None


def _pg():
    return psycopg2.connect(**DB_DSN)


def _crgm_excluded_rgms(_unused=None) -> set:
    """Retorna conjunto de RGMs normalizados cujo registro mais recente (maior id)
    no snapshot atual NÃƒO é EM CURSO. Usa conexão própria para não contaminar
    transações do chamador em caso de erro."""
    _conn = None
    try:
        _conn = _pg()
        _cur = _conn.cursor()
        _cur.execute("""
            SELECT
                regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g') AS rgm,
                UPPER(TRIM(COALESCE(r.data->>'situacao','')))                   AS situacao
            FROM (
                SELECT DISTINCT ON (regexp_replace(COALESCE(r2.data->>'rgm',''), '[^0-9]', '', 'g'))
                    r2.data,
                    r2.id
                FROM xl_rows r2
                JOIN xl_snapshots s ON s.id = r2.snapshot_id
                WHERE s.id = (
                    SELECT id FROM xl_snapshots WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
                )
                  AND COALESCE(r2.data->>'rgm','') ~ '[0-9]'
                ORDER BY regexp_replace(COALESCE(r2.data->>'rgm',''), '[^0-9]', '', 'g'), r2.id DESC
            ) r
            WHERE UPPER(TRIM(COALESCE(r.data->>'situacao',''))) != 'EM CURSO'
        """)
        excluded = set()
        for rgm_raw, _ in _cur.fetchall():
            n = _normalize_rgm(rgm_raw)
            if n:
                excluded.add(n)
        _cur.close()
        return excluded
    except Exception as e:
        logger.warning("_crgm_excluded_rgms: %s", e)
        return set()
    finally:
        if _conn:
            try:
                _conn.close()
            except Exception:
                pass


def _crgm_periodo_data(dt_ini=None, dt_fim=None, polo=None, nivel=None, ciclo_filter=None, turma=None):
    """
    Retorna TODOS os RGMs únicos do período (registro mais recente por id),
    aplicando filtros de tipo_matricula, empresa e ciclo, mas SEM filtro de situação.
    Retorna lista de dicts: {rgm, nome, situacao, data_matricula, polo, nivel, ciclo}
    """
    _conn = None
    try:
        _conn = _pg()
        cur = _conn.cursor()

        # Filtros extras aplicados na camada deduplicated
        outer_conds = []
        params = []

        if dt_ini:
            outer_conds.append("data_matricula >= %s")
            params.append(dt_ini)
        if dt_fim:
            outer_conds.append("data_matricula <= %s")
            params.append(dt_fim)
        if polo:
            outer_conds.append(f"{_POLO_SQL} = %s")
            params.append(_normalize_polo(polo))
        if nivel:
            outer_conds.append("nivel = %s")
            params.append(nivel)
        if turma:
            outer_conds.append("turma = %s")
            params.append(turma)
        if ciclo_filter:
            outer_conds.append("ciclo = %s")
            params.append(ciclo_filter)
        else:
            outer_conds.append(
                "ciclo IN (SELECT ciclo FROM ciclo_atual_comercial)"
            )

        outer_where = ("WHERE " + " AND ".join(outer_conds)) if outer_conds else ""

        sql = f"""
            SELECT rgm, nome, situacao, data_matricula, polo, nivel, ciclo
            FROM (
                SELECT DISTINCT ON (regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'))
                    regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g')  AS rgm,
                    NULLIF(TRIM(COALESCE(r.data->>'nome','')), '')                  AS nome,
                    UPPER(TRIM(COALESCE(r.data->>'situacao','')))                   AS situacao,
                    CASE
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                            THEN to_date(r.data->>'data_mat','DD/MM/YYYY')
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN (r.data->>'data_mat')::date
                        ELSE NULL
                    END AS data_matricula,
                    CASE
                        WHEN COALESCE(r.data->>'nivel','')   ~* 'p[oó]s'                                        THEN 'Pós-Graduação'
                        WHEN COALESCE(r.data->>'negocio','') ~* 'p[oó]s'                                        THEN 'Pós-Graduação'
                        WHEN COALESCE(r.data->>'curso','')   ~* '(mba|especializa|p.s.gradua|lato.sensu|stricto)' THEN 'Pós-Graduação'
                        ELSE 'Graduação'
                    END AS nivel,
                    TRIM(regexp_replace(COALESCE(r.data->>'polo',''), '^[0-9]+\\s*[-]\\s*', '')) AS polo,
                    NULLIF(TRIM(COALESCE(r.data->>'ciclo','')), '')                AS ciclo,
                    NULLIF(TRIM(COALESCE(r.data->>'curso','')), '')                AS turma
                FROM xl_rows r
                JOIN xl_snapshots s ON s.id = r.snapshot_id
                WHERE s.id = (SELECT id FROM xl_snapshots WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1)
                  AND COALESCE(r.data->>'rgm','') ~ '[0-9]'
                  AND UPPER(TRIM(COALESCE(r.data->>'tipo_matricula','')))
                      = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])
                  AND TRIM(COALESCE(r.data->>'empresa','')) ~ '^(12|7) -'
                ORDER BY regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'), r.id DESC
            ) deduped
            {outer_where}
        """

        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()

        result = []
        for rgm, nome, situacao, dm, polo_v, nivel_v, ciclo_v in rows:
            if not rgm:
                continue
            try:
                dt_str = dm.isoformat() if hasattr(dm, "isoformat") else str(dm)[:10]
            except Exception:
                dt_str = None
            result.append({
                "rgm": rgm,
                "nome": nome or "",
                "situacao": situacao or "",
                "data_matricula": dt_str,
                "polo": polo_v or "",
                "nivel": nivel_v or "",
                "ciclo": ciclo_v or "",
            })
        return result

    except Exception as e:
        logger.warning("_crgm_periodo_data: %s", e)
        return []
    finally:
        if _conn:
            try:
                _conn.close()
            except Exception:
                pass


def _pg_kommo():
    return psycopg2.connect(**KOMMO_DB_DSN)


# â”€â”€ Schema â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS comercial_rgm (
    id              SERIAL PRIMARY KEY,
    rgm             TEXT,
    polo            TEXT,
    nivel           TEXT,
    modalidade      TEXT,
    data_matricula  DATE,
    ciclo           TEXT,
    turma           TEXT,
    financeiro      TEXT,
    valor_real      NUMERIC(12,2),
    mes_pagamento   TEXT,
    tipo_pagamento  TEXT,
    uploaded_at     TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_crgm_data  ON comercial_rgm(data_matricula);
CREATE INDEX IF NOT EXISTS idx_crgm_polo  ON comercial_rgm(polo);
CREATE INDEX IF NOT EXISTS idx_crgm_nivel ON comercial_rgm(nivel);

CREATE TABLE IF NOT EXISTS kommo_users (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    email       TEXT,
    synced_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mm_snapshots (
    id               SERIAL PRIMARY KEY,
    snapshot_id      TEXT NOT NULL,
    executed_at      TIMESTAMP DEFAULT NOW(),
    nivel            TEXT,
    total_inscritos  INTEGER,
    total_matriculados INTEGER,
    total_cruzados   INTEGER
);

CREATE TABLE IF NOT EXISTS mm_inscritos_hist (
    id SERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    tipo TEXT, status TEXT, dt_pag_insc TEXT, inscricao TEXT,
    nome TEXT, sexo TEXT, cpf TEXT, rg TEXT,
    curso_raw TEXT, curso_limpo TEXT, grau_curso TEXT, modalidade TEXT,
    polo_raw TEXT, polo_normalizado TEXT, marca_instituicao TEXT,
    data_inscr DATE, data_prova DATE,
    telefone TEXT, telefone_res TEXT, telefone_com TEXT,
    email TEXT, cep TEXT, endereco TEXT, bairro TEXT, cidade TEXT, estado TEXT,
    data_pagamento TEXT, data_matricula TEXT,
    situacao_raw TEXT, situacao_final TEXT,
    observacao TEXT, captador TEXT, trimestre_ingresso TEXT,
    chave_preco TEXT, preco_balcao TEXT, area_curso TEXT, semestres TEXT,
    arquivo_origem TEXT, uploaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mmih_snap ON mm_inscritos_hist(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_mmih_cpf  ON mm_inscritos_hist(cpf);
CREATE INDEX IF NOT EXISTS idx_mmih_data ON mm_inscritos_hist(data_inscr);

CREATE TABLE IF NOT EXISTS mm_matriculados_hist (
    id SERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    tipo TEXT, nome TEXT, cpf TEXT, rgm TEXT, rg TEXT, sexo TEXT, data_nasc TEXT,
    polo_captador TEXT, tipo_polo TEXT, polo_aulas TEXT,
    curso_raw TEXT, curso_limpo TEXT,
    prouni TEXT, serie TEXT, data_matricula TEXT, ano_tri_ingresso TEXT,
    tipo_matricula TEXT, situacao_raw TEXT, situacao TEXT,
    fone_res TEXT, fone_com TEXT, fone_cel TEXT, email TEXT, email_ad TEXT,
    endereco TEXT, bairro TEXT, cidade TEXT,
    arquivo_origem TEXT, uploaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mmhm_snap ON mm_matriculados_hist(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_mmhm_cpf  ON mm_matriculados_hist(cpf);

CREATE INDEX IF NOT EXISTS idx_mmhm_data ON mm_matriculados_hist(data_matricula);
"""

METAS_CATEGORIAS = [
    {"id": "matriculas",  "label": "Matrículas"},
    {"id": "inscricoes",  "label": "Inscrições"},
    {"id": "valor",       "label": "Valor vendido (R$)"},
    {"id": "novos_leads", "label": "Novos leads"},
    {"id": "conversao",   "label": "Taxa conversão (%)"},
]

_METAS_DDL = """
CREATE TABLE IF NOT EXISTS comercial_metas (
    id                  SERIAL PRIMARY KEY,
    user_id             INTEGER NOT NULL,
    user_name           TEXT,
    meta                NUMERIC NOT NULL DEFAULT 0,
    meta_intermediaria  NUMERIC NOT NULL DEFAULT 0,
    supermeta           NUMERIC NOT NULL DEFAULT 0,
    categoria           TEXT NOT NULL DEFAULT 'matriculas',
    dt_inicio           DATE NOT NULL,
    dt_fim              DATE NOT NULL,
    descricao           TEXT DEFAULT '',
    created_at          TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cm_user ON comercial_metas(user_id);
CREATE INDEX IF NOT EXISTS idx_cm_dates ON comercial_metas(dt_inicio, dt_fim);
CREATE INDEX IF NOT EXISTS idx_cm_cat ON comercial_metas(categoria);
"""


def _ensure_table():
    conn = _pg()
    cur = conn.cursor()
    cur.execute(_CREATE_SQL)
    conn.commit()

    # Migrate comercial_metas
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'comercial_metas' AND column_name = 'dt_inicio'
        """)
        has_dt_inicio = cur.fetchone() is not None

        if not has_dt_inicio:
            cur.execute("DROP TABLE IF EXISTS comercial_metas CASCADE")
            conn.commit()

        cur.execute(_METAS_DDL)
        conn.commit()

        # Add missing columns incrementally
        for col, defn in [
            ("categoria", "TEXT NOT NULL DEFAULT 'matriculas'"),
            ("meta_intermediaria", "NUMERIC NOT NULL DEFAULT 0"),
            ("supermeta", "NUMERIC NOT NULL DEFAULT 0"),
        ]:
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'comercial_metas' AND column_name = %s
            """, (col,))
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE comercial_metas ADD COLUMN {col} {defn}")
                conn.commit()
                logger.info("comercial_metas: added '%s' column", col)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_cm_cat ON comercial_metas(categoria)")
        conn.commit()

        # Tabela de resolucoes de conflito de atribuicao de RGMs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS comercial_rgm_conflito_resolucao (
                rgm         TEXT PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                user_name   TEXT,
                resolved_at TIMESTAMPTZ DEFAULT NOW(),
                resolved_by TEXT DEFAULT 'manual'
            )
        """)
        conn.commit()

        # Ensure unique constraint for batch upsert
        cur.execute("""
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uq_cm_user_period_cat'
        """)
        if not cur.fetchone():
            cur.execute("""
                ALTER TABLE comercial_metas
                ADD CONSTRAINT uq_cm_user_period_cat
                UNIQUE (user_id, dt_inicio, dt_fim, categoria)
            """)
            conn.commit()
            logger.info("comercial_metas: added unique constraint uq_cm_user_period_cat")

        # Ensure meta column is NUMERIC
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_name = 'comercial_metas' AND column_name = 'meta'
        """)
        row = cur.fetchone()
        if row and row[0] == 'integer':
            cur.execute("ALTER TABLE comercial_metas ALTER COLUMN meta TYPE NUMERIC")
            conn.commit()
            logger.info("comercial_metas: changed 'meta' to NUMERIC")

    except Exception as e:
        conn.rollback()
        logger.warning("comercial_metas migration: %s", e)
        try:
            cur.execute("DROP TABLE IF EXISTS comercial_metas CASCADE")
            conn.commit()
            cur.execute(_METAS_DDL)
            conn.commit()
        except Exception as e2:
            conn.rollback()
            logger.error("comercial_metas create failed: %s", e2)

    cur.close()
    conn.close()


try:
    _ensure_table()
except Exception as _e:
    logger.warning("comercial_rgm: could not ensure tables at startup: %s", _e)


# â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _normalize_polo(polo: str) -> str:
    """Remove prefixo numérico e 'CEB ' do nome do polo para unificar duplicatas."""
    if not polo:
        return polo
    # Remove código numérico inicial: '1876 - ' ou '43 - '
    p = re.sub(r'^\d+\s*[-—]\s*', '', polo.strip())
    # Remove prefixo 'CEB ': 'CEB POLO SP_...' → 'POLO SP_...'
    p = re.sub(r'^CEB\s+', '', p, flags=re.IGNORECASE)
    return p.strip()


# Expressão SQL que normaliza a coluna polo da mesma forma que _normalize_polo()
_POLO_SQL = "regexp_replace(regexp_replace(polo, E'^\\\\d+\\\\s*[-\\u2013]\\\\s*', ''), '^CEB\\s+', '', 'i')"


def _normalize_rgm(val):
    """Normalize RGM: strip non-digits, remove leading zeros."""
    if not val:
        return None
    digits = re.sub(r"\D", "", str(val))
    if not digits:
        return None
    try:
        return str(int(digits))
    except ValueError:
        return digits


def _parse_date_br(s):
    """Parse dd/mm/yyyy or dd/m/yyyy to date object."""
    if not s or not s.strip():
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal_br(s):
    """Parse '33,62' or '1.234,56' to float."""
    if not s or not s.strip():
        return None
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _shift_months(d, months):
    """Desloca uma data por N meses.
    Se d é o último dia do mês, o resultado também é o último dia do mês alvo.
    """
    import calendar
    is_last = d.day == calendar.monthrange(d.year, d.month)[1]
    m = d.month + months
    y = d.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    max_day = calendar.monthrange(y, m)[1]
    return date(y, m, max_day if is_last else min(d.day, max_day))


def _safe_date(year, month, day):
    """Cria date ajustando dia para o máximo do mês (ex: 29/Fev → 28/Fev)."""
    import calendar
    max_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day, max_day))


COL_MAP = {
    "RGM": "rgm",
    "Polo": "polo",
    "Nível": "nivel",
    "N\xedvel": "nivel",
    "Modalidade": "modalidade",
    "Data de Matrícula": "data_matricula",
    "Data de Matr\xedcula": "data_matricula",
    "Ciclo": "ciclo",
    "Turma": "turma",
    "Financeiro": "financeiro",
    "Valor Real": "valor_real",
    "Mês Pagamento": "mes_pagamento",
    "M\xeas Pagamento": "mes_pagamento",
    "Tipo de Pagamento": "tipo_pagamento",
}


def populate_comercial_from_snapshot(snapshot_id=None):
    """Auto-populate comercial_rgm from the latest matriculados snapshot.

    Filters by tipo_matricula (INGRESSANTE, NOVA MATRÃCULA, RETORNO, RECOMPRA)
    and merges with existing comercial_rgm data (new RGMs only).
    Called automatically after a matriculados upload.
    """
    import json
    import unicodedata
    import re as _re

    _POS_RE = _re.compile(r'p[oó]s', _re.IGNORECASE)
    _POS_CURSO_RE = _re.compile(
        r'(mba|especializa.+o|p[oó]s.gradua|lato.sensu|stricto)',
        _re.IGNORECASE,
    )

    def _classify(data):
        if data.get("nivel") and _POS_RE.search(data["nivel"]):
            return "Pós-Graduação"
        if _POS_RE.search(data.get("negocio", "") or ""):
            return "Pós-Graduação"
        if _POS_CURSO_RE.search(data.get("curso", "") or ""):
            return "Pós-Graduação"
        return "Graduação"

    conn = _pg()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT nome, dt_inicio, dt_fim FROM ciclos_comercial
            ORDER BY dt_inicio
        """)
        ciclos = cur.fetchall()

        cur.execute("""
            SELECT nome, nivel, dt_inicio, dt_fim FROM turmas_comercial
            ORDER BY dt_inicio
        """)
        turmas = cur.fetchall()

        def _resolve_ciclo(dt_matricula):
            if not dt_matricula:
                return None
            for nome, dt_ini, dt_end in ciclos:
                if dt_ini <= dt_matricula <= dt_end:
                    return nome
            return None

        def _resolve_turma(dt_matricula, nivel_aluno):
            if not dt_matricula:
                return None
            for nome, nivel_turma, dt_ini, dt_end in turmas:
                if dt_ini <= dt_matricula <= dt_end and nivel_turma == nivel_aluno:
                    return nome
            for nome, nivel_turma, dt_ini, dt_end in turmas:
                if dt_ini <= dt_matricula <= dt_end:
                    return nome
            return None

        if snapshot_id:
            snap_id = snapshot_id
        else:
            cur.execute("""
                SELECT id FROM xl_snapshots
                WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                logger.warning("populate_comercial: no matriculados snapshot found")
                return 0
            snap_id = row[0]

        cur.execute(
            "SELECT data FROM xl_rows WHERE snapshot_id = %s",
            (snap_id,),
        )
        xl_rows = cur.fetchall()
        if not xl_rows:
            return 0

        cur.execute("SELECT rgm FROM comercial_rgm")
        existing_rgms = {r[0] for r in cur.fetchall() if r[0]}

        new_rows = []
        for (data_json,) in xl_rows:
            d = data_json if isinstance(data_json, dict) else json.loads(data_json)

            tipo_mat = (d.get("tipo_matricula") or "").strip().upper()
            if tipo_mat != "INGRESSANTE":
                continue

            situacao = (d.get("situacao") or "").strip().upper()
            if situacao == "TRANSFERIDO":
                continue

            rgm = _normalize_rgm(d.get("rgm") or d.get("rgm_digits"))
            if not rgm or rgm in existing_rgms:
                continue

            raw_date = d.get("data_mat", "")
            dt = _parse_date_br(raw_date) if raw_date else None
            if dt is None and raw_date:
                for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
                    try:
                        dt = datetime.strptime(raw_date.strip()[:10], fmt).date()
                        break
                    except (ValueError, TypeError):
                        continue

            polo_raw = d.get("polo", "") or ""
            polo = _re.sub(r'^\d+\s*[-—]\s*', '', polo_raw).strip() or None
            nivel = _classify(d)
            modalidade = (d.get("modalidade") or "").strip() or None
            ciclo = _resolve_ciclo(dt)
            turma = _resolve_turma(dt, nivel)

            new_rows.append((rgm, polo, nivel, modalidade, dt, ciclo, turma,
                             None, None, None, None))
            existing_rgms.add(rgm)

        if not new_rows:
            logger.info("populate_comercial: no new commercial records to add")
            return 0

        cols = ["rgm", "polo", "nivel", "modalidade", "data_matricula", "ciclo",
                "turma", "financeiro", "valor_real", "mes_pagamento", "tipo_pagamento"]
        sql = f"INSERT INTO comercial_rgm ({', '.join(cols)}) VALUES %s"
        tpl = "(" + ", ".join(["%s"] * len(cols)) + ")"
        psycopg2.extras.execute_values(cur, sql, new_rows, template=tpl, page_size=2000)
        conn.commit()
        logger.info("populate_comercial: added %d new rows from snapshot %s", len(new_rows), snap_id)
        return len(new_rows)

    except Exception as e:
        conn.rollback()
        logger.exception("populate_comercial error: %s", e)
        return 0
    finally:
        cur.close()
        conn.close()


def _import_csv(stream, encoding="utf-8-sig"):
    """Parse CSV stream and insert rows into comercial_rgm. Returns count."""
    reader = csv.DictReader(stream)

    rows = []
    for raw in reader:
        row = {}
        for csv_col, val in raw.items():
            db_col = COL_MAP.get(csv_col)
            if not db_col:
                continue
            row[db_col] = val
        row["rgm"] = _normalize_rgm(row.get("rgm"))
        if not row["rgm"]:
            continue

        row["data_matricula"] = _parse_date_br(row.get("data_matricula", ""))
        row["valor_real"] = _parse_decimal_br(row.get("valor_real", ""))

        for k in ("polo", "nivel", "modalidade", "ciclo", "turma",
                   "financeiro", "mes_pagamento", "tipo_pagamento"):
            row.setdefault(k, None)
            if row[k] is not None:
                row[k] = row[k].strip() or None

        rows.append(row)

    if not rows:
        return 0

    conn = _pg()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE comercial_rgm RESTART IDENTITY")

    cols = ["rgm", "polo", "nivel", "modalidade", "data_matricula", "ciclo",
            "turma", "financeiro", "valor_real", "mes_pagamento", "tipo_pagamento"]
    sql = f"INSERT INTO comercial_rgm ({', '.join(cols)}) VALUES %s"
    tpl = "(" + ", ".join(["%s"] * len(cols)) + ")"

    values = [tuple(r.get(c) for c in cols) for r in rows]
    psycopg2.extras.execute_values(cur, sql, values, template=tpl, page_size=2000)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("comercial_rgm: imported %d rows", len(rows))
    return len(rows)


# â”€â”€ Endpoints â€” Congelar â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/congelar", methods=["POST"])
def crgm_congelar():
    """Congela dados da view comercial_rgm_atual para a tabela comercial_rgm_congelados
    e avança o ciclo ativo para o próximo.

    Body JSON: { "nivel": "Graduação" | "Pós-Graduação" }
    """
    body = request.json or {}
    nivel = (body.get("nivel") or "").strip()

    if nivel not in ("Graduação", "Pós-Graduação"):
        return jsonify({"error": "Selecione Graduação ou Pós-Graduação"}), 400

    conn = _pg()
    try:
        cur = conn.cursor()

        cur.execute(
            "SELECT ciclo FROM ciclo_atual_comercial WHERE nivel = %s",
            (nivel,),
        )
        row = cur.fetchone()
        ciclo_atual = row[0] if row else None

        cur.execute(
            "SELECT COUNT(*) FROM comercial_rgm_atual WHERE nivel = %s",
            (nivel,),
        )
        total_source = cur.fetchone()[0]

        if total_source == 0:
            return jsonify({
                "error": f"Nenhum registro de {nivel} no ciclo {ciclo_atual or '?'}"
            }), 400

        cur.execute("""
            INSERT INTO comercial_rgm_congelados
                (rgm, polo, nivel, modalidade, data_matricula, ciclo, turma)
            SELECT rgm, polo, nivel, modalidade, data_matricula, ciclo, turma
            FROM comercial_rgm_atual
            WHERE nivel = %s
              AND rgm NOT IN (
                  SELECT rgm FROM comercial_rgm_congelados WHERE rgm IS NOT NULL
              )
        """, (nivel,))
        inserted = cur.rowcount

        next_ciclo = None
        if ciclo_atual:
            cur.execute("""
                SELECT ciclo FROM (
                    SELECT DISTINCT TRIM(data->>'ciclo') AS ciclo
                    FROM xl_rows r
                    JOIN xl_snapshots s ON s.id = r.snapshot_id
                    WHERE s.id = (SELECT id FROM xl_snapshots
                                  WHERE tipo = 'matriculados'
                                  ORDER BY id DESC LIMIT 1)
                      AND TRIM(data->>'ciclo') SIMILAR TO '\\d{4}/\\d'
                ) sub
                WHERE ciclo > %s
                ORDER BY ciclo
                LIMIT 1
            """, (ciclo_atual,))
            nxt = cur.fetchone()
            if nxt:
                next_ciclo = nxt[0]
                cur.execute(
                    "UPDATE ciclo_atual_comercial SET ciclo = %s WHERE nivel = %s",
                    (next_ciclo, nivel),
                )

        conn.commit()

        logger.info(
            "congelar: %d novos registros congelados (%s ciclo %s). Próximo ciclo: %s",
            inserted, nivel, ciclo_atual, next_ciclo or "nenhum",
        )
        return jsonify({
            "ok": True,
            "nivel": nivel,
            "ciclo_congelado": ciclo_atual,
            "total_view": total_source,
            "congelados": inserted,
            "proximo_ciclo": next_ciclo,
        })
    except Exception as e:
        conn.rollback()
        logger.exception("congelar error")
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            conn.close()
        except Exception:
            pass


@comercial_rgm_bp.route("/api/comercial-rgm/ciclo-atual")
def crgm_ciclo_atual():
    """Returns current active cycle per nivel."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("SELECT nivel, ciclo FROM ciclo_atual_comercial ORDER BY nivel")
        rows = cur.fetchall()
        cur.close()
        return jsonify({
            "ok": True,
            "ciclos": {r[0]: r[1] for r in rows},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# â”€â”€ Endpoints â€” Ciclos â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/ciclos")
def crgm_ciclos_list():
    """List all commercial cycles (dimension)."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nome, ano, semestre, dt_inicio, dt_fim, ativo, descricao
            FROM ciclos_comercial ORDER BY dt_inicio DESC
        """)
        rows = cur.fetchall()
        cur.close()
        return jsonify({
            "ok": True,
            "ciclos": [
                {"id": r[0], "nome": r[1], "ano": r[2], "semestre": r[3],
                 "dt_inicio": r[4].isoformat(), "dt_fim": r[5].isoformat(),
                 "ativo": r[6], "descricao": r[7]}
                for r in rows
            ],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/ciclos", methods=["POST"])
def crgm_ciclos_create():
    """Create a new commercial cycle (auto-derives ano, semestre, descricao).

    Also updates ciclo_atual_comercial for the selected nivel(s).
    Body: { nome, nivel ("Graduação"|"Pós-Graduação"|"Ambos"), dt_inicio, dt_fim, ativo }
    """
    body = request.json or {}
    nome = (body.get("nome") or "").strip()
    nivel_target = (body.get("nivel") or "Ambos").strip()
    dt_inicio = body.get("dt_inicio", "")
    dt_fim = body.get("dt_fim", "")
    ativo = body.get("ativo", False)

    if not nome or not dt_inicio or not dt_fim:
        return jsonify({"error": "nome, dt_inicio e dt_fim são obrigatórios"}), 400

    ano, semestre, descricao = None, None, nome
    parts = nome.replace("/", ".").split(".")
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        y = int(parts[0])
        ano = y if y > 100 else 2000 + y
        semestre = int(parts[1])
        descricao = f"{semestre}Âº Semestre {ano}"

    conn = _pg()
    try:
        cur = conn.cursor()
        if ativo:
            cur.execute("UPDATE ciclos_comercial SET ativo = FALSE WHERE ativo = TRUE")

        cur.execute("""
            INSERT INTO ciclos_comercial (nome, ano, semestre, dt_inicio, dt_fim, ativo, descricao)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (nome) DO NOTHING
            RETURNING id
        """, (nome, ano, semestre, dt_inicio, dt_fim, ativo, descricao))
        row = cur.fetchone()
        new_id = row[0] if row else None

        nivels = []
        if nivel_target == "Ambos":
            nivels = ["Graduação", "Pós-Graduação"]
        elif nivel_target in ("Graduação", "Pós-Graduação"):
            nivels = [nivel_target]

        for nv in nivels:
            cur.execute("""
                INSERT INTO ciclo_atual_comercial (nivel, ciclo)
                VALUES (%s, %s)
                ON CONFLICT (nivel) DO UPDATE SET ciclo = EXCLUDED.ciclo
            """, (nv, nome))

        conn.commit()
        cur.close()
        return jsonify({"ok": True, "id": new_id, "ciclo_atual_atualizado": nivels})
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return jsonify({"error": f"Ciclo '{nome}' já existe"}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/ciclos/<int:ciclo_id>", methods=["PUT"])
def crgm_ciclos_update(ciclo_id):
    """Update a commercial cycle."""
    body = request.json or {}
    conn = _pg()
    try:
        cur = conn.cursor()
        fields, vals = [], []
        for col in ("nome", "dt_inicio", "dt_fim"):
            if col in body:
                fields.append(f"{col} = %s")
                vals.append(body[col])
        if "ativo" in body:
            if body["ativo"]:
                cur.execute("UPDATE ciclos_comercial SET ativo = FALSE WHERE ativo = TRUE")
            fields.append("ativo = %s")
            vals.append(body["ativo"])

        if not fields:
            return jsonify({"error": "Nenhum campo para atualizar"}), 400

        vals.append(ciclo_id)
        cur.execute(
            f"UPDATE ciclos_comercial SET {', '.join(fields)} WHERE id = %s",
            vals,
        )
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/ciclos/<int:ciclo_id>", methods=["DELETE"])
def crgm_ciclos_delete(ciclo_id):
    """Delete a commercial cycle."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM ciclos_comercial WHERE id = %s", (ciclo_id,))
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# â”€â”€ Endpoints â€” Turmas â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/turmas")
def crgm_turmas_list():
    """List all turmas (monthly cohorts), optionally filtered by ciclo and/or nivel."""
    ciclo_id = request.args.get("ciclo_id", "")
    nivel = request.args.get("nivel", "")
    conn = _pg()
    try:
        cur = conn.cursor()
        wheres, params = [], []
        if ciclo_id:
            wheres.append("t.ciclo_id = %s")
            params.append(ciclo_id)
        if nivel:
            wheres.append("t.nivel = %s")
            params.append(nivel)
        w = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        cur.execute(f"""
            SELECT t.id, t.nome, t.nivel, t.ciclo_id, c.nome, t.dt_inicio, t.dt_fim
            FROM turmas_comercial t
            LEFT JOIN ciclos_comercial c ON c.id = t.ciclo_id
            {w}
            ORDER BY t.nivel, t.dt_inicio
        """, params)
        rows = cur.fetchall()
        cur.close()
        return jsonify({
            "ok": True,
            "turmas": [
                {"id": r[0], "nome": r[1], "nivel": r[2], "ciclo_id": r[3],
                 "ciclo_nome": r[4], "dt_inicio": r[5].isoformat(),
                 "dt_fim": r[6].isoformat()}
                for r in rows
            ],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/turmas/stats")
def crgm_turmas_stats():
    """Contagens de matrículas por turma usando snapshots históricos."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nivel, dt_inicio, dt_fim
            FROM turmas_comercial
            ORDER BY nivel, dt_inicio
        """)
        turmas = cur.fetchall()

        # Todos os snapshots de matriculados disponíveis
        cur.execute("""
            SELECT id, uploaded_at::date FROM xl_snapshots
            WHERE tipo = 'matriculados' ORDER BY id
        """)
        snaps = cur.fetchall()
        snap_latest = snaps[-1][0] if snaps else None

        # Raw strings: \d chega ao PostgreSQL sem ser consumido pelo Python
        # empresa 12=Grad, 7=Pos UCS, 79=Pos UCS-CL — todos começam com (12|7x)
        _NIVEL_SQL = r"""CASE
            WHEN coalesce(r.data->>'nivel','') ~* 'p[oó]s'
              OR coalesce(r.data->>'negocio','') ~* 'p[oó]s'
              OR coalesce(r.data->>'curso','') ~* '(mba|especializa|p[oó]s.gradua|lato.sensu|stricto)'
            THEN 'Pós-Graduação' ELSE 'Graduação' END"""

        _DM_SQL = r"""CASE
            WHEN (r.data->>'data_mat') ~ '^\d{2}/\d{2}/\d{4}$'
                THEN to_date(r.data->>'data_mat', 'DD/MM/YYYY')
            WHEN (r.data->>'data_mat') ~ '^\d{4}-\d{2}-\d{2}'
                THEN (r.data->>'data_mat')::date
            ELSE NULL END"""

        _EMP_SQL = r"trim(coalesce(r.data->>'empresa','')) ~ '^(12|7[0-9]*) -'"

        def _count_snap(snap_id, nivel, dt_ini, dt_fim):
            cur.execute(f"""
                SELECT COUNT(DISTINCT regexp_replace(coalesce(r.data->>'rgm',''), '[^0-9]', '', 'g'))
                FROM xl_rows r
                WHERE r.snapshot_id = %s
                  AND upper(trim(coalesce(r.data->>'situacao',''))) = 'EM CURSO'
                  AND upper(trim(coalesce(r.data->>'tipo_matricula','')))
                      = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])
                  AND {_EMP_SQL}
                  AND coalesce(r.data->>'rgm','') ~ '[0-9]'
                  AND {_NIVEL_SQL} = %s
                  AND {_DM_SQL} BETWEEN %s AND %s
            """, (snap_id, nivel, dt_ini, dt_fim))
            return cur.fetchone()[0] or 0

        stats = {}
        for tid, tnivel, dt_ini, dt_fim in turmas:
            snaps_ate_dtfim = [s[0] for s in snaps if s[1] <= dt_fim]
            snap_id_periodo = snaps_ate_dtfim[-1] if snaps_ate_dtfim else None

            mat_periodo = _count_snap(snap_id_periodo, tnivel, dt_ini, dt_fim) if snap_id_periodo else None
            em_curso = _count_snap(snap_latest, tnivel, dt_ini, dt_fim) if snap_latest else None

            stats[tid] = {
                "mat_periodo": mat_periodo,
                "em_curso_hoje": em_curso,
                "sem_dados": snap_id_periodo is None,
            }

        cur.close()
        return jsonify({"ok": True, "stats": stats})
    except Exception as e:
        logger.exception("turmas stats error")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/turmas", methods=["POST"])
def crgm_turmas_create():
    """Create a new turma."""
    body = request.json or {}
    nome = (body.get("nome") or "").strip()
    nivel = (body.get("nivel") or "Graduação").strip()
    ciclo_id = body.get("ciclo_id")
    dt_inicio = body.get("dt_inicio", "")
    dt_fim = body.get("dt_fim", "")

    if not nome or not dt_inicio or not dt_fim:
        return jsonify({"error": "nome, dt_inicio e dt_fim são obrigatórios"}), 400

    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO turmas_comercial (nome, nivel, ciclo_id, dt_inicio, dt_fim)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (nome, nivel, ciclo_id or None, dt_inicio, dt_fim))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return jsonify({"ok": True, "id": new_id})
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return jsonify({"error": f"Turma '{nome}' ({nivel}) já existe nesse ciclo"}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@comercial_rgm_bp.route("/api/comercial-rgm/turmas/<int:turma_id>", methods=["DELETE"])
def crgm_turmas_delete(turma_id):
    """Delete a turma."""
    conn = _pg()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM turmas_comercial WHERE id = %s", (turma_id,))
        conn.commit()
        cur.close()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# â”€â”€ Endpoints â€” Upload & Data â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/upload", methods=["POST"])
def crgm_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    fname_lower = f.filename.lower()

    if fname_lower.endswith(".csv"):
        try:
            raw = f.read()
            for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
                try:
                    text = raw.decode(enc)
                    break
                except (UnicodeDecodeError, ValueError):
                    continue
            else:
                return jsonify({"error": "Encoding não suportado"}), 400

            stream = io.StringIO(text)
            count = _import_csv(stream)
            return jsonify({"ok": True, "rows": count, "filename": f.filename})
        except Exception as e:
            logger.exception("comercial_rgm upload CSV error")
            return jsonify({"error": str(e)}), 500

    elif fname_lower.endswith((".xlsx", ".xlsm")):
        try:
            from routes.upload import _save_xl_snapshot
            import tempfile, shutil
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            f.save(tmp.name)
            tmp.close()

            row_count = _save_xl_snapshot(tmp.name, f.filename, "matriculados")
            added = populate_comercial_from_snapshot()

            try:
                os.unlink(tmp.name)
            except OSError:
                pass

            return jsonify({
                "ok": True,
                "filename": f.filename,
                "snapshot_rows": row_count,
                "comercial_added": added,
            })
        except Exception as e:
            logger.exception("comercial_rgm upload XLSX error")
            return jsonify({"error": str(e)}), 500

    else:
        return jsonify({"error": "Aceitos: .csv ou .xlsx"}), 400


@comercial_rgm_bp.route("/api/comercial-rgm/populate-from-matriculados", methods=["POST"])
def crgm_populate():
    """Manually trigger population of comercial_rgm from latest matriculados snapshot."""
    try:
        added = populate_comercial_from_snapshot()
        return jsonify({"ok": True, "added": added})
    except Exception as e:
        logger.exception("populate_comercial endpoint error")
        return jsonify({"error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/snapshot-info")
def crgm_snapshot_info():
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*), MIN(data_matricula), MAX(data_matricula),
                   MAX(uploaded_at)
            FROM comercial_rgm
        """)
        row = cur.fetchone()

        cur.execute("SELECT COUNT(*) FROM mm_inscritos_hist")
        mm_insc = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM mm_matriculados_hist")
        mm_mat = cur.fetchone()[0] or 0

        cur.close()
        conn.close()
        return jsonify({
            "ok": True,
            "total": row[0] or 0,
            "min_date": row[1].isoformat() if row[1] else None,
            "max_date": row[2].isoformat() if row[2] else None,
            "uploaded_at": row[3].isoformat() if row[3] else None,
            "mm_inscritos": mm_insc,
            "mm_matriculados": mm_mat,
        })
    except Exception as e:
        logger.exception("snapshot-info error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/sync-users", methods=["POST"])
def crgm_sync_users():
    """Sync Kommo users via API v4 and store in both databases."""
    if not KOMMO_TOKEN:
        return jsonify({"error": "KOMMO_TOKEN não configurado"}), 500
    try:
        headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
        url = f"{KOMMO_BASE_URL}/api/v4/users"
        all_users = []
        page = 1
        while True:
            resp = requests.get(url, headers=headers, params={"page": page, "limit": 250}, timeout=15)
            logger.info("sync-users page %d -> status %d", page, resp.status_code)
            if resp.status_code != 200:
                logger.warning("sync-users API returned %d: %s", resp.status_code, resp.text[:300])
                break
            data = resp.json()
            embedded = data.get("_embedded", {}).get("users", [])
            if not embedded:
                break
            all_users.extend(embedded)
            page += 1

        if not all_users:
            return jsonify({"ok": True, "synced": 0, "msg": "Nenhum usuário retornado pela API"})

        conn = _pg()
        cur = conn.cursor()
        for u in all_users:
            cur.execute("""
                INSERT INTO kommo_users (id, name, email, synced_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, synced_at = NOW()
            """, (u["id"], u.get("name", ""), u.get("email", "")))
        conn.commit()
        cur.close()
        conn.close()

        try:
            kconn = _pg_kommo()
            kcur = kconn.cursor()
            kcur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY, name TEXT, email TEXT,
                    lang TEXT, rights_json JSONB, synced_at TEXT
                )
            """)
            for u in all_users:
                kcur.execute("""
                    INSERT INTO users (id, name, email, synced_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, synced_at = NOW()
                """, (u["id"], u.get("name", ""), u.get("email", "")))
            kconn.commit()
            kcur.close()
            kconn.close()
        except Exception as e:
            logger.warning("sync-users kommo_sync write: %s", e)

        return jsonify({"ok": True, "synced": len(all_users)})
    except Exception as e:
        logger.exception("sync-users error")
        return jsonify({"error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/filters")
def crgm_filters():
    try:
        conn = _pg()
        cur = conn.cursor()
        # Usa apenas comercial_rgm_atual (xl_rows) — fonte principal do dashboard
        cur.execute("""
            SELECT DISTINCT polo FROM comercial_rgm_atual
            WHERE polo IS NOT NULL AND polo != ''
            ORDER BY polo
        """)
        _polo_set = {}
        for (p,) in cur.fetchall():
            n = _normalize_polo(p)
            if n and n not in _polo_set:
                _polo_set[n] = True
        polos = sorted(_polo_set.keys())
        cur.execute("SELECT DISTINCT nivel FROM comercial_rgm WHERE nivel IS NOT NULL ORDER BY nivel")
        niveis = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT ciclo FROM comercial_rgm WHERE ciclo IS NOT NULL ORDER BY ciclo")
        ciclos = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id, name FROM kommo_users ORDER BY name")
        agentes = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
        cur.close()
        conn.close()

        if not agentes:
            agentes = [{"id": k, "name": v} for k, v in sorted(_KNOWN_USERS.items(), key=lambda x: x[1])]

        return jsonify({"ok": True, "polos": polos, "niveis": niveis, "ciclos": ciclos, "agentes": agentes})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


_KNOWN_USERS = {
    8239958:  "Fran",
    8240165:  "Isabela",
    8240189:  "Juliana",
    8240438:  "Claudia",
    8261837:  "Admin",
    9718419:  "Felipe",
    10329248: "Andreina",
    10729260: "Jessica",
    11741316: "Bruno",
    12158628: "Hugo",
    12209212: "Gabriela",
    12908868: "Diogo",
    13018348: "Kamily",
    13304804: "T.I",
    14205944: "Thainá",
    14464488: "Tamires",
    14482884: "Eduardo",
    14546744: "Suporte",
    14546760: "Jessica C",
    14932700: "Beatriz",
}


def _fetch_kommo_user_names(user_ids):
    """Get user names: known map -> kommo_sync.users -> dcz_sync.kommo_users -> API."""
    user_map = {}
    if not user_ids:
        return user_map

    for uid in user_ids:
        if uid in _KNOWN_USERS:
            user_map[uid] = _KNOWN_USERS[uid]

    missing = [uid for uid in user_ids if uid not in user_map]
    if not missing:
        return user_map

    try:
        conn = _pg_kommo()
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM users WHERE id = ANY(%s)", (missing,))
        for r in cur.fetchall():
            user_map[r[0]] = r[1]
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("fetch user names from kommo_sync.users: %s", e)

    missing = [uid for uid in user_ids if uid not in user_map]
    if missing:
        try:
            conn = _pg()
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM kommo_users WHERE id = ANY(%s)", (missing,))
            for r in cur.fetchall():
                user_map[r[0]] = r[1]
            cur.close()
            conn.close()
        except Exception:
            pass

    missing = [uid for uid in user_ids if uid not in user_map]
    if missing and KOMMO_TOKEN:
        try:
            headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
            all_resp = requests.get(
                f"{KOMMO_BASE_URL}/api/v4/users",
                headers=headers, params={"limit": 250}, timeout=15
            )
            if all_resp.status_code == 200:
                api_users = all_resp.json().get("_embedded", {}).get("users", [])
                for u in api_users:
                    uid = u.get("id")
                    if uid in missing:
                        user_map[uid] = u.get("name", f"User #{uid}")
        except Exception as e:
            logger.warning("fetch user names from API: %s", e)

    return user_map


def _date_to_epoch(dt_str):
    """Convert 'YYYY-MM-DD' to Unix epoch int, or None."""
    if not dt_str:
        return None
    try:
        return int(datetime.strptime(dt_str, "%Y-%m-%d").timestamp())
    except Exception:
        return None


def _build_agent_ranking(dt_ini=None, dt_fim=None, polo=None):
    """Build agent ranking by cross-referencing CSV matrículas with Kommo leads.

    Logic (matches the BI):
      1. kommo_sync: leads with status=142 (Ganho) -> extract RGM from custom fields
         -> build RGM->responsible_user_id map
      2. dcz_sync: comercial_rgm (CSV) filtered by date/polo
         -> count matrículas per agent using the RGM map
      3. Also include CRM-only stats (total leads, novos, perdidos, ativos)
    """
    try:
        # --- Step 1: build RGM -> responsible_user_id from Kommo leads ---
        kconn = _pg_kommo()
        kcur = kconn.cursor()

        # Build RGM->user map from TWO sources (lead_custom_field_values + leads.custom_fields_json)
        # Source 1: lead_custom_field_values (case-insensitive)
        kcur.execute("""
            SELECT regexp_replace(lcf.values_json->0->>'value', '[^0-9]', '', 'g') AS rgm,
                   l.responsible_user_id,
                   l.status_id
            FROM lead_custom_field_values lcf
            JOIN leads l ON l.id = lcf.lead_id AND l.is_deleted = FALSE
            WHERE LOWER(lcf.field_name) = 'rgm'
              AND lcf.values_json->0->>'value' IS NOT NULL
              AND lcf.values_json->0->>'value' != ''
            ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
        """)
        rgm_to_user = {}
        src1_count = 0
        for row in kcur.fetchall():
            rgm, uid = _normalize_rgm(row[0]), row[1]
            if rgm and uid and rgm not in rgm_to_user:
                rgm_to_user[rgm] = uid
                src1_count += 1

        # Source 2: leads.custom_fields_json (fallback for leads not in cf_values table)
        kcur.execute("""
            SELECT regexp_replace(cf_elem->'values'->0->>'value', '[^0-9]', '', 'g') AS rgm,
                   l.responsible_user_id,
                   l.status_id
            FROM leads l,
                 jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) cf_elem
            WHERE l.is_deleted = FALSE
              AND LOWER(cf_elem->>'field_name') = 'rgm'
              AND cf_elem->'values'->0->>'value' IS NOT NULL
              AND cf_elem->'values'->0->>'value' != ''
            ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
        """)
        src2_count = 0
        for row in kcur.fetchall():
            rgm, uid = _normalize_rgm(row[0]), row[1]
            if rgm and uid and rgm not in rgm_to_user:
                rgm_to_user[rgm] = uid
                src2_count += 1

        logger.info("rgm_to_user map: %d total (%d from cf_values, %d extra from custom_fields_json)",
                     len(rgm_to_user), src1_count, src2_count)

        # --- CRM totals per agent (all-time) ---
        ep_ini = _date_to_epoch(dt_ini)
        ep_fim = _date_to_epoch(dt_fim)
        if ep_fim is not None:
            ep_fim += 86399

        kcur.execute("""
            SELECT l.responsible_user_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN l.status_id = 142 THEN 1 ELSE 0 END) AS ganhos,
                   SUM(CASE WHEN l.status_id = 143 THEN 1 ELSE 0 END) AS perdidos,
                   SUM(CASE WHEN l.status_id NOT IN (142, 143) THEN 1 ELSE 0 END) AS ativos,
                   SUM(CASE WHEN l.status_id = 143 AND l.closed_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.closed_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.closed_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS perdidos_periodo,
                   SUM(CASE WHEN l.created_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.created_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.created_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS novos_periodo
            FROM leads l
            WHERE l.responsible_user_id IS NOT NULL
                  AND l.is_deleted = FALSE
            GROUP BY l.responsible_user_id
        """, {"ep_ini": ep_ini, "ep_fim": ep_fim})
        crm_stats = {}
        for r in kcur.fetchall():
            crm_stats[r[0]] = {
                "total": r[1], "ganhos": r[2], "perdidos": r[3],
                "ativos": r[4], "perdidos_periodo": r[5], "novos_periodo": r[6],
            }
        kcur.close()
        kconn.close()

        # --- Step 2: count matrículas per agent via RGM ---
        # Sources: comercial_rgm (CSV upload) + mm_matriculados (M&M upload)
        conn = _pg()
        cur = conn.cursor()

        all_rgms = set()
        cpf_to_rgm = {}

        # Source A: CSV (comercial_rgm)
        csv_where = []
        csv_params = []
        if dt_ini:
            csv_where.append("data_matricula >= %s")
            csv_params.append(dt_ini)
        if dt_fim:
            csv_where.append("data_matricula <= %s")
            csv_params.append(dt_fim)
        if polo:
            csv_where.append(f"{_POLO_SQL} = %s")
            csv_params.append(_normalize_polo(polo))
        csv_w = ("WHERE " + " AND ".join(csv_where)) if csv_where else ""

        cur.execute(f"SELECT rgm FROM comercial_rgm {csv_w}", csv_params)
        for r in cur.fetchall():
            n = _normalize_rgm(r[0])
            if n:
                all_rgms.add(n)

        # Source B: M&M matriculados (dedup via set)
        mm_where = ["UPPER(COALESCE(tipo_matricula,'')) IN %s"]
        mm_params = [MM_TIPO_MAT_VALIDOS]
        if dt_ini:
            mm_where.append("data_matricula >= %s")
            mm_params.append(dt_ini)
        if dt_fim:
            mm_where.append("data_matricula <= %s")
            mm_params.append(dt_fim)
        if polo:
            mm_where.append("polo_aulas = %s")
            mm_params.append(polo)
        mm_w = "WHERE " + " AND ".join(mm_where)

        cur.execute(f"SELECT rgm, cpf, nome FROM mm_matriculados {mm_w}", mm_params)
        nome_to_rgm = {}
        for r in cur.fetchall():
            rgm = _normalize_rgm(r[0])
            if rgm:
                all_rgms.add(rgm)
                if r[1] and r[1].strip():
                    cpf_to_rgm[r[1].strip()] = rgm
                if r[2] and r[2].strip():
                    nome_to_rgm[r[2].strip().upper()] = rgm

        cur.close()
        conn.close()

        pre_fallback = len(rgm_to_user)

        # Fallback 1: CPF -> Kommo contact -> lead -> responsible_user_id
        unmatched_rgms = {r for r in all_rgms if r not in rgm_to_user}
        if unmatched_rgms and cpf_to_rgm:
            try:
                kconn2 = _pg_kommo()
                kcur2 = kconn2.cursor()
                # Source A: contact_custom_field_values
                kcur2.execute("""
                    SELECT
                        regexp_replace(ccf.values_json->0->>'value', '[^0-9]', '', 'g') AS cpf,
                        l.responsible_user_id
                    FROM contact_custom_field_values ccf
                    JOIN contacts c ON c.id = ccf.contact_id AND c.is_deleted = FALSE
                    JOIN lead_contacts lc ON lc.contact_id = c.id
                    JOIN leads l ON l.id = lc.lead_id AND l.is_deleted = FALSE
                    WHERE LOWER(ccf.field_name) IN ('cpf')
                      AND ccf.values_json->0->>'value' IS NOT NULL
                      AND ccf.values_json->0->>'value' != ''
                    ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
                """)
                cpf_to_uid = {}
                for row in kcur2.fetchall():
                    cpf_val, uid = row[0], row[1]
                    if cpf_val and uid and cpf_val not in cpf_to_uid:
                        cpf_to_uid[cpf_val] = uid

                # Source B: contacts.custom_fields_json (fallback)
                kcur2.execute("""
                    SELECT regexp_replace(cf_elem->'values'->0->>'value', '[^0-9]', '', 'g') AS cpf,
                           l.responsible_user_id
                    FROM contacts c,
                         jsonb_array_elements(COALESCE(c.custom_fields_json, '[]'::jsonb)) cf_elem,
                         lead_contacts lc,
                         leads l
                    WHERE c.is_deleted = FALSE
                      AND LOWER(cf_elem->>'field_name') = 'cpf'
                      AND cf_elem->'values'->0->>'value' IS NOT NULL
                      AND cf_elem->'values'->0->>'value' != ''
                      AND lc.contact_id = c.id
                      AND l.id = lc.lead_id AND l.is_deleted = FALSE
                    ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
                """)
                for row in kcur2.fetchall():
                    cpf_val, uid = row[0], row[1]
                    if cpf_val and uid and cpf_val not in cpf_to_uid:
                        cpf_to_uid[cpf_val] = uid

                kcur2.close()
                kconn2.close()

                cpf_added = 0
                for cpf, rgm in cpf_to_rgm.items():
                    if rgm not in rgm_to_user and cpf in cpf_to_uid:
                        rgm_to_user[rgm] = cpf_to_uid[cpf]
                        cpf_added += 1
                logger.info("CPF fallback: %d CPFs in Kommo, %d new RGM->user mapped", len(cpf_to_uid), cpf_added)
            except Exception as e:
                logger.warning("CPF fallback error: %s", e)

        # Fallback 2: nome (student name) -> Kommo lead name or contact name
        unmatched_rgms = {r for r in all_rgms if r not in rgm_to_user}
        if unmatched_rgms and nome_to_rgm:
            try:
                kconn3 = _pg_kommo()
                kcur3 = kconn3.cursor()
                kcur3.execute("""
                    SELECT UPPER(l.name), l.responsible_user_id
                    FROM leads l
                    WHERE l.is_deleted = FALSE
                      AND l.name IS NOT NULL AND l.name != ''
                      AND l.responsible_user_id IS NOT NULL
                    ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
                """)
                nome_to_uid = {}
                for row in kcur3.fetchall():
                    n, uid = row[0], row[1]
                    if n and uid and n not in nome_to_uid:
                        nome_to_uid[n] = uid
                kcur3.close()
                kconn3.close()

                nome_added = 0
                for nome, rgm in nome_to_rgm.items():
                    if rgm not in rgm_to_user and nome in nome_to_uid:
                        rgm_to_user[rgm] = nome_to_uid[nome]
                        nome_added += 1
                logger.info("Nome fallback: %d names in Kommo, %d new RGM->user mapped", len(nome_to_uid), nome_added)
            except Exception as e:
                logger.warning("Nome fallback error: %s", e)

        logger.info("RGM->user total: %d (base: %d, +fallbacks: %d)",
                     len(rgm_to_user), pre_fallback, len(rgm_to_user) - pre_fallback)

        mat_per_agent = {}
        matched_count = 0
        unmatched_sample = []
        for rgm in all_rgms:
            uid = rgm_to_user.get(rgm)
            if uid:
                mat_per_agent[uid] = mat_per_agent.get(uid, 0) + 1
                matched_count += 1
            elif len(unmatched_sample) < 10:
                unmatched_sample.append(rgm)
        if unmatched_sample:
            logger.info("Sample unmatched RGMs (%d total): %s",
                        len(all_rgms) - matched_count, unmatched_sample)

        # --- Step 3: merge CRM stats + CSV matrículas ---
        all_uids = set(crm_stats.keys()) | set(mat_per_agent.keys())
        user_map = _fetch_kommo_user_names(list(all_uids))

        ranking = []
        for uid in all_uids:
            cs = crm_stats.get(uid, {})
            total = cs.get("total", 0)
            ganhos = cs.get("ganhos", 0)
            perdidos = cs.get("perdidos", 0)
            ativos = cs.get("ativos", 0)
            mat_periodo = mat_per_agent.get(uid, 0)
            perdidos_p = cs.get("perdidos_periodo", 0)
            novos_p = cs.get("novos_periodo", 0)
            name = user_map.get(uid, f"User #{uid}")
            taxa = round(ganhos / total * 100, 1) if total > 0 else 0
            ranking.append({
                "user_id": uid,
                "nome": name,
                "total": total,
                "ganhos": ganhos,
                "perdidos": perdidos,
                "ativos": ativos,
                "taxa_conversao": taxa,
                "matriculas_periodo": mat_periodo,
                "perdidos_periodo": perdidos_p,
                "novos_periodo": novos_p,
            })

        ranking.sort(key=lambda x: x["matriculas_periodo"], reverse=True)
        logger.info(
            "Agent ranking: %d agents, %d unique RGMs, %d matched (%.0f%%)",
            len(ranking), len(all_rgms),
            sum(mat_per_agent.values()),
            sum(mat_per_agent.values()) / max(len(all_rgms), 1) * 100
        )
        return ranking
    except Exception as e:
        logger.warning("agent ranking error: %s", e)
        import traceback
        logger.warning(traceback.format_exc())
        return []


def _build_agent_ranking_completa_vw(
    dt_ini=None, dt_fim=None, polo=None, nivel=None, ciclo=None, turma=None,
    excluded_rgms: set = None
):
    """Matrículas em comercial_rgm_completa Ã— responsável em vw_leads_rgm. Sem match → transferencia/regresso."""
    TR = -1
    try:
        conn = _pg()
        if excluded_rgms is None:
            excluded_rgms = _crgm_excluded_rgms(conn)
        cur = conn.cursor()
        cw, cp = [], []
        if polo:
            cw.append(f"{_POLO_SQL} = %s")
            cp.append(_normalize_polo(polo))
        if nivel:
            cw.append("nivel = %s")
            cp.append(nivel)
        if dt_ini:
            cw.append("data_matricula >= %s")
            cp.append(dt_ini)
        if dt_fim:
            cw.append("data_matricula <= %s")
            cp.append(dt_fim)
        if ciclo:
            cw.append("ciclo = %s")
            cp.append(ciclo)
        if turma:
            cw.append("turma = %s")
            cp.append(turma)
        w = "WHERE " + " AND ".join(cw) if cw else ""
        cur.execute(
            f"SELECT rgm, nome, polo, data_matricula FROM comercial_rgm_atual {w}  ORDER BY data_matricula DESC NULLS LAST",
            cp,
        )
        rgm_nome = {}
        for rgm, nome, _polo, _dm in cur.fetchall():
            n = _normalize_rgm(rgm)
            if not n or n in rgm_nome or n in excluded_rgms:
                continue
            rgm_nome[n] = (nome or "").strip()

        # Regra de cancelados: inclui alunos que estavam EM CURSO em qualquer upload
        # feito ATÉ dt_fim, mas foram cancelados depois (cancelamento após meta = conta)
        if dt_fim:
            _NIVEL_CASE = """CASE
                WHEN coalesce(r.data->>'nivel','') ~* 'p[oó]s'
                  OR coalesce(r.data->>'negocio','') ~* 'p[oó]s'
                  OR coalesce(r.data->>'curso','') ~* '(mba|especializa|p[oó]s.gradua|lato.sensu|stricto)'
                THEN 'Pós-Graduação' ELSE 'Graduação' END"""
            _DM_EXPR = """CASE
                WHEN (r.data->>'data_mat') ~ E'^\\d{2}/\\d{2}/\\d{4}$'
                    THEN to_date(r.data->>'data_mat', 'DD/MM/YYYY')
                WHEN (r.data->>'data_mat') ~ E'^\\d{4}-\\d{2}-\\d{2}'
                    THEN (r.data->>'data_mat')::date
                ELSE NULL END"""
            supp_cw = [
                "s.tipo = 'matriculados'",
                "s.uploaded_at::date <= %s",
                "upper(trim(coalesce(r.data->>'situacao',''))) = 'EM CURSO'",
                "upper(trim(coalesce(r.data->>'tipo_matricula',''))) = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])",
                "trim(coalesce(r.data->>'empresa','')) ~ '^(12|7) -'",
                "coalesce(r.data->>'rgm','') ~ '\\d'",
                f"""(({_NIVEL_CASE} = 'Graduação'
                    AND trim(r.data->>'ciclo') = (SELECT ciclo FROM ciclo_atual_comercial WHERE nivel='Graduação'))
                   OR ({_NIVEL_CASE} = 'Pós-Graduação'
                    AND trim(r.data->>'ciclo') = (SELECT ciclo FROM ciclo_atual_comercial WHERE nivel='Pós-Graduação')))""",
            ]
            supp_cp = [dt_fim]
            if dt_ini:
                supp_cw.append(f"{_DM_EXPR} >= %s")
                supp_cp.append(dt_ini)
            if polo:
                supp_cw.append("trim(regexp_replace(coalesce(r.data->>'polo',''), E'^\\d+\\s*[-–]\\s*', '')) = %s")
                supp_cp.append(_normalize_polo(polo))
            if nivel:
                supp_cw.append(f"{_NIVEL_CASE} = %s")
                supp_cp.append(nivel)
            if ciclo:
                # Ciclo manual: remove o filtro automático e adiciona o manual
                supp_cw = [c for c in supp_cw if 'ciclo_atual_comercial' not in c]
                supp_cw.append("trim(coalesce(r.data->>'ciclo','')) = %s")
                supp_cp.append(ciclo)
            if turma:
                supp_cw.append("nullif(trim(coalesce(r.data->>'curso','')), '') = %s")
                supp_cp.append(turma)
            # Exclui RGMs já contabilizados na query principal
            already = tuple(rgm_nome.keys()) if rgm_nome else ('__NONE__',)
            supp_cw.append("regexp_replace(coalesce(r.data->>'rgm',''), '[^0-9]', '', 'g') != ALL(%s)")
            supp_cp.append(list(already))
            supp_where = "WHERE " + " AND ".join(supp_cw)
            try:
                cur2 = conn.cursor() if not conn.closed else _pg().cursor()
                cur2.execute(f"""
                    SELECT DISTINCT ON (rgm_norm) rgm_norm, nome
                    FROM (
                        SELECT
                            regexp_replace(coalesce(r.data->>'rgm',''), '[^0-9]', '', 'g') AS rgm_norm,
                            nullif(trim(coalesce(r.data->>'nome','')), '') AS nome,
                            s.uploaded_at
                        FROM xl_rows r
                        JOIN xl_snapshots s ON s.id = r.snapshot_id
                        {supp_where}
                    ) t
                    WHERE rgm_norm != ''
                    ORDER BY rgm_norm, uploaded_at DESC
                """, supp_cp)
                for rgm_raw, nome in cur2.fetchall():
                    n = _normalize_rgm(rgm_raw)
                    if n and n not in rgm_nome:
                        rgm_nome[n] = (nome or "").strip()
                cur2.close()
                logger.info("ranking: +%d RGMs cancelados-pós-meta incluídos", len(rgm_nome) - len(mat_rows) if mat_rows else 0)
            except Exception as _se:
                logger.warning("ranking supp cancelados: %s", _se)

        mat_rows = list(rgm_nome.items())
        cur.close()
        conn.close()

        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT DISTINCT ON (v.rgm) v.rgm, l.responsible_user_id
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            WHERE l.responsible_user_id IS NOT NULL
            ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
        """)
        rgm_to_uid = {}
        for row in kcur.fetchall():
            nk = _normalize_rgm(row[0])
            if nk and row[1]:
                rgm_to_uid[nk] = row[1]

        # Aplicar overrides de conflito salvos manualmente
        try:
            _oc = _pg()
            _oc_cur = _oc.cursor()
            _oc_cur.execute("SELECT rgm, user_id FROM comercial_rgm_conflito_resolucao")
            for _rgm_raw, _uid in _oc_cur.fetchall():
                _nk = _normalize_rgm(_rgm_raw)
                if _nk:
                    rgm_to_uid[_nk] = _uid
            _oc_cur.close()
            _oc.close()
        except Exception as _oe:
            logger.warning("conflito_resolucao override: %s", _oe)

        ep_ini = _date_to_epoch(dt_ini)
        ep_fim = _date_to_epoch(dt_fim)
        if ep_fim is not None:
            ep_fim += 86399
        kcur.execute("""
            SELECT l.responsible_user_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN l.status_id = 142 THEN 1 ELSE 0 END) AS ganhos,
                   SUM(CASE WHEN l.status_id = 143 THEN 1 ELSE 0 END) AS perdidos,
                   SUM(CASE WHEN l.status_id NOT IN (142, 143) THEN 1 ELSE 0 END) AS ativos,
                   SUM(CASE WHEN l.status_id = 143 AND l.closed_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.closed_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.closed_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS perdidos_periodo,
                   SUM(CASE WHEN l.created_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.created_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.created_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS novos_periodo
            FROM leads l
            WHERE l.responsible_user_id IS NOT NULL AND NOT l.is_deleted
            GROUP BY l.responsible_user_id
        """, {"ep_ini": ep_ini, "ep_fim": ep_fim})
        crm_stats = {
            r[0]: {
                "total": r[1], "ganhos": r[2], "perdidos": r[3], "ativos": r[4],
                "perdidos_periodo": r[5], "novos_periodo": r[6],
            }
            for r in kcur.fetchall()
        }
        kcur.close()
        kconn.close()

        mat_per_agent = {}
        transferencia_itens = []
        for rgm, nome in mat_rows:
            uid = rgm_to_uid.get(rgm)
            if uid:
                mat_per_agent[uid] = mat_per_agent.get(uid, 0) + 1
            else:
                transferencia_itens.append({"rgm": rgm, "nome": nome})
        tr_count = len(transferencia_itens)
        if tr_count:
            mat_per_agent[TR] = tr_count

        uids_real = [u for u in (set(crm_stats) | set(mat_per_agent)) if u != TR]
        user_map = _fetch_kommo_user_names(uids_real)
        ranking = []
        for uid in uids_real:
            cs = crm_stats.get(uid, {})
            t, g = cs.get("total", 0), cs.get("ganhos", 0)
            ranking.append({
                "user_id": uid,
                "nome": user_map.get(uid, f"User #{uid}"),
                "total": t,
                "ganhos": g,
                "perdidos": cs.get("perdidos", 0),
                "ativos": cs.get("ativos", 0),
                "taxa_conversao": round(g / t * 100, 1) if t > 0 else 0,
                "matriculas_periodo": mat_per_agent.get(uid, 0),
                "perdidos_periodo": cs.get("perdidos_periodo", 0),
                "novos_periodo": cs.get("novos_periodo", 0),
            })
        if tr_count:
            ranking.append({
                "user_id": TR,
                "nome": "transferencia/regresso",
                "total": 0,
                "ganhos": 0,
                "perdidos": 0,
                "ativos": 0,
                "taxa_conversao": 0.0,
                "matriculas_periodo": tr_count,
                "perdidos_periodo": 0,
                "novos_periodo": 0,
                "is_transferencia": True,
            })
        ranking.sort(key=lambda x: x["matriculas_periodo"], reverse=True)
        return ranking, {
            "titulo": "transferencia/regresso",
            "total": tr_count,
            "itens": sorted(transferencia_itens, key=lambda x: x["rgm"]),
        }
    except Exception as e:
        logger.warning("ranking completa/vw: %s", e)
        return [], {"titulo": "transferencia/regresso", "total": 0, "itens": []}


@comercial_rgm_bp.route("/api/comercial-rgm/data")
def crgm_data():
    polo = request.args.get("polo", "")
    nivel = request.args.get("nivel", "")
    dt_ini = request.args.get("dt_ini", "")
    dt_fim = request.args.get("dt_fim", "")
    ciclo_nome = request.args.get("ciclo", "")
    turma_nome = request.args.get("turma", "")

    where = []
    params = []

    if polo:
        where.append(f"{_POLO_SQL} = %s")
        params.append(_normalize_polo(polo))
    if nivel:
        where.append("nivel = %s")
        params.append(nivel)
    if dt_ini:
        where.append("data_matricula >= %s")
        params.append(dt_ini)
    if dt_fim:
        where.append("data_matricula <= %s")
        params.append(dt_fim)
    if ciclo_nome:
        where.append("ciclo = %s")
        params.append(ciclo_nome)
    # turma_nome é apenas um rótulo de preset (datas+nível) — não filtra por curso

    # comercial_rgm_atual já aplica todos os filtros de negócio (ciclo atual, em curso, empresa 7/12, tipos)
    w = ("WHERE " + " AND ".join(where)) if where else ""

    try:
        conn = _pg()
        cur = conn.cursor()

        # Busca TODOS os RGMs do período (sem filtro de situação) para contagem bruta + evasão
        _periodo_rows = _crgm_periodo_data(
            dt_ini=dt_ini or None,
            dt_fim=dt_fim or None,
            polo=polo or None,
            nivel=nivel or None,
            ciclo_filter=ciclo_nome or None,
            turma=None,  # turma é preset de datas/nível, não filtra por curso
        )

        rgms_periodo   = set()          # EM CURSO (líquido)
        rgms_bruto     = set()          # todos (bruto)
        evasao_rows    = []             # não EM CURSO
        day_rgms       = defaultdict(set)   # líquido por dia
        day_rgms_bruto = defaultdict(set)   # bruto por dia (sombra)
        polo_rgms      = defaultdict(set)

        for row in _periodo_rows:
            n = row["rgm"]
            if not n:
                continue
            rgms_bruto.add(n)
            try:
                dt = date.fromisoformat(row["data_matricula"][:10]) if row["data_matricula"] else None
            except (ValueError, TypeError):
                dt = None
            if dt:
                day_rgms_bruto[dt].add(n)
            if row["situacao"] == "EM CURSO":
                rgms_periodo.add(n)
                if dt:
                    day_rgms[dt].add(n)
                if row["polo"]:
                    polo_rgms[_normalize_polo(row["polo"])].add(n)
            else:
                evasao_rows.append(row)

        vendas = len(rgms_bruto)         # KPI mostra BRUTO
        vendas_liquidas = len(rgms_periodo)
        _excluded = rgms_bruto - rgms_periodo   # conjunto excluído (para ranking)
        all_kpi_rgms = rgms_periodo
        day_counts       = {d: len(s) for d, s in day_rgms.items()}
        day_counts_bruto = {d: len(s) for d, s in day_rgms_bruto.items()}
        polo_counts = {p: len(s) for p, s in polo_rgms.items()}
        dias = len(day_counts) or 1
        media_diaria = round(vendas_liquidas / dias, 1) if dias else 0

        # --- Ticket médio via Kommo lead price (cruzado por RGM) ---
        ticket_medio = 0.0
        try:
            kconn = _pg_kommo()
            kcur = kconn.cursor()
            kcur.execute("""
                SELECT rgm_val, price FROM (
                    SELECT regexp_replace(lcf.values_json->0->>'value', '[^0-9]', '', 'g') AS rgm_val,
                           l.price
                    FROM lead_custom_field_values lcf
                    JOIN leads l ON l.id = lcf.lead_id AND l.status_id = 142 AND l.is_deleted = FALSE
                    WHERE LOWER(lcf.field_name) = 'rgm'
                      AND lcf.values_json->0->>'value' IS NOT NULL
                      AND lcf.values_json->0->>'value' != ''
                      AND l.price IS NOT NULL AND l.price > 0
                    UNION ALL
                    SELECT regexp_replace(cf_elem->'values'->0->>'value', '[^0-9]', '', 'g'),
                           l.price
                    FROM leads l,
                         jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) cf_elem
                    WHERE l.status_id = 142 AND l.is_deleted = FALSE
                      AND LOWER(cf_elem->>'field_name') = 'rgm'
                      AND cf_elem->'values'->0->>'value' IS NOT NULL
                      AND cf_elem->'values'->0->>'value' != ''
                      AND l.price IS NOT NULL AND l.price > 0
                ) sub WHERE rgm_val IS NOT NULL AND rgm_val != ''
            """)
            rgm_price = {}
            for r in kcur.fetchall():
                n = _normalize_rgm(r[0])
                if n and n not in rgm_price:
                    rgm_price[n] = r[1]
            kcur.close()
            kconn.close()

            prices = [rgm_price[rgm] for rgm in all_kpi_rgms if rgm in rgm_price and rgm_price[rgm] > 0]
            if prices:
                ticket_medio = round((sum(prices) / len(prices)) * 0.30, 2)
        except Exception as e:
            logger.warning("ticket medio kommo: %s", e)

        # --- MM Inscritos no período (hist + atual) ---
        mm_insc_count = 0

        insc_where_hist = []
        insc_params_hist = []
        if dt_ini:
            insc_where_hist.append("data_inscr >= %s")
            insc_params_hist.append(dt_ini)
        if dt_fim:
            insc_where_hist.append("data_inscr <= %s")
            insc_params_hist.append(dt_fim)
        if polo:
            insc_where_hist.append("polo_normalizado = %s")
            insc_params_hist.append(polo)
        insc_w_hist = ("WHERE " + " AND ".join(insc_where_hist)) if insc_where_hist else ""

        insc_where_cur = []
        insc_params_cur = []
        if dt_ini:
            insc_where_cur.append("data_inscr >= %s")
            insc_params_cur.append(dt_ini)
        if dt_fim:
            insc_where_cur.append("data_inscr <= %s")
            insc_params_cur.append(dt_fim)
        if polo:
            insc_where_cur.append("polo_normalizado = %s")
            insc_params_cur.append(polo)
        insc_w_cur = ("WHERE " + " AND ".join(insc_where_cur)) if insc_where_cur else ""

        try:
            cur.execute(f"""
                SELECT COUNT(DISTINCT cpf) FROM (
                    SELECT cpf FROM mm_inscritos_hist {insc_w_hist}
                    UNION
                    SELECT cpf FROM mm_inscritos {insc_w_cur}
                ) sub WHERE cpf IS NOT NULL
            """, insc_params_hist + insc_params_cur)
            mm_insc_count = cur.fetchone()[0] or 0
        except Exception:
            try:
                cur.execute(f"SELECT COUNT(*) FROM mm_inscritos_hist {insc_w_hist}", insc_params_hist)
                mm_insc_count = cur.fetchone()[0] or 0
            except Exception:
                mm_insc_count = 0

        # --- Comparações: 6M / 1 ano / YTD ---
        vendas_6m = 0
        vendas_1a = 0
        vendas_ytd = 0
        vendas_prev_ytd = 0

        def _count_period(cur_, d_start, d_end, polo_=polo, nivel_=nivel):
            cw = ["data_matricula >= %s", "data_matricula <= %s"]
            cp = [d_start.isoformat(), d_end.isoformat()]
            if polo_:
                cw.append(f"{_POLO_SQL} = %s"); cp.append(_normalize_polo(polo_))
            if nivel_:
                cw.append("nivel = %s"); cp.append(nivel_)
            if ciclo_nome:
                cw.append("ciclo = %s"); cp.append(ciclo_nome)
            if turma_nome:
                cw.append("turma = %s"); cp.append(turma_nome)
            cur_.execute(
                f"SELECT rgm FROM comercial_rgm_atual WHERE {' AND '.join(cw)}",
                cp,
            )
            return len({_normalize_rgm(r[0]) for r in cur_.fetchall()
                        if _normalize_rgm(r[0]) and _normalize_rgm(r[0]) not in _excluded})

        def _count_hist(cur_, d_start, d_end, polo_=polo, nivel_=nivel):
            """Contagem histórica via comercial_rgm_completa (sem restrição de ciclo)."""
            cw = ["data_matricula >= %s", "data_matricula <= %s"]
            cp = [d_start.isoformat(), d_end.isoformat()]
            if polo_:
                cw.append(f"{_POLO_SQL} = %s"); cp.append(_normalize_polo(polo_))
            if nivel_:
                cw.append("nivel = %s"); cp.append(nivel_)
            cur_.execute(
                f"SELECT rgm FROM comercial_rgm_completa WHERE {' AND '.join(cw)}",
                cp,
            )
            return len({_normalize_rgm(r[0]) for r in cur_.fetchall() if _normalize_rgm(r[0])})

        if dt_ini and dt_fim:
            try:
                d_ini = date.fromisoformat(dt_ini)
                d_fim = date.fromisoformat(dt_fim)

                vendas_6m = _count_hist(
                    cur, _shift_months(d_ini, -6), _shift_months(d_fim, -6)
                )
                vendas_1a = _count_hist(
                    cur, _shift_months(d_ini, -12), _shift_months(d_fim, -12)
                )
                vendas_ytd = _count_period(
                    cur, date(d_fim.year, 1, 1), d_fim
                )
                prev_year = d_fim.year - 1
                vendas_prev_ytd = _count_hist(
                    cur,
                    date(prev_year, 1, 1),
                    _safe_date(prev_year, d_fim.month, d_fim.day),
                )
            except Exception as exc:
                logger.warning("Erro no cálculo comparativos: %s", exc)

        pct_6m = round((vendas / vendas_6m - 1) * 100, 1) if vendas_6m > 0 else 0
        pct_1a = round((vendas / vendas_1a - 1) * 100, 1) if vendas_1a > 0 else 0
        pct_ytd = round((vendas_ytd / vendas_prev_ytd - 1) * 100, 1) if vendas_prev_ytd > 0 else 0

        evolucao = [{"data": d.isoformat(), "count": c} for d, c in sorted(day_counts.items())]
        # bruto: union das datas de bruto + liquido para alinhar os dois datasets
        all_dates_bruto = sorted(set(day_counts_bruto.keys()) | set(day_counts.keys()))
        evolucao_bruto = [{"data": d.isoformat(), "count": day_counts_bruto.get(d, 0)} for d in all_dates_bruto]

        # --- Evolução ano anterior (por linha / data_matricula) ---
        evolucao_prev = []
        if dt_ini and dt_fim:
            try:
                d_ini = date.fromisoformat(dt_ini)
                d_fim_d = date.fromisoformat(dt_fim)
                prev_ini = _shift_months(d_ini, -12)
                prev_fim = _shift_months(d_fim_d, -12)

                prev_csv_w = ["data_matricula >= %s", "data_matricula <= %s"]
                prev_csv_p = [prev_ini.isoformat(), prev_fim.isoformat()]
                if polo:
                    prev_csv_w.append(f"{_POLO_SQL} = %s")
                    prev_csv_p.append(_normalize_polo(polo))
                if nivel:
                    prev_csv_w.append("nivel = %s")
                    prev_csv_p.append(nivel)
                if ciclo_nome:
                    prev_csv_w.append("ciclo = %s")
                    prev_csv_p.append(ciclo_nome)
                if turma_nome:
                    prev_csv_w.append("turma = %s")
                    prev_csv_p.append(turma_nome)
                pcw = "WHERE " + " AND ".join(prev_csv_w)

                cur.execute(
                    f"SELECT rgm, data_matricula FROM comercial_rgm_atual {pcw}",
                    prev_csv_p,
                )
                prev_day_rgms = defaultdict(set)
                for rgm, dm in cur.fetchall():
                    n = _normalize_rgm(rgm)
                    if not n:
                        continue
                    try:
                        dt_val = (
                            dm
                            if hasattr(dm, "isoformat")
                            else date.fromisoformat(str(dm)[:10])
                        )
                    except (ValueError, TypeError, AttributeError):
                        dt_val = None
                    if dt_val:
                        prev_day_rgms[dt_val].add(n)
                prev_day_counts = {d: len(s) for d, s in prev_day_rgms.items()}
                evolucao_prev = [{"data": d.isoformat(), "count": c}
                                 for d, c in sorted(prev_day_counts.items())]
            except Exception as exc:
                logger.warning("evolucao prev year: %s", exc)

        ranking_polo = [{"nome": p, "total": c}
                        for p, c in sorted(polo_counts.items(), key=lambda x: -x[1])]

        ciclo_extra = " AND ciclo IS NOT NULL" if where else " WHERE ciclo IS NOT NULL"
        cur.execute(
            f"SELECT rgm, ciclo FROM comercial_rgm_atual {w}{ciclo_extra}",
            params,
        )
        ciclo_rgms = defaultdict(set)
        for rgm, ciclo in cur.fetchall():
            if not ciclo:
                continue
            n = _normalize_rgm(rgm)
            if n and n not in _excluded:
                ciclo_rgms[ciclo].add(n)
        ranking_ciclo = [
            {"nome": c, "total": len(s)}
            for c, s in sorted(ciclo_rgms.items(), key=lambda x: -len(x[1]))[:10]
        ]

        cur.close()
        conn.close()

        ranking_agentes, transferencia_regresso = _build_agent_ranking_completa_vw(
            dt_ini or None,
            dt_fim or None,
            polo or None,
            nivel or None,
            ciclo_nome or None,
            None,  # turma: preset de datas/nível, não filtra por curso
            excluded_rgms=_excluded,
        )

        # --- Metas por agente: premiacao_campanha_meta (primary) + comercial_metas (fallback) ---
        # Structure: {cat: {uid_int: {meta, intermediaria, supermeta}}}
        metas_by_cat = {}
        campanha_meta_uids = set()
        metas_load_error = None
        try:
            conn2 = _pg()
            cur2 = conn2.cursor()

            cur2.execute("""
                SELECT pcm.kommo_user_id, pcm.meta, pcm.meta_intermediaria, pcm.supermeta
                FROM premiacao_campanha_meta pcm
                JOIN premiacao_campanha pc ON pc.id = pcm.campanha_id
                WHERE pc.dt_inicio <= %s AND pc.dt_fim >= %s
            """, (dt_fim or '9999-12-31', dt_ini or '1900-01-01'))
            for r in cur2.fetchall():
                uid = _kommo_uid_int(r[0])
                if uid is None:
                    continue
                campanha_meta_uids.add(uid)
                metas_by_cat.setdefault("matriculas", {})
                metas_by_cat["matriculas"][uid] = {
                    "meta": float(r[1]),
                    "intermediaria": float(r[2]),
                    "supermeta": float(r[3]),
                }

            cur2.execute("""
                SELECT user_id, meta, COALESCE(meta_intermediaria,0),
                       COALESCE(supermeta,0), categoria
                FROM comercial_metas
                WHERE dt_inicio <= %s AND dt_fim >= %s
            """, (dt_fim or '9999-12-31', dt_ini or '1900-01-01'))
            for r in cur2.fetchall():
                uid = _kommo_uid_int(r[0])
                if uid is None:
                    continue
                cat = r[4] or "matriculas"
                if cat == "matriculas" and uid in campanha_meta_uids:
                    continue
                metas_by_cat.setdefault(cat, {})
                prev = metas_by_cat[cat].get(uid, {"meta": 0, "intermediaria": 0, "supermeta": 0})
                prev["meta"] += float(r[1])
                prev["intermediaria"] += float(r[2])
                prev["supermeta"] += float(r[3])
                metas_by_cat[cat][uid] = prev
            cur2.close()
            conn2.close()
        except Exception as e:
            logger.warning("metas por periodo: %s", e)
            metas_load_error = str(e)

        mat_metas = metas_by_cat.get("matriculas", {})
        for ag in ranking_agentes:
            uid = ag["user_id"]
            if uid == -1:
                ag["meta"] = 0
                ag["meta_intermediaria"] = 0
                ag["supermeta"] = 0
                ag["metas_cat"] = {}
                continue
            uki = _kommo_uid_int(uid)
            m = mat_metas.get(uki, {}) if uki is not None else {}
            ag["meta"] = m.get("meta", 0)
            ag["meta_intermediaria"] = m.get("intermediaria", 0)
            ag["supermeta"] = m.get("supermeta", 0)
            ag["metas_cat"] = {}
            for cat, users in metas_by_cat.items():
                if uki is not None and uki in users:
                    ag["metas_cat"][cat] = users[uki]

        # --- Evasão: RGMs brutos que não são EM CURSO → breakdown por tipo e por agente ---
        evasao_data = {"total": 0, "por_tipo": {}, "por_agente": [], "itens": []}
        if evasao_rows:
            # Mapa RGM → responsible_user_id via Kommo (reutiliza a query do ranking)
            ev_rgm_to_uid = {}
            ev_uid_to_nome = {}
            try:
                ek_conn = _pg_kommo()
                ek_cur = ek_conn.cursor()
                ek_cur.execute("""
                    SELECT DISTINCT ON (v.rgm) v.rgm, l.responsible_user_id,
                           u.name AS user_name
                    FROM vw_leads_rgm v
                    JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                    LEFT JOIN users u ON u.id = l.responsible_user_id
                    WHERE l.responsible_user_id IS NOT NULL
                    ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
                """)
                for row_k in ek_cur.fetchall():
                    nk = _normalize_rgm(row_k[0])
                    if nk:
                        ev_rgm_to_uid[nk] = row_k[1]
                        if row_k[1] and row_k[2]:
                            ev_uid_to_nome[row_k[1]] = row_k[2]
                ek_cur.close()
                ek_conn.close()
            except Exception as ek_e:
                logger.warning("evasao kommo lookup: %s", ek_e)

            por_tipo = defaultdict(int)
            por_agente = defaultdict(list)

            for ev in evasao_rows:
                sit = ev["situacao"] or "OUTROS"
                por_tipo[sit] += 1
                uid_ev = ev_rgm_to_uid.get(ev["rgm"])
                nome_ev = ev_uid_to_nome.get(uid_ev, "Não identificado") if uid_ev else "Não identificado"
                por_agente[nome_ev].append(ev)

            evasao_data = {
                "total": len(evasao_rows),
                "por_tipo": dict(por_tipo),
                "por_agente": [
                    {"agente": ag_nome, "total": len(itens),
                     "itens": [{"rgm": i["rgm"], "nome": i["nome"],
                                "situacao": i["situacao"], "data_matricula": i["data_matricula"]}
                               for i in itens]}
                    for ag_nome, itens in sorted(por_agente.items(), key=lambda x: -len(x[1]))
                ],
            }

        metas_aviso = None
        if metas_load_error:
            metas_aviso = (
                "Não foi possível carregar as metas deste período. "
                "Verifique o log do servidor ou a conexão com o banco."
            )
        elif not mat_metas and any(
            (a.get("matriculas_periodo") or 0) > 0 and a.get("user_id") != -1
            for a in ranking_agentes
        ):
            metas_aviso = (
                "Nenhuma meta de matrículas cadastrada para o intervalo de datas dos filtros "
                "(ou campanha sem sobreposição). Cadastre metas comerciais ou ajuste dt início/fim."
            )

        return jsonify({
            "ok": True,
            "metas_aviso": metas_aviso,
            "kpis": {
                "vendas": vendas,
                "vendas_liquidas": vendas_liquidas,
                "vendas_6m": vendas_6m,
                "pct_6m": pct_6m,
                "vendas_1a": vendas_1a,
                "pct_1a": pct_1a,
                "vendas_ytd": vendas_ytd,
                "vendas_prev_ytd": vendas_prev_ytd,
                "pct_ytd": pct_ytd,
                "ticket_medio": ticket_medio,
                "media_diaria": media_diaria,
                "dias": dias,
                "mm_inscritos": mm_insc_count,
            },
            "evolucao": evolucao,
            "evolucao_bruto": evolucao_bruto,
            "evolucao_prev": evolucao_prev,
            "ranking_polo": ranking_polo,
            "ranking_ciclo": ranking_ciclo,
            "ranking_agentes": ranking_agentes,
            "transferencia_regresso": transferencia_regresso,
            "evasao": evasao_data,
        })
    except Exception as e:
        logger.exception("comercial_rgm data error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/agente-detalhe")
def crgm_agente_detalhe():
    """Lista as matrículas do período para um agente específico (ou transferencia/regresso)."""
    from flask import Response as _FlaskResponse

    try:
        user_id  = request.args.get("user_id", "")
        dt_ini   = request.args.get("dt_ini", "")
        dt_fim   = request.args.get("dt_fim", "")
        polo     = request.args.get("polo", "")
        nivel    = request.args.get("nivel", "")
        ciclo    = request.args.get("ciclo", "")
        turma    = request.args.get("turma", "")
        fmt      = request.args.get("fmt", "json")   # json | csv

        try:
            uid = int(user_id) if user_id not in ("", "-1") else -1
        except ValueError:
            return jsonify({"ok": False, "error": "user_id inválido"}), 400

        # 1. Buscar todos os RGMs do período (mesma lógica do ranking)
        where, params = [], []
        if polo:    where.append(f"{_POLO_SQL} = %s");           params.append(_normalize_polo(polo))
        if nivel:   where.append("nivel = %s");          params.append(nivel)
        if dt_ini:  where.append("data_matricula >= %s"); params.append(dt_ini)
        if dt_fim:  where.append("data_matricula <= %s"); params.append(dt_fim)
        if ciclo:   where.append("ciclo = %s");          params.append(ciclo)
        if turma:   where.append("turma = %s");          params.append(turma)
        w = ("WHERE " + " AND ".join(where)) if where else ""

        try:
            conn = _pg()
            cur  = conn.cursor()
            cur.execute(
                f"SELECT rgm, nome, polo, nivel, data_matricula, ciclo, turma, tipo_matricula "
                f"FROM comercial_rgm_atual {w} ORDER BY data_matricula DESC NULLS LAST",
                params,
            )
            rows = cur.fetchall()
            _det_excluded = _crgm_excluded_rgms(conn)
            cur.close(); conn.close()
        except Exception as e:
            logger.warning("agente-detalhe db: %s", e)
            return jsonify({"ok": False, "error": str(e)}), 500

        # 2. Mapa RGM → responsible_user_id (Kommo)
        try:
            kconn = _pg_kommo()
            kcur  = kconn.cursor()
            kcur.execute("""
                SELECT DISTINCT ON (v.rgm) v.rgm, l.responsible_user_id
                FROM vw_leads_rgm v
                JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                WHERE l.responsible_user_id IS NOT NULL
                ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
            """)
            rgm_to_uid = {}
            for row in kcur.fetchall():
                nk = _normalize_rgm(row[0])
                if nk and row[1]:
                    rgm_to_uid[nk] = row[1]
            kcur.close(); kconn.close()
        except Exception as e:
            logger.warning("agente-detalhe kommo: %s", e)
            rgm_to_uid = {}

        # 3. Calcular prefixo dominante apenas para exibição visual (não afeta contagem)
        from collections import Counter as _Counter
        _pfx_c = _Counter()
        for row in rows:
            n = _normalize_rgm(row[0])
            if n and len(n) >= 2 and n[:2].isdigit():
                _pfx_c[n[:2]] += 1
        dominant_prefix = int(_pfx_c.most_common(1)[0][0]) if _pfx_c else 99

        # 4. Filtrar linhas do agente solicitado
        seen = set()
        resultado = []
        for row in rows:
            rgm_raw, nome, p_polo, p_nivel, dm, ciclo_v, turma_v, tipo_mat = row
            n = _normalize_rgm(rgm_raw)
            if not n or n in seen or n in _det_excluded:
                continue
            seen.add(n)
            assigned_uid = rgm_to_uid.get(n)
            if uid == -1:
                if assigned_uid is not None:
                    continue
            else:
                if assigned_uid != uid:
                    continue
            try:
                data_str = dm.isoformat() if hasattr(dm, "isoformat") else str(dm)[:10]
            except Exception:
                data_str = ""
            # Flag outlier: qualquer RGM cujo prefixo seja inferior ao dominante do ciclo
            rgm_prefix = int(n[:2]) if len(n) >= 2 and n[:2].isdigit() else dominant_prefix
            outlier = rgm_prefix < dominant_prefix
            resultado.append({
                "rgm": rgm_raw or "",
                "nome": nome or "",
                "polo": p_polo or "",
                "nivel": p_nivel or "",
                "data_matricula": data_str,
                "ciclo": ciclo_v or "",
                "turma": turma_v or "",
                "tipo_matricula": tipo_mat or "",
                "outlier": outlier,
            })

        if fmt == "csv":
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=";")
            writer.writerow(["RGM", "Nome", "Polo", "Nível", "Data Matrícula", "Ciclo", "Turma", "Tipo Matrícula", "Outlier RGM"])
            for r in resultado:
                writer.writerow([r["rgm"], r["nome"], r["polo"], r["nivel"],
                                 r["data_matricula"], r["ciclo"], r["turma"],
                                 r["tipo_matricula"], "SIM" if r["outlier"] else ""])
            safe_uid = str(uid).replace("-", "neg")
            return _FlaskResponse(
                buf.getvalue(),
                mimetype="text/csv; charset=utf-8",
                headers={"Content-Disposition": f"attachment; filename=matriculas_agente_{safe_uid}.csv"},
            )

        return jsonify({"ok": True, "total": len(resultado), "itens": resultado})

    except Exception as e:
        logger.exception("agente-detalhe erro inesperado: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/categorias")
def crgm_metas_categorias():
    return jsonify({"ok": True, "categorias": METAS_CATEGORIAS})


@comercial_rgm_bp.route("/api/comercial-rgm/diagnostics")
def crgm_diagnostics():
    """Diagnostic endpoint to debug RGM matching between CSV/MM and Kommo."""
    try:
        # RGMs from CSV
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT rgm FROM comercial_rgm")
        csv_rgms = set()
        csv_raw_samples = []
        for r in cur.fetchall():
            raw = r[0]
            n = _normalize_rgm(raw)
            if n:
                csv_rgms.add(n)
                if len(csv_raw_samples) < 5:
                    csv_raw_samples.append({"raw": raw, "normalized": n})

        # RGMs from MM
        cur.execute("SELECT rgm, tipo_matricula FROM mm_matriculados LIMIT 500")
        mm_rgms = set()
        mm_raw_samples = []
        for r in cur.fetchall():
            n = _normalize_rgm(r[0])
            if n:
                mm_rgms.add(n)
                if len(mm_raw_samples) < 5:
                    mm_raw_samples.append({"raw": r[0], "normalized": n, "tipo": r[1]})

        cur.close()
        conn.close()

        # RGMs from Kommo (source 1: cf_values)
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT lcf.field_name,
                   lcf.values_json->0->>'value' AS raw_val,
                   l.responsible_user_id, l.status_id
            FROM lead_custom_field_values lcf
            JOIN leads l ON l.id = lcf.lead_id AND l.is_deleted = FALSE
            WHERE LOWER(lcf.field_name) = 'rgm'
              AND lcf.values_json->0->>'value' IS NOT NULL
              AND lcf.values_json->0->>'value' != ''
        """)
        kommo_cf_rgms = {}
        cf_samples = []
        for r in kcur.fetchall():
            n = _normalize_rgm(r[1])
            if n:
                kommo_cf_rgms[n] = r[2]
                if len(cf_samples) < 5:
                    cf_samples.append({"field_name": r[0], "raw": r[1], "normalized": n,
                                       "user_id": r[2], "status": r[3]})

        # RGMs from Kommo (source 2: custom_fields_json)
        kcur.execute("""
            SELECT cf_elem->>'field_name' AS fname,
                   cf_elem->'values'->0->>'value' AS raw_val,
                   l.responsible_user_id, l.status_id
            FROM leads l,
                 jsonb_array_elements(COALESCE(l.custom_fields_json, '[]'::jsonb)) cf_elem
            WHERE l.is_deleted = FALSE
              AND LOWER(cf_elem->>'field_name') = 'rgm'
              AND cf_elem->'values'->0->>'value' IS NOT NULL
              AND cf_elem->'values'->0->>'value' != ''
        """)
        kommo_json_rgms = {}
        json_samples = []
        for r in kcur.fetchall():
            n = _normalize_rgm(r[1])
            if n:
                kommo_json_rgms[n] = r[2]
                if len(json_samples) < 5:
                    json_samples.append({"field_name": r[0], "raw": r[1], "normalized": n,
                                          "user_id": r[2], "status": r[3]})

        # Also check distinct field_name values that contain 'rgm'
        kcur.execute("""
            SELECT DISTINCT field_name FROM lead_custom_field_values
            WHERE LOWER(field_name) LIKE '%rgm%'
        """)
        rgm_field_names = [r[0] for r in kcur.fetchall()]

        kcur.close()
        kconn.close()

        all_kommo = set(kommo_cf_rgms.keys()) | set(kommo_json_rgms.keys())
        all_base = csv_rgms | mm_rgms
        matched = all_base & all_kommo
        unmatched = all_base - all_kommo

        return jsonify({
            "ok": True,
            "csv_rgms": len(csv_rgms),
            "mm_rgms": len(mm_rgms),
            "all_base_rgms": len(all_base),
            "kommo_cf_values_rgms": len(kommo_cf_rgms),
            "kommo_json_rgms": len(kommo_json_rgms),
            "kommo_total_unique": len(all_kommo),
            "matched": len(matched),
            "unmatched": len(unmatched),
            "match_rate": f"{len(matched)/max(len(all_base),1)*100:.1f}%",
            "rgm_field_names_in_kommo": rgm_field_names,
            "samples": {
                "csv": csv_raw_samples,
                "mm": mm_raw_samples,
                "kommo_cf": cf_samples,
                "kommo_json": json_samples,
                "unmatched": sorted(list(unmatched))[:15],
                "matched": sorted(list(matched))[:15],
            }
        })
    except Exception as e:
        logger.exception("diagnostics error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas", methods=["GET"])
def crgm_get_metas():
    try:
        conn = _pg()
        cur = conn.cursor()
        dt_ini = request.args.get("dt_ini", "")
        dt_fim = request.args.get("dt_fim", "")
        categoria = request.args.get("categoria", "")
        wheres = []
        params = []
        if dt_ini and dt_fim:
            wheres.append("dt_inicio <= %s AND dt_fim >= %s")
            params.extend([dt_fim, dt_ini])
        if categoria:
            wheres.append("categoria = %s")
            params.append(categoria)
        w = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        cur.execute(f"""
            SELECT id, user_id, user_name, meta,
                   COALESCE(meta_intermediaria,0), COALESCE(supermeta,0),
                   dt_inicio, dt_fim, descricao, categoria
            FROM comercial_metas {w}
            ORDER BY dt_inicio DESC, categoria, user_name
        """, params)
        rows = [{"id": r[0], "user_id": r[1], "user_name": r[2],
                 "meta": float(r[3]), "meta_intermediaria": float(r[4]),
                 "supermeta": float(r[5]),
                 "dt_inicio": r[6].isoformat() if r[6] else None,
                 "dt_fim": r[7].isoformat() if r[7] else None,
                 "descricao": r[8], "categoria": r[9] or "matriculas"}
                for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({"ok": True, "metas": rows, "categorias": METAS_CATEGORIAS})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas", methods=["POST"])
def crgm_save_metas():
    data = request.get_json(force=True)
    metas = data.get("metas", [])
    if not metas:
        return jsonify({"error": "Nenhuma meta enviada"}), 400
    valid_cats = {c["id"] for c in METAS_CATEGORIAS}
    try:
        conn = _pg()
        cur = conn.cursor()
        saved = 0
        for m in metas:
            uid = int(m["user_id"])
            meta_val = float(m.get("meta", 0))
            intermediaria = float(m.get("meta_intermediaria", 0))
            supermeta = float(m.get("supermeta", 0))
            name = m.get("user_name", "")
            dt_inicio = m.get("dt_inicio")
            dt_fim = m.get("dt_fim")
            descricao = m.get("descricao", "")
            categoria = m.get("categoria", "matriculas")
            if categoria not in valid_cats:
                categoria = "matriculas"
            if not dt_inicio or not dt_fim:
                continue
            if meta_val <= 0 and intermediaria <= 0 and supermeta <= 0:
                continue
            cur.execute("""
                INSERT INTO comercial_metas
                    (user_id, user_name, meta, meta_intermediaria, supermeta,
                     categoria, dt_inicio, dt_fim, descricao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (uid, name, meta_val, intermediaria, supermeta,
                  categoria, dt_inicio, dt_fim, descricao))
            saved += 1
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": saved})
    except Exception as e:
        logger.exception("save metas error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/batch", methods=["PUT"])
def crgm_update_metas_batch():
    """Salva metas de múltiplos agentes de uma vez para um período."""
    try:
        body    = request.json or {}
        dt_ini  = body.get("dt_inicio")
        dt_fim  = body.get("dt_fim")
        descr   = body.get("descricao", "")
        cat     = body.get("categoria", "matriculas")
        items   = body.get("items", [])   # [{user_id, user_name, meta, meta_intermediaria, supermeta}]

        if not dt_ini or not dt_fim or not items:
            return jsonify({"ok": False, "error": "dt_inicio, dt_fim e items são obrigatórios"}), 400

        conn = _pg()
        cur  = conn.cursor()
        saved = 0
        for it in items:
            uid   = it.get("user_id")
            uname = it.get("user_name", "")
            meta  = float(it.get("meta", 0) or 0)
            interm= float(it.get("meta_intermediaria", 0) or 0)
            sup   = float(it.get("supermeta", 0) or 0)
            if not uid:
                continue
            cur.execute("""
                INSERT INTO comercial_metas
                    (user_id, user_name, meta, meta_intermediaria, supermeta,
                     dt_inicio, dt_fim, descricao, categoria)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (user_id, dt_inicio, dt_fim, categoria)
                DO UPDATE SET
                    meta               = EXCLUDED.meta,
                    meta_intermediaria = EXCLUDED.meta_intermediaria,
                    supermeta          = EXCLUDED.supermeta,
                    user_name          = EXCLUDED.user_name,
                    descricao          = EXCLUDED.descricao
            """, (uid, uname, meta, interm, sup, dt_ini, dt_fim, descr, cat))
            saved += 1

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": saved})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/<int:meta_id>", methods=["PUT"])
def crgm_update_meta(meta_id):
    try:
        body = request.json or {}
        meta_val   = float(body.get("meta", 0))
        interm_val = float(body.get("meta_intermediaria", 0))
        super_val  = float(body.get("supermeta", 0))
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            UPDATE comercial_metas
            SET meta = %s, meta_intermediaria = %s, supermeta = %s
            WHERE id = %s
        """, (meta_val, interm_val, super_val, meta_id))
        conn.commit()
        updated = cur.rowcount
        cur.close()
        conn.close()
        return jsonify({"ok": True, "updated": updated})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/<int:meta_id>", methods=["DELETE"])
def crgm_delete_meta(meta_id):
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM comercial_metas WHERE id = %s", (meta_id,))
        conn.commit()
        deleted = cur.rowcount
        cur.close()
        conn.close()
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# â”€â”€ Atualizar 1 lead (Kommo → PostgreSQL) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/kommo-sync-lead", methods=["POST"])
def crgm_kommo_sync_lead():
    """
    Busca um lead na API Kommo e grava em kommo_sync (Postgres).
    Body JSON: { "lead_id": 20796123 } OU { "rgm": "48411612" }
    Se vários leads com o mesmo RGM, retorna lista para escolher (ou use lead_id).
    """
    if not KOMMO_TOKEN:
        return jsonify({"ok": False, "error": "KOMMO_TOKEN não configurado no servidor."}), 500
    try:
        body = request.get_json(force=True, silent=True) or {}
        lead_id = body.get("lead_id")
        rgm = body.get("rgm")

        if lead_id is not None and str(lead_id).strip():
            try:
                lid = int(lead_id)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "ID do lead inválido."}), 400
        else:
            lid = None

        rgm_clean = re.sub(r"[^0-9]", "", str(rgm or ""))
        if lid is None and len(rgm_clean) == 8:
            found, err = _kommo_resolve_lead_id_by_rgm(rgm_clean)
            if err:
                return jsonify({"ok": False, "error": err}), 404
            if len(found) > 1:
                return jsonify({
                    "ok": False,
                    "error": "Vários leads com esse RGM. Informe o ID do lead correto.",
                    "lead_ids": found,
                }), 409
            lid = found[0]
        elif lid is None:
            return jsonify({
                "ok": False,
                "error": "Informe o ID do lead (Kommo) ou o RGM com 8 dígitos.",
            }), 400

        lead = _kommo_fetch_lead_full(lid)
        if not lead:
            return jsonify({
                "ok": False,
                "error": f"Lead {lid} não encontrado na API (verifique token e URL KOMMO_BASE_URL).",
            }), 404

        _kommo_upsert_lead_postgres(lead)

        cfs = lead.get("custom_fields_values") or []
        rgm_out = None
        for cf in cfs:
            if str(cf.get("field_name", "")).lower() == "rgm":
                rgm_out = (cf.get("values") or [{}])[0].get("value")
                break

        pipeline = None
        try:
            kc = _pg_kommo()
            kcur = kc.cursor()
            kcur.execute(
                "SELECT p.name FROM pipelines p WHERE p.id = %s",
                (lead.get("pipeline_id"),),
            )
            pr = kcur.fetchone()
            pipeline = pr[0] if pr else None
            kcur.close()
            kc.close()
        except Exception:
            pass

        st = lead.get("status_id")
        status_txt = "Ganho" if st == 142 else "Perdido" if st == 143 else f"Ativo ({st})"

        return jsonify({
            "ok": True,
            "lead_id": lead["id"],
            "nome_card": lead.get("name"),
            "rgm": rgm_out,
            "pipeline": pipeline,
            "pipeline_id": lead.get("pipeline_id"),
            "status": status_txt,
            "msg": "Lead atualizado no banco kommo_sync. O cruzamento com matrículas pode refletir na próxima carga.",
        })
    except Exception as e:
        logger.exception("kommo-sync-lead")
        return jsonify({"ok": False, "error": str(e)}), 500


# â”€â”€ Duplicatas (cross-ref comercial_rgm_completa x leads) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@comercial_rgm_bp.route("/api/comercial-rgm/duplicatas")
def crgm_duplicatas():
    """Detect RGMs from comercial_rgm_completa that map to multiple Kommo leads.

    Two-phase approach for performance:
      1) Lightweight pass: find RGMs with >1 lead (rgm + lead_id only)
      2) Detail pass: fetch lead info only for the duplicated RGMs
    """
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT rgm FROM comercial_rgm_completa WHERE rgm IS NOT NULL AND rgm <> ''")
        completa_rgms = {r[0] for r in cur.fetchall()}
        cur.close()
        conn.close()

        if not completa_rgms:
            return jsonify({"ok": True, "duplicatas": [], "total": 0})

        kconn = _pg_kommo()
        kcur = kconn.cursor()

        kcur.execute("""
            SELECT rgm_clean, lead_id
            FROM (
                SELECT l.id AS lead_id,
                       regexp_replace((lcf.values_json->0)->>'value', '[^0-9]', '', 'g') AS rgm_clean
                FROM leads l
                JOIN lead_custom_field_values lcf
                  ON lcf.lead_id = l.id
                 AND lower(lcf.field_name) = 'rgm'
                 AND (lcf.values_json->0)->>'value' IS NOT NULL
                 AND (lcf.values_json->0)->>'value' <> ''
                 AND length(regexp_replace((lcf.values_json->0)->>'value', '[^0-9]', '', 'g')) = 8
                JOIN pipelines p ON p.id = l.pipeline_id
                 AND p.name IN ('Funil de vendas', 'Licenciado')
                WHERE l.is_deleted = false
            ) sub
        """)

        rgm_leads = {}
        for rgm, lid in kcur.fetchall():
            if rgm in completa_rgms:
                rgm_leads.setdefault(rgm, []).append(lid)

        dup_rgm_leads = {k: v for k, v in rgm_leads.items() if len(v) > 1}

        if not dup_rgm_leads:
            kcur.close()
            kconn.close()
            return jsonify({"ok": True, "duplicatas": [], "total": 0,
                            "total_completa_rgms": len(completa_rgms)})

        all_dup_ids = []
        for ids in dup_rgm_leads.values():
            all_dup_ids.extend(ids)

        kcur.execute("""
            SELECT l.id,
                   COALESCE(u.name, 'N/A'),
                   l.price,
                   p.name,
                   CASE l.status_id WHEN 142 THEN 'Ganho' WHEN 143 THEN 'Perdido' ELSE 'Ativo' END
            FROM leads l
            JOIN pipelines p ON p.id = l.pipeline_id
            LEFT JOIN users u ON u.id = l.responsible_user_id
            WHERE l.id = ANY(%s)
        """, (all_dup_ids,))

        lead_info = {}
        for r in kcur.fetchall():
            lead_info[r[0]] = {
                "lead_id": r[0], "consultora": r[1], "preco": r[2],
                "pipeline": r[3], "status": r[4],
            }
        kcur.close()
        kconn.close()

        duplicatas = []
        for rgm, ids in sorted(dup_rgm_leads.items(), key=lambda x: -len(x[1])):
            leads = [lead_info.get(lid, {"lead_id": lid, "consultora": "?", "preco": 0, "pipeline": "?", "status": "?"})
                     for lid in sorted(ids, reverse=True)]
            duplicatas.append({"rgm": rgm, "count": len(leads), "leads": leads})

        return jsonify({
            "ok": True,
            "duplicatas": duplicatas,
            "total": len(duplicatas),
            "total_completa_rgms": len(completa_rgms),
        })
    except Exception as e:
        logger.exception("duplicatas error")
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Conflitos de atribuição ───────────────────────────────────────────────────

@comercial_rgm_bp.route("/api/comercial-rgm/conflitos")
def crgm_conflitos():
    """Retorna RGMs do painel atual (filtrado por data) com múltiplos agentes no Kommo."""
    dt_ini = request.args.get("dt_ini", "")
    dt_fim = request.args.get("dt_fim", "")
    polo   = request.args.get("polo", "")
    nivel  = request.args.get("nivel", "")
    try:
        # 1. RGMs e nomes do painel no período filtrado
        conn = _pg()
        cur = conn.cursor()
        cw, cp = [], []
        if polo:
            cw.append(f"{_POLO_SQL} = %s"); cp.append(_normalize_polo(polo))
        if nivel:
            cw.append("nivel = %s"); cp.append(nivel)
        if dt_ini:
            cw.append("data_matricula >= %s"); cp.append(dt_ini)
        if dt_fim:
            cw.append("data_matricula <= %s"); cp.append(dt_fim)
        w = ("WHERE " + " AND ".join(cw)) if cw else ""
        cur.execute(
            f"SELECT rgm, nome, data_matricula FROM comercial_rgm_atual {w} ORDER BY data_matricula DESC NULLS LAST",
            cp,
        )
        rgm_info = {}
        for rgm, nome, dm in cur.fetchall():
            n = _normalize_rgm(rgm)
            if n and n not in rgm_info:
                rgm_info[n] = {"nome": (nome or "").strip(), "data_matricula": dm.isoformat() if dm else None}
        cur.close()
        conn.close()

        if not rgm_info:
            return jsonify({"ok": True, "conflitos": [], "total": 0})

        # 2. Todos os leads ativos para esses RGMs no Kommo
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT v.rgm, l.id AS lead_id, l.responsible_user_id,
                   l.status_id, u.name AS agente_nome,
                   ps.name AS status_nome
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            LEFT JOIN users u ON u.id = l.responsible_user_id
            LEFT JOIN pipeline_statuses ps ON ps.id = l.status_id
            WHERE l.responsible_user_id IS NOT NULL
            ORDER BY v.rgm,
                     CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END,
                     l.id DESC
        """)

        from collections import defaultdict
        rgm_leads_map = defaultdict(list)
        for rgm_raw, lead_id, uid, status_id, agente, status_nome in kcur.fetchall():
            nk = _normalize_rgm(rgm_raw)
            if nk and nk in rgm_info:
                rgm_leads_map[nk].append({
                    "lead_id": lead_id,
                    "user_id": uid,
                    "agente": agente or f"User #{uid}",
                    "status_id": status_id,
                    "status_nome": status_nome or "",
                })

        # 3. Resolucoes ja salvas
        kcur.close(); kconn.close()
        conn2 = _pg()
        cur2 = conn2.cursor()
        cur2.execute("SELECT rgm, user_id FROM comercial_rgm_conflito_resolucao")
        resolucoes = {_normalize_rgm(r[0]): r[1] for r in cur2.fetchall() if _normalize_rgm(r[0])}
        cur2.close(); conn2.close()

        # 4. Filtra apenas RGMs com agentes diferentes
        conflitos = []
        for rgm, leads in rgm_leads_map.items():
            agentes_set = {l["user_id"] for l in leads}
            if len(agentes_set) <= 1:
                continue
            # Vencedor atual (primeiro da lista, já ordenado)
            uid_atual = leads[0]["user_id"]
            # Override salvo
            uid_override = resolucoes.get(rgm)
            info = rgm_info[rgm]
            conflitos.append({
                "rgm": rgm,
                "nome_aluno": info["nome"],
                "data_matricula": info["data_matricula"],
                "user_id_atual": uid_atual,
                "user_id_resolucao": uid_override,
                "resolvido": uid_override is not None,
                "leads": leads,
            })

        conflitos.sort(key=lambda x: (x["resolvido"], x["nome_aluno"]))
        return jsonify({"ok": True, "conflitos": conflitos, "total": len(conflitos),
                        "total_nao_resolvidos": sum(1 for c in conflitos if not c["resolvido"])})
    except Exception as e:
        logger.exception("conflitos error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/conflitos/resolver", methods=["POST"])
def crgm_conflitos_resolver():
    """Salva resoluções de conflito: [{rgm, user_id, user_name}]."""
    data = request.get_json(force=True) or {}
    items = data.get("items", [])
    if not items:
        return jsonify({"ok": False, "error": "items vazios"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        for item in items:
            rgm = _normalize_rgm(str(item.get("rgm", "")))
            uid = item.get("user_id")
            nome = item.get("user_name", "")
            if not rgm or not uid:
                continue
            cur.execute("""
                INSERT INTO comercial_rgm_conflito_resolucao (rgm, user_id, user_name, resolved_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (rgm) DO UPDATE
                  SET user_id = EXCLUDED.user_id,
                      user_name = EXCLUDED.user_name,
                      resolved_at = NOW()
            """, (rgm, uid, nome))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": len(items)})
    except Exception as e:
        logger.exception("conflitos resolver error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/conflitos/resolver", methods=["DELETE"])
def crgm_conflitos_resolver_delete():
    """Remove uma resolução de conflito pelo RGM."""
    data = request.get_json(force=True) or {}
    rgm = _normalize_rgm(str(data.get("rgm", "")))
    if not rgm:
        return jsonify({"ok": False, "error": "rgm obrigatório"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM comercial_rgm_conflito_resolucao WHERE rgm = %s", (rgm,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
