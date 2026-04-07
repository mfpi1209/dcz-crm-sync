"""
Módulo de banco de dados SQLite + PostgreSQL.
Gerencia schema, conexões e operações CRUD para sincronização Kommo.

Estratégia: UPSERT incremental
- Não faz DROP TABLE; usa INSERT OR REPLACE
- Mantém dados disponíveis durante o sync
- sync_metadata fica no PostgreSQL (persiste entre deploys)
- Dados de staging (leads, contacts, etc.) ficam no SQLite
- Cada batch de leads/contatos também pode espelhar no PostgreSQL (kommo_sync) na hora,
  para o dashboard não depender só do migrate_to_postgres.py. Ver KOMMO_DUAL_WRITE_PG.
"""

import os
import sqlite3
import json
import logging
from datetime import datetime
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from config import DB_PATH

logger = logging.getLogger(__name__)

# PostgreSQL (kommo_sync) — usado para sync_metadata persistente
_PG_DSN = dict(
    host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "31.97.91.47")),
    port=int(os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432"))),
    user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER", "adm_eduit")),
    password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS", "IaDm24Sx3HxrYoqT")),
    dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
)


def _pg():
    return psycopg2.connect(**_PG_DSN)


_PG_SYNC_META_DDL = """
CREATE TABLE IF NOT EXISTS sync_metadata (
    entity_type     TEXT PRIMARY KEY,
    last_sync_at    TEXT,
    last_full_sync_at TEXT,
    records_synced  INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'pending'
);
"""


# ============================
# Schema SQL
# ============================

SCHEMA_SQL = """
-- Metadados de sincronização
CREATE TABLE IF NOT EXISTS sync_metadata (
    entity_type     TEXT PRIMARY KEY,
    last_sync_at    TEXT,
    last_full_sync_at TEXT,
    records_synced  INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'pending'
);

-- Pipelines
CREATE TABLE IF NOT EXISTS pipelines (
    id              INTEGER PRIMARY KEY,
    name            TEXT,
    sort            INTEGER,
    is_main         INTEGER DEFAULT 0,
    is_unsorted_on  INTEGER DEFAULT 0,
    is_archive      INTEGER DEFAULT 0,
    account_id      INTEGER,
    raw_json        TEXT,
    synced_at       TEXT
);

-- Stages (status dos pipelines)
CREATE TABLE IF NOT EXISTS pipeline_statuses (
    id              INTEGER PRIMARY KEY,
    pipeline_id     INTEGER,
    name            TEXT,
    sort            INTEGER,
    is_editable     INTEGER DEFAULT 1,
    color           TEXT,
    type            INTEGER DEFAULT 0,
    account_id      INTEGER,
    raw_json        TEXT,
    synced_at       TEXT,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);

-- Leads
CREATE TABLE IF NOT EXISTS leads (
    id                  INTEGER PRIMARY KEY,
    name                TEXT,
    price               INTEGER DEFAULT 0,
    responsible_user_id INTEGER,
    group_id            INTEGER,
    status_id           INTEGER,
    pipeline_id         INTEGER,
    loss_reason_id      INTEGER,
    source_id           INTEGER,
    created_by          INTEGER,
    updated_by          INTEGER,
    closed_at           INTEGER,
    created_at          INTEGER,
    updated_at          INTEGER,
    closest_task_at     INTEGER,
    is_deleted          INTEGER DEFAULT 0,
    score               REAL,
    account_id          INTEGER,
    labor_cost          INTEGER,
    is_price_modified   INTEGER DEFAULT 0,
    custom_fields_json  TEXT,
    tags_json           TEXT,
    contacts_json       TEXT,
    raw_json            TEXT,
    synced_at           TEXT,
    FOREIGN KEY (pipeline_id) REFERENCES pipelines(id),
    FOREIGN KEY (status_id) REFERENCES pipeline_statuses(id)
);

-- Contatos
CREATE TABLE IF NOT EXISTS contacts (
    id                  INTEGER PRIMARY KEY,
    name                TEXT,
    first_name          TEXT,
    last_name           TEXT,
    responsible_user_id INTEGER,
    group_id            INTEGER,
    created_by          INTEGER,
    updated_by          INTEGER,
    created_at          INTEGER,
    updated_at          INTEGER,
    closest_task_at     INTEGER,
    is_deleted          INTEGER DEFAULT 0,
    account_id          INTEGER,
    custom_fields_json  TEXT,
    tags_json           TEXT,
    raw_json            TEXT,
    synced_at           TEXT
);

-- Definição de campos customizados
CREATE TABLE IF NOT EXISTS custom_fields (
    id              INTEGER,
    entity_type     TEXT,
    name            TEXT,
    type            TEXT,
    sort            INTEGER,
    code            TEXT,
    is_api_only     INTEGER DEFAULT 0,
    group_id        TEXT,
    required_statuses TEXT,
    enums_json      TEXT,
    raw_json        TEXT,
    synced_at       TEXT,
    PRIMARY KEY (id, entity_type)
);

-- Valores de campos customizados de leads (normalizado)
CREATE TABLE IF NOT EXISTS lead_custom_field_values (
    lead_id         INTEGER,
    field_id        INTEGER,
    field_name      TEXT,
    field_code      TEXT,
    field_type      TEXT,
    values_json     TEXT,
    synced_at       TEXT,
    PRIMARY KEY (lead_id, field_id),
    FOREIGN KEY (lead_id) REFERENCES leads(id)
);

-- Valores de campos customizados de contatos (normalizado)
CREATE TABLE IF NOT EXISTS contact_custom_field_values (
    contact_id      INTEGER,
    field_id        INTEGER,
    field_name      TEXT,
    field_code      TEXT,
    field_type      TEXT,
    values_json     TEXT,
    synced_at       TEXT,
    PRIMARY KEY (contact_id, field_id),
    FOREIGN KEY (contact_id) REFERENCES contacts(id)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_leads_pipeline ON leads(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status_id);
CREATE INDEX IF NOT EXISTS idx_leads_updated ON leads(updated_at);
CREATE INDEX IF NOT EXISTS idx_leads_responsible ON leads(responsible_user_id);
CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at);
CREATE INDEX IF NOT EXISTS idx_leads_synced ON leads(synced_at);

CREATE INDEX IF NOT EXISTS idx_contacts_updated ON contacts(updated_at);
CREATE INDEX IF NOT EXISTS idx_contacts_synced ON contacts(synced_at);
CREATE INDEX IF NOT EXISTS idx_contacts_responsible ON contacts(responsible_user_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_statuses_pipeline ON pipeline_statuses(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_lead_cf_field ON lead_custom_field_values(field_id);
CREATE INDEX IF NOT EXISTS idx_contact_cf_field ON contact_custom_field_values(field_id);
"""


# ============================
# Gerenciamento de Conexão
# ============================

def get_connection() -> sqlite3.Connection:
    """Cria conexão SQLite com configurações otimizadas (menos travamentos)."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")       # 32MB (reduz pico de memória)
    conn.execute("PRAGMA busy_timeout=10000")      # 10s: espera em vez de travar se DB ocupado
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db():
    """Context manager para conexão com o banco."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_database():
    """Inicializa SQLite (staging) e PostgreSQL (sync_metadata persistente)."""
    logger.info("Inicializando banco SQLite: %s", DB_PATH)
    with get_db() as conn:
        conn.executescript(SCHEMA_SQL)
    logger.info("SQLite inicializado.")

    try:
        pg = _pg()
        cur = pg.cursor()
        cur.execute(_PG_SYNC_META_DDL)
        pg.commit()
        cur.close()
        pg.close()
        logger.info("PostgreSQL sync_metadata inicializado.")
    except Exception as e:
        logger.error("Falha ao inicializar sync_metadata no PG: %s", e)


# ============================
# Operações de Sync Metadata
# ============================

def get_last_sync(entity_type: str) -> dict | None:
    """Retorna metadados da última sincronização (do PostgreSQL persistente)."""
    try:
        pg = _pg()
        cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM sync_metadata WHERE entity_type = %s", (entity_type,))
        row = cur.fetchone()
        cur.close()
        pg.close()
        return dict(row) if row else None
    except Exception as e:
        logger.error("get_last_sync PG error: %s", e)
        return None


def update_sync_metadata(entity_type: str, records_synced: int, is_full_sync: bool = False):
    """Atualiza os metadados de sincronização no PostgreSQL.

    PROTEÇÃO: Em delta sync, só avança o cursor se registros foram encontrados.
    Isso evita criar gaps quando a API retorna 0 por erro/bug temporário.
    """
    now = datetime.utcnow().isoformat()
    try:
        pg = _pg()
        cur = pg.cursor()
        cur.execute("SELECT 1 FROM sync_metadata WHERE entity_type = %s", (entity_type,))
        exists = cur.fetchone() is not None

        if exists:
            if is_full_sync:
                cur.execute("""
                    UPDATE sync_metadata
                    SET last_sync_at = %s, last_full_sync_at = %s,
                        records_synced = %s, status = 'completed'
                    WHERE entity_type = %s
                """, (now, now, records_synced, entity_type))
            elif records_synced > 0:
                cur.execute("""
                    UPDATE sync_metadata
                    SET last_sync_at = %s, records_synced = %s, status = 'completed'
                    WHERE entity_type = %s
                """, (now, records_synced, entity_type))
            else:
                logger.info(
                    "Delta sync para %s retornou 0 registros — cursor NÃO avançado "
                    "(protege contra gaps).", entity_type
                )
                cur.execute("""
                    UPDATE sync_metadata
                    SET records_synced = %s, status = 'completed'
                    WHERE entity_type = %s
                """, (records_synced, entity_type))
        else:
            cur.execute("""
                INSERT INTO sync_metadata
                    (entity_type, last_sync_at, last_full_sync_at, records_synced, status)
                VALUES (%s, %s, %s, %s, 'completed')
            """, (entity_type, now, now if is_full_sync else None, records_synced))

        pg.commit()
        cur.close()
        pg.close()
    except Exception as e:
        logger.error("update_sync_metadata PG error: %s", e)


def set_sync_status(entity_type: str, status: str):
    """Define o status da sincronização no PostgreSQL."""
    try:
        pg = _pg()
        cur = pg.cursor()
        cur.execute("""
            INSERT INTO sync_metadata (entity_type, status)
            VALUES (%s, %s)
            ON CONFLICT(entity_type) DO UPDATE SET status = excluded.status
        """, (entity_type, status))
        pg.commit()
        cur.close()
        pg.close()
    except Exception as e:
        logger.error("set_sync_status PG error: %s", e)


# ============================
# Operações de Pipelines
# ============================

def upsert_pipeline(pipeline: dict):
    """Insere ou atualiza um pipeline."""
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO pipelines 
            (id, name, sort, is_main, is_unsorted_on, is_archive, account_id, raw_json, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pipeline.get("id"),
            pipeline.get("name"),
            pipeline.get("sort"),
            1 if pipeline.get("is_main") else 0,
            1 if pipeline.get("is_unsorted_on") else 0,
            1 if pipeline.get("is_archive") else 0,
            pipeline.get("account_id"),
            json.dumps(pipeline, ensure_ascii=False),
            now
        ))


def upsert_pipeline_status(status: dict, pipeline_id: int):
    """Insere ou atualiza um status de pipeline."""
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO pipeline_statuses 
            (id, pipeline_id, name, sort, is_editable, color, type, account_id, raw_json, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            status.get("id"),
            pipeline_id,
            status.get("name"),
            status.get("sort"),
            1 if status.get("is_editable") else 0,
            status.get("color"),
            status.get("type"),
            status.get("account_id"),
            json.dumps(status, ensure_ascii=False),
            now
        ))


# ============================
# Operações de Leads
# ============================

def _lead_row(lead: dict, now: str) -> tuple:
    """Monta uma tupla para INSERT de lead (sem raw_json para economizar memória e disco)."""
    custom_fields = lead.get("custom_fields_values") or []
    tags = lead.get("_embedded", {}).get("tags") or []
    contacts = lead.get("_embedded", {}).get("contacts") or []
    return (
        lead.get("id"),
        lead.get("name"),
        lead.get("price"),
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
        1 if lead.get("is_deleted") else 0,
        lead.get("score"),
        lead.get("account_id"),
        lead.get("labor_cost"),
        1 if lead.get("is_price_modified_by_robot") else 0,
        json.dumps(custom_fields, ensure_ascii=False) if custom_fields else None,
        json.dumps(tags, ensure_ascii=False) if tags else None,
        json.dumps(contacts, ensure_ascii=False) if contacts else None,
        None,  # raw_json omitido para reduzir I/O e memória (não usado pelo dashboard)
        now,
    )


_LEAD_INSERT_SQL = """
INSERT OR REPLACE INTO leads 
(id, name, price, responsible_user_id, group_id, status_id, pipeline_id,
 loss_reason_id, source_id, created_by, updated_by, closed_at, created_at,
 updated_at, closest_task_at, is_deleted, score, account_id, labor_cost,
 is_price_modified, custom_fields_json, tags_json, contacts_json, raw_json, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_LEAD_CF_INSERT_SQL = """
INSERT OR REPLACE INTO lead_custom_field_values
(lead_id, field_id, field_name, field_code, field_type, values_json, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def _dual_write_pg_enabled() -> bool:
    v = os.getenv("KOMMO_DUAL_WRITE_PG", "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def _append_lead_postgres_cur(cur, lead: dict, now: str) -> None:
    """INSERT/UPDATE um lead + CF no PostgreSQL (usa cursor já aberto)."""
    from psycopg2.extras import Json

    cfs = lead.get("custom_fields_values") or []
    emb = lead.get("_embedded") or {}
    tags = emb.get("tags") or []
    emb_contacts = emb.get("contacts") or []
    cur.execute(
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
            Json(emb_contacts) if emb_contacts else None,
            None,
            now,
        ),
    )
    for cf in cfs:
        cur.execute(
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


def _mirror_leads_batch_postgres(leads: list[dict], now: str) -> None:
    if not _dual_write_pg_enabled() or not leads:
        return
    k = None
    c = None
    try:
        k = _pg()
        c = k.cursor()
        for lead in leads:
            _append_lead_postgres_cur(c, lead, now)
        k.commit()
    except Exception as e:
        logger.error("Espelho PostgreSQL (leads batch): %s", e)
        if k:
            try:
                k.rollback()
            except Exception:
                pass
    finally:
        if c:
            try:
                c.close()
            except Exception:
                pass
        if k:
            try:
                k.close()
            except Exception:
                pass


def upsert_leads_batch(leads: list[dict]):
    """Insere ou atualiza um lote de leads com executemany (menos locks, menos I/O). Sem raw_json."""
    if not leads:
        return
    now = datetime.utcnow().isoformat()
    rows = [_lead_row(lead, now) for lead in leads]
    cf_rows = []
    for lead in leads:
        lid = lead.get("id")
        for cf in lead.get("custom_fields_values") or []:
            cf_rows.append((
                lid,
                cf.get("field_id"),
                cf.get("field_name"),
                cf.get("field_code"),
                cf.get("field_type"),
                json.dumps(cf.get("values", []), ensure_ascii=False),
                now,
            ))
    with get_db() as conn:
        conn.executemany(_LEAD_INSERT_SQL, rows)
        if cf_rows:
            conn.executemany(_LEAD_CF_INSERT_SQL, cf_rows)
    _mirror_leads_batch_postgres(leads, now)


def upsert_lead_postgres(lead: dict) -> None:
    """Grava/atualiza um lead e lead_custom_field_values no PostgreSQL kommo_sync (espelho do sync)."""
    now = datetime.utcnow().isoformat()
    k = _pg()
    c = k.cursor()
    try:
        _append_lead_postgres_cur(c, lead, now)
        k.commit()
    finally:
        c.close()
        k.close()


# ============================
# Operações de Contatos
# ============================

_CONTACT_INSERT_SQL = """
INSERT OR REPLACE INTO contacts 
(id, name, first_name, last_name, responsible_user_id, group_id,
 created_by, updated_by, created_at, updated_at, closest_task_at,
 is_deleted, account_id, custom_fields_json, tags_json, raw_json, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_CONTACT_CF_INSERT_SQL = """
INSERT OR REPLACE INTO contact_custom_field_values
(contact_id, field_id, field_name, field_code, field_type, values_json, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def _append_contact_postgres_cur(cur, contact: dict, now: str) -> None:
    from psycopg2.extras import Json

    custom_fields = contact.get("custom_fields_values") or []
    tags = contact.get("_embedded", {}).get("tags") or []
    cid = contact.get("id")
    cur.execute(
        """
        INSERT INTO contacts (
            id, name, first_name, last_name, responsible_user_id, group_id,
            created_by, updated_by, created_at, updated_at, closest_task_at,
            is_deleted, account_id, custom_fields_json, tags_json, raw_json, synced_at
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
            responsible_user_id = EXCLUDED.responsible_user_id, group_id = EXCLUDED.group_id,
            created_by = EXCLUDED.created_by, updated_by = EXCLUDED.updated_by,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            closest_task_at = EXCLUDED.closest_task_at, is_deleted = EXCLUDED.is_deleted,
            account_id = EXCLUDED.account_id, custom_fields_json = EXCLUDED.custom_fields_json,
            tags_json = EXCLUDED.tags_json, synced_at = EXCLUDED.synced_at
        """,
        (
            cid,
            contact.get("name"),
            contact.get("first_name"),
            contact.get("last_name"),
            contact.get("responsible_user_id"),
            contact.get("group_id"),
            contact.get("created_by"),
            contact.get("updated_by"),
            contact.get("created_at"),
            contact.get("updated_at"),
            contact.get("closest_task_at"),
            bool(contact.get("is_deleted")),
            contact.get("account_id"),
            Json(custom_fields) if custom_fields else None,
            Json(tags) if tags else None,
            None,
            now,
        ),
    )
    for cf in custom_fields:
        cur.execute(
            """
            INSERT INTO contact_custom_field_values
            (contact_id, field_id, field_name, field_code, field_type, values_json, synced_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (contact_id, field_id) DO UPDATE SET
                field_name = EXCLUDED.field_name, field_code = EXCLUDED.field_code,
                field_type = EXCLUDED.field_type, values_json = EXCLUDED.values_json,
                synced_at = EXCLUDED.synced_at
            """,
            (
                cid,
                cf.get("field_id"),
                cf.get("field_name"),
                cf.get("field_code"),
                cf.get("field_type"),
                Json(cf.get("values") or []),
                now,
            ),
        )


def _mirror_contacts_batch_postgres(contacts: list[dict], now: str) -> None:
    if not _dual_write_pg_enabled() or not contacts:
        return
    k = None
    c = None
    try:
        k = _pg()
        c = k.cursor()
        for contact in contacts:
            _append_contact_postgres_cur(c, contact, now)
        k.commit()
    except Exception as e:
        logger.error("Espelho PostgreSQL (contacts batch): %s", e)
        if k:
            try:
                k.rollback()
            except Exception:
                pass
    finally:
        if c:
            try:
                c.close()
            except Exception:
                pass
        if k:
            try:
                k.close()
            except Exception:
                pass


def upsert_contacts_batch(contacts: list[dict]):
    """Insere ou atualiza contatos com executemany; sem raw_json (menos I/O)."""
    if not contacts:
        return
    now = datetime.utcnow().isoformat()
    rows = []
    cf_rows = []
    for contact in contacts:
        custom_fields = contact.get("custom_fields_values") or []
        tags = contact.get("_embedded", {}).get("tags") or []
        cid = contact.get("id")
        rows.append((
            cid,
            contact.get("name"),
            contact.get("first_name"),
            contact.get("last_name"),
            contact.get("responsible_user_id"),
            contact.get("group_id"),
            contact.get("created_by"),
            contact.get("updated_by"),
            contact.get("created_at"),
            contact.get("updated_at"),
            contact.get("closest_task_at"),
            1 if contact.get("is_deleted") else 0,
            contact.get("account_id"),
            json.dumps(custom_fields, ensure_ascii=False) if custom_fields else None,
            json.dumps(tags, ensure_ascii=False) if tags else None,
            None,
            now,
        ))
        for cf in custom_fields:
            cf_rows.append((
                cid,
                cf.get("field_id"),
                cf.get("field_name"),
                cf.get("field_code"),
                cf.get("field_type"),
                json.dumps(cf.get("values", []), ensure_ascii=False),
                now,
            ))
    with get_db() as conn:
        conn.executemany(_CONTACT_INSERT_SQL, rows)
        if cf_rows:
            conn.executemany(_CONTACT_CF_INSERT_SQL, cf_rows)
    _mirror_contacts_batch_postgres(contacts, now)


# ============================
# Operações de Custom Fields
# ============================

def upsert_custom_fields_batch(fields: list[dict], entity_type: str):
    """Insere ou atualiza definições de campos customizados."""
    now = datetime.utcnow().isoformat()
    with get_db() as conn:
        for field in fields:
            enums = field.get("enums") or []
            conn.execute("""
                INSERT OR REPLACE INTO custom_fields
                (id, entity_type, name, type, sort, code, is_api_only, group_id,
                 required_statuses, enums_json, raw_json, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                field.get("id"),
                entity_type,
                field.get("name"),
                field.get("type"),
                field.get("sort"),
                field.get("code"),
                1 if field.get("is_api_only") else 0,
                field.get("group_id"),
                json.dumps(field.get("required_statuses") or [], ensure_ascii=False),
                json.dumps(enums, ensure_ascii=False) if enums else None,
                json.dumps(field, ensure_ascii=False),
                now
            ))


# ============================
# Queries Úteis
# ============================

def get_leads_count() -> int:
    """Retorna o total de leads no banco."""
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM leads").fetchone()
        return row["cnt"] if row else 0


def get_contacts_count() -> int:
    """Retorna o total de contatos no banco."""
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM contacts").fetchone()
        return row["cnt"] if row else 0


def get_sync_summary() -> list[dict]:
    """Retorna resumo de todas as sincronizações (do PostgreSQL)."""
    try:
        pg = _pg()
        cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM sync_metadata ORDER BY entity_type")
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        pg.close()
        return rows
    except Exception as e:
        logger.error("get_sync_summary PG error: %s", e)
        return []
