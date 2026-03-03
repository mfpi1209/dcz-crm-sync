"""
Migração SQLite -> PostgreSQL.
Cria o schema no PostgreSQL e transfere todos os dados do SQLite local.

Uso: python migrate_to_postgres.py
"""

import sqlite3
import psycopg2
import psycopg2.extras
import sys
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# === Configurações ===
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

SQLITE_PATH = os.getenv("KOMMO_DB_PATH", os.path.join(os.path.dirname(__file__), "kommo_sync.db"))

PG_CONFIG = {
    "host": os.getenv("KOMMO_PG_HOST", "31.97.91.47"),
    "port": int(os.getenv("KOMMO_PG_PORT", "5432")),
    "dbname": os.getenv("KOMMO_PG_DB", "kommo_sync"),
    "user": os.getenv("KOMMO_PG_USER", "adm_eduit"),
    "password": os.getenv("KOMMO_PG_PASS", "IaDm24Sx3HxrYoqT"),
}

# === Schema PostgreSQL ===
PG_SCHEMA = """
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
    is_main         BOOLEAN DEFAULT FALSE,
    is_unsorted_on  BOOLEAN DEFAULT FALSE,
    is_archive      BOOLEAN DEFAULT FALSE,
    account_id      INTEGER,
    raw_json        JSONB,
    synced_at       TEXT
);

-- Stages (status dos pipelines)
CREATE TABLE IF NOT EXISTS pipeline_statuses (
    id              INTEGER PRIMARY KEY,
    pipeline_id     INTEGER REFERENCES pipelines(id),
    name            TEXT,
    sort            INTEGER,
    is_editable     BOOLEAN DEFAULT TRUE,
    color           TEXT,
    type            INTEGER DEFAULT 0,
    account_id      INTEGER,
    raw_json        JSONB,
    synced_at       TEXT
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
    is_deleted          BOOLEAN DEFAULT FALSE,
    score               REAL,
    account_id          INTEGER,
    labor_cost          INTEGER,
    is_price_modified   BOOLEAN DEFAULT FALSE,
    custom_fields_json  JSONB,
    tags_json           JSONB,
    contacts_json       JSONB,
    raw_json            JSONB,
    synced_at           TEXT
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
    is_deleted          BOOLEAN DEFAULT FALSE,
    account_id          INTEGER,
    custom_fields_json  JSONB,
    tags_json           JSONB,
    raw_json            JSONB,
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
    is_api_only     BOOLEAN DEFAULT FALSE,
    group_id        TEXT,
    required_statuses TEXT,
    enums_json      JSONB,
    raw_json        JSONB,
    synced_at       TEXT,
    PRIMARY KEY (id, entity_type)
);

-- Valores de campos customizados de leads
CREATE TABLE IF NOT EXISTS lead_custom_field_values (
    lead_id         INTEGER REFERENCES leads(id),
    field_id        INTEGER,
    field_name      TEXT,
    field_code      TEXT,
    field_type      TEXT,
    values_json     JSONB,
    synced_at       TEXT,
    PRIMARY KEY (lead_id, field_id)
);

-- Valores de campos customizados de contatos
CREATE TABLE IF NOT EXISTS contact_custom_field_values (
    contact_id      INTEGER REFERENCES contacts(id),
    field_id        INTEGER,
    field_name      TEXT,
    field_code      TEXT,
    field_type      TEXT,
    values_json     JSONB,
    synced_at       TEXT,
    PRIMARY KEY (contact_id, field_id)
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_leads_pipeline ON leads(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status_id);
CREATE INDEX IF NOT EXISTS idx_leads_updated ON leads(updated_at);
CREATE INDEX IF NOT EXISTS idx_leads_responsible ON leads(responsible_user_id);
CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at);

CREATE INDEX IF NOT EXISTS idx_contacts_updated ON contacts(updated_at);
CREATE INDEX IF NOT EXISTS idx_contacts_responsible ON contacts(responsible_user_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_statuses_pipeline ON pipeline_statuses(pipeline_id);

CREATE INDEX IF NOT EXISTS idx_lead_cf_field ON lead_custom_field_values(field_id);
CREATE INDEX IF NOT EXISTS idx_contact_cf_field ON contact_custom_field_values(field_id);
"""

# Mapeamento: tabela -> (colunas SQLite, colunas PG, colunas que são JSON no PG, colunas boolean no PG)
TABLE_MAP = {
    "sync_metadata": {
        "columns": ["entity_type", "last_sync_at", "last_full_sync_at", "records_synced", "status"],
        "json_cols": [],
        "bool_cols": [],
    },
    "pipelines": {
        "columns": ["id", "name", "sort", "is_main", "is_unsorted_on", "is_archive", "account_id", "raw_json", "synced_at"],
        "json_cols": ["raw_json"],
        "bool_cols": ["is_main", "is_unsorted_on", "is_archive"],
    },
    "pipeline_statuses": {
        "columns": ["id", "pipeline_id", "name", "sort", "is_editable", "color", "type", "account_id", "raw_json", "synced_at"],
        "json_cols": ["raw_json"],
        "bool_cols": ["is_editable"],
    },
    "leads": {
        "columns": ["id", "name", "price", "responsible_user_id", "group_id", "status_id",
                     "pipeline_id", "loss_reason_id", "source_id", "created_by", "updated_by",
                     "closed_at", "created_at", "updated_at", "closest_task_at", "is_deleted",
                     "score", "account_id", "labor_cost", "is_price_modified",
                     "custom_fields_json", "tags_json", "contacts_json", "raw_json", "synced_at"],
        "json_cols": ["custom_fields_json", "tags_json", "contacts_json", "raw_json"],
        "bool_cols": ["is_deleted", "is_price_modified"],
    },
    "contacts": {
        "columns": ["id", "name", "first_name", "last_name", "responsible_user_id", "group_id",
                     "created_by", "updated_by", "created_at", "updated_at", "closest_task_at",
                     "is_deleted", "account_id", "custom_fields_json", "tags_json", "raw_json", "synced_at"],
        "json_cols": ["custom_fields_json", "tags_json", "raw_json"],
        "bool_cols": ["is_deleted"],
    },
    "custom_fields": {
        "columns": ["id", "entity_type", "name", "type", "sort", "code", "is_api_only",
                     "group_id", "required_statuses", "enums_json", "raw_json", "synced_at"],
        "json_cols": ["enums_json", "raw_json"],
        "bool_cols": ["is_api_only"],
    },
    "lead_custom_field_values": {
        "columns": ["lead_id", "field_id", "field_name", "field_code", "field_type", "values_json", "synced_at"],
        "json_cols": ["values_json"],
        "bool_cols": [],
    },
    "contact_custom_field_values": {
        "columns": ["contact_id", "field_id", "field_name", "field_code", "field_type", "values_json", "synced_at"],
        "json_cols": ["values_json"],
        "bool_cols": [],
    },
}

# Ordem de migração (respeita foreign keys)
MIGRATION_ORDER = [
    "sync_metadata",
    "pipelines",
    "pipeline_statuses",
    "custom_fields",
    "leads",
    "contacts",
    "lead_custom_field_values",
    "contact_custom_field_values",
]

# Light: sem cf_values (sincronizadas separadamente por lead_id - mais rápido)
MIGRATION_ORDER_LIGHT = [
    "pipelines",
    "pipeline_statuses",
    "custom_fields",
    "leads",
    "contacts",
    "sync_metadata",
]

BATCH_SIZE = 5000


def convert_row(row, columns, json_cols, bool_cols):
    """Converte uma row SQLite para formato PostgreSQL."""
    import json as json_mod
    result = []
    for i, col in enumerate(columns):
        val = row[i]
        if col in json_cols:
            if val is not None and isinstance(val, str):
                try:
                    val = json_mod.loads(val)
                except (json_mod.JSONDecodeError, ValueError):
                    pass
            if val is not None:
                val = psycopg2.extras.Json(val)
        elif col in bool_cols:
            val = bool(val) if val is not None else None
        result.append(val)
    return tuple(result)


def get_pg_last_sync(pg_conn, since_override=None):
    """
    Determina desde quando migrar.
    Se since_override for passado, usa esse valor (vem do sync que acabou de rodar).
    Senão, usa o last_full_sync_at como fallback seguro.
    """
    if since_override:
        logger.info("Usando --since fornecido: %s", since_override[:19])
        return since_override
    try:
        cur = pg_conn.cursor()
        cur.execute("""
            SELECT last_full_sync_at FROM sync_metadata
            WHERE entity_type = 'leads' AND last_full_sync_at IS NOT NULL
            LIMIT 1
        """)
        row = cur.fetchone()
        if row and row[0]:
            logger.info("PG referência (full sync): %s", row[0][:19])
            return row[0]
        return None
    except Exception:
        return None


def migrate_table(sqlite_conn, pg_conn, table_name, config, since=None):
    """
    Migra uma tabela do SQLite para PostgreSQL.
    Se 'since' for informado, migra apenas registros com synced_at >= since (delta).
    """
    columns = config["columns"]
    json_cols = config["json_cols"]
    bool_cols = config["bool_cols"]

    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    # Conflict handling (UPSERT)
    if table_name == "custom_fields":
        conflict_key = "id, entity_type"
    elif table_name in ("lead_custom_field_values",):
        conflict_key = "lead_id, field_id"
    elif table_name in ("contact_custom_field_values",):
        conflict_key = "contact_id, field_id"
    elif table_name == "sync_metadata":
        conflict_key = "entity_type"
    else:
        conflict_key = "id"

    update_cols = [c for c in columns if c not in conflict_key.replace(" ", "").split(",")]
    update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    insert_sql = f"""
        INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})
        ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}
    """

    sqlite_cur = sqlite_conn.cursor()

    # Delta: filtrar por synced_at se a tabela tem essa coluna e 'since' foi informado
    has_synced_at = "synced_at" in columns
    use_delta = since and has_synced_at and table_name not in ("sync_metadata",)

    if use_delta:
        where = f" WHERE synced_at >= '{since}'"

        # Para tabelas grandes (cf_values), pular COUNT (full scan lento sem índice)
        skip_count = table_name in ("lead_custom_field_values", "contact_custom_field_values")

        if skip_count:
            logger.info("  %s: migrando delta desde %s (sem count)...", table_name, since[:19])
            total = None
        else:
            sqlite_cur.execute(f"SELECT COUNT(*) FROM {table_name}{where}")
            total = sqlite_cur.fetchone()[0]
            if total == 0:
                logger.info("  %s: sem alteracoes desde %s, pulando.", table_name, since[:19])
                return 0
            logger.info("  %s: %d registros alterados (delta desde %s)...", table_name, total, since[:19])

        sqlite_cur.execute(f"SELECT {col_list} FROM {table_name}{where}")
    else:
        sqlite_cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total = sqlite_cur.fetchone()[0]

        if total == 0:
            logger.info("  %s: vazia, pulando.", table_name)
            return 0

        logger.info("  %s: %d registros (full)...", table_name, total)
        sqlite_cur.execute(f"SELECT {col_list} FROM {table_name}")

    pg_cur = pg_conn.cursor()
    migrated = 0

    while True:
        rows = sqlite_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        converted = [convert_row(row, columns, json_cols, bool_cols) for row in rows]
        psycopg2.extras.execute_batch(pg_cur, insert_sql, converted, page_size=1000)
        pg_conn.commit()

        migrated += len(rows)
        if total:
            pct = (migrated / total) * 100
            logger.info("  %s: %d/%d (%.1f%%)", table_name, migrated, total, pct)
        else:
            logger.info("  %s: %d registros migrados...", table_name, migrated)

    return migrated


def main(light=False, since=None):
    start = time.time()

    mode_label = "LIGHT" if light else "FULL"
    logger.info("=" * 60)
    logger.info("MIGRAÇÃO SQLite -> PostgreSQL (%s)", mode_label)
    logger.info("=" * 60)

    # Conectar SQLite
    logger.info("Conectando ao SQLite: %s", SQLITE_PATH)
    sqlite_conn = sqlite3.connect(SQLITE_PATH)

    # Conectar PostgreSQL
    logger.info("Conectando ao PostgreSQL: %s@%s:%s/%s",
                PG_CONFIG["user"], PG_CONFIG["host"], PG_CONFIG["port"], PG_CONFIG["dbname"])
    pg_conn = psycopg2.connect(**PG_CONFIG)

    # Criar schema
    logger.info("Criando schema no PostgreSQL...")
    pg_cur = pg_conn.cursor()
    pg_cur.execute(PG_SCHEMA)
    pg_conn.commit()
    logger.info("Schema criado com sucesso.")

    # Detectar delta
    pg_last_sync = get_pg_last_sync(pg_conn, since_override=since)
    if pg_last_sync:
        logger.info("Modo DELTA: migrando apenas registros alterados desde %s", pg_last_sync[:19])
    else:
        logger.info("Modo FULL: primeira migração ou sem dados no PostgreSQL")

    # Migrar cada tabela
    order = MIGRATION_ORDER_LIGHT if light else MIGRATION_ORDER
    if light:
        logger.info("Modo LIGHT: pulando lead/contact_custom_field_values (JSON já está nos leads)")

    stats = {}
    for table_name in order:
        config = TABLE_MAP[table_name]
        try:
            count = migrate_table(sqlite_conn, pg_conn, table_name, config, since=pg_last_sync)
            stats[table_name] = count
        except Exception as e:
            logger.error("ERRO migrando %s: %s", table_name, e)
            pg_conn.rollback()
            stats[table_name] = f"ERRO: {e}"

    # Se light + since: sincronizar cf_values apenas dos leads/contacts recentes (por ID, não por synced_at)
    if light and since:
        try:
            sqlite_cur = sqlite_conn.cursor()

            # Leads: pegar IDs recentes
            sqlite_cur.execute(f"SELECT id FROM leads WHERE synced_at >= '{since}'")
            lead_ids = [r[0] for r in sqlite_cur.fetchall()]

            if lead_ids:
                logger.info("  Sincronizando cf_values para %d leads recentes...", len(lead_ids))
                pg_cur = pg_conn.cursor()
                batch_ids = lead_ids[:500]  # Limitar para não sobrecarregar
                placeholders_sq = ",".join(["?" for _ in batch_ids])
                sqlite_cur.execute(
                    f"SELECT lead_id, field_id, field_name, field_code, field_type, values_json, synced_at "
                    f"FROM lead_custom_field_values WHERE lead_id IN ({placeholders_sq})",
                    batch_ids,
                )
                rows = sqlite_cur.fetchall()
                if rows:
                    cf_config = TABLE_MAP["lead_custom_field_values"]
                    converted = [convert_row(r, cf_config["columns"], cf_config["json_cols"], cf_config["bool_cols"]) for r in rows]
                    insert_sql = """
                        INSERT INTO lead_custom_field_values (lead_id, field_id, field_name, field_code, field_type, values_json, synced_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (lead_id, field_id) DO UPDATE SET
                            field_name = EXCLUDED.field_name, field_code = EXCLUDED.field_code,
                            field_type = EXCLUDED.field_type, values_json = EXCLUDED.values_json,
                            synced_at = EXCLUDED.synced_at
                    """
                    psycopg2.extras.execute_batch(pg_cur, insert_sql, converted, page_size=1000)
                    pg_conn.commit()
                    logger.info("  lead_custom_field_values: %d registros migrados", len(rows))
                    stats["lead_custom_field_values"] = len(rows)

            # Contacts: pegar IDs recentes
            sqlite_cur.execute(f"SELECT id FROM contacts WHERE synced_at >= '{since}'")
            contact_ids = [r[0] for r in sqlite_cur.fetchall()]

            if contact_ids:
                logger.info("  Sincronizando cf_values para %d contatos recentes...", len(contact_ids))
                pg_cur = pg_conn.cursor()
                batch_ids_c = contact_ids[:500]
                placeholders_sq_c = ",".join(["?" for _ in batch_ids_c])
                sqlite_cur.execute(
                    f"SELECT contact_id, field_id, field_name, field_code, field_type, values_json, synced_at "
                    f"FROM contact_custom_field_values WHERE contact_id IN ({placeholders_sq_c})",
                    batch_ids_c,
                )
                rows_c = sqlite_cur.fetchall()
                if rows_c:
                    cf_config_c = TABLE_MAP["contact_custom_field_values"]
                    converted_c = [convert_row(r, cf_config_c["columns"], cf_config_c["json_cols"], cf_config_c["bool_cols"]) for r in rows_c]
                    insert_sql_c = """
                        INSERT INTO contact_custom_field_values (contact_id, field_id, field_name, field_code, field_type, values_json, synced_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (contact_id, field_id) DO UPDATE SET
                            field_name = EXCLUDED.field_name, field_code = EXCLUDED.field_code,
                            field_type = EXCLUDED.field_type, values_json = EXCLUDED.values_json,
                            synced_at = EXCLUDED.synced_at
                    """
                    psycopg2.extras.execute_batch(pg_cur, insert_sql_c, converted_c, page_size=1000)
                    pg_conn.commit()
                    logger.info("  contact_custom_field_values: %d registros migrados", len(rows_c))
                    stats["contact_custom_field_values"] = len(rows_c)

        except Exception as e:
            logger.error("Erro ao migrar cf_values por ID: %s", e)

    # Fechar conexões
    sqlite_conn.close()
    pg_conn.close()

    # Relatório
    elapsed = time.time() - start
    logger.info("")
    logger.info("=" * 60)
    logger.info("RELATÓRIO DA MIGRAÇÃO")
    logger.info("=" * 60)
    for table, count in stats.items():
        logger.info("  %-35s %s registros", table, count)
    logger.info("")
    logger.info("  Tempo total: %.1f segundos (%.1f minutos)", elapsed, elapsed / 60)
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--light", action="store_true", help="Pular tabelas pesadas de custom field values")
    ap.add_argument("--since", type=str, default=None, help="Migrar apenas registros com synced_at >= este valor ISO")
    args = ap.parse_args()
    main(light=args.light, since=args.since)
