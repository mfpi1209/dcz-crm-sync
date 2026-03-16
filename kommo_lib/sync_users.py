"""
Sincronização de usuários Kommo -> PostgreSQL (kommo_sync).

Busca todos os usuários via GET /api/v4/users e salva no PostgreSQL.
"""

import logging
from datetime import datetime

import psycopg2
import psycopg2.extras

from database import _pg, update_sync_metadata, set_sync_status

logger = logging.getLogger(__name__)

_USERS_DDL = """
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    email       TEXT,
    lang        TEXT,
    rights_json JSONB,
    synced_at   TEXT
);
"""


def _ensure_users_table():
    try:
        pg = _pg()
        cur = pg.cursor()
        cur.execute(_USERS_DDL)
        pg.commit()
        cur.close()
        pg.close()
    except Exception as e:
        logger.error("Erro ao criar tabela users: %s", e)


def sync_users(client) -> dict:
    """Fetch all users from Kommo API v4 and upsert into PostgreSQL."""
    _ensure_users_table()
    set_sync_status("users", "syncing")

    logger.info("Iniciando sincronização de usuários...")

    try:
        all_users = client.get_all_pages("api/v4/users", embedded_key="users")
    except Exception as e:
        logger.error("Falha ao buscar usuários: %s", e)
        set_sync_status("users", "error")
        return {"fetched": 0, "error": str(e)}

    if not all_users:
        logger.warning("Nenhum usuário retornado pela API")
        set_sync_status("users", "completed")
        return {"fetched": 0, "upserted": 0}

    logger.info("Recebidos %d usuários da API", len(all_users))

    now = datetime.utcnow().isoformat()
    upserted = 0

    try:
        pg = _pg()
        cur = pg.cursor()

        for u in all_users:
            cur.execute("""
                INSERT INTO users (id, name, email, lang, rights_json, synced_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    lang = EXCLUDED.lang,
                    rights_json = EXCLUDED.rights_json,
                    synced_at = EXCLUDED.synced_at
            """, (
                u.get("id"),
                u.get("name", ""),
                u.get("email", ""),
                u.get("lang", ""),
                psycopg2.extras.Json(u.get("rights") or {}),
                now,
            ))
            upserted += 1

        pg.commit()
        cur.close()
        pg.close()
    except Exception as e:
        logger.error("Erro ao gravar usuários no PG: %s", e)
        set_sync_status("users", "error")
        return {"fetched": len(all_users), "upserted": upserted, "error": str(e)}

    update_sync_metadata("users", upserted, is_full_sync=True)
    logger.info("Sincronização de usuários concluída: %d upserted", upserted)
    return {"fetched": len(all_users), "upserted": upserted}
