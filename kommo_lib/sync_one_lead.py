#!/usr/bin/env python3
"""
Busca UM lead pelo ID na API Kommo e grava em SQLite + PostgreSQL.
Uso: python sync_one_lead.py 20796123
     python sync_one_lead.py 20796123 --no-pg   # só SQLite local
"""
import argparse
import logging
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

from api_client import KommoAPIClient
from config import DB_PATH
from database import init_database, upsert_leads_batch


def _parse_lead(data: dict) -> dict | None:
    if not data:
        return None
    if "_embedded" in data and data["_embedded"].get("leads"):
        return data["_embedded"]["leads"][0]
    if isinstance(data.get("id"), int):
        return data
    return None


def _upsert_sql(table_name: str, config: dict) -> str:
    columns = config["columns"]
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    if table_name == "custom_fields":
        conflict_key = "id, entity_type"
    elif table_name == "lead_custom_field_values":
        conflict_key = "lead_id, field_id"
    else:
        conflict_key = "id"
    update_cols = [c for c in columns if c not in conflict_key.replace(" ", "").split(",")]
    update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    return f"""
        INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})
        ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}
    """


def push_lead_to_postgres(lead_id: int) -> None:
    try:
        import psycopg2
        from migrate_to_postgres import TABLE_MAP, convert_row, PG_CONFIG
    except ImportError as e:
        logger.error("Instale psycopg2 ou use --no-pg: %s", e)
        raise

    sqlite_conn = sqlite3.connect(DB_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    pg = psycopg2.connect(**PG_CONFIG)
    try:
        cfg = TABLE_MAP["leads"]
        cols = ", ".join(cfg["columns"])
        cur = sqlite_conn.cursor()
        cur.execute(f"SELECT {cols} FROM leads WHERE id = ?", (lead_id,))
        row = cur.fetchone()
        if not row:
            logger.error("Lead %s não está no SQLite após gravar.", lead_id)
            return
        tup = tuple(row[c] for c in cfg["columns"])
        converted = convert_row(tup, cfg["columns"], cfg["json_cols"], cfg["bool_cols"])
        sql = _upsert_sql("leads", cfg)
        pg_cur = pg.cursor()
        pg_cur.execute(sql, converted)

        cfg_cf = TABLE_MAP["lead_custom_field_values"]
        cols_cf = ", ".join(cfg_cf["columns"])
        cur.execute(
            f"SELECT {cols_cf} FROM lead_custom_field_values WHERE lead_id = ?",
            (lead_id,),
        )
        cf_rows = cur.fetchall()
        if cf_rows:
            sql_cf = _upsert_sql("lead_custom_field_values", cfg_cf)
            for r in cf_rows:
                tup = tuple(r[c] for c in cfg_cf["columns"])
                conv = convert_row(tup, cfg_cf["columns"], cfg_cf["json_cols"], cfg_cf["bool_cols"])
                pg_cur.execute(sql_cf, conv)
        pg.commit()
        logger.info("PostgreSQL: lead %s + %d campo(s) custom atualizados.", lead_id, len(cf_rows))
    finally:
        sqlite_conn.close()
        pg.close()


def main():
    p = argparse.ArgumentParser(description="Sincronizar um lead pelo ID (API Kommo → SQLite → PG)")
    p.add_argument("lead_id", type=int, nargs="?", default=20796123)
    p.add_argument("--no-pg", action="store_true", help="Não enviar para PostgreSQL")
    args = p.parse_args()

    init_database()
    client = KommoAPIClient()
    data = client.get(f"leads/{args.lead_id}", params={"with": "contacts"})
    lead = _parse_lead(data)

    if not lead:
        logger.error(
            "Lead %s não retornado pela API (404, token ou ID inválido). Resposta: %s",
            args.lead_id,
            str(data)[:500] if data else "vazio",
        )
        sys.exit(1)

    # Garantir formato esperado por upsert_leads_batch (lista de custom fields no root)
    if "custom_fields_values" not in lead and lead.get("custom_fields_values") is None:
        lead["custom_fields_values"] = lead.get("custom_fields_values") or []

    upsert_leads_batch([lead])
    logger.info(
        "SQLite: lead %s gravado — nome=%s pipeline_id=%s status_id=%s",
        lead.get("id"),
        lead.get("name"),
        lead.get("pipeline_id"),
        lead.get("status_id"),
    )

    # RGM no JSON
    cfs = lead.get("custom_fields_values") or []
    rgm = next(
        (
            (cf.get("values") or [{}])[0].get("value")
            for cf in cfs
            if str(cf.get("field_name", "")).lower() == "rgm"
        ),
        None,
    )
    logger.info("RGM no payload da API: %s", rgm)

    if not args.no_pg:
        try:
            push_lead_to_postgres(args.lead_id)
        except Exception as e:
            logger.exception("Falha ao enviar ao PostgreSQL: %s", e)
            logger.info("Dados ficaram no SQLite; rode migrate_to_postgres depois se quiser.")
            sys.exit(2)

    logger.info("Concluído.")
    sys.exit(0)


if __name__ == "__main__":
    main()
