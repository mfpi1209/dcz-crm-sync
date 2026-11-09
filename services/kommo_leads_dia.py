"""Leads do Kommo por dia e consultor, para a conversão do ranking antes do Bwipo.

O ranking do Dashboard Bwipo divide as matrículas pelos leads criados no período.
Até 30/09/2026 os leads estão no Kommo (Funil de vendas); de 01/10/2026 em diante,
no Pipeline Principal do Bwipo. Mesmo corte do Repasse.

Consultor do lead = o primeiro consultor que o recebeu (lead_responsible_history).
O lead entra sem dono ou num administrador do Kommo (Admin Sistema, Isabela, TI…),
que fazem a distribuição; conta para o primeiro dono que não é administrador.
Sem troca registrada, vale o responsável atual. Sem consultor nenhum, Admin Sistema.

A conta pesa (cerca de 280 mil leads), então roda num job e grava a tabela pronta
`kommo_leads_dia` no dcz_sync. O request só soma linhas dessa tabela.
"""
from __future__ import annotations

import logging
import threading
from datetime import date, datetime, timedelta, timezone

from db import get_conn

logger = logging.getLogger(__name__)

# Leads criados antes deste dia (BRT) vêm do Kommo; deste dia em diante, do Bwipo.
CORTE_KOMMO_BWIPO = date(2026, 10, 1)
FUNIL_KOMMO = "Funil de vendas"

_lock = threading.Lock()

_DDL = """
CREATE TABLE IF NOT EXISTS kommo_leads_dia (
    dia DATE NOT NULL,
    kommo_user_id BIGINT NOT NULL,
    n INTEGER NOT NULL,
    built_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dia, kommo_user_id)
)
"""

_SQL_KOMMO = """
WITH l AS (
    SELECT l.id, l.created_at, l.responsible_user_id,
           (to_timestamp(l.created_at) AT TIME ZONE 'America/Sao_Paulo')::date AS dia
      FROM leads l
      JOIN pipelines p ON p.id = l.pipeline_id
     WHERE NOT COALESCE(l.is_deleted, FALSE)
       AND p.name = %(funil)s
       AND (to_timestamp(l.created_at) AT TIME ZONE 'America/Sao_Paulo')::date < %(corte)s
)
SELECT l.dia,
       COALESCE(
           -- 0 = lead sem responsável; administradores do Kommo (Admin Sistema, Isabela…)
           -- são a fila de distribuição. Vale o primeiro consultor que recebeu o lead.
           CASE WHEN NOT (h.from_user_id = ANY(%(fila)s)) THEN h.from_user_id
                WHEN h.from_user_id IS NOT NULL THEN
                    (SELECT h2.to_user_id
                       FROM lead_responsible_history h2
                      WHERE h2.lead_id = l.id
                        AND h2.changed_at >= h.changed_at
                        AND NOT (h2.to_user_id = ANY(%(fila)s))
                      ORDER BY h2.changed_at
                      LIMIT 1)
           END,
           CASE WHEN NOT (l.responsible_user_id = ANY(%(fila)s)) THEN l.responsible_user_id END,
           %(admin)s
       ) AS uid,
       COUNT(*)
  FROM l
  LEFT JOIN LATERAL (
        SELECT h.from_user_id, h.changed_at
          FROM lead_responsible_history h
         WHERE h.lead_id = l.id
           AND h.changed_at >= to_timestamp(l.created_at)
         ORDER BY h.changed_at
         LIMIT 1
  ) h ON TRUE
 GROUP BY 1, 2
"""


def _admin_uid() -> int:
    from routes.comercial_rgm import _admin_sistema_uid

    return int(_admin_sistema_uid())


def ensure_table() -> None:
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(_DDL)
    finally:
        conn.close()


def rebuild() -> int:
    """Remonta `kommo_leads_dia` a partir do kommo_sync. Devolve o número de linhas."""
    from routes.bwipo_sync import _pg_kommo

    if not _lock.acquire(blocking=False):
        logger.info("kommo_leads_dia: remontagem já em andamento")
        return -1
    try:
        t0 = datetime.now(timezone.utc)
        admin = _admin_uid()
        kconn = _pg_kommo()
        try:
            with kconn.cursor() as kcur:
                kcur.execute("SELECT id FROM users WHERE COALESCE((rights_json->>'is_admin')::boolean, FALSE)")
                fila = sorted({0, admin} | {int(r[0]) for r in kcur.fetchall()})
                kcur.execute(
                    _SQL_KOMMO,
                    {"funil": FUNIL_KOMMO, "corte": CORTE_KOMMO_BWIPO, "admin": admin, "fila": fila},
                )
                rows = [(d, int(u), int(n)) for d, u, n in kcur.fetchall() if d and u is not None]
        finally:
            kconn.close()
        if not rows:
            logger.warning("kommo_leads_dia: Kommo sem leads no %s; tabela mantida", FUNIL_KOMMO)
            return 0
        conn = get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(_DDL)
                cur.execute("DELETE FROM kommo_leads_dia")
                from psycopg2.extras import execute_values

                execute_values(
                    cur,
                    "INSERT INTO kommo_leads_dia (dia, kommo_user_id, n) VALUES %s",
                    rows,
                    page_size=2000,
                )
        finally:
            conn.close()
        logger.info(
            "kommo_leads_dia: %d linhas, %d leads em %.1fs",
            len(rows), sum(r[2] for r in rows), (datetime.now(timezone.utc) - t0).total_seconds(),
        )
        return len(rows)
    except Exception:
        logger.exception("kommo_leads_dia: falha ao remontar")
        return -1
    finally:
        _lock.release()


def leads_por_consultor(dt_ini, dt_fim) -> dict[int, int]:
    """{kommo_user_id: leads} do Kommo no período, só os dias antes do corte.

    Consultor excluído é reatribuído como na venda (_consultor_reassign_map).
    """
    ini = _as_date(dt_ini)
    fim = _as_date(dt_fim)
    ultimo = CORTE_KOMMO_BWIPO - timedelta(days=1)
    if ini and ini > ultimo:
        return {}
    if fim is None or fim > ultimo:
        fim = ultimo
    conds = ["dia <= %s"]
    params: list = [fim]
    if ini:
        conds.append("dia >= %s")
        params.append(ini)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('kommo_leads_dia')")
            if cur.fetchone()[0] is None:
                return {}
            cur.execute(
                f"SELECT kommo_user_id, SUM(n) FROM kommo_leads_dia WHERE {' AND '.join(conds)} GROUP BY 1",
                params,
            )
            fetched = cur.fetchall()
    finally:
        conn.close()
    from routes.comercial_rgm import _consultor_reassign_map

    reassign = _consultor_reassign_map()
    out: dict[int, int] = {}
    for uid, n in fetched:
        key = reassign.get(int(uid), int(uid))
        out[key] = out.get(key, 0) + int(n or 0)
    return out


def _as_date(v):
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def register_job(sched) -> None:
    """Remonta 2 min após o boot e todo dia às 04:00 (o Kommo não recebe mais leads novos)."""
    try:
        from apscheduler.triggers.cron import CronTrigger

        ensure_table()
        sched.add_job(
            rebuild,
            CronTrigger(hour=4, minute=0),
            id="kommo_leads_dia",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now(timezone.utc) + timedelta(minutes=2),
        )
        logger.info("kommo_leads_dia: job agendado (boot + 04:00)")
    except Exception:
        logger.exception("kommo_leads_dia: job não agendou")
