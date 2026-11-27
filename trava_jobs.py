"""Trava no Postgres para nenhum job agendado rodar em dobro (Modernização, Fase 3).

Cada execução de job pega `pg_try_advisory_lock(<hash do id do job>)` numa
conexão própria (fora do pool: lock de sessão sobrevive ao RESET ALL do pool).
Se outro processo já está rodando o mesmo job, esta execução é pulada.

Cobre:
- deploy com o container velho e o novo de pé ao mesmo tempo;
- site e worker ligados com o mesmo job por engano;
- mais de um worker do Gunicorn no futuro.

O lock some sozinho se o processo morrer (a sessão cai), então não fica travado.
Se o banco não responder, o job roda sem trava, como antes (`TRAVA_JOBS_ESTRITA=1` pula em vez disso).
"""
from __future__ import annotations

import contextlib
import functools
import hashlib
import logging
import os

import psycopg2
import psycopg2.errors

logger = logging.getLogger(__name__)

_PREFIXO = "dcz_job:"


def chave_do_job(job_id: str) -> int:
    """bigint estável (signed 64 bits) a partir do id do job."""
    h = hashlib.sha1((_PREFIXO + job_id).encode()).digest()
    return int.from_bytes(h[:8], "big", signed=True)


def ligada() -> bool:
    return os.getenv("TRAVA_JOBS", "1") != "0"


def _conectar():
    from db import DB_DSN
    return psycopg2.connect(connect_timeout=10, application_name="dcz_trava_jobs", **DB_DSN)


def com_trava(job_id: str, func):
    """Embrulha `func` para rodar só se conseguir a trava do `job_id`."""
    if getattr(func, "_com_trava", False):
        return func

    @functools.wraps(func)
    def executar(*args, **kwargs):
        if not ligada():
            return func(*args, **kwargs)
        chave = chave_do_job(job_id)
        try:
            conn = _conectar()
            conn.autocommit = True
        except Exception as e:
            if os.getenv("TRAVA_JOBS_ESTRITA") == "1":
                logger.warning("[trava] %s pulado: banco indisponível para a trava (%s)", job_id, e)
                return None
            logger.warning("[trava] %s roda sem trava: banco indisponível (%s)", job_id, e)
            return func(*args, **kwargs)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (chave,))
                pegou = cur.fetchone()[0]
            if not pegou:
                logger.info("[trava] %s já está rodando em outro processo; esta execução foi pulada", job_id)
                return None
            try:
                return func(*args, **kwargs)
            finally:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_advisory_unlock(%s)", (chave,))
                except Exception:
                    pass  # fechar a conexão abaixo solta a trava de qualquer jeito
        finally:
            try:
                conn.close()
            except Exception:
                pass

    executar._com_trava = True
    executar._job_id = job_id
    return executar


@contextlib.contextmanager
def trava_global(nome: str, esperar: bool = True, espera_max_s: int = 300):
    """Trava entre processos para um trecho (ex.: montagem da base do painel).

    Substitui o `threading.Lock` quando site e worker podem fazer a mesma coisa.
    `esperar=True` aguarda até `espera_max_s`; senão só tenta. Entrega True/False.
    Sem banco para a trava, entrega True (comportamento de antes).
    Use um `nome` diferente do id do job que chama este trecho: a trava do job
    fica numa outra sessão e as duas se bloqueariam.
    """
    if not ligada():
        yield True
        return
    chave = chave_do_job("trecho:" + nome)
    try:
        conn = _conectar()
        conn.autocommit = True
    except Exception as e:
        logger.warning("[trava] %s sem trava: banco indisponível (%s)", nome, e)
        yield True
        return
    pegou = False
    try:
        with conn.cursor() as cur:
            if esperar:
                cur.execute("SET lock_timeout = %s", (f"{int(espera_max_s)}s",))
                try:
                    cur.execute("SELECT pg_advisory_lock(%s)", (chave,))
                    pegou = True
                except psycopg2.errors.LockNotAvailable:
                    logger.warning("[trava] %s: esperou %ss e desistiu", nome, espera_max_s)
            else:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (chave,))
                pegou = cur.fetchone()[0]
        yield pegou
    finally:
        try:
            conn.close()          # fechar a sessão solta a trava
        except Exception:
            pass


# ── Última execução no banco ────────────────────────────────────────────────
# Com o job no worker, a memória do site não sabe quando ele rodou. Quem precisa
# mostrar isso na tela grava aqui e a rota lê daqui (ex.: Conexão SIAA).

_SQL_STATUS = """
CREATE TABLE IF NOT EXISTS painel_job_status (
    job_id TEXT PRIMARY KEY,
    em TIMESTAMPTZ NOT NULL,
    erro TEXT
)
"""


def gravar_status(job_id: str, em, erro: str | None) -> None:
    """Upsert da última execução. Falha só vira aviso no log."""
    try:
        from db import get_conn
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(_SQL_STATUS)
                cur.execute(
                    """INSERT INTO painel_job_status (job_id, em, erro) VALUES (%s, %s, %s)
                       ON CONFLICT (job_id) DO UPDATE SET em = EXCLUDED.em, erro = EXCLUDED.erro""",
                    (job_id, em, erro),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("[status] não gravei a última execução de %s (%s)", job_id, type(e).__name__)


def ler_status(job_id: str) -> dict | None:
    """{'em': datetime, 'erro': str|None} ou None (nunca rodou / sem tabela / sem banco)."""
    try:
        from db import get_conn
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.painel_job_status')")
                if not cur.fetchone()[0]:
                    return None
                cur.execute("SELECT em, erro FROM painel_job_status WHERE job_id = %s", (job_id,))
                row = cur.fetchone()
            conn.rollback()
        finally:
            conn.close()
    except Exception:
        return None
    return {"em": row[0], "erro": row[1]} if row else None
