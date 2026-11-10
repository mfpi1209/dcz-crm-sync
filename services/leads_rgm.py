"""Cópia indexada da vw_leads_rgm no banco do Kommo (kommo_sync).

A view abre o custom_fields_json de todos os leads a cada consulta e só depois
filtra pelo RGM, então nenhum índice ajuda: cada consulta varre o Kommo inteiro.
A materialized view `mv_leads_rgm` guarda só (lead_id, rgm), com índice nos dois.

O Kommo foi desativado e o espelho kommo_sync não muda mais. Por isso, por
padrão (LEADS_RGM_REFRESH_MIN=0), a cópia é criada uma vez e reaproveitada no
boot, sem refresh periódico. Um sync manual do Kommo ainda a refaz no fim.
Com LEADS_RGM_REFRESH_MIN > 0, ela volta a ser refeita nesse intervalo
(REFRESH CONCURRENTLY, sem bloquear leitura) e vence após LEADS_RGM_MAX_AGE_MIN.

As consultas continuam juntando `leads`: responsável, status e exclusão do lead
vêm do espelho, não da cópia. Enquanto a cópia não está pronta, voltam para a view.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

VIEW = "vw_leads_rgm"
MV = "mv_leads_rgm"
REFRESH_MIN = int(os.getenv("LEADS_RGM_REFRESH_MIN", "0"))
MAX_IDADE_S = int(os.getenv("LEADS_RGM_MAX_AGE_MIN", "180")) * 60
# Sem refresh periódico, só confere de tempos em tempos se a cópia ainda existe.
_CHECAGEM_MIN = 10

_VIEW_RE = re.compile(rf"\b{VIEW}\b")
_lock = threading.Lock()
_ultimo_ok: float | None = None


def pronta() -> bool:
    """A cópia existe e pode ser usada no lugar da view."""
    if _ultimo_ok is None:
        return False
    if REFRESH_MIN <= 0:
        return True
    return time.time() - _ultimo_ok < MAX_IDADE_S


def sql(texto: str) -> str:
    """Troca vw_leads_rgm pela cópia indexada quando ela está pronta."""
    return _VIEW_RE.sub(MV, texto) if pronta() else texto


def _executar(refazer: bool) -> bool:
    """Cria a cópia se não existir. Com `refazer`, refaz a que existe."""
    global _ultimo_ok
    if not _lock.acquire(blocking=False):
        return False
    t0 = time.time()
    try:
        from routes.comercial_rgm import _pg_kommo

        conn = _pg_kommo()
        try:
            cur = conn.cursor()
            cur.execute("SELECT to_regclass(%s)", (f"public.{MV}",))
            if cur.fetchone()[0]:
                if refazer:
                    cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {MV}")
                    acao = "refeita"
                else:
                    acao = "existente, em uso"
            else:
                _ultimo_ok = None  # sumiu (DROP manual): volta para a view até recriar
                cur.execute(
                    f"CREATE MATERIALIZED VIEW IF NOT EXISTS {MV} AS "
                    f"SELECT lead_id, rgm FROM {VIEW}"
                )
                # O índice único é o que permite o REFRESH CONCURRENTLY.
                cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {MV}_lead_uq ON {MV} (lead_id)")
                cur.execute(f"CREATE INDEX IF NOT EXISTS {MV}_rgm_idx ON {MV} (rgm)")
                cur.execute(f"ANALYZE {MV}")
                acao = "criada"
            conn.commit()
        finally:
            conn.close()
        # Checagem de rotina (já pronta, nada mudou) não loga nem mexe no relógio.
        if refazer or _ultimo_ok is None:
            _ultimo_ok = time.time()
            logger.info("%s %s em %.1fs", MV, acao, _ultimo_ok - t0)
        return True
    except Exception:
        logger.exception("%s: falhou; consultas seguem na %s enquanto a cópia não estiver pronta", MV, VIEW)
        return False
    finally:
        _lock.release()


def garantir() -> bool:
    """Usa a cópia que já existe; cria se não existir. Não refaz."""
    return _executar(refazer=False)


def refrescar() -> bool:
    """Cria a cópia na primeira vez; depois refaz sem bloquear leitura."""
    return _executar(refazer=True)


def agendar_refresh() -> None:
    """Refaz a cópia em segundo plano (ex.: logo depois de um sync do Kommo)."""
    threading.Thread(target=refrescar, name="mv-leads-rgm", daemon=True).start()


def register_job(sched) -> None:
    from apscheduler.triggers.interval import IntervalTrigger

    periodico = REFRESH_MIN > 0
    sched.add_job(
        refrescar if periodico else garantir,
        IntervalTrigger(minutes=REFRESH_MIN if periodico else _CHECAGEM_MIN),
        id="mv_leads_rgm",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=5),
    )
    if periodico:
        logger.info("%s: refresh a cada %s min", MV, REFRESH_MIN)
    else:
        logger.info("%s: Kommo congelado, cria uma vez e confere a cada %s min", MV, _CHECAGEM_MIN)
