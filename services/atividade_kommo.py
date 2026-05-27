"""Service para consultar atividade Kommo dos consultores via Supabase REST."""
import logging
import os
from datetime import date, timedelta
from typing import Optional

import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

_TABLE = "kommo_consultor_atividade_dia"


def _headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    }


def _is_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_KEY)


def fetch_atividade_periodo(
    dt_ini: date,
    dt_fim: date,
    user_id: Optional[int] = None,
    incluir_intervalos: bool = True,
) -> list[dict]:
    """Retorna linhas brutas da tabela no período.

    Se incluir_intervalos=False, omite os JSONB pesados (intervalos_sem_atividade,
    eventos_por_tipo) para melhorar performance em períodos longos.
    """
    if not _is_configured():
        return []
    cols = [
        "id", "data_referencia", "created_by",
        "primeira_acao", "ultima_acao",
        "total_eventos", "total_leads_unicos",
        "minutos_ativos", "horas_ativas",
        "maior_intervalo_sem_atividade_minutos",
        "total_intervalos_sem_atividade",
    ]
    if incluir_intervalos:
        cols += ["intervalos_sem_atividade", "eventos_por_tipo"]
    # PostgREST aceita múltiplos filtros via repetição de query params como lista
    query = [
        ("select", ",".join(cols)),
        ("data_referencia", f"gte.{dt_ini.isoformat()}"),
        ("data_referencia", f"lte.{dt_fim.isoformat()}"),
        ("order", "data_referencia.desc,created_by.asc"),
    ]
    if user_id is not None:
        query.append(("created_by", f"eq.{int(user_id)}"))
    url = f"{SUPABASE_URL}/rest/v1/{_TABLE}"
    try:
        r = requests.get(url, headers=_headers(), params=query, timeout=15)
        r.raise_for_status()
        return r.json() or []
    except Exception as e:
        logging.getLogger(__name__).warning("Supabase REST falhou: %s", e)
        return []


def _peso_dia(d: date) -> float:
    # date.weekday(): 0=segunda ... 5=sábado, 6=domingo
    wd = d.weekday()
    if wd <= 4:
        return 1.0
    if wd == 5:
        return 0.5
    return 0.0


def divisor_ponderado(dt_ini: date, dt_fim: date) -> float:
    if dt_fim < dt_ini:
        return 0.0
    total = 0.0
    d = dt_ini
    while d <= dt_fim:
        total += _peso_dia(d)
        d += timedelta(days=1)
    return total


def horas_media_por_consultor(dt_ini: date, dt_fim: date) -> dict[int, dict]:
    """Retorna { created_by: { total_horas_ativas, total_eventos, total_leads_unicos, horas_media } }."""
    rows = fetch_atividade_periodo(dt_ini, dt_fim, incluir_intervalos=False)
    agg: dict[int, dict] = {}
    for row in rows:
        uid = row.get("created_by")
        if uid is None:
            continue
        a = agg.setdefault(int(uid), {
            "total_horas_ativas": 0.0,
            "total_eventos": 0,
            "total_leads_unicos": 0,
        })
        a["total_horas_ativas"] += float(row.get("horas_ativas") or 0)
        a["total_eventos"] += int(row.get("total_eventos") or 0)
        a["total_leads_unicos"] += int(row.get("total_leads_unicos") or 0)
    div = divisor_ponderado(dt_ini, dt_fim)
    for uid, a in agg.items():
        if div > 0:
            a["horas_media"] = round(a["total_horas_ativas"] / div, 2)
        else:
            a["horas_media"] = round(a["total_horas_ativas"], 2)
        a["total_horas_ativas"] = round(a["total_horas_ativas"], 2)
    return agg
