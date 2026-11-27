"""
Performance de Campanhas — leads Meta e Sem Campanha a partir do Bwipo (desde 01/10/2026).

Até 30/09/2026 os leads vinham do Kommo (Supabase `campanhas_meta` via n8n e `sem_campanha`).
Com a migração, essas tabelas pararam em 01/10. O Bwipo guarda a origem do anúncio no
contato (adUtm*, adCtwaClid, gclid, fbclid…), mas esses campos só vêm em
GET /api/contacts/{id}: a listagem e o contato embutido no negócio não trazem.

- Coleta: job `bwipo_contact_ads` (a cada 5 min) busca o detalhe dos contatos dos negócios do
  Pipeline Principal criados desde 01/10 que ainda não estão em `bwipo_contact_ads`, do mais
  novo para o mais antigo (o dia de hoje enche primeiro, o histórico vem atrás).
- Leitura: um lead = um contato com negócio no Pipeline Principal criado no período (dia em BRT).
  Status do negócio mais recente do período (WON/LOST/aberto), como no Google.
  Matrículas = RGMs da `bwipo_painel_base` com data no período cujo negócio é de contato Meta.

Cache de 3 min por período (o gunicorn tem só 4 threads).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone

from db import get_conn
from services import bwipo_comercial as crm
from services.google_ads_insights import SEM_UTM, norm_campaign

logger = logging.getLogger(__name__)

CORTE_BWIPO = date(2026, 10, 1)

_PRINCIPAL = (
    "(position('principal' in lower(COALESCE(d.pipeline_name, ''))) > 0"
    " OR COALESCE(d.pipeline_name, '') = '')"
)

# campo da API -> coluna de bwipo_contact_ads
_CAMPOS = {
    "adUtmSource": "utm_source",
    "adUtmMedium": "utm_medium",
    "adUtmCampaign": "utm_campaign",
    "adUtmContent": "utm_content",
    "adUtmTerm": "utm_term",
    "adCtwaClid": "ctwa_clid",
    "adSourceId": "ad_source_id",
    "adSourceType": "ad_source_type",
    "adHeadline": "ad_headline",
    "adResolveStatus": "resolve_status",
    "adResolvedCampaignName": "ad_campaign_name",
    "adResolvedAdsetName": "ad_adset_name",
    "adResolvedName": "ad_name",
    "gclid": "gclid",
    "fbclid": "fbclid",
    "referrer": "referrer",
    "source": "contact_source",
}
_COLS = list(_CAMPOS.values())
_MAX_TENTATIVAS = 5


# ---------------------------------------------------------------------------
# Coleta (job)
# ---------------------------------------------------------------------------

def _txt(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s[:1000] or None


def campos_do_contato(contato: dict) -> dict:
    return {col: _txt(contato.get(api)) for api, col in _CAMPOS.items()}


def _pendentes(cur, limite: int) -> list[str]:
    """Contatos sem coleta, com erro a repetir, anúncio ainda não resolvido ou negócio novo depois da coleta."""
    cur.execute(
        f"""
        SELECT d.contact_id
        FROM bwipo_deals d
        LEFT JOIN bwipo_contact_ads a ON a.contact_id = d.contact_id
        WHERE NOT d.is_deleted AND {_PRINCIPAL}
          AND d.contact_id IS NOT NULL
          AND d.created_at >= %s
        GROUP BY d.contact_id, a.contact_id, a.erro, a.tentativas, a.fetched_at, a.ad_source_id, a.resolve_status
        HAVING a.contact_id IS NULL
            OR (a.tentativas < %s AND a.erro IS NOT NULL AND a.fetched_at < NOW() - INTERVAL '30 minutes')
            OR (a.tentativas < %s AND a.ad_source_id IS NOT NULL
                AND COALESCE(a.resolve_status, '') <> 'ok' AND a.fetched_at < NOW() - INTERVAL '1 hour')
            OR max(d.created_at) > a.fetched_at + INTERVAL '10 minutes'
        ORDER BY max(d.created_at) DESC
        LIMIT %s
        """,
        (datetime(CORTE_BWIPO.year, CORTE_BWIPO.month, CORTE_BWIPO.day, 3, tzinfo=timezone.utc),
         _MAX_TENTATIVAS, _MAX_TENTATIVAS, limite),
    )
    return [r[0] for r in cur.fetchall()]


def _gravar(cur, contact_id: str, campos: dict | None, erro: str | None) -> None:
    if campos is None:
        # erro: guarda só o erro e soma a tentativa; não apaga o que já tinha
        cur.execute(
            """
            INSERT INTO bwipo_contact_ads (contact_id, erro, tentativas, fetched_at)
            VALUES (%s, %s, 1, NOW())
            ON CONFLICT (contact_id) DO UPDATE SET
                erro = EXCLUDED.erro, tentativas = bwipo_contact_ads.tentativas + 1, fetched_at = NOW()
            """,
            (contact_id, erro),
        )
        return
    cols = ", ".join(_COLS)
    ph = ", ".join(["%s"] * len(_COLS))
    upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in _COLS)
    cur.execute(
        f"""
        INSERT INTO bwipo_contact_ads (contact_id, {cols}, erro, tentativas, fetched_at)
        VALUES (%s, {ph}, NULL, 1, NOW())
        ON CONFLICT (contact_id) DO UPDATE SET {upd}, erro = NULL,
            tentativas = bwipo_contact_ads.tentativas + 1, fetched_at = NOW()
        """,
        [contact_id] + [campos[c] for c in _COLS],
    )


def coletar_lote(tempo_max: float = 240.0, limite: int = 800) -> dict:
    """Busca o detalhe de até `limite` contatos pendentes, parando em `tempo_max` segundos."""
    if not crm.configured():
        return {"ok": False, "motivo": "BWIPO_COMERCIAL_API_TOKEN ausente"}
    inicio = time.monotonic()
    stats = Counter()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ids = _pendentes(cur, limite)
        conn.commit()
        stats["fila"] = len(ids)
        for i, cid in enumerate(ids, 1):
            if time.monotonic() - inicio > tempo_max:
                break
            try:
                contato = crm.get_contact(cid)
                campos, erro = campos_do_contato(contato), None
                stats["anuncio" if campos["ctwa_clid"] or campos["utm_source"] or campos["gclid"] else "sem_anuncio"] += 1
            except crm.BwipoComercialError as e:
                campos, erro = None, f"{e.status}: {str(e)[:200]}"
                stats["erro"] += 1
                if e.status in (401, 403, 503):
                    with conn.cursor() as cur:
                        _gravar(cur, cid, None, erro)
                    conn.commit()
                    logger.warning("bwipo_contact_ads: token sem acesso (%s); parando o lote", e.status)
                    break
            with conn.cursor() as cur:
                _gravar(cur, cid, campos, erro)
            if i % 50 == 0:
                conn.commit()
        conn.commit()
    finally:
        conn.close()
    stats["segundos"] = round(time.monotonic() - inicio)
    return dict(stats)


def register_bwipo_contact_ads_job(sched) -> None:
    """A cada 5 min. BWIPO_CONTACT_ADS_JOB=0 desliga (ex.: rodando local sem querer bater na API)."""
    if os.getenv("BWIPO_CONTACT_ADS_JOB", "1").strip() == "0":
        logger.info("bwipo_contact_ads desligado por BWIPO_CONTACT_ADS_JOB=0")
        return
    try:
        from apscheduler.triggers.interval import IntervalTrigger

        def _run():
            try:
                logger.info("bwipo_contact_ads %s", coletar_lote())
            except Exception:
                logger.exception("bwipo_contact_ads")

        sched.add_job(
            _run,
            IntervalTrigger(minutes=5),
            id="bwipo_contact_ads",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=60),
        )
        logger.info("bwipo_contact_ads a cada 5 min")
    except Exception:
        logger.exception("bwipo_contact_ads não agendou")


# ---------------------------------------------------------------------------
# Classificação e leitura
# ---------------------------------------------------------------------------

_META_SOURCES = {"meta", "facebook", "fb", "instagram", "ig", "insta", "meta_ads", "facebook_ads", "fb_ads", "ig_ads"}


def origem(a: dict | None) -> str:
    """meta | google | outra | sem | pendente | erro."""
    if not a or not a.get("coletado"):
        return "pendente"
    if a.get("erro") and not any(a.get(c) for c in _COLS):
        return "erro"
    src = (a.get("utm_source") or "").strip().lower()
    if a.get("ctwa_clid") or (a.get("ad_source_type") or "").lower() == "ad":
        return "meta"  # clique em anúncio para o WhatsApp (só a Meta tem)
    if src in _META_SOURCES or "facebook" in src or "instagram" in src or src.startswith("meta"):
        return "meta"
    if a.get("gclid") or "google" in src:
        return "google"
    if a.get("fbclid"):
        return "meta"
    if src or a.get("utm_campaign") or a.get("ad_source_id"):
        return "outra"
    return "sem"


def nome_campanha(a: dict) -> str:
    return norm_campaign(a.get("utm_campaign")) or norm_campaign(a.get("ad_campaign_name")) or SEM_UTM


def _brt(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 3, tzinfo=timezone.utc)  # 00:00 BRT


_SEL_ADS = ", ".join(f"a.{c}" for c in _COLS)


def _leads(cur, ini: date, fim: date) -> list[dict]:
    """Um registro por contato com negócio no Principal criado no período; status do mais recente."""
    cur.execute(
        f"""
        WITH d AS (
            SELECT DISTINCT ON (d.contact_id) d.contact_id, upper(COALESCE(d.status, '')) AS st
            FROM bwipo_deals d
            WHERE NOT d.is_deleted AND {_PRINCIPAL}
              AND d.contact_id IS NOT NULL
              AND d.created_at >= %s AND d.created_at < %s
            ORDER BY d.contact_id, d.created_at DESC
        )
        SELECT d.contact_id, d.st, (a.contact_id IS NOT NULL) AS coletado, a.erro, {_SEL_ADS}
        FROM d LEFT JOIN bwipo_contact_ads a ON a.contact_id = d.contact_id
        """,
        (_brt(ini), _brt(fim + timedelta(days=1))),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _matriculas(cur, ini: date, fim: date) -> list[dict]:
    """RGMs com matrícula no período e o contato do negócio Bwipo (Principal) de cada um."""
    cur.execute(
        f"""
        SELECT DISTINCT ON (b.rgm) b.rgm, (a.contact_id IS NOT NULL) AS coletado, a.erro, {_SEL_ADS}
        FROM bwipo_painel_base b
        JOIN bwipo_deals d ON d.rgm_norm = b.rgm AND NOT d.is_deleted AND {_PRINCIPAL}
        LEFT JOIN bwipo_contact_ads a ON a.contact_id = d.contact_id
        WHERE b.data_matricula BETWEEN %s AND %s
        ORDER BY b.rgm,
                 (a.ctwa_clid IS NOT NULL OR a.utm_source IS NOT NULL) DESC,
                 d.created_at DESC NULLS LAST
        """,
        (ini, fim),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _status(st: str) -> str:
    return {"WON": "ganhos", "LOST": "perdidos"}.get(st, "novos")


def _build(ini: date, fim: date) -> dict:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '60s'")
            leads = _leads(cur, ini, fim)
            mats = _matriculas(cur, ini, fim)
        conn.commit()
    finally:
        conn.close()

    por_origem = Counter()
    camp: dict[str, dict] = {}
    sem = {"utm_campaign": "Sem Campanha", "utm_source": "sem_campanha", "utm_medium": "—",
           "novos": 0, "ganhos": 0, "perdidos": 0}

    def slot(a: dict) -> dict:
        nome = nome_campanha(a)
        if nome not in camp:
            camp[nome] = {"utm_campaign": nome, "utm_source": a.get("utm_source") or "Meta",
                          "utm_medium": a.get("utm_medium") or "", "novos": 0, "ganhos": 0,
                          "perdidos": 0, "matriculas": 0}
        return camp[nome]

    for l in leads:
        o = origem(l)
        por_origem[o] += 1
        if o == "meta":
            slot(l)[_status(l["st"])] += 1
        elif o == "sem":
            sem[_status(l["st"])] += 1

    mat_meta = 0
    for m in mats:
        if origem(m) == "meta":
            mat_meta += 1
            slot(m)["matriculas"] += 1

    return {
        "meta": list(camp.values()),
        "sem_campanha": [sem] if (sem["novos"] + sem["ganhos"] + sem["perdidos"]) else [],
        "resumo": {
            "leads": len(leads),
            "por_origem": dict(por_origem),
            "pendentes": por_origem.get("pendente", 0),
            "matriculas_meta": mat_meta,
        },
    }


_TTL = 180
_cache: dict[tuple, tuple[float, dict]] = {}
_cache_lock = threading.Lock()
_period_locks: dict[tuple, threading.Lock] = {}


def leads_bwipo(ini: date, fim: date) -> dict:
    key = (ini, fim)
    with _cache_lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < _TTL:
            return hit[1]
        lock = _period_locks.setdefault(key, threading.Lock())
    with lock:
        with _cache_lock:
            hit = _cache.get(key)
            if hit and time.time() - hit[0] < _TTL:
                return hit[1]
        data = _build(ini, fim)
        with _cache_lock:
            _cache[key] = (time.time(), data)
            for k in [k for k, (t, _) in _cache.items() if time.time() - t > _TTL]:
                _cache.pop(k, None)
        return data


def periodo_bwipo(from_date: date, to_date: date) -> tuple[date, date] | None:
    """Parte do período que vem do Bwipo (de 01/10/2026 em diante)."""
    ini = max(from_date, CORTE_BWIPO)
    return (ini, to_date) if ini <= to_date else None


def periodo_legado(from_date: date, to_date: date) -> tuple[date, date] | None:
    """Parte do período que vem das tabelas antigas do Kommo (até 30/09/2026)."""
    fim = min(to_date, CORTE_BWIPO - timedelta(days=1))
    return (from_date, fim) if from_date <= fim else None
