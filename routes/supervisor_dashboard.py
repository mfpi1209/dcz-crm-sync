"""Painel Supervisor Comercial — endpoint agregado.

Reaproveita as fontes existentes (PG do app + PG do Kommo) para entregar
em uma única chamada todos os KPIs, séries e ranking necessários para o
dashboard da categoria "Supervisor Comercial".
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, date
from typing import Any

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

from helpers import BRT

logger = logging.getLogger(__name__)

supervisor_dashboard_bp = Blueprint("supervisor_dashboard", __name__)


# ── conexões ─────────────────────────────────────────────────────────────────

def _pg():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        dbname=os.getenv("DB_NAME", "dcz_sync"),
    )


def _pg_kommo():
    return psycopg2.connect(
        host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
        port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
        user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
        password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
        dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
    )


# ── helpers ──────────────────────────────────────────────────────────────────

def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _delta_pct(curr: float, prev: float) -> float | None:
    """Retorna variação percentual; None quando o anterior é 0/none."""
    if prev is None or prev == 0:
        return None
    return round(((curr - prev) / prev) * 100, 1)


def _to_ts(d: date) -> int:
    return int(datetime.combine(d, datetime.min.time(), tzinfo=BRT).timestamp())


def _get_aceite_status_ids() -> list[int]:
    """Procura status_ids cujo nome contém 'aceite' nos pipelines Kommo."""
    try:
        kc = _pg_kommo()
        cur = kc.cursor()
        cur.execute(
            "SELECT id FROM pipeline_statuses WHERE LOWER(name) LIKE %s",
            ("%aceite%",),
        )
        ids = [int(r[0]) for r in cur.fetchall() if r and r[0] is not None]
        cur.close()
        kc.close()
        return ids
    except Exception as e:
        logger.warning("aceite ids lookup failed: %s", e)
        return []


def _kpi_novos_leads(dt_ini: date, dt_fim: date) -> dict[str, Any]:
    """Total de leads criados no período + variação vs período anterior de mesma duração."""
    days = max(1, (dt_fim - dt_ini).days + 1)
    prev_fim = dt_ini - timedelta(days=1)
    prev_ini = prev_fim - timedelta(days=days - 1)

    try:
        kc = _pg_kommo()
        cur = kc.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM leads WHERE created_at >= %s AND created_at <= %s AND NOT is_deleted",
            (_to_ts(dt_ini), _to_ts(dt_fim) + 86399),
        )
        curr = cur.fetchone()[0] or 0
        cur.execute(
            "SELECT COUNT(*) FROM leads WHERE created_at >= %s AND created_at <= %s AND NOT is_deleted",
            (_to_ts(prev_ini), _to_ts(prev_fim) + 86399),
        )
        prev = cur.fetchone()[0] or 0
        cur.close()
        kc.close()
    except Exception as e:
        logger.warning("kpi novos_leads fail: %s", e)
        return {"valor": 0, "delta_pct": None, "comparacao": "vs período anterior"}

    return {
        "valor": int(curr),
        "delta_pct": _delta_pct(curr, prev),
        "anterior": int(prev),
        "comparacao": "vs período anterior",
    }


def _kpi_vendas(dt_ini: date, dt_fim: date) -> dict[str, Any]:
    """Matrículas EM CURSO no período (tabela comercial_rgm_atual)."""
    days = max(1, (dt_fim - dt_ini).days + 1)
    prev_fim = dt_ini - timedelta(days=1)
    prev_ini = prev_fim - timedelta(days=days - 1)
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(DISTINCT rgm) FROM comercial_rgm_atual "
            "WHERE data_matricula BETWEEN %s AND %s",
            (dt_ini, dt_fim),
        )
        curr = cur.fetchone()[0] or 0
        cur.execute(
            "SELECT COUNT(DISTINCT rgm) FROM comercial_rgm_atual "
            "WHERE data_matricula BETWEEN %s AND %s",
            (prev_ini, prev_fim),
        )
        prev = cur.fetchone()[0] or 0
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("kpi vendas fail: %s", e)
        return {"valor": 0, "delta_pct": None, "comparacao": "vs período anterior"}

    return {
        "valor": int(curr),
        "delta_pct": _delta_pct(curr, prev),
        "anterior": int(prev),
        "comparacao": "vs período anterior",
    }


def _kpi_aceites_pendentes() -> dict[str, Any]:
    """Total de leads na fila com status 'aceite' (ainda não viraram matrícula)."""
    ids = _get_aceite_status_ids()
    if not ids:
        return {"valor": 0, "comparacao": "leads na fila"}
    try:
        ph = ",".join(["%s"] * len(ids))
        kc = _pg_kommo()
        cur = kc.cursor()
        cur.execute(
            f"SELECT COUNT(*) FROM leads WHERE NOT is_deleted AND status_id IN ({ph})",
            ids,
        )
        total = cur.fetchone()[0] or 0
        cur.close()
        kc.close()
    except Exception as e:
        logger.warning("kpi aceites fail: %s", e)
        total = 0
    return {"valor": int(total), "comparacao": "na fila"}


def _kpi_leads_parados(threshold_hours: int = 24) -> dict[str, Any]:
    """Total de leads em pipelines de venda que estão sem movimentação há mais de N horas."""
    try:
        threshold_ts = int(datetime.now(BRT).timestamp()) - (threshold_hours * 3600)
        kc = _pg_kommo()
        cur = kc.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM leads WHERE NOT is_deleted AND updated_at <= %s",
            (threshold_ts,),
        )
        total = cur.fetchone()[0] or 0
        cur.close()
        kc.close()
    except Exception as e:
        logger.warning("kpi leads_parados fail: %s", e)
        total = 0
    return {"valor": int(total), "comparacao": f">{threshold_hours}h sem mover", "threshold_h": threshold_hours}


# ── séries diárias ──────────────────────────────────────────────────────────

def _serie_leads_diaria(dt_ini: date, dt_fim: date) -> dict[date, int]:
    out: dict[date, int] = {}
    try:
        kc = _pg_kommo()
        cur = kc.cursor()
        cur.execute(
            """
            SELECT DATE(to_timestamp(created_at) AT TIME ZONE 'America/Sao_Paulo') AS d, COUNT(*)
            FROM leads
            WHERE NOT is_deleted
              AND created_at >= %s AND created_at <= %s
            GROUP BY d
            """,
            (_to_ts(dt_ini), _to_ts(dt_fim) + 86399),
        )
        for d, c in cur.fetchall():
            if d:
                out[d] = int(c)
        cur.close()
        kc.close()
    except Exception as e:
        logger.warning("serie leads fail: %s", e)
    return out


def _serie_vendas_diaria(dt_ini: date, dt_fim: date) -> dict[date, int]:
    out: dict[date, int] = {}
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT data_matricula, COUNT(DISTINCT rgm)
            FROM comercial_rgm_atual
            WHERE data_matricula BETWEEN %s AND %s
            GROUP BY data_matricula
            """,
            (dt_ini, dt_fim),
        )
        for d, c in cur.fetchall():
            if d:
                out[d] = int(c)
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("serie vendas fail: %s", e)
    return out


def _build_perf_diaria(dt_ini: date, dt_fim: date) -> dict[str, Any]:
    """Monta a série diária com leads + vendas, e a versão de 1 ano atrás."""
    yoy_ini = dt_ini.replace(year=dt_ini.year - 1) if dt_ini.year > 1 else dt_ini
    yoy_fim = dt_fim.replace(year=dt_fim.year - 1) if dt_fim.year > 1 else dt_fim

    leads = _serie_leads_diaria(dt_ini, dt_fim)
    vendas = _serie_vendas_diaria(dt_ini, dt_fim)
    leads_yoy = _serie_leads_diaria(yoy_ini, yoy_fim)
    vendas_yoy = _serie_vendas_diaria(yoy_ini, yoy_fim)

    days: list[date] = []
    d = dt_ini
    while d <= dt_fim:
        days.append(d)
        d += timedelta(days=1)

    labels = [d.isoformat() for d in days]
    return {
        "labels": labels,
        "leads": [leads.get(d, 0) for d in days],
        "vendas": [vendas.get(d, 0) for d in days],
        "leads_yoy": [
            leads_yoy.get(d.replace(year=d.year - 1) if d.year > 1 else d, 0) for d in days
        ],
        "vendas_yoy": [
            vendas_yoy.get(d.replace(year=d.year - 1) if d.year > 1 else d, 0) for d in days
        ],
    }


# ── médias ──────────────────────────────────────────────────────────────────

def _medias(dt_ini: date, dt_fim: date) -> dict[str, Any]:
    """Médias diárias de leads e vendas no período atual, últimos 6 meses e último ano."""
    today = datetime.now(BRT).date()
    days_atual = max(1, (dt_fim - dt_ini).days + 1)

    m6_ini = today - timedelta(days=180)
    y1_ini = today - timedelta(days=365)
    days_6m = 180
    days_1a = 365

    out = {
        "leads_dia": {"atual": 0.0, "m6": 0.0, "y1": 0.0},
        "vendas_dia": {"atual": 0.0, "m6": 0.0, "y1": 0.0},
        "vendas_total": {"atual": 0, "m6": 0, "y1": 0},
        "leads_total": {"atual": 0, "m6": 0, "y1": 0},
    }

    try:
        kc = _pg_kommo()
        cur = kc.cursor()

        cur.execute(
            "SELECT COUNT(*) FROM leads WHERE NOT is_deleted AND created_at >= %s AND created_at <= %s",
            (_to_ts(dt_ini), _to_ts(dt_fim) + 86399),
        )
        leads_atual = cur.fetchone()[0] or 0

        cur.execute(
            "SELECT COUNT(*) FROM leads WHERE NOT is_deleted AND created_at >= %s",
            (_to_ts(m6_ini),),
        )
        leads_6m = cur.fetchone()[0] or 0

        cur.execute(
            "SELECT COUNT(*) FROM leads WHERE NOT is_deleted AND created_at >= %s",
            (_to_ts(y1_ini),),
        )
        leads_1a = cur.fetchone()[0] or 0

        cur.close()
        kc.close()

        out["leads_total"] = {"atual": int(leads_atual), "m6": int(leads_6m), "y1": int(leads_1a)}
        out["leads_dia"] = {
            "atual": round(leads_atual / days_atual, 1),
            "m6": round(leads_6m / days_6m, 1),
            "y1": round(leads_1a / days_1a, 1),
        }
    except Exception as e:
        logger.warning("medias leads fail: %s", e)

    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(DISTINCT rgm) FROM comercial_rgm_atual WHERE data_matricula BETWEEN %s AND %s",
            (dt_ini, dt_fim),
        )
        v_atual = cur.fetchone()[0] or 0
        cur.execute(
            "SELECT COUNT(DISTINCT rgm) FROM comercial_rgm_atual WHERE data_matricula >= %s",
            (m6_ini,),
        )
        v_6m = cur.fetchone()[0] or 0
        cur.execute(
            "SELECT COUNT(DISTINCT rgm) FROM comercial_rgm_atual WHERE data_matricula >= %s",
            (y1_ini,),
        )
        v_1a = cur.fetchone()[0] or 0
        cur.close()
        conn.close()

        out["vendas_total"] = {"atual": int(v_atual), "m6": int(v_6m), "y1": int(v_1a)}
        out["vendas_dia"] = {
            "atual": round(v_atual / days_atual, 1),
            "m6": round(v_6m / days_6m, 1),
            "y1": round(v_1a / days_1a, 1),
        }
    except Exception as e:
        logger.warning("medias vendas fail: %s", e)

    return out


# ── ranking ─────────────────────────────────────────────────────────────────

def _get_active_campanha_lite() -> dict[str, Any] | None:
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        ref = datetime.now(BRT).date()
        cur.execute(
            """
            SELECT id, nome, dt_inicio, dt_fim FROM premiacao_campanha
            WHERE ativa = TRUE AND dt_inicio <= %s AND dt_fim >= %s
            ORDER BY dt_inicio DESC LIMIT 1
            """,
            (ref, ref),
        )
        row = cur.fetchone()
        if not row:
            cur.execute("SELECT id, nome, dt_inicio, dt_fim FROM premiacao_campanha ORDER BY dt_inicio DESC LIMIT 1")
            row = cur.fetchone()
        cur.close()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def _ranking_consultores(dt_ini: date, dt_fim: date, top_n: int = 10) -> list[dict[str, Any]]:
    """Ranking por matrículas + leads atendidos no período. Inclui taxa de conversão e tier."""
    # 1) matrículas por agente (via vw_leads_rgm + leads.responsible_user_id)
    matriculas_por_uid: dict[int, int] = {}
    rgms_periodo: set[str] = set()
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT rgm FROM comercial_rgm_atual WHERE data_matricula BETWEEN %s AND %s",
            (dt_ini, dt_fim),
        )
        rgms_periodo = {r[0] for r in cur.fetchall() if r[0]}
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("ranking rgms fail: %s", e)

    if rgms_periodo:
        try:
            kc = _pg_kommo()
            cur = kc.cursor()
            ph = ",".join(["%s"] * len(rgms_periodo))
            cur.execute(
                f"""
                SELECT l.responsible_user_id, COUNT(DISTINCT v.rgm) AS total
                FROM vw_leads_rgm v
                JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                WHERE v.rgm IN ({ph})
                GROUP BY l.responsible_user_id
                """,
                list(rgms_periodo),
            )
            for uid, total in cur.fetchall():
                if uid:
                    matriculas_por_uid[int(uid)] = int(total)
            cur.close()
            kc.close()
        except Exception as e:
            logger.warning("ranking matriculas fail: %s", e)

    # 2) leads recebidos no período por agente (denominador da taxa de conversão)
    leads_por_uid: dict[int, int] = {}
    nomes_por_uid: dict[int, str] = {}
    try:
        kc = _pg_kommo()
        cur = kc.cursor()
        cur.execute(
            """
            SELECT l.responsible_user_id, COUNT(*)
            FROM leads l
            WHERE NOT l.is_deleted
              AND l.created_at >= %s AND l.created_at <= %s
              AND l.responsible_user_id IS NOT NULL
            GROUP BY l.responsible_user_id
            """,
            (_to_ts(dt_ini), _to_ts(dt_fim) + 86399),
        )
        for uid, total in cur.fetchall():
            if uid:
                leads_por_uid[int(uid)] = int(total)

        all_uids = set(matriculas_por_uid.keys()) | set(leads_por_uid.keys())
        if all_uids:
            ph = ",".join(["%s"] * len(all_uids))
            cur.execute(
                f"SELECT id, name FROM users WHERE id IN ({ph})",
                list(all_uids),
            )
            for uid, name in cur.fetchall():
                if uid:
                    nomes_por_uid[int(uid)] = (name or "").strip() or f"ID {uid}"
        cur.close()
        kc.close()
    except Exception as e:
        logger.warning("ranking leads/nomes fail: %s", e)

    # Fallback: nomes faltantes via dcz_sync.kommo_users
    pendentes = [uid for uid in (set(matriculas_por_uid.keys()) | set(leads_por_uid.keys()))
                 if uid not in nomes_por_uid]
    if pendentes:
        try:
            conn = _pg()
            cur = conn.cursor()
            ph = ",".join(["%s"] * len(pendentes))
            cur.execute(f"SELECT id, name FROM kommo_users WHERE id IN ({ph})", pendentes)
            for uid, name in cur.fetchall():
                if uid:
                    nomes_por_uid[int(uid)] = (name or "").strip() or f"ID {uid}"
            cur.close()
            conn.close()
        except Exception as e:
            logger.warning("ranking nomes fallback fail: %s", e)

    # 3) metas (campanha ativa ou última)
    metas_por_uid: dict[int, dict[str, int]] = {}
    campanha = _get_active_campanha_lite()
    if campanha:
        try:
            conn = _pg()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT kommo_user_id, meta, meta_intermediaria, supermeta
                FROM premiacao_campanha_meta WHERE campanha_id = %s
                """,
                (campanha["id"],),
            )
            for uid, meta, inter, sup in cur.fetchall():
                if uid:
                    metas_por_uid[int(uid)] = {
                        "meta": int(meta or 0),
                        "intermediaria": int(inter or 0),
                        "supermeta": int(sup or 0),
                    }
            cur.close()
            conn.close()
        except Exception as e:
            logger.warning("ranking metas fail: %s", e)

    def _tier(total: int, m: dict[str, int]) -> tuple[str, int, int]:
        sup = m.get("supermeta", 0)
        meta = m.get("meta", 0)
        inter = m.get("intermediaria", 0)
        if sup > 0 and total >= sup:
            return "supermeta", sup, 100
        if meta > 0 and total >= meta:
            target = sup if sup > 0 else meta
            pct = min(100, round(total / target * 100)) if target > 0 else 100
            return "meta", target, pct
        if inter > 0 and total >= inter:
            target = meta if meta > 0 else inter
            pct = min(100, round(total / target * 100)) if target > 0 else 100
            return "intermediaria", target, pct
        target = inter if inter > 0 else (meta if meta > 0 else 0)
        pct = min(100, round(total / target * 100)) if target > 0 else 0
        return "base", target, pct

    rows: list[dict[str, Any]] = []
    all_uids = set(matriculas_por_uid.keys()) | set(leads_por_uid.keys())
    for uid in all_uids:
        if uid <= 0:
            continue
        mats = matriculas_por_uid.get(uid, 0)
        leads = leads_por_uid.get(uid, 0)
        conv = round((mats / leads) * 100, 1) if leads > 0 else 0.0
        metas = metas_por_uid.get(uid, {})
        tier, target, progresso = _tier(mats, metas)
        rows.append({
            "kommo_uid": uid,
            "nome": nomes_por_uid.get(uid, f"ID {uid}"),
            "vendas": mats,
            "leads": leads,
            "conversao": conv,
            "tier": tier,
            "meta_target": target,
            "progresso": progresso,
        })

    rows.sort(key=lambda r: (-r["vendas"], -r["conversao"]))
    for i, r in enumerate(rows, start=1):
        r["posicao"] = i

    return rows[:top_n]


# ── totais agregados ────────────────────────────────────────────────────────

def _totais(ranking: list[dict[str, Any]], dt_ini: date, dt_fim: date) -> dict[str, Any]:
    total_vendas = sum(r["vendas"] for r in ranking)
    n_analistas = sum(1 for r in ranking if r["vendas"] > 0)
    media = round(total_vendas / n_analistas, 1) if n_analistas else 0.0

    meta_global_target = 0
    for r in ranking:
        meta_global_target += r.get("meta_target") or 0
    meta_global_pct = (
        min(100, round(total_vendas / meta_global_target * 100)) if meta_global_target else None
    )

    today = datetime.now(BRT).date()
    if dt_fim < today:
        status_campanha = "Encerrado"
    else:
        days_total = max(1, (dt_fim - dt_ini).days + 1)
        days_passed = max(1, (today - dt_ini).days + 1) if today >= dt_ini else 0
        if not days_passed:
            status_campanha = "Aguardando"
        elif meta_global_pct is None:
            status_campanha = "Em andamento"
        elif meta_global_pct >= 100:
            status_campanha = "Meta batida"
        elif meta_global_pct >= 75:
            status_campanha = "Aceleração"
        elif meta_global_pct >= 40:
            status_campanha = "Construção"
        else:
            status_campanha = "Início"

    return {
        "total_vendas": total_vendas,
        "media_analista": media,
        "n_analistas": n_analistas,
        "meta_global_pct": meta_global_pct,
        "status_campanha": status_campanha,
    }


# ── endpoint ────────────────────────────────────────────────────────────────

@supervisor_dashboard_bp.route("/api/dashboard/supervisor")
def api_dashboard_supervisor():
    today = datetime.now(BRT).date()

    qs_ini = _parse_date(request.args.get("dt_ini"))
    qs_fim = _parse_date(request.args.get("dt_fim"))

    campanha = _get_active_campanha_lite()

    # Default: usa o intervalo da campanha mais recente cadastrada.
    # Quando o usuário passa filtros explícitos, eles têm prioridade.
    if qs_ini and qs_fim:
        dt_ini, dt_fim = qs_ini, qs_fim
    elif campanha:
        camp_ini = campanha.get("dt_inicio")
        camp_fim = campanha.get("dt_fim")
        # normaliza pra date
        if hasattr(camp_ini, "date"):
            camp_ini = camp_ini.date()
        if hasattr(camp_fim, "date"):
            camp_fim = camp_fim.date()
        dt_ini = qs_ini or camp_ini or today.replace(day=1)
        dt_fim = qs_fim or camp_fim or today
    else:
        dt_ini = qs_ini or today.replace(day=1)
        dt_fim = qs_fim or today

    if dt_fim < dt_ini:
        dt_ini, dt_fim = dt_fim, dt_ini

    try:
        kpis = {
            "novos_leads": _kpi_novos_leads(dt_ini, dt_fim),
            "vendas": _kpi_vendas(dt_ini, dt_fim),
            "aceites_pendentes": _kpi_aceites_pendentes(),
            "leads_parados": _kpi_leads_parados(threshold_hours=24),
        }
        ranking = _ranking_consultores(dt_ini, dt_fim, top_n=10)
        totais = _totais(ranking, dt_ini, dt_fim)
        perf = _build_perf_diaria(dt_ini, dt_fim)
        medias = _medias(dt_ini, dt_fim)

        return jsonify({
            "ok": True,
            "periodo": {
                "dt_ini": dt_ini.isoformat(),
                "dt_fim": dt_fim.isoformat(),
                "label": _format_periodo(dt_ini, dt_fim),
            },
            "kpis": kpis,
            "performance_diaria": perf,
            "medias": medias,
            "ranking": ranking,
            "totais": totais,
            "campanha": ({
                "id": campanha["id"],
                "nome": campanha["nome"],
                "dt_inicio": campanha["dt_inicio"].isoformat() if hasattr(campanha["dt_inicio"], "isoformat") else str(campanha["dt_inicio"]),
                "dt_fim": campanha["dt_fim"].isoformat() if hasattr(campanha["dt_fim"], "isoformat") else str(campanha["dt_fim"]),
            } if campanha else None),
        })
    except Exception as e:
        logger.exception("supervisor dashboard error")
        return jsonify({"ok": False, "error": str(e)}), 500


def _format_periodo(dt_ini: date, dt_fim: date) -> str:
    meses_pt = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"]
    a = f"{dt_ini.day:02d} {meses_pt[dt_ini.month - 1]}, {dt_ini.year}"
    b = f"{dt_fim.day:02d} {meses_pt[dt_fim.month - 1]}, {dt_fim.year}"
    return f"{a} — {b}"
