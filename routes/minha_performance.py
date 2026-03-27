"""
eduit. — Minha Performance / Premiação.

Página individual do agente comercial + painel admin.

Endpoints (agente):
  GET  /api/minha-performance              dados do agente
  GET  /api/minha-performance/premiacao     cálculo de premiação
  GET  /api/minha-performance/historico     períodos anteriores
  GET  /api/minha-performance/insights      livro motivacional

Endpoints (admin):
  GET  /api/minha-performance/agentes       lista de agentes
  CRUD /api/premiacao/campanhas             campanhas
  CRUD /api/premiacao/campanhas/<id>/grupos grupos de agentes
  CRUD /api/premiacao/grupos/<id>           editar/deletar grupo
  GET|POST /api/premiacao/campanhas/<id>/diarias-grupo  metas por grupo
  GET|POST /api/premiacao/campanhas/<id>/diarias        metas legacy
  POST /api/premiacao/campanhas/<id>/diarias/auto       auto-calc
  POST /api/recebimentos/upload             upload CSV
  GET  /api/recebimentos                    listar snapshots
"""

import os
import io
import csv
import re
import logging
from collections import defaultdict
from datetime import datetime, date, timedelta

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, session

logger = logging.getLogger(__name__)

minha_performance_bp = Blueprint("minha_performance", __name__)

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)

KOMMO_DB_DSN = dict(
    host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
    port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
    user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
    password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
    dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
)


def _pg():
    return psycopg2.connect(**DB_DSN)


def _pg_kommo():
    return psycopg2.connect(**KOMMO_DB_DSN)


def _normalize_rgm(val):
    if not val:
        return None
    digits = re.sub(r"\D", "", str(val))
    if not digits:
        return None
    try:
        return str(int(digits))
    except Exception:
        return None


def _is_admin():
    return session.get("role") == "admin"


def _get_kommo_uid():
    """Return kommo_user_id for the current session user."""
    uid = session.get("user_id", 0)
    if not uid or uid == 0:
        return None
    try:
        conn = _pg()
        with conn.cursor() as cur:
            cur.execute("SELECT kommo_user_id FROM app_users WHERE id = %s", (uid,))
            row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def _resolve_kommo_uid(args_uid=None):
    """Resolve which kommo_user_id to use. Admin can pass ?kommo_uid=X."""
    if _is_admin() and args_uid:
        try:
            return int(args_uid)
        except (ValueError, TypeError):
            pass
    return _get_kommo_uid()


def _get_agent_matriculas(kommo_uid, dt_ini=None, dt_fim=None):
    """Get matriculas for a specific agent by crossing comercial_rgm_atual with Kommo."""
    try:
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT DISTINCT v.rgm
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            WHERE l.responsible_user_id = %s
        """, (kommo_uid,))
        agent_rgms = set()
        for row in kcur.fetchall():
            n = _normalize_rgm(row[0])
            if n:
                agent_rgms.add(n)
        kcur.close()
        kconn.close()
    except Exception as e:
        logger.warning("Error fetching agent RGMs from Kommo: %s", e)
        return []

    if not agent_rgms:
        return []

    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cw, cp = [], []
        if dt_ini:
            cw.append("data_matricula >= %s")
            cp.append(dt_ini)
        if dt_fim:
            cw.append("data_matricula <= %s")
            cp.append(dt_fim)
        w = ("WHERE " + " AND ".join(cw)) if cw else ""
        cur.execute(
            f"SELECT rgm, nome, nivel, modalidade, polo, data_matricula, turma, ciclo FROM comercial_rgm_atual {w} ORDER BY data_matricula DESC NULLS LAST",
            cp,
        )
        results = []
        seen = set()
        for row in cur.fetchall():
            n = _normalize_rgm(row["rgm"])
            if n and n in agent_rgms and n not in seen:
                seen.add(n)
                results.append(dict(row))
        cur.close()
        conn.close()
        return results
    except Exception as e:
        logger.warning("Error fetching agent matriculas: %s", e)
        return []


def _get_agent_metas(kommo_uid, dt_ini=None, dt_fim=None):
    """Get metas for an agent. Tries premiacao_campanha_meta first, falls back to comercial_metas."""
    try:
        conn = _pg()
        cur = conn.cursor()

        cur.execute("""
            SELECT pcm.meta, pcm.meta_intermediaria, pcm.supermeta
            FROM premiacao_campanha_meta pcm
            JOIN premiacao_campanha pc ON pc.id = pcm.campanha_id
            WHERE pcm.kommo_user_id = %s AND pc.dt_inicio <= %s AND pc.dt_fim >= %s
            LIMIT 1
        """, (kommo_uid, dt_fim or '9999-12-31', dt_ini or '1900-01-01'))
        row = cur.fetchone()
        if row:
            cur.close()
            conn.close()
            return {"meta": float(row[0]), "intermediaria": float(row[1]), "supermeta": float(row[2])}

        cur.execute("""
            SELECT meta, COALESCE(meta_intermediaria,0), COALESCE(supermeta,0), categoria
            FROM comercial_metas
            WHERE user_id = %s AND dt_inicio <= %s AND dt_fim >= %s
        """, (kommo_uid, dt_fim or '9999-12-31', dt_ini or '1900-01-01'))
        result = {"meta": 0, "intermediaria": 0, "supermeta": 0}
        for r in cur.fetchall():
            cat = r[3] or "matriculas"
            if cat == "matriculas":
                result["meta"] += float(r[0])
                result["intermediaria"] += float(r[1])
                result["supermeta"] += float(r[2])
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.warning("Error fetching agent metas: %s", e)
        return {"meta": 0, "intermediaria": 0, "supermeta": 0}


def _get_active_campanha(dt=None):
    """Return the active campaign covering the given date (or today)."""
    ref = dt or date.today()
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM premiacao_campanha
            WHERE ativa = TRUE AND dt_inicio <= %s AND dt_fim >= %s
            ORDER BY dt_inicio DESC LIMIT 1
        """, (ref, ref))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def _get_tier_bonuses(campanha_id):
    """Return tier bonus config for a campaign: {tier: valor_por_mat}."""
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT tier, valor_por_mat FROM premiacao_tier_bonus WHERE campanha_id = %s", (campanha_id,))
        result = {}
        for r in cur.fetchall():
            result[r[0]] = float(r[1])
        cur.close()
        conn.close()
        return result
    except Exception:
        return {}


def _get_daily_config(campanha_id, kommo_uid):
    """Return daily targets/bonuses for an agent via grupo membership.

    Resolution order:
    1. Find the grupo the agent belongs to in this campaign
    2. Fetch meta_diaria rows for that grupo_id
    3. Fallback: legacy rows with kommo_user_id (no grupo_id)
    """
    try:
        conn = _pg()
        cur = conn.cursor()

        cur.execute("""
            SELECT g.id FROM premiacao_grupo g
            JOIN premiacao_grupo_membro gm ON gm.grupo_id = g.id
            WHERE g.campanha_id = %s AND gm.kommo_user_id = %s
            LIMIT 1
        """, (campanha_id, kommo_uid))
        row = cur.fetchone()
        grupo_id = row[0] if row else None

        result = {}
        if grupo_id:
            cur.execute("""
                SELECT dia_semana, meta_diaria, bonus_fixo, bonus_extra
                FROM premiacao_meta_diaria
                WHERE campanha_id = %s AND grupo_id = %s
            """, (campanha_id, grupo_id))
            for r in cur.fetchall():
                result[r[0]] = {"meta": r[1], "fixo": float(r[2]), "extra": float(r[3])}

        if not result:
            cur.execute("""
                SELECT dia_semana, meta_diaria, bonus_fixo, bonus_extra
                FROM premiacao_meta_diaria
                WHERE campanha_id = %s AND kommo_user_id = %s AND grupo_id IS NULL
            """, (campanha_id, kommo_uid))
            for r in cur.fetchall():
                result[r[0]] = {"meta": r[1], "fixo": float(r[2]), "extra": float(r[3])}

        cur.close()
        conn.close()
        return result
    except Exception:
        return {}


def _calc_daily_premiacao(matriculas, daily_config, dt_ini, dt_fim):
    """Calculate daily bonus breakdown.

    Returns list of {data, dia_semana, meta, realizadas, bonus_fixo, bonus_extra, total}
    and accumulated total.
    """
    DIA_NAMES = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]

    mat_by_date = defaultdict(int)
    for m in matriculas:
        dm = m.get("data_matricula")
        if dm:
            if isinstance(dm, str):
                try:
                    dm = datetime.strptime(dm[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
            mat_by_date[dm] += 1

    d_ini = datetime.strptime(dt_ini, "%Y-%m-%d").date() if isinstance(dt_ini, str) else dt_ini
    d_fim = datetime.strptime(dt_fim, "%Y-%m-%d").date() if isinstance(dt_fim, str) else dt_fim
    today = date.today()
    if d_fim > today:
        d_fim = today

    breakdown = []
    total_bonus = 0.0
    dias_batidos = 0
    dias_total = 0

    d = d_ini
    while d <= d_fim:
        dow = d.weekday()  # 0=Mon, 6=Sun
        cfg = daily_config.get(dow, {})
        meta = cfg.get("meta", 0)
        fixo = cfg.get("fixo", 0)
        extra = cfg.get("extra", 0)

        if meta > 0:
            dias_total += 1
            realizadas = mat_by_date.get(d, 0)
            b_fixo = fixo if realizadas >= meta else 0.0
            b_extra = extra * max(0, realizadas - meta) if realizadas > meta else 0.0
            total_dia = b_fixo + b_extra
            if realizadas >= meta:
                dias_batidos += 1
            total_bonus += total_dia
            breakdown.append({
                "data": d.isoformat(),
                "dia_semana": dow,
                "dia_nome": DIA_NAMES[dow],
                "meta": meta,
                "realizadas": realizadas,
                "bonus_fixo": b_fixo,
                "bonus_extra": b_extra,
                "total": total_dia,
            })
        d += timedelta(days=1)

    return breakdown, total_bonus, dias_batidos, dias_total


def _determine_tier(total_mat, metas):
    """Determine which tier the agent reached."""
    sup = metas.get("supermeta", 0)
    met = metas.get("meta", 0)
    inter = metas.get("intermediaria", 0)
    if sup > 0 and total_mat >= sup:
        return "supermeta"
    if met > 0 and total_mat >= met:
        return "meta"
    if inter > 0 and total_mat >= inter:
        return "intermediaria"
    return None


# ---------------------------------------------------------------------------
# API: Dados do agente
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/minha-performance")
def api_minha_performance():
    kommo_uid = _resolve_kommo_uid(request.args.get("kommo_uid"))
    if not kommo_uid:
        return jsonify({"ok": False, "error": "Agente não vinculado ao Kommo"}), 400

    campanha = _get_active_campanha()
    if not campanha:
        return jsonify({
            "ok": True,
            "campanha": None,
            "matriculas": [],
            "total": 0,
            "metas": {"meta": 0, "intermediaria": 0, "supermeta": 0},
            "tier": None,
            "pct": 0,
        })

    dt_ini = str(campanha["dt_inicio"])
    dt_fim = str(campanha["dt_fim"])

    matriculas = _get_agent_matriculas(kommo_uid, dt_ini, dt_fim)
    total = len(matriculas)
    metas = _get_agent_metas(kommo_uid, dt_ini, dt_fim)
    tier = _determine_tier(total, metas)
    meta_val = metas.get("meta", 0)
    pct = round(total / meta_val * 100, 1) if meta_val > 0 else 0

    for m in matriculas:
        if m.get("data_matricula") and hasattr(m["data_matricula"], "isoformat"):
            m["data_matricula"] = m["data_matricula"].isoformat()

    agent_name = ""
    try:
        kc = _pg_kommo()
        kcu = kc.cursor()
        kcu.execute("SELECT name FROM users WHERE id = %s", (kommo_uid,))
        r = kcu.fetchone()
        if r:
            agent_name = r[0] or ""
        kcu.close()
        kc.close()
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "agent_name": agent_name,
        "kommo_uid": kommo_uid,
        "campanha": {
            "id": campanha["id"],
            "nome": campanha["nome"],
            "dt_inicio": str(campanha["dt_inicio"]),
            "dt_fim": str(campanha["dt_fim"]),
        },
        "matriculas": matriculas,
        "total": total,
        "metas": metas,
        "tier": tier,
        "pct": pct,
        "falta_inter": max(0, metas["intermediaria"] - total) if metas["intermediaria"] > 0 else None,
        "falta_meta": max(0, metas["meta"] - total) if metas["meta"] > 0 else None,
        "falta_super": max(0, metas["supermeta"] - total) if metas["supermeta"] > 0 else None,
    })


@minha_performance_bp.route("/api/minha-performance/premiacao")
def api_minha_premiacao():
    kommo_uid = _resolve_kommo_uid(request.args.get("kommo_uid"))
    if not kommo_uid:
        return jsonify({"ok": False, "error": "Agente não vinculado"}), 400

    campanha = _get_active_campanha()
    if not campanha:
        return jsonify({"ok": True, "campanha": None, "tier_bonus": 0, "daily_bonus": 0,
                        "receb_bonus": 0, "total": 0, "breakdown": []})

    cid = campanha["id"]
    dt_ini = str(campanha["dt_inicio"])
    dt_fim = str(campanha["dt_fim"])

    matriculas = _get_agent_matriculas(kommo_uid, dt_ini, dt_fim)
    total_mat = len(matriculas)
    metas = _get_agent_metas(kommo_uid, dt_ini, dt_fim)
    tier = _determine_tier(total_mat, metas)

    # Tier bonus
    tier_bonuses = _get_tier_bonuses(cid)
    tier_valor = tier_bonuses.get(tier, 0) if tier else 0
    tier_bonus_total = tier_valor * total_mat

    # Daily bonus
    daily_config = _get_daily_config(cid, kommo_uid)
    breakdown, daily_bonus_total, dias_batidos, dias_total = _calc_daily_premiacao(
        matriculas, daily_config, dt_ini, dt_fim
    )

    # Recebimentos bonus
    receb_bonus_total = 0.0
    receb_total_valor = 0.0
    try:
        conn = _pg()
        cur = conn.cursor()
        # Get agent RGMs
        agent_rgms = {_normalize_rgm(m["rgm"]) for m in matriculas if m.get("rgm")}
        if agent_rgms:
            placeholders = ",".join(["%s"] * len(agent_rgms))
            cur.execute(
                f"SELECT COALESCE(SUM(valor), 0) FROM comercial_recebimentos WHERE rgm IN ({placeholders})",
                list(agent_rgms),
            )
            receb_total_valor = float(cur.fetchone()[0])

        # Apply recebimento rule
        cur.execute("""
            SELECT modo, valor, tier FROM premiacao_recebimento_regra
            WHERE campanha_id = %s
        """, (cid,))
        for modo, valor, regra_tier in cur.fetchall():
            applicable = regra_tier == "qualquer" or regra_tier == tier
            if applicable and receb_total_valor > 0:
                if modo == "percentual":
                    receb_bonus_total += receb_total_valor * float(valor) / 100.0
                else:
                    receb_bonus_total += float(valor)
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("Error calculating recebimentos bonus: %s", e)

    grand_total = tier_bonus_total + daily_bonus_total + receb_bonus_total

    return jsonify({
        "ok": True,
        "campanha": {"id": cid, "nome": campanha["nome"]},
        "total_matriculas": total_mat,
        "tier": tier,
        "tier_valor_por_mat": tier_valor,
        "tier_bonus": round(tier_bonus_total, 2),
        "daily_bonus": round(daily_bonus_total, 2),
        "daily_dias_batidos": dias_batidos,
        "daily_dias_total": dias_total,
        "daily_breakdown": breakdown,
        "receb_total_valor": round(receb_total_valor, 2),
        "receb_bonus": round(receb_bonus_total, 2),
        "total": round(grand_total, 2),
        "metas": metas,
    })


@minha_performance_bp.route("/api/minha-performance/historico")
def api_minha_historico():
    kommo_uid = _resolve_kommo_uid(request.args.get("kommo_uid"))
    if not kommo_uid:
        return jsonify({"ok": False, "error": "Agente não vinculado"}), 400

    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT pc.id, pc.nome, pc.dt_inicio, pc.dt_fim, pc.ativa
            FROM premiacao_campanha pc
            ORDER BY pc.dt_inicio DESC
            LIMIT 12
        """)
        campanhas = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception:
        campanhas = []

    history = []
    for c in campanhas:
        dt_ini = str(c["dt_inicio"])
        dt_fim = str(c["dt_fim"])
        matriculas = _get_agent_matriculas(kommo_uid, dt_ini, dt_fim)
        total = len(matriculas)
        metas = _get_agent_metas(kommo_uid, dt_ini, dt_fim)
        tier = _determine_tier(total, metas)

        tier_bonuses = _get_tier_bonuses(c["id"])
        tier_valor = tier_bonuses.get(tier, 0) if tier else 0
        tier_bonus = tier_valor * total

        daily_config = _get_daily_config(c["id"], kommo_uid)
        _, daily_bonus, dias_batidos, dias_total = _calc_daily_premiacao(
            matriculas, daily_config, dt_ini, dt_fim
        )

        history.append({
            "campanha_id": c["id"],
            "nome": c["nome"],
            "dt_inicio": dt_ini,
            "dt_fim": dt_fim,
            "ativa": c["ativa"],
            "total_matriculas": total,
            "metas": metas,
            "tier": tier,
            "tier_bonus": round(tier_bonus, 2),
            "daily_bonus": round(daily_bonus, 2),
            "dias_batidos": dias_batidos,
            "dias_total": dias_total,
            "total_premiacao": round(tier_bonus + daily_bonus, 2),
        })

    return jsonify({"ok": True, "historico": history})


@minha_performance_bp.route("/api/minha-performance/insights")
def api_minha_insights():
    """Calculated insights for the motivational book page."""
    kommo_uid = _resolve_kommo_uid(request.args.get("kommo_uid"))
    if not kommo_uid:
        return jsonify({"ok": False, "error": "Agente não vinculado"}), 400

    campanha = _get_active_campanha()
    if not campanha:
        return jsonify({"ok": True, "campanha": None})

    cid = campanha["id"]
    dt_ini_str = str(campanha["dt_inicio"])
    dt_fim_str = str(campanha["dt_fim"])
    dt_ini = datetime.strptime(dt_ini_str, "%Y-%m-%d").date()
    dt_fim = datetime.strptime(dt_fim_str, "%Y-%m-%d").date()
    today = date.today()

    matriculas = _get_agent_matriculas(kommo_uid, dt_ini_str, dt_fim_str)
    total_mat = len(matriculas)
    metas = _get_agent_metas(kommo_uid, dt_ini_str, dt_fim_str)
    tier = _determine_tier(total_mat, metas)

    daily_config = _get_daily_config(cid, kommo_uid)

    def _count_work_days(start, end):
        count = 0
        d = start
        while d <= end:
            if daily_config.get(d.weekday(), {}).get("meta", 0) > 0:
                count += 1
            elif d.weekday() < 6:
                count += 1
            d += timedelta(days=1)
        return max(count, 1)

    effective_end = min(dt_fim, today)
    dias_passados = _count_work_days(dt_ini, effective_end)
    dias_uteis_total = _count_work_days(dt_ini, dt_fim)
    dias_uteis_restantes = max(0, _count_work_days(today + timedelta(days=1), dt_fim)) if today < dt_fim else 0
    dias_restantes = max(0, (dt_fim - today).days)

    pace_atual = round(total_mat / dias_passados, 2) if dias_passados > 0 else 0

    meta_val = metas.get("meta", 0)
    inter_val = metas.get("intermediaria", 0)
    super_val = metas.get("supermeta", 0)

    def _pace_needed(target):
        falta = max(0, target - total_mat)
        return round(falta / dias_uteis_restantes, 2) if dias_uteis_restantes > 0 else (0 if falta == 0 else 999)

    pace_meta = _pace_needed(meta_val)
    pace_inter = _pace_needed(inter_val)
    pace_super = _pace_needed(super_val)

    projecao = total_mat + round(pace_atual * dias_uteis_restantes)
    projecao_tier = _determine_tier(projecao, metas)

    # Today's challenge
    dow_today = today.weekday()
    today_cfg = daily_config.get(dow_today, {})
    today_meta = today_cfg.get("meta", 0)
    today_fixo = today_cfg.get("fixo", 0)
    today_extra = today_cfg.get("extra", 0)

    mat_by_date = defaultdict(int)
    for m in matriculas:
        dm = m.get("data_matricula")
        if dm:
            if isinstance(dm, str):
                try:
                    dm = datetime.strptime(dm[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
            mat_by_date[dm] += 1

    today_realizadas = mat_by_date.get(today, 0)

    # Streak: consecutive days hitting daily target
    sequencia = 0
    d = effective_end
    while d >= dt_ini:
        dcfg = daily_config.get(d.weekday(), {})
        dmeta = dcfg.get("meta", 0)
        if dmeta > 0:
            if mat_by_date.get(d, 0) >= dmeta:
                sequencia += 1
            else:
                break
        d -= timedelta(days=1)

    # Heatmap data: week-by-week daily breakdown
    heatmap = []
    d = dt_ini
    while d <= dt_fim:
        dcfg = daily_config.get(d.weekday(), {})
        dmeta = dcfg.get("meta", 0)
        realizadas = mat_by_date.get(d, 0) if d <= today else None
        status = "future"
        if d <= today:
            if dmeta > 0 and realizadas >= dmeta:
                status = "hit"
            elif realizadas and realizadas > 0:
                status = "partial"
            else:
                status = "miss"
        heatmap.append({
            "data": d.isoformat(),
            "dia_semana": d.weekday(),
            "meta": dmeta,
            "realizadas": realizadas,
            "status": status,
        })
        d += timedelta(days=1)

    # Best past campaign
    melhor_campanha = None
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, nome, dt_inicio, dt_fim FROM premiacao_campanha
            WHERE ativa = FALSE ORDER BY dt_inicio DESC LIMIT 12
        """)
        past = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        best_total = 0
        for pc in past:
            pmat = _get_agent_matriculas(kommo_uid, str(pc["dt_inicio"]), str(pc["dt_fim"]))
            pmetas = _get_agent_metas(kommo_uid, str(pc["dt_inicio"]), str(pc["dt_fim"]))
            ptier = _determine_tier(len(pmat), pmetas)
            if len(pmat) > best_total:
                best_total = len(pmat)
                melhor_campanha = {"nome": pc["nome"], "total": len(pmat), "tier": ptier}
    except Exception:
        pass

    # Tier bonus + daily bonus + receb
    tier_bonuses = _get_tier_bonuses(cid)
    tier_valor = tier_bonuses.get(tier, 0) if tier else 0
    tier_bonus_total = tier_valor * total_mat

    breakdown, daily_bonus_total, dias_batidos, dias_total = _calc_daily_premiacao(
        matriculas, daily_config, dt_ini_str, dt_fim_str
    )

    receb_bonus_total = 0.0
    receb_total_valor = 0.0
    try:
        conn = _pg()
        cur = conn.cursor()
        agent_rgms = {_normalize_rgm(m["rgm"]) for m in matriculas if m.get("rgm")}
        if agent_rgms:
            placeholders = ",".join(["%s"] * len(agent_rgms))
            cur.execute(
                f"SELECT COALESCE(SUM(valor), 0) FROM comercial_recebimentos WHERE rgm IN ({placeholders})",
                list(agent_rgms),
            )
            receb_total_valor = float(cur.fetchone()[0])
        cur.execute("SELECT modo, valor, tier FROM premiacao_recebimento_regra WHERE campanha_id = %s", (cid,))
        for modo, valor, regra_tier in cur.fetchall():
            applicable = regra_tier == "qualquer" or regra_tier == tier
            if applicable and receb_total_valor > 0:
                if modo == "percentual":
                    receb_bonus_total += receb_total_valor * float(valor) / 100.0
                else:
                    receb_bonus_total += float(valor)
        cur.close()
        conn.close()
    except Exception:
        pass

    total_acumulado = tier_bonus_total + daily_bonus_total + receb_bonus_total

    # Potenciais: what the agent WOULD earn at each tier with current matrículas
    potenciais = {}
    for t_name in ("intermediaria", "meta", "supermeta"):
        tv = tier_bonuses.get(t_name, 0)
        if tv > 0:
            potenciais[t_name] = {
                "valor_por_mat": tv,
                "total_tier": round(tv * total_mat, 2),
                "total_com_diaria": round(tv * total_mat + daily_bonus_total + receb_bonus_total, 2),
            }

    # "Desbloqueie mais": tiers not yet reached, showing what they'd gain
    desbloqueie = []
    tier_order = [("intermediaria", inter_val), ("meta", meta_val), ("supermeta", super_val)]
    for t_name, t_target in tier_order:
        if t_target <= 0:
            continue
        tv = tier_bonuses.get(t_name, 0)
        if tv <= 0:
            continue
        falta = max(0, t_target - total_mat)
        ganho_tier = tv * total_mat
        ganho_extra = ganho_tier - tier_bonus_total
        desbloqueie.append({
            "tier": t_name,
            "target": t_target,
            "falta": falta,
            "atingido": falta == 0,
            "valor_por_mat": tv,
            "ganho_total": round(ganho_tier, 2),
            "ganho_adicional": round(max(0, ganho_extra), 2),
        })

    # Projeção financeira
    proj_tier = _determine_tier(projecao, metas)
    proj_tier_valor = tier_bonuses.get(proj_tier, 0) if proj_tier else 0
    projecao_financeira = round(proj_tier_valor * projecao + daily_bonus_total + receb_bonus_total, 2)

    # Dynamic motivational message (money-centric)
    if super_val > 0 and total_mat >= super_val:
        mensagem = f"SUPERMETA! Cada nova matrícula = +R$ {tier_bonuses.get('supermeta', 0):.0f}. Continue!"
    elif meta_val > 0 and total_mat >= meta_val:
        falta_s = max(0, super_val - total_mat) if super_val > 0 else 0
        sv = tier_bonuses.get("supermeta", 0)
        ganho_extra = round((sv - tier_valor) * total_mat, 2) if sv > tier_valor else 0
        mensagem = f"Meta batida! Faltam {falta_s} para SUPERMETA (+R$ {ganho_extra:,.0f})."
    elif inter_val > 0 and total_mat >= inter_val:
        falta_m = max(0, meta_val - total_mat)
        mv = tier_bonuses.get("meta", 0)
        ganho_extra = round((mv - tier_valor) * total_mat, 2) if mv > tier_valor else 0
        mensagem = f"Faltam {falta_m} para a META e +R$ {ganho_extra:,.0f} no bolso!"
    elif inter_val > 0:
        falta_i = max(0, inter_val - total_mat)
        iv = tier_bonuses.get("intermediaria", 0)
        ganho = round(iv * total_mat, 2)
        mensagem = f"{falta_i} matrículas para a Intermediária e garantir R$ {ganho:,.0f}!"
    else:
        mensagem = "Campanha ativa! Cada matrícula conta."

    agent_name = ""
    try:
        kc = _pg_kommo()
        kcu = kc.cursor()
        kcu.execute("SELECT name FROM users WHERE id = %s", (kommo_uid,))
        r = kcu.fetchone()
        if r:
            agent_name = r[0] or ""
        kcu.close()
        kc.close()
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "agent_name": agent_name,
        "campanha": {
            "id": cid,
            "nome": campanha["nome"],
            "dt_inicio": dt_ini_str,
            "dt_fim": dt_fim_str,
        },
        "total_matriculas": total_mat,
        "metas": metas,
        "tier": tier,
        "pct": round(total_mat / meta_val * 100, 1) if meta_val > 0 else 0,
        "pace_atual": pace_atual,
        "pace_meta": pace_meta,
        "pace_inter": pace_inter,
        "pace_super": pace_super,
        "projecao": projecao,
        "projecao_tier": projecao_tier,
        "projecao_financeira": projecao_financeira,
        "dias_restantes": dias_restantes,
        "dias_uteis_restantes": dias_uteis_restantes,
        "hoje": {
            "dia_semana": dow_today,
            "meta": today_meta,
            "realizadas": today_realizadas,
            "bonus_fixo": today_fixo,
            "bonus_extra": today_extra,
        },
        "sequencia": sequencia,
        "heatmap": heatmap,
        "melhor_campanha": melhor_campanha,
        "mensagem": mensagem,
        "premiacao": {
            "tier_bonus": round(tier_bonus_total, 2),
            "tier_valor_por_mat": tier_valor,
            "daily_bonus": round(daily_bonus_total, 2),
            "daily_dias_batidos": dias_batidos,
            "daily_dias_total": dias_total,
            "daily_breakdown": breakdown,
            "receb_bonus": round(receb_bonus_total, 2),
            "receb_total_valor": round(receb_total_valor, 2),
            "total": round(total_acumulado, 2),
            "potenciais": potenciais,
            "desbloqueie": desbloqueie,
        },
        "matriculas": [{
            "rgm": m.get("rgm"),
            "nivel": m.get("nivel"),
            "modalidade": m.get("modalidade"),
            "data_matricula": m["data_matricula"].isoformat() if hasattr(m.get("data_matricula"), "isoformat") else m.get("data_matricula"),
        } for m in matriculas],
    })


@minha_performance_bp.route("/api/minha-performance/agentes")
def api_mp_agentes():
    """List agents (admin only) for the agent selector."""
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT DISTINCT u.id, u.name
            FROM users u
            JOIN leads l ON l.responsible_user_id = u.id AND NOT l.is_deleted
            WHERE u.name IS NOT NULL AND u.name != ''
            ORDER BY u.name
        """)
        agents = [{"kommo_uid": r[0], "name": r[1]} for r in kcur.fetchall()]
        kcur.close()
        kconn.close()
        return jsonify({"ok": True, "agentes": agents})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Campanhas (admin)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanhas", methods=["GET"])
def api_campanhas_list():
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM premiacao_campanha ORDER BY dt_inicio DESC")
        campanhas = []
        for c in cur.fetchall():
            c = dict(c)
            c["dt_inicio"] = str(c["dt_inicio"])
            c["dt_fim"] = str(c["dt_fim"])
            cur.execute("SELECT tier, valor_por_mat FROM premiacao_tier_bonus WHERE campanha_id = %s", (c["id"],))
            c["tiers"] = {r["tier"]: float(r["valor_por_mat"]) for r in cur.fetchall()}
            cur.execute("SELECT tier, modo, valor FROM premiacao_recebimento_regra WHERE campanha_id = %s", (c["id"],))
            c["receb_regras"] = [dict(r) for r in cur.fetchall()]
            campanhas.append(c)
        cur.close()
        conn.close()
        return jsonify({"ok": True, "campanhas": campanhas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas", methods=["POST"])
def api_campanhas_create():
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    nome = body.get("nome", "").strip()
    dt_inicio = body.get("dt_inicio")
    dt_fim = body.get("dt_fim")
    tiers = body.get("tiers", {})
    receb_regras = body.get("receb_regras", [])
    if not nome or not dt_inicio or not dt_fim:
        return jsonify({"error": "Nome e datas são obrigatórios"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO premiacao_campanha (nome, dt_inicio, dt_fim) VALUES (%s, %s, %s) RETURNING id",
            (nome, dt_inicio, dt_fim),
        )
        cid = cur.fetchone()[0]
        for tier, valor in tiers.items():
            if tier in ("intermediaria", "meta", "supermeta") and float(valor) > 0:
                cur.execute(
                    "INSERT INTO premiacao_tier_bonus (campanha_id, tier, valor_por_mat) VALUES (%s, %s, %s)",
                    (cid, tier, float(valor)),
                )
        for rr in receb_regras:
            cur.execute(
                "INSERT INTO premiacao_recebimento_regra (campanha_id, tier, modo, valor) VALUES (%s, %s, %s, %s)",
                (cid, rr.get("tier", "qualquer"), rr.get("modo", "percentual"), float(rr.get("valor", 0))),
            )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "id": cid})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>", methods=["PUT"])
def api_campanhas_update(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    try:
        conn = _pg()
        cur = conn.cursor()
        sets, vals = [], []
        for col in ("nome", "dt_inicio", "dt_fim"):
            if col in body:
                sets.append(f"{col} = %s")
                vals.append(body[col])
        if "ativa" in body:
            sets.append("ativa = %s")
            vals.append(bool(body["ativa"]))
        if sets:
            vals.append(cid)
            cur.execute(f"UPDATE premiacao_campanha SET {', '.join(sets)} WHERE id = %s", vals)
        if "tiers" in body:
            cur.execute("DELETE FROM premiacao_tier_bonus WHERE campanha_id = %s", (cid,))
            for tier, valor in body["tiers"].items():
                if tier in ("intermediaria", "meta", "supermeta") and float(valor) > 0:
                    cur.execute(
                        "INSERT INTO premiacao_tier_bonus (campanha_id, tier, valor_por_mat) VALUES (%s, %s, %s)",
                        (cid, tier, float(valor)),
                    )
        if "receb_regras" in body:
            cur.execute("DELETE FROM premiacao_recebimento_regra WHERE campanha_id = %s", (cid,))
            for rr in body["receb_regras"]:
                cur.execute(
                    "INSERT INTO premiacao_recebimento_regra (campanha_id, tier, modo, valor) VALUES (%s, %s, %s, %s)",
                    (cid, rr.get("tier", "qualquer"), rr.get("modo", "percentual"), float(rr.get("valor", 0))),
                )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>", methods=["DELETE"])
def api_campanhas_delete(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM premiacao_campanha WHERE id = %s", (cid,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Grupos de agentes (admin)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/grupos", methods=["GET"])
def api_grupos_list(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, nome FROM premiacao_grupo WHERE campanha_id = %s ORDER BY nome", (cid,))
        grupos = []
        for g in cur.fetchall():
            g = dict(g)
            cur.execute("""
                SELECT gm.kommo_user_id FROM premiacao_grupo_membro gm
                WHERE gm.grupo_id = %s ORDER BY gm.kommo_user_id
            """, (g["id"],))
            g["membros"] = [r["kommo_user_id"] for r in cur.fetchall()]
            grupos.append(g)
        cur.close()
        conn.close()
        return jsonify({"ok": True, "grupos": grupos})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/grupos", methods=["POST"])
def api_grupos_create(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    nome = body.get("nome", "").strip()
    membros = body.get("membros", [])
    if not nome:
        return jsonify({"error": "Nome do grupo é obrigatório"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO premiacao_grupo (campanha_id, nome) VALUES (%s, %s) RETURNING id",
            (cid, nome),
        )
        gid = cur.fetchone()[0]
        for uid in membros:
            cur.execute(
                "INSERT INTO premiacao_grupo_membro (grupo_id, kommo_user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (gid, int(uid)),
            )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "id": gid})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/grupos/<int:gid>", methods=["PUT"])
def api_grupos_update(gid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    try:
        conn = _pg()
        cur = conn.cursor()
        if "nome" in body:
            cur.execute("UPDATE premiacao_grupo SET nome = %s WHERE id = %s", (body["nome"].strip(), gid))
        if "membros" in body:
            cur.execute("DELETE FROM premiacao_grupo_membro WHERE grupo_id = %s", (gid,))
            for uid in body["membros"]:
                cur.execute(
                    "INSERT INTO premiacao_grupo_membro (grupo_id, kommo_user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (gid, int(uid)),
                )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/grupos/<int:gid>", methods=["DELETE"])
def api_grupos_delete(gid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM premiacao_meta_diaria WHERE grupo_id = %s", (gid,))
        cur.execute("DELETE FROM premiacao_grupo WHERE id = %s", (gid,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Metas por agente na campanha (admin)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/metas", methods=["GET"])
def api_campanha_metas_get(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT kommo_user_id, meta, meta_intermediaria, supermeta
            FROM premiacao_campanha_meta WHERE campanha_id = %s
            ORDER BY kommo_user_id
        """, (cid,))
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["meta"] = float(r["meta"])
            r["meta_intermediaria"] = float(r["meta_intermediaria"])
            r["supermeta"] = float(r["supermeta"])
        cur.close()
        conn.close()
        return jsonify({"ok": True, "metas": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/metas", methods=["POST"])
def api_campanha_metas_save(cid):
    """Save per-agent metas for a campaign. Replaces existing metas."""
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    items = body.get("metas", [])
    if not items:
        return jsonify({"error": "Nenhuma meta enviada"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM premiacao_campanha_meta WHERE campanha_id = %s", (cid,))
        count = 0
        for item in items:
            uid = int(item.get("kommo_user_id", 0))
            meta = float(item.get("meta", 0))
            inter = float(item.get("meta_intermediaria", 0))
            sup = float(item.get("supermeta", 0))
            if uid and (meta > 0 or inter > 0 or sup > 0):
                cur.execute("""
                    INSERT INTO premiacao_campanha_meta (campanha_id, kommo_user_id, meta, meta_intermediaria, supermeta)
                    VALUES (%s, %s, %s, %s, %s)
                """, (cid, uid, meta, inter, sup))
                count += 1
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": count})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Metas diárias por grupo (admin)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/diarias-grupo", methods=["GET"])
def api_diarias_grupo_get(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT grupo_id, dia_semana, meta_diaria, bonus_fixo, bonus_extra
            FROM premiacao_meta_diaria
            WHERE campanha_id = %s AND grupo_id IS NOT NULL
            ORDER BY grupo_id, dia_semana
        """, (cid,))
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["bonus_fixo"] = float(r["bonus_fixo"])
            r["bonus_extra"] = float(r["bonus_extra"])
        cur.close()
        conn.close()
        return jsonify({"ok": True, "diarias": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/diarias-grupo", methods=["POST"])
def api_diarias_grupo_save(cid):
    """Save daily metas for groups. Replaces all grupo-based rows for this campaign."""
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    items = body.get("items", [])
    if not items:
        return jsonify({"error": "Nenhum item enviado"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM premiacao_meta_diaria WHERE campanha_id = %s AND grupo_id IS NOT NULL", (cid,))
        for item in items:
            gid = int(item["grupo_id"])
            dow = int(item["dia_semana"])
            meta = int(item.get("meta_diaria", 0))
            fixo = float(item.get("bonus_fixo", 0))
            extra = float(item.get("bonus_extra", 0))
            if meta > 0 or fixo > 0 or extra > 0:
                cur.execute("""
                    INSERT INTO premiacao_meta_diaria (campanha_id, grupo_id, kommo_user_id, dia_semana, meta_diaria, bonus_fixo, bonus_extra)
                    VALUES (%s, %s, 0, %s, %s, %s, %s)
                """, (cid, gid, dow, meta, fixo, extra))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Metas diárias legacy (admin)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/diarias", methods=["GET"])
def api_diarias_get(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT kommo_user_id, dia_semana, meta_diaria, bonus_fixo, bonus_extra
            FROM premiacao_meta_diaria WHERE campanha_id = %s
            ORDER BY kommo_user_id, dia_semana
        """, (cid,))
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["bonus_fixo"] = float(r["bonus_fixo"])
            r["bonus_extra"] = float(r["bonus_extra"])
        cur.close()
        conn.close()
        return jsonify({"ok": True, "diarias": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/diarias/auto", methods=["POST"])
def api_diarias_auto_calc(cid):
    """Auto-calculate daily targets based on each agent's meta and working days.

    Mon-Fri = 1.0 effective day, Sat = 0.5, Sun = 0.
    Daily target = ceil(meta / effective_days) for weekdays,
                   ceil(meta / effective_days * 0.5) for Saturday.
    """
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403

    import math

    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM premiacao_campanha WHERE id = %s", (cid,))
        camp = cur.fetchone()
        if not camp:
            cur.close()
            conn.close()
            return jsonify({"ok": False, "error": "Campanha não encontrada"}), 404

        dt_ini = camp["dt_inicio"]
        dt_fim = camp["dt_fim"]

        weekdays = 0
        saturdays = 0
        d = dt_ini
        while d <= dt_fim:
            if d.weekday() < 5:
                weekdays += 1
            elif d.weekday() == 5:
                saturdays += 1
            d += timedelta(days=1)

        effective_days = weekdays + saturdays * 0.5
        if effective_days <= 0:
            cur.close()
            conn.close()
            return jsonify({"ok": False, "error": "Período sem dias úteis"}), 400

        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT DISTINCT u.id, u.name
            FROM users u
            JOIN leads l ON l.responsible_user_id = u.id AND NOT l.is_deleted
            WHERE u.name IS NOT NULL AND u.name != ''
            ORDER BY u.name
        """)
        agents = [{"kommo_uid": r[0], "name": r[1]} for r in kcur.fetchall()]
        kcur.close()
        kconn.close()

        calculated = []
        agents_with_meta = 0

        for agent in agents:
            uid = agent["kommo_uid"]
            metas = _get_agent_metas(uid, str(dt_ini), str(dt_fim))
            meta_val = metas.get("meta", 0)
            if meta_val <= 0:
                continue

            agents_with_meta += 1
            daily_rate = meta_val / effective_days

            for dow in range(7):
                if dow < 5:
                    target = math.ceil(daily_rate)
                elif dow == 5:
                    target = math.ceil(daily_rate * 0.5)
                else:
                    target = 0

                if target > 0:
                    calculated.append({
                        "kommo_user_id": uid,
                        "dia_semana": dow,
                        "meta_diaria": target,
                    })

        cur.close()
        conn.close()

        return jsonify({
            "ok": True,
            "agents_count": agents_with_meta,
            "effective_days": effective_days,
            "weekdays": weekdays,
            "saturdays": saturdays,
            "calculated": calculated,
        })
    except Exception as e:
        logger.error("Auto-calc daily error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/diarias", methods=["POST"])
def api_diarias_save(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    items = body.get("items", [])
    if not items:
        return jsonify({"error": "Nenhum item enviado"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        for item in items:
            kuid = int(item["kommo_user_id"])
            dow = int(item["dia_semana"])
            meta = int(item.get("meta_diaria", 0))
            fixo = float(item.get("bonus_fixo", 0))
            extra = float(item.get("bonus_extra", 0))
            cur.execute("""
                INSERT INTO premiacao_meta_diaria (campanha_id, kommo_user_id, dia_semana, meta_diaria, bonus_fixo, bonus_extra)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (campanha_id, kommo_user_id, dia_semana) DO UPDATE SET
                    meta_diaria = EXCLUDED.meta_diaria,
                    bonus_fixo = EXCLUDED.bonus_fixo,
                    bonus_extra = EXCLUDED.bonus_extra
            """, (cid, kuid, dow, meta, fixo, extra))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Recebimentos upload (admin)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/recebimentos/upload", methods=["POST"])
def api_recebimentos_upload():
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    f = request.files.get("file")
    mes_ref = request.form.get("mes_ref", "")
    if not f:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    try:
        raw = f.read()
        for enc in ("utf-8-sig", "latin-1", "cp1252"):
            try:
                text = raw.decode(enc)
                break
            except Exception:
                continue
        else:
            text = raw.decode("utf-8", errors="replace")

        reader = csv.DictReader(io.StringIO(text), delimiter=";")
        if not reader.fieldnames:
            reader = csv.DictReader(io.StringIO(text), delimiter=",")

        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO recebimentos_snapshots (filename, mes_ref) VALUES (%s, %s) RETURNING id",
            (f.filename, mes_ref),
        )
        snap_id = cur.fetchone()[0]

        count = 0
        for row in reader:
            rgm = None
            valor = 0
            for k, v in row.items():
                if not k:
                    continue
                kl = k.strip().lower()
                if "rgm" in kl:
                    rgm = _normalize_rgm(v)
                elif "valor" in kl and "receb" in kl:
                    try:
                        valor = float(str(v).replace("R$", "").replace(".", "").replace(",", ".").strip())
                    except Exception:
                        valor = 0
                elif kl == "valor":
                    try:
                        valor = float(str(v).replace("R$", "").replace(".", "").replace(",", ".").strip())
                    except Exception:
                        valor = 0
            if not rgm:
                continue

            nivel = ""
            modalidade = ""
            data_mat = None
            tipo_pag = ""
            turma = ""
            from psycopg2.extras import Json
            for k, v in row.items():
                if not k:
                    continue
                kl = k.strip().lower()
                if "nivel" in kl or "nível" in kl:
                    nivel = (v or "").strip()
                elif "modalidade" in kl:
                    modalidade = (v or "").strip()
                elif "matrícula" in kl or "matricula" in kl:
                    if "data" in kl or "dt" in kl:
                        try:
                            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
                                try:
                                    data_mat = datetime.strptime(v.strip(), fmt).date()
                                    break
                                except Exception:
                                    continue
                        except Exception:
                            pass
                elif "tipo" in kl and "pag" in kl:
                    tipo_pag = (v or "").strip()
                elif "turma" in kl:
                    turma = (v or "").strip()

            cur.execute("""
                INSERT INTO comercial_recebimentos
                    (snapshot_id, rgm, nivel, modalidade, data_matricula, valor, tipo_pagamento, mes_referencia, turma, data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (snap_id, rgm, nivel, modalidade, data_mat, valor, tipo_pag, mes_ref, turma, Json(row)))
            count += 1

        cur.execute("UPDATE recebimentos_snapshots SET row_count = %s WHERE id = %s", (count, snap_id))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "snapshot_id": snap_id, "rows": count})
    except Exception as e:
        logger.error("Recebimentos upload error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/recebimentos", methods=["GET"])
def api_recebimentos_list():
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM recebimentos_snapshots ORDER BY uploaded_at DESC LIMIT 20")
        snaps = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({"ok": True, "snapshots": snaps})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
