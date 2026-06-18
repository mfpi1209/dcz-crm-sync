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
from datetime import datetime, date, timedelta, timezone as _tz

BRT = _tz(timedelta(hours=-3))

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


_suporte_uids_cache = None


def _can_manage_premiacao():
    """Admin ou permissão à página Premiação."""
    if _is_admin():
        return True
    uid = session.get("user_id")
    if uid is None:
        return False
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM user_permissions WHERE user_id = %s AND page = %s",
            (uid, "premiacao_admin"),
        )
        ok = cur.fetchone() is not None
        cur.close()
        conn.close()
        return ok
    except Exception:
        return False


def _get_suporte_kommo_uids():
    """Kommo IDs de app_users com categoria Suporte Comercial (ou login do time)."""
    global _suporte_uids_cache
    if _suporte_uids_cache is not None:
        return _suporte_uids_cache
    try:
        from helpers import SUPORTE_COMERCIAL_LOGINS

        logins = list(SUPORTE_COMERCIAL_LOGINS)
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT kommo_user_id FROM app_users
            WHERE kommo_user_id IS NOT NULL AND kommo_user_id > 0
              AND (
                LOWER(TRIM(COALESCE(categoria, ''))) = 'suporte comercial'
                OR LOWER(TRIM(username)) = ANY(%s)
              )
        """, (logins,))
        _suporte_uids_cache = [int(r[0]) for r in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception:
        _suporte_uids_cache = []
    return _suporte_uids_cache


def _is_suporte_agent(kommo_uid):
    if kommo_uid is None:
        return False
    try:
        return int(kommo_uid) in _get_suporte_kommo_uids()
    except (TypeError, ValueError):
        return False


def _get_suporte_team_matriculas(dt_ini, dt_fim):
    """Legado: matrículas por agente Suporte. Preferir _aggregate_comercial_periodo_vendas."""
    uids = _get_suporte_kommo_uids()
    if not uids:
        return []
    seen = set()
    out = []
    for uid in uids:
        for m in _get_agent_matriculas(uid, dt_ini, dt_fim, only_em_curso=True):
            rgm = _normalize_rgm(m.get("rgm"))
            if rgm and rgm not in seen:
                seen.add(rgm)
                out.append(m)
    return out


def _aggregate_comercial_periodo_vendas(dt_ini, dt_fim):
    """KPI bruto + série EM CURSO — idêntico ao Dashboard Comercial."""
    empty = {
        "total": 0,
        "total_liquido": 0,
        "mat_by_date": {},
        "dias_com_venda": 1,
        "media_diaria": 0,
        "evolucao": [],
    }
    try:
        from routes.comercial_rgm import comercial_periodo_vendas_resumo
        r = comercial_periodo_vendas_resumo(dt_ini=dt_ini, dt_fim=dt_fim)
    except Exception as e:
        logger.exception("comercial_periodo_vendas_resumo falhou: %s", e)
        return empty
    return {
        "total": r.get("vendas", 0),
        "total_liquido": r.get("vendas_liquidas", 0),
        "mat_by_date": r.get("mat_by_date") or {},
        "dias_com_venda": r.get("dias") or 1,
        "media_diaria": r.get("media_diaria", 0),
        "evolucao": r.get("evolucao") or [],
    }


def _get_meta_suporte(campanha_id):
    try:
        from db import _ensure_suporte_tables
        _ensure_suporte_tables()
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT meta, meta_intermediaria, supermeta
            FROM premiacao_meta_suporte WHERE campanha_id = %s
        """, (campanha_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return {"meta": 0, "intermediaria": 0, "supermeta": 0}
        return {
            "meta": float(row[0] or 0),
            "intermediaria": float(row[1] or 0),
            "supermeta": float(row[2] or 0),
        }
    except Exception as e:
        logger.warning("meta suporte load: %s", e)
        return {"meta": 0, "intermediaria": 0, "supermeta": 0}


def _build_meta_equipe_suporte(campanha_id, dt_ini, dt_fim, viewer_kommo_uid=None, allow_empty_meta=False):
    """Meta unificada do Suporte: apenas um alvo (matrículas do time)."""
    sm = _get_meta_suporte(campanha_id)
    meta_val = float(sm.get("meta", 0) or 0)
    if meta_val <= 0 and not allow_empty_meta:
        return None
    vendas = _aggregate_comercial_periodo_vendas(dt_ini, dt_fim)
    team_total = vendas["total"]
    pct = round(team_total / meta_val * 100, 1) if meta_val > 0 else 0
    return {
        "meta": meta_val,
        "total_matriculas": team_total,
        "pct": pct,
        "meta_batida": meta_val > 0 and team_total >= meta_val,
        "agentes_count": len(_get_suporte_kommo_uids()),
        "matriculas_fonte": "comercial",
        "is_suporte": _is_suporte_agent(viewer_kommo_uid) or _suporte_equipe_requested(),
    }


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


def _suporte_equipe_requested():
    if request.args.get("suporte_equipe") in ("1", "true", "yes"):
        return True
    uid = request.args.get("kommo_uid")
    return uid is not None and str(uid).strip() == "suporte_equipe"


def _resolve_kommo_uid(args_uid=None):
    """Resolve which kommo_user_id to use. Admin / Premiação can pass ?kommo_uid=X."""
    if _suporte_equipe_requested():
        uids = _get_suporte_kommo_uids()
        if uids:
            return uids[0]
        if _is_admin() or _can_manage_premiacao():
            return _get_kommo_uid()
        return None
    can_pick = _is_admin() or _can_manage_premiacao()
    if can_pick and args_uid:
        try:
            return int(args_uid)
        except (ValueError, TypeError):
            pass
    return _get_kommo_uid()


def _use_suporte_painel(kommo_uid):
    if _suporte_equipe_requested():
        return True
    return _is_suporte_agent(kommo_uid)


def _load_conflito_overrides() -> dict:
    """Mapa rgm_normalizado -> kommo_user_id resolvido manualmente em
    'Vendas em Conflito' (Dashboard Comercial). Esses overrides DEVEM ser
    respeitados em qualquer relatório por agente, senão o mesmo RGM é
    contabilizado para mais de um consultor (ex.: lead Hugo + lead Bruno
    com a venda atribuída ao Bruno via conflito_resolucao).
    """
    out: dict[str, int] = {}
    try:
        conn = _pg()
        with conn.cursor() as cur:
            cur.execute("SELECT rgm, user_id FROM comercial_rgm_conflito_resolucao")
            for r_raw, uid in cur.fetchall():
                n = _normalize_rgm(r_raw)
                if n and uid:
                    out[n] = int(uid)
        conn.close()
    except Exception as e:
        logger.warning("conflito_resolucao override load: %s", e)
    return out


def _apply_conflito_overrides_to_agent_rgms(agent_rgms: set, kommo_uid) -> set:
    """Ajusta o conjunto de RGMs do agente conforme overrides:
       - Se override.user_id == kommo_uid → garante que o RGM aparece.
       - Se override.user_id != kommo_uid → remove o RGM (pertence a outro).
    Retorna o conjunto ajustado (mutação in-place também aplicada).
    """
    overrides = _load_conflito_overrides()
    if not overrides:
        return agent_rgms
    for rgm, owner_uid in overrides.items():
        if owner_uid == kommo_uid:
            agent_rgms.add(rgm)
        else:
            agent_rgms.discard(rgm)
    return agent_rgms


def _kommo_user_display_name(kommo_uid: int | None) -> str | None:
    """Nome exibido do consultor (app_users ou tabela users do kommo_sync)."""
    if not kommo_uid:
        return None
    try:
        conn = _pg()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT username FROM app_users WHERE kommo_user_id = %s LIMIT 1",
                (kommo_uid,),
            )
            row = cur.fetchone()
        conn.close()
        if row and row[0]:
            return str(row[0]).strip() or None
    except Exception as e:
        logger.warning("kommo_user_display_name app_users: %s", e)
    try:
        kconn = _pg_kommo()
        with kconn.cursor() as cur:
            cur.execute("SELECT name FROM users WHERE id = %s", (kommo_uid,))
            row = cur.fetchone()
        kconn.close()
        if row and row[0]:
            return str(row[0]).strip() or None
    except Exception as e:
        logger.warning("kommo_user_display_name kommo users: %s", e)
    return None


def _upsert_conflito_resolucao(cur, rgm, kommo_user_id, user_name: str | None = None) -> bool:
    """Credita RGM ao consultor (mesma tabela de Vendas em Conflito no Dashboard Comercial)."""
    rgm_n = _normalize_rgm(rgm)
    if not rgm_n or not kommo_user_id:
        return False
    nome = (user_name or "").strip() or _kommo_user_display_name(kommo_user_id) or f"User #{kommo_user_id}"
    cur.execute(
        """
        INSERT INTO comercial_rgm_conflito_resolucao (rgm, user_id, user_name, resolved_at, resolved_by)
        VALUES (%s, %s, %s, NOW(), 'ajuste_aprovado')
        ON CONFLICT (rgm) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            user_name = EXCLUDED.user_name,
            resolved_at = NOW(),
            resolved_by = EXCLUDED.resolved_by
        """,
        (rgm_n, int(kommo_user_id), nome),
    )
    return True


def _get_agent_matriculas(kommo_uid, dt_ini=None, dt_fim=None, only_em_curso=False):
    """Get matriculas for a specific agent from xl_rows.
    only_em_curso=True filters to situacao='EM CURSO' only (para contagens oficiais).
    only_em_curso=False retorna todas incluindo cancelados (para listagem informativa).
    """
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

    # Aplica overrides manuais de "Vendas em Conflito" — mesma lógica do
    # Dashboard Comercial. Sem isso, RGMs com lead em mais de um consultor
    # apareciam para todos eles.
    _apply_conflito_overrides_to_agent_rgms(agent_rgms, kommo_uid)

    if not agent_rgms:
        return []

    try:
        conn = _pg()
        cur = conn.cursor()
        outer_conds, params = [], []
        if only_em_curso:
            outer_conds.append("situacao = 'EM CURSO'")
        if dt_ini:
            outer_conds.append("data_matricula >= %s")
            params.append(dt_ini)
        if dt_fim:
            outer_conds.append("data_matricula <= %s")
            params.append(dt_fim)
        outer_where = ("WHERE " + " AND ".join(outer_conds)) if outer_conds else ""

        cur.execute(f"""
            SELECT rgm, nome, situacao, curso, data_matricula, polo, nivel, ciclo, modalidade, tipo_matricula
            FROM (
                SELECT DISTINCT ON (regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'))
                    regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g')  AS rgm,
                    NULLIF(TRIM(COALESCE(r.data->>'nome','')), '')                  AS nome,
                    UPPER(TRIM(COALESCE(r.data->>'situacao','')))                   AS situacao,
                    NULLIF(TRIM(COALESCE(r.data->>'curso','')), '')                 AS curso,
                    CASE
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                            THEN to_date(r.data->>'data_mat','DD/MM/YYYY')
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN (r.data->>'data_mat')::date
                        ELSE NULL
                    END AS data_matricula,
                    TRIM(regexp_replace(COALESCE(r.data->>'polo',''), '^[0-9]+\\s*[-]\\s*', '')) AS polo,
                    CASE
                        WHEN COALESCE(r.data->>'nivel','')   ~* 'p[oó]s'                                        THEN 'Pós-Graduação'
                        WHEN COALESCE(r.data->>'negocio','') ~* 'p[oó]s'                                        THEN 'Pós-Graduação'
                        WHEN COALESCE(r.data->>'curso','')   ~* '(mba|especializa|p.s.gradua|lato.sensu|stricto)' THEN 'Pós-Graduação'
                        ELSE 'Graduação'
                    END AS nivel,
                    NULLIF(TRIM(COALESCE(r.data->>'ciclo','')), '')                 AS ciclo,
                    NULLIF(TRIM(COALESCE(r.data->>'modalidade','')), '')            AS modalidade,
                    UPPER(TRIM(COALESCE(r.data->>'tipo_matricula','')))             AS tipo_matricula
                FROM xl_rows r
                JOIN xl_snapshots s ON s.id = r.snapshot_id
                WHERE s.id = (SELECT id FROM xl_snapshots WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1)
                  AND COALESCE(r.data->>'rgm','') ~ '[0-9]'
                  AND UPPER(TRIM(COALESCE(r.data->>'tipo_matricula','')))
                      = ANY(ARRAY['NOVA MATRICULA','RECOMPRA','RETORNO'])
                  AND TRIM(COALESCE(r.data->>'empresa','')) ~ '^(12|7) -'
                ORDER BY
                    regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'),
                    -- Em transferências internas o aluno aparece 2x: prioriza
                    -- a linha que ainda está EM CURSO sobre TRANSFERIDO/CANCELADO.
                    CASE
                        WHEN UPPER(TRIM(COALESCE(r.data->>'situacao',''))) = 'EM CURSO' THEN 0
                        WHEN UPPER(TRIM(COALESCE(r.data->>'situacao',''))) IN ('TRANCADO','SEM EVOLUCAO','SEM EVOLUÇÃO') THEN 1
                        ELSE 2
                    END,
                    -- Desempate: matrícula mais recente.
                    CASE
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                            THEN to_date(r.data->>'data_mat','DD/MM/YYYY')
                        WHEN (r.data->>'data_mat') ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                            THEN (r.data->>'data_mat')::date
                        ELSE NULL
                    END DESC NULLS LAST,
                    r.id DESC
            ) deduped
            {outer_where}
            ORDER BY data_matricula DESC NULLS LAST
        """, params)

        cols = [d[0] for d in cur.description]
        results = []
        seen = set()
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            n = _normalize_rgm(d.get("rgm"))
            if n and n in agent_rgms and n not in seen:
                seen.add(n)
                results.append(d)
        cur.close()
        conn.close()
        return results
    except Exception as e:
        logger.warning("Error fetching agent matriculas: %s", e)
        return []


def _get_agent_metas(kommo_uid, dt_ini=None, dt_fim=None):
    """Retorna metas do agente com cadeia de fallback de 4 passos:
    1. premiacao_campanha_meta (override individual — retorna imediatamente se encontrado)
    2. premiacao_grupo_meta (override por equipe — campo-a-campo)
    3. def_meta_* em premiacao_campanha (padrão da campanha)
    4. comercial_metas (fallback final)
    """
    try:
        conn = _pg()
        cur = conn.cursor()
        dt_fim_q = dt_fim or '9999-12-31'
        dt_ini_q = dt_ini or '1900-01-01'

        # PASSO 1: override individual por agente
        cur.execute("""
            SELECT pcm.meta, pcm.meta_intermediaria, pcm.supermeta
            FROM premiacao_campanha_meta pcm
            JOIN premiacao_campanha pc ON pc.id = pcm.campanha_id
            WHERE pcm.kommo_user_id = %s AND pc.dt_inicio <= %s AND pc.dt_fim >= %s
            LIMIT 1
        """, (kommo_uid, dt_fim_q, dt_ini_q))
        row = cur.fetchone()
        if row:
            cur.close()
            conn.close()
            return {"meta": float(row[0]), "intermediaria": float(row[1]), "supermeta": float(row[2])}

        # PASSO 4 (base): comercial_metas — usado como base para sobrescrição pelos passos 2-3
        cur.execute("""
            SELECT meta, COALESCE(meta_intermediaria,0), COALESCE(supermeta,0), categoria
            FROM comercial_metas
            WHERE user_id = %s AND dt_inicio <= %s AND dt_fim >= %s
        """, (kommo_uid, dt_fim_q, dt_ini_q))
        result = {"meta": 0, "intermediaria": 0, "supermeta": 0}
        for r in cur.fetchall():
            cat = r[3] or "matriculas"
            if cat == "matriculas":
                result["meta"] += float(r[0])
                result["intermediaria"] += float(r[1])
                result["supermeta"] += float(r[2])

        # PASSO 3: def_meta_* da campanha ativa no período (sobrescreve campos não-None)
        cur.execute("""
            SELECT id, def_meta_intermediaria, def_meta, def_supermeta
            FROM premiacao_campanha
            WHERE dt_inicio <= %s AND dt_fim >= %s
            ORDER BY ativa DESC
            LIMIT 1
        """, (dt_fim_q, dt_ini_q))
        camp_row = cur.fetchone()
        campanha_id = None
        if camp_row:
            campanha_id = camp_row[0]
            if camp_row[1] is not None:
                result["intermediaria"] = float(camp_row[1])
            if camp_row[2] is not None:
                result["meta"] = float(camp_row[2])
            if camp_row[3] is not None:
                result["supermeta"] = float(camp_row[3])

        cur.close()
        conn.close()

        # PASSO 2: override por equipe — campo-a-campo, tem precedência sobre passo 3
        if campanha_id:
            grupo_id = _get_agent_grupo_id(kommo_uid, campanha_id)
            if grupo_id:
                gm = _get_grupo_meta_overrides(campanha_id, grupo_id)
                if gm:
                    if gm.get("meta") is not None:
                        result["meta"] = gm["meta"]
                    if gm.get("meta_intermediaria") is not None:
                        result["intermediaria"] = gm["meta_intermediaria"]
                    if gm.get("supermeta") is not None:
                        result["supermeta"] = gm["supermeta"]

        return result
    except Exception as e:
        logger.warning("Error fetching agent metas: %s", e)
        return {"meta": 0, "intermediaria": 0, "supermeta": 0}


ACEITE_STATUS_ID = 48566207
FUNNEL_PIPELINE_ID = 5481944

_aceite_status_ids_cache = None

def _get_aceite_status_ids():
    """Return ALL status IDs whose name contains 'aceite' (case-insensitive).
    Falls back to the hardcoded ACEITE_STATUS_ID if query fails."""
    global _aceite_status_ids_cache
    if _aceite_status_ids_cache is not None:
        return _aceite_status_ids_cache
    try:
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("SELECT id FROM pipeline_statuses WHERE LOWER(name) LIKE '%aceite%'")
        ids = [r[0] for r in kcur.fetchall()]
        kcur.close()
        kconn.close()
        if ids:
            _aceite_status_ids_cache = ids
            logger.info("Aceite status IDs found: %s", ids)
            return ids
    except Exception as e:
        logger.warning("Could not fetch aceite status IDs: %s", e)
    _aceite_status_ids_cache = [ACEITE_STATUS_ID]
    return _aceite_status_ids_cache


def _calc_ranking_batch(kommo_uid, my_total, dt_ini, dt_fim, campanha_id):
    """Calculate ranking using same logic as the RGM dashboard (DISTINCT ON rgm).
    Also counts aceites in Kommo pipeline as +1 each."""

    # 1. Kommo: map each RGM to exactly ONE agent (same as dashboard)
    kconn = _pg_kommo()
    kcur = kconn.cursor()
    kcur.execute("""
        SELECT DISTINCT ON (v.rgm) v.rgm, l.responsible_user_id
        FROM vw_leads_rgm v
        JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
        WHERE l.responsible_user_id IS NOT NULL
        ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
    """)
    rgm_to_uid = {}
    for rgm_raw, uid in kcur.fetchall():
        n = _normalize_rgm(rgm_raw)
        if n and uid:
            rgm_to_uid[n] = uid

    # Aplica overrides manuais de conflito (mesma lógica do Dashboard Comercial)
    overrides_rgm = _load_conflito_overrides()
    if overrides_rgm:
        for r, uid in overrides_rgm.items():
            rgm_to_uid[r] = uid

    # Count aceites per agent (leads in ANY Aceite stage)
    ace_ids = _get_aceite_status_ids()
    aceites_per_agent = {}
    if ace_ids:
        ace_ph = ",".join(["%s"] * len(ace_ids))
        kcur.execute(f"""
            SELECT responsible_user_id, COUNT(*)
            FROM leads
            WHERE status_id IN ({ace_ph})
              AND NOT is_deleted
              AND responsible_user_id IS NOT NULL
            GROUP BY responsible_user_id
        """, ace_ids)
        aceites_per_agent = {r[0]: r[1] for r in kcur.fetchall()}
    kcur.close()
    kconn.close()

    # 2. DCZ: get all matrículas in the period
    conn = _pg()
    cur = conn.cursor()
    cw, cp = [], []
    if dt_ini:
        cw.append("data_matricula >= %s"); cp.append(dt_ini)
    if dt_fim:
        cw.append("data_matricula <= %s"); cp.append(dt_fim)
    w = ("WHERE " + " AND ".join(cw)) if cw else ""
    cur.execute(f"SELECT rgm FROM comercial_rgm_atual {w}", cp)
    mat_per_agent = defaultdict(int)
    for row in cur.fetchall():
        n = _normalize_rgm(row[0])
        if n and n in rgm_to_uid:
            mat_per_agent[rgm_to_uid[n]] += 1
    cur.close()
    conn.close()

    # 3. Build scores: matrículas + aceites
    all_uids = set(mat_per_agent.keys()) | set(aceites_per_agent.keys())
    if campanha_id:
        conn2 = _pg()
        cur2 = conn2.cursor()
        cur2.execute(
            "SELECT DISTINCT kommo_user_id FROM premiacao_campanha_meta WHERE campanha_id = %s",
            (campanha_id,),
        )
        campaign_agents = {r[0] for r in cur2.fetchall()}
        cur2.close()
        conn2.close()
        if campaign_agents:
            all_uids = all_uids | campaign_agents

    agent_scores = []
    for uid in all_uids:
        mat = mat_per_agent.get(uid, 0)
        ace = aceites_per_agent.get(uid, 0)
        agent_scores.append({"uid": uid, "mat": mat, "aceites": ace, "total": mat + ace})
    agent_scores.sort(key=lambda x: (-x["total"], -x["mat"], x["uid"]))

    pos = 1
    total_agents = len(agent_scores)
    leader = agent_scores[0] if agent_scores else {"total": 0, "mat": 0, "aceites": 0}
    my_entry = None
    for i, s in enumerate(agent_scores):
        if s["uid"] == kommo_uid:
            pos = i + 1
            my_entry = s
            break

    my_score = (my_entry["total"] if my_entry else my_total)
    my_mat = my_entry["mat"] if my_entry else my_total
    my_ace = my_entry["aceites"] if my_entry else 0

    media_time = round(sum(s["total"] for s in agent_scores) / total_agents, 1) if total_agents > 0 else 0

    return {
        "posicao": pos,
        "total_agentes": total_agents,
        "lider_total": leader["total"],
        "diferenca_lider": max(0, leader["total"] - my_score),
        "meu_total": my_score,
        "minhas_mat": my_mat,
        "meus_aceites": my_ace,
        "media_time": media_time,
    }


def _get_active_campanha(dt=None, campanha_id=None):
    """Retorna a campanha solicitada.

    - Se ``campanha_id`` for informado, busca exatamente essa campanha.
      O flag ``is_active`` reflete se ela cobre a data ``dt`` (ou hoje).
    - Senão, retorna a campanha vigente para ``dt`` (ou hoje).
    - Fallback: a mais recente por ``dt_inicio DESC`` com ``is_active=False``,
      para a tela continuar mostrando dados históricos ao invés de ficar vazia.
    """
    ref = dt or datetime.now(BRT).date()
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if campanha_id:
            cur.execute("SELECT * FROM premiacao_campanha WHERE id = %s", (campanha_id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if not row:
                return None
            result = dict(row)
            result["is_active"] = bool(
                row.get("ativa") and row["dt_inicio"] <= ref <= row["dt_fim"]
            )
            return result

        cur.execute("""
            SELECT * FROM premiacao_campanha
            WHERE ativa = TRUE AND dt_inicio <= %s AND dt_fim >= %s
            ORDER BY dt_inicio DESC LIMIT 1
        """, (ref, ref))
        row = cur.fetchone()

        if row:
            result = dict(row)
            result["is_active"] = True
            cur.close()
            conn.close()
            return result

        cur.execute("""
            SELECT * FROM premiacao_campanha
            ORDER BY dt_inicio DESC LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        result = dict(row)
        result["is_active"] = False
        return result
    except Exception:
        return None


def _parse_campanha_id():
    """Lê e normaliza o param ``campanha_id`` da query atual."""
    raw = (request.args.get("campanha_id") or "").strip()
    try:
        return int(raw) if raw else None
    except ValueError:
        return None


@minha_performance_bp.route("/api/minha-performance/campanhas")
def api_minha_campanhas():
    """Lista todas as campanhas pra popular o seletor da tela Minha Performance."""
    try:
        ref = datetime.now(BRT).date()
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, nome, dt_inicio, dt_fim, ativa
            FROM premiacao_campanha
            ORDER BY dt_inicio DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        out = []
        default_id = None
        for r in rows:
            d = dict(r)
            d["dt_inicio"] = str(d["dt_inicio"])
            d["dt_fim"] = str(d["dt_fim"])
            d["is_active"] = bool(d.get("ativa")) and d["dt_inicio"] <= str(ref) <= d["dt_fim"]
            out.append(d)
            if default_id is None and d["is_active"]:
                default_id = d["id"]
        if default_id is None and out:
            default_id = out[0]["id"]

        return jsonify({"ok": True, "campanhas": out, "default_id": default_id})
    except Exception as e:
        logger.exception("listar campanhas")
        return jsonify({"ok": False, "error": str(e)}), 500


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


def _get_tier_bonuses_for_agent(campanha_id, kommo_uid):
    """Retorna tier bonuses para o agente, sobrescrevendo com os valores da equipe quando definidos.
    Mantém os valores da campanha para faixas sem override de equipe (fallback transparente)."""
    bonuses = _get_tier_bonuses(campanha_id)
    grupo_id = _get_agent_grupo_id(kommo_uid, campanha_id)
    if not grupo_id:
        return bonuses
    gm = _get_grupo_meta_overrides(campanha_id, grupo_id)
    if not gm:
        return bonuses
    # Mapeamento faixa → campo em premiacao_grupo_meta
    tier_field_map = {
        "base":         "valor_base",
        "intermediaria": "valor_intermediaria",
        "meta":         "valor_meta",
        "supermeta":    "valor_supermeta",
    }
    for tier, field in tier_field_map.items():
        if gm.get(field) is not None:
            bonuses[tier] = gm[field]
    return bonuses


def _faixas_for_dow(pix_faixas, dow):
    """Faixas aplicáveis: seg–sex (semana) ou sábado."""
    if not pix_faixas:
        return []
    if dow == 5:
        return pix_faixas.get("sabado") or []
    if dow < 5:
        return pix_faixas.get("semana") or []
    return []


def _pix_valor_from_faixas(realizadas, faixas):
    """Maior faixa atingida no dia (ex.: 13 mat → faixa de 12, não a de 10)."""
    if not faixas:
        return 0.0, 0
    for f in sorted(faixas, key=lambda x: -x["min"]):
        if realizadas >= f["min"]:
            return f["valor"], f["min"]
    return 0.0, 0


def _matriculas_to_by_date(matriculas):
    mat_by_date = defaultdict(int)
    for m in matriculas:
        dm = m.get("data_matricula")
        if not dm:
            continue
        if isinstance(dm, str):
            try:
                dm = datetime.strptime(dm[:10], "%Y-%m-%d").date()
            except Exception:
                continue
        mat_by_date[dm] += 1
    return mat_by_date


def _all_consultants_aceites_by_date(dt_ini_str, dt_fim_str):
    """Aceites na fila Kommo (status Aceite) agregados de todos os consultores."""
    aceites_by_date = defaultdict(int)
    try:
        ace_ids = _get_aceite_status_ids()
        if ace_ids:
            ace_ph = ",".join(["%s"] * len(ace_ids))
            dt_ini = datetime.strptime(dt_ini_str, "%Y-%m-%d").date()
            ini_ts = int(datetime.combine(dt_ini, datetime.min.time(), tzinfo=BRT).timestamp())
            kconn = _pg_kommo()
            kcur = kconn.cursor()
            kcur.execute(f"""
                SELECT DATE(to_timestamp(updated_at) AT TIME ZONE 'America/Sao_Paulo') AS dt, COUNT(*)
                FROM leads
                WHERE status_id IN ({ace_ph})
                  AND NOT is_deleted
                  AND updated_at >= %s
                GROUP BY dt
            """, ace_ids + [ini_ts])
            for row in kcur.fetchall():
                if row[0]:
                    aceites_by_date[row[0]] = row[1]
            kcur.close()
            kconn.close()
    except Exception as e:
        logger.warning("all consultants aceites by date: %s", e)
    return aceites_by_date


def _epoch_range_brt(dt_ini, dt_fim):
    """Intervalo epoch (início/fim do dia) em America/Sao_Paulo."""
    ep_ini = int(datetime.combine(dt_ini, datetime.min.time(), tzinfo=BRT).timestamp())
    ep_fim = int(datetime.combine(dt_fim, datetime.min.time(), tzinfo=BRT).timestamp()) + 86399
    return ep_ini, ep_fim


def _all_consultants_ganhos_by_date(dt_ini_str, dt_fim_str):
    """Fechados ganhos (status 142) por dia de closed_at — todos os consultores."""
    ganhos_by_date = defaultdict(int)
    try:
        dt_ini = datetime.strptime(dt_ini_str, "%Y-%m-%d").date()
        dt_fim = datetime.strptime(dt_fim_str, "%Y-%m-%d").date()
        ep_ini, ep_fim = _epoch_range_brt(dt_ini, dt_fim)
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("""
            SELECT DATE(to_timestamp(l.closed_at) AT TIME ZONE 'America/Sao_Paulo') AS dt,
                   COUNT(*)
            FROM leads l
            WHERE l.status_id = 142
              AND NOT l.is_deleted
              AND l.closed_at IS NOT NULL
              AND l.closed_at > 0
              AND l.closed_at >= %s
              AND l.closed_at <= %s
            GROUP BY dt
        """, [ep_ini, ep_fim])
        for row in kcur.fetchall():
            if row[0]:
                ganhos_by_date[row[0]] = row[1]
        kcur.close()
        kconn.close()
    except Exception as e:
        logger.warning("all consultants ganhos by date: %s", e)
    return ganhos_by_date


def _all_consultants_aceites_fila():
    """Total na fila de aceite (todos os consultores)."""
    try:
        ace_ids = _get_aceite_status_ids()
        if ace_ids:
            ace_ph = ",".join(["%s"] * len(ace_ids))
            kconn = _pg_kommo()
            kcur = kconn.cursor()
            kcur.execute(f"""
                SELECT COUNT(*) FROM leads
                WHERE status_id IN ({ace_ph})
                  AND NOT is_deleted
            """, ace_ids)
            n = kcur.fetchone()[0] or 0
            kcur.close()
            kconn.close()
            return n
    except Exception as e:
        logger.warning("all consultants aceites fila: %s", e)
    return 0


def _pix_counts_mat_plus_aceites(mat_by_date, aceites_by_date):
    """Soma matrículas + aceites por dia (mesma regra dos consultores, escopo agregado)."""
    combined = defaultdict(int)
    for d, v in (mat_by_date or {}).items():
        combined[d] += v
    for d, v in (aceites_by_date or {}).items():
        combined[d] += v
    return combined


def _suporte_team_daily_counts(dt_ini_str, dt_fim_str):
    """Matrículas por data_matricula (base comercial, EM CURSO) + aceites Kommo por dia."""
    vendas = _aggregate_comercial_periodo_vendas(dt_ini_str, dt_fim_str)
    mat_by_date = vendas["mat_by_date"]
    aceites_by_date = _all_consultants_aceites_by_date(dt_ini_str, dt_fim_str)
    return mat_by_date, aceites_by_date


def _get_pix_faixas_for_suporte(campanha_id):
    """Faixas PIX do time Suporte (matrículas somadas do time no dia)."""
    try:
        from db import _ensure_suporte_tables
        _ensure_suporte_tables()
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT min_matriculas, valor, apenas_sabado
            FROM premiacao_pix_suporte
            WHERE campanha_id = %s
            ORDER BY apenas_sabado, min_matriculas
        """, (campanha_id,))
        semana, sabado = [], []
        for mn, val, sab in cur.fetchall():
            item = {"min": int(mn), "valor": float(val)}
            (sabado if sab else semana).append(item)
        cur.close()
        conn.close()
        if not semana and not sabado:
            return None
        for lst in (semana, sabado):
            lst.sort(key=lambda x: -x["min"])
        return {"suporte_equipe": True, "semana": semana, "sabado": sabado}
    except Exception as e:
        logger.warning("pix suporte load: %s", e)
        return None


def _get_pix_faixas_for_agent(campanha_id, kommo_uid):
    """Faixas PIX da equipe (grupo) do agente na campanha."""
    if _is_suporte_agent(kommo_uid) or _suporte_equipe_requested():
        su = _get_pix_faixas_for_suporte(campanha_id)
        if su:
            return su
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
        if not row:
            cur.close()
            conn.close()
            return None
        grupo_id = row[0]
        cur.execute("""
            SELECT min_matriculas, valor, apenas_sabado
            FROM premiacao_pix_faixa
            WHERE campanha_id = %s AND grupo_id = %s
            ORDER BY apenas_sabado, min_matriculas
        """, (campanha_id, grupo_id))
        semana, sabado = [], []
        for mn, val, sab in cur.fetchall():
            item = {"min": int(mn), "valor": float(val)}
            (sabado if sab else semana).append(item)
        cur.close()
        conn.close()
        if not semana and not sabado:
            return None
        for lst in (semana, sabado):
            lst.sort(key=lambda x: -x["min"])
        return {"grupo_id": grupo_id, "semana": semana, "sabado": sabado}
    except Exception:
        return None


def _get_agent_grupo_id(kommo_uid, campanha_id):
    """Retorna o grupo_id do agente na campanha, ou None se não estiver em nenhum grupo.
    Segue o mesmo padrão de _get_pix_faixas_for_agent."""
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
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def _get_grupo_meta_overrides(campanha_id, grupo_id):
    """Retorna dict com os campos de premiacao_grupo_meta (podem ser None) ou None se não existir linha."""
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT meta_intermediaria, meta, supermeta,
                   valor_base, valor_intermediaria, valor_meta, valor_supermeta
            FROM premiacao_grupo_meta
            WHERE campanha_id = %s AND grupo_id = %s
        """, (campanha_id, grupo_id))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        return {
            "meta_intermediaria": float(row[0]) if row[0] is not None else None,
            "meta":               float(row[1]) if row[1] is not None else None,
            "supermeta":          float(row[2]) if row[2] is not None else None,
            "valor_base":         float(row[3]) if row[3] is not None else None,
            "valor_intermediaria": float(row[4]) if row[4] is not None else None,
            "valor_meta":         float(row[5]) if row[5] is not None else None,
            "valor_supermeta":    float(row[6]) if row[6] is not None else None,
        }
    except Exception:
        return None


def _get_daily_config(campanha_id, kommo_uid):
    """Metas/bônus legado por dia da semana (sem faixas por equipe)."""
    try:
        conn = _pg()
        cur = conn.cursor()

        cur.execute("""
            SELECT pix_nivel FROM premiacao_pix_nivel_membro
            WHERE campanha_id = %s AND kommo_user_id = %s
            LIMIT 1
        """, (campanha_id, kommo_uid))
        row = cur.fetchone()
        pix_nivel = row[0] if row else None

        result = {}
        if pix_nivel:
            cur.execute("""
                SELECT dia_semana, meta_diaria, bonus_fixo, bonus_extra
                FROM premiacao_meta_diaria
                WHERE campanha_id = %s AND pix_nivel = %s
            """, (campanha_id, pix_nivel))
            for r in cur.fetchall():
                result[r[0]] = {"meta": r[1], "fixo": float(r[2]), "extra": float(r[3])}

        if not result:
            cur.execute("""
                SELECT g.id FROM premiacao_grupo g
                JOIN premiacao_grupo_membro gm ON gm.grupo_id = g.id
                WHERE g.campanha_id = %s AND gm.kommo_user_id = %s
                LIMIT 1
            """, (campanha_id, kommo_uid))
            row = cur.fetchone()
            grupo_id = row[0] if row else None
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


def _calc_daily_premiacao(matriculas, daily_config, dt_ini, dt_fim, pix_faixas=None):
    """PIX diário: faixas por equipe (matrículas do dia) ou legado meta+fixo por dia."""
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
    today = datetime.now(BRT).date()
    if d_fim > today:
        d_fim = today

    breakdown = []
    total_bonus = 0.0
    dias_batidos = 0
    dias_total = 0

    d = d_ini
    while d <= d_fim:
        dow = d.weekday()
        realizadas = mat_by_date.get(d, 0)

        if pix_faixas:
            faixas_list = _faixas_for_dow(pix_faixas, dow)
            if not faixas_list:
                d += timedelta(days=1)
                continue
            dias_total += 1
            valor, meta_hit = _pix_valor_from_faixas(realizadas, faixas_list)
            if valor > 0:
                dias_batidos += 1
            total_bonus += valor
            breakdown.append({
                "data": d.isoformat(),
                "dia_semana": dow,
                "dia_nome": DIA_NAMES[dow],
                "meta": meta_hit,
                "realizadas": realizadas,
                "bonus_fixo": valor,
                "bonus_extra": 0.0,
                "total": valor,
                "modo": "faixas",
            })
        else:
            cfg = daily_config.get(dow, {})
            meta = cfg.get("meta", 0)
            fixo = cfg.get("fixo", 0)
            extra = cfg.get("extra", 0)
            if meta > 0:
                dias_total += 1
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


def _calc_daily_premiacao_from_counts(counts_by_date, daily_config, dt_ini, dt_fim, pix_faixas=None):
    """PIX diário a partir de contagem por dia (ex.: aceites do time Suporte)."""
    DIA_NAMES = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    if not isinstance(counts_by_date, dict):
        counts_by_date = dict(counts_by_date or {})

    d_ini = datetime.strptime(dt_ini, "%Y-%m-%d").date() if isinstance(dt_ini, str) else dt_ini
    d_fim = datetime.strptime(dt_fim, "%Y-%m-%d").date() if isinstance(dt_fim, str) else dt_fim
    today = datetime.now(BRT).date()
    if d_fim > today:
        d_fim = today

    breakdown = []
    total_bonus = 0.0
    dias_batidos = 0
    dias_total = 0

    d = d_ini
    while d <= d_fim:
        dow = d.weekday()
        realizadas = int(counts_by_date.get(d, 0) or 0)

        if pix_faixas:
            faixas_list = _faixas_for_dow(pix_faixas, dow)
            if not faixas_list:
                d += timedelta(days=1)
                continue
            dias_total += 1
            valor, meta_hit = _pix_valor_from_faixas(realizadas, faixas_list)
            if valor > 0:
                dias_batidos += 1
            total_bonus += valor
            breakdown.append({
                "data": d.isoformat(),
                "dia_semana": dow,
                "dia_nome": DIA_NAMES[dow],
                "meta": meta_hit,
                "realizadas": realizadas,
                "bonus_fixo": valor,
                "bonus_extra": 0.0,
                "total": valor,
                "modo": "faixas",
            })
        else:
            cfg = daily_config.get(dow, {})
            meta = cfg.get("meta", 0)
            fixo = cfg.get("fixo", 0)
            extra = cfg.get("extra", 0)
            if meta > 0:
                dias_total += 1
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
    """Determine which tier the agent reached. Always returns a tier (base if none)."""
    sup = metas.get("supermeta", 0)
    met = metas.get("meta", 0)
    inter = metas.get("intermediaria", 0)
    if sup > 0 and total_mat >= sup:
        return "supermeta"
    if met > 0 and total_mat >= met:
        return "meta"
    if inter > 0 and total_mat >= inter:
        return "intermediaria"
    return "base"


# ---------------------------------------------------------------------------
# Diagnóstico de aceites e sync
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/minha-performance/diagnostico")
def api_diagnostico():
    """Endpoint de diagnóstico para verificar estado dos aceites e sync."""
    if not _is_admin():
        return jsonify({"error": "admin only"}), 403
    result = {}

    try:
        ace_ids = _get_aceite_status_ids()
        result["aceite_status_ids"] = ace_ids
    except Exception as e:
        result["aceite_status_ids_error"] = str(e)

    try:
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("SELECT id, pipeline_id, name FROM pipeline_statuses WHERE LOWER(name) LIKE '%aceite%'")
        result["pipeline_statuses_aceite"] = [{"id": r[0], "pipeline_id": r[1], "name": r[2]} for r in kcur.fetchall()]
        kcur.execute("SELECT COUNT(*) FROM pipeline_statuses")
        result["total_pipeline_statuses"] = kcur.fetchone()[0]
        kcur.execute("SELECT COUNT(*) FROM leads WHERE NOT is_deleted")
        result["total_leads_active"] = kcur.fetchone()[0]
        kcur.execute("SELECT COUNT(*) FROM leads WHERE is_deleted")
        result["total_leads_deleted"] = kcur.fetchone()[0]

        if ace_ids:
            ace_ph = ",".join(["%s"] * len(ace_ids))
            kcur.execute(f"SELECT COUNT(*) FROM leads WHERE status_id IN ({ace_ph}) AND NOT is_deleted", ace_ids)
            result["aceites_fila_total"] = kcur.fetchone()[0]
            kcur.execute(f"SELECT COUNT(*) FROM leads WHERE status_id IN ({ace_ph}) AND is_deleted", ace_ids)
            result["aceites_deleted"] = kcur.fetchone()[0]
            kcur.execute(f"""
                SELECT responsible_user_id, COUNT(*) FROM leads
                WHERE status_id IN ({ace_ph}) AND NOT is_deleted
                GROUP BY responsible_user_id ORDER BY COUNT(*) DESC LIMIT 10
            """, ace_ids)
            result["aceites_por_agente"] = [{"uid": r[0], "count": r[1]} for r in kcur.fetchall()]

        kcur.execute("SELECT MAX(synced_at) FROM leads")
        result["last_lead_sync"] = str(kcur.fetchone()[0])
        kcur.close()
        kconn.close()
    except Exception as e:
        result["kommo_db_error"] = str(e)

    try:
        import app as _app
        result["sync_running"] = _app._sync_running
        result["sync_log_count"] = len(_app._sync_logs)
        result["sync_last_logs"] = list(_app._sync_logs)[-5:] if _app._sync_logs else []
    except Exception as e:
        result["app_state_error"] = str(e)

    return jsonify(result)


# ---------------------------------------------------------------------------
# API: Dados do agente
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/minha-performance")
def api_minha_performance():
    kommo_uid = _resolve_kommo_uid(request.args.get("kommo_uid"))
    if not kommo_uid:
        return jsonify({"ok": False, "error": "Agente não vinculado ao Kommo"}), 400

    campanha = _get_active_campanha(campanha_id=_parse_campanha_id())
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

    matriculas = _get_agent_matriculas(kommo_uid, dt_ini, dt_fim, only_em_curso=True)
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

    payload = {
        "ok": True,
        "agent_name": agent_name,
        "kommo_uid": kommo_uid,
        "campanha": {
            "id": campanha["id"],
            "nome": campanha["nome"],
            "dt_inicio": str(campanha["dt_inicio"]),
            "dt_fim": str(campanha["dt_fim"]),
            "is_active": campanha.get("is_active", True),
        },
        "matriculas": matriculas,
        "total": total,
        "metas": metas,
        "tier": tier,
        "pct": pct,
        "falta_inter": max(0, metas["intermediaria"] - total) if metas["intermediaria"] > 0 else None,
        "falta_meta": max(0, metas["meta"] - total) if metas["meta"] > 0 else None,
        "falta_super": max(0, metas["supermeta"] - total) if metas["supermeta"] > 0 else None,
    }
    equipe = _build_meta_equipe_suporte(campanha["id"], dt_ini, dt_fim, kommo_uid)
    if equipe:
        payload["meta_equipe_suporte"] = equipe
    return jsonify(payload)


@minha_performance_bp.route("/api/minha-performance/premiacao")
def api_minha_premiacao():
    kommo_uid = _resolve_kommo_uid(request.args.get("kommo_uid"))
    if not kommo_uid:
        return jsonify({"ok": False, "error": "Agente não vinculado"}), 400

    campanha = _get_active_campanha(campanha_id=_parse_campanha_id())
    if not campanha:
        return jsonify({"ok": True, "campanha": None, "tier_bonus": 0, "daily_bonus": 0,
                        "receb_bonus": 0, "total": 0, "breakdown": []})

    cid = campanha["id"]
    dt_ini = str(campanha["dt_inicio"])
    dt_fim = str(campanha["dt_fim"])

    matriculas = _get_agent_matriculas(kommo_uid, dt_ini, dt_fim, only_em_curso=True)
    total_mat = len(matriculas)
    metas = _get_agent_metas(kommo_uid, dt_ini, dt_fim)
    tier = _determine_tier(total_mat, metas)

    # Tier bonus — usa override por equipe quando disponível (cálculo por agente)
    tier_bonuses = _get_tier_bonuses_for_agent(cid, kommo_uid)
    tier_valor = tier_bonuses.get(tier, 0)
    tier_bonus_total = tier_valor * total_mat

    # Daily bonus
    pix_faixas = _get_pix_faixas_for_agent(cid, kommo_uid)
    daily_config = {} if pix_faixas else _get_daily_config(cid, kommo_uid)
    suporte_pix = bool(pix_faixas and pix_faixas.get("suporte_equipe"))
    if suporte_pix:
        mat_by_p, _ = _suporte_team_daily_counts(dt_ini, dt_fim)
        breakdown, daily_bonus_total, dias_batidos, dias_total = _calc_daily_premiacao_from_counts(
            mat_by_p, daily_config, dt_ini, dt_fim, pix_faixas=pix_faixas
        )
    else:
        breakdown, daily_bonus_total, dias_batidos, dias_total = _calc_daily_premiacao(
            matriculas, daily_config, dt_ini, dt_fim, pix_faixas=pix_faixas
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
        "campanha": {"id": cid, "nome": campanha["nome"], "is_active": campanha.get("is_active", True)},
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
        matriculas = _get_agent_matriculas(kommo_uid, dt_ini, dt_fim, only_em_curso=True)
        total = len(matriculas)
        metas = _get_agent_metas(kommo_uid, dt_ini, dt_fim)
        tier = _determine_tier(total, metas)

        # usa override por equipe quando disponível (cálculo por agente no histórico)
        tier_bonuses = _get_tier_bonuses_for_agent(c["id"], kommo_uid)
        tier_valor = tier_bonuses.get(tier, 0) if tier else 0
        tier_bonus = tier_valor * total

        pix_f = _get_pix_faixas_for_agent(c["id"], kommo_uid)
        daily_config = {} if pix_f else _get_daily_config(c["id"], kommo_uid)
        suporte_pix_h = bool(pix_f and pix_f.get("suporte_equipe"))
        if suporte_pix_h:
            mat_by_h, _ = _suporte_team_daily_counts(dt_ini, dt_fim)
            _, daily_bonus, dias_batidos, dias_total = _calc_daily_premiacao_from_counts(
                mat_by_h, daily_config, dt_ini, dt_fim, pix_faixas=pix_f
            )
        else:
            _, daily_bonus, dias_batidos, dias_total = _calc_daily_premiacao(
                matriculas, daily_config, dt_ini, dt_fim, pix_faixas=pix_f
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

    campanha = _get_active_campanha(campanha_id=_parse_campanha_id())
    if not campanha:
        return jsonify({"ok": True, "campanha": None})

    cid = campanha["id"]
    dt_ini_str = str(campanha["dt_inicio"])
    dt_fim_str = str(campanha["dt_fim"])
    dt_ini = datetime.strptime(dt_ini_str, "%Y-%m-%d").date()
    dt_fim = datetime.strptime(dt_fim_str, "%Y-%m-%d").date()
    today = datetime.now(BRT).date()

    matriculas = _get_agent_matriculas(kommo_uid, dt_ini_str, dt_fim_str, only_em_curso=True)
    total_mat = len(matriculas)
    metas = _get_agent_metas(kommo_uid, dt_ini_str, dt_fim_str)
    tier = _determine_tier(total_mat, metas)

    suporte_modo = _use_suporte_painel(kommo_uid)
    pix_faixas = _get_pix_faixas_for_agent(cid, kommo_uid)
    daily_config = {} if pix_faixas else _get_daily_config(cid, kommo_uid)
    suporte_pix = bool(pix_faixas and pix_faixas.get("suporte_equipe"))

    def _count_work_days(start, end):
        count = 0
        d = start
        while d <= end:
            if pix_faixas:
                if _faixas_for_dow(pix_faixas, d.weekday()):
                    count += 1
            elif daily_config.get(d.weekday(), {}).get("meta", 0) > 0:
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

    if suporte_modo or suporte_pix:
        mat_by_date, aceites_by_date = _suporte_team_daily_counts(dt_ini_str, dt_fim_str)
    else:
        mat_by_date = _matriculas_to_by_date(matriculas)
        aceites_by_date = defaultdict(int)

    today_mat = mat_by_date.get(today, 0)
    yesterday = today - timedelta(days=1)
    yesterday_mat = mat_by_date.get(yesterday, 0)

    # Aceites na fila do Kommo (leads em qualquer stage "Aceite")
    aceites_fila = 0
    aceites_hoje = 0
    if not suporte_pix:
        try:
            ace_ids = _get_aceite_status_ids()
            if ace_ids:
                ace_ph = ",".join(["%s"] * len(ace_ids))
                kconn_ac = _pg_kommo()
                kcur_ac = kconn_ac.cursor()
                kcur_ac.execute(f"""
                    SELECT COUNT(*) FROM leads
                    WHERE responsible_user_id = %s
                      AND status_id IN ({ace_ph})
                      AND NOT is_deleted
                """, [kommo_uid] + ace_ids)
                aceites_fila = kcur_ac.fetchone()[0] or 0
                today_ts = int(datetime.combine(today, datetime.min.time(), tzinfo=BRT).timestamp())
                kcur_ac.execute(f"""
                    SELECT COUNT(*) FROM leads
                    WHERE responsible_user_id = %s
                      AND status_id IN ({ace_ph})
                      AND NOT is_deleted
                      AND updated_at >= %s
                """, [kommo_uid] + ace_ids + [today_ts])
                aceites_hoje = kcur_ac.fetchone()[0] or 0

                ini_ts = int(datetime.combine(dt_ini, datetime.min.time(), tzinfo=BRT).timestamp())
                kcur_ac.execute(f"""
                    SELECT DATE(to_timestamp(updated_at) AT TIME ZONE 'America/Sao_Paulo') AS dt, COUNT(*)
                    FROM leads
                    WHERE responsible_user_id = %s
                      AND status_id IN ({ace_ph})
                      AND NOT is_deleted
                      AND updated_at >= %s
                    GROUP BY dt
                """, [kommo_uid] + ace_ids + [ini_ts])
                for row in kcur_ac.fetchall():
                    if row[0]:
                        aceites_by_date[row[0]] = row[1]

                logger.info("Aceites uid=%s: fila=%d, hoje=%d, by_date=%d days (status_ids=%s)",
                             kommo_uid, aceites_fila, aceites_hoje, len(aceites_by_date), ace_ids)
                kcur_ac.close()
                kconn_ac.close()
            else:
                logger.info("No aceite status IDs found, skipping aceites queries")
        except Exception as e:
            logger.warning("Error fetching aceites: %s", e)
    else:
        aceites_hoje = aceites_by_date.get(today, 0)
        aceites_fila = _all_consultants_aceites_fila()

    if suporte_pix:
        aceites_fila = _all_consultants_aceites_fila()
        aceites_hoje = aceites_fila
        today_realizadas = aceites_fila
        yesterday_realizadas = mat_by_date.get(yesterday, 0)
    elif suporte_modo:
        today_realizadas = mat_by_date.get(today, 0)
        yesterday_realizadas = mat_by_date.get(yesterday, 0)
    else:
        today_realizadas = today_mat + aceites_by_date.get(today, 0)
        yesterday_realizadas = yesterday_mat + aceites_by_date.get(yesterday, 0)

    dow_today = today.weekday()
    if pix_faixas:
        fl_hoje = _faixas_for_dow(pix_faixas, dow_today)
        today_fixo, today_meta = _pix_valor_from_faixas(today_realizadas, fl_hoje)
        today_extra = 0
        if not today_fixo and fl_hoje:
            proximas = sorted([f for f in fl_hoje if f["min"] > today_realizadas], key=lambda x: x["min"])
            if proximas:
                today_meta = proximas[0]["min"]
                today_fixo = proximas[0]["valor"]
    else:
        today_cfg = daily_config.get(dow_today, {})
        today_meta = today_cfg.get("meta", 0)
        today_fixo = today_cfg.get("fixo", 0)
        today_extra = today_cfg.get("extra", 0)

    # Streak: dias consecutivos batendo meta PIX (aceites no Suporte; mat+aceites nos demais)
    sequencia = 0
    d = effective_end
    while d >= dt_ini:
        if pix_faixas:
            fl = _faixas_for_dow(pix_faixas, d.weekday())
            if fl:
                if suporte_pix or suporte_modo:
                    day_total = mat_by_date.get(d, 0)
                else:
                    day_total = mat_by_date.get(d, 0) + aceites_by_date.get(d, 0)
                valor, _ = _pix_valor_from_faixas(day_total, fl)
                if valor > 0:
                    sequencia += 1
                else:
                    break
        else:
            dcfg = daily_config.get(d.weekday(), {})
            dmeta = dcfg.get("meta", 0)
            if dmeta > 0:
                day_total = mat_by_date.get(d, 0) + aceites_by_date.get(d, 0)
                if day_total >= dmeta:
                    sequencia += 1
                else:
                    break
        d -= timedelta(days=1)

    # Heatmap data: week-by-week daily breakdown (mat + aceites)
    heatmap = []
    d = dt_ini
    while d <= dt_fim:
        dow_d = d.weekday()
        fl = _faixas_for_dow(pix_faixas, dow_d) if pix_faixas else None
        dcfg = daily_config.get(dow_d, {})
        dmeta = dcfg.get("meta", 0)
        if fl:
            dmeta = min(f["min"] for f in fl)
        if suporte_pix or suporte_modo:
            # Mesma base do gráfico Matrículas por Dia: data_matricula, situação EM CURSO
            vendas_dia = mat_by_date.get(d, 0) if d <= today else 0
            mat_count = vendas_dia if d <= today else None
            ace_count = aceites_by_date.get(d, 0) if d <= today else 0
            realizadas = vendas_dia if d <= today else None
        else:
            mat_count = mat_by_date.get(d, 0) if d <= today else None
            ace_count = aceites_by_date.get(d, 0) if d <= today else 0
            realizadas = (mat_count + ace_count) if mat_count is not None else None
        status = "future"
        if d <= today:
            if fl and realizadas is not None:
                valor_hit, meta_hit = _pix_valor_from_faixas(realizadas, fl)
                dmeta = meta_hit or dmeta
                if valor_hit > 0:
                    status = "hit"
                elif realizadas > 0:
                    status = "partial"
                else:
                    status = "miss"
            elif dmeta > 0 and realizadas >= dmeta:
                status = "hit"
            elif dmeta > 0 and realizadas and realizadas > 0:
                status = "partial"
            elif dmeta > 0:
                status = "miss"
            elif realizadas and realizadas > 0:
                status = "partial"
            else:
                status = "rest"
        heatmap.append({
            "data": d.isoformat(),
            "dia_semana": dow_d,
            "meta": dmeta,
            "realizadas": realizadas,
            "mat": mat_count if mat_count is not None else 0,
            "aceites": ace_count,
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
            pmat = _get_agent_matriculas(kommo_uid, str(pc["dt_inicio"]), str(pc["dt_fim"]), only_em_curso=True)
            pmetas = _get_agent_metas(kommo_uid, str(pc["dt_inicio"]), str(pc["dt_fim"]))
            ptier = _determine_tier(len(pmat), pmetas)
            if len(pmat) > best_total:
                best_total = len(pmat)
                melhor_campanha = {"nome": pc["nome"], "total": len(pmat), "tier": ptier}
    except Exception:
        pass

    # Tier bonus + daily bonus + receb — usa override por equipe quando disponível (Minha Performance)
    tier_bonuses = _get_tier_bonuses_for_agent(cid, kommo_uid)
    tier_valor = tier_bonuses.get(tier, 0)
    tier_bonus_total = tier_valor * total_mat

    if suporte_pix:
        breakdown, daily_bonus_total, dias_batidos, dias_total = _calc_daily_premiacao_from_counts(
            mat_by_date, daily_config, dt_ini_str, dt_fim_str, pix_faixas=pix_faixas
        )
    else:
        breakdown, daily_bonus_total, dias_batidos, dias_total = _calc_daily_premiacao(
            matriculas, daily_config, dt_ini_str, dt_fim_str, pix_faixas=pix_faixas
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
    for t_name in ("base", "intermediaria", "meta", "supermeta"):
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
    proj_tier_valor = tier_bonuses.get(proj_tier, 0)
    projecao_financeira = round(proj_tier_valor * projecao + daily_bonus_total + receb_bonus_total, 2)

    # Dynamic motivational messages (HTML, money-centric, emojis)
    base_v = tier_bonuses.get("base", 0)
    _fmt = lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # Quebra do saldo total (tier + diárias + recebimentos), pro usuário
    # entender de onde vem cada parte quando o número total é diferente
    # do simples "matrículas × valor por mat".
    def _build_breakdown_html():
        parts = []
        if tier_bonus_total > 0:
            parts.append(f"<span class=\"text-white/70\">Por matrícula:</span> <strong>{_fmt(tier_bonus_total)}</strong>")
        if daily_bonus_total > 0:
            parts.append(f"<span class=\"text-white/70\">Bônus diário:</span> <strong>{_fmt(daily_bonus_total)}</strong>")
        if receb_bonus_total > 0:
            parts.append(f"<span class=\"text-white/70\">Recebimentos:</span> <strong>{_fmt(receb_bonus_total)}</strong>")
        if len(parts) > 1:
            return "<br><span class=\"text-[11px] text-white/60\">" + " · ".join(parts) + "</span>"
        return ""

    breakdown_html = _build_breakdown_html()

    if super_val > 0 and total_mat >= super_val:
        sv = tier_bonuses.get("supermeta", 0)
        extra_mat = total_mat - super_val
        extra_ganho = round(sv * extra_mat, 2)
        mensagem = (
            f"🏆 <strong>LENDÁRIO! SUPERMETA CONQUISTADA!</strong><br>"
            f"Você é referência no time! Cada nova matrícula = <strong>+{_fmt(sv)}</strong> direto no bolso."
        )
        if extra_ganho > 0:
            mensagem += f"<br>💰 Já são <strong>+{_fmt(extra_ganho)}</strong> além da supermeta ({extra_mat} matrículas extras). Continue voando! 🚀"

    elif meta_val > 0 and total_mat >= meta_val:
        falta_s = max(0, super_val - total_mat) if super_val > 0 else 0
        sv = tier_bonuses.get("supermeta", 0)
        ganho_extra = round((sv - tier_valor) * total_mat, 2) if sv > tier_valor else 0
        mensagem = f"🔥 <strong>Meta batida! Você já garantiu {_fmt(total_acumulado)}!</strong>{breakdown_html}"
        if falta_s > 0 and dias_uteis_restantes > 0:
            pace_s = round(falta_s / dias_uteis_restantes, 1)
            mensagem += (
                f"<br><br>Agora é foco total na <strong>SUPERMETA</strong> 🚀<br>"
                f"Faltam apenas <strong>{falta_s}</strong>, o que dá <strong>{pace_s}/dia</strong> nos próximos "
                f"<strong>{dias_uteis_restantes} dias</strong> — totalmente alcançável!"
                f"<br><br>💰 Isso significa mais <strong>+{_fmt(ganho_extra)}</strong> no bolso"
                f"<br><br>Bora acelerar! Você já provou que consegue — agora é só manter o ritmo e fechar com chave de ouro 💪"
            )
        elif falta_s > 0:
            mensagem += f"<br>🚀 Faltam <strong>{falta_s}</strong> para a SUPERMETA (+{_fmt(ganho_extra)}). Bora! 💪"
        else:
            mensagem += "<br>Mandou muito! Resultado garantido! 🎉"

    elif inter_val > 0 and total_mat >= inter_val:
        falta_m = max(0, meta_val - total_mat)
        mv = tier_bonuses.get("meta", 0)
        ganho_extra = round((mv - tier_valor) * total_mat, 2) if mv > tier_valor else 0
        if falta_m > 0 and falta_m <= 3:
            mensagem = (
                f"🔥 <strong>Intermediária batida!</strong> E a META está ali na frente!<br><br>"
                f"Faltam <strong>APENAS {falta_m}</strong> — isso é menos de 1 por dia! 😱<br>"
                f"💰 Ao bater, são <strong>+{_fmt(ganho_extra)}</strong> a mais no seu bolso.<br><br>"
                f"Você está tão perto! Mais um esforço e a META é sua! 💪"
            )
        elif falta_m > 0 and dias_uteis_restantes > 0:
            pace_m = round(falta_m / dias_uteis_restantes, 1)
            mensagem = (
                f"💪 <strong>Intermediária conquistada!</strong> Agora mira na META!<br><br>"
                f"Faltam <strong>{falta_m}</strong> matrículas (<strong>{pace_m}/dia</strong> nos próximos "
                f"<strong>{dias_uteis_restantes} dias</strong>).<br>"
                f"💰 Na META, você garante <strong>+{_fmt(ganho_extra)}</strong> a mais!"
                f"<br><br>Cada matrícula te aproxima. Não desacelere agora! 🚀"
            )
        else:
            mensagem = (
                f"✅ <strong>Intermediária batida!</strong><br>"
                f"Continue empurrando para a META — <strong>+{_fmt(ganho_extra)}</strong> te esperam! 🚀"
            )

    elif inter_val > 0:
        falta_i = max(0, inter_val - total_mat)
        iv = tier_bonuses.get("intermediaria", 0)
        # base_total = saldo total acumulado (inclui tier + diárias + recebs),
        # pra bater com o número exibido no Hero ("Seu saldo acumulado").
        base_total = round(total_acumulado, 2)
        upgrade_total = round(iv * total_mat + daily_bonus_total + receb_bonus_total, 2)
        if falta_i <= 2 and falta_i > 0:
            mensagem = (
                f"🔥 <strong>Quase lá!</strong> Só <strong>{falta_i} matrícula{'s' if falta_i > 1 else ''}</strong> "
                f"para a Intermediária!<br>"
                f"💰 Ao bater, cada matrícula passa a valer <strong>{_fmt(iv)}</strong> = total de <strong>{_fmt(upgrade_total)}</strong>!"
                f"<br><br>Isso é hoje! Vai com tudo! 💪"
            )
        elif base_total > 0 and dias_uteis_restantes > 0:
            pace_i = round(falta_i / dias_uteis_restantes, 1)
            mensagem = (
                f"💰 Você já garante <strong>{_fmt(base_total)}</strong>! Bom começo!<br><br>"
                f"Faltam <strong>{falta_i}</strong> para a Intermediária (<strong>{pace_i}/dia</strong> nos "
                f"próximos <strong>{dias_uteis_restantes} dias</strong>).<br>"
                f"Na Intermediária, seu total sobe para <strong>{_fmt(upgrade_total)}</strong>! 🚀"
                f"<br><br>Cada matrícula conta — bora construir esse resultado! 💪"
            )
        elif base_total > 0:
            mensagem = (
                f"💰 Você já garante <strong>{_fmt(base_total)}</strong>!<br>"
                f"Faltam <strong>{falta_i}</strong> para a Intermediária e subir para <strong>{_fmt(upgrade_total)}</strong>! 🚀"
            )
        else:
            mensagem = (
                f"🎯 Faltam <strong>{falta_i}</strong> matrículas para a Intermediária!<br>"
                f"💰 Cada matrícula vale <strong>{_fmt(iv)}</strong> — total de <strong>{_fmt(upgrade_total)}</strong>! Bora! 💪"
            )

    else:
        if base_v > 0 and total_mat > 0:
            mensagem = (
                f"💰 Cada matrícula = <strong>{_fmt(base_v)}</strong>!<br>"
                f"Você já acumula <strong>{_fmt(round(total_acumulado, 2))}</strong>. Continue assim! 🚀"
                f"{breakdown_html}"
            )
        elif total_mat > 0:
            mensagem = "🔥 Campanha ativa! Você já tem matrículas — agora é acelerar! 💪"
        else:
            mensagem = "🚀 Campanha ativa! A primeira matrícula está te esperando. Bora começar! 💪"

    # Ranking: position among all agents (batch query — single pass)
    ranking = None
    try:
        ranking = _calc_ranking_batch(kommo_uid, total_mat, dt_ini_str, dt_fim_str, cid)
    except Exception as e:
        logger.warning("Error calculating ranking: %s", e)

    # Achievements / conquistas
    conquistas = []
    try:
        if total_mat >= 1:
            conquistas.append({"id": "primeira_mat", "nome": "Primeira Matrícula", "icone": "school", "desc": "Abriu o placar na campanha!"})
        if sequencia >= 3:
            conquistas.append({"id": "streak_3", "nome": "3 Dias Seguidos", "icone": "local_fire_department", "desc": "Aquecendo!"})
        if sequencia >= 5:
            conquistas.append({"id": "streak_5", "nome": "5 Dias Seguidos", "icone": "whatshot", "desc": "Em chamas!"})
        if sequencia >= 7:
            conquistas.append({"id": "streak_7", "nome": "Imparável", "icone": "bolt", "desc": "7+ dias consecutivos!"})
        if tier == "meta":
            conquistas.append({"id": "meta_batida", "nome": "Meta Batida", "icone": "emoji_events", "desc": "Atingiu a meta da campanha!"})
        if tier == "supermeta":
            conquistas.append({"id": "supermeta", "nome": "Supermeta", "icone": "military_tech", "desc": "O topo é seu!"})
        if meta_val > 0 and total_mat >= meta_val and dias_uteis_restantes > 0:
            conquistas.append({"id": "meta_antecipada", "nome": "Meta Antecipada", "icone": "schedule", "desc": "Bateu a meta antes do prazo!"})
        best_day_count = max(mat_by_date.values()) if mat_by_date else 0
        if best_day_count >= 3:
            conquistas.append({"id": "melhor_dia", "nome": f"Super Dia ({best_day_count} mat.)", "icone": "star", "desc": "Dia com mais matrículas!"})
        if ranking and ranking["posicao"] <= 3 and ranking["total_agentes"] >= 3:
            medal = {1: "Ouro", 2: "Prata", 3: "Bronze"}[ranking["posicao"]]
            conquistas.append({"id": f"top_{ranking['posicao']}", "nome": f"Top {ranking['posicao']} ({medal})", "icone": "workspace_premium", "desc": f"Você está no Top 3 do time!"})
    except Exception:
        pass

    # Unified campaigns: check if linked campaigns give a better result
    unificado = None
    linked_ids = _get_linked_campanhas(cid)
    if len(linked_ids) > 1:
        try:
            conn_lk = _pg()
            cur_lk = conn_lk.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            placeholders_lk = ",".join(["%s"] * len(linked_ids))
            cur_lk.execute(
                f"SELECT id, nome, dt_inicio, dt_fim FROM premiacao_campanha WHERE id IN ({placeholders_lk}) ORDER BY dt_inicio",
                linked_ids,
            )
            linked_camps = [dict(r) for r in cur_lk.fetchall()]
            cur_lk.close()
            conn_lk.close()

            uni_dt_ini = min(str(c["dt_inicio"]) for c in linked_camps)
            uni_dt_fim = max(str(c["dt_fim"]) for c in linked_camps)
            uni_matriculas = _get_agent_matriculas(kommo_uid, uni_dt_ini, uni_dt_fim, only_em_curso=True)
            uni_total = len(uni_matriculas)

            uni_metas_sum = {"meta": 0, "intermediaria": 0, "supermeta": 0}
            for lc in linked_camps:
                lm = _get_agent_metas(kommo_uid, str(lc["dt_inicio"]), str(lc["dt_fim"]))
                uni_metas_sum["meta"] += lm.get("meta", 0)
                uni_metas_sum["intermediaria"] += lm.get("intermediaria", 0)
                uni_metas_sum["supermeta"] += lm.get("supermeta", 0)

            uni_tier = _determine_tier(uni_total, uni_metas_sum)
            uni_tier_valor = tier_bonuses.get(uni_tier, 0)
            uni_tier_bonus = uni_tier_valor * uni_total

            if uni_tier_bonus > tier_bonus_total:
                unificado = {
                    "campanhas": [c["nome"] for c in linked_camps],
                    "total_matriculas": uni_total,
                    "metas": uni_metas_sum,
                    "tier": uni_tier,
                    "tier_valor_por_mat": uni_tier_valor,
                    "tier_bonus": round(uni_tier_bonus, 2),
                    "ganho_extra": round(uni_tier_bonus - tier_bonus_total, 2),
                }
                tier = uni_tier
                tier_valor = uni_tier_valor
                tier_bonus_total = uni_tier_bonus
                total_acumulado = tier_bonus_total + daily_bonus_total + receb_bonus_total
        except Exception as e:
            logger.warning("Error calculating unified campaigns: %s", e)

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

    result = {
        "ok": True,
        "agent_name": agent_name,
        "campanha": {
            "id": cid,
            "nome": campanha["nome"],
            "dt_inicio": dt_ini_str,
            "dt_fim": dt_fim_str,
            "is_active": campanha.get("is_active", True),
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
            "aceites_fila": aceites_fila,
            "aceites_hoje": aceites_hoje,
            "bonus_fixo": today_fixo,
            "bonus_extra": today_extra,
            "ontem_realizadas": yesterday_realizadas,
        },
        "sequencia": sequencia,
        "streak_nivel": "imparavel" if sequencia >= 7 else ("em_chamas" if sequencia >= 5 else ("aquecendo" if sequencia >= 3 else None)),
        "heatmap": heatmap,
        "melhor_campanha": melhor_campanha,
        "mensagem": mensagem,
        "tier_progress": [
            {"tier": "base", "target": 0, "pct": 100, "atingido": True,
             "valor_por_mat": tier_bonuses.get("base", 0), "ganho": round(tier_bonuses.get("base", 0) * total_mat, 2)},
            {"tier": "intermediaria", "target": inter_val, "pct": min(100, round(total_mat / inter_val * 100)) if inter_val > 0 else 0, "atingido": total_mat >= inter_val if inter_val > 0 else False,
             "valor_por_mat": tier_bonuses.get("intermediaria", 0), "ganho": round(tier_bonuses.get("intermediaria", 0) * total_mat, 2)},
            {"tier": "meta", "target": meta_val, "pct": min(100, round(total_mat / meta_val * 100)) if meta_val > 0 else 0, "atingido": total_mat >= meta_val if meta_val > 0 else False,
             "valor_por_mat": tier_bonuses.get("meta", 0), "ganho": round(tier_bonuses.get("meta", 0) * total_mat, 2)},
            {"tier": "supermeta", "target": super_val, "pct": min(100, round(total_mat / super_val * 100)) if super_val > 0 else 0, "atingido": total_mat >= super_val if super_val > 0 else False,
             "valor_por_mat": tier_bonuses.get("supermeta", 0), "ganho": round(tier_bonuses.get("supermeta", 0) * total_mat, 2)},
        ],
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
        "ranking": ranking,
        "conquistas": conquistas,
        "matriculas": [{
            "rgm": m.get("rgm"),
            "nivel": m.get("nivel"),
            "modalidade": m.get("modalidade"),
            "data_matricula": m["data_matricula"].isoformat() if hasattr(m.get("data_matricula"), "isoformat") else m.get("data_matricula"),
        } for m in matriculas],
    }
    if unificado:
        result["unificado"] = unificado
    equipe = _build_meta_equipe_suporte(
        cid, dt_ini_str, dt_fim_str, kommo_uid,
        allow_empty_meta=suporte_modo,
    )
    if equipe:
        result["meta_equipe_suporte"] = equipe
    if suporte_pix:
        result["pix_suporte_equipe"] = True
        result["pix_fonte_fila"] = True
    if suporte_modo:
        result["painel_suporte"] = True
        result["suporte_equipe_view"] = bool(_suporte_equipe_requested())
        vendas_time = _aggregate_comercial_periodo_vendas(dt_ini_str, dt_fim_str)
        team_mats = equipe.get("total_matriculas", 0) if equipe else vendas_time["total"]
        team_pace = vendas_time.get("media_diaria", 0)
        if not team_pace and vendas_time["dias_com_venda"]:
            team_pace = round(
                team_mats / vendas_time["dias_com_venda"], 1
            )
        result["media_vendas"] = team_pace
        result["pace_atual"] = team_pace
        chart_mat = [
            {"data": p["data"], "matriculas": int(p["count"])}
            for p in vendas_time.get("evolucao") or []
            if date.fromisoformat(p["data"][:10]) <= min(dt_fim, today)
        ]
        if not chart_mat:
            chart_mat = [
                {"data": d.isoformat(), "matriculas": int(c)}
                for d, c in sorted(vendas_time["mat_by_date"].items())
                if d <= min(dt_fim, today)
            ]
        result["chart_matriculas_dia"] = chart_mat
        result["total_matriculas_time"] = team_mats
        result["vendas_liquidas_time"] = vendas_time.get("total_liquido", 0)
    return jsonify(result)


@minha_performance_bp.route("/api/minha-performance/agentes")
def api_mp_agentes():
    """List agents (admin / Premiação) for the agent selector."""
    if not _can_manage_premiacao():
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
        suporte_uids = _get_suporte_kommo_uids()
        return jsonify({
            "ok": True,
            "agentes": agents,
            "suporte_uids": suporte_uids,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Campanhas (admin)
# ---------------------------------------------------------------------------

def _parse_metas_padrao_from_body(body):
    """Pré-definição em quantidade de matrículas (Dashboard comercial), não confundir com R$/faixa."""
    mp = (body or {}).get("metas_padrao") or {}
    out = []
    for k in ("meta_intermediaria", "meta", "supermeta"):
        v = mp.get(k)
        if v is None or v == "":
            out.append(None)
            continue
        try:
            out.append(float(v))
        except (TypeError, ValueError):
            out.append(None)
    return tuple(out)


@minha_performance_bp.route("/api/premiacao/campanhas", methods=["GET"])
def api_campanhas_list():
    if not _can_manage_premiacao():
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
            di, dm, ds = c.get("def_meta_intermediaria"), c.get("def_meta"), c.get("def_supermeta")
            c["metas_padrao"] = {
                "meta_intermediaria": float(di) if di is not None else None,
                "meta": float(dm) if dm is not None else None,
                "supermeta": float(ds) if ds is not None else None,
            }
            campanhas.append(c)
        cur.close()
        conn.close()
        return jsonify({"ok": True, "campanhas": campanhas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas-periodos", methods=["GET"])
def api_campanhas_periodos():
    """Retorna períodos de metas de comercial_metas + premiacao_campanha — acessível a todos os usuários logados."""
    # Não usar `if not session.get("user_id")`: user_id 0 é válido (login APP_USER/APP_PASS em auth.py)
    if not session.get("authenticated") or session.get("user_id") is None:
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor()
        # Períodos distintos de comercial_metas (mesma fonte do Histórico de Metas do dashboard)
        cur.execute("""
            SELECT DISTINCT ON (dt_inicio, dt_fim)
                dt_inicio, dt_fim,
                COALESCE(NULLIF(TRIM(descricao), ''), TO_CHAR(dt_inicio, 'DD/MM/YYYY') || ' → ' || TO_CHAR(dt_fim, 'DD/MM/YYYY')) AS nome
            FROM comercial_metas
            ORDER BY dt_inicio DESC, dt_fim DESC
        """)
        seen = set()
        rows = []
        for r in cur.fetchall():
            key = (str(r[0]), str(r[1]))
            if key not in seen:
                seen.add(key)
                rows.append({"nome": r[2], "dt_inicio": str(r[0]), "dt_fim": str(r[1])})

        # Adiciona campanhas de premiacao_campanha que não estejam já cobertas
        cur.execute("SELECT nome, dt_inicio, dt_fim FROM premiacao_campanha ORDER BY dt_inicio DESC")
        for r in cur.fetchall():
            key = (str(r[1]), str(r[2]))
            if key not in seen:
                seen.add(key)
                rows.append({"nome": r[0], "dt_inicio": str(r[1]), "dt_fim": str(r[2])})

        # Reordena por data decrescente
        rows.sort(key=lambda x: x["dt_inicio"], reverse=True)

        cur.close()
        conn.close()
        return jsonify({"ok": True, "campanhas": rows})
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
        di, dm, ds = _parse_metas_padrao_from_body(body)
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO premiacao_campanha (nome, dt_inicio, dt_fim,
                def_meta_intermediaria, def_meta, def_supermeta)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (nome, dt_inicio, dt_fim, di, dm, ds),
        )
        cid = cur.fetchone()[0]
        for tier, valor in tiers.items():
            if tier in ("base", "intermediaria", "meta", "supermeta") and float(valor) > 0:
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
                if tier in ("base", "intermediaria", "meta", "supermeta") and float(valor) > 0:
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
        if "metas_padrao" in body:
            di, dm, ds = _parse_metas_padrao_from_body(body)
            cur.execute(
                """
                UPDATE premiacao_campanha SET
                    def_meta_intermediaria = %s, def_meta = %s, def_supermeta = %s
                WHERE id = %s
                """,
                (di, dm, ds, cid),
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
# API: Links entre campanhas (admin)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanha-links", methods=["GET"])
def api_campanha_links_list():
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT cl.id, cl.campanha_a_id, cl.campanha_b_id,
                   ca.nome AS nome_a, cb.nome AS nome_b
            FROM premiacao_campanha_link cl
            JOIN premiacao_campanha ca ON ca.id = cl.campanha_a_id
            JOIN premiacao_campanha cb ON cb.id = cl.campanha_b_id
            ORDER BY cl.created_at DESC
        """)
        links = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({"ok": True, "links": links})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanha-links", methods=["POST"])
def api_campanha_links_create():
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    a_id = body.get("campanha_a_id")
    b_id = body.get("campanha_b_id")
    if not a_id or not b_id or int(a_id) == int(b_id):
        return jsonify({"error": "Selecione duas campanhas diferentes"}), 400
    lo, hi = min(int(a_id), int(b_id)), max(int(a_id), int(b_id))
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO premiacao_campanha_link (campanha_a_id, campanha_b_id) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING id",
            (lo, hi),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        if row:
            return jsonify({"ok": True, "id": row[0]})
        return jsonify({"ok": True, "message": "Vínculo já existe"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanha-links/<int:lid>", methods=["DELETE"])
def api_campanha_links_delete(lid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM premiacao_campanha_link WHERE id = %s", (lid,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _get_linked_campanhas(campanha_id):
    """Return list of campaign IDs linked to this one (including itself)."""
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT campanha_a_id, campanha_b_id FROM premiacao_campanha_link
            WHERE campanha_a_id = %s OR campanha_b_id = %s
        """, (campanha_id, campanha_id))
        linked = {campanha_id}
        for a, b in cur.fetchall():
            linked.add(a)
            linked.add(b)
        cur.close()
        conn.close()
        return list(linked)
    except Exception:
        return [campanha_id]


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

@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/meta-suporte", methods=["GET"])
def api_meta_suporte_get(cid):
    if not _can_manage_premiacao():
        return jsonify({"ok": False, "error": "Sem permissão"}), 403
    try:
        from db import _ensure_suporte_tables
        _ensure_suporte_tables()
        sm = _get_meta_suporte(cid)
        uids = _get_suporte_kommo_uids()
        return jsonify({
            "ok": True,
            "meta": sm["meta"],
            "meta_intermediaria": sm["intermediaria"],
            "supermeta": sm["supermeta"],
            "agentes_count": len(uids),
            "agentes_kommo_ids": uids,
        })
    except Exception as e:
        logger.exception("meta-suporte GET campanha %s: %s", cid, e)
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/meta-suporte", methods=["POST"])
def api_meta_suporte_save(cid):
    """Meta unificada do time Suporte Comercial (soma das matrículas de todos)."""
    if not _can_manage_premiacao():
        return jsonify({"ok": False, "error": "Sem permissão"}), 403
    body = request.json or {}
    meta = float(body.get("meta", 0) or 0)
    if meta <= 0:
        return jsonify({"error": "Informe a meta do time"}), 400
    inter = 0.0
    sup = 0.0
    try:
        from db import _ensure_suporte_tables
        _ensure_suporte_tables()
        global _suporte_uids_cache
        _suporte_uids_cache = None
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO premiacao_meta_suporte
                (campanha_id, meta, meta_intermediaria, supermeta, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (campanha_id) DO UPDATE SET
                meta = EXCLUDED.meta,
                meta_intermediaria = EXCLUDED.meta_intermediaria,
                supermeta = EXCLUDED.supermeta,
                updated_at = NOW()
        """, (cid, meta, inter, sup))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": True, "agentes_count": len(_get_suporte_kommo_uids())})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/pix-suporte", methods=["GET"])
def api_pix_suporte_get(cid):
    if not _can_manage_premiacao():
        return jsonify({"ok": False, "error": "Sem permissão"}), 403
    try:
        from db import _ensure_suporte_tables
        _ensure_suporte_tables()
        fx = _get_pix_faixas_for_suporte(cid)
        faixas = []
        if fx:
            for f in fx.get("semana", []):
                faixas.append({
                    "min_matriculas": f["min"],
                    "valor": f["valor"],
                    "apenas_sabado": False,
                })
            for f in fx.get("sabado", []):
                faixas.append({
                    "min_matriculas": f["min"],
                    "valor": f["valor"],
                    "apenas_sabado": True,
                })
        return jsonify({
            "ok": True,
            "faixas": faixas,
            "agentes_count": len(_get_suporte_kommo_uids()),
        })
    except Exception as e:
        logger.exception("pix-suporte GET campanha %s: %s", cid, e)
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/pix-suporte", methods=["POST"])
def api_pix_suporte_save(cid):
    """Faixas PIX diário do time Suporte (soma de matrículas do time no dia)."""
    if not _can_manage_premiacao():
        return jsonify({"ok": False, "error": "Sem permissão"}), 403
    body = request.json or {}
    faixas = body.get("faixas", [])
    try:
        from db import _ensure_suporte_tables
        _ensure_suporte_tables()
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM premiacao_pix_suporte WHERE campanha_id = %s", (cid,))
        saved = 0
        for item in faixas:
            mn = int(item.get("min_matriculas", 0))
            val = float(item.get("valor", 0))
            sab = bool(item.get("apenas_sabado", False))
            if mn > 0 and val > 0:
                cur.execute("""
                    INSERT INTO premiacao_pix_suporte
                        (campanha_id, min_matriculas, valor, apenas_sabado)
                    VALUES (%s, %s, %s, %s)
                """, (cid, mn, val, sab))
                saved += 1
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": saved})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/metas", methods=["GET"])
def api_campanha_metas_get(cid):
    if not _can_manage_premiacao():
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
    if not _can_manage_premiacao():
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
# API: PIX diário por equipe (faixas por matrículas do dia)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/pix-equipe", methods=["GET"])
def api_pix_equipe_get(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        from db import _ensure_pix_faixa_tables
        _ensure_pix_faixa_tables()
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, nome FROM premiacao_grupo WHERE campanha_id = %s ORDER BY nome", (cid,))
        equipes = []
        for g in cur.fetchall():
            g = dict(g)
            cur.execute(
                "SELECT kommo_user_id FROM premiacao_grupo_membro WHERE grupo_id = %s ORDER BY kommo_user_id",
                (g["id"],),
            )
            g["membros"] = [r["kommo_user_id"] for r in cur.fetchall()]
            cur.execute("""
                SELECT min_matriculas, valor, apenas_sabado
                FROM premiacao_pix_faixa
                WHERE campanha_id = %s AND grupo_id = %s
                ORDER BY apenas_sabado, min_matriculas
            """, (cid, g["id"]))
            g["faixas"] = [
                {
                    "min_matriculas": r["min_matriculas"],
                    "valor": float(r["valor"]),
                    "apenas_sabado": bool(r["apenas_sabado"]),
                }
                for r in cur.fetchall()
            ]
            equipes.append(g)
        cur.close()
        conn.close()
        return jsonify({"ok": True, "equipes": equipes})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/pix-equipe", methods=["POST"])
def api_pix_equipe_save(cid):
    """Salva faixas PIX por equipe (grupo). Agentes são atribuídos na seção Grupos."""
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    faixas = body.get("faixas", [])
    try:
        from db import _ensure_pix_faixa_tables
        _ensure_pix_faixa_tables()
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM premiacao_pix_faixa WHERE campanha_id = %s", (cid,))
        for item in faixas:
            gid = int(item["grupo_id"])
            mn = int(item.get("min_matriculas", 0))
            val = float(item.get("valor", 0))
            sab = bool(item.get("apenas_sabado", False))
            if mn > 0 and val > 0:
                cur.execute("""
                    INSERT INTO premiacao_pix_faixa (campanha_id, grupo_id, min_matriculas, valor, apenas_sabado)
                    VALUES (%s, %s, %s, %s, %s)
                """, (cid, gid, mn, val, sab))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Metas + R$/mat por equipe (admin)
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/metas-grupo", methods=["GET"])
def api_metas_grupo_get(cid):
    """Lista todas as equipes da campanha com seus overrides de meta (null = usar padrão da campanha)."""
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT gv.id AS grupo_id, gv.nome AS grupo_nome,
                   pgm.meta_intermediaria, pgm.meta, pgm.supermeta,
                   pgm.valor_base, pgm.valor_intermediaria, pgm.valor_meta, pgm.valor_supermeta
            FROM premiacao_grupo gv
            LEFT JOIN premiacao_grupo_meta pgm
                   ON pgm.grupo_id = gv.id AND pgm.campanha_id = gv.campanha_id
            WHERE gv.campanha_id = %s
            ORDER BY gv.nome
        """, (cid,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        grupos = []
        for r in rows:
            grupos.append({
                "grupo_id":         r["grupo_id"],
                "grupo_nome":       r["grupo_nome"],
                "meta_intermediaria": float(r["meta_intermediaria"]) if r["meta_intermediaria"] is not None else None,
                "meta":             float(r["meta"]) if r["meta"] is not None else None,
                "supermeta":        float(r["supermeta"]) if r["supermeta"] is not None else None,
                "valor_base":       float(r["valor_base"]) if r["valor_base"] is not None else None,
                "valor_intermediaria": float(r["valor_intermediaria"]) if r["valor_intermediaria"] is not None else None,
                "valor_meta":       float(r["valor_meta"]) if r["valor_meta"] is not None else None,
                "valor_supermeta":  float(r["valor_supermeta"]) if r["valor_supermeta"] is not None else None,
            })
        return jsonify({"ok": True, "grupos": grupos})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/metas-grupo", methods=["POST"])
def api_metas_grupo_save(cid):
    """UPSERT de metas e R$/mat por equipe. Campos null/ausentes gravam NULL (= usar fallback da campanha)."""
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    grupos = body.get("grupos", [])
    try:
        conn = _pg()
        cur = conn.cursor()

        def _to_numeric(val):
            if val is None or val == "":
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        for item in grupos:
            gid = int(item["grupo_id"])
            mi  = _to_numeric(item.get("meta_intermediaria"))
            m   = _to_numeric(item.get("meta"))
            sm  = _to_numeric(item.get("supermeta"))
            vb  = _to_numeric(item.get("valor_base"))
            vi  = _to_numeric(item.get("valor_intermediaria"))
            vm  = _to_numeric(item.get("valor_meta"))
            vs  = _to_numeric(item.get("valor_supermeta"))
            cur.execute("""
                INSERT INTO premiacao_grupo_meta
                    (campanha_id, grupo_id, meta_intermediaria, meta, supermeta,
                     valor_base, valor_intermediaria, valor_meta, valor_supermeta, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (campanha_id, grupo_id) DO UPDATE SET
                    meta_intermediaria  = EXCLUDED.meta_intermediaria,
                    meta                = EXCLUDED.meta,
                    supermeta           = EXCLUDED.supermeta,
                    valor_base          = EXCLUDED.valor_base,
                    valor_intermediaria = EXCLUDED.valor_intermediaria,
                    valor_meta          = EXCLUDED.valor_meta,
                    valor_supermeta     = EXCLUDED.valor_supermeta,
                    updated_at          = NOW()
            """, (cid, gid, mi, m, sm, vb, vi, vm, vs))

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: PIX diário por nível 1–3 (admin) — legado
# ---------------------------------------------------------------------------

@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/pix-nivel", methods=["GET"])
def api_pix_nivel_get(cid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        from db import _ensure_pix_nivel_tables
        _ensure_pix_nivel_tables()
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT kommo_user_id, pix_nivel
            FROM premiacao_pix_nivel_membro
            WHERE campanha_id = %s
            ORDER BY pix_nivel, kommo_user_id
        """, (cid,))
        membros = [dict(r) for r in cur.fetchall()]
        cur.execute("""
            SELECT pix_nivel, dia_semana, meta_diaria, bonus_fixo, bonus_extra
            FROM premiacao_meta_diaria
            WHERE campanha_id = %s AND pix_nivel IS NOT NULL
            ORDER BY pix_nivel, dia_semana
        """, (cid,))
        diarias = [dict(r) for r in cur.fetchall()]
        for d in diarias:
            d["bonus_fixo"] = float(d["bonus_fixo"])
            d["bonus_extra"] = float(d["bonus_extra"])
        cur.close()
        conn.close()
        return jsonify({"ok": True, "membros": membros, "diarias": diarias})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/premiacao/campanhas/<int:cid>/pix-nivel", methods=["POST"])
def api_pix_nivel_save(cid):
    """Salva agentes por nível PIX (1–3) e metas/bônus diários por nível."""
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    membros = body.get("membros", [])
    items = body.get("items", [])
    try:
        from db import _ensure_pix_nivel_tables
        _ensure_pix_nivel_tables()
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM premiacao_pix_nivel_membro WHERE campanha_id = %s", (cid,))
        for m in membros:
            uid = int(m.get("kommo_user_id", 0))
            nivel = int(m.get("pix_nivel", 0))
            if uid and nivel in (1, 2, 3):
                cur.execute("""
                    INSERT INTO premiacao_pix_nivel_membro (campanha_id, kommo_user_id, pix_nivel)
                    VALUES (%s, %s, %s)
                """, (cid, uid, nivel))
        cur.execute("DELETE FROM premiacao_meta_diaria WHERE campanha_id = %s AND pix_nivel IS NOT NULL", (cid,))
        for item in items:
            nivel = int(item["pix_nivel"])
            dow = int(item["dia_semana"])
            meta = int(item.get("meta_diaria", 0))
            fixo = float(item.get("bonus_fixo", 0))
            extra = float(item.get("bonus_extra", 0))
            if nivel in (1, 2, 3) and (meta > 0 or fixo > 0 or extra > 0):
                cur.execute("""
                    INSERT INTO premiacao_meta_diaria
                        (campanha_id, pix_nivel, kommo_user_id, dia_semana, meta_diaria, bonus_fixo, bonus_extra)
                    VALUES (%s, %s, 0, %s, %s, %s, %s)
                """, (cid, nivel, dow, meta, fixo, extra))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# API: Metas diárias por grupo (admin) — legado
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

        from psycopg2.extras import Json

        def _parse_valor(v):
            try:
                return float(str(v).replace("R$", "").replace(".", "").replace(",", ".").strip())
            except Exception:
                return 0

        def _parse_date(v):
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
                try:
                    return datetime.strptime((v or "").strip(), fmt).date()
                except Exception:
                    continue
            return None

        # ── Mapeamento de colunas resolvido UMA VEZ antes do loop ───────────
        # Para cada campo definimos: (matchers exatos, matchers substring).
        # A escolha prefere match exato; só cai para substring se não houver.
        field_defs = {
            "rgm":         (("rgm",), ("rgm",)),
            "valor":       (("valor pago", "valor recebido", "valor receb", "valor"),
                            ("valor",)),
            "nivel":       (("nivel", "nível"), ("nivel", "nível")),
            "modalidade":  (("modalidade",), ("modalidade",)),
            "data_mat":    (("data matricula", "data matrícula", "dt matricula", "dt mat"),
                            ()),
            "tipo_pag":    (("tipo pagamento", "tipo pag", "tipo_pag", "beleza", "categoria"),
                            ("beleza",)),
            "turma":       (("turma",), ("turma",)),
            "ciclo":       (("ciclo",), ("ciclo",)),
        }

        original_headers = [k for k in (reader.fieldnames or []) if k]
        norm_headers = {k: k.strip().lower() for k in original_headers}

        col_map: dict[str, str] = {}
        used_cols: set[str] = set()

        # 1ª passada: matches exatos (do mais específico para o mais genérico)
        for field, (exact_list, _) in field_defs.items():
            for cand in exact_list:
                for col, kl in norm_headers.items():
                    if col in used_cols:
                        continue
                    if kl == cand:
                        col_map[field] = col
                        used_cols.add(col)
                        break
                if field in col_map:
                    break

        # 2ª passada: substring para o que sobrou
        for field, (_, substr_list) in field_defs.items():
            if field in col_map:
                continue
            for cand in substr_list:
                for col, kl in norm_headers.items():
                    if col in used_cols:
                        continue
                    if cand in kl:
                        col_map[field] = col
                        used_cols.add(col)
                        break
                if field in col_map:
                    break

        # Validações: campos obrigatórios devem ter sido mapeados
        missing = [f for f in ("rgm", "valor", "turma", "tipo_pag", "ciclo") if f not in col_map]
        if missing:
            label = {"rgm": "RGM", "valor": "Valor Pago", "turma": "Turma",
                     "tipo_pag": "Beleza (tipo)", "ciclo": "Ciclo"}
            return jsonify({
                "ok": False,
                "error": "Colunas obrigatórias não encontradas no cabeçalho do CSV: "
                         + ", ".join(label.get(m, m) for m in missing)
                         + f". Cabeçalho recebido: {original_headers}",
            }), 400

        ciclo_re = re.compile(r"^\s*\d{4}\s*/\s*\d\s*$")  # ex.: 2026/1

        count = 0
        first_row_issues: list[str] = []
        for row_idx, row in enumerate(reader, start=2):  # linha 1 é cabeçalho
            rgm = _normalize_rgm(row.get(col_map["rgm"], ""))
            if not rgm:
                continue
            valor = _parse_valor(row.get(col_map["valor"], ""))
            nivel = (row.get(col_map.get("nivel", ""), "") or "").strip()
            modalidade = (row.get(col_map.get("modalidade", ""), "") or "").strip()
            data_mat = _parse_date(row.get(col_map.get("data_mat", ""), ""))
            tipo_pag = (row.get(col_map["tipo_pag"], "") or "").strip()
            turma = (row.get(col_map["turma"], "") or "").strip()
            ciclo = (row.get(col_map["ciclo"], "") or "").strip()

            # Detecção precoce de colunas trocadas: se na primeira linha
            # válida o `turma` veio em formato YYYY/N (típico de ciclo) e
            # `ciclo` não, aborta com mensagem clara para o usuário.
            if count == 0 and len(first_row_issues) == 0:
                if ciclo_re.match(turma) and not ciclo_re.match(ciclo):
                    first_row_issues.append(
                        f"Linha {row_idx}: a coluna mapeada como 'Turma' "
                        f"({col_map['turma']!r}) tem valor {turma!r}, que parece um Ciclo. "
                        f"O cabeçalho está fora da ordem esperada? "
                        f"Mapeamento detectado: turma={col_map['turma']!r}, ciclo={col_map['ciclo']!r}."
                    )
                    break

            cur.execute("""
                INSERT INTO comercial_recebimentos
                    (snapshot_id, rgm, nivel, modalidade, data_matricula, valor,
                     tipo_pagamento, mes_referencia, turma, ciclo, data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (snap_id, rgm, nivel, modalidade, data_mat, valor,
                  tipo_pag, mes_ref, turma, ciclo, Json(row)))
            count += 1

        if first_row_issues:
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({
                "ok": False,
                "error": "Upload abortado: " + " ".join(first_row_issues),
            }), 400

        cur.execute("UPDATE recebimentos_snapshots SET row_count = %s WHERE id = %s", (count, snap_id))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({
            "ok": True,
            "snapshot_id": snap_id,
            "rows": count,
            "_col_map": col_map,
        })
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


# ══════════════════════════════════════════════════════════════════════════
# Matriculas do agente — listagem oficial
# ══════════════════════════════════════════════════════════════════════════

@minha_performance_bp.route("/api/minha-performance/matriculas")
def api_minha_matriculas():
    kommo_uid = _resolve_kommo_uid(request.args.get("kommo_uid"))
    if not kommo_uid:
        return jsonify({"ok": False, "error": "Sem vínculo Kommo"}), 400

    dt_ini = request.args.get("dt_ini")
    dt_fim = request.args.get("dt_fim")
    mats = _get_agent_matriculas(kommo_uid, dt_ini, dt_fim)
    for m in mats:
        if m.get("data_matricula"):
            m["data_matricula"] = str(m["data_matricula"])
    return jsonify({"ok": True, "matriculas": mats, "total": len(mats)})


# ══════════════════════════════════════════════════════════════════════════
# Lista propria do agente — CRUD
# ══════════════════════════════════════════════════════════════════════════

def _get_agent_user_id():
    uid = session.get("user_id")
    if not uid:
        return None, None
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT id, kommo_user_id FROM app_users WHERE id = %s", (uid,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row[0], row[1]
    except Exception:
        pass
    return uid, None


@minha_performance_bp.route("/api/minha-performance/minhas-matriculas", methods=["GET"])
def api_minhas_mat_list():
    user_id, kommo_uid = _get_agent_user_id()
    if not user_id:
        return jsonify({"error": "Não autenticado"}), 401
    target_uid = request.args.get("kommo_uid", type=int)
    view_user_id = user_id
    if target_uid and _is_admin():
        kommo_uid = target_uid
        try:
            conn_u = _pg()
            cur_u = conn_u.cursor()
            cur_u.execute(
                "SELECT id FROM app_users WHERE kommo_user_id = %s LIMIT 1",
                (kommo_uid,),
            )
            row_u = cur_u.fetchone()
            cur_u.close()
            conn_u.close()
            if row_u:
                view_user_id = row_u[0]
        except Exception:
            pass
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if kommo_uid:
            cur.execute(
                """
                SELECT * FROM agent_matriculas
                WHERE user_id = %s OR kommo_user_id = %s
                ORDER BY data_matricula DESC NULLS LAST, created_at DESC
                """,
                (view_user_id, kommo_uid),
            )
        else:
            cur.execute(
                "SELECT * FROM agent_matriculas WHERE user_id = %s ORDER BY data_matricula DESC NULLS LAST, created_at DESC",
                (view_user_id,),
            )
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            for k in ("data_matricula", "created_at", "updated_at"):
                if r.get(k):
                    r[k] = str(r[k])
        return jsonify({"ok": True, "matriculas": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/minha-performance/minhas-matriculas", methods=["POST"])
def api_minhas_mat_create():
    user_id, kommo_uid = _get_agent_user_id()
    if not user_id:
        return jsonify({"error": "Não autenticado"}), 401
    b = request.get_json(force=True)
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_matriculas (user_id, kommo_user_id, rgm, nome, curso, polo, data_matricula, ciclo, nivel, kommo_lead_id, observacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """, (
            user_id, kommo_uid,
            (b.get("rgm") or "").strip(),
            (b.get("nome") or "").strip(),
            (b.get("curso") or "").strip(),
            (b.get("polo") or "").strip(),
            b.get("data_matricula") or None,
            (b.get("ciclo") or "").strip(),
            (b.get("nivel") or "").strip(),
            (b.get("kommo_lead_id") or "").strip(),
            (b.get("observacao") or "").strip(),
        ))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/minha-performance/minhas-matriculas/<int:mid>", methods=["PUT"])
def api_minhas_mat_update(mid):
    user_id, _ = _get_agent_user_id()
    if not user_id:
        return jsonify({"error": "Não autenticado"}), 401
    b = request.get_json(force=True)
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM agent_matriculas WHERE id = %s", (mid,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({"ok": False, "error": "Não encontrado"}), 404
        if row[0] != user_id and not _is_admin():
            cur.close(); conn.close()
            return jsonify({"ok": False, "error": "Sem permissão"}), 403
        cur.execute("""
            UPDATE agent_matriculas SET rgm=%s, nome=%s, curso=%s, polo=%s, data_matricula=%s,
            ciclo=%s, nivel=%s, kommo_lead_id=%s, observacao=%s, updated_at=NOW()
            WHERE id=%s
        """, (
            (b.get("rgm") or "").strip(),
            (b.get("nome") or "").strip(),
            (b.get("curso") or "").strip(),
            (b.get("polo") or "").strip(),
            b.get("data_matricula") or None,
            (b.get("ciclo") or "").strip(),
            (b.get("nivel") or "").strip(),
            (b.get("kommo_lead_id") or "").strip(),
            (b.get("observacao") or "").strip(),
            mid,
        ))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/minha-performance/minhas-matriculas/<int:mid>", methods=["DELETE"])
def api_minhas_mat_delete(mid):
    user_id, _ = _get_agent_user_id()
    if not user_id:
        return jsonify({"error": "Não autenticado"}), 401
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM agent_matriculas WHERE id = %s", (mid,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({"ok": False, "error": "Não encontrado"}), 404
        if row[0] != user_id and not _is_admin():
            cur.close(); conn.close()
            return jsonify({"ok": False, "error": "Sem permissão"}), 403
        cur.execute("DELETE FROM agent_matriculas WHERE id = %s", (mid,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════
# Solicitações de ajuste — agente
# ══════════════════════════════════════════════════════════════════════════

@minha_performance_bp.route("/api/minha-performance/ajustes", methods=["GET"])
def api_ajustes_agent_list():
    user_id, kommo_uid = _get_agent_user_id()
    if not user_id:
        return jsonify({"error": "Não autenticado"}), 401
    target_uid = request.args.get("kommo_uid", type=int)
    view_user_id = user_id
    if target_uid and _is_admin():
        kommo_uid = target_uid
        try:
            conn_u = _pg()
            cur_u = conn_u.cursor()
            cur_u.execute(
                "SELECT id FROM app_users WHERE kommo_user_id = %s LIMIT 1",
                (kommo_uid,),
            )
            row_u = cur_u.fetchone()
            cur_u.close()
            conn_u.close()
            if row_u:
                view_user_id = row_u[0]
        except Exception:
            pass
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if kommo_uid:
            cur.execute(
                """
                SELECT * FROM matricula_ajustes
                WHERE user_id = %s OR kommo_user_id = %s
                ORDER BY created_at DESC
                """,
                (view_user_id, kommo_uid),
            )
        else:
            cur.execute(
                "SELECT * FROM matricula_ajustes WHERE user_id = %s ORDER BY created_at DESC",
                (view_user_id,),
            )
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            for k in ("data_matricula", "created_at", "resolved_at"):
                if r.get(k):
                    r[k] = str(r[k])
        return jsonify({"ok": True, "ajustes": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/minha-performance/ajustes", methods=["POST"])
def api_ajustes_agent_create():
    user_id, kommo_uid = _get_agent_user_id()
    if not user_id:
        return jsonify({"error": "Não autenticado"}), 401
    b = request.get_json(force=True)
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO matricula_ajustes (user_id, kommo_user_id, tipo, rgm, nome_aluno, curso, polo, data_matricula, kommo_lead_id, descricao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """, (
            user_id, kommo_uid,
            b.get("tipo", "matricula_nao_computada"),
            (b.get("rgm") or "").strip(),
            (b.get("nome_aluno") or "").strip(),
            (b.get("curso") or "").strip(),
            (b.get("polo") or "").strip(),
            b.get("data_matricula") or None,
            (b.get("kommo_lead_id") or "").strip(),
            (b.get("descricao") or "").strip(),
        ))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════
# Solicitações de ajuste — admin
# ══════════════════════════════════════════════════════════════════════════

@minha_performance_bp.route("/api/ajustes-matricula", methods=["GET"])
def api_ajustes_admin_list():
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    status_filter = request.args.get("status")
    try:
        conn = _pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        q = """
            SELECT a.*, u.username AS agent_name
            FROM matricula_ajustes a
            LEFT JOIN app_users u ON u.id = a.user_id
        """
        params = []
        if status_filter:
            q += " WHERE a.status = %s"
            params.append(status_filter)
        q += " ORDER BY a.created_at DESC"
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            for k in ("data_matricula", "created_at", "resolved_at"):
                if r.get(k):
                    r[k] = str(r[k])
        return jsonify({"ok": True, "ajustes": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@minha_performance_bp.route("/api/ajustes-matricula/<int:aid>", methods=["PUT"])
def api_ajustes_admin_update(aid):
    if not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    b = request.get_json(force=True)
    new_status = b.get("status")
    if new_status not in ("pendente", "em_analise", "aprovado", "rejeitado"):
        return jsonify({"ok": False, "error": "Status inválido"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT rgm, kommo_user_id, kommo_lead_id FROM matricula_ajustes WHERE id = %s",
            (aid,),
        )
        ajuste_row = cur.fetchone()
        if not ajuste_row:
            cur.close()
            conn.close()
            return jsonify({"ok": False, "error": "Solicitação não encontrada"}), 404

        resolved = "NOW()" if new_status in ("aprovado", "rejeitado") else "NULL"
        cur.execute(f"""
            UPDATE matricula_ajustes
            SET status = %s, resposta_admin = %s, admin_user_id = %s, resolved_at = {resolved}
            WHERE id = %s
        """, (
            new_status,
            (b.get("resposta_admin") or "").strip() or None,
            session.get("user_id"),
            aid,
        ))

        conflito_aplicado = False
        conflito_aviso = None
        if new_status == "aprovado":
            rgm, kommo_uid, lead_id = ajuste_row[0], ajuste_row[1], ajuste_row[2]
            if kommo_uid and _upsert_conflito_resolucao(cur, rgm, kommo_uid):
                conflito_aplicado = True
            elif kommo_uid and not _normalize_rgm(rgm):
                conflito_aviso = (
                    "Aprovado, mas sem RGM válido — crédito manual em Vendas em Conflito pode ser necessário."
                )
                logger.warning(
                    "ajuste %s aprovado sem RGM (lead=%s, kommo_uid=%s)",
                    aid, lead_id, kommo_uid,
                )
            elif not kommo_uid:
                conflito_aviso = "Aprovado, mas o agente não tem kommo_user_id vinculado."
                logger.warning("ajuste %s aprovado sem kommo_user_id", aid)

        conn.commit()
        cur.close()
        conn.close()
        out = {"ok": True, "conflito_aplicado": conflito_aplicado}
        if conflito_aviso:
            out["aviso"] = conflito_aviso
        return jsonify(out)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
