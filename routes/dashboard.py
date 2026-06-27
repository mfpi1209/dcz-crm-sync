import re
import traceback
import unicodedata
from datetime import date, datetime

import psycopg2
import psycopg2.extras
import requests
from flask import Blueprint, render_template, request, jsonify, current_app

from db import get_conn
from helpers import BRT, to_brt, normalize_polo_display

dashboard_bp = Blueprint("dashboard", __name__)


def _strip_accents_lower(s):
    return unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode('ascii').lower()


def _classify_tipo(raw):
    """Classifica tipo_matricula por substring, tolerante a acentos e variações."""
    if not raw or raw.strip() in ('', 'Não informado', 'N/I'):
        return 'outros'
    s = _strip_accents_lower(raw)
    if 'remat' in s or 'renovacao' in s or 'veterano' in s:
        return 'rematricula'
    if 'regresso' in s or 'retorno' in s:
        return 'regresso'
    if 'recompra' in s:
        return 'recompra'
    if 'matricula' in s or 'calouro' in s:
        return 'novos'
    return 'outros'


def _get_process_state():
    from routes.crm import _sync_running, _update_running
    return _sync_running, _update_running


def _is_rematricula_empresa(empresa):
    """Rematrícula conta só Graduação EAD (código 12). Pós (7) e UCS-CL (79) ficam fora."""
    return bool(re.match(r'^12 -', (empresa or '').strip()))


def _mat_cte_sql(empresa_re=r'^(12|7) -'):
    return f"""
WITH mat AS (
    SELECT
        r.data->>'tipo_matricula' AS tipo_aluno,
        r.data->>'empresa' AS empresa,
        CASE
          WHEN r.data->>'data_mat' ~ '^\\d{{2}}/\\d{{2}}/\\d{{4}}' THEN
            TO_DATE(SUBSTRING(r.data->>'data_mat' FROM 1 FOR 10), 'DD/MM/YYYY')
          WHEN r.data->>'data_mat' ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN
            (SUBSTRING(r.data->>'data_mat' FROM 1 FOR 10))::date
          ELSE NULL
        END AS data_matricula,
        r.data->>'situacao' AS situacao,
        CASE
          WHEN COALESCE(r.data->>'nivel','') != '' THEN
            CASE WHEN r.data->>'nivel' ~* 'p[oó]s' THEN 'Pós-Graduação'
                 ELSE 'Graduação' END
          WHEN r.data->>'negocio' ~* 'p[oó]s' THEN 'Pós-Graduação'
          WHEN r.data->>'curso' ~* '(mba|especializa[cç][aã]o|p[oó]s.gradua|lato.sensu|stricto)'
               THEN 'Pós-Graduação'
          ELSE 'Graduação'
        END AS nivel,
        TRIM(REGEXP_REPLACE(COALESCE(r.data->>'polo',''), '^\\d+\\s*[-–]\\s*', '')) AS polo,
        NULLIF(regexp_replace(COALESCE(r.data->>'rgm',''), '[^0-9]', '', 'g'), '') AS rgm,
        r.data->>'curso' AS turma,
        CASE
          WHEN TRIM(COALESCE(r.data->>'ciclo','')) ~ '^\\d{{4}}/\\d$'
            THEN TRIM(r.data->>'ciclo')
          ELSE NULL
        END AS ciclo
    FROM xl_rows r
    WHERE r.snapshot_id = (
        SELECT id FROM xl_snapshots
        WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
    )
      AND TRIM(COALESCE(r.data->>'empresa','')) ~ '{empresa_re}'
)
"""


# Dashboard geral: Graduação (12) + Pós UCS (7).
_MAT_CTE = _mat_cte_sql(r'^(12|7) -')
# Rematrícula: só Graduação EAD (12) — Pós não rematricula; UCS-CL (79) fora do escopo.
_MAT_CTE_REMAT = _mat_cte_sql(r'^12 -')

# Data em que o SIAA liberou rematrícula por ciclo destino (antes disso não há conversão).
_REMAT_LIBERACAO_POR_CICLO = {
    "2026/2": date(2026, 6, 14),
}

_REMAT_TIPO_SQL = """
  AND LOWER(COALESCE(m.tipo_aluno, '')) ~ '(remat|renovacao|veterano)'
"""


def _remat_liberacao_min(ciclo):
    if not ciclo:
        return None
    return _REMAT_LIBERACAO_POR_CICLO.get(str(ciclo).strip())


def _clamp_remat_range_start(range_start, ciclo):
    lib = _remat_liberacao_min(ciclo)
    if lib and range_start < lib:
        return lib
    return range_start


def _situacao_norm_sql(col):
    """Comparação de situação sem acento/caixa (ex.: EM CURSO = Em Curso)."""
    return (
        f"TRANSLATE(LOWER(TRIM(COALESCE({col},''))), "
        f"'áàãâéèêíìîóòõôúùûçñ', 'aaaaeeeiiioooouuucn')"
    )


def _norm_situacao_param(s):
    if not s or not str(s).strip():
        return None
    return _strip_accents_lower(str(s).strip())


def _tipo_matches_filter(cat, f_tipo):
    if not f_tipo:
        return True
    _NOVOS_AGG = {"novos", "regresso", "recompra"}
    if f_tipo == "novos_agg":
        return cat in _NOVOS_AGG
    return cat == f_tipo


def _is_em_curso_sit(sit_norm):
    return sit_norm == "em curso"


def _rgm_fora_padrao_nao_conta(rgm, dom_pfx, overrides):
    from routes.comercial_rgm import (
        _is_rgm_prefix_outlier,
        _rgm_conta_para_venda,
    )

    if not rgm or dom_pfx is None:
        return False
    return _is_rgm_prefix_outlier(rgm, dom_pfx) and not _rgm_conta_para_venda(
        rgm, dom_pfx, overrides
    )


def _matches_rgm_padrao_filter(rgm, sit_norm, mode, dom_pfx, overrides):
    """mode: todos | padrao | fora_padrao — mesma regra do Dashboard Comercial."""
    mode = (mode or "todos").strip().lower()
    if mode in ("", "todos"):
        return True
    if not _is_em_curso_sit(sit_norm):
        return mode != "fora_padrao"
    if _rgm_fora_padrao_nao_conta(rgm, dom_pfx, overrides):
        return mode == "fora_padrao"
    return mode != "fora_padrao"


# ---------------------------------------------------------------------------
# Rota — Index
# ---------------------------------------------------------------------------

@dashboard_bp.route("/")
def index():
    resp = current_app.make_response(render_template("index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


# ---------------------------------------------------------------------------
# Rotas — Dashboard
# ---------------------------------------------------------------------------

@dashboard_bp.route("/api/dashboard")
def api_dashboard():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, tipo, filename, row_count, uploaded_at
                FROM xl_snapshots WHERE tipo = 'matriculados'
                ORDER BY id DESC LIMIT 1
            """)
            snap = cur.fetchone()
            diag = None
            if snap:
                snap["uploaded_at"] = to_brt(snap["uploaded_at"])
                cur.execute("""
                    SELECT
                        ARRAY_AGG(DISTINCT r.data->>'negocio') FILTER (WHERE r.data->>'negocio' IS NOT NULL AND r.data->>'negocio' != '') AS negocio_vals,
                        ARRAY_AGG(DISTINCT r.data->>'nivel')   FILTER (WHERE r.data->>'nivel' IS NOT NULL AND r.data->>'nivel' != '')     AS nivel_vals,
                        ARRAY_AGG(DISTINCT r.data->>'tipo_matricula') FILTER (WHERE r.data->>'tipo_matricula' IS NOT NULL AND r.data->>'tipo_matricula' != '') AS tipo_vals
                    FROM (SELECT r.data FROM xl_rows r WHERE r.snapshot_id = %s LIMIT 500) r
                """, (snap["id"],))
                diag = cur.fetchone()

        _sync_running, _update_running = _get_process_state()

        return jsonify({
            "snapshot": snap,
            "sync_running": _sync_running,
            "update_running": _update_running,
            "diag": diag,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Dashboard: Métricas de Alunos (from xl_rows snapshots)
# ---------------------------------------------------------------------------

_STUDENT_METRICS_QUERY = _MAT_CTE + """
SELECT
    COALESCE(m.tipo_aluno, 'Não informado') AS tipo,
    m.empresa,
    m.situacao,
    m.nivel,
    m.polo,
    m.turma,
    m.ciclo AS ciclo,
    m.rgm,
    COUNT(*) AS total
FROM mat m
WHERE (%(dt_from)s IS NULL OR m.data_matricula >= %(dt_from)s::date)
  AND (%(dt_to)s   IS NULL OR m.data_matricula <= %(dt_to)s::date)
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
  AND (%(f_ciclo)s IS NULL OR m.ciclo = %(f_ciclo)s)
GROUP BY m.tipo_aluno, m.situacao, m.nivel, m.polo, m.turma, m.ciclo, m.empresa, m.rgm
""".format(_SIT_NORM=_situacao_norm_sql("m.situacao")) + """
ORDER BY total DESC
"""


@dashboard_bp.route("/api/dashboard/students")
def api_dashboard_students():
    dt_from = request.args.get("from", "")
    dt_to = request.args.get("to", "")
    f_nivel = request.args.get("nivel", "")
    f_sit = request.args.get("situacao", "")
    f_ciclo = request.args.get("ciclo", "")
    f_tipo = request.args.get("tipo", "")
    f_polo = request.args.get("polo", "")
    f_rgm_padrao = (request.args.get("rgm_padrao", "") or "todos").strip().lower()
    if f_rgm_padrao not in ("todos", "padrao", "fora_padrao"):
        f_rgm_padrao = "todos"
    f_sit_norm = _norm_situacao_param(f_sit)
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_STUDENT_METRICS_QUERY, {
                "dt_from": dt_from or None,
                "dt_to": dt_to or None,
                "f_nivel": f_nivel or None,
                "f_sit_norm": f_sit_norm,
                "f_ciclo": f_ciclo or None,
            })
            rows = cur.fetchall()

        from routes.comercial_rgm import (
            _compute_dominant_rgm_prefix,
            _load_outlier_contagem_overrides,
        )

        em_curso_rgms = []
        for r in rows:
            sn = _norm_situacao_param(r.get("situacao") or "")
            if _is_em_curso_sit(sn) and r.get("rgm"):
                em_curso_rgms.append(r["rgm"])
        dom_pfx = _compute_dominant_rgm_prefix(em_curso_rgms)
        overrides = _load_outlier_contagem_overrides(conn)
        fora_padrao_total = 0
        for r in rows:
            sn = _norm_situacao_param(r.get("situacao") or "")
            if _is_em_curso_sit(sn) and _rgm_fora_padrao_nao_conta(
                r.get("rgm"), dom_pfx, overrides
            ):
                fora_padrao_total += r["total"]

        totals = {"novos": 0, "regresso": 0, "recompra": 0, "rematricula": 0, "outros": 0}
        by_situacao = {}
        by_nivel = {}
        by_polo = {}
        by_turma = {}
        by_ciclo = {}
        by_tipo_detail = {}
        raw_tipos = {}

        for r in rows:
            tipo = r["tipo"] or "Não informado"
            cat = _classify_tipo(tipo)
            if cat == "rematricula" and not _is_rematricula_empresa(r.get("empresa")):
                continue

            sit = r["situacao"] or "N/I"
            sit_norm = _norm_situacao_param(sit) or ""
            niv = r["nivel"] or "N/I"
            polo = normalize_polo_display(r["polo"] or "") or "N/I"
            turma = r["turma"] or "N/I"
            ciclo = r["ciclo"] or "N/I"

            if f_polo and polo != f_polo:
                continue

            if not _matches_rgm_padrao_filter(
                r.get("rgm"), sit_norm, f_rgm_padrao, dom_pfx, overrides
            ):
                continue

            # Cards de situação: sempre mostram todas as situações (respeitam ciclo/período/tipo/polo).
            if not f_tipo or _tipo_matches_filter(cat, f_tipo):
                by_situacao[sit] = by_situacao.get(sit, 0) + r["total"]

            if f_sit_norm and sit_norm != f_sit_norm:
                continue

            totals[cat] += r["total"]
            raw_tipos[tipo] = raw_tipos.get(tipo, 0) + r["total"]

            if cat not in by_tipo_detail:
                by_tipo_detail[cat] = {"by_situacao": {}, "by_nivel": {}, "by_polo": {}}
            td = by_tipo_detail[cat]

            td["by_situacao"][sit] = td["by_situacao"].get(sit, 0) + r["total"]
            td["by_nivel"][niv] = td["by_nivel"].get(niv, 0) + r["total"]
            td["by_polo"][polo] = td["by_polo"].get(polo, 0) + r["total"]

            if _tipo_matches_filter(cat, f_tipo):
                by_nivel[niv] = by_nivel.get(niv, 0) + r["total"]
                by_polo[polo] = by_polo.get(polo, 0) + r["total"]
                by_turma[turma] = by_turma.get(turma, 0) + r["total"]
                by_ciclo[ciclo] = by_ciclo.get(ciclo, 0) + r["total"]

        for cat in by_tipo_detail:
            td = by_tipo_detail[cat]
            td["by_situacao"] = dict(sorted(td["by_situacao"].items(), key=lambda x: -x[1]))
            td["by_nivel"] = dict(sorted(td["by_nivel"].items(), key=lambda x: -x[1]))
            td["by_polo"] = dict(sorted(td["by_polo"].items(), key=lambda x: -x[1])[:8])

        filtered_total = sum(totals.values())
        display_totals = dict(totals)
        if f_tipo:
            display_totals = {
                k: (totals[k] if _tipo_matches_filter(k, f_tipo) else 0)
                for k in totals
            }
            filtered_total = sum(display_totals.values())

        return jsonify({
            "totals": display_totals,
            "by_tipo_detail": by_tipo_detail,
            "by_situacao": dict(sorted(by_situacao.items(), key=lambda x: -x[1])),
            "by_nivel": dict(sorted(by_nivel.items(), key=lambda x: -x[1])),
            "by_polo": dict(sorted(by_polo.items(), key=lambda x: -x[1])),
            "by_turma": dict(sorted(by_turma.items(), key=lambda x: -x[1])),
            "by_ciclo": dict(sorted(by_ciclo.items(), key=lambda x: -x[1])),
            "grand_total": filtered_total,
            "raw_tipos": dict(sorted(raw_tipos.items(), key=lambda x: -x[1])),
            "filter": {"from": dt_from, "to": dt_to},
            "active_tipo": f_tipo,
            "active_situacao": f_sit or None,
            "active_polo": f_polo or None,
            "active_rgm_padrao": f_rgm_padrao,
            "rgm_padrao": {
                "filter": f_rgm_padrao,
                "dominant_prefix": dom_pfx,
                "fora_padrao_total": fora_padrao_total,
            },
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@dashboard_bp.route("/api/dashboard/funnel-yesterday")
def api_dashboard_funnel_yesterday():
    """KPI Fechado Ganho Ontem + leads ontem (desacoplado do cache do funil live)."""
    force = request.args.get("force", "0") == "1"
    try:
        from datetime import date as _date
        from routes.kommo_sync import _get_yesterday_summary_cached
        from routes.comercial_rgm import comercial_periodo_vendas_resumo

        data = _get_yesterday_summary_cached(force=force)
        if not data.get("vendas"):
            y = _date.fromisoformat(data["date"])
            y_str = y.isoformat()
            resumo = comercial_periodo_vendas_resumo(dt_ini=y_str, dt_fim=y_str)
            by_day = resumo.get("mat_by_date") or {}
            data["vendas"] = int(by_day.get(y) or resumo.get("vendas_liquidas") or 0)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        current_app.logger.exception("funnel-yesterday: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@dashboard_bp.route("/api/dashboard/ciclos-distinct")
def api_dashboard_ciclos_distinct():
    """Retorna ciclos distintos presentes no snapshot atual de matriculados,
    com contagem por ciclo. Usado para popular o dropdown de filtro de ciclo
    no Dashboard Acadêmico (mais confiável que /api/ciclos da tabela `ciclos`,
    que pode estar desatualizada — ex.: 2026/2 não cadastrado).

    Query params:
      tipo=rematricula — inclui apenas contagem de rematrículas no campo total
                         (retrocompat: rematricula.js usa isso no dropdown).
    """
    count_remat_only = request.args.get("tipo", "").strip().lower() == "rematricula"
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                  TRIM(r.data->>'ciclo') AS ciclo,
                  COUNT(*) AS total,
                  COUNT(*) FILTER (
                    WHERE LOWER(COALESCE(r.data->>'tipo_matricula', '')) ~ '(remat|renovacao|veterano)'
                      AND TRIM(COALESCE(r.data->>'empresa','')) ~ '^12 -'
                  ) AS rematricula
                FROM xl_rows r
                WHERE r.snapshot_id = (
                    SELECT id FROM xl_snapshots
                    WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
                )
                  AND TRIM(COALESCE(r.data->>'ciclo','')) ~ '^\\d{4}/\\d$'
                  AND TRIM(COALESCE(r.data->>'empresa','')) ~ '^(12|7) -'
                GROUP BY 1
                ORDER BY
                  (substring(TRIM(r.data->>'ciclo') from '^([0-9]{4})'))::int DESC,
                  (substring(TRIM(r.data->>'ciclo') from '/([0-9])$'))::int DESC
            """)
            rows = cur.fetchall()
        result = []
        for r in rows:
            remat = int(r["rematricula"] or 0)
            total = int(r["total"] or 0)
            result.append({
                "nome": r["ciclo"],
                "total": remat if count_remat_only else total,
                "matriculados": total,
                "rematricula": remat,
            })
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Dashboard Timeline (gráficos de linha com drill-down)
# ---------------------------------------------------------------------------

_TIMELINE_RANGE_QUERY = _MAT_CTE + """
SELECT MIN(m.data_matricula) AS dmin, MAX(m.data_matricula) AS dmax
FROM mat m
WHERE m.data_matricula IS NOT NULL
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
  AND (%(f_ciclo)s IS NULL OR m.ciclo = %(f_ciclo)s)
  AND (%(f_sit_norm)s IS NULL OR {sit} = %(f_sit_norm)s)
""".format(sit=_situacao_norm_sql("m.situacao"))

_TIMELINE_QUERY = _MAT_CTE + """
SELECT
    CASE WHEN %(granularity)s = 'month'
         THEN TO_CHAR(m.data_matricula, 'YYYY-MM')
         ELSE TO_CHAR(m.data_matricula, 'YYYY-MM-DD')
    END AS period,
    COALESCE(m.tipo_aluno, 'Não informado') AS tipo,
    m.situacao,
    TRIM(REGEXP_REPLACE(COALESCE(m.polo,''), '^\\d+\\s*[-–]\\s*', '')) AS polo,
    m.empresa,
    COUNT(*) AS total
FROM mat m
WHERE m.data_matricula IS NOT NULL
  AND m.data_matricula BETWEEN %(range_start)s AND %(range_end)s
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
  AND (%(f_ciclo)s IS NULL OR m.ciclo = %(f_ciclo)s)
  AND (%(f_sit_norm)s IS NULL OR {sit} = %(f_sit_norm)s)
GROUP BY period, m.tipo_aluno, m.situacao, m.polo, m.empresa
ORDER BY period, total DESC
""".format(sit=_situacao_norm_sql("m.situacao"))

_TIMELINE_RANGE_QUERY_REMAT = _MAT_CTE_REMAT + """
SELECT MIN(m.data_matricula) AS dmin, MAX(m.data_matricula) AS dmax
FROM mat m
WHERE m.data_matricula IS NOT NULL
""" + _REMAT_TIPO_SQL + """
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
  AND (%(f_ciclo)s IS NULL OR m.ciclo = %(f_ciclo)s)
  AND (%(f_sit_norm)s IS NULL OR {sit} = %(f_sit_norm)s)
""".format(sit=_situacao_norm_sql("m.situacao"))

_TIMELINE_QUERY_REMAT = _MAT_CTE_REMAT + """
SELECT
    CASE WHEN %(granularity)s = 'month'
         THEN TO_CHAR(m.data_matricula, 'YYYY-MM')
         ELSE TO_CHAR(m.data_matricula, 'YYYY-MM-DD')
    END AS period,
    COALESCE(m.tipo_aluno, 'Não informado') AS tipo,
    m.situacao,
    m.polo,
    m.empresa,
    COUNT(*) AS total
FROM mat m
WHERE m.data_matricula IS NOT NULL
""" + _REMAT_TIPO_SQL + """
  AND m.data_matricula BETWEEN %(range_start)s AND %(range_end)s
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
  AND (%(f_ciclo)s IS NULL OR m.ciclo = %(f_ciclo)s)
  AND (%(f_sit_norm)s IS NULL OR {sit} = %(f_sit_norm)s)
GROUP BY period, m.tipo_aluno, m.situacao, m.polo, m.empresa
ORDER BY period, total DESC
""".format(sit=_situacao_norm_sql("m.situacao"))


@dashboard_bp.route("/api/dashboard/timeline")
def api_dashboard_timeline():
    """Retorna dados de timeline agrupados por mês ou dia, para gráficos de linha."""
    from dateutil.relativedelta import relativedelta

    granularity = request.args.get("granularity", "month")
    f_nivel = request.args.get("nivel") or None
    f_ciclo = request.args.get("ciclo") or None
    f_sit = request.args.get("situacao", "")
    f_tipo = request.args.get("tipo", "")
    f_polo = request.args.get("polo", "")
    f_sit_norm = _norm_situacao_param(f_sit)
    dt_from = request.args.get("from", "")
    dt_to = request.args.get("to", "")
    remat_scope = request.args.get("scope", "").strip().lower() == "rematricula"
    range_q = _TIMELINE_RANGE_QUERY_REMAT if remat_scope else _TIMELINE_RANGE_QUERY
    timeline_q = _TIMELINE_QUERY_REMAT if remat_scope else _TIMELINE_QUERY

    today = datetime.now().date()

    if dt_from:
        range_start = datetime.strptime(dt_from, "%Y-%m-%d").date()
    else:
        range_start = today - relativedelta(months=6)

    if dt_to:
        range_end = datetime.strptime(dt_to, "%Y-%m-%d").date()
    else:
        range_end = today

    tl_params = {
        "f_nivel": f_nivel,
        "f_ciclo": f_ciclo,
        "f_sit_norm": f_sit_norm,
    }

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if f_ciclo and not dt_from and not dt_to:
                cur.execute(range_q, tl_params)
                bounds = cur.fetchone()
                if bounds and bounds.get("dmin"):
                    range_start = bounds["dmin"]
                    range_end = bounds["dmax"] or today

            if remat_scope:
                range_start = _clamp_remat_range_start(range_start, f_ciclo)

            cur.execute(timeline_q, {
                **tl_params,
                "granularity": granularity,
                "range_start": range_start,
                "range_end": range_end,
            })
            rows = cur.fetchall()

        series = {}
        all_periods = set()
        for r in rows:
            cat = _classify_tipo(r["tipo"] or "")
            if cat == "rematricula" and not _is_rematricula_empresa(r.get("empresa")):
                continue
            if not _tipo_matches_filter(cat, f_tipo):
                continue
            polo_canon = normalize_polo_display(r.get("polo") or "") or "N/I"
            if f_polo and polo_canon != f_polo:
                continue
            p = r["period"]
            all_periods.add(p)
            if cat not in series:
                series[cat] = {}
            series[cat][p] = series[cat].get(p, 0) + r["total"]

        periods = sorted(all_periods)

        result = {
            "periods": periods,
            "series": {},
            "granularity": granularity,
            "range": {"from": str(range_start), "to": str(range_end)},
            "meta": {
                "ciclo": f_ciclo,
                "nivel": f_nivel,
                "remat_liberacao": (
                    str(_remat_liberacao_min(f_ciclo)) if remat_scope and _remat_liberacao_min(f_ciclo) else None
                ),
            },
        }
        for cat in ["novos", "rematricula", "regresso", "recompra"]:
            if cat in series:
                result["series"][cat] = [series[cat].get(p, 0) for p in periods]

        total_series = [0] * len(periods)
        for cat, vals in result["series"].items():
            for i, v in enumerate(vals):
                total_series[i] += v
        result["series"]["total"] = total_series

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Dashboard Ciclos (Master Panel)
# ---------------------------------------------------------------------------

_CICLO_COMPARE_QUERY = _MAT_CTE + """
SELECT
    c.nome AS ciclo, c.nivel AS ciclo_nivel,
    COALESCE(m.tipo_aluno, 'Não informado') AS tipo,
    m.situacao, m.nivel, m.polo, COUNT(*) AS total
FROM mat m
INNER JOIN ciclos c ON c.nivel = m.nivel
    AND m.data_matricula IS NOT NULL
    AND m.data_matricula BETWEEN c.dt_inicio AND c.dt_fim
GROUP BY c.nome, c.nivel, m.tipo_aluno, m.situacao, m.nivel, m.polo
ORDER BY c.nome, total DESC
"""

_DATE_RANGE_QUERY = _MAT_CTE + """
SELECT
    COALESCE(m.tipo_aluno, 'Não informado') AS tipo,
    m.situacao, m.nivel, m.polo, COUNT(*) AS total
FROM mat m
WHERE m.data_matricula IS NOT NULL
  AND m.data_matricula BETWEEN %(range_start)s AND %(range_end)s
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
GROUP BY m.tipo_aluno, m.situacao, m.nivel, m.polo
ORDER BY total DESC
"""


def _aggregate_rows(rows):
    result = {
        "totals": {"novos": 0, "regresso": 0, "recompra": 0, "rematricula": 0, "outros": 0},
        "by_situacao": {}, "by_polo": {}, "grand_total": 0,
    }
    for r in rows:
        tipo = r["tipo"] or "Não informado"
        cat = _classify_tipo(tipo)
        result["totals"][cat] += r["total"]
        result["grand_total"] += r["total"]
        sit = r["situacao"] or "N/I"
        result["by_situacao"][sit] = result["by_situacao"].get(sit, 0) + r["total"]
        polo = normalize_polo_display(r["polo"] or "") or "N/I"
        result["by_polo"][polo] = result["by_polo"].get(polo, 0) + r["total"]
    result["by_situacao"] = dict(sorted(result["by_situacao"].items(), key=lambda x: -x[1]))
    result["by_polo"] = dict(sorted(result["by_polo"].items(), key=lambda x: -x[1]))
    return result


@dashboard_bp.route("/api/dashboard/ciclos")
def api_dashboard_ciclos():
    """Retorna métricas por ciclo + comparações temporais (YTD vs ano anterior, vs 6 meses)."""
    from dateutil.relativedelta import relativedelta

    f_nivel = request.args.get("nivel") or None

    conn = get_conn()
    try:
        today = datetime.now().date()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if f_nivel:
                cur.execute("SELECT nome, nivel, dt_inicio, dt_fim FROM ciclos WHERE nivel = %s ORDER BY dt_inicio", (f_nivel,))
            else:
                cur.execute("SELECT nome, nivel, dt_inicio, dt_fim FROM ciclos ORDER BY dt_inicio")
            ciclos_config = cur.fetchall()

            cur.execute("""
                SELECT nivel, COUNT(*) AS total FROM (
                    SELECT CASE
                      WHEN COALESCE(r.data->>'nivel','') != '' THEN
                        CASE WHEN r.data->>'nivel' ~* 'p[oó]s' THEN 'Pós-Graduação'
                             ELSE 'Graduação' END
                      WHEN r.data->>'negocio' ~* 'p[oó]s' THEN 'Pós-Graduação'
                      WHEN r.data->>'curso' ~* '(mba|especializa[cç][aã]o|p[oó]s.gradua|lato.sensu|stricto)'
                           THEN 'Pós-Graduação'
                      ELSE 'Graduação'
                    END AS nivel
                    FROM xl_rows r
                    WHERE r.snapshot_id = (
                        SELECT id FROM xl_snapshots
                        WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
                    )
                ) sub GROUP BY nivel ORDER BY total DESC
            """, {})
            distinct_nivels = {r["nivel"]: r["total"] for r in cur.fetchall()}

            cur.execute(_CICLO_COMPARE_QUERY, {})
            cycle_rows = cur.fetchall()
            if f_nivel:
                cycle_rows = [r for r in cycle_rows if r.get("ciclo_nivel") == f_nivel]

            ytd_start = today.replace(month=1, day=1)
            cur.execute(_DATE_RANGE_QUERY, {
                "range_start": ytd_start, "range_end": today, "f_nivel": f_nivel,
            })
            ytd_current = cur.fetchall()

            ytd_prev_start = ytd_start.replace(year=today.year - 1)
            ytd_prev_end = today.replace(year=today.year - 1)
            cur.execute(_DATE_RANGE_QUERY, {
                "range_start": ytd_prev_start, "range_end": ytd_prev_end, "f_nivel": f_nivel,
            })
            ytd_previous = cur.fetchall()

            m6_start = today - relativedelta(months=6)
            cur.execute(_DATE_RANGE_QUERY, {
                "range_start": m6_start, "range_end": today, "f_nivel": f_nivel,
            })
            m6_current = cur.fetchall()

            m6_prev_start = today - relativedelta(months=12)
            m6_prev_end = today - relativedelta(months=6)
            cur.execute(_DATE_RANGE_QUERY, {
                "range_start": m6_prev_start, "range_end": m6_prev_end, "f_nivel": f_nivel,
            })
            m6_previous = cur.fetchall()

        ciclos = {}
        for r in cycle_rows:
            cn = r["ciclo"]
            if cn not in ciclos:
                ciclos[cn] = {"nome": cn, "nivel": r["ciclo_nivel"],
                              "totals": {"novos": 0, "regresso": 0, "recompra": 0, "rematricula": 0, "outros": 0},
                              "by_situacao": {}, "by_polo": {}, "grand_total": 0}
            c = ciclos[cn]
            cat = _classify_tipo(r["tipo"] or "")
            c["totals"][cat] += r["total"]
            c["grand_total"] += r["total"]
            sit = r["situacao"] or "N/I"
            c["by_situacao"][sit] = c["by_situacao"].get(sit, 0) + r["total"]
            polo = normalize_polo_display(r["polo"] or "") or "N/I"
            c["by_polo"][polo] = c["by_polo"].get(polo, 0) + r["total"]
        for cn in ciclos:
            ciclos[cn]["by_situacao"] = dict(sorted(ciclos[cn]["by_situacao"].items(), key=lambda x: -x[1]))
            ciclos[cn]["by_polo"] = dict(sorted(ciclos[cn]["by_polo"].items(), key=lambda x: -x[1]))

        config_list = []
        for cc in ciclos_config:
            row = dict(cc)
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
            config_list.append(row)

        return jsonify({
            "ciclos": sorted(ciclos.values(), key=lambda x: x["nome"], reverse=True),
            "config": config_list,
            "distinct_nivels": distinct_nivels,
            "comparisons": {
                "ytd": {
                    "label": f"YTD {today.year}",
                    "period": f"{ytd_start.isoformat()} → {today.isoformat()}",
                    "current": _aggregate_rows(ytd_current),
                },
                "ytd_prev": {
                    "label": f"YTD {today.year - 1}",
                    "period": f"{ytd_prev_start.isoformat()} → {ytd_prev_end.isoformat()}",
                    "current": _aggregate_rows(ytd_previous),
                },
                "m6": {
                    "label": "Últimos 6 meses",
                    "period": f"{m6_start.isoformat()} → {today.isoformat()}",
                    "current": _aggregate_rows(m6_current),
                },
                "m6_prev": {
                    "label": "6 meses anteriores",
                    "period": f"{m6_prev_start.isoformat()} → {m6_prev_end.isoformat()}",
                    "current": _aggregate_rows(m6_previous),
                },
            },
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Turmas (constantes e helpers usados pelo dashboard)
# ---------------------------------------------------------------------------

GRAD_MONTHS = [2, 3, 4, 5, 8, 9, 10, 11]
POS_MONTHS = list(range(1, 13))
MONTH_NAMES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}


def _turma_defaults(nivel, ano):
    """Gera ranges padrão de turmas para um nível/ano."""
    import calendar
    months = GRAD_MONTHS if nivel == "Graduação" else POS_MONTHS
    rows = []
    for m in months:
        last_day = calendar.monthrange(ano, m)[1]
        rows.append({
            "nivel": nivel,
            "nome": f"{MONTH_NAMES[m]} {ano}",
            "dt_inicio": f"{ano}-{m:02d}-01",
            "dt_fim": f"{ano}-{m:02d}-{last_day:02d}",
            "ano": ano,
        })
    return rows


# ---------------------------------------------------------------------------
# Rotas — Meta Campaigns (Marketing Performance Dashboard)
# ---------------------------------------------------------------------------

@dashboard_bp.route("/api/meta/campaigns")
def api_meta_campaigns():
    """Busca dados de campanhas do Meta Ads via webhook do n8n."""
    import requests
    
    WEBHOOK_URL = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/count_leads_meta"
    
    from_date = request.args.get("from", "")
    to_date = request.args.get("to", "")
    
    try:
        payload = {}
        if from_date:
            payload["from"] = from_date
        if to_date:
            payload["to"] = to_date
        
        response = requests.post(WEBHOOK_URL, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if isinstance(data, list):
            campaigns = data
        elif isinstance(data, dict) and "campaigns" in data:
            campaigns = data["campaigns"]
        else:
            campaigns = [data] if data else []
        
        return jsonify({
            "campaigns": campaigns,
            "status": "OK",
            "count": len(campaigns)
        })
    except requests.exceptions.Timeout:
        return jsonify({
            "campaigns": [],
            "status": "TIMEOUT",
            "error": "Webhook não respondeu a tempo"
        })
    except requests.exceptions.RequestException as e:
        traceback.print_exc()
        return jsonify({
            "campaigns": [],
            "status": "ERROR",
            "error": str(e)
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "campaigns": [],
            "status": "ERROR",
            "error": str(e)
        })


# ---------------------------------------------------------------------------
# Rotas — Google Campaigns (Marketing Performance Dashboard)
# ---------------------------------------------------------------------------

SUPABASE_GOOGLE_URL = "https://vtlbndvcgajcoajhcnnx.supabase.co"
SUPABASE_GOOGLE_KEY = "sb_publishable_sW0h7aqgrjiwqGqKpawm4g_FuMi5xU_"


def _fetch_paginated(url_base, headers, limit=1000):
    """Fetches all pages from url_base (must already contain select + filter params, no limit/offset)."""
    all_rows = []
    offset = 0
    while True:
        url = f"{url_base}&limit={limit}&offset={offset}"
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not isinstance(batch, list) or len(batch) == 0:
            break
        all_rows.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return all_rows


@dashboard_bp.route("/api/google/campaigns")
def api_google_campaigns():
    """Busca dados de campanhas do Google Ads diretamente do Supabase.

    Leads novos (novo_ganho_perdido IS NULL) são filtrados por data_criacao.
    Leads ganhos/perdidos são filtrados por updated_at.
    """
    from_date = request.args.get("from", "")
    to_date = request.args.get("to", "")

    try:
        headers = {
            "apikey": SUPABASE_GOOGLE_KEY,
            "Authorization": f"Bearer {SUPABASE_GOOGLE_KEY}",
        }

        base_select = (
            f"{SUPABASE_GOOGLE_URL}/rest/v1/campanhas_google"
            f"?select=utm_campaign,utm_source,utm_medium,novo_ganho_perdido"
        )

        # Request A — novos (novo_ganho_perdido IS NULL), filtrar por data_criacao
        url_a = f"{base_select}&novo_ganho_perdido=is.null"
        if from_date:
            url_a += f"&data_criacao=gte.{from_date}T00:00:00"
        if to_date:
            url_a += f"&data_criacao=lte.{to_date}T23:59:59.999"

        # Request B — ganhos/perdidos, filtrar por updated_at
        url_b = f"{base_select}&novo_ganho_perdido=in.(ganho,perdido)"
        if from_date:
            url_b += f"&updated_at=gte.{from_date}T00:00:00"
        if to_date:
            url_b += f"&updated_at=lte.{to_date}T23:59:59.999"

        rows_a = _fetch_paginated(url_a, headers)
        rows_b = _fetch_paginated(url_b, headers)
        all_rows = rows_a + rows_b

        grouped = {}
        for row in all_rows:
            key = row.get("utm_campaign") or "Sem nome"
            if key not in grouped:
                grouped[key] = {
                    "utm_campaign": row.get("utm_campaign"),
                    "utm_source": None,
                    "utm_medium": None,
                    "novos": 0,
                    "ganhos": 0,
                    "perdidos": 0,
                }

            if grouped[key]["utm_source"] is None and row.get("utm_source"):
                grouped[key]["utm_source"] = row["utm_source"]
            if grouped[key]["utm_medium"] is None and row.get("utm_medium"):
                grouped[key]["utm_medium"] = row["utm_medium"]

            ngp = (row.get("novo_ganho_perdido") or "").strip().lower()
            if ngp == "ganho":
                grouped[key]["ganhos"] += 1
            elif ngp == "perdido":
                grouped[key]["perdidos"] += 1
            else:
                grouped[key]["novos"] += 1

        campaigns = list(grouped.values())

        return jsonify({
            "campaigns": campaigns,
            "status": "OK",
            "count": len(campaigns),
        })
    except requests.exceptions.Timeout:
        return jsonify({
            "campaigns": [],
            "status": "TIMEOUT",
            "error": "Supabase não respondeu a tempo",
        })
    except requests.exceptions.RequestException as e:
        traceback.print_exc()
        return jsonify({
            "campaigns": [],
            "status": "ERROR",
            "error": str(e),
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "campaigns": [],
            "status": "ERROR",
            "error": str(e),
        })


# ---------------------------------------------------------------------------
# Rotas — Sem Campanha (Marketing Performance Dashboard)
# ---------------------------------------------------------------------------

@dashboard_bp.route("/api/sem-campanha/leads")
def api_sem_campanha_leads():
    """Busca leads sem campanha diretamente do Supabase (tabela sem_campanha).

    Filtra por updated_at (que coincide com data_criacao para leads não
    atualizados e reflete a data de mudança para ganho/perdido).
    Retorna um único item agregado com utm_campaign='Sem Campanha'.
    """
    from_date = request.args.get("from", "")
    to_date = request.args.get("to", "")

    try:
        headers = {
            "apikey": SUPABASE_GOOGLE_KEY,
            "Authorization": f"Bearer {SUPABASE_GOOGLE_KEY}",
        }

        url_base = (
            f"{SUPABASE_GOOGLE_URL}/rest/v1/sem_campanha"
            f"?select=perdido_ganho"
        )
        if from_date:
            url_base += f"&updated_at=gte.{from_date}T00:00:00"
        if to_date:
            url_base += f"&updated_at=lte.{to_date}T23:59:59.999"

        all_rows = _fetch_paginated(url_base, headers)

        novos = 0
        ganhos = 0
        perdidos = 0
        for row in all_rows:
            pg = (row.get("perdido_ganho") or "").strip().lower()
            if pg == "ganho":
                ganhos += 1
            elif pg == "perdido":
                perdidos += 1
            else:
                novos += 1

        if all_rows:
            campaigns = [{
                "utm_campaign": "Sem Campanha",
                "utm_source": "sem_campanha",
                "utm_medium": "—",
                "novos": novos,
                "ganhos": ganhos,
                "perdidos": perdidos,
            }]
            count = 1
        else:
            campaigns = []
            count = 0

        return jsonify({
            "campaigns": campaigns,
            "status": "OK",
            "count": count,
        })
    except requests.exceptions.Timeout:
        return jsonify({
            "campaigns": [],
            "status": "TIMEOUT",
            "error": "Supabase não respondeu a tempo",
        })
    except requests.exceptions.RequestException as e:
        traceback.print_exc()
        return jsonify({
            "campaigns": [],
            "status": "ERROR",
            "error": str(e),
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "campaigns": [],
            "status": "ERROR",
            "error": str(e),
        })


# ---------------------------------------------------------------------------
# Rotas — Recadastros (Dashboard de Recadastros por Origem)
# ---------------------------------------------------------------------------

@dashboard_bp.route("/api/recadastros")
def api_recadastros():
    """Busca dados de recadastros por origem via webhook do n8n."""
    try:
        import requests
    except ImportError:
        return jsonify({
            "data": [],
            "status": "ERROR",
            "error": "Módulo requests não instalado"
        }), 200
    
    WEBHOOK_URL = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/recadastro_csv"
    
    from_date = request.args.get("from", "")
    to_date = request.args.get("to", "")
    
    try:
        payload = {}
        if from_date:
            payload["from"] = from_date
        if to_date:
            payload["to"] = to_date
        
        response = requests.post(WEBHOOK_URL, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        return jsonify({
            "data": data,
            "status": "OK"
        }), 200
    except requests.exceptions.Timeout:
        return jsonify({
            "data": [],
            "status": "TIMEOUT",
            "error": "Webhook não respondeu a tempo"
        }), 200
    except requests.exceptions.RequestException as e:
        return jsonify({
            "data": [],
            "status": "ERROR",
            "error": str(e)
        }), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "data": [],
            "status": "ERROR",
            "error": str(e)
        }), 200


@dashboard_bp.route("/api/meta/webhook", methods=["POST"])
def api_meta_webhook():
    """Webhook para receber dados do Meta Ads."""
    from flask import request
    
    data = request.get_json(force=True, silent=True) or {}
    
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS meta_campaigns (
                    id SERIAL PRIMARY KEY,
                    campaign_id VARCHAR(255),
                    campaign_name VARCHAR(500),
                    creative_type VARCHAR(100),
                    status VARCHAR(50) DEFAULT 'ACTIVE',
                    leads INTEGER DEFAULT 0,
                    conversions INTEGER DEFAULT 0,
                    impressions INTEGER DEFAULT 0,
                    clicks INTEGER DEFAULT 0,
                    spend DECIMAL(10,2) DEFAULT 0,
                    ctr DECIMAL(5,2) DEFAULT 0,
                    cpc DECIMAL(10,2) DEFAULT 0,
                    cpl DECIMAL(10,2) DEFAULT 0,
                    date DATE DEFAULT CURRENT_DATE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS meta_webhook_config (
                    id SERIAL PRIMARY KEY,
                    connected BOOLEAN DEFAULT TRUE,
                    last_sync TIMESTAMP DEFAULT NOW()
                )
            """)
            
            if data.get("campaigns"):
                for campaign in data["campaigns"]:
                    cur.execute("""
                        INSERT INTO meta_campaigns (
                            campaign_id, campaign_name, creative_type, status,
                            leads, conversions, impressions, clicks, spend,
                            ctr, cpc, cpl, date
                        ) VALUES (
                            %(campaign_id)s, %(campaign_name)s, %(creative_type)s, %(status)s,
                            %(leads)s, %(conversions)s, %(impressions)s, %(clicks)s, %(spend)s,
                            %(ctr)s, %(cpc)s, %(cpl)s, %(date)s
                        )
                        ON CONFLICT (campaign_id, date) DO UPDATE SET
                            leads = EXCLUDED.leads,
                            conversions = EXCLUDED.conversions,
                            impressions = EXCLUDED.impressions,
                            clicks = EXCLUDED.clicks,
                            spend = EXCLUDED.spend,
                            ctr = EXCLUDED.ctr,
                            cpc = EXCLUDED.cpc,
                            cpl = EXCLUDED.cpl,
                            updated_at = NOW()
                    """, {
                        "campaign_id": campaign.get("campaign_id", ""),
                        "campaign_name": campaign.get("campaign_name", ""),
                        "creative_type": campaign.get("creative_type", ""),
                        "status": campaign.get("status", "ACTIVE"),
                        "leads": campaign.get("leads", 0),
                        "conversions": campaign.get("conversions", 0),
                        "impressions": campaign.get("impressions", 0),
                        "clicks": campaign.get("clicks", 0),
                        "spend": campaign.get("spend", 0),
                        "ctr": campaign.get("ctr", 0),
                        "cpc": campaign.get("cpc", 0),
                        "cpl": campaign.get("cpl", 0),
                        "date": campaign.get("date", datetime.now().date()),
                    })
            
            cur.execute("""
                INSERT INTO meta_webhook_config (connected, last_sync)
                VALUES (TRUE, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    connected = TRUE,
                    last_sync = NOW()
            """)
            
            conn.commit()
            
        return jsonify({"status": "ok", "message": "Dados recebidos com sucesso"})
    except Exception as e:
        traceback.print_exc()
        conn.rollback()
        return jsonify({"status": "error", "error": str(e)}), 500
    finally:
        conn.close()
