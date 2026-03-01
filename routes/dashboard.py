import json
import traceback
from datetime import datetime, timezone, timedelta, date

import psycopg2
import psycopg2.extras
from flask import Blueprint, render_template, request, jsonify, current_app

from db import get_conn
from helpers import (
    BRT, to_brt, FIELD_RGM,
    TIPO_ALUNO_FIELD, DATA_MATRICULA_FIELD,
    SITUACAO_FIELD, NIVEL_FIELD, POLO_FIELD, TURMA_FIELD,
    SYNC_STATE_QUERY, RECENT_BIZ_UPDATES_QUERY,
)

dashboard_bp = Blueprint("dashboard", __name__)


def _get_process_state():
    from routes.crm import _sync_running, _update_running
    return _sync_running, _update_running


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
            cur.execute("SELECT COUNT(*) AS total FROM leads")
            total_leads = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) AS total FROM businesses")
            total_biz = cur.fetchone()["total"]

            cur.execute(SYNC_STATE_QUERY)
            states = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                states.append(row)

            cur.execute(RECENT_BIZ_UPDATES_QUERY)
            recent = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                recent.append(row)

            cur.execute("SELECT COUNT(*) AS total FROM pipelines")
            total_pipelines = cur.fetchone()["total"]

            try:
                cur.execute("SELECT * FROM schedules ORDER BY created_at")
                schedules = [dict(r) for r in cur.fetchall()]
                for s in schedules:
                    for k, v in s.items():
                        if isinstance(v, datetime):
                            s[k] = to_brt(v)
            except Exception:
                schedules = []

        _sync_running, _update_running = _get_process_state()

        return jsonify({
            "total_leads": total_leads,
            "total_businesses": total_biz,
            "total_pipelines": total_pipelines,
            "sync_states": states,
            "recent_updates": recent,
            "schedules": schedules,
            "sync_running": _sync_running,
            "update_running": _update_running,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Dashboard: Métricas de Alunos
# ---------------------------------------------------------------------------

_STUDENT_METRICS_QUERY = """
WITH biz_fields AS (
    SELECT
        b.id,
        MAX(CASE WHEN af->>'additionalFieldId' = %(tipo_id)s OR af->'additionalField'->>'id' = %(tipo_id)s
                 THEN af->>'value' END) AS tipo_aluno,
        MAX(CASE WHEN af->>'additionalFieldId' = %(dt_id)s   OR af->'additionalField'->>'id' = %(dt_id)s
                 THEN af->>'value' END) AS data_matricula,
        MAX(CASE WHEN af->>'additionalFieldId' = %(sit_id)s  OR af->'additionalField'->>'id' = %(sit_id)s
                 THEN af->>'value' END) AS situacao,
        MAX(CASE WHEN af->>'additionalFieldId' = %(niv_id)s  OR af->'additionalField'->>'id' = %(niv_id)s
                 THEN af->>'value' END) AS nivel,
        MAX(CASE WHEN af->>'additionalFieldId' = %(polo_id)s OR af->'additionalField'->>'id' = %(polo_id)s
                 THEN af->>'value' END) AS polo,
        MAX(CASE WHEN af->>'additionalFieldId' = %(turma_id)s OR af->'additionalField'->>'id' = %(turma_id)s
                 THEN af->>'value' END) AS turma
    FROM businesses b,
         jsonb_array_elements(b.data->'additionalFields') af
    GROUP BY b.id
)
SELECT
    COALESCE(bf.tipo_aluno, 'Não informado') AS tipo,
    bf.situacao,
    bf.nivel,
    bf.polo,
    bf.turma,
    c.nome AS ciclo,
    COUNT(*) AS total
FROM biz_fields bf
LEFT JOIN LATERAL (
    SELECT ci.nome FROM ciclos ci
    WHERE ci.nivel = bf.nivel
      AND bf.data_matricula IS NOT NULL
      AND bf.data_matricula ~ '^\\d{4}-\\d{2}-\\d{2}'
      AND bf.data_matricula::date BETWEEN ci.dt_inicio AND ci.dt_fim
    LIMIT 1
) c ON TRUE
WHERE (%(dt_from)s IS NULL OR bf.data_matricula >= %(dt_from)s)
  AND (%(dt_to)s   IS NULL OR bf.data_matricula <= %(dt_to)s)
  AND (%(f_nivel)s IS NULL OR bf.nivel = %(f_nivel)s)
  AND (%(f_sit)s   IS NULL OR bf.situacao = %(f_sit)s)
GROUP BY bf.tipo_aluno, bf.situacao, bf.nivel, bf.polo, bf.turma, c.nome
ORDER BY total DESC
"""


@dashboard_bp.route("/api/dashboard/students")
def api_dashboard_students():
    dt_from = request.args.get("from", "")
    dt_to = request.args.get("to", "")
    f_nivel = request.args.get("nivel", "")
    f_sit = request.args.get("situacao", "")
    f_ciclo = request.args.get("ciclo", "")
    conn = get_conn()
    try:
        if f_ciclo:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT MIN(dt_inicio) AS dt_start, MAX(dt_fim) AS dt_end "
                    "FROM ciclos WHERE nome = %s", (f_ciclo,)
                )
                crow = cur.fetchone()
                if crow and crow["dt_start"]:
                    dt_from = str(crow["dt_start"])
                    dt_to = str(crow["dt_end"])

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_STUDENT_METRICS_QUERY, {
                "tipo_id": TIPO_ALUNO_FIELD,
                "dt_id": DATA_MATRICULA_FIELD,
                "sit_id": SITUACAO_FIELD,
                "niv_id": NIVEL_FIELD,
                "polo_id": POLO_FIELD,
                "turma_id": TURMA_FIELD,
                "dt_from": dt_from or None,
                "dt_to": dt_to or None,
                "f_nivel": f_nivel or None,
                "f_sit": f_sit or None,
            })
            rows = cur.fetchall()

        tipo_map = {
            "Calouro": "novos",
            "Regresso (Retorno)": "regresso",
            "Calouro (Recompra)": "recompra",
            "Veterano": "rematricula",
        }

        totals = {"novos": 0, "regresso": 0, "recompra": 0, "rematricula": 0, "outros": 0}
        by_situacao = {}
        by_nivel = {}
        by_polo = {}
        by_turma = {}
        by_ciclo = {}
        by_tipo_detail = {}

        for r in rows:
            tipo = r["tipo"] or "Não informado"
            cat = tipo_map.get(tipo, "outros")
            totals[cat] += r["total"]

            sit = r["situacao"] or "N/I"
            by_situacao[sit] = by_situacao.get(sit, 0) + r["total"]

            niv = r["nivel"] or "N/I"
            by_nivel[niv] = by_nivel.get(niv, 0) + r["total"]

            polo = r["polo"] or "N/I"
            by_polo[polo] = by_polo.get(polo, 0) + r["total"]

            turma = r["turma"] or "N/I"
            by_turma[turma] = by_turma.get(turma, 0) + r["total"]

            ciclo = r["ciclo"] or "N/I"
            by_ciclo[ciclo] = by_ciclo.get(ciclo, 0) + r["total"]

            if cat not in by_tipo_detail:
                by_tipo_detail[cat] = {"by_situacao": {}, "by_nivel": {}, "by_polo": {}}
            td = by_tipo_detail[cat]
            td["by_situacao"][sit] = td["by_situacao"].get(sit, 0) + r["total"]
            td["by_nivel"][niv] = td["by_nivel"].get(niv, 0) + r["total"]
            td["by_polo"][polo] = td["by_polo"].get(polo, 0) + r["total"]

        for cat in by_tipo_detail:
            td = by_tipo_detail[cat]
            td["by_situacao"] = dict(sorted(td["by_situacao"].items(), key=lambda x: -x[1]))
            td["by_nivel"] = dict(sorted(td["by_nivel"].items(), key=lambda x: -x[1]))
            td["by_polo"] = dict(sorted(td["by_polo"].items(), key=lambda x: -x[1])[:8])

        return jsonify({
            "totals": totals,
            "by_tipo_detail": by_tipo_detail,
            "by_situacao": dict(sorted(by_situacao.items(), key=lambda x: -x[1])),
            "by_nivel": dict(sorted(by_nivel.items(), key=lambda x: -x[1])),
            "by_polo": dict(sorted(by_polo.items(), key=lambda x: -x[1])),
            "by_turma": dict(sorted(by_turma.items(), key=lambda x: -x[1])),
            "by_ciclo": dict(sorted(by_ciclo.items(), key=lambda x: -x[1])),
            "grand_total": sum(totals.values()),
            "filter": {"from": dt_from, "to": dt_to},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Dashboard Timeline (gráficos de linha com drill-down)
# ---------------------------------------------------------------------------

_TIMELINE_QUERY = """
WITH biz_fields AS (
    SELECT
        b.id,
        MAX(CASE WHEN af->>'additionalFieldId' = %(tipo_id)s OR af->'additionalField'->>'id' = %(tipo_id)s
                 THEN af->>'value' END) AS tipo_aluno,
        MAX(CASE WHEN af->>'additionalFieldId' = %(dt_id)s   OR af->'additionalField'->>'id' = %(dt_id)s
                 THEN af->>'value' END) AS data_matricula,
        MAX(CASE WHEN af->>'additionalFieldId' = %(niv_id)s  OR af->'additionalField'->>'id' = %(niv_id)s
                 THEN af->>'value' END) AS nivel
    FROM businesses b,
         jsonb_array_elements(b.data->'additionalFields') af
    GROUP BY b.id
)
SELECT
    CASE WHEN %(granularity)s = 'month'
         THEN TO_CHAR(bf.data_matricula::date, 'YYYY-MM')
         ELSE TO_CHAR(bf.data_matricula::date, 'YYYY-MM-DD')
    END AS period,
    COALESCE(bf.tipo_aluno, 'Não informado') AS tipo,
    COUNT(*) AS total
FROM biz_fields bf
WHERE bf.data_matricula IS NOT NULL
  AND bf.data_matricula ~ '^\\d{4}-\\d{2}-\\d{2}'
  AND bf.data_matricula::date BETWEEN %(range_start)s AND %(range_end)s
  AND (%(f_nivel)s IS NULL OR bf.nivel = %(f_nivel)s)
GROUP BY period, bf.tipo_aluno
ORDER BY period, total DESC
"""


@dashboard_bp.route("/api/dashboard/timeline")
def api_dashboard_timeline():
    """Retorna dados de timeline agrupados por mês ou dia, para gráficos de linha."""
    from dateutil.relativedelta import relativedelta

    granularity = request.args.get("granularity", "month")
    f_nivel = request.args.get("nivel") or None
    dt_from = request.args.get("from", "")
    dt_to = request.args.get("to", "")

    today = datetime.now().date()

    if dt_from:
        range_start = datetime.strptime(dt_from, "%Y-%m-%d").date()
    else:
        range_start = today - relativedelta(months=6)

    if dt_to:
        range_end = datetime.strptime(dt_to, "%Y-%m-%d").date()
    else:
        range_end = today

    tipo_map = {
        "Calouro": "novos",
        "Regresso (Retorno)": "regresso",
        "Calouro (Recompra)": "recompra",
        "Veterano": "rematricula",
    }

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_TIMELINE_QUERY, {
                "tipo_id": TIPO_ALUNO_FIELD,
                "dt_id": DATA_MATRICULA_FIELD,
                "niv_id": NIVEL_FIELD,
                "granularity": granularity,
                "range_start": range_start,
                "range_end": range_end,
                "f_nivel": f_nivel,
            })
            rows = cur.fetchall()

        series = {}
        all_periods = set()
        for r in rows:
            p = r["period"]
            all_periods.add(p)
            cat = tipo_map.get(r["tipo"] or "", "outros")
            if cat not in series:
                series[cat] = {}
            series[cat][p] = series[cat].get(p, 0) + r["total"]

        periods = sorted(all_periods)

        result = {
            "periods": periods,
            "series": {},
            "granularity": granularity,
            "range": {"from": str(range_start), "to": str(range_end)},
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

_BIZ_FIELDS_CTE = """
WITH biz_fields AS (
    SELECT
        b.id,
        MAX(CASE WHEN af->>'additionalFieldId' = %(tipo_id)s OR af->'additionalField'->>'id' = %(tipo_id)s
                 THEN af->>'value' END) AS tipo_aluno,
        MAX(CASE WHEN af->>'additionalFieldId' = %(dt_id)s   OR af->'additionalField'->>'id' = %(dt_id)s
                 THEN af->>'value' END) AS data_matricula,
        MAX(CASE WHEN af->>'additionalFieldId' = %(sit_id)s  OR af->'additionalField'->>'id' = %(sit_id)s
                 THEN af->>'value' END) AS situacao,
        MAX(CASE WHEN af->>'additionalFieldId' = %(niv_id)s  OR af->'additionalField'->>'id' = %(niv_id)s
                 THEN af->>'value' END) AS nivel,
        MAX(CASE WHEN af->>'additionalFieldId' = %(polo_id)s OR af->'additionalField'->>'id' = %(polo_id)s
                 THEN af->>'value' END) AS polo
    FROM businesses b,
         jsonb_array_elements(b.data->'additionalFields') af
    GROUP BY b.id
)
"""

_CICLO_COMPARE_QUERY = _BIZ_FIELDS_CTE + """
SELECT
    c.nome AS ciclo, c.nivel AS ciclo_nivel,
    COALESCE(bf.tipo_aluno, 'Não informado') AS tipo,
    bf.situacao, bf.nivel, bf.polo, COUNT(*) AS total
FROM biz_fields bf
INNER JOIN ciclos c ON c.nivel = bf.nivel
    AND bf.data_matricula IS NOT NULL
    AND bf.data_matricula ~ '^\\d{4}-\\d{2}-\\d{2}'
    AND bf.data_matricula::date BETWEEN c.dt_inicio AND c.dt_fim
GROUP BY c.nome, c.nivel, bf.tipo_aluno, bf.situacao, bf.nivel, bf.polo
ORDER BY c.nome, total DESC
"""

_DATE_RANGE_QUERY = _BIZ_FIELDS_CTE + """
SELECT
    COALESCE(bf.tipo_aluno, 'Não informado') AS tipo,
    bf.situacao, bf.nivel, bf.polo, COUNT(*) AS total
FROM biz_fields bf
WHERE bf.data_matricula IS NOT NULL
  AND bf.data_matricula ~ '^\\d{4}-\\d{2}-\\d{2}'
  AND bf.data_matricula::date BETWEEN %(range_start)s AND %(range_end)s
  AND (%(f_nivel)s IS NULL OR bf.nivel = %(f_nivel)s)
GROUP BY bf.tipo_aluno, bf.situacao, bf.nivel, bf.polo
ORDER BY total DESC
"""


def _aggregate_rows(rows, tipo_map):
    result = {
        "totals": {"novos": 0, "regresso": 0, "recompra": 0, "rematricula": 0, "outros": 0},
        "by_situacao": {}, "by_polo": {}, "grand_total": 0,
    }
    for r in rows:
        tipo = r["tipo"] or "Não informado"
        cat = tipo_map.get(tipo, "outros")
        result["totals"][cat] += r["total"]
        result["grand_total"] += r["total"]
        sit = r["situacao"] or "N/I"
        result["by_situacao"][sit] = result["by_situacao"].get(sit, 0) + r["total"]
        polo = r["polo"] or "N/I"
        result["by_polo"][polo] = result["by_polo"].get(polo, 0) + r["total"]
    result["by_situacao"] = dict(sorted(result["by_situacao"].items(), key=lambda x: -x[1]))
    result["by_polo"] = dict(sorted(result["by_polo"].items(), key=lambda x: -x[1]))
    return result


@dashboard_bp.route("/api/dashboard/ciclos")
def api_dashboard_ciclos():
    """Retorna métricas por ciclo + comparações temporais (YTD vs ano anterior, vs 6 meses)."""
    from dateutil.relativedelta import relativedelta
    import traceback

    f_nivel = request.args.get("nivel") or None

    conn = get_conn()
    try:
        today = datetime.now().date()
        field_params = {
            "tipo_id": TIPO_ALUNO_FIELD, "dt_id": DATA_MATRICULA_FIELD,
            "sit_id": SITUACAO_FIELD, "niv_id": NIVEL_FIELD, "polo_id": POLO_FIELD,
        }
        tipo_map = {
            "Calouro": "novos", "Regresso (Retorno)": "regresso",
            "Calouro (Recompra)": "recompra", "Veterano": "rematricula",
        }

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if f_nivel:
                cur.execute("SELECT nome, nivel, dt_inicio, dt_fim FROM ciclos WHERE nivel = %s ORDER BY dt_inicio", (f_nivel,))
            else:
                cur.execute("SELECT nome, nivel, dt_inicio, dt_fim FROM ciclos ORDER BY dt_inicio")
            ciclos_config = cur.fetchall()

            cur.execute("""
                SELECT DISTINCT bf.nivel, COUNT(*) AS total
                FROM (
                    SELECT MAX(CASE WHEN af->>'additionalFieldId' = %(niv_id)s
                                    OR af->'additionalField'->>'id' = %(niv_id)s
                                THEN af->>'value' END) AS nivel
                    FROM businesses b, jsonb_array_elements(b.data->'additionalFields') af
                    GROUP BY b.id
                ) bf
                WHERE bf.nivel IS NOT NULL
                GROUP BY bf.nivel ORDER BY total DESC
            """, {"niv_id": NIVEL_FIELD})
            distinct_nivels = {r["nivel"]: r["total"] for r in cur.fetchall()}

            cur.execute(_CICLO_COMPARE_QUERY, field_params)
            cycle_rows = cur.fetchall()
            if f_nivel:
                cycle_rows = [r for r in cycle_rows if r.get("ciclo_nivel") == f_nivel]

            ytd_start = today.replace(month=1, day=1)
            cur.execute(_DATE_RANGE_QUERY, {
                **field_params, "range_start": ytd_start, "range_end": today, "f_nivel": f_nivel,
            })
            ytd_current = cur.fetchall()

            ytd_prev_start = ytd_start.replace(year=today.year - 1)
            ytd_prev_end = today.replace(year=today.year - 1)
            cur.execute(_DATE_RANGE_QUERY, {
                **field_params, "range_start": ytd_prev_start, "range_end": ytd_prev_end, "f_nivel": f_nivel,
            })
            ytd_previous = cur.fetchall()

            m6_start = today - relativedelta(months=6)
            cur.execute(_DATE_RANGE_QUERY, {
                **field_params, "range_start": m6_start, "range_end": today, "f_nivel": f_nivel,
            })
            m6_current = cur.fetchall()

            m6_prev_start = today - relativedelta(months=12)
            m6_prev_end = today - relativedelta(months=6)
            cur.execute(_DATE_RANGE_QUERY, {
                **field_params, "range_start": m6_prev_start, "range_end": m6_prev_end, "f_nivel": f_nivel,
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
            cat = tipo_map.get(r["tipo"] or "", "outros")
            c["totals"][cat] += r["total"]
            c["grand_total"] += r["total"]
            sit = r["situacao"] or "N/I"
            c["by_situacao"][sit] = c["by_situacao"].get(sit, 0) + r["total"]
            polo = r["polo"] or "N/I"
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
                    "current": _aggregate_rows(ytd_current, tipo_map),
                },
                "ytd_prev": {
                    "label": f"YTD {today.year - 1}",
                    "period": f"{ytd_prev_start.isoformat()} → {ytd_prev_end.isoformat()}",
                    "current": _aggregate_rows(ytd_previous, tipo_map),
                },
                "m6": {
                    "label": "Últimos 6 meses",
                    "period": f"{m6_start.isoformat()} → {today.isoformat()}",
                    "current": _aggregate_rows(m6_current, tipo_map),
                },
                "m6_prev": {
                    "label": "6 meses anteriores",
                    "period": f"{m6_prev_start.isoformat()} → {m6_prev_end.isoformat()}",
                    "current": _aggregate_rows(m6_previous, tipo_map),
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
