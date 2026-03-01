import traceback
import unicodedata
from datetime import datetime

import psycopg2
import psycopg2.extras
from flask import Blueprint, render_template, request, jsonify, current_app

from db import get_conn
from helpers import BRT, to_brt

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


# ---------------------------------------------------------------------------
# SQL fragments — snapshot-based queries (xl_rows)
# ---------------------------------------------------------------------------

_MAT_CTE = """
WITH mat AS (
    SELECT
        r.data->>'tipo_matricula' AS tipo_aluno,
        CASE
          WHEN r.data->>'data_mat' ~ '^\\d{2}/\\d{2}/\\d{4}' THEN
            TO_DATE(SUBSTRING(r.data->>'data_mat' FROM 1 FOR 10), 'DD/MM/YYYY')
          WHEN r.data->>'data_mat' ~ '^\\d{4}-\\d{2}-\\d{2}' THEN
            (SUBSTRING(r.data->>'data_mat' FROM 1 FOR 10))::date
          ELSE NULL
        END AS data_matricula,
        r.data->>'situacao' AS situacao,
        CASE WHEN r.data->>'negocio' ILIKE '%%pos%%'
             THEN 'Pós-Graduação' ELSE 'Graduação' END AS nivel,
        r.data->>'polo'  AS polo,
        r.data->>'curso' AS turma
    FROM xl_rows r
    WHERE r.snapshot_id = (
        SELECT id FROM xl_snapshots
        WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
    )
)
"""


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
            if snap:
                snap["uploaded_at"] = to_brt(snap["uploaded_at"])

        _sync_running, _update_running = _get_process_state()

        return jsonify({
            "snapshot": snap,
            "sync_running": _sync_running,
            "update_running": _update_running,
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
    m.situacao,
    m.nivel,
    m.polo,
    m.turma,
    c.nome AS ciclo,
    COUNT(*) AS total
FROM mat m
LEFT JOIN LATERAL (
    SELECT ci.nome FROM ciclos ci
    WHERE ci.nivel = m.nivel
      AND m.data_matricula IS NOT NULL
      AND m.data_matricula BETWEEN ci.dt_inicio AND ci.dt_fim
    LIMIT 1
) c ON TRUE
WHERE (%(dt_from)s IS NULL OR m.data_matricula >= %(dt_from)s::date)
  AND (%(dt_to)s   IS NULL OR m.data_matricula <= %(dt_to)s::date)
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
  AND (%(f_sit)s   IS NULL OR m.situacao = %(f_sit)s)
GROUP BY m.tipo_aluno, m.situacao, m.nivel, m.polo, m.turma, c.nome
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
                "dt_from": dt_from or None,
                "dt_to": dt_to or None,
                "f_nivel": f_nivel or None,
                "f_sit": f_sit or None,
            })
            rows = cur.fetchall()

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
            totals[cat] += r["total"]
            raw_tipos[tipo] = raw_tipos.get(tipo, 0) + r["total"]

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
            "raw_tipos": dict(sorted(raw_tipos.items(), key=lambda x: -x[1])),
            "filter": {"from": dt_from, "to": dt_to},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Dashboard Timeline (gráficos de linha com drill-down)
# ---------------------------------------------------------------------------

_TIMELINE_QUERY = _MAT_CTE + """
SELECT
    CASE WHEN %(granularity)s = 'month'
         THEN TO_CHAR(m.data_matricula, 'YYYY-MM')
         ELSE TO_CHAR(m.data_matricula, 'YYYY-MM-DD')
    END AS period,
    COALESCE(m.tipo_aluno, 'Não informado') AS tipo,
    COUNT(*) AS total
FROM mat m
WHERE m.data_matricula IS NOT NULL
  AND m.data_matricula BETWEEN %(range_start)s AND %(range_end)s
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
GROUP BY period, m.tipo_aluno
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

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_TIMELINE_QUERY, {
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
            cat = _classify_tipo(r["tipo"] or "")
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
        polo = r["polo"] or "N/I"
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
                SELECT
                    CASE WHEN r.data->>'negocio' ILIKE '%%pos%%'
                         THEN 'Pós-Graduação' ELSE 'Graduação' END AS nivel,
                    COUNT(*) AS total
                FROM xl_rows r
                WHERE r.snapshot_id = (
                    SELECT id FROM xl_snapshots
                    WHERE tipo = 'matriculados' ORDER BY id DESC LIMIT 1
                )
                GROUP BY nivel ORDER BY total DESC
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
