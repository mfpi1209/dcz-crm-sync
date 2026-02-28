"""
DataCrazy CRM Sync — Interface Web (Flask).

Uso:
    python app.py
    Acesse http://localhost:5001
"""

import os
import sys
import json
import subprocess
import threading
import time
import re
from datetime import datetime, timezone, timedelta

BRT = timezone(timedelta(hours=-3))
from pathlib import Path
from collections import deque

from flask import (
    Flask, render_template, request, jsonify,
    redirect, url_for, session, send_file,
)
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")


def to_brt(dt):
    """Convert a datetime to BRT (UTC-3) string."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BRT).strftime("%d/%m/%Y %H:%M:%S")
    return str(dt)


app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dcz-sync-default-key-change-me")

# ---------------------------------------------------------------------------
# Autenticação por sessão
# ---------------------------------------------------------------------------

APP_USER = os.getenv("APP_USER", "admin")
APP_PASS = os.getenv("APP_PASS", "")


@app.before_request
def require_auth():
    if not APP_PASS:
        return
    if request.path in ("/login",):
        return
    if request.path.startswith("/static/"):
        return
    if not session.get("authenticated"):
        if request.path.startswith("/api/"):
            return jsonify({"error": "Não autenticado"}), 401
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = request.form.get("username", "")
        pwd = request.form.get("password", "")
        if user == APP_USER and pwd == APP_PASS:
            session["authenticated"] = True
            return redirect(url_for("index"))
        app.logger.warning("Login falhou: user=%r (esperado %r), pass_len=%d (esperado %d)",
                           user, APP_USER, len(pwd), len(APP_PASS))
        error = "Usuário ou senha incorretos."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)

BASE_DIR = Path(__file__).parent
SYNC_SCRIPT = str(BASE_DIR / "sync.py")
UPDATE_SCRIPT = str(BASE_DIR / "update_crm.py")
SANITIZE_SCRIPT = str(BASE_DIR / "sanitize_crm.py")
PIPELINE_SCRIPT = str(BASE_DIR / "pipeline_crm.py")
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

MAX_LOG_LINES = 2000

# ---------------------------------------------------------------------------
# Estado global
# ---------------------------------------------------------------------------

_sync_running = False
_sync_proc = None
_sync_logs: deque = deque(maxlen=MAX_LOG_LINES)

_update_running = False
_update_proc = None
_update_logs: deque = deque(maxlen=MAX_LOG_LINES)

_sanitize_running = False
_sanitize_proc = None
_sanitize_logs: deque = deque(maxlen=MAX_LOG_LINES)

_pipeline_running = False
_pipeline_proc = None
_pipeline_logs: deque = deque(maxlen=MAX_LOG_LINES)


def _add_sync_log(line: str):
    _sync_logs.append(line.rstrip())


def _add_update_log(line: str):
    _update_logs.append(line.rstrip())


def _add_sanitize_log(line: str):
    _sanitize_logs.append(line.rstrip())


def _add_pipeline_log(line: str):
    _pipeline_logs.append(line.rstrip())

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(**DB_DSN)


FIELD_RGM = "2ac4e30f-cfd7-435f-b688-fbce27f76c38"

SEARCH_QUERY = """
SELECT
    l.id                                  AS lead_id,
    l.data->>'name'                       AS lead_nome,
    l.data->>'phone'                      AS lead_telefone,
    l.data->>'rawPhone'                   AS lead_telefone_raw,
    l.data->>'email'                      AS lead_email,
    l.data->>'source'                     AS lead_origem,
    l.data->>'taxId'                      AS lead_cpf,
    l.data->'address'->>'city'            AS lead_cidade,
    l.data->'address'->>'state'           AS lead_estado,
    l.data->>'createdAt'                  AS lead_criado_em,

    b.id                                  AS negocio_id,
    b.data->>'code'                       AS negocio_codigo,
    b.data->>'status'                     AS negocio_status,
    b.data->>'total'                      AS negocio_valor,
    b.data->>'createdAt'                  AS negocio_criado_em,
    b.data->>'lastMovedAt'                AS negocio_movido_em,

    p.data->>'name'                       AS pipeline_nome,
    ps.data->>'name'                      AS etapa_nome,
    ps.data->>'color'                     AS etapa_cor,

    b.data->'attendant'->>'name'          AS atendente,

    biz_cf.campos                         AS campos_negocio,
    lead_cf.campos                        AS campos_lead

FROM businesses b
LEFT JOIN leads l            ON l.id  = b.data->>'leadId'
LEFT JOIN pipeline_stages ps ON ps.id = b.data->>'stageId'
LEFT JOIN pipelines p        ON p.id  = ps.pipeline_id
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(
        elem->'additionalField'->>'name',
        COALESCE(elem->>'value', '')
    ) AS campos
    FROM jsonb_array_elements(b.data->'additionalFields') elem
    WHERE elem->'additionalField'->>'name' IS NOT NULL
) biz_cf ON true
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(
        elem->'additionalField'->>'name',
        COALESCE(elem->>'value', '')
    ) AS campos
    FROM jsonb_array_elements(l.data->'additionalFields') elem
    WHERE elem->'additionalField'->>'name' IS NOT NULL
) lead_cf ON true
WHERE (
    (%(cpf)s != '' AND REPLACE(REPLACE(l.data->>'taxId', '.', ''), '-', '') LIKE '%%' || REPLACE(REPLACE(%(cpf)s, '.', ''), '-', '') || '%%')
    OR (%(rgm)s != '' AND EXISTS (
        SELECT 1 FROM jsonb_array_elements(b.data->'additionalFields') e
        WHERE e->'additionalField'->>'id' = '2ac4e30f-cfd7-435f-b688-fbce27f76c38'
          AND e->>'value' LIKE '%%' || %(rgm)s || '%%'
    ))
    OR (%(telefone)s != '' AND (
        l.data->>'rawPhone' LIKE '%%' || %(telefone)s || '%%'
        OR REPLACE(REPLACE(REPLACE(REPLACE(l.data->>'phone', ' ', ''), '(', ''), ')', ''), '-', '') LIKE '%%' || %(telefone)s || '%%'
    ))
)
ORDER BY b.data->>'lastMovedAt' DESC NULLS LAST
LIMIT 50;
"""

RECENT_BIZ_UPDATES_QUERY = """
SELECT
    'negocio' AS tipo,
    b.id,
    b.data->'lead'->>'name' AS nome_lead,
    b.data->>'status' AS status,
    p.data->>'name' AS pipeline,
    ps.data->>'name' AS etapa,
    b.synced_at
FROM businesses b
LEFT JOIN pipeline_stages ps ON ps.id = b.data->>'stageId'
LEFT JOIN pipelines p ON p.id = ps.pipeline_id
WHERE b.synced_at = (SELECT MAX(synced_at) FROM businesses)
ORDER BY b.synced_at DESC
LIMIT 10;
"""

SYNC_STATE_QUERY = """
SELECT entity_type, last_sync_at, last_full_sync_at, run_count
FROM sync_state ORDER BY entity_type;
"""

# ---------------------------------------------------------------------------
# Rotas — Páginas
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    resp = app.make_response(render_template("index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


# ---------------------------------------------------------------------------
# Rotas — Dashboard
# ---------------------------------------------------------------------------

@app.route("/api/dashboard")
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

            # Schedules
            try:
                cur.execute("SELECT * FROM schedules ORDER BY created_at")
                schedules = [dict(r) for r in cur.fetchall()]
                for s in schedules:
                    for k, v in s.items():
                        if isinstance(v, datetime):
                            s[k] = to_brt(v)
            except Exception:
                schedules = []

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

TIPO_ALUNO_FIELD = "4230e4db-970b-4444-abaf-c3135a03b79c"
DATA_MATRICULA_FIELD = "bf93a8e9-42c0-4517-8518-6f604746a300"
SITUACAO_FIELD = "fd08d44b-a4a5-4343-b7a9-37f75e2c1caa"
NIVEL_FIELD = "233fcf6f-0bed-49d7-89a1-d1cd54fb9c12"
POLO_FIELD = "0ec9d8dc-d547-4482-b9ad-d4a3e6ec1b54"
TURMA_FIELD = "8815a8de-f755-4597-b6f4-8da6d289b6eb"

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
      AND bf.data_matricula ~ '^\d{4}-\d{2}-\d{2}'
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


@app.route("/api/dashboard/students")
def api_dashboard_students():
    dt_from = request.args.get("from", "")
    dt_to = request.args.get("to", "")
    f_nivel = request.args.get("nivel", "")
    f_sit = request.args.get("situacao", "")
    conn = get_conn()
    try:
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

        return jsonify({
            "totals": totals,
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


@app.route("/api/dashboard/ciclos")
def api_dashboard_ciclos():
    """Retorna métricas por ciclo + comparações temporais (YTD vs ano anterior, vs 6 meses)."""
    from dateutil.relativedelta import relativedelta
    import traceback

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
            cur.execute("SELECT nome, nivel, dt_inicio, dt_fim FROM ciclos ORDER BY dt_inicio")
            ciclos_config = cur.fetchall()

            # Per-cycle data
            cur.execute(_CICLO_COMPARE_QUERY, field_params)
            cycle_rows = cur.fetchall()

            # YTD: Jan 1 of current year → today
            ytd_start = today.replace(month=1, day=1)
            cur.execute(_DATE_RANGE_QUERY, {
                **field_params, "range_start": ytd_start, "range_end": today, "f_nivel": None,
            })
            ytd_current = cur.fetchall()

            # YTD previous year: same range but 1 year back
            ytd_prev_start = ytd_start.replace(year=today.year - 1)
            ytd_prev_end = today.replace(year=today.year - 1)
            cur.execute(_DATE_RANGE_QUERY, {
                **field_params, "range_start": ytd_prev_start, "range_end": ytd_prev_end, "f_nivel": None,
            })
            ytd_previous = cur.fetchall()

            # Last 6 months: today - 6 months → today
            m6_start = today - relativedelta(months=6)
            cur.execute(_DATE_RANGE_QUERY, {
                **field_params, "range_start": m6_start, "range_end": today, "f_nivel": None,
            })
            m6_current = cur.fetchall()

            # Previous 6 months: -12 months → -6 months
            m6_prev_start = today - relativedelta(months=12)
            m6_prev_end = today - relativedelta(months=6)
            cur.execute(_DATE_RANGE_QUERY, {
                **field_params, "range_start": m6_prev_start, "range_end": m6_prev_end, "f_nivel": None,
            })
            m6_previous = cur.fetchall()

        # Aggregate cycle data
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
                    "label": f"Últimos 6 meses",
                    "period": f"{m6_start.isoformat()} → {today.isoformat()}",
                    "current": _aggregate_rows(m6_current, tipo_map),
                },
                "m6_prev": {
                    "label": f"6 meses anteriores",
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
# Rotas — Turmas
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


@app.route("/api/turmas")
def api_turmas_list():
    nivel = request.args.get("nivel", "")
    ano = request.args.get("ano", "")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            q = "SELECT * FROM turmas WHERE 1=1"
            params = []
            if nivel:
                q += " AND nivel = %s"
                params.append(nivel)
            if ano:
                q += " AND ano = %s"
                params.append(int(ano))
            q += " ORDER BY ano, dt_inicio"
            cur.execute(q, params)
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat() if v else None
                rows.append(row)
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/turmas", methods=["POST"])
def api_turmas_create():
    body = request.json or {}
    required = ("nivel", "nome", "dt_inicio", "dt_fim", "ano")
    if not all(body.get(k) for k in required):
        return jsonify({"error": "Campos obrigatórios: " + ", ".join(required)}), 400
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO turmas (nivel, nome, dt_inicio, dt_fim, ano) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (nivel, nome) DO UPDATE SET dt_inicio=EXCLUDED.dt_inicio, dt_fim=EXCLUDED.dt_fim, ano=EXCLUDED.ano "
                "RETURNING id",
                (body["nivel"], body["nome"], body["dt_inicio"], body["dt_fim"], int(body["ano"])),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/turmas/<int:tid>", methods=["PUT"])
def api_turmas_update(tid):
    body = request.json or {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE turmas SET nivel=COALESCE(%s,nivel), nome=COALESCE(%s,nome), "
                "dt_inicio=COALESCE(%s,dt_inicio), dt_fim=COALESCE(%s,dt_fim), ano=COALESCE(%s,ano) "
                "WHERE id=%s",
                (body.get("nivel"), body.get("nome"), body.get("dt_inicio"), body.get("dt_fim"),
                 int(body["ano"]) if body.get("ano") else None, tid),
            )
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/turmas/<int:tid>", methods=["DELETE"])
def api_turmas_delete(tid):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM turmas WHERE id=%s", (tid,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/turmas/seed", methods=["POST"])
def api_turmas_seed():
    body = request.json or {}
    ano = int(body.get("ano", datetime.now().year))
    conn = get_conn()
    try:
        created = 0
        with conn.cursor() as cur:
            for nivel in ("Graduação", "Pós-Graduação"):
                for t in _turma_defaults(nivel, ano):
                    cur.execute(
                        "INSERT INTO turmas (nivel, nome, dt_inicio, dt_fim, ano) "
                        "VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (nivel, nome) DO NOTHING",
                        (t["nivel"], t["nome"], t["dt_inicio"], t["dt_fim"], t["ano"]),
                    )
                    created += cur.rowcount
        conn.commit()
        return jsonify({"ok": True, "created": created, "ano": ano})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Ciclos
# ---------------------------------------------------------------------------

@app.route("/api/ciclos")
def api_ciclos_list():
    nivel = request.args.get("nivel", "")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            q = "SELECT * FROM ciclos WHERE 1=1"
            params = []
            if nivel:
                q += " AND nivel = %s"
                params.append(nivel)
            q += " ORDER BY dt_inicio"
            cur.execute(q, params)
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat() if v else None
                rows.append(row)
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/ciclos", methods=["POST"])
def api_ciclos_create():
    body = request.json or {}
    required = ("nivel", "nome", "dt_inicio", "dt_fim")
    if not all(body.get(k) for k in required):
        return jsonify({"error": "Campos obrigatórios: " + ", ".join(required)}), 400
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ciclos (nivel, nome, dt_inicio, dt_fim) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (nivel, nome) DO UPDATE SET dt_inicio=EXCLUDED.dt_inicio, dt_fim=EXCLUDED.dt_fim "
                "RETURNING id",
                (body["nivel"], body["nome"], body["dt_inicio"], body["dt_fim"]),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/ciclos/<int:cid>", methods=["PUT"])
def api_ciclos_update(cid):
    body = request.json or {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ciclos SET nivel=COALESCE(%s,nivel), nome=COALESCE(%s,nome), "
                "dt_inicio=COALESCE(%s,dt_inicio), dt_fim=COALESCE(%s,dt_fim) "
                "WHERE id=%s",
                (body.get("nivel"), body.get("nome"), body.get("dt_inicio"), body.get("dt_fim"), cid),
            )
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/ciclos/<int:cid>", methods=["DELETE"])
def api_ciclos_delete(cid):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ciclos WHERE id=%s", (cid,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/ciclos/seed", methods=["POST"])
def api_ciclos_seed():
    """Gera ciclos padrão: Graduação semestral, Pós-Graduação semestral."""
    body = request.json or {}
    ano = int(body.get("ano", datetime.now().year))
    conn = get_conn()
    try:
        created = 0
        defaults = [
            ("Graduação", f"{ano}.1", f"{ano-1}-11-16", f"{ano}-05-15"),
            ("Graduação", f"{ano}.2", f"{ano}-05-16", f"{ano}-11-15"),
            ("Pós-Graduação", f"{ano}.1", f"{ano-1}-11-16", f"{ano}-05-15"),
            ("Pós-Graduação", f"{ano}.2", f"{ano}-05-16", f"{ano}-11-15"),
        ]
        with conn.cursor() as cur:
            for nivel, nome, dt_ini, dt_fim in defaults:
                cur.execute(
                    "INSERT INTO ciclos (nivel, nome, dt_inicio, dt_fim) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (nivel, nome) DO NOTHING",
                    (nivel, nome, dt_ini, dt_fim),
                )
                created += cur.rowcount
        conn.commit()
        return jsonify({"ok": True, "created": created, "ano": ano})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Diagnóstico de address
# ---------------------------------------------------------------------------

@app.route("/api/debug/address")
def api_debug_address():
    """Compara address no banco local vs API direta para diagnosticar sync."""
    import requests as req
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, data->>'name' AS nome, data->'address' AS local_address
                FROM leads
                WHERE data->'address' IS NOT NULL
                   AND data->'address' != 'null'::jsonb
                   AND data->'address' != '{}'::jsonb
                LIMIT 3
            """)
            with_addr = cur.fetchall()

            cur.execute("""
                SELECT id, data->>'name' AS nome, data->'address' AS local_address
                FROM leads
                WHERE data->'address' IS NULL
                   OR data->'address' = 'null'::jsonb
                   OR data->'address' = '{}'::jsonb
                LIMIT 3
            """)
            without_addr = cur.fetchall()

            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE data->'address' IS NOT NULL
                        AND data->'address' != 'null'::jsonb
                        AND data->'address' != '{}'::jsonb) AS com_address,
                    COUNT(*) FILTER (WHERE data->'address' IS NULL
                        OR data->'address' = 'null'::jsonb
                        OR data->'address' = '{}'::jsonb) AS sem_address,
                    COUNT(*) AS total
                FROM leads
            """)
            stats = dict(cur.fetchone())

        token = os.getenv("DATACRAZY_API_TOKEN", "")
        headers = {"Authorization": f"Bearer {token}"}
        api_base = "https://api.g1.datacrazy.io/api/v1"

        api_samples = []
        sample_ids = [r["id"] for r in (with_addr[:1] + without_addr[:1])]
        for lid in sample_ids:
            try:
                r = req.get(f"{api_base}/leads/{lid}", headers=headers, timeout=15)
                if r.ok:
                    d = r.json()
                    api_samples.append({
                        "id": lid,
                        "nome": d.get("name"),
                        "api_address": d.get("address"),
                    })
            except Exception as e:
                api_samples.append({"id": lid, "error": str(e)})

        list_sample = []
        try:
            r = req.get(f"{api_base}/leads", headers=headers, params={
                "take": 2,
                "complete[additionalFields]": "true",
            }, timeout=15)
            if r.ok:
                for lead in r.json().get("data", [])[:2]:
                    list_sample.append({
                        "id": lead.get("id"),
                        "nome": lead.get("name"),
                        "list_address": lead.get("address"),
                        "has_address_key": "address" in lead,
                    })
        except Exception as e:
            list_sample = [{"error": str(e)}]

        return jsonify({
            "stats": stats,
            "local_with_addr_samples": [dict(r) for r in with_addr],
            "local_without_addr_samples": [dict(r) for r in without_addr],
            "api_individual_samples": api_samples,
            "api_list_samples": list_sample,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Busca
# ---------------------------------------------------------------------------

@app.route("/api/search")
def api_search():
    cpf = request.args.get("cpf", "").strip()
    rgm = request.args.get("rgm", "").strip()
    telefone = request.args.get("telefone", "").strip()

    if not cpf and not rgm and not telefone:
        return jsonify({"results": [], "error": "Informe pelo menos um critério de busca."})

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SEARCH_QUERY, {"cpf": cpf, "rgm": rgm, "telefone": telefone})
            rows = cur.fetchall()
            results = []
            for r in rows:
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                results.append(row)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"results": [], "error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/sync-state")
def api_sync_state():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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

        return jsonify({"states": states, "recent_updates": recent})
    except Exception as e:
        return jsonify({"states": [], "recent_updates": [], "error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Sync
# ---------------------------------------------------------------------------

@app.route("/api/sync/<mode>", methods=["POST"])
def api_sync(mode):
    global _sync_running

    if mode not in ("delta", "full"):
        return jsonify({"error": "Modo inválido. Use 'delta' ou 'full'."}), 400

    if _sync_running:
        return jsonify({"error": "Sincronização já em andamento."}), 409

    _sync_running = True
    _sync_logs.clear()

    def run():
        global _sync_running, _sync_proc
        try:
            cmd = [sys.executable, SYNC_SCRIPT]
            if mode == "full":
                cmd.append("--full")

            _add_sync_log(f"[INÍCIO] Sincronização {mode.upper()} iniciada")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(BASE_DIR),
                env=env,
            )
            _sync_proc = proc

            for line in proc.stdout:
                _add_sync_log(line)

            proc.wait()

            if proc.returncode == 0:
                _add_sync_log("[FIM] Sincronização concluída com sucesso")
            elif proc.returncode < 0:
                _add_sync_log("[PARADO] Sincronização interrompida.")
            else:
                _add_sync_log(f"[ERRO] Sincronização falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_sync_log(f"[ERRO] {e}")
            _add_sync_log(traceback.format_exc())
        finally:
            _sync_proc = None
            _sync_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/sync/logs")
def api_sync_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_sync_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _sync_running})


@app.route("/api/sync/status")
def api_sync_status():
    global _sync_running
    if _sync_running and (_sync_proc is None or _sync_proc.poll() is not None):
        _sync_running = False
    return jsonify({"running": _sync_running})


@app.route("/api/sync/stop", methods=["POST"])
def api_sync_stop():
    global _sync_running
    if _sync_proc is not None:
        try:
            _sync_proc.terminate()
        except Exception:
            pass
    _sync_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Update CRM
# ---------------------------------------------------------------------------

@app.route("/api/update/<mode>", methods=["POST"])
def api_update(mode):
    global _update_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _update_running:
        return jsonify({"error": "Atualização já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")
    rate = body.get("rate")
    with_address = body.get("withAddress", False)

    _update_running = True
    _update_logs.clear()

    def run():
        global _update_running, _update_proc
        try:
            cmd = [sys.executable, UPDATE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])
            if rate is not None:
                cmd.extend(["--rate", str(int(rate))])
            if with_address:
                cmd.append("--with-address")

            _add_update_log(f"[INÍCIO] Update CRM — modo {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(BASE_DIR),
                env=env,
            )
            _update_proc = proc

            for line in proc.stdout:
                _add_update_log(line)

            proc.wait()

            if proc.returncode == 0:
                _add_update_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_update_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_update_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_update_log(f"[ERRO] {e}")
            _add_update_log(traceback.format_exc())
        finally:
            _update_proc = None
            _update_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/update/logs")
def api_update_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_update_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _update_running})


@app.route("/api/update/status")
def api_update_status():
    global _update_running
    if _update_running and (_update_proc is None or _update_proc.poll() is not None):
        _update_running = False
    return jsonify({"running": _update_running})


@app.route("/api/update/stop", methods=["POST"])
def api_update_stop():
    global _update_running
    if _update_proc is not None:
        try:
            _update_proc.terminate()
        except Exception:
            pass
    _update_running = False
    return jsonify({"ok": True})


@app.route("/api/update/preview")
def api_update_preview():
    preview_path = REPORTS_DIR / "update_preview.csv"
    if not preview_path.exists():
        return jsonify({"rows": [], "error": "Rode dry-run primeiro para gerar o preview."})

    import csv as csv_mod
    rows = []
    with open(preview_path, "r", encoding="utf-8-sig") as f:
        reader = csv_mod.DictReader(f, delimiter=";")
        for i, row in enumerate(reader):
            if i >= 500:
                break
            rows.append(dict(row))
    return jsonify({"rows": rows, "total": len(rows)})


# ---------------------------------------------------------------------------
# Rotas — Saneamento
# ---------------------------------------------------------------------------

@app.route("/api/sanitize/<mode>", methods=["POST"])
def api_sanitize(mode):
    global _sanitize_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _sanitize_running:
        return jsonify({"error": "Saneamento já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")

    _sanitize_running = True
    _sanitize_logs.clear()

    def run():
        global _sanitize_running, _sanitize_proc
        try:
            cmd = [sys.executable, SANITIZE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])

            _add_sanitize_log(f"[INÍCIO] Saneamento — modo {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
            )
            _sanitize_proc = proc
            for line in proc.stdout:
                _add_sanitize_log(line)
            proc.wait()

            if proc.returncode == 0:
                _add_sanitize_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_sanitize_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_sanitize_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_sanitize_log(f"[ERRO] {e}")
            _add_sanitize_log(traceback.format_exc())
        finally:
            _sanitize_proc = None
            _sanitize_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/sanitize/logs")
def api_sanitize_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_sanitize_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _sanitize_running})


@app.route("/api/sanitize/status")
def api_sanitize_status():
    global _sanitize_running
    if _sanitize_running and (_sanitize_proc is None or _sanitize_proc.poll() is not None):
        _sanitize_running = False
    return jsonify({"running": _sanitize_running})


@app.route("/api/sanitize/stop", methods=["POST"])
def api_sanitize_stop():
    global _sanitize_running
    if _sanitize_proc is not None:
        try:
            _sanitize_proc.terminate()
        except Exception:
            pass
    _sanitize_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Pipeline
# ---------------------------------------------------------------------------

@app.route("/api/pipeline/<mode>", methods=["POST"])
def api_pipeline(mode):
    global _pipeline_running

    if mode not in ("dry-run", "test", "execute"):
        return jsonify({"error": "Modo inválido. Use 'dry-run', 'test' ou 'execute'."}), 400

    if _pipeline_running:
        return jsonify({"error": "Pipeline já em andamento."}), 409

    body = request.json if request.is_json else {}
    limit = body.get("limit")

    _pipeline_running = True
    _pipeline_logs.clear()

    def run():
        global _pipeline_running, _pipeline_proc
        try:
            cmd = [sys.executable, PIPELINE_SCRIPT, f"--{mode}"]
            if limit and mode == "execute":
                cmd.extend(["--limit", str(int(limit))])

            _add_pipeline_log(f"[INÍCIO] Pipeline — modo {mode.upper()}")

            env = {**os.environ, "PYTHONUNBUFFERED": "1"}
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
            )
            _pipeline_proc = proc
            for line in proc.stdout:
                _add_pipeline_log(line)
            proc.wait()

            if proc.returncode == 0:
                _add_pipeline_log("[FIM] Concluído com sucesso (exit code 0)")
            elif proc.returncode < 0:
                _add_pipeline_log("[PARADO] Processo interrompido pelo usuário.")
            else:
                _add_pipeline_log(f"[ERRO] Falhou (exit code {proc.returncode})")
        except Exception as e:
            import traceback
            _add_pipeline_log(f"[ERRO] {e}")
            _add_pipeline_log(traceback.format_exc())
        finally:
            _pipeline_proc = None
            _pipeline_running = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "mode": mode})


@app.route("/api/pipeline/logs")
def api_pipeline_logs():
    since = int(request.args.get("since", 0))
    logs_list = list(_pipeline_logs)
    lines = logs_list[since:]
    return jsonify({"lines": lines, "total": len(logs_list), "running": _pipeline_running})


@app.route("/api/pipeline/status")
def api_pipeline_status():
    global _pipeline_running
    if _pipeline_running and (_pipeline_proc is None or _pipeline_proc.poll() is not None):
        _pipeline_running = False
    return jsonify({"running": _pipeline_running})


@app.route("/api/pipeline/stop", methods=["POST"])
def api_pipeline_stop():
    global _pipeline_running
    if _pipeline_proc is not None:
        try:
            _pipeline_proc.terminate()
        except Exception:
            pass
    _pipeline_running = False
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Upload
# ---------------------------------------------------------------------------

UPLOAD_DIR = BASE_DIR


def _find_xlsx():
    for f in UPLOAD_DIR.iterdir():
        if f.suffix.lower() == ".xlsx" and "matriculados" in f.name.lower():
            stat = f.stat()
            return {
                "name": f.name,
                "size": stat.st_size,
                "modified": to_brt(datetime.fromtimestamp(stat.st_mtime, tz=BRT)),
            }
    return None


@app.route("/api/upload/info")
def api_upload_info():
    return jsonify({"file": _find_xlsx()})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    if "file" not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado."}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Nenhum arquivo selecionado."}), 400

    if not f.filename.lower().endswith(".xlsx"):
        return jsonify({"error": "Apenas arquivos .xlsx são aceitos."}), 400

    for old in UPLOAD_DIR.iterdir():
        if old.suffix.lower() == ".xlsx" and "matriculados" in old.name.lower():
            old.unlink()

    safe_name = f.filename
    if "matriculados" not in safe_name.lower():
        safe_name = "Relação de matriculados por polo.xlsx"

    dest = UPLOAD_DIR / safe_name
    f.save(str(dest))

    stat = dest.stat()
    return jsonify({
        "ok": True,
        "file": {
            "name": dest.name,
            "size": stat.st_size,
            "modified": to_brt(datetime.fromtimestamp(stat.st_mtime, tz=BRT)),
        },
    })


# ---------------------------------------------------------------------------
# Rotas — Explorador de Logs
# ---------------------------------------------------------------------------

SAFE_LOG_DIRS = [LOG_DIR, REPORTS_DIR]


def _list_log_files():
    files = []
    for d in SAFE_LOG_DIRS:
        if not d.exists():
            continue
        for f in d.iterdir():
            if f.is_file() and f.suffix.lower() in (".csv", ".log", ".txt"):
                stat = f.stat()
                files.append({
                    "name": f.name,
                    "dir": d.name,
                    "path": f"{d.name}/{f.name}",
                    "size": stat.st_size,
                    "modified": to_brt(datetime.fromtimestamp(stat.st_mtime, tz=BRT)),
                })
    files.sort(key=lambda x: x["modified"], reverse=True)
    return files


def _resolve_log_path(filepath):
    """Resolve and validate a log file path, preventing directory traversal."""
    filepath = filepath.replace("\\", "/")
    if ".." in filepath:
        return None
    for d in SAFE_LOG_DIRS:
        candidate = d.parent / filepath
        try:
            candidate = candidate.resolve()
            if candidate.is_file() and any(str(candidate).startswith(str(sd.resolve())) for sd in SAFE_LOG_DIRS):
                return candidate
        except Exception:
            pass
    return None


@app.route("/api/logs")
def api_logs_list():
    return jsonify({"files": _list_log_files()})


@app.route("/api/logs/view/<path:filepath>")
def api_logs_view(filepath):
    fpath = _resolve_log_path(filepath)
    if not fpath:
        return jsonify({"error": "Arquivo não encontrado."}), 404

    tail = int(request.args.get("tail", 200))

    try:
        with open(fpath, "r", encoding="utf-8-sig", errors="replace") as f:
            lines = f.readlines()

        total = len(lines)
        if tail and tail < total:
            lines = lines[-tail:]

        return jsonify({
            "name": fpath.name,
            "total_lines": total,
            "showing": len(lines),
            "lines": [l.rstrip() for l in lines],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs/download/<path:filepath>")
def api_logs_download(filepath):
    fpath = _resolve_log_path(filepath)
    if not fpath:
        return jsonify({"error": "Arquivo não encontrado."}), 404
    return send_file(str(fpath), as_attachment=True)


# ---------------------------------------------------------------------------
# Rotas — Agendamento (Schedules)
# ---------------------------------------------------------------------------

@app.route("/api/schedules")
def api_schedules_list():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM schedules ORDER BY created_at")
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                rows.append(row)

        # Add next run info from scheduler
        for row in rows:
            job = scheduler.get_job(row["id"])
            if job and job.next_run_time:
                row["next_run"] = to_brt(job.next_run_time)
            else:
                row["next_run"] = None

        return jsonify({"schedules": rows})
    except Exception as e:
        return jsonify({"schedules": [], "error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/schedules", methods=["POST"])
def api_schedules_save():
    data = request.json
    if not data:
        return jsonify({"error": "Dados inválidos."}), 400

    job_type = data.get("job_type", "")
    if job_type not in ("sync_delta", "sync_full"):
        return jsonify({"error": "Tipo inválido. Use 'sync_delta' ou 'sync_full'."}), 400

    cron_days = data.get("cron_days", "*")
    cron_hour = int(data.get("cron_hour", 2))
    cron_minute = int(data.get("cron_minute", 0))
    enabled = bool(data.get("enabled", True))
    schedule_id = data.get("id") or f"{job_type}_{cron_hour:02d}{cron_minute:02d}"

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO schedules (id, job_type, cron_days, cron_hour, cron_minute, enabled)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    job_type = EXCLUDED.job_type,
                    cron_days = EXCLUDED.cron_days,
                    cron_hour = EXCLUDED.cron_hour,
                    cron_minute = EXCLUDED.cron_minute,
                    enabled = EXCLUDED.enabled
            """, (schedule_id, job_type, cron_days, cron_hour, cron_minute, enabled))
        conn.commit()

        _register_schedule_job(schedule_id, job_type, cron_days, cron_hour, cron_minute, enabled)

        return jsonify({"ok": True, "id": schedule_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/schedules/<schedule_id>", methods=["DELETE"])
def api_schedules_delete(schedule_id):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedules WHERE id = %s", (schedule_id,))
        conn.commit()

        try:
            scheduler.remove_job(schedule_id)
        except Exception:
            pass

        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/schedules/<schedule_id>/toggle", methods=["POST"])
def api_schedules_toggle(schedule_id):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("UPDATE schedules SET enabled = NOT enabled WHERE id = %s RETURNING *", (schedule_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Agendamento não encontrado."}), 404
        conn.commit()

        _register_schedule_job(
            row["id"], row["job_type"], row["cron_days"],
            row["cron_hour"], row["cron_minute"], row["enabled"],
        )

        return jsonify({"ok": True, "enabled": row["enabled"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Debug
# ---------------------------------------------------------------------------

@app.route("/api/debug")
def api_debug():
    return jsonify({
        "sync_running": _sync_running,
        "sync_proc_alive": _sync_proc is not None and _sync_proc.poll() is None if _sync_proc else False,
        "sync_log_count": len(_sync_logs),
        "sync_logs_last5": list(_sync_logs)[-5:] if _sync_logs else [],
        "update_running": _update_running,
        "update_log_count": len(_update_logs),
        "python": sys.executable,
        "sync_script": SYNC_SCRIPT,
        "sync_script_exists": Path(SYNC_SCRIPT).exists(),
        "cwd": str(BASE_DIR),
    })


# ---------------------------------------------------------------------------
# APScheduler
# ---------------------------------------------------------------------------

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")

DAY_MAP = {"0": "mon", "1": "tue", "2": "wed", "3": "thu", "4": "fri", "5": "sat", "6": "sun"}


def _run_scheduled_sync(job_type):
    """Executa sync agendado (roda no thread do scheduler)."""
    global _sync_running, _sync_proc

    if _sync_running:
        app.logger.info("Scheduled %s skipped — sync already running", job_type)
        return

    mode = "full" if job_type == "sync_full" else "delta"
    _sync_running = True
    _sync_logs.clear()

    try:
        cmd = [sys.executable, SYNC_SCRIPT]
        if mode == "full":
            cmd.append("--full")

        _add_sync_log(f"[AGENDADO] Sincronização {mode.upper()} iniciada automaticamente")

        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, cwd=str(BASE_DIR), env=env,
        )
        _sync_proc = proc

        for line in proc.stdout:
            _add_sync_log(line)

        proc.wait()

        if proc.returncode == 0:
            _add_sync_log("[FIM] Sincronização agendada concluída com sucesso")
        else:
            _add_sync_log(f"[ERRO] Sincronização agendada falhou (exit code {proc.returncode})")

        # Update last_run_at
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("UPDATE schedules SET last_run_at = NOW() WHERE job_type = %s", (job_type,))
            conn.commit()
            conn.close()
        except Exception:
            pass

    except Exception as e:
        _add_sync_log(f"[ERRO] {e}")
    finally:
        _sync_proc = None
        _sync_running = False


def _register_schedule_job(schedule_id, job_type, cron_days, cron_hour, cron_minute, enabled):
    """Register or update a scheduler job."""
    try:
        scheduler.remove_job(schedule_id)
    except Exception:
        pass

    if not enabled:
        return

    if cron_days == "*":
        day_of_week = "*"
    else:
        parts = [d.strip() for d in cron_days.split(",")]
        day_of_week = ",".join(DAY_MAP.get(p, p) for p in parts)

    trigger = CronTrigger(
        day_of_week=day_of_week,
        hour=cron_hour,
        minute=cron_minute,
        timezone="America/Sao_Paulo",
    )

    scheduler.add_job(
        _run_scheduled_sync,
        trigger=trigger,
        args=[job_type],
        id=schedule_id,
        replace_existing=True,
        misfire_grace_time=300,
    )


def _load_schedules_from_db():
    """Load all schedules from DB and register them in APScheduler."""
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM schedules")
            for row in cur.fetchall():
                _register_schedule_job(
                    row["id"], row["job_type"], row["cron_days"],
                    row["cron_hour"], row["cron_minute"], row["enabled"],
                )
        conn.close()
        app.logger.info("Schedules loaded from DB")
    except Exception as e:
        app.logger.warning("Could not load schedules: %s", e)


def _ensure_schedules_table():
    """Create the schedules table if it doesn't exist yet."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS schedules (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    cron_days TEXT NOT NULL DEFAULT '*',
                    cron_hour INTEGER NOT NULL DEFAULT 2,
                    cron_minute INTEGER NOT NULL DEFAULT 0,
                    enabled BOOLEAN DEFAULT TRUE,
                    last_run_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        app.logger.warning("Could not ensure schedules table: %s", e)


def _ensure_turmas_table():
    """Create the turmas table if it doesn't exist yet."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS turmas (
                    id         SERIAL PRIMARY KEY,
                    nivel      TEXT NOT NULL,
                    nome       TEXT NOT NULL,
                    dt_inicio  DATE NOT NULL,
                    dt_fim     DATE NOT NULL,
                    ano        INTEGER NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(nivel, nome)
                )
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        app.logger.warning("Could not ensure turmas table: %s", e)


def _ensure_ciclos_table():
    """Create the ciclos table if it doesn't exist yet."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ciclos (
                    id         SERIAL PRIMARY KEY,
                    nivel      TEXT NOT NULL,
                    nome       TEXT NOT NULL,
                    dt_inicio  DATE NOT NULL,
                    dt_fim     DATE NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(nivel, nome)
                )
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        app.logger.warning("Could not ensure ciclos table: %s", e)


# Start scheduler
_ensure_schedules_table()
_ensure_turmas_table()
_ensure_ciclos_table()
scheduler.start()
_load_schedules_from_db()


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001, threaded=True, use_reloader=False)
