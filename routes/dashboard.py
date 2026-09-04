import logging
import re
import threading
import traceback
import unicodedata
from datetime import date, datetime

import psycopg2
import psycopg2.extras
import requests
from flask import Blueprint, render_template, request, jsonify, current_app

from db import get_conn, get_disparos_conn
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


def _is_rematricula_empresa(empresa, nivel=None):
    """Graduação conta só empresa 12; Pós usa o próprio tipo_matricula do snapshot."""
    if (nivel or "").strip() == "Pós-Graduação":
        return True
    return bool(re.match(r'^12 -', (empresa or '').strip()))


# Colunas derivadas de matriculados_rows.data (DB disparos / Bases do Disparador).
# Headers do XLSX SIAA vêm em Title Case com acento; COALESCE cobre aliases.
_J_EMPRESA = "COALESCE(r.data->>'Empresa', r.data->>'empresa', '')"
_J_TIPO = "COALESCE(r.data->>'Tipo Matrícula', r.data->>'Tipo Matricula', r.data->>'tipo_matricula', '')"
_J_SIT = "COALESCE(r.data->>'Situação Matrícula', r.data->>'Situacao Matricula', r.data->>'situacao', '')"
_J_CICLO = "COALESCE(r.data->>'Ciclo', r.data->>'ciclo', '')"
_J_POLO = "COALESCE(r.data->>'Polo', r.data->>'polo', '')"
_J_RGM = "COALESCE(r.data->>'RGM', r.data->>'rgm', '')"
_J_CURSO = "COALESCE(r.data->>'Curso', r.data->>'curso', '')"
_J_NIVEL = "COALESCE(r.data->>'nível', r.data->>'nivel', '')"
_J_NEGOCIO = "COALESCE(r.data->>'Negócio', r.data->>'Negocio', r.data->>'negocio', '')"
_J_DATA_MAT = "COALESCE(r.data->>'Data Matrícula', r.data->>'Data Matricula', r.data->>'data_mat', r.data->>'data_matricula', '')"

_MAT_COLS = rf"""
        {_J_TIPO} AS tipo_aluno,
        {_J_EMPRESA} AS empresa,
        CASE
          WHEN {_J_DATA_MAT} ~ '^\d{{2}}/\d{{2}}/\d{{4}}' THEN
            TO_DATE(SUBSTRING({_J_DATA_MAT} FROM 1 FOR 10), 'DD/MM/YYYY')
          WHEN {_J_DATA_MAT} ~ '^\d{{4}}-\d{{2}}-\d{{2}}' THEN
            (SUBSTRING({_J_DATA_MAT} FROM 1 FOR 10))::date
          ELSE NULL
        END AS data_matricula,
        -- TRANSFERIDO no relatório = aluno que veio de outro polo para a gente.
        CASE
          WHEN TRIM({_J_SIT}) ~* '^transfer' THEN 'EM CURSO'
          ELSE NULLIF(TRIM({_J_SIT}), '')
        END AS situacao,
        CASE
          WHEN {_J_NIVEL} != '' THEN
            CASE WHEN {_J_NIVEL} ~* 'p[oó]s' THEN 'Pós-Graduação'
                 ELSE 'Graduação' END
          WHEN {_J_NEGOCIO} ~* 'p[oó]s' THEN 'Pós-Graduação'
          WHEN {_J_CURSO} ~* '(mba|especializa[cç][aã]o|p[oó]s.gradua|lato.sensu|stricto)'
               THEN 'Pós-Graduação'
          ELSE 'Graduação'
        END AS nivel,
        TRIM(REGEXP_REPLACE({_J_POLO}, '^\d+\s*[-–]\s*', '')) AS polo,
        NULLIF(regexp_replace({_J_RGM}, '[^0-9]', '', 'g'), '') AS rgm,
        {_J_CURSO} AS turma,
        CASE
          WHEN TRIM({_J_CICLO}) ~ '^\d{{4}}/\d$'
            THEN TRIM({_J_CICLO})
          ELSE NULL
        END AS ciclo,
        (TRIM({_J_SIT}) ~* '^transfer') AS inbound_transfer
"""


def _mat_cte_sql(empresa_re=r'^(12|7) -'):
    return f"""
WITH mat AS (
    SELECT
{_MAT_COLS}
    FROM matriculados_rows r
    WHERE r.snapshot_id = (
        SELECT id FROM matriculados_snapshots
        ORDER BY created_at DESC LIMIT 1
    )
      AND TRIM({_J_EMPRESA}) ~ '{empresa_re}'
)
"""


# Dashboard geral: Graduação (12) + Pós UCS (7).
_MAT_CTE = _mat_cte_sql(r'^(12|7) -')
# Rematrícula timeline: só Graduação EAD (12) — UCS-CL (79) fora do escopo.
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
    conn = get_disparos_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id::text AS id, 'matriculados' AS tipo,
                       file_name AS filename, row_count,
                       created_at AS uploaded_at
                FROM matriculados_snapshots
                ORDER BY created_at DESC LIMIT 1
            """)
            snap = cur.fetchone()
            diag = None
            if snap:
                snap["uploaded_at"] = to_brt(snap["uploaded_at"])
                snap["fonte"] = "disparador"
                cur.execute(f"""
                    SELECT
                        ARRAY_AGG(DISTINCT {_J_NEGOCIO}) FILTER (WHERE {_J_NEGOCIO} IS NOT NULL AND {_J_NEGOCIO} != '') AS negocio_vals,
                        ARRAY_AGG(DISTINCT {_J_NIVEL}) FILTER (WHERE {_J_NIVEL} IS NOT NULL AND {_J_NIVEL} != '') AS nivel_vals,
                        ARRAY_AGG(DISTINCT {_J_TIPO}) FILTER (WHERE {_J_TIPO} IS NOT NULL AND {_J_TIPO} != '') AS tipo_vals
                    FROM (
                        SELECT r.data FROM matriculados_rows r
                        WHERE r.snapshot_id = %s::uuid LIMIT 500
                    ) r
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
# Rotas — Dashboard: Métricas de Alunos (fonte: Bases Disparador / disparos)
# ---------------------------------------------------------------------------

# Alunos que já estiveram em algum relatório de matriculados e não estão no mais
# recente: na prática são transferências para outro polo. Entram no dashboard como
# situação TRANSFERIDO, com os dados da última vez em que foram vistos.
_SUMIDOS_QUERY = f"""
WITH latest AS (
    SELECT id FROM matriculados_snapshots ORDER BY created_at DESC LIMIT 1
),
latest_rgms AS (
    SELECT DISTINCT NULLIF(regexp_replace({_J_RGM}, '[^0-9]', '', 'g'), '') AS rgm
    FROM matriculados_rows r
    WHERE r.snapshot_id = (SELECT id FROM latest)
),
hist AS (
    SELECT s.created_at AS snap_at,
{_MAT_COLS}
    FROM matriculados_rows r
    JOIN matriculados_snapshots s ON s.id = r.snapshot_id
    WHERE TRIM({_J_EMPRESA}) ~ '^(12|7) -'
      AND NULLIF(regexp_replace({_J_RGM}, '[^0-9]', '', 'g'), '') IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM latest_rgms lr
          WHERE lr.rgm = NULLIF(regexp_replace({_J_RGM}, '[^0-9]', '', 'g'), '')
      )
),
ultimo AS (
    SELECT DISTINCT ON (rgm) *
    FROM hist
    ORDER BY rgm, snap_at DESC
)
SELECT
    COALESCE(u.tipo_aluno, 'Não informado') AS tipo,
    u.empresa,
    'TRANSFERIDO' AS situacao,
    u.nivel,
    u.polo,
    u.turma,
    u.ciclo,
    u.rgm,
    u.data_matricula,
    1 AS total
FROM ultimo u
"""

_SUMIDOS_CACHE = {"snapshot_id": None, "rows": None}
_SUMIDOS_LOCK = threading.Lock()
_SUMIDOS_REFRESHING = set()


def _sumidos_latest_snapshot_id(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM matriculados_snapshots ORDER BY created_at DESC LIMIT 1"
        )
        row = cur.fetchone()
    return row[0] if row else None


def _compute_sumidos(conn, snap_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_SUMIDOS_QUERY)
        rows = cur.fetchall()
    with _SUMIDOS_LOCK:
        _SUMIDOS_CACHE["snapshot_id"] = snap_id
        _SUMIDOS_CACHE["rows"] = rows
    return rows


def _refresh_sumidos_async(snap_id):
    def _run():
        conn = None
        try:
            conn = get_disparos_conn()
            _compute_sumidos(conn, snap_id)
        except Exception as e:
            logging.getLogger(__name__).warning("warm sumidos: %s", e)
        finally:
            if conn is not None:
                conn.close()
            _SUMIDOS_REFRESHING.discard(snap_id)

    if snap_id in _SUMIDOS_REFRESHING:
        return
    _SUMIDOS_REFRESHING.add(snap_id)
    threading.Thread(target=_run, daemon=True, name="warm-sumidos").start()


def _academic_sumidos_rows(conn):
    """Linhas TRANSFERIDO dos alunos ausentes do relatório mais recente (Disparador)."""
    snap_id = _sumidos_latest_snapshot_id(conn)
    if snap_id is None:
        return []

    cached_id = _SUMIDOS_CACHE["snapshot_id"]
    cached_rows = _SUMIDOS_CACHE["rows"]
    if cached_id == snap_id:
        return cached_rows
    if cached_rows is not None:
        _refresh_sumidos_async(snap_id)
        return cached_rows
    return _compute_sumidos(conn, snap_id)


def warm_academic_sumidos_cache():
    """Pré-aquece o cache no boot para ninguém pagar a varredura na primeira visita."""
    def _run():
        conn = None
        try:
            conn = get_disparos_conn()
            snap_id = _sumidos_latest_snapshot_id(conn)
            if snap_id is not None and _SUMIDOS_CACHE["snapshot_id"] != snap_id:
                _compute_sumidos(conn, snap_id)
        except Exception as e:
            logging.getLogger(__name__).warning("warm sumidos (boot): %s", e)
        finally:
            if conn is not None:
                conn.close()

    threading.Thread(target=_run, daemon=True, name="warm-sumidos-boot").start()


def _filter_sumidos(rows, dt_from=None, dt_to=None, nivel=None, ciclo=None):
    """Aplica os mesmos recortes do _STUDENT_METRICS_QUERY nas linhas sumidas."""
    di = date.fromisoformat(dt_from) if dt_from else None
    df = date.fromisoformat(dt_to) if dt_to else None
    out = []
    for r in rows:
        dm = r.get("data_matricula")
        if (di or df) and dm is None:
            continue
        if di and dm < di:
            continue
        if df and dm > df:
            continue
        if nivel and r.get("nivel") != nivel:
            continue
        if ciclo and r.get("ciclo") != ciclo:
            continue
        out.append(r)
    return out


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
    BOOL_OR(COALESCE(m.inbound_transfer, FALSE)) AS inbound_transfer,
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
    f_sit_norm = _norm_situacao_param(f_sit)
    conn = get_disparos_conn()
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

        # Quem sumiu do relatório mais recente foi transferido para outro polo:
        # entra aqui como TRANSFERIDO, com os dados da última vez que apareceu.
        # (O caminho inverso — TRANSFERIDO no relatório, aluno que veio de outro
        # polo para a gente — já vira EM CURSO no _MAT_COLS.)
        rows = list(rows) + _filter_sumidos(
            _academic_sumidos_rows(conn),
            dt_from=dt_from, dt_to=dt_to, nivel=f_nivel, ciclo=f_ciclo,
        )

        # Novos vêm do próprio snapshot (calouros + regresso + recompra). O
        # alinhamento ao "em curso" do Comercial foi revertido a pedido: as duas
        # telas voltam a ter recortes próprios. O recorte de "RGM fora do padrão"
        # também saiu daqui — no Acadêmico ele pegava todos os veteranos de
        # ciclos anteriores e não dizia nada; ele vive só no Dashboard Comercial.
        totals = {"novos": 0, "regresso": 0, "recompra": 0, "rematricula": 0, "outros": 0}
        by_situacao = {}
        by_nivel = {}
        by_polo = {}
        by_turma = {}
        by_ciclo = {}
        by_tipo_detail = {}
        raw_tipos = {}
        # Conta RGM único: no XLSX o mesmo aluno pode vir em 2 linhas (TRANSFERIDO +
        # EM CURSO). O GROUP BY junta as duas, BOOL_OR marca inbound e COUNT(*)=2 —
        # somar total inflava o pill (ex.: 77) vs a lista (só linhas transfer = 42).
        inbound_rgms = set()
        inbound_sem_rgm = 0

        for r in rows:
            tipo = r["tipo"] or "Não informado"
            cat = _classify_tipo(tipo)
            if cat == "rematricula" and not _is_rematricula_empresa(r.get("empresa"), r.get("nivel")):
                continue

            sit = r["situacao"] or "N/I"
            sit_norm = _norm_situacao_param(sit) or ""
            niv = r["nivel"] or "N/I"
            polo = normalize_polo_display(r["polo"] or "") or "N/I"
            turma = r["turma"] or "N/I"
            ciclo = r["ciclo"] or "N/I"

            if f_polo and polo != f_polo:
                continue

            # Cards de situação: sempre mostram todas as situações (respeitam ciclo/período/tipo/polo).
            if not f_tipo or _tipo_matches_filter(cat, f_tipo):
                by_situacao[sit] = by_situacao.get(sit, 0) + r["total"]
                # inbound = TRANSFERIDO no relatório (já remapeado para EM CURSO)
                if r.get("inbound_transfer") and sit_norm == "em curso":
                    rgm = (r.get("rgm") or "").strip()
                    if rgm:
                        inbound_rgms.add(rgm)
                    else:
                        inbound_sem_rgm += 1

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
            "inbound_transfers": len(inbound_rgms) + inbound_sem_rgm,
            "card_filters": {
                "novos": (
                    "Fonte: Bases do Disparador (último upload de Matriculados). "
                    "Filtrado por ciclo/nível/período, somando Calouros + Regresso + Recompra. "
                    "TRANSFERIDO no relatório vira Em Curso (veio de outro polo). "
                    "Quem sumiu do relatório atual entra como Transferido. "
                    "Empresas 12 (Graduação) e 7 (Pós UCS)."
                ),
                "rematricula": (
                    "Fonte: Bases do Disparador. Veteranos/rematrículas do snapshot mais recente, "
                    "filtrados por ciclo/nível/período. Em Pós-Graduação usa tipo_matricula do relatório. "
                    "Mesma regra de transferência dos Novos."
                ),
            },
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


_INBOUND_LIST_QUERY = f"""
WITH latest AS (
    SELECT id FROM matriculados_snapshots ORDER BY created_at DESC LIMIT 1
),
mat AS (
    SELECT
{_MAT_COLS},
        COALESCE(r.data->>'Nome', r.data->>'nome', '') AS nome
    FROM matriculados_rows r
    WHERE r.snapshot_id = (SELECT id FROM latest)
      AND TRIM({_J_EMPRESA}) ~ '^(12|7) -'
      AND TRIM({_J_SIT}) ~* '^transfer'
)
SELECT
    m.rgm,
    m.nome,
    m.polo,
    m.turma AS curso,
    COALESCE(m.tipo_aluno, '') AS tipo,
    m.ciclo,
    m.nivel,
    m.empresa
FROM mat m
WHERE m.rgm IS NOT NULL
  AND (%(dt_from)s IS NULL OR m.data_matricula >= %(dt_from)s::date)
  AND (%(dt_to)s   IS NULL OR m.data_matricula <= %(dt_to)s::date)
  AND (%(f_nivel)s IS NULL OR m.nivel = %(f_nivel)s)
  AND (%(f_ciclo)s IS NULL OR m.ciclo = %(f_ciclo)s)
ORDER BY m.nome NULLS LAST, m.rgm
"""


@dashboard_bp.route("/api/dashboard/inbound-transfers")
def api_dashboard_inbound_transfers():
    """Lista alunos que vieram de outro polo (TRANSFERIDO no relatório → Em Curso)."""
    dt_from = request.args.get("from", "")
    dt_to = request.args.get("to", "")
    f_nivel = request.args.get("nivel", "")
    f_ciclo = request.args.get("ciclo", "")
    f_polo = request.args.get("polo", "")
    f_tipo = request.args.get("tipo", "")
    conn = get_disparos_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(_INBOUND_LIST_QUERY, {
                "dt_from": dt_from or None,
                "dt_to": dt_to or None,
                "f_nivel": f_nivel or None,
                "f_ciclo": f_ciclo or None,
            })
            rows = cur.fetchall()
        out = []
        for r in rows:
            polo = normalize_polo_display(r.get("polo") or "") or "N/I"
            if f_polo and polo != f_polo:
                continue
            cat = _classify_tipo(r.get("tipo") or "")
            if f_tipo and not _tipo_matches_filter(cat, f_tipo):
                continue
            if cat == "rematricula" and not _is_rematricula_empresa(r.get("empresa"), r.get("nivel")):
                continue
            out.append({
                "rgm": r.get("rgm") or "",
                "nome": (r.get("nome") or "").strip() or "—",
                "polo": polo,
                "curso": (r.get("curso") or "").strip() or "—",
                "tipo": r.get("tipo") or "",
                "ciclo": r.get("ciclo") or "",
                "nivel": r.get("nivel") or "",
            })
        return jsonify({"ok": True, "total": len(out), "alunos": out})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
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
    conn = get_disparos_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT
                  TRIM({_J_CICLO}) AS ciclo,
                  COUNT(*) AS total,
                  COUNT(*) FILTER (
                    WHERE LOWER({_J_TIPO}) ~ '(remat|renovacao|veterano)'
                      AND TRIM({_J_EMPRESA}) ~ '^12 -'
                  ) AS rematricula
                FROM matriculados_rows r
                WHERE r.snapshot_id = (
                    SELECT id FROM matriculados_snapshots
                    ORDER BY created_at DESC LIMIT 1
                )
                  AND TRIM({_J_CICLO}) ~ '^\\d{{4}}/\\d$'
                  AND TRIM({_J_EMPRESA}) ~ '^(12|7) -'
                GROUP BY 1
                ORDER BY
                  (substring(TRIM({_J_CICLO}) from '^([0-9]{{4}})'))::int DESC,
                  (substring(TRIM({_J_CICLO}) from '/([0-9])$'))::int DESC
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

    conn = get_disparos_conn()
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
            if cat == "rematricula" and not _is_rematricula_empresa(r.get("empresa"), r.get("nivel")):
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
    m.ciclo AS ciclo, m.nivel AS ciclo_nivel,
    COALESCE(m.tipo_aluno, 'Não informado') AS tipo,
    m.situacao, m.nivel, m.polo, COUNT(*) AS total
FROM mat m
WHERE m.ciclo IS NOT NULL
GROUP BY m.ciclo, m.nivel, m.tipo_aluno, m.situacao, m.polo
ORDER BY m.ciclo DESC, total DESC
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

    conn_cfg = get_conn()
    conn = get_disparos_conn()
    try:
        today = datetime.now().date()

        with conn_cfg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if f_nivel:
                cur.execute("SELECT nome, nivel, dt_inicio, dt_fim FROM ciclos WHERE nivel = %s ORDER BY dt_inicio", (f_nivel,))
            else:
                cur.execute("SELECT nome, nivel, dt_inicio, dt_fim FROM ciclos ORDER BY dt_inicio")
            ciclos_config = cur.fetchall()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT nivel, COUNT(*) AS total FROM (
                    SELECT CASE
                      WHEN {_J_NIVEL} != '' THEN
                        CASE WHEN {_J_NIVEL} ~* 'p[oó]s' THEN 'Pós-Graduação'
                             ELSE 'Graduação' END
                      WHEN {_J_NEGOCIO} ~* 'p[oó]s' THEN 'Pós-Graduação'
                      WHEN {_J_CURSO} ~* '(mba|especializa[cç][aã]o|p[oó]s.gradua|lato.sensu|stricto)'
                           THEN 'Pós-Graduação'
                      ELSE 'Graduação'
                    END AS nivel
                    FROM matriculados_rows r
                    WHERE r.snapshot_id = (
                        SELECT id FROM matriculados_snapshots
                        ORDER BY created_at DESC LIMIT 1
                    )
                ) sub GROUP BY nivel ORDER BY total DESC
            """)
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
        try:
            conn.close()
        except Exception:
            pass
        try:
            conn_cfg.close()
        except Exception:
            pass


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
