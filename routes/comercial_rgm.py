"""
eduit. — Dashboard Comercial RGM.

Upload de CSV de matrículas (Power BI), armazenamento no banco e
dashboard com KPIs, evolução diária e ranking por polo.

Endpoints:
  POST /api/comercial-rgm/upload        upload CSV e importa para o banco
  GET  /api/comercial-rgm/data          dados filtrados (KPIs + evolução + ranking)
  GET  /api/comercial-rgm/filters       listas de polos e níveis disponíveis
  GET  /api/comercial-rgm/snapshot-info info do último upload
"""

import os
import csv
import io
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)

comercial_rgm_bp = Blueprint("comercial_rgm", __name__)

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)


def _pg():
    return psycopg2.connect(**DB_DSN)


# ── Schema ────────────────────────────────────────────────────────────────

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS comercial_rgm (
    id              SERIAL PRIMARY KEY,
    rgm             TEXT,
    polo            TEXT,
    nivel           TEXT,
    modalidade      TEXT,
    data_matricula  DATE,
    ciclo           TEXT,
    turma           TEXT,
    financeiro      TEXT,
    valor_real      NUMERIC(12,2),
    mes_pagamento   TEXT,
    tipo_pagamento  TEXT,
    uploaded_at     TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_crgm_data  ON comercial_rgm(data_matricula);
CREATE INDEX IF NOT EXISTS idx_crgm_polo  ON comercial_rgm(polo);
CREATE INDEX IF NOT EXISTS idx_crgm_nivel ON comercial_rgm(nivel);
"""


def _ensure_table():
    conn = _pg()
    cur = conn.cursor()
    cur.execute(_CREATE_SQL)
    conn.commit()
    cur.close()
    conn.close()


_ensure_table()


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_date_br(s):
    """Parse dd/mm/yyyy or dd/m/yyyy to date object."""
    if not s or not s.strip():
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal_br(s):
    """Parse '33,62' or '1.234,56' to float."""
    if not s or not s.strip():
        return None
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _shift_months(d, months):
    """Desloca uma data por N meses, ajustando dia se necessário."""
    m = d.month + months
    y = d.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    import calendar
    max_day = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, max_day))


def _safe_date(year, month, day):
    """Cria date ajustando dia para o máximo do mês (ex: 29/Fev → 28/Fev)."""
    import calendar
    max_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day, max_day))


COL_MAP = {
    "RGM": "rgm",
    "Polo": "polo",
    "Nível": "nivel",
    "N\xedvel": "nivel",
    "Modalidade": "modalidade",
    "Data de Matrícula": "data_matricula",
    "Data de Matr\xedcula": "data_matricula",
    "Ciclo": "ciclo",
    "Turma": "turma",
    "Financeiro": "financeiro",
    "Valor Real": "valor_real",
    "Mês Pagamento": "mes_pagamento",
    "M\xeas Pagamento": "mes_pagamento",
    "Tipo de Pagamento": "tipo_pagamento",
}


def _import_csv(stream, encoding="utf-8-sig"):
    """Parse CSV stream and insert rows into comercial_rgm. Returns count."""
    reader = csv.DictReader(stream)

    rows = []
    for raw in reader:
        row = {}
        for csv_col, val in raw.items():
            db_col = COL_MAP.get(csv_col)
            if not db_col:
                continue
            row[db_col] = val
        if not row.get("rgm"):
            continue

        row["data_matricula"] = _parse_date_br(row.get("data_matricula", ""))
        row["valor_real"] = _parse_decimal_br(row.get("valor_real", ""))

        for k in ("polo", "nivel", "modalidade", "ciclo", "turma",
                   "financeiro", "mes_pagamento", "tipo_pagamento"):
            row.setdefault(k, None)
            if row[k] is not None:
                row[k] = row[k].strip() or None

        rows.append(row)

    if not rows:
        return 0

    conn = _pg()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE comercial_rgm RESTART IDENTITY")

    cols = ["rgm", "polo", "nivel", "modalidade", "data_matricula", "ciclo",
            "turma", "financeiro", "valor_real", "mes_pagamento", "tipo_pagamento"]
    sql = f"INSERT INTO comercial_rgm ({', '.join(cols)}) VALUES %s"
    tpl = "(" + ", ".join(["%s"] * len(cols)) + ")"

    values = [tuple(r.get(c) for c in cols) for r in rows]
    psycopg2.extras.execute_values(cur, sql, values, template=tpl, page_size=2000)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("comercial_rgm: imported %d rows", len(rows))
    return len(rows)


# ── Endpoints ─────────────────────────────────────────────────────────────

@comercial_rgm_bp.route("/api/comercial-rgm/upload", methods=["POST"])
def crgm_upload():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
    if not f.filename.lower().endswith(".csv"):
        return jsonify({"error": "Apenas arquivos .csv"}), 400

    try:
        raw = f.read()
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                text = raw.decode(enc)
                break
            except (UnicodeDecodeError, ValueError):
                continue
        else:
            return jsonify({"error": "Encoding não suportado"}), 400

        stream = io.StringIO(text)
        count = _import_csv(stream)
        return jsonify({"ok": True, "rows": count, "filename": f.filename})
    except Exception as e:
        logger.exception("comercial_rgm upload error")
        return jsonify({"error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/snapshot-info")
def crgm_snapshot_info():
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*), MIN(data_matricula), MAX(data_matricula),
                   MAX(uploaded_at)
            FROM comercial_rgm
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({
            "ok": True,
            "total": row[0] or 0,
            "min_date": row[1].isoformat() if row[1] else None,
            "max_date": row[2].isoformat() if row[2] else None,
            "uploaded_at": row[3].isoformat() if row[3] else None,
        })
    except Exception as e:
        logger.exception("snapshot-info error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/filters")
def crgm_filters():
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT polo FROM comercial_rgm WHERE polo IS NOT NULL ORDER BY polo")
        polos = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT nivel FROM comercial_rgm WHERE nivel IS NOT NULL ORDER BY nivel")
        niveis = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT ciclo FROM comercial_rgm WHERE ciclo IS NOT NULL ORDER BY ciclo")
        ciclos = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({"ok": True, "polos": polos, "niveis": niveis, "ciclos": ciclos})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/data")
def crgm_data():
    polo = request.args.get("polo", "")
    nivel = request.args.get("nivel", "")
    dt_ini = request.args.get("dt_ini", "")
    dt_fim = request.args.get("dt_fim", "")

    where = []
    params = []

    if polo:
        where.append("polo = %s")
        params.append(polo)
    if nivel:
        where.append("nivel = %s")
        params.append(nivel)
    if dt_ini:
        where.append("data_matricula >= %s")
        params.append(dt_ini)
    if dt_fim:
        where.append("data_matricula <= %s")
        params.append(dt_fim)

    w = ("WHERE " + " AND ".join(where)) if where else ""

    try:
        conn = _pg()
        cur = conn.cursor()

        # --- KPIs ---
        cur.execute(f"""
            SELECT COUNT(*) AS vendas,
                   COALESCE(AVG(valor_real), 0) AS ticket_medio,
                   COALESCE(SUM(valor_real), 0) AS valor_total,
                   COUNT(DISTINCT data_matricula) AS dias
            FROM comercial_rgm {w}
        """, params)
        kpi = cur.fetchone()
        vendas = kpi[0] or 0
        ticket_medio = round(float(kpi[1]), 2)
        valor_total = round(float(kpi[2]), 2)
        dias = kpi[3] or 1
        media_diaria = round(vendas / dias, 1) if dias > 0 else 0

        # --- 6M + YTD comparisons ---
        vendas_6m = 0
        vendas_ytd = 0
        vendas_prev_ytd = 0

        if dt_ini and dt_fim:
            try:
                d_ini = date.fromisoformat(dt_ini)
                d_fim = date.fromisoformat(dt_fim)

                # -- 6M: mesmo intervalo deslocado 6 meses para trás --
                d_6m_ini = _shift_months(d_ini, -6)
                d_6m_fim = _shift_months(d_fim, -6)

                cmp_where = []
                cmp_params = []
                cmp_where.append("data_matricula >= %s")
                cmp_params.append(d_6m_ini.isoformat())
                cmp_where.append("data_matricula <= %s")
                cmp_params.append(d_6m_fim.isoformat())
                if polo:
                    cmp_where.append("polo = %s")
                    cmp_params.append(polo)
                if nivel:
                    cmp_where.append("nivel = %s")
                    cmp_params.append(nivel)

                cmp_sql = "WHERE " + " AND ".join(cmp_where)
                cur.execute(f"SELECT COUNT(*) FROM comercial_rgm {cmp_sql}", cmp_params)
                vendas_6m = cur.fetchone()[0] or 0

                # -- YTD atual: 1/Jan do ano de d_fim até d_fim --
                ytd_where = ["data_matricula >= %s", "data_matricula <= %s"]
                ytd_params = [date(d_fim.year, 1, 1).isoformat(), d_fim.isoformat()]
                if polo:
                    ytd_where.append("polo = %s")
                    ytd_params.append(polo)
                if nivel:
                    ytd_where.append("nivel = %s")
                    ytd_params.append(nivel)

                cur.execute(
                    f"SELECT COUNT(*) FROM comercial_rgm WHERE {' AND '.join(ytd_where)}",
                    ytd_params,
                )
                vendas_ytd = cur.fetchone()[0] or 0

                # -- YTD anterior: 1/Jan do ano anterior até mesma data do ano anterior --
                prev_year = d_fim.year - 1
                prev_ytd_fim = _safe_date(prev_year, d_fim.month, d_fim.day)
                pytd_where = ["data_matricula >= %s", "data_matricula <= %s"]
                pytd_params = [date(prev_year, 1, 1).isoformat(), prev_ytd_fim.isoformat()]
                if polo:
                    pytd_where.append("polo = %s")
                    pytd_params.append(polo)
                if nivel:
                    pytd_where.append("nivel = %s")
                    pytd_params.append(nivel)

                cur.execute(
                    f"SELECT COUNT(*) FROM comercial_rgm WHERE {' AND '.join(pytd_where)}",
                    pytd_params,
                )
                vendas_prev_ytd = cur.fetchone()[0] or 0
            except Exception as exc:
                logger.warning("Erro no cálculo 6M/YTD: %s", exc)

        pct_6m = round((vendas / vendas_6m - 1) * 100, 1) if vendas_6m > 0 else 0
        pct_ytd = round((vendas_ytd / vendas_prev_ytd - 1) * 100, 1) if vendas_prev_ytd > 0 else 0

        # --- Evolução diária ---
        cur.execute(f"""
            SELECT data_matricula, COUNT(*)
            FROM comercial_rgm {w}
            GROUP BY data_matricula
            ORDER BY data_matricula
        """, params)
        evolucao = [{"data": r[0].isoformat(), "count": r[1]} for r in cur.fetchall() if r[0]]

        # --- Ranking por polo ---
        cur.execute(f"""
            SELECT polo, COUNT(*) AS total
            FROM comercial_rgm {w} {"AND" if w else "WHERE"} polo IS NOT NULL
            GROUP BY polo ORDER BY total DESC
        """, params)
        ranking_polo = [{"nome": r[0], "total": r[1]} for r in cur.fetchall()]

        # --- Ranking por ciclo ---
        cur.execute(f"""
            SELECT ciclo, COUNT(*) AS total
            FROM comercial_rgm {w} {"AND" if w else "WHERE"} ciclo IS NOT NULL
            GROUP BY ciclo ORDER BY ciclo DESC LIMIT 10
        """, params)
        ranking_ciclo = [{"nome": r[0], "total": r[1]} for r in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({
            "ok": True,
            "kpis": {
                "vendas": vendas,
                "vendas_6m": vendas_6m,
                "pct_6m": pct_6m,
                "vendas_ytd": vendas_ytd,
                "pct_ytd": pct_ytd,
                "ticket_medio": ticket_medio,
                "valor_total": valor_total,
                "media_diaria": media_diaria,
                "dias": dias,
            },
            "evolucao": evolucao,
            "ranking_polo": ranking_polo,
            "ranking_ciclo": ranking_ciclo,
        })
    except Exception as e:
        logger.exception("comercial_rgm data error")
        return jsonify({"ok": False, "error": str(e)}), 500
