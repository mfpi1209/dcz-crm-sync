"""
eduit. — Dashboard Comercial.

Upload de CSV de matrículas (Power BI), integração com dados do Match & Merge,
ranking de agentes comerciais via Kommo, e dashboard com KPIs e comparativos.

Endpoints:
  POST /api/comercial-rgm/upload        upload CSV e importa para o banco
  GET  /api/comercial-rgm/data          dados filtrados (KPIs + evolução + ranking)
  GET  /api/comercial-rgm/filters       listas de polos, níveis e agentes
  GET  /api/comercial-rgm/snapshot-info info do último upload
  POST /api/comercial-rgm/sync-users    sincroniza usuários do Kommo
"""

import os
import csv
import io
import logging
import requests
from datetime import datetime, date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)

comercial_rgm_bp = Blueprint("comercial_rgm", __name__)

MM_TIPO_MAT_VALIDOS = (
    'INGRESSANTE', 'NOVA MATRICULA', 'NOVA MATRÍCULA', 'RETORNO', 'RECOMPRA'
)

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

KOMMO_BASE_URL = os.getenv("KOMMO_BASE_URL", "https://eduitbr.kommo.com").rstrip("/")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")


def _pg():
    return psycopg2.connect(**DB_DSN)


def _pg_kommo():
    return psycopg2.connect(**KOMMO_DB_DSN)


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

CREATE TABLE IF NOT EXISTS kommo_users (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    email       TEXT,
    synced_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mm_snapshots (
    id               SERIAL PRIMARY KEY,
    snapshot_id      TEXT NOT NULL,
    executed_at      TIMESTAMP DEFAULT NOW(),
    nivel            TEXT,
    total_inscritos  INTEGER,
    total_matriculados INTEGER,
    total_cruzados   INTEGER
);

CREATE TABLE IF NOT EXISTS mm_inscritos_hist (
    id SERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    tipo TEXT, status TEXT, dt_pag_insc TEXT, inscricao TEXT,
    nome TEXT, sexo TEXT, cpf TEXT, rg TEXT,
    curso_raw TEXT, curso_limpo TEXT, grau_curso TEXT, modalidade TEXT,
    polo_raw TEXT, polo_normalizado TEXT, marca_instituicao TEXT,
    data_inscr DATE, data_prova DATE,
    telefone TEXT, telefone_res TEXT, telefone_com TEXT,
    email TEXT, cep TEXT, endereco TEXT, bairro TEXT, cidade TEXT, estado TEXT,
    data_pagamento TEXT, data_matricula TEXT,
    situacao_raw TEXT, situacao_final TEXT,
    observacao TEXT, captador TEXT, trimestre_ingresso TEXT,
    chave_preco TEXT, preco_balcao TEXT, area_curso TEXT, semestres TEXT,
    arquivo_origem TEXT, uploaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mmih_snap ON mm_inscritos_hist(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_mmih_cpf  ON mm_inscritos_hist(cpf);
CREATE INDEX IF NOT EXISTS idx_mmih_data ON mm_inscritos_hist(data_inscr);

CREATE TABLE IF NOT EXISTS mm_matriculados_hist (
    id SERIAL PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    tipo TEXT, nome TEXT, cpf TEXT, rgm TEXT, rg TEXT, sexo TEXT, data_nasc TEXT,
    polo_captador TEXT, tipo_polo TEXT, polo_aulas TEXT,
    curso_raw TEXT, curso_limpo TEXT,
    prouni TEXT, serie TEXT, data_matricula TEXT, ano_tri_ingresso TEXT,
    tipo_matricula TEXT, situacao_raw TEXT, situacao TEXT,
    fone_res TEXT, fone_com TEXT, fone_cel TEXT, email TEXT, email_ad TEXT,
    endereco TEXT, bairro TEXT, cidade TEXT,
    arquivo_origem TEXT, uploaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mmhm_snap ON mm_matriculados_hist(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_mmhm_cpf  ON mm_matriculados_hist(cpf);

CREATE INDEX IF NOT EXISTS idx_mmhm_data ON mm_matriculados_hist(data_matricula);
"""

_METAS_DDL = """
CREATE TABLE IF NOT EXISTS comercial_metas (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    user_name  TEXT,
    meta       INTEGER NOT NULL DEFAULT 0,
    dt_inicio  DATE NOT NULL,
    dt_fim     DATE NOT NULL,
    descricao  TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cm_user ON comercial_metas(user_id);
CREATE INDEX IF NOT EXISTS idx_cm_dates ON comercial_metas(dt_inicio, dt_fim);
"""


def _ensure_table():
    conn = _pg()
    cur = conn.cursor()
    cur.execute(_CREATE_SQL)
    conn.commit()

    # Migrate comercial_metas if old schema exists
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'comercial_metas' AND column_name = 'dt_inicio'
        """)
        has_new_schema = cur.fetchone() is not None

        if not has_new_schema:
            cur.execute("DROP TABLE IF EXISTS comercial_metas CASCADE")
            conn.commit()

        cur.execute(_METAS_DDL)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning("comercial_metas migration: %s", e)
        try:
            cur.execute("DROP TABLE IF EXISTS comercial_metas CASCADE")
            conn.commit()
            cur.execute(_METAS_DDL)
            conn.commit()
        except Exception as e2:
            conn.rollback()
            logger.error("comercial_metas create failed: %s", e2)

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
    """Desloca uma data por N meses.
    Se d é o último dia do mês, o resultado também é o último dia do mês alvo.
    """
    import calendar
    is_last = d.day == calendar.monthrange(d.year, d.month)[1]
    m = d.month + months
    y = d.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    max_day = calendar.monthrange(y, m)[1]
    return date(y, m, max_day if is_last else min(d.day, max_day))


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

        cur.execute("SELECT COUNT(*) FROM mm_inscritos_hist")
        mm_insc = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM mm_matriculados_hist")
        mm_mat = cur.fetchone()[0] or 0

        cur.close()
        conn.close()
        return jsonify({
            "ok": True,
            "total": row[0] or 0,
            "min_date": row[1].isoformat() if row[1] else None,
            "max_date": row[2].isoformat() if row[2] else None,
            "uploaded_at": row[3].isoformat() if row[3] else None,
            "mm_inscritos": mm_insc,
            "mm_matriculados": mm_mat,
        })
    except Exception as e:
        logger.exception("snapshot-info error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/sync-users", methods=["POST"])
def crgm_sync_users():
    """Sync Kommo users via API v4 and store in both databases."""
    if not KOMMO_TOKEN:
        return jsonify({"error": "KOMMO_TOKEN não configurado"}), 500
    try:
        headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
        url = f"{KOMMO_BASE_URL}/api/v4/users"
        all_users = []
        page = 1
        while True:
            resp = requests.get(url, headers=headers, params={"page": page, "limit": 250}, timeout=15)
            logger.info("sync-users page %d -> status %d", page, resp.status_code)
            if resp.status_code != 200:
                logger.warning("sync-users API returned %d: %s", resp.status_code, resp.text[:300])
                break
            data = resp.json()
            embedded = data.get("_embedded", {}).get("users", [])
            if not embedded:
                break
            all_users.extend(embedded)
            page += 1

        if not all_users:
            return jsonify({"ok": True, "synced": 0, "msg": "Nenhum usuário retornado pela API"})

        conn = _pg()
        cur = conn.cursor()
        for u in all_users:
            cur.execute("""
                INSERT INTO kommo_users (id, name, email, synced_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, synced_at = NOW()
            """, (u["id"], u.get("name", ""), u.get("email", "")))
        conn.commit()
        cur.close()
        conn.close()

        try:
            kconn = _pg_kommo()
            kcur = kconn.cursor()
            kcur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY, name TEXT, email TEXT,
                    lang TEXT, rights_json JSONB, synced_at TEXT
                )
            """)
            for u in all_users:
                kcur.execute("""
                    INSERT INTO users (id, name, email, synced_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, synced_at = NOW()
                """, (u["id"], u.get("name", ""), u.get("email", "")))
            kconn.commit()
            kcur.close()
            kconn.close()
        except Exception as e:
            logger.warning("sync-users kommo_sync write: %s", e)

        return jsonify({"ok": True, "synced": len(all_users)})
    except Exception as e:
        logger.exception("sync-users error")
        return jsonify({"error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/filters")
def crgm_filters():
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT polo FROM (
                SELECT polo FROM comercial_rgm WHERE polo IS NOT NULL
                UNION
                SELECT polo_aulas AS polo FROM mm_matriculados
                WHERE polo_aulas IS NOT NULL
                  AND UPPER(COALESCE(tipo_matricula,'')) IN %s
            ) sub ORDER BY polo
        """, (MM_TIPO_MAT_VALIDOS,))
        polos = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT nivel FROM comercial_rgm WHERE nivel IS NOT NULL ORDER BY nivel")
        niveis = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT ciclo FROM comercial_rgm WHERE ciclo IS NOT NULL ORDER BY ciclo")
        ciclos = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id, name FROM kommo_users ORDER BY name")
        agentes = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
        cur.close()
        conn.close()

        if not agentes:
            agentes = [{"id": k, "name": v} for k, v in sorted(_KNOWN_USERS.items(), key=lambda x: x[1])]

        return jsonify({"ok": True, "polos": polos, "niveis": niveis, "ciclos": ciclos, "agentes": agentes})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


_KNOWN_USERS = {
    8239958:  "Fran",
    8240165:  "Isabela",
    8240189:  "Juliana",
    8240438:  "Claudia",
    8261837:  "Admin",
    9718419:  "Felipe",
    10329248: "Andreina",
    10729260: "Jessica",
    11741316: "Bruno",
    12158628: "Hugo",
    12209212: "Gabriela",
    12908868: "Diogo",
    13018348: "Kamily",
    13304804: "T.I",
    14205944: "Thainá",
    14464488: "Tamires",
    14482884: "Eduardo",
    14546744: "Suporte",
    14546760: "Jessica C",
    14932700: "Beatriz",
}


def _fetch_kommo_user_names(user_ids):
    """Get user names: known map -> kommo_sync.users -> dcz_sync.kommo_users -> API."""
    user_map = {}
    if not user_ids:
        return user_map

    for uid in user_ids:
        if uid in _KNOWN_USERS:
            user_map[uid] = _KNOWN_USERS[uid]

    missing = [uid for uid in user_ids if uid not in user_map]
    if not missing:
        return user_map

    try:
        conn = _pg_kommo()
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM users WHERE id = ANY(%s)", (missing,))
        for r in cur.fetchall():
            user_map[r[0]] = r[1]
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("fetch user names from kommo_sync.users: %s", e)

    missing = [uid for uid in user_ids if uid not in user_map]
    if missing:
        try:
            conn = _pg()
            cur = conn.cursor()
            cur.execute("SELECT id, name FROM kommo_users WHERE id = ANY(%s)", (missing,))
            for r in cur.fetchall():
                user_map[r[0]] = r[1]
            cur.close()
            conn.close()
        except Exception:
            pass

    missing = [uid for uid in user_ids if uid not in user_map]
    if missing and KOMMO_TOKEN:
        try:
            headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
            all_resp = requests.get(
                f"{KOMMO_BASE_URL}/api/v4/users",
                headers=headers, params={"limit": 250}, timeout=15
            )
            if all_resp.status_code == 200:
                api_users = all_resp.json().get("_embedded", {}).get("users", [])
                for u in api_users:
                    uid = u.get("id")
                    if uid in missing:
                        user_map[uid] = u.get("name", f"User #{uid}")
        except Exception as e:
            logger.warning("fetch user names from API: %s", e)

    return user_map


def _date_to_epoch(dt_str):
    """Convert 'YYYY-MM-DD' to Unix epoch int, or None."""
    if not dt_str:
        return None
    try:
        return int(datetime.strptime(dt_str, "%Y-%m-%d").timestamp())
    except Exception:
        return None


def _build_agent_ranking(dt_ini=None, dt_fim=None, polo=None):
    """Build agent ranking by cross-referencing CSV matrículas with Kommo leads.

    Logic (matches the BI):
      1. kommo_sync: leads with status=142 (Ganho) -> extract RGM from custom fields
         -> build RGM->responsible_user_id map
      2. dcz_sync: comercial_rgm (CSV) filtered by date/polo
         -> count matrículas per agent using the RGM map
      3. Also include CRM-only stats (total leads, novos, perdidos, ativos)
    """
    try:
        # --- Step 1: build RGM -> responsible_user_id from Kommo leads ---
        kconn = _pg_kommo()
        kcur = kconn.cursor()

        # Priority: Ganho leads first, then any lead with RGM
        kcur.execute("""
            SELECT regexp_replace(lcf.values_json->0->>'value', '[^0-9]', '', 'g') AS rgm,
                   l.responsible_user_id,
                   l.status_id
            FROM lead_custom_field_values lcf
            JOIN leads l ON l.id = lcf.lead_id AND l.is_deleted = FALSE
            WHERE lcf.field_name = 'RGM'
              AND lcf.values_json->0->>'value' IS NOT NULL
              AND lcf.values_json->0->>'value' != ''
            ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
        """)
        rgm_to_user = {}
        for row in kcur.fetchall():
            rgm, uid = row[0], row[1]
            if rgm and uid and rgm not in rgm_to_user:
                rgm_to_user[rgm] = uid

        logger.info("rgm_to_user map: %d entries from Kommo leads", len(rgm_to_user))

        # --- CRM totals per agent (all-time) ---
        ep_ini = _date_to_epoch(dt_ini)
        ep_fim = _date_to_epoch(dt_fim)
        if ep_fim is not None:
            ep_fim += 86399

        kcur.execute("""
            SELECT l.responsible_user_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN l.status_id = 142 THEN 1 ELSE 0 END) AS ganhos,
                   SUM(CASE WHEN l.status_id = 143 THEN 1 ELSE 0 END) AS perdidos,
                   SUM(CASE WHEN l.status_id NOT IN (142, 143) THEN 1 ELSE 0 END) AS ativos,
                   SUM(CASE WHEN l.status_id = 143 AND l.closed_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.closed_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.closed_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS perdidos_periodo,
                   SUM(CASE WHEN l.created_at IS NOT NULL
                            AND (%(ep_ini)s IS NULL OR l.created_at >= %(ep_ini)s)
                            AND (%(ep_fim)s IS NULL OR l.created_at <= %(ep_fim)s)
                       THEN 1 ELSE 0 END) AS novos_periodo
            FROM leads l
            WHERE l.responsible_user_id IS NOT NULL
                  AND l.is_deleted = FALSE
            GROUP BY l.responsible_user_id
        """, {"ep_ini": ep_ini, "ep_fim": ep_fim})
        crm_stats = {}
        for r in kcur.fetchall():
            crm_stats[r[0]] = {
                "total": r[1], "ganhos": r[2], "perdidos": r[3],
                "ativos": r[4], "perdidos_periodo": r[5], "novos_periodo": r[6],
            }
        kcur.close()
        kconn.close()

        # --- Step 2: count matrículas per agent via RGM ---
        # Sources: comercial_rgm (CSV upload) + mm_matriculados (M&M upload)
        conn = _pg()
        cur = conn.cursor()

        all_rgms = set()
        cpf_to_rgm = {}

        # Source A: CSV (comercial_rgm)
        csv_where = []
        csv_params = []
        if dt_ini:
            csv_where.append("data_matricula >= %s")
            csv_params.append(dt_ini)
        if dt_fim:
            csv_where.append("data_matricula <= %s")
            csv_params.append(dt_fim)
        if polo:
            csv_where.append("polo = %s")
            csv_params.append(polo)
        csv_w = ("WHERE " + " AND ".join(csv_where)) if csv_where else ""

        cur.execute(f"SELECT rgm FROM comercial_rgm {csv_w}", csv_params)
        for r in cur.fetchall():
            if r[0] and r[0].strip():
                all_rgms.add(r[0].strip())

        # Source B: M&M matriculados (dedup via set)
        mm_where = ["UPPER(COALESCE(tipo_matricula,'')) IN %s"]
        mm_params = [MM_TIPO_MAT_VALIDOS]
        if dt_ini:
            mm_where.append("data_matricula >= %s")
            mm_params.append(dt_ini)
        if dt_fim:
            mm_where.append("data_matricula <= %s")
            mm_params.append(dt_fim)
        if polo:
            mm_where.append("polo_aulas = %s")
            mm_params.append(polo)
        mm_w = "WHERE " + " AND ".join(mm_where)

        cur.execute(f"SELECT rgm, cpf FROM mm_matriculados {mm_w}", mm_params)
        for r in cur.fetchall():
            if r[0] and r[0].strip():
                rgm = r[0].strip()
                all_rgms.add(rgm)
                if r[1] and r[1].strip():
                    cpf_to_rgm[r[1].strip()] = rgm

        cur.close()
        conn.close()

        # Fallback: CPF -> Kommo contact -> lead -> responsible_user_id
        unmatched_rgms = [r for r in all_rgms if r not in rgm_to_user]
        if unmatched_rgms and cpf_to_rgm:
            try:
                kconn2 = _pg_kommo()
                kcur2 = kconn2.cursor()
                kcur2.execute("""
                    SELECT
                        regexp_replace(ccf.values_json->0->>'value', '[^0-9]', '', 'g') AS cpf,
                        l.responsible_user_id
                    FROM contact_custom_field_values ccf
                    JOIN contacts c ON c.id = ccf.contact_id AND c.is_deleted = FALSE
                    JOIN lead_contacts lc ON lc.contact_id = c.id
                    JOIN leads l ON l.id = lc.lead_id AND l.is_deleted = FALSE
                    WHERE ccf.field_name IN ('CPF', 'Cpf', 'cpf')
                      AND ccf.values_json->0->>'value' IS NOT NULL
                      AND ccf.values_json->0->>'value' != ''
                    ORDER BY CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END
                """)
                cpf_to_uid = {}
                for row in kcur2.fetchall():
                    cpf_val, uid = row[0], row[1]
                    if cpf_val and uid and cpf_val not in cpf_to_uid:
                        cpf_to_uid[cpf_val] = uid
                kcur2.close()
                kconn2.close()

                for cpf, rgm in cpf_to_rgm.items():
                    if rgm not in rgm_to_user and cpf in cpf_to_uid:
                        rgm_to_user[rgm] = cpf_to_uid[cpf]

                logger.info("CPF fallback: added %d extra RGM->user mappings",
                            len(rgm_to_user) - len([r for r in rgm_to_user if r]))
            except Exception as e:
                logger.warning("CPF fallback error: %s", e)

        mat_per_agent = {}
        matched_count = 0
        for rgm in all_rgms:
            uid = rgm_to_user.get(rgm)
            if uid:
                mat_per_agent[uid] = mat_per_agent.get(uid, 0) + 1
                matched_count += 1

        # --- Step 3: merge CRM stats + CSV matrículas ---
        all_uids = set(crm_stats.keys()) | set(mat_per_agent.keys())
        user_map = _fetch_kommo_user_names(list(all_uids))

        ranking = []
        for uid in all_uids:
            cs = crm_stats.get(uid, {})
            total = cs.get("total", 0)
            ganhos = cs.get("ganhos", 0)
            perdidos = cs.get("perdidos", 0)
            ativos = cs.get("ativos", 0)
            mat_periodo = mat_per_agent.get(uid, 0)
            perdidos_p = cs.get("perdidos_periodo", 0)
            novos_p = cs.get("novos_periodo", 0)
            name = user_map.get(uid, f"User #{uid}")
            taxa = round(ganhos / total * 100, 1) if total > 0 else 0
            ranking.append({
                "user_id": uid,
                "nome": name,
                "total": total,
                "ganhos": ganhos,
                "perdidos": perdidos,
                "ativos": ativos,
                "taxa_conversao": taxa,
                "matriculas_periodo": mat_periodo,
                "perdidos_periodo": perdidos_p,
                "novos_periodo": novos_p,
            })

        ranking.sort(key=lambda x: x["matriculas_periodo"], reverse=True)
        logger.info(
            "Agent ranking: %d agents, %d unique RGMs, %d matched (%.0f%%)",
            len(ranking), len(all_rgms),
            sum(mat_per_agent.values()),
            sum(mat_per_agent.values()) / max(len(all_rgms), 1) * 100
        )
        return ranking
    except Exception as e:
        logger.warning("agent ranking error: %s", e)
        import traceback
        logger.warning(traceback.format_exc())
        return []


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

        # --- Load deduplicated matrículas (CSV + MM, dedup by RGM; MM wins) ---
        cur.execute(f"SELECT rgm, data_matricula, polo FROM comercial_rgm {w}", params)
        mat = {}
        for r in cur.fetchall():
            if r[0] and r[0].strip():
                mat[r[0].strip()] = {"dt": r[1], "polo": r[2]}

        mm_kpi_where = ["UPPER(COALESCE(tipo_matricula,'')) IN %s"]
        mm_kpi_params = [MM_TIPO_MAT_VALIDOS]
        if polo:
            mm_kpi_where.append("polo_aulas = %s")
            mm_kpi_params.append(polo)
        if dt_ini:
            mm_kpi_where.append("data_matricula >= %s")
            mm_kpi_params.append(dt_ini)
        if dt_fim:
            mm_kpi_where.append("data_matricula <= %s")
            mm_kpi_params.append(dt_fim)
        mm_kpi_w = "WHERE " + " AND ".join(mm_kpi_where)

        cur.execute(f"SELECT rgm, data_matricula, polo_aulas FROM mm_matriculados {mm_kpi_w}", mm_kpi_params)
        for r in cur.fetchall():
            if r[0] and r[0].strip():
                rgm = r[0].strip()
                try:
                    dt = date.fromisoformat(r[1]) if isinstance(r[1], str) else r[1]
                except (ValueError, TypeError):
                    dt = None
                mat[rgm] = {"dt": dt, "polo": r[2]}

        all_kpi_rgms = set(mat.keys())
        vendas = len(mat)
        all_days = set(v["dt"] for v in mat.values() if v["dt"])
        dias = len(all_days) or 1
        media_diaria = round(vendas / dias, 1)

        # --- Ticket médio via Kommo lead price (cruzado por RGM) ---
        ticket_medio = 0.0
        valor_total = 0.0
        try:
            kconn = _pg_kommo()
            kcur = kconn.cursor()
            kcur.execute("""
                SELECT regexp_replace(lcf.values_json->0->>'value', '[^0-9]', '', 'g') AS rgm,
                       l.price
                FROM lead_custom_field_values lcf
                JOIN leads l ON l.id = lcf.lead_id
                              AND l.status_id = 142
                              AND l.is_deleted = FALSE
                WHERE lcf.field_name = 'RGM'
                  AND lcf.values_json->0->>'value' IS NOT NULL
                  AND lcf.values_json->0->>'value' != ''
                  AND l.price IS NOT NULL AND l.price > 0
            """)
            rgm_price = {r[0]: r[1] for r in kcur.fetchall() if r[0]}
            kcur.close()
            kconn.close()

            prices = [rgm_price[rgm] for rgm in all_kpi_rgms if rgm in rgm_price and rgm_price[rgm] > 0]
            if prices:
                valor_total = round(sum(prices) / 100, 2)
                ticket_medio = round(valor_total / len(prices), 2)
        except Exception as e:
            logger.warning("ticket medio kommo: %s", e)

        # --- MM Inscritos no período (hist + atual) ---
        mm_insc_count = 0

        insc_where_hist = []
        insc_params_hist = []
        if dt_ini:
            insc_where_hist.append("data_inscr >= %s")
            insc_params_hist.append(dt_ini)
        if dt_fim:
            insc_where_hist.append("data_inscr <= %s")
            insc_params_hist.append(dt_fim)
        if polo:
            insc_where_hist.append("polo_normalizado = %s")
            insc_params_hist.append(polo)
        insc_w_hist = ("WHERE " + " AND ".join(insc_where_hist)) if insc_where_hist else ""

        insc_where_cur = []
        insc_params_cur = []
        if dt_ini:
            insc_where_cur.append("data_inscr >= %s")
            insc_params_cur.append(dt_ini)
        if dt_fim:
            insc_where_cur.append("data_inscr <= %s")
            insc_params_cur.append(dt_fim)
        if polo:
            insc_where_cur.append("polo_normalizado = %s")
            insc_params_cur.append(polo)
        insc_w_cur = ("WHERE " + " AND ".join(insc_where_cur)) if insc_where_cur else ""

        try:
            cur.execute(f"""
                SELECT COUNT(DISTINCT cpf) FROM (
                    SELECT cpf FROM mm_inscritos_hist {insc_w_hist}
                    UNION
                    SELECT cpf FROM mm_inscritos {insc_w_cur}
                ) sub WHERE cpf IS NOT NULL
            """, insc_params_hist + insc_params_cur)
            mm_insc_count = cur.fetchone()[0] or 0
        except Exception:
            try:
                cur.execute(f"SELECT COUNT(*) FROM mm_inscritos_hist {insc_w_hist}", insc_params_hist)
                mm_insc_count = cur.fetchone()[0] or 0
            except Exception:
                mm_insc_count = 0

        # --- Comparações: 6M / 1 ano / YTD ---
        vendas_6m = 0
        vendas_1a = 0
        vendas_ytd = 0
        vendas_prev_ytd = 0

        def _count_period(cur_, d_start, d_end, polo_=polo, nivel_=nivel):
            cw = ["data_matricula >= %s", "data_matricula <= %s"]
            cp = [d_start.isoformat(), d_end.isoformat()]
            if polo_:
                cw.append("polo = %s"); cp.append(polo_)
            if nivel_:
                cw.append("nivel = %s"); cp.append(nivel_)

            cur_.execute(f"SELECT rgm FROM comercial_rgm WHERE {' AND '.join(cw)}", cp)
            p_rgms = set()
            for r in cur_.fetchall():
                if r[0] and r[0].strip():
                    p_rgms.add(r[0].strip())

            mw = ["UPPER(COALESCE(tipo_matricula,'')) IN %s",
                  "data_matricula >= %s", "data_matricula <= %s"]
            mp = [MM_TIPO_MAT_VALIDOS, d_start.isoformat(), d_end.isoformat()]
            if polo_:
                mw.append("polo_aulas = %s"); mp.append(polo_)

            cur_.execute(f"SELECT rgm FROM mm_matriculados WHERE {' AND '.join(mw)}", mp)
            for r in cur_.fetchall():
                if r[0] and r[0].strip():
                    p_rgms.add(r[0].strip())

            return len(p_rgms)

        if dt_ini and dt_fim:
            try:
                d_ini = date.fromisoformat(dt_ini)
                d_fim = date.fromisoformat(dt_fim)

                vendas_6m = _count_period(
                    cur, _shift_months(d_ini, -6), _shift_months(d_fim, -6)
                )
                vendas_1a = _count_period(
                    cur, _shift_months(d_ini, -12), _shift_months(d_fim, -12)
                )
                vendas_ytd = _count_period(
                    cur, date(d_fim.year, 1, 1), d_fim
                )
                prev_year = d_fim.year - 1
                vendas_prev_ytd = _count_period(
                    cur,
                    date(prev_year, 1, 1),
                    _safe_date(prev_year, d_fim.month, d_fim.day),
                )
            except Exception as exc:
                logger.warning("Erro no cálculo comparativos: %s", exc)

        pct_6m = round((vendas / vendas_6m - 1) * 100, 1) if vendas_6m > 0 else 0
        pct_1a = round((vendas / vendas_1a - 1) * 100, 1) if vendas_1a > 0 else 0
        pct_ytd = round((vendas_ytd / vendas_prev_ytd - 1) * 100, 1) if vendas_prev_ytd > 0 else 0

        # --- Evolução diária (from deduplicated mat) ---
        day_counts = {}
        for v in mat.values():
            if v["dt"]:
                d = v["dt"]
                day_counts[d] = day_counts.get(d, 0) + 1
        evolucao = [{"data": d.isoformat(), "count": c} for d, c in sorted(day_counts.items())]

        # --- Evolução ano anterior (deduplicated by RGM) ---
        evolucao_prev = []
        if dt_ini and dt_fim:
            try:
                d_ini = date.fromisoformat(dt_ini)
                d_fim_d = date.fromisoformat(dt_fim)
                prev_ini = _shift_months(d_ini, -12)
                prev_fim = _shift_months(d_fim_d, -12)

                prev_csv_w = ["data_matricula >= %s", "data_matricula <= %s"]
                prev_csv_p = [prev_ini.isoformat(), prev_fim.isoformat()]
                if polo:
                    prev_csv_w.append("polo = %s")
                    prev_csv_p.append(polo)
                if nivel:
                    prev_csv_w.append("nivel = %s")
                    prev_csv_p.append(nivel)
                pcw = "WHERE " + " AND ".join(prev_csv_w)

                cur.execute(f"SELECT rgm, data_matricula FROM comercial_rgm {pcw}", prev_csv_p)
                prev_mat = {}
                for r in cur.fetchall():
                    if r[0] and r[0].strip():
                        prev_mat[r[0].strip()] = r[1]

                prev_mm_w = ["UPPER(COALESCE(tipo_matricula,'')) IN %s",
                             "data_matricula >= %s", "data_matricula <= %s"]
                prev_mm_p = [MM_TIPO_MAT_VALIDOS, prev_ini.isoformat(), prev_fim.isoformat()]
                if polo:
                    prev_mm_w.append("polo_aulas = %s")
                    prev_mm_p.append(polo)
                pmw = "WHERE " + " AND ".join(prev_mm_w)

                cur.execute(f"SELECT rgm, data_matricula FROM mm_matriculados {pmw}", prev_mm_p)
                for r in cur.fetchall():
                    if r[0] and r[0].strip():
                        rgm = r[0].strip()
                        try:
                            dt = date.fromisoformat(r[1]) if isinstance(r[1], str) else r[1]
                        except (ValueError, TypeError):
                            dt = None
                        prev_mat[rgm] = dt

                prev_day_counts = {}
                for dt_val in prev_mat.values():
                    if dt_val:
                        prev_day_counts[dt_val] = prev_day_counts.get(dt_val, 0) + 1
                evolucao_prev = [{"data": d.isoformat(), "count": c}
                                 for d, c in sorted(prev_day_counts.items())]
            except Exception as exc:
                logger.warning("evolucao prev year: %s", exc)

        # --- Ranking por polo (from deduplicated mat) ---
        polo_counts = {}
        for v in mat.values():
            if v["polo"]:
                polo_counts[v["polo"]] = polo_counts.get(v["polo"], 0) + 1
        ranking_polo = [{"nome": p, "total": c}
                        for p, c in sorted(polo_counts.items(), key=lambda x: -x[1])]

        # --- Ranking por ciclo ---
        cur.execute(f"""
            SELECT ciclo, COUNT(*) AS total
            FROM comercial_rgm {w} {"AND" if w else "WHERE"} ciclo IS NOT NULL
            GROUP BY ciclo ORDER BY ciclo DESC LIMIT 10
        """, params)
        ranking_ciclo = [{"nome": r[0], "total": r[1]} for r in cur.fetchall()]

        cur.close()
        conn.close()

        # --- Ranking de agentes (CSV x Kommo cross-ref) ---
        ranking_agentes = _build_agent_ranking(dt_ini or None, dt_fim or None, polo or None)

        # --- Metas por agente (overlapping date range) ---
        metas = {}
        try:
            conn2 = _pg()
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT user_id, meta FROM comercial_metas
                WHERE dt_inicio <= %s AND dt_fim >= %s
            """, (dt_fim or '9999-12-31', dt_ini or '1900-01-01'))
            for r in cur2.fetchall():
                metas[r[0]] = metas.get(r[0], 0) + r[1]
            cur2.close()
            conn2.close()
        except Exception:
            pass

        for ag in ranking_agentes:
            ag["meta"] = metas.get(ag["user_id"], 0)

        return jsonify({
            "ok": True,
            "kpis": {
                "vendas": vendas,
                "vendas_6m": vendas_6m,
                "pct_6m": pct_6m,
                "vendas_1a": vendas_1a,
                "pct_1a": pct_1a,
                "vendas_ytd": vendas_ytd,
                "vendas_prev_ytd": vendas_prev_ytd,
                "pct_ytd": pct_ytd,
                "ticket_medio": ticket_medio,
                "valor_total": valor_total,
                "media_diaria": media_diaria,
                "dias": dias,
                "mm_inscritos": mm_insc_count,
            },
            "evolucao": evolucao,
            "evolucao_prev": evolucao_prev,
            "ranking_polo": ranking_polo,
            "ranking_ciclo": ranking_ciclo,
            "ranking_agentes": ranking_agentes,
        })
    except Exception as e:
        logger.exception("comercial_rgm data error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas", methods=["GET"])
def crgm_get_metas():
    try:
        conn = _pg()
        cur = conn.cursor()
        dt_ini = request.args.get("dt_ini", "")
        dt_fim = request.args.get("dt_fim", "")
        if dt_ini and dt_fim:
            cur.execute("""
                SELECT id, user_id, user_name, meta, dt_inicio, dt_fim, descricao
                FROM comercial_metas
                WHERE dt_inicio <= %s AND dt_fim >= %s
                ORDER BY dt_inicio DESC, user_name
            """, (dt_fim, dt_ini))
        else:
            cur.execute("""
                SELECT id, user_id, user_name, meta, dt_inicio, dt_fim, descricao
                FROM comercial_metas ORDER BY dt_inicio DESC, user_name
            """)
        rows = [{"id": r[0], "user_id": r[1], "user_name": r[2], "meta": r[3],
                 "dt_inicio": r[4].isoformat() if r[4] else None,
                 "dt_fim": r[5].isoformat() if r[5] else None,
                 "descricao": r[6]}
                for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({"ok": True, "metas": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas", methods=["POST"])
def crgm_save_metas():
    data = request.get_json(force=True)
    metas = data.get("metas", [])
    if not metas:
        return jsonify({"error": "Nenhuma meta enviada"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        saved = 0
        for m in metas:
            uid = int(m["user_id"])
            meta_val = int(m.get("meta", 0))
            name = m.get("user_name", "")
            dt_inicio = m.get("dt_inicio")
            dt_fim = m.get("dt_fim")
            descricao = m.get("descricao", "")
            if not dt_inicio or not dt_fim:
                continue
            cur.execute("""
                INSERT INTO comercial_metas (user_id, user_name, meta, dt_inicio, dt_fim, descricao)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (uid, name, meta_val, dt_inicio, dt_fim, descricao))
            saved += 1
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "saved": saved})
    except Exception as e:
        logger.exception("save metas error")
        return jsonify({"ok": False, "error": str(e)}), 500


@comercial_rgm_bp.route("/api/comercial-rgm/metas/<int:meta_id>", methods=["DELETE"])
def crgm_delete_meta(meta_id):
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM comercial_metas WHERE id = %s", (meta_id,))
        conn.commit()
        deleted = cur.rowcount
        cur.close()
        conn.close()
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
