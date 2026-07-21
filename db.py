"""
eduit. — Conexão e inicialização do banco de dados.
"""

import os
import logging
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from helpers import (
    _hash_pw,
    APP_USER_FALLBACK,
    APP_PASS_FALLBACK,
    ALL_PAGES,
    XL_TIPOS,
    SUPORTE_COMERCIAL_LOGINS,
    SUPORTE_COMERCIAL_PAGES,
)

load_dotenv(Path(__file__).parent / ".env")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DSN
# ---------------------------------------------------------------------------

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)


def get_conn():
    return psycopg2.connect(**DB_DSN)


ME_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname="marco_email",
)


def get_me_conn():
    return psycopg2.connect(**ME_DSN)


# ---------------------------------------------------------------------------
# Ensure tables
# ---------------------------------------------------------------------------


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
        logger.warning("Could not ensure schedules table: %s", e)


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
        logger.warning("Could not ensure turmas table: %s", e)


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
        logger.warning("Could not ensure ciclos table: %s", e)


def _ensure_ciclos_comercial_table():
    """Create ciclos_comercial dimension table for commercial cycle management.

    Columns:
      id, nome (ex: '26.1'), ano, semestre, dt_inicio, dt_fim, ativo,
      descricao (auto-generated label), created_at
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ciclos_comercial (
                    id         SERIAL PRIMARY KEY,
                    nome       TEXT NOT NULL UNIQUE,
                    ano        INTEGER,
                    semestre   INTEGER,
                    dt_inicio  DATE NOT NULL,
                    dt_fim     DATE NOT NULL,
                    ativo      BOOLEAN NOT NULL DEFAULT FALSE,
                    descricao  TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            for col, defn in [
                ("ano", "INTEGER"),
                ("semestre", "INTEGER"),
                ("descricao", "TEXT"),
            ]:
                cur.execute("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'ciclos_comercial' AND column_name = %s
                """, (col,))
                if not cur.fetchone():
                    cur.execute(f"ALTER TABLE ciclos_comercial ADD COLUMN {col} {defn}")
                    logger.info("ciclos_comercial: added column '%s'", col)

            cur.execute("""
                UPDATE ciclos_comercial
                SET ano = (2000 + CAST(split_part(nome, '.', 1) AS INTEGER)),
                    semestre = CAST(split_part(nome, '.', 2) AS INTEGER),
                    descricao = CASE split_part(nome, '.', 2)
                        WHEN '1' THEN '1º Semestre ' || (2000 + CAST(split_part(nome, '.', 1) AS INTEGER))
                        WHEN '2' THEN '2º Semestre ' || (2000 + CAST(split_part(nome, '.', 1) AS INTEGER))
                        ELSE nome
                    END
                WHERE nome ~ '^\\d+\\.\\d+$'
                  AND (ano IS NULL OR semestre IS NULL OR descricao IS NULL)
            """)

            # Corrige dt_fim com ano errado (ex.: 2026/2 com fim 2025-12-15 < início 2026-05-14)
            cur.execute("""
                UPDATE ciclos_comercial
                SET dt_fim = make_date(
                    EXTRACT(YEAR FROM dt_inicio)::int,
                    EXTRACT(MONTH FROM dt_fim)::int,
                    EXTRACT(DAY FROM dt_fim)::int
                )
                WHERE dt_fim < dt_inicio
            """)

            cur.execute("SELECT COUNT(*) FROM ciclos_comercial")
            if cur.fetchone()[0] == 0:
                cur.execute("""
                    INSERT INTO ciclos_comercial (nome, ano, semestre, dt_inicio, dt_fim, ativo, descricao) VALUES
                    ('25.1', 2025, 1, '2025-01-01', '2025-06-30', FALSE, '1º Semestre 2025'),
                    ('25.2', 2025, 2, '2025-07-01', '2025-12-31', FALSE, '2º Semestre 2025'),
                    ('26.1', 2026, 1, '2026-01-01', '2026-06-30', TRUE,  '1º Semestre 2026')
                """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure ciclos_comercial table: %s", e)


def _ensure_turmas_comercial_table():
    """Create turmas_comercial dimension table (monthly enrollment cohorts).

    Columns:
      id, nome (ex: 'Fevereiro'), nivel ('Graduação' | 'Pós-Graduação'),
      ciclo_id (FK), dt_inicio, dt_fim, created_at
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS turmas_comercial (
                    id         SERIAL PRIMARY KEY,
                    nome       TEXT NOT NULL,
                    nivel      TEXT NOT NULL DEFAULT 'Graduação',
                    ciclo_id   INTEGER REFERENCES ciclos_comercial(id) ON DELETE SET NULL,
                    dt_inicio  DATE NOT NULL,
                    dt_fim     DATE NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(nome, nivel, ciclo_id)
                )
            """)

            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'turmas_comercial' AND column_name = 'nivel'
            """)
            if not cur.fetchone():
                cur.execute("ALTER TABLE turmas_comercial ADD COLUMN nivel TEXT NOT NULL DEFAULT 'Graduação'")
                cur.execute("ALTER TABLE turmas_comercial DROP CONSTRAINT IF EXISTS turmas_comercial_nome_ciclo_id_key")
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_turma_nome_nivel_ciclo
                    ON turmas_comercial (nome, nivel, ciclo_id)
                """)
                logger.info("turmas_comercial: added 'nivel' column")

            cur.execute("SELECT COUNT(*) FROM turmas_comercial")
            if cur.fetchone()[0] == 0:
                cur.execute("SELECT id FROM ciclos_comercial WHERE nome = '26.1' LIMIT 1")
                row = cur.fetchone()
                if row:
                    cid = row[0]
                    cur.execute("""
                        INSERT INTO turmas_comercial (nome, nivel, ciclo_id, dt_inicio, dt_fim) VALUES
                        ('Janeiro',   'Graduação', %(c)s, '2026-01-01', '2026-01-31'),
                        ('Fevereiro', 'Graduação', %(c)s, '2026-02-01', '2026-02-28'),
                        ('Março',     'Graduação', %(c)s, '2026-03-01', '2026-03-31'),
                        ('Abril',     'Graduação', %(c)s, '2026-04-01', '2026-04-30'),
                        ('Maio',      'Graduação', %(c)s, '2026-05-01', '2026-05-31'),
                        ('Junho',     'Graduação', %(c)s, '2026-06-01', '2026-06-30'),
                        ('Janeiro',   'Pós-Graduação', %(c)s, '2026-01-01', '2026-01-31'),
                        ('Fevereiro', 'Pós-Graduação', %(c)s, '2026-02-01', '2026-02-28'),
                        ('Março',     'Pós-Graduação', %(c)s, '2026-03-01', '2026-03-31'),
                        ('Abril',     'Pós-Graduação', %(c)s, '2026-04-01', '2026-04-30'),
                        ('Maio',      'Pós-Graduação', %(c)s, '2026-05-01', '2026-05-31'),
                        ('Junho',     'Pós-Graduação', %(c)s, '2026-06-01', '2026-06-30')
                    """, {"c": cid})
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure turmas_comercial table: %s", e)


def _ensure_ciclo_atual_comercial_table():
    """Control table: tracks the current active cycle per nivel (Graduação / Pós-Graduação)."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ciclo_atual_comercial (
                    nivel   TEXT PRIMARY KEY,
                    ciclo   TEXT NOT NULL
                )
            """)
            cur.execute("SELECT COUNT(*) FROM ciclo_atual_comercial")
            if cur.fetchone()[0] == 0:
                cur.execute("""
                    INSERT INTO ciclo_atual_comercial (nivel, ciclo) VALUES
                    ('Graduação', '2026/1'),
                    ('Pós-Graduação', '2026/1')
                """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure ciclo_atual_comercial table: %s", e)


def _ensure_xl_snapshots_table():
    """Create xl_snapshots + xl_rows tables for spreadsheet history."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS xl_snapshots (
                    id          SERIAL PRIMARY KEY,
                    tipo        TEXT NOT NULL DEFAULT 'matriculados',
                    filename    TEXT NOT NULL,
                    row_count   INTEGER NOT NULL DEFAULT 0,
                    uploaded_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS xl_rows (
                    id          SERIAL PRIMARY KEY,
                    snapshot_id INTEGER NOT NULL REFERENCES xl_snapshots(id) ON DELETE CASCADE,
                    data        JSONB NOT NULL
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_xl_rows_snapshot
                ON xl_rows(snapshot_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_xl_rows_cpf
                ON xl_rows ((data->>'cpf_digits'))
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_xl_rows_rgm
                ON xl_rows ((data->>'rgm'))
            """)
            cur.execute("""
                ALTER TABLE xl_snapshots ADD COLUMN IF NOT EXISTS tipo TEXT NOT NULL DEFAULT 'matriculados'
            """)
            cur.execute("""
                ALTER TABLE xl_snapshots ADD COLUMN IF NOT EXISTS nivel TEXT
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS xl_snapshot_stats (
                    id          SERIAL PRIMARY KEY,
                    snapshot_id INTEGER NOT NULL REFERENCES xl_snapshots(id) ON DELETE CASCADE,
                    metric      TEXT NOT NULL,
                    value       NUMERIC,
                    detail      JSONB,
                    UNIQUE(snapshot_id, metric)
                )
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure xl_snapshots table: %s", e)


def _ensure_users_table():
    """Create app_users + user_permissions tables and seed admin from env."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_users (
                    id         SERIAL PRIMARY KEY,
                    username   TEXT NOT NULL UNIQUE,
                    pw_hash    TEXT NOT NULL,
                    role       TEXT NOT NULL DEFAULT 'viewer',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_permissions (
                    user_id    INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
                    page       TEXT NOT NULL,
                    PRIMARY KEY (user_id, page)
                )
            """)
            cur.execute("""
                ALTER TABLE app_users ADD COLUMN IF NOT EXISTS kommo_user_id INTEGER
            """)
            cur.execute("""
                ALTER TABLE app_users ADD COLUMN IF NOT EXISTS email_cruzeiro TEXT
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_app_users_kommo
                ON app_users(kommo_user_id) WHERE kommo_user_id IS NOT NULL
            """)
            cur.execute("""
                ALTER TABLE app_users ADD COLUMN IF NOT EXISTS categoria TEXT DEFAULT NULL
            """)
            cur.execute("""
                ALTER TABLE app_users ADD COLUMN IF NOT EXISTS datacrazy_user_id TEXT DEFAULT NULL
            """)
            cur.execute("SELECT COUNT(*) FROM app_users")
            if cur.fetchone()[0] == 0 and APP_PASS_FALLBACK:
                cur.execute(
                    "INSERT INTO app_users (username, pw_hash, role) VALUES (%s, %s, 'admin')",
                    (APP_USER_FALLBACK, _hash_pw(APP_PASS_FALLBACK)),
                )
                uid = cur.lastrowid
                cur.execute("SELECT id FROM app_users WHERE username = %s", (APP_USER_FALLBACK,))
                uid = cur.fetchone()[0]
                for page in ALL_PAGES:
                    cur.execute("INSERT INTO user_permissions (user_id, page) VALUES (%s, %s)",
                                (uid, page))
                logger.info("Admin user seeded from env vars: %s", APP_USER_FALLBACK)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure users table: %s", e)


def _ensure_suporte_comercial_users():
    """Garante categoria e permissões do time Suporte Comercial (logins conhecidos)."""
    if not SUPORTE_COMERCIAL_LOGINS:
        return
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            logins = list(SUPORTE_COMERCIAL_LOGINS)
            cur.execute(
                """
                SELECT id, username FROM app_users
                WHERE LOWER(TRIM(username)) = ANY(%s)
                """,
                (logins,),
            )
            rows = cur.fetchall()
            found = set()
            for uid, username in rows:
                found.add((username or "").strip().lower())
                cur.execute(
                    "UPDATE app_users SET categoria = %s WHERE id = %s",
                    ("Suporte Comercial", uid),
                )
                cur.execute(
                    "DELETE FROM user_permissions WHERE user_id = %s AND page = %s",
                    (uid, "dashboard"),
                )
                for page in SUPORTE_COMERCIAL_PAGES:
                    if page not in ALL_PAGES:
                        continue
                    cur.execute(
                        """
                        INSERT INTO user_permissions (user_id, page)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, page) DO NOTHING
                        """,
                        (uid, page),
                    )
                logger.info(
                    "Suporte Comercial: %s (#%s) — categoria e permissões aplicadas",
                    username,
                    uid,
                )
            missing = SUPORTE_COMERCIAL_LOGINS - found
            if missing:
                logger.warning(
                    "Suporte Comercial: logins sem cadastro em app_users: %s",
                    ", ".join(sorted(missing)),
                )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure suporte comercial users: %s", e)


def _ensure_engagement_tables():
    """Create ava_engagement, comm_rules, comm_queue, comm_log tables."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ava_engagement (
                    id                   SERIAL PRIMARY KEY,
                    rgm                  TEXT NOT NULL,
                    snapshot_date        DATE NOT NULL DEFAULT CURRENT_DATE,
                    score                INTEGER NOT NULL DEFAULT 0,
                    risk_level           TEXT NOT NULL DEFAULT 'critico',
                    days_since_enrollment INTEGER,
                    days_since_last_access INTEGER,
                    access_count         INTEGER DEFAULT 0,
                    interaction_count    INTEGER DEFAULT 0,
                    total_minutes        NUMERIC DEFAULT 0,
                    detail               JSONB,
                    UNIQUE(rgm, snapshot_date)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comm_rules (
                    id                   SERIAL PRIMARY KEY,
                    name                 TEXT NOT NULL,
                    description          TEXT DEFAULT '',
                    audience             TEXT NOT NULL DEFAULT 'todos',
                    trigger_type         TEXT NOT NULL DEFAULT 'inatividade',
                    trigger_days         INTEGER NOT NULL DEFAULT 7,
                    channel              TEXT NOT NULL DEFAULT 'email',
                    escalation_channel   TEXT,
                    escalation_after_days INTEGER,
                    message_template     TEXT NOT NULL DEFAULT '',
                    cooldown_days        INTEGER NOT NULL DEFAULT 3,
                    max_per_week         INTEGER NOT NULL DEFAULT 2,
                    priority             INTEGER NOT NULL DEFAULT 0,
                    enabled              BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at           TIMESTAMPTZ DEFAULT NOW(),
                    updated_at           TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comm_queue (
                    id                   SERIAL PRIMARY KEY,
                    rgm                  TEXT NOT NULL,
                    rule_id              INTEGER REFERENCES comm_rules(id) ON DELETE SET NULL,
                    channel              TEXT NOT NULL,
                    status               TEXT NOT NULL DEFAULT 'pendente',
                    payload              JSONB,
                    scheduled_for        TIMESTAMPTZ DEFAULT NOW(),
                    sent_at              TIMESTAMPTZ,
                    n8n_response         JSONB,
                    created_at           TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comm_log (
                    id                   SERIAL PRIMARY KEY,
                    rgm                  TEXT NOT NULL,
                    rule_id              INTEGER REFERENCES comm_rules(id) ON DELETE SET NULL,
                    channel              TEXT NOT NULL,
                    sent_at              TIMESTAMPTZ DEFAULT NOW(),
                    message_preview      TEXT,
                    status               TEXT NOT NULL DEFAULT 'enviado',
                    metadata             JSONB
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ava_eng_rgm ON ava_engagement(rgm)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ava_eng_date ON ava_engagement(snapshot_date)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ava_eng_risk ON ava_engagement(risk_level)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_comm_queue_status ON comm_queue(status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_comm_queue_rgm ON comm_queue(rgm)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_comm_log_rgm ON comm_log(rgm)")

            cur.execute("SELECT COUNT(*) FROM comm_rules")
            if cur.fetchone()[0] == 0:
                _seed_default_comm_rules(cur)

        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure engagement tables: %s", e)


def _seed_default_comm_rules(cur):
    """Insert default communication rules (best practices for student retention)."""
    rules = [
        ("Boas-vindas", "Email de boas-vindas com link do AVA", "novo_aluno",
         "sem_acesso_inicial", 0, "email", None, None,
         "Olá {{primeiro_nome}}! Bem-vindo(a) à {{curso}}! Seu ambiente virtual de aprendizagem já está disponível. Acesse agora e comece sua jornada acadêmica.", 3, 2, 10),
        ("Primeiro lembrete - 3 dias", "Lembrete gentil para novos alunos sem acesso", "novo_aluno",
         "sem_acesso_inicial", 3, "email", "whatsapp", 2,
         "Oi {{primeiro_nome}}, notamos que você ainda não acessou o ambiente virtual. Seu espaço de estudos está pronto e esperando por você! Precisa de ajuda?", 3, 2, 20),
        ("Segundo lembrete - 5 dias", "WhatsApp para novos alunos sem acesso", "novo_aluno",
         "sem_acesso_inicial", 5, "whatsapp", None, None,
         "Oi {{primeiro_nome}}! Já se passaram alguns dias desde sua matrícula em {{curso}} e ainda não identificamos seu acesso ao AVA. Precisa de ajuda para entrar? Estamos aqui!", 3, 2, 30),
        ("Alerta - 7 dias sem acesso", "Alerta para novos alunos", "novo_aluno",
         "sem_acesso_inicial", 7, "ambos", None, None,
         "{{primeiro_nome}}, já faz uma semana desde sua matrícula e não identificamos nenhum acesso ao ambiente virtual. É importante iniciar seus estudos o quanto antes. Entre em contato se precisar de suporte.", 3, 2, 40),
        ("Alerta crítico - 14 dias", "Urgência para novos alunos inativos", "novo_aluno",
         "sem_acesso_inicial", 14, "ambos", None, None,
         "{{primeiro_nome}}, notamos que ainda não houve acesso ao AVA desde sua matrícula há 14 dias. Gostaríamos de ajudá-lo(a) a iniciar seus estudos. Por favor, entre em contato conosco.", 5, 1, 50),
        ("Re-engajamento veterano", "Check-in para veteranos inativos há 7 dias", "veterano",
         "inatividade", 7, "email", "whatsapp", 5,
         "Oi {{primeiro_nome}}, sentimos sua falta! Faz alguns dias que você não acessa o AVA. Tem alguma dificuldade? Estamos à disposição.", 5, 2, 60),
        ("Escalação veterano", "WhatsApp para veteranos inativos há 14 dias", "veterano",
         "inatividade", 14, "whatsapp", None, None,
         "{{primeiro_nome}}, tudo bem? Notamos que faz 14 dias sem acessar o ambiente virtual. Podemos ajudar de alguma forma?", 5, 1, 70),
        ("Alerta veterano", "Alerta para veteranos inativos há 21 dias", "veterano",
         "inatividade", 21, "ambos", None, None,
         "{{primeiro_nome}}, já faz 21 dias sem acesso ao AVA. Isso pode impactar seu desempenho acadêmico. Entre em contato para que possamos te ajudar.", 7, 1, 80),
    ]
    for r in rules:
        cur.execute("""
            INSERT INTO comm_rules (name, description, audience, trigger_type, trigger_days,
                channel, escalation_channel, escalation_after_days, message_template,
                cooldown_days, max_per_week, priority)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, r)


def _ensure_funnel_log_table():
    """Create kommo_funnel_log table for historical funnel snapshots."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kommo_funnel_log (
                    id            SERIAL PRIMARY KEY,
                    captured_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    captured_date DATE NOT NULL,
                    source        TEXT NOT NULL DEFAULT 'live',
                    total         INTEGER NOT NULL,
                    new_today     INTEGER,
                    stages        JSONB NOT NULL
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_funnel_log_date
                ON kommo_funnel_log(captured_date)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_funnel_log_captured
                ON kommo_funnel_log(captured_at)
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure funnel_log table: %s", e)


def _ensure_suporte_tables():
    """Meta unificada + PIX diário do time Suporte Comercial (idempotente)."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_campanha (
                    id          SERIAL PRIMARY KEY,
                    nome        TEXT NOT NULL,
                    dt_inicio   DATE NOT NULL,
                    dt_fim      DATE NOT NULL,
                    ativa       BOOLEAN DEFAULT TRUE,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_meta_suporte (
                    campanha_id         INTEGER PRIMARY KEY
                        REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    meta                NUMERIC NOT NULL DEFAULT 0,
                    meta_intermediaria  NUMERIC NOT NULL DEFAULT 0,
                    supermeta           NUMERIC NOT NULL DEFAULT 0,
                    updated_at          TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_pix_suporte (
                    id              SERIAL PRIMARY KEY,
                    campanha_id     INTEGER NOT NULL
                        REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    min_matriculas  INTEGER NOT NULL,
                    valor           NUMERIC NOT NULL DEFAULT 0,
                    apenas_sabado   BOOLEAN NOT NULL DEFAULT FALSE,
                    UNIQUE(campanha_id, min_matriculas, apenas_sabado)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pps_camp ON premiacao_pix_suporte(campanha_id)")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure suporte tables: %s", e)


def _ensure_pix_faixa_tables():
    """Faixas PIX por equipe (grupo): valor conforme matrículas do dia."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_pix_faixa (
                    id              SERIAL PRIMARY KEY,
                    campanha_id     INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    grupo_id        INTEGER NOT NULL REFERENCES premiacao_grupo(id) ON DELETE CASCADE,
                    min_matriculas  INTEGER NOT NULL,
                    valor           NUMERIC NOT NULL DEFAULT 0,
                    apenas_sabado   BOOLEAN NOT NULL DEFAULT FALSE,
                    UNIQUE(campanha_id, grupo_id, min_matriculas, apenas_sabado)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ppf_camp ON premiacao_pix_faixa(campanha_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ppf_grupo ON premiacao_pix_faixa(grupo_id)")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure pix faixa tables: %s", e)


def _ensure_pix_nivel_tables():
    """Schema PIX diário por nível 1–3 (idempotente, commit isolado)."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE premiacao_meta_diaria
                ADD COLUMN IF NOT EXISTS pix_nivel INTEGER
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_pix_nivel_membro (
                    id              SERIAL PRIMARY KEY,
                    campanha_id     INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    kommo_user_id   INTEGER NOT NULL,
                    pix_nivel       INTEGER NOT NULL CHECK (pix_nivel BETWEEN 1 AND 3),
                    UNIQUE(campanha_id, kommo_user_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ppnm_camp ON premiacao_pix_nivel_membro(campanha_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pmd_pix_nivel ON premiacao_meta_diaria(pix_nivel)")
            cur.execute("""
                ALTER TABLE premiacao_meta_diaria
                DROP CONSTRAINT IF EXISTS premiacao_meta_diaria_campanha_id_kommo_user_id_dia_semana_key
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_pmd_camp_user_dow
                ON premiacao_meta_diaria (campanha_id, kommo_user_id, dia_semana)
                WHERE grupo_id IS NULL AND pix_nivel IS NULL
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_pmd_camp_pix_nivel_dow
                ON premiacao_meta_diaria (campanha_id, pix_nivel, dia_semana)
                WHERE pix_nivel IS NOT NULL
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_pmd_camp_grupo_dow
                ON premiacao_meta_diaria (campanha_id, grupo_id, dia_semana)
                WHERE grupo_id IS NOT NULL AND pix_nivel IS NULL
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure pix nivel tables: %s", e)


def _ensure_premiacao_tables():
    """Create all tables for the premiação/performance system."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_campanha (
                    id          SERIAL PRIMARY KEY,
                    nome        TEXT NOT NULL,
                    dt_inicio   DATE NOT NULL,
                    dt_fim      DATE NOT NULL,
                    ativa       BOOLEAN DEFAULT TRUE,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_tier_bonus (
                    id              SERIAL PRIMARY KEY,
                    campanha_id     INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    tier            TEXT NOT NULL,
                    valor_por_mat   NUMERIC NOT NULL DEFAULT 0,
                    UNIQUE(campanha_id, tier)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_meta_diaria (
                    id              SERIAL PRIMARY KEY,
                    campanha_id     INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    kommo_user_id   INTEGER NOT NULL,
                    dia_semana      INTEGER NOT NULL,
                    meta_diaria     INTEGER NOT NULL DEFAULT 0,
                    bonus_fixo      NUMERIC NOT NULL DEFAULT 0,
                    bonus_extra     NUMERIC NOT NULL DEFAULT 0,
                    UNIQUE(campanha_id, kommo_user_id, dia_semana)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_recebimento_regra (
                    id              SERIAL PRIMARY KEY,
                    campanha_id     INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    tier            TEXT NOT NULL DEFAULT 'qualquer',
                    modo            TEXT NOT NULL DEFAULT 'percentual',
                    valor           NUMERIC NOT NULL DEFAULT 0,
                    UNIQUE(campanha_id, tier)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS recebimentos_snapshots (
                    id          SERIAL PRIMARY KEY,
                    filename    TEXT,
                    row_count   INTEGER DEFAULT 0,
                    mes_ref     TEXT,
                    uploaded_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comercial_recebimentos (
                    id              SERIAL PRIMARY KEY,
                    snapshot_id     INTEGER REFERENCES recebimentos_snapshots(id) ON DELETE CASCADE,
                    rgm             TEXT NOT NULL,
                    nivel           TEXT,
                    modalidade      TEXT,
                    data_matricula  DATE,
                    valor           NUMERIC NOT NULL DEFAULT 0,
                    tipo_pagamento  TEXT,
                    mes_referencia  TEXT,
                    turma           TEXT,
                    data            JSONB
                )
            """)
            cur.execute(
                "ALTER TABLE comercial_recebimentos ADD COLUMN IF NOT EXISTS ciclo TEXT"
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cr_rgm ON comercial_recebimentos(rgm)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_cr_snap ON comercial_recebimentos(snapshot_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pmd_camp ON premiacao_meta_diaria(campanha_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pmd_user ON premiacao_meta_diaria(kommo_user_id)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_grupo (
                    id          SERIAL PRIMARY KEY,
                    campanha_id INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    nome        TEXT NOT NULL,
                    created_at  TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(campanha_id, nome)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_grupo_membro (
                    id              SERIAL PRIMARY KEY,
                    grupo_id        INTEGER NOT NULL REFERENCES premiacao_grupo(id) ON DELETE CASCADE,
                    kommo_user_id   INTEGER NOT NULL,
                    UNIQUE(grupo_id, kommo_user_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pgm_grupo ON premiacao_grupo_membro(grupo_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pgm_user ON premiacao_grupo_membro(kommo_user_id)")

            cur.execute("""
                ALTER TABLE premiacao_meta_diaria
                ADD COLUMN IF NOT EXISTS grupo_id INTEGER REFERENCES premiacao_grupo(id) ON DELETE CASCADE
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pmd_grupo ON premiacao_meta_diaria(grupo_id)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_campanha_meta (
                    id                  SERIAL PRIMARY KEY,
                    campanha_id         INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    kommo_user_id       INTEGER NOT NULL,
                    meta                NUMERIC NOT NULL DEFAULT 0,
                    meta_intermediaria  NUMERIC NOT NULL DEFAULT 0,
                    supermeta           NUMERIC NOT NULL DEFAULT 0,
                    UNIQUE(campanha_id, kommo_user_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pcm_camp ON premiacao_campanha_meta(campanha_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pcm_user ON premiacao_campanha_meta(kommo_user_id)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_meta_suporte (
                    campanha_id         INTEGER PRIMARY KEY
                        REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    meta                NUMERIC NOT NULL DEFAULT 0,
                    meta_intermediaria  NUMERIC NOT NULL DEFAULT 0,
                    supermeta           NUMERIC NOT NULL DEFAULT 0,
                    updated_at          TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_pix_suporte (
                    id              SERIAL PRIMARY KEY,
                    campanha_id     INTEGER NOT NULL
                        REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    min_matriculas  INTEGER NOT NULL,
                    valor           NUMERIC NOT NULL DEFAULT 0,
                    apenas_sabado   BOOLEAN NOT NULL DEFAULT FALSE,
                    UNIQUE(campanha_id, min_matriculas, apenas_sabado)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pps_camp ON premiacao_pix_suporte(campanha_id)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_campanha_link (
                    id              SERIAL PRIMARY KEY,
                    campanha_a_id   INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    campanha_b_id   INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(campanha_a_id, campanha_b_id)
                )
            """)

            # Pré-definição de metas em quantidade de matrículas (Dashboard comercial + ranking)
            cur.execute(
                "ALTER TABLE premiacao_campanha ADD COLUMN IF NOT EXISTS def_meta_intermediaria NUMERIC"
            )
            cur.execute("ALTER TABLE premiacao_campanha ADD COLUMN IF NOT EXISTS def_meta NUMERIC")
            cur.execute("ALTER TABLE premiacao_campanha ADD COLUMN IF NOT EXISTS def_supermeta NUMERIC")

            # Metas + R$/matrícula por equipe (override da campanha, com fallback transparente)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_grupo_meta (
                    campanha_id       INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    grupo_id          INTEGER NOT NULL REFERENCES premiacao_grupo(id) ON DELETE CASCADE,
                    meta_intermediaria NUMERIC,
                    meta              NUMERIC,
                    supermeta         NUMERIC,
                    valor_base        NUMERIC,
                    valor_intermediaria NUMERIC,
                    valor_meta        NUMERIC,
                    valor_supermeta   NUMERIC,
                    updated_at        TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (campanha_id, grupo_id)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS agent_matriculas (
                    id              SERIAL PRIMARY KEY,
                    user_id         INTEGER REFERENCES app_users(id),
                    kommo_user_id   INTEGER,
                    rgm             TEXT,
                    nome            TEXT,
                    curso           TEXT,
                    polo            TEXT,
                    data_matricula  DATE,
                    ciclo           TEXT,
                    nivel           TEXT,
                    kommo_lead_id   TEXT,
                    observacao      TEXT,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Corrige datas com ano typo (62026 → 2026) que quebram psycopg2/Python
            cur.execute("""
                UPDATE agent_matriculas
                SET data_matricula = make_date(
                    2000 + (EXTRACT(YEAR FROM data_matricula)::int % 100),
                    EXTRACT(MONTH FROM data_matricula)::int,
                    EXTRACT(DAY FROM data_matricula)::int
                )
                WHERE data_matricula IS NOT NULL
                  AND EXTRACT(YEAR FROM data_matricula) > 9999
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS matricula_ajustes (
                    id              SERIAL PRIMARY KEY,
                    user_id         INTEGER REFERENCES app_users(id),
                    kommo_user_id   INTEGER,
                    tipo            TEXT NOT NULL DEFAULT 'matricula_nao_computada',
                    rgm             TEXT,
                    nome_aluno      TEXT,
                    curso           TEXT,
                    polo            TEXT,
                    data_matricula  DATE,
                    kommo_lead_id   TEXT,
                    descricao       TEXT,
                    status          TEXT NOT NULL DEFAULT 'pendente',
                    resposta_admin  TEXT,
                    admin_user_id   INTEGER,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    resolved_at     TIMESTAMPTZ
                )
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure premiacao tables: %s", e)
    _ensure_pix_nivel_tables()
    _ensure_pix_faixa_tables()


def _ensure_page_views_table():
    """Tracking de navegação no dashboard."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS page_views (
                    id        BIGSERIAL PRIMARY KEY,
                    user_id   INTEGER REFERENCES app_users(id) ON DELETE SET NULL,
                    username  TEXT,
                    role      TEXT,
                    page      TEXT NOT NULL,
                    ts        TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_page_views_ts   ON page_views (ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_page_views_page ON page_views (page)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_page_views_user ON page_views (user_id)")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure page_views table: %s", e)


def _ensure_avisos_tables():
    """Create avisos + aviso_lido tables."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS avisos (
                    id              SERIAL PRIMARY KEY,
                    titulo          TEXT NOT NULL,
                    corpo           TEXT NOT NULL,
                    prioridade      TEXT NOT NULL DEFAULT 'normal',
                    target_role     TEXT NOT NULL DEFAULT 'todos',
                    target_user_ids INTEGER[] DEFAULT '{}',
                    created_by      INTEGER REFERENCES app_users(id),
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    expires_at      TIMESTAMPTZ,
                    active          BOOLEAN DEFAULT TRUE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS aviso_lido (
                    aviso_id  INTEGER NOT NULL REFERENCES avisos(id) ON DELETE CASCADE,
                    user_id   INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
                    read_at   TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (aviso_id, user_id)
                )
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure avisos tables: %s", e)


def _ensure_dist_comercial_schedule_tables():
    """Cria as tabelas do agendamento de troca de turno (Dia/Noite) do dist_comercial.

    Ver AGENTS.md 2026-07-07 — regra automatica de troca de turno na
    Distribuicao Comercial.

    - dist_comercial_schedule: regras {hora, turno_alvo, enabled, last_run_date}.
    - dist_comercial_turno_map: mapa global {id_lead -> 'dia' | 'noite'}. Promove
      o localStorage anterior pra fonte unica (todos os gestores veem o mesmo).
    - dist_comercial_snapshot: um payload por turno, capturado apos SALVAR
      quando o gestor aplica manualmente Modo DIA/NOITE. Ex:
      {'noite': {'12345': 'ATIVO', '67890': 'INATIVO'}, ...}
    - dist_comercial_apply_log: historico de disparos (manual ou automatico)
      pra auditoria — nunca truncado, pequeno o suficiente pra caber tudo.
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dist_comercial_schedule (
                    id                    SERIAL PRIMARY KEY,
                    hora_inicio           TIME NOT NULL,
                    hora_fim              TIME NOT NULL,
                    turno_alvo            TEXT NOT NULL,
                    enabled               BOOLEAN NOT NULL DEFAULT TRUE,
                    last_run_inicio_date  DATE,
                    last_run_fim_date     DATE,
                    last_run_at           TIMESTAMPTZ,
                    last_run_result       TEXT,
                    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT ck_dcs_turno CHECK (turno_alvo IN ('dia', 'noite'))
                )
            """)
            # Migration idempotente: se veio da versao anterior com coluna `hora`, converte.
            # AGENTS.md 2026-07-07 (segunda entrada) — modelo de janela.
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name = 'dist_comercial_schedule' AND column_name = 'hora') THEN
                        ALTER TABLE dist_comercial_schedule ADD COLUMN IF NOT EXISTS hora_inicio TIME;
                        ALTER TABLE dist_comercial_schedule ADD COLUMN IF NOT EXISTS hora_fim TIME;
                        -- Fallback hardcoded (00:00 / 05:00) garante non-NULL mesmo em
                        -- schemas em que `hora` tenha sido NULL por alguma migracao previa
                        -- que dropou o constraint. Sem WHERE = cobre todas as rows.
                        UPDATE dist_comercial_schedule
                           SET hora_inicio = COALESCE(hora_inicio, hora, '00:00:00'::time),
                               hora_fim = COALESCE(hora_fim, (hora + INTERVAL '5 hours')::time, '05:00:00'::time);
                        ALTER TABLE dist_comercial_schedule ALTER COLUMN hora_inicio SET NOT NULL;
                        ALTER TABLE dist_comercial_schedule ALTER COLUMN hora_fim SET NOT NULL;
                        ALTER TABLE dist_comercial_schedule DROP COLUMN hora;
                    END IF;
                    IF EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name = 'dist_comercial_schedule' AND column_name = 'last_run_date') THEN
                        ALTER TABLE dist_comercial_schedule ADD COLUMN IF NOT EXISTS last_run_inicio_date DATE;
                        ALTER TABLE dist_comercial_schedule ADD COLUMN IF NOT EXISTS last_run_fim_date DATE;
                        UPDATE dist_comercial_schedule
                           SET last_run_inicio_date = COALESCE(last_run_inicio_date, last_run_date)
                         WHERE last_run_date IS NOT NULL;
                        ALTER TABLE dist_comercial_schedule DROP COLUMN last_run_date;
                    END IF;
                END $$;
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_dcs_enabled ON dist_comercial_schedule(enabled)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS dist_comercial_turno_map (
                    id_lead     BIGINT PRIMARY KEY,
                    turno       TEXT NOT NULL,
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_by  TEXT,
                    CONSTRAINT ck_dctm_turno CHECK (turno IN ('dia', 'noite'))
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS dist_comercial_snapshot (
                    turno       TEXT PRIMARY KEY,
                    payload     JSONB NOT NULL,
                    taken_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    taken_by    TEXT,
                    CONSTRAINT ck_dcs_snap_turno CHECK (turno IN ('dia', 'noite'))
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS dist_comercial_apply_log (
                    id          SERIAL PRIMARY KEY,
                    turno_alvo  TEXT NOT NULL,
                    origem      TEXT NOT NULL,
                    schedule_id INTEGER,
                    autor       TEXT,
                    payload     JSONB,
                    resultado   TEXT NOT NULL,
                    mensagem    TEXT,
                    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_dcal_executed ON dist_comercial_apply_log(executed_at DESC)")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure dist_comercial schedule tables: %s", e)


def _ensure_premiacao_interna_tables():
    """Create Premiacoes Internas workflow tables (lote, colaborador, evento).

    Kept in a dedicated ensure to avoid mixing with `premiacao_*` (campanhas
    comerciais) — see AGENTS.md 2026-07-02 for the naming rationale.
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_interna_lote (
                    id                       SERIAL PRIMARY KEY,
                    mes_referencia           TEXT NOT NULL,
                    setor                    TEXT NOT NULL,
                    gestor_user_id           INTEGER NOT NULL REFERENCES app_users(id),
                    gestor_nome              TEXT NOT NULL,
                    observacoes_gerais       TEXT,
                    status                   TEXT NOT NULL DEFAULT 'rascunho',
                    valor_total              NUMERIC(14,2) NOT NULL DEFAULT 0,
                    enviado_em               TIMESTAMPTZ,
                    decidido_em              TIMESTAMPTZ,
                    aprovador_user_id        INTEGER REFERENCES app_users(id),
                    aprovador_nome           TEXT,
                    aprovador_justificativa  TEXT,
                    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pil_status ON premiacao_interna_lote(status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pil_gestor ON premiacao_interna_lote(gestor_user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pil_mes    ON premiacao_interna_lote(mes_referencia)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_interna_colaborador (
                    id                  SERIAL PRIMARY KEY,
                    lote_id             INTEGER NOT NULL REFERENCES premiacao_interna_lote(id) ON DELETE CASCADE,
                    nome                TEXT NOT NULL,
                    cargo               TEXT NOT NULL,
                    setor               TEXT NOT NULL,
                    valor               NUMERIC(14,2) NOT NULL,
                    justificativa       TEXT NOT NULL,
                    observacoes         TEXT,
                    is_auto_premiacao   BOOLEAN NOT NULL DEFAULT FALSE,
                    ordem               INTEGER NOT NULL DEFAULT 0,
                    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pic_lote ON premiacao_interna_colaborador(lote_id)")

            cur.execute("""
                ALTER TABLE premiacao_interna_colaborador
                ADD COLUMN IF NOT EXISTS app_user_id INTEGER
                    REFERENCES app_users(id) ON DELETE SET NULL
            """)
            cur.execute("""
                ALTER TABLE premiacao_interna_colaborador
                ADD COLUMN IF NOT EXISTS email TEXT
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pic_user ON premiacao_interna_colaborador(app_user_id)")

            cur.execute("""
                ALTER TABLE premiacao_interna_lote
                ALTER COLUMN gestor_user_id DROP NOT NULL
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS premiacao_interna_evento (
                    id               SERIAL PRIMARY KEY,
                    lote_id          INTEGER NOT NULL REFERENCES premiacao_interna_lote(id) ON DELETE CASCADE,
                    tipo             TEXT NOT NULL,
                    status_anterior  TEXT,
                    status_novo      TEXT,
                    autor_user_id    INTEGER NOT NULL REFERENCES app_users(id),
                    autor_nome       TEXT NOT NULL,
                    justificativa    TEXT,
                    payload_diff     JSONB,
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pie_lote    ON premiacao_interna_evento(lote_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_pie_created ON premiacao_interna_evento(created_at)")

            cur.execute("""
                ALTER TABLE premiacao_interna_evento
                ALTER COLUMN autor_user_id DROP NOT NULL
            """)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure premiacao_interna tables: %s", e)


def _ensure_materias_alunos_tables():
    """Cria as tabelas materias_alunos e materias_alunos_consultas."""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS materias_alunos (
                    id            BIGSERIAL PRIMARY KEY,
                    rgm           TEXT NOT NULL,
                    aluno         TEXT,
                    sigla         TEXT NOT NULL,
                    disciplina    TEXT,
                    resultado     TEXT,
                    consultado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (rgm, sigla)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_materias_alunos_rgm ON materias_alunos (rgm)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS materias_alunos_consultas (
                    rgm           TEXT PRIMARY KEY,
                    aluno         TEXT,
                    status        TEXT NOT NULL,
                    mensagem      TEXT,
                    qtd_materias  INTEGER NOT NULL DEFAULT 0,
                    consultado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_mac_status ON materias_alunos_consultas (status)")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Could not ensure materias_alunos tables: %s", e)
