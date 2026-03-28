"""
eduit. — Conexão e inicialização do banco de dados.
"""

import os
import logging
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from helpers import _hash_pw, APP_USER_FALLBACK, APP_PASS_FALLBACK, ALL_PAGES, XL_TIPOS

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
                CREATE TABLE IF NOT EXISTS premiacao_campanha_link (
                    id              SERIAL PRIMARY KEY,
                    campanha_a_id   INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    campanha_b_id   INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(campanha_a_id, campanha_b_id)
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
