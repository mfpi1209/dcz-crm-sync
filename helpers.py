"""
eduit. — Constantes e utilitários compartilhados.
"""

import os
import re
import hashlib
import unicodedata
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ---------------------------------------------------------------------------
# Timezone
# ---------------------------------------------------------------------------

BRT = timezone(timedelta(hours=-3))


def to_brt(dt):
    """Convert a datetime to BRT (UTC-3) string."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(BRT).strftime("%d/%m/%Y %H:%M:%S")
    return str(dt)


def fold_name(s: str) -> str:
    """Lowercase ASCII fold for matching names across dashboard and CRM."""
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def display_name_from_login(username: str = "", email: str = "") -> str:
    """wesley.guerreiro@… / wesley.guerreiro → 'Wesley Guerreiro'."""
    src = (username or email or "").strip()
    if "@" in src:
        src = src.split("@", 1)[0]
    parts = [p for p in re.split(r"[._\-+]+", src) if p]
    if not parts:
        return (username or email or "").strip()
    return " ".join(p[:1].upper() + p[1:] for p in parts)


# ---------------------------------------------------------------------------
# Autenticação — constantes
# ---------------------------------------------------------------------------

ALL_PAGES = [
    "dashboard", "search", "sync", "kommo_sync", "update", "pipeline", "match_merge",
    "comercial_rgm", "logs", "distribuicao", "ativacoes", "intelligence", "inadimplencia",
    "feedback", "config", "schedule", "inscricao", "avisos", "kommo_dispatcher",
    "leads_parados", "dist_consultor", "minha_performance", "premiacao_admin",
    "macro_email", "ajustes_matricula", "repasse",
    "recadastros", "vocacional", "comercial_dashboard", "auditoria_comercial",
    "meta-campaigns", "dist_comercial", "atualizar_preco",
    "comparar_cursos", "recomendacao_cursos", "localizacao_polos", "info_cursos",
    "leads_inscricao", "captacao", "clicks", "leads_promotores", "meus_atendimentos",
    "premiacoes_internas", "aprovacao_premiacoes",
    "cadastro_leads",
    "disparador_whatsapp",
    "ia_comercial",
    "page_views",
    "solicitacoes_ti",
    "siaa_consulta", "siaa_sessao",
    "match_inadimplentes",
    "materias_alunos",
    "academico_interacoes",
    # Sub-permissoes do Disparador WhatsApp (uma por aba do iframe do
    # tool_whatsapp_alunos). Quem tem 'disparador_whatsapp' mas nenhuma
    # sub abaixo => ve TUDO (compat). Quem tem 1+ sub => ve so as marcadas.
    "disparador_whatsapp_disparador",
    "disparador_whatsapp_painel",
    "disparador_whatsapp_metas",
    "disparador_whatsapp_alunos",
    "disparador_whatsapp_calendario",
    "disparador_whatsapp_bases",
    "disparador_whatsapp_relatorios",
    "disparador_whatsapp_conversao",
    "disparador_whatsapp_meu_painel",
    "disparador_whatsapp_regras",
    "rematricula",
]

# Mapping slug curto -> rota no app tool_whatsapp_alunos. Usado pelo
# context_processor de abas permitidas.
DISPARADOR_WHATSAPP_ABA_SLUGS = [
    "painel",
    "metas",
    "disparador",
    "alunos",
    "calendario",
    "bases",
    "relatorios",
    "conversao",
    "meu_painel",
    "regras",
]


def compute_abas_disparador_permitidas(role, user_pages):
    """Calcula a lista de abas que o usuario pode ver no iframe do Disparador.
    - Admin: retorna None (= sem filtro = ve tudo).
    - Sem nenhuma sub-permissao 'disparador_whatsapp_*': None (compat).
    - Com pelo menos 1 sub: retorna lista de slugs curtos das permitidas.
    """
    if (role or "").strip().lower() == "admin":
        return None
    pages_set = set(user_pages or [])
    prefix = "disparador_whatsapp_"
    subs = [p[len(prefix):] for p in pages_set if p.startswith(prefix)]
    subs_validos = [s for s in subs if s in DISPARADOR_WHATSAPP_ABA_SLUGS]
    if not subs_validos:
        return None
    return subs_validos

# Logins do time Suporte Comercial (mesmo painel Equipe Suporte em Minha Performance)
SUPORTE_COMERCIAL_LOGINS = frozenset({
    "felipe.nolasco@cruzeiroead.com.br",
    "jessica.castro@eduit.com.br",
    "suporte@eduit.com.br",
    "thais.martins@cruzeiroead.com.br",
})

# Preset de páginas (sem dashboard acadêmico; alinhado ao config.js)
SUPORTE_COMERCIAL_PAGES = [
    "comparar_cursos", "recomendacao_cursos", "localizacao_polos", "info_cursos",
    "minha_performance", "search", "avisos",
]


def norm_categoria(categoria):
    return (categoria or "").strip().lower()


def is_suporte_comercial_categoria(categoria):
    return norm_categoria(categoria) == "suporte comercial"


def is_suporte_comercial_login(username):
    return (username or "").strip().lower() in SUPORTE_COMERCIAL_LOGINS


def is_supervisor_academico_categoria(categoria):
    n = unicodedata.normalize("NFD", (categoria or "")).encode("ascii", "ignore").decode("ascii")
    return n.strip().lower() == "supervisor academico"


def user_has_disparador_full_access(role, categoria):
    """Admin ou Supervisor Acadêmico — painel/metas/meu painel ver tudo."""
    if (role or "").strip().lower() == "admin":
        return True
    return is_supervisor_academico_categoria(categoria)


# ---------------------------------------------------------------------------
# Consultores acadêmicos — listagem para o iframe do Disparador WhatsApp.
# Usado pelo modal "Atribuir consultor" do Meu Painel (no tool externo) pra
# admin escolher entre TODOS os consultores acadêmicos, e nao so os ja
# gravados em activation_responses.
# ---------------------------------------------------------------------------

def _derive_nome_from_username(username):
    """Strip @dominio e converte separadores em espacos + title-case.
    Espelha a logica de _disparador_whatsapp.html. Usado pra produzir o nome
    que sera comparado com activation_responses.consultor_responsavel_nome.
    """
    local = (username or "").split("@")[0]
    return local.replace(".", " ").replace("_", " ").replace("-", " ").title().strip()


def list_consultores_academicos():
    """Retorna lista de dicts {username, nome} de usuarios com categoria
    contendo 'acad' (Academico, Acadêmico, ACADÊMICO, etc).
    Falha silenciosa em []. Cache em memoria de 5 min via _CONSULTORES_CACHE."""
    cached = _consultores_cache_get()
    if cached is not None:
        return cached
    try:
        from db import get_conn  # import local pra evitar ciclo
    except Exception:
        return []
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT username
                FROM app_users
                WHERE categoria ILIKE %s
                  AND username IS NOT NULL
                  AND TRIM(username) <> ''
                ORDER BY username
                """,
                ("%acad%",),
            )
            rows = cur.fetchall()
        conn.close()
    except Exception:
        return []
    out = []
    seen_nomes = set()
    for r in rows:
        username = (r[0] or "").strip()
        if not username:
            continue
        nome = _derive_nome_from_username(username)
        if not nome or nome in seen_nomes:
            continue
        seen_nomes.add(nome)
        out.append({"username": username, "nome": nome})
    _consultores_cache_set(out)
    return out


_CONSULTORES_CACHE = {"data": None, "ts": 0.0}
_CONSULTORES_CACHE_TTL_S = 300  # 5 min


def _consultores_cache_get():
    import time
    if _CONSULTORES_CACHE["data"] is None:
        return None
    if (time.time() - _CONSULTORES_CACHE["ts"]) > _CONSULTORES_CACHE_TTL_S:
        return None
    return _CONSULTORES_CACHE["data"]


def _consultores_cache_set(data):
    import time
    _CONSULTORES_CACHE["data"] = list(data)
    _CONSULTORES_CACHE["ts"] = time.time()


def invalidate_consultores_cache():
    """Chamada quando admin altera app_users (criar/editar/deletar usuario)."""
    _CONSULTORES_CACHE["data"] = None
    _CONSULTORES_CACHE["ts"] = 0.0


APP_USER_FALLBACK = os.getenv("APP_USER", "admin")
APP_PASS_FALLBACK = os.getenv("APP_PASS", "")


def _hash_pw(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
SYNC_SCRIPT = str(BASE_DIR / "sync.py")
UPDATE_SCRIPT = str(BASE_DIR / "update_crm.py")
SANITIZE_SCRIPT = str(BASE_DIR / "sanitize_crm.py")
PIPELINE_SCRIPT = str(BASE_DIR / "pipeline_crm.py")
ENRICH_SCRIPT = str(BASE_DIR / "enrich_crosslead.py")
MERGE_SCRIPT = str(BASE_DIR / "merge_leads.py")
INADIMPLENTES_SCRIPT = str(BASE_DIR / "update_inadimplentes.py")
CONCLUINTES_SCRIPT = str(BASE_DIR / "update_concluintes.py")
LOG_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

MAX_LOG_LINES = 2000

# ---------------------------------------------------------------------------
# Field IDs (custom fields do CRM)
# ---------------------------------------------------------------------------

FIELD_RGM = "2ac4e30f-cfd7-435f-b688-fbce27f76c38"

TIPO_ALUNO_FIELD = "4230e4db-970b-4444-abaf-c3135a03b79c"
DATA_MATRICULA_FIELD = "bf93a8e9-42c0-4517-8518-6f604746a300"
SITUACAO_FIELD = "fd08d44b-a4a5-4343-b7a9-37f75e2c1caa"
NIVEL_FIELD = "233fcf6f-0bed-49d7-89a1-d1cd54fb9c12"
POLO_FIELD = "0ec9d8dc-d547-4482-b9ad-d4a3e6ec1b54"
TURMA_FIELD = "8815a8de-f755-4597-b6f4-8da6d289b6eb"

# ---------------------------------------------------------------------------
# SQL Queries
# ---------------------------------------------------------------------------

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
# Utilitários
# ---------------------------------------------------------------------------


def _normalize_digits(s):
    """Remove tudo exceto dígitos, tratando floats do Excel (46901353.0 → 46901353)."""
    if not s:
        return ""
    if isinstance(s, float) and s == int(s):
        s = int(s)
    raw = str(s).strip()
    if re.match(r"^\d+\.0+$", raw):
        raw = raw.split(".")[0]
    return re.sub(r"\D", "", raw)


# ---------------------------------------------------------------------------
# Tipos de planilha
# ---------------------------------------------------------------------------

XL_TIPOS = ["matriculados", "inadimplentes", "concluintes", "acesso_ava", "sem_rematricula", "lista_alunos"]


# ---------------------------------------------------------------------------
# Avisos — helpers programaticos
# ---------------------------------------------------------------------------
# Reaproveitam a tabela `avisos` (ver `_ensure_avisos_tables` em db.py). O
# sistema atual so cria avisos via `POST /api/avisos` (admin). Estes helpers
# encapsulam INSERT direto para uso interno por outros modulos.


def criar_aviso_para_usuarios(user_ids, titulo, corpo, *,
                               prioridade="normal",
                               created_by=None,
                               expires_at=None):
    """Cria um aviso direcionado a uma lista explicita de user_ids.

    target_role='todos' + target_user_ids=[...] combina em AND na query de
    visibilidade (_VISIBLE_WHERE em routes/avisos.py), so os IDs listados veem.
    """
    from db import get_conn

    if not user_ids:
        return None
    ids = sorted({int(u) for u in user_ids if u is not None and int(u) > 0})
    if not ids:
        return None

    titulo = (titulo or "").strip()
    corpo = (corpo or "").strip()
    if not titulo or not corpo:
        raise ValueError("titulo e corpo sao obrigatorios")

    prio = prioridade if prioridade in ("normal", "importante", "urgente") else "normal"

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO avisos (
                    titulo, corpo, prioridade, target_role,
                    target_user_ids, created_by, expires_at
                ) VALUES (%s, %s, %s, 'todos', %s, %s, %s)
                RETURNING id
                """,
                (titulo, corpo, prio, ids, created_by, expires_at),
            )
            aviso_id = cur.fetchone()[0]
        conn.commit()
        return aviso_id
    finally:
        conn.close()


def criar_aviso_por_permissao(page, titulo, corpo, *,
                               prioridade="normal",
                               extra_user_ids=None,
                               excluir_user_ids=None,
                               incluir_admins=True,
                               created_by=None,
                               expires_at=None):
    """Cria aviso direcionado a todos usuarios com `user_permissions.page = page`."""
    from db import get_conn

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if incluir_admins:
                cur.execute(
                    """
                    SELECT DISTINCT u.id
                      FROM app_users u
                      LEFT JOIN user_permissions p
                        ON p.user_id = u.id AND p.page = %s
                     WHERE u.role = 'admin' OR p.user_id IS NOT NULL
                    """,
                    (page,),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT p.user_id
                      FROM user_permissions p
                     WHERE p.page = %s
                    """,
                    (page,),
                )
            base_ids = {row[0] for row in cur.fetchall() if row[0]}
    finally:
        conn.close()

    if extra_user_ids:
        base_ids.update(int(u) for u in extra_user_ids if u)
    if excluir_user_ids:
        base_ids.difference_update(int(u) for u in excluir_user_ids if u)

    return criar_aviso_para_usuarios(
        base_ids,
        titulo,
        corpo,
        prioridade=prioridade,
        created_by=created_by,
        expires_at=expires_at,
    )


# ---------------------------------------------------------------------------
# Polos — nomes canônicos (Dashboard Acadêmico + Comercial)
# ---------------------------------------------------------------------------

def _polo_raw_key(polo: str) -> str:
    """Chave de matching: strip prefixos CEB/POLO/código numérico e parênteses."""
    p = unicodedata.normalize("NFKD", (polo or "")).encode("ascii", "ignore").decode().lower()
    p = re.sub(r"^\d+\s*[-—]\s*", "", p.strip())
    p = re.sub(r"^ceb\s+", "", p)
    p = re.sub(r"^polo\s+sp_", "", p)
    p = re.sub(r"^polo\s+", "", p)
    p = re.sub(r"\([^)]*\)", "", p)
    p = re.sub(r"\s+", " ", p).strip()
    return p


def normalize_polo_display(polo: str) -> str:
    """Nome canônico para exibição (Title Case, polos Cruzeiro SP + interior)."""
    if not polo or not str(polo).strip():
        return ""
    k = _polo_raw_key(polo)

    if "taboao" in k or "taboa" in k:
        if "mituzi" in k or "jardim" in k:
            return "Taboão da Serra_Jardim Mituzi"
        return "Taboão da Serra_Centro"
    if "barra funda" in k:
        return "Barra Funda"
    if "sapopemba" in k:
        return "Sapopemba"
    if "vila prudente" in k:
        return "Vila Prudente"
    if "santana" in k:
        return "Santana 2"
    if "ibirapuera" in k:
        return "Ibirapuera"
    if "morumbi" in k:
        return "Morumbi"
    if "campinas" in k:
        return "Campinas"
    if "capivari" in k:
        return "Capivari"
    if "itapira" in k:
        return "Itapira"
    if "freguesia" in k:
        return "Freguesia do Ó"
    if "vila mariana" in k:
        return "Vila Mariana"

    # Fallback: limpa prefixos e aplica title case simples
    cleaned = _polo_raw_key(polo).replace("_", " ")
    if not cleaned:
        return str(polo).strip()
    parts = cleaned.split()
    titled = []
    for i, w in enumerate(parts):
        if w in ("da", "de", "do", "dos", "das", "e") and i > 0:
            titled.append(w)
        else:
            titled.append(w[:1].upper() + w[1:] if w else w)
    return " ".join(titled)
