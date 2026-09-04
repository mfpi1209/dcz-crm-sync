"""
eduit. — Gestão Acadêmica (Flask).

Uso:
    python app.py
    Acesse http://localhost:5001
"""

import sys, os, io, logging, warnings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)

warnings.filterwarnings("ignore", message=".*collation.*")

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        _orig = getattr(sys, _s)
        if hasattr(_orig, "buffer"):
            try:
                setattr(sys, _s, io.TextIOWrapper(_orig.buffer, encoding="utf-8", errors="replace", line_buffering=True))
            except Exception:
                pass

import time
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask, session, request

load_dotenv(Path(__file__).parent / ".env")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dcz-sync-default-key-change-me")


def _static_cache_bust() -> str:
    """Versão dos assets estáticos (?v=) — maior mtime em static/js."""
    js_dir = Path(__file__).parent / "static" / "js"
    try:
        return str(int(max(f.stat().st_mtime for f in js_dir.glob("*.js"))))
    except (ValueError, OSError):
        return str(int(time.time()))


app.config["CACHE_BUST"] = _static_cache_bust()


def kommo_web_base_url() -> str:
    """URL base do Kommo no navegador (sem /api/v4), ex.: https://subdominio.kommo.com"""
    w = os.getenv("KOMMO_WEB_URL", "").strip().rstrip("/")
    if w:
        return w
    b = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com").strip().rstrip("/")
    if "/api" in b:
        b = b.split("/api", 1)[0].rstrip("/")
    return b or "https://admamoeduitcombr.kommo.com"


@app.context_processor
def inject_kommo_web_base():
    return {"kommo_web_base": kommo_web_base_url()}


def whatsapp_tool_base_url() -> str:
    """URL base do app externo Disparador WhatsApp (sem barra final)."""
    u = os.getenv(
        "WHATSAPP_TOOL_BASE_URL",
        "https://banco-disparador-whatsapp.6tqx2r.easypanel.host",
    ).strip().rstrip("/")
    return u


@app.context_processor
def inject_whatsapp_tool_base():
    return {"whatsapp_tool_base": whatsapp_tool_base_url()}


@app.context_processor
def inject_consultores_academicos_admin():
    """Lista de consultores acadêmicos (app_users) para admin e Supervisor Acadêmico."""
    try:
        role, _, categoria = _nav_load_user_data()
        from helpers import list_consultores_academicos, user_has_disparador_full_access
        if not user_has_disparador_full_access(role, categoria):
            return {"consultores_academicos_admin": []}
        return {"consultores_academicos_admin": list_consultores_academicos()}
    except Exception:
        return {"consultores_academicos_admin": []}


@app.context_processor
def inject_abas_disparador_permitidas():
    """Lista de slugs curtos das abas do Disparador WhatsApp que o usuario
    logado pode ver. None = sem filtro (admin ou compat de quem nao tem
    sub-permissoes setadas). Consumida pelo _disparador_whatsapp.html pra
    anexar ?abas_permitidas= na URL do iframe."""
    try:
        from helpers import compute_abas_disparador_permitidas
        role, pages, _ = _nav_load_user_data()
        return {
            "abas_disparador_permitidas": compute_abas_disparador_permitidas(role, pages),
        }
    except Exception:
        return {"abas_disparador_permitidas": None}


@app.context_processor
def inject_static_version():
    return {"_v": app.config.get("CACHE_BUST", "1")}


# ── Permissões de navegação injetadas no template (sem flash de UI) ───────
from helpers import ALL_PAGES as _NAV_ALL_PAGES
from helpers import can_access_subir_blog as _can_access_subir_blog
from db import get_conn as _nav_get_conn

# Páginas pessoais — sempre visíveis (exceto regras específicas, ex: comercial sem dashboard)
# solicitacoes_ti = formulário de chamados de TI (qualquer pessoa pode abrir ticket).
_NAV_ALWAYS = ("avisos", "profile", "solicitacoes_ti")
# Páginas restritas a admin — nunca visíveis para outros perfis, mesmo com permissão explícita.
_NAV_ADMIN_ONLY = frozenset({"siaa_consulta", "siaa_sessao", "match_inadimplentes", "materias_alunos"})
# Conjunto completo conhecido pelo front (PAGES no utils.js + páginas pessoais)
_NAV_KNOWN_PAGES = sorted(set(_NAV_ALL_PAGES) | set(_NAV_ALWAYS) | {"dashboard"})


def _nav_load_user_data():
    """Retorna (role, set(pages), categoria) do usuário logado, com fallback seguro."""
    if not session.get("authenticated"):
        return "", set(), ""
    role = (session.get("role") or "").strip()
    uid = session.get("user_id", 0)
    if role == "admin" or uid == 0:
        return (role or "admin"), set(_NAV_ALL_PAGES), ""
    pages = set()
    categoria = ""
    try:
        conn = _nav_get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT categoria FROM app_users WHERE id = %s", (uid,))
            r = cur.fetchone()
            if r and r[0]:
                categoria = r[0]
            cur.execute("SELECT page FROM user_permissions WHERE user_id = %s", (uid,))
            pages = {row[0] for row in cur.fetchall()}
        conn.close()
    except Exception:
        pages = set()
    # Promove sub-permissoes "disparador_whatsapp_*" pra master "disparador_whatsapp"
    # — o sidebar/nav_can testa a master pra decidir se mostra o link no menu
    # do Acadêmico. Ter qualquer sub equivale a ter acesso ao modulo.
    if any(p.startswith("disparador_whatsapp_") for p in pages):
        pages.add("disparador_whatsapp")
    return role, pages, categoria


@app.context_processor
def inject_nav_perms():
    role, pages, categoria = _nav_load_user_data()
    from helpers import is_suporte_comercial_categoria, is_suporte_comercial_login

    is_admin = role == "admin"
    username = (session.get("username") or "").strip()
    cat_lower = (categoria or "").strip().lower()
    is_comercial = (not is_admin) and cat_lower == "comercial"
    is_suporte_comercial = (not is_admin) and (
        is_suporte_comercial_categoria(categoria)
        or is_suporte_comercial_login(username)
    )
    perf_home = is_comercial or is_suporte_comercial

    def nav_can(page):
        if page == "subir_blog":
            return _can_access_subir_blog(role, username)
        if is_admin:
            return True
        if page in _NAV_ADMIN_ONLY:
            return False
        if perf_home and page == "dashboard":
            return False
        if page in _NAV_ALWAYS:
            return True
        if (not perf_home) and page == "dashboard":
            return True
        return page in pages

    is_academico_simples = (
        (not is_admin)
        and cat_lower in ("acadêmico", "academico")
        and ("meus_atendimentos" in pages)
    )

    if perf_home:
        nav_initial_page = "minha_performance"
    elif is_academico_simples:
        nav_initial_page = "meus_atendimentos"
    else:
        nav_initial_page = "dashboard"

    allowed_pages = sorted(p for p in _NAV_KNOWN_PAGES if nav_can(p))

    return {
        "nav_role": role,
        "nav_pages": sorted(pages),
        "nav_categoria": categoria,
        "nav_is_admin": is_admin,
        "nav_is_comercial": is_comercial,
        "nav_is_suporte_comercial": is_suporte_comercial,
        "nav_can": nav_can,
        "nav_initial_page": nav_initial_page,
        "nav_allowed_pages": allowed_pages,
    }


from collections import deque

_sync_running = False
_sync_proc = None
_sync_logs = deque(maxlen=500)
_update_running = False
_update_logs = deque(maxlen=500)

def _add_sync_log(msg):
    import datetime
    _sync_logs.append(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

def _add_update_log(msg):
    import datetime
    _update_logs.append(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── Registrar Blueprints ──────────────────────────────────────────────────

from routes.auth import auth_bp
from routes.dashboard import dashboard_bp
from routes.crm import crm_bp
from routes.upload import upload_bp
from routes.engagement import engagement_bp, register_engagement_job
from routes.config import config_bp, init_scheduler, _load_schedules_from_db, register_delta_interval, register_aceite_reconcile, register_responsible_history_job
from routes.logs import logs_bp
from routes.kommo_sync import kommo_bp, register_funnel_cache_job
from routes.match_merge import match_merge_bp
from routes.comercial_rgm import comercial_rgm_bp
from routes.ativacoes import ativacoes_bp
from routes.avisos import avisos_bp
from routes.kommo_merge_route import kommo_merge_bp
from routes.kommo_dispatcher import kommo_dispatcher_bp
from routes.leads_parados import leads_parados_bp
from routes.minha_performance import minha_performance_bp
from routes.repasse import repasse_bp
from routes.supervisor_dashboard import supervisor_dashboard_bp
from routes.meus_atendimentos import meus_atendimentos_bp
from routes.premiacoes_internas import premiacoes_internas_bp
from routes.inadimplencia import inadimplencia_bp
from routes.disparador_whatsapp import disparador_whatsapp_bp
from routes.page_views import page_views_bp
from routes.solicitacoes_ti import solicitacoes_ti_bp
from routes.captacao import captacao_bp
from routes.siaa import siaa_bp
from routes.match_inadimplentes import match_inadimplentes_bp
from routes.materias_alunos import materias_alunos_bp
from routes.academico_interacoes import academico_interacoes_bp
from routes.blog_posts import blog_posts_bp
from routes.inscricao import inscricao_bp
from routes.dist_comercial_schedule import (
    dist_comercial_schedule_bp,
    register_dist_comercial_schedule_job,
)

app.register_blueprint(auth_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(crm_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(engagement_bp)
app.register_blueprint(config_bp)
app.register_blueprint(logs_bp)
app.register_blueprint(kommo_bp)
app.register_blueprint(match_merge_bp)
app.register_blueprint(comercial_rgm_bp)
app.register_blueprint(ativacoes_bp)
app.register_blueprint(avisos_bp)
app.register_blueprint(kommo_merge_bp)
app.register_blueprint(kommo_dispatcher_bp)
app.register_blueprint(leads_parados_bp)
app.register_blueprint(minha_performance_bp)
app.register_blueprint(repasse_bp)
app.register_blueprint(supervisor_dashboard_bp)
app.register_blueprint(meus_atendimentos_bp)
app.register_blueprint(premiacoes_internas_bp)
app.register_blueprint(inadimplencia_bp)
app.register_blueprint(disparador_whatsapp_bp)
app.register_blueprint(page_views_bp)
app.register_blueprint(solicitacoes_ti_bp)
app.register_blueprint(captacao_bp)
app.register_blueprint(siaa_bp)
app.register_blueprint(match_inadimplentes_bp)
app.register_blueprint(materias_alunos_bp)
app.register_blueprint(academico_interacoes_bp)
app.register_blueprint(blog_posts_bp)
app.register_blueprint(inscricao_bp)
app.register_blueprint(dist_comercial_schedule_bp)

# ── Atualizar Preço — rotas do webapp standalone integrado ────────────────
try:
    from routes.atualizar_preco_app import app as _preco_app
    for _rule in list(_preco_app.url_map.iter_rules()):
        _ep = _rule.endpoint
        if _rule.rule.startswith('/api/') and _ep in _preco_app.view_functions:
            _methods = [m for m in _rule.methods if m not in ('HEAD', 'OPTIONS')]
            if _methods:
                try:
                    app.add_url_rule(
                        _rule.rule,
                        endpoint='preco_' + _ep,
                        view_func=_preco_app.view_functions[_ep],
                        methods=_methods,
                    )
                except (AssertionError, ValueError):
                    pass
except Exception as _e:
    import logging as _logging
    _logging.getLogger(__name__).warning(f"Atualizar Preco routes not loaded: {_e}")

# ── Inicialização do banco ────────────────────────────────────────────────

from db import (
    _ensure_schedules_table,
    _ensure_turmas_table,
    _ensure_ciclos_table,
    _ensure_ciclos_comercial_table,
    _ensure_turmas_comercial_table,
    _ensure_ciclo_atual_comercial_table,
    _ensure_users_table,
    _ensure_academico_interacoes_page,
    _ensure_suporte_comercial_users,
    _ensure_xl_snapshots_table,
    _ensure_engagement_tables,
    _ensure_avisos_tables,
    _ensure_page_views_table,
    _ensure_funnel_log_table,
    _ensure_premiacao_tables,
    _ensure_premiacao_interna_tables,
    _ensure_app_users_delete_fks,
    _ensure_materias_alunos_tables,
    _ensure_pix_nivel_tables,
    _ensure_pix_faixa_tables,
    _ensure_suporte_tables,
    _ensure_dist_comercial_schedule_tables,
)

_ensure_schedules_table()
_ensure_turmas_table()
_ensure_ciclos_table()
_ensure_ciclos_comercial_table()
_ensure_turmas_comercial_table()
_ensure_ciclo_atual_comercial_table()
_ensure_users_table()
_ensure_academico_interacoes_page()
_ensure_suporte_comercial_users()
_ensure_xl_snapshots_table()
_ensure_engagement_tables()
_ensure_avisos_tables()
_ensure_page_views_table()
_ensure_funnel_log_table()
_ensure_premiacao_tables()
_ensure_premiacao_interna_tables()
_ensure_app_users_delete_fks()
_ensure_materias_alunos_tables()
_ensure_pix_nivel_tables()
_ensure_pix_faixa_tables()
_ensure_suporte_tables()
_ensure_dist_comercial_schedule_tables()

try:
    from routes.academico_interacoes import _ensure_claim_table
    _ensure_claim_table()
except Exception as _e:
    import logging as _logging
    _logging.getLogger(__name__).warning("academico_atendimento_claim: %s", _e)

# ── APScheduler ───────────────────────────────────────────────────────────

from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
init_scheduler(scheduler)
scheduler.start()
_load_schedules_from_db()
register_engagement_job(scheduler)
register_delta_interval(scheduler)
register_aceite_reconcile(scheduler)
register_responsible_history_job(scheduler)
register_funnel_cache_job(scheduler)
register_dist_comercial_schedule_job(scheduler)
from routes.conversao_backfill import register_conversao_backfill_job
register_conversao_backfill_job(scheduler)

# ── Entrypoint ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001, threaded=True, use_reloader=False)
