"""Fase 4, etapa 1 (27/11): regra do menu em navegacao.py e bloco `nav` no /api/me.

A regra saiu do `app.inject_nav_perms` sem mudar. Para provar isso, os testes
comparam com uma cópia congelada da versão anterior (a função `nav_can` e as
expressões `{% set _show_... %}` do _sidebar.html), numa grade de usuários.
"""
from __future__ import annotations

import itertools
import os
import sys
from pathlib import Path
from unittest.mock import patch

import flask
import jinja2
import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

import navegacao  # noqa: E402
from helpers import (  # noqa: E402
    ALL_PAGES,
    can_access_subir_blog,
    is_suporte_comercial_categoria,
    is_suporte_comercial_login,
    pode_abrir_chamado,
)


# ── Referência congelada: o código como estava no app.py até 27/11 ─────────
_ALWAYS = ("avisos", "profile")
_ADMIN_ONLY = frozenset({"siaa_consulta", "siaa_sessao", "match_inadimplentes", "materias_alunos", "tracker_tarefas", "conexao_siaa"})
_KNOWN = sorted(set(ALL_PAGES) | set(_ALWAYS) | {"dashboard"})


def _referencia(role, pages, categoria, username):
    is_admin = role == "admin"
    cat_lower = (categoria or "").strip().lower()
    is_comercial = (not is_admin) and cat_lower == "comercial"
    is_suporte_comercial = (not is_admin) and (
        is_suporte_comercial_categoria(categoria) or is_suporte_comercial_login(username)
    )
    perf_home = is_comercial or is_suporte_comercial
    pode_chamado = pode_abrir_chamado(role, categoria)

    def nav_can(page):
        if page == "subir_blog":
            return can_access_subir_blog(role, username, pages)
        if is_admin:
            return True
        if page in _ADMIN_ONLY:
            return False
        if page == "solicitacoes_ti":
            return pode_chamado
        if page == "meus_chamados_ti":
            return pode_chamado or ("meus_chamados_ti" in pages)
        if perf_home and page == "dashboard":
            return False
        if page in _ALWAYS:
            return True
        if (not perf_home) and page == "dashboard":
            return True
        return page in pages

    is_academico_simples = (not is_admin) and cat_lower in ("acadêmico", "academico") and ("meus_atendimentos" in pages)
    if perf_home:
        inicial = "minha_performance"
    elif is_academico_simples:
        inicial = "meus_atendimentos"
    else:
        inicial = "dashboard"
    return {
        "nav_can": nav_can,
        "nav_is_admin": is_admin,
        "nav_is_comercial": is_comercial,
        "nav_is_suporte_comercial": is_suporte_comercial,
        "inicial": inicial,
        "permitidas": sorted(p for p in _KNOWN if nav_can(p)),
    }


# Expressões do _sidebar.html até 27/11 (variável -> seção nova).
_SETS_ANTIGOS = {
    "academico": "nav_can('ativacoes') or nav_can('motor_ativacoes') or nav_can('distribuicao') or nav_can('intelligence') or nav_can('inadimplencia') or nav_can('feedback') or nav_can('macro_email') or nav_can('meus_atendimentos') or nav_can('rematricula') or nav_can('disparador_whatsapp') or nav_can('academico_interacoes') or nav_is_admin",
    "ferramentas": "nav_can('comparar_cursos') or nav_can('recomendacao_cursos') or nav_can('localizacao_polos') or nav_can('info_cursos') or nav_can('leads_inscricao') or nav_can('cadastro_leads') or nav_can('subir_blog')",
    "minha_perf_no_comercial": "(not nav_is_comercial) and nav_can('minha_performance')",
    "comercial": "nav_can('dist_consultor') or nav_can('comercial_rgm') or nav_can('bwipo_comercial') or nav_can('dist_comercial') or nav_can('inscricao') or nav_can('inscricao_automatica') or nav_can('recadastros') or nav_can('comercial_dashboard') or nav_can('auditoria_comercial') or nav_can('atualizar_preco') or nav_can('precos_bwipo') or nav_can('leads_parados') or ((not nav_is_comercial) and nav_can('minha_performance')) or nav_can('repasse') or nav_can('captacao') or nav_can('clicks') or nav_can('leads_promotores') or nav_can('premiacao_admin')",
    "vocacional": "nav_can('vocacional')",
    "meta": "nav_can('meta-campaigns')",
    "ti": "nav_is_admin and (nav_can('ia_comercial') or nav_can('page_views') or nav_can('match_inadimplentes') or nav_can('materias_alunos') or nav_can('tracker_tarefas') or nav_can('conexao_siaa'))",
    "chamados": "nav_can('solicitacoes_ti') or nav_can('meus_chamados_ti') or nav_can('chamados_ti')",
    "sistema": "nav_can('pipeline') or nav_can('update') or nav_can('ajustes_matricula') or nav_can('kommo_sync') or nav_can('bwipo_sync') or nav_can('match_merge') or nav_can('config') or nav_can('schedule') or nav_can('kommo_dispatcher') or nav_can('logs')",
    "premiacoes_internas": "nav_can('premiacoes_internas') or nav_can('aprovacao_premiacoes')",
    "sistema_crm": "nav_can('pipeline') or nav_can('update') or nav_can('ajustes_matricula') or nav_can('kommo_sync') or nav_can('bwipo_sync') or nav_can('match_merge')",
    "sistema_geral": "nav_can('config') or nav_can('schedule') or nav_can('kommo_dispatcher') or nav_can('logs')",
}
_JINJA = jinja2.Environment()


# ── Grade de usuários ──────────────────────────────────────────────────────
_ROLES = ["admin", "viewer", "editor", ""]
_CATEGORIAS = ["", "Comercial", "Suporte Comercial", "Acadêmico", "academico", "Supervisor Comercial",
               "Supervisor Acadêmico", "Marketing", "Financeiro"]
_USERNAMES = ["fulano@eduit.com.br", "suporte@eduit.com.br", "mikami@eduit.com.br"]
_CONJUNTOS = [
    set(),
    {"dashboard"},
    {"meus_atendimentos"},
    {"minha_performance", "repasse"},
    {"subir_blog", "meus_chamados_ti"},
    {"siaa_consulta", "page_views", "materias_alunos"},
    {"config", "logs", "pipeline"},
    {"premiacoes_internas", "vocacional", "meta-campaigns", "chamados_ti"},
    set(ALL_PAGES),
]
_GRADE = list(itertools.product(_ROLES, _CATEGORIAS, _USERNAMES, range(len(_CONJUNTOS))))


@pytest.mark.parametrize("role,categoria,username,ic", _GRADE)
def test_menu_igual_ao_de_antes(role, categoria, username, ic):
    pages = set(_CONJUNTOS[ic])
    ref = _referencia(role, pages, categoria, username)
    menu = navegacao.Menu(role, pages, categoria, username)

    for p in _KNOWN + ["pagina_que_nao_existe"]:
        assert menu.pode(p) == ref["nav_can"](p), p
    assert menu.is_admin == ref["nav_is_admin"]
    assert menu.is_comercial == ref["nav_is_comercial"]
    assert menu.is_suporte_comercial == ref["nav_is_suporte_comercial"]
    assert menu.pagina_inicial == ref["inicial"]
    assert menu.paginas_permitidas() == ref["permitidas"]

    secoes = menu.secoes()
    assert set(secoes) == set(_SETS_ANTIGOS)
    for nome, expr in _SETS_ANTIGOS.items():
        antigo = bool(_JINJA.compile_expression(expr)(**ref))
        assert secoes[nome] == antigo, nome


def test_constantes_do_app_continuam_as_mesmas():
    assert navegacao.SEMPRE == _ALWAYS
    assert navegacao.SO_ADMIN == _ADMIN_ONLY
    assert navegacao.CONHECIDAS == _KNOWN


def test_sidebar_usa_as_secoes_do_servidor():
    """Nenhum grupo do menu lateral recalcula a regra no template."""
    html = (RAIZ / "templates" / "partials" / "_sidebar.html").read_text(encoding="utf-8")
    import re

    sets = re.findall(r"\{% set (_show_\w+) = ([^%]*)%\}", html)
    assert len(sets) == 12
    for var, expr in sets:
        assert expr.strip().startswith("nav_secoes."), var
        assert expr.strip().split(".", 1)[1] in _SETS_ANTIGOS, var


def test_todas_as_telas_do_menu_entram_na_lista_da_api():
    """Uma tela no menu que ficasse fora de CONHECIDAS sumiria do front novo."""
    import re

    usadas = set()
    for f in (RAIZ / "templates").rglob("*.html"):
        usadas |= set(re.findall(r"nav_can\('([^']+)'\)", f.read_text(encoding="utf-8")))
    for paginas in navegacao.GRUPOS.values():
        usadas |= set(paginas)
    assert usadas <= set(navegacao.CONHECIDAS), sorted(usadas - set(navegacao.CONHECIDAS))


# ── carregar_usuario (banco simulado) ──────────────────────────────────────
class _Cur:
    def __init__(self, categoria, pages):
        self._categoria, self._pages, self._ult = categoria, pages, None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._ult = sql

    def fetchone(self):
        if "app_users" in self._ult:
            return (self._categoria,)
        if "role, kommo_user_id" in self._ult:
            return None
        return None

    def fetchall(self):
        return [(p,) for p in self._pages]


class _Conn:
    def __init__(self, categoria="", pages=(), role="viewer"):
        self.categoria, self.pages, self.role = categoria, list(pages), role

    def cursor(self, *a, **k):
        return _Cur(self.categoria, self.pages)

    def close(self):
        pass


def _app():
    app = flask.Flask(__name__)
    app.secret_key = "t"
    return app


def test_carregar_usuario_promove_subpaginas():
    app = _app()
    conn = _Conn("Acadêmico", ["disparador_whatsapp_metas", "inscricao_automatica_admin", "chamados_marketing"])
    with app.test_request_context(), patch.object(navegacao, "get_conn", return_value=conn):
        flask.session.update(authenticated=True, user_id=7, role="viewer")
        role, pages, categoria = navegacao.carregar_usuario()
    assert role == "viewer" and categoria == "Acadêmico"
    assert {"disparador_whatsapp", "inscricao_automatica", "chamados_ti"} <= pages


def test_carregar_usuario_sem_login_e_admin():
    app = _app()
    with app.test_request_context():
        assert navegacao.carregar_usuario() == ("", set(), "")
        flask.session.update(authenticated=True, user_id=0)
        role, pages, _ = navegacao.carregar_usuario()
        assert role == "admin" and pages == set(ALL_PAGES)


def test_carregar_usuario_banco_fora_nao_derruba():
    app = _app()
    with app.test_request_context(), patch.object(navegacao, "get_conn", side_effect=RuntimeError("fora")):
        flask.session.update(authenticated=True, user_id=7, role="viewer")
        assert navegacao.carregar_usuario() == ("viewer", set(), "")


# ── /api/me ────────────────────────────────────────────────────────────────
class _CurMe(_Cur):
    def fetchone(self):
        if "role, kommo_user_id" in self._ult:
            return ("viewer", 123, self._categoria, "x@cruzeiro.br")
        return super().fetchone()


class _ConnMe(_Conn):
    def cursor(self, *a, **k):
        return _CurMe(self.categoria, self.pages)


def _cliente_api_me(categoria, pages, username="fulano@eduit.com.br", user_id=7, role="viewer"):
    import routes.auth as auth

    app = _app()
    app.register_blueprint(auth.auth_bp)
    conn = _ConnMe(categoria, pages)
    patches = [
        patch.object(auth, "get_conn", return_value=conn),
        patch.object(auth, "_get_user_permissions", return_value=list(pages)),
        patch.object(navegacao, "get_conn", return_value=conn),
        patch.object(auth.troca_senha, "bloqueio", return_value=None),
    ]
    for p in patches:
        p.start()
    cli = app.test_client()
    with cli.session_transaction() as s:
        s.update(authenticated=True, user_id=user_id, role=role, username=username)
    return cli, patches


def _parar(patches):
    for p in patches:
        p.stop()


def test_api_me_traz_nav_igual_ao_menu():
    pages = ["minha_performance", "repasse", "bwipo_comercial"]
    cli, patches = _cliente_api_me("Comercial", pages)
    try:
        r = cli.get("/api/me")
    finally:
        _parar(patches)
    assert r.status_code == 200
    d = r.get_json()
    esperado = navegacao.Menu("viewer", set(pages), "Comercial", "fulano@eduit.com.br").para_api()
    assert d["nav"] == esperado
    assert d["nav"]["pagina_inicial"] == "minha_performance"
    assert "dashboard" not in d["nav"]["paginas"]
    assert d["nav"]["secoes"]["comercial"] is True
    assert d["nav"]["secoes"]["ti"] is False


def test_api_me_mantem_campos_antigos():
    """7 telas antigas leem /api/me: nenhum campo some nem muda de nome."""
    cli, patches = _cliente_api_me("Acadêmico", ["meus_atendimentos"])
    try:
        d = cli.get("/api/me").get_json()
    finally:
        _parar(patches)
    for campo in ("user_id", "username", "role", "pages", "kommo_user_id", "categoria",
                  "email_cruzeiro", "display_name", "is_suporte_comercial"):
        assert campo in d, campo
    assert d["pages"] == ["meus_atendimentos"]
    assert d["nav"]["pagina_inicial"] == "meus_atendimentos"


def test_api_me_admin_de_emergencia():
    cli, patches = _cliente_api_me("", [], username="admin", user_id=0, role="admin")
    try:
        d = cli.get("/api/me").get_json()
    finally:
        _parar(patches)
    assert d["nav"]["is_admin"] is True
    assert set(d["nav"]["paginas"]) == set(navegacao.CONHECIDAS)
    assert d["nav"]["secoes"]["ti"] is True


def test_api_me_sem_login_401():
    import routes.auth as auth

    app = _app()
    app.register_blueprint(auth.auth_bp)
    with patch.dict(os.environ, {"DEV_SKIP_AUTH": ""}):
        r = app.test_client().get("/api/me")
    assert r.status_code == 401
