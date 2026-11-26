"""Fase 1, etapa 2 (26/11): permissão por página nas rotas que ainda não checavam.

O mapa `permissoes.PAGINAS_POR_ENDPOINT` é aplicado no `app.py` (`aplicar_mapa`).
Aqui: o mecanismo (403 sem a página, 200 com ela), o conteúdo aprovado, que cada
endpoint existe no código, e uma guarda para rota `/api/` nova sem permissão.
Sem Postgres: o código das rotas é lido, não importado.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from flask import Flask, jsonify

import permissoes
from helpers import ALL_PAGES

RAIZ = Path(__file__).resolve().parent.parent
MAPA = permissoes.PAGINAS_POR_ENDPOINT
VERBOS = ("route", "get", "post", "put", "delete", "patch")


# ── leitura estática das rotas ───────────────────────────────────────────

def _rotas_do_codigo():
    """[(arquivo, endpoint, caminho, corpo+decoradores, nomes de auxiliares com checagem)]"""
    out = []
    for f in sorted((RAIZ / "routes").glob("*.py")):
        src = f.read_text(encoding="utf-8")
        m = re.search(r"^\w+\s*=\s*Blueprint\(\s*['\"]([^'\"]+)['\"]", src, re.M)
        bp = m.group(1) if m else None
        arv = ast.parse(src)
        linhas = src.splitlines()
        funcs = [n for n in ast.walk(arv) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]

        def _eh_rota(n):
            return any(isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr in VERBOS
                       for d in n.decorator_list)

        def _texto(n):
            ini = min([d.lineno for d in n.decorator_list] + [n.lineno])
            return "\n".join(linhas[ini - 1: n.end_lineno])

        aux = set()
        for _ in range(3):
            for n in funcs:
                if not _eh_rota(n) and (_RX_CHECAGEM.search(_texto(n)) or any(
                        re.search(rf"\b{re.escape(a)}\(", _texto(n)) for a in aux if a != n.name)):
                    aux.add(n.name)
        for n in funcs:
            if not _eh_rota(n):
                continue
            for d in n.decorator_list:
                if isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr in VERBOS:
                    caminho = d.args[0].value if d.args and isinstance(d.args[0], ast.Constant) else "?"
                    ep = f"{bp}.{n.name}" if bp else n.name
                    out.append((f.name, ep, caminho, _texto(n), aux))
    return out


_RX_CHECAGEM = re.compile(
    r"exige_pagina|so_admin|tem_alguma_pagina|user_permissions|abort\(\s*403|,\s*403\b"
    r"|session\.get\(\s*['\"]role['\"]\s*\)\s*(!=|==|in|not in)|session\[\s*['\"]role['\"]\s*\]\s*(!=|==)"
)
ROTAS = _rotas_do_codigo()
ENDPOINTS = {ep for _, ep, _, _, _ in ROTAS}


def _tem_checagem_propria(texto: str, aux: set) -> bool:
    return bool(_RX_CHECAGEM.search(texto)) or any(re.search(rf"\b{re.escape(a)}\(", texto) for a in aux)


# ── conteúdo do mapa ─────────────────────────────────────────────────────

def test_todo_endpoint_do_mapa_existe_no_codigo():
    assert set(MAPA) - ENDPOINTS == set()


def test_todas_as_paginas_do_mapa_existem():
    usadas = {p for pags in MAPA.values() for p in pags}
    assert usadas - set(ALL_PAGES) == set()


def test_quantidade_aprovada_em_26_11():
    assert len(MAPA) == 170
    assert sum(1 for p in MAPA.values() if p == ()) == 14


@pytest.mark.parametrize("endpoint,paginas", [
    ("inadimplencia.api_inadimplencia_list", ("inadimplencia",)),
    ("comercial_rgm.crgm_cache_clear", ()),
    ("engagement.api_comm_dispatch_manual", ()),
    ("dashboard.api_meta_webhook", ()),
    ("kommo_bp.api_diag_kommo", ()),
    ("crm.api_debug_address", ()),
    ("kommo_bp.api_kommo_funnel_live", ("dashboard", "kommo_sync")),
    ("comercial_rgm.crgm_get_metas", ("comercial_rgm", "bwipo_comercial", "minha_performance")),
    ("config_bp.api_schedules_list", ("schedule", "config")),
    ("logs_bp.api_logs_list", ("logs", "config")),
    ("engagement.api_engagement_recalculate", ("intelligence",)),
    ("engagement.api_comm_evaluate", ("intelligence",)),
])
def test_amostra_da_lista_aprovada(endpoint, paginas):
    if endpoint not in MAPA:
        pytest.fail(f"{endpoint} fora do mapa; endpoints parecidos: "
                    f"{sorted(e for e in MAPA if e.split('.')[0] == endpoint.split('.')[0])[:8]}")
    assert MAPA[endpoint] == paginas


@pytest.mark.parametrize("caminho", sorted(permissoes.ROTAS_SEM_PAGINA))
def test_rotas_livres_nao_estao_no_mapa(caminho):
    eps = {ep for _, ep, c, _, _ in ROTAS if c == caminho}
    assert not (eps & set(MAPA))


def test_rotas_do_proprio_usuario_nao_estao_no_mapa():
    for c in permissoes.ROTAS_DO_PROPRIO_USUARIO:
        eps = {ep for _, ep, cam, _, _ in ROTAS if cam == c}
        assert eps, c
        assert not (eps & set(MAPA))


def test_avisos_e_rastreio_continuam_livres():
    for c in ("/api/avisos", "/api/avisos/nao-lidos", "/api/track-page-view", "/health", "/login", "/logout"):
        assert c in permissoes.ROTAS_SEM_PAGINA


# ── guarda: rota /api/ nova precisa de permissão ─────────────────────────

def test_toda_rota_api_tem_permissao_ou_esta_na_lista_de_livres():
    soltas = []
    for arquivo, ep, caminho, texto, aux in ROTAS:
        if arquivo == "atualizar_preco_app.py":  # protegidas no app.py (_so_precos)
            continue
        if not caminho.startswith("/api/") or caminho in permissoes.ROTAS_SEM_PAGINA:
            continue
        if caminho in permissoes.ROTAS_DO_PROPRIO_USUARIO:
            continue
        if ep in MAPA or _tem_checagem_propria(texto, aux):
            continue
        soltas.append(f"{arquivo}: {caminho} ({ep})")
    assert soltas == [], (
        "Rota /api/ sem permissão no servidor. Use @exige_pagina(...) ou inclua em "
        "permissoes.PAGINAS_POR_ENDPOINT:\n" + "\n".join(soltas))


# ── aplicação no app.py ──────────────────────────────────────────────────

def test_app_aplica_o_mapa_depois_de_registrar_tudo():
    src = (RAIZ / "app.py").read_text(encoding="utf-8")
    m = re.search(r"aplicar_mapa\w*\(app\)", src)
    assert m
    pos = m.start()
    ultimo_registro = max(src.rfind("register_blueprint("), src.rfind("app.add_url_rule("))
    assert pos > ultimo_registro


# ── mecanismo: 403 sem a página, 200 com ela ─────────────────────────────

class _Cur:
    def __init__(self, paginas):
        self.paginas = paginas
        self._r = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        _uid, pedidas = params
        self._r = (1,) if set(pedidas) & self.paginas else None

    def fetchone(self):
        return self._r


class _Conn:
    def __init__(self, paginas):
        self.paginas = paginas

    def cursor(self):
        return _Cur(self.paginas)

    def close(self):
        pass


@pytest.fixture
def app_falso():
    a = Flask("permissao_teste")
    a.secret_key = "teste"
    for i, ep in enumerate(sorted(MAPA)):
        a.add_url_rule(f"/r/{i}", endpoint=ep, view_func=lambda: jsonify({"ok": True}))
    faltando = permissoes.aplicar_mapa(a)
    assert faltando == []
    a.rota_de = {ep: f"/r/{i}" for i, ep in enumerate(sorted(MAPA))}
    return a


def _cliente(app, monkeypatch, role="viewer", uid=50, paginas=()):
    monkeypatch.setattr(permissoes, "get_conn", lambda: _Conn(set(paginas)))
    c = app.test_client()
    with c.session_transaction() as s:
        s.update(authenticated=True, user_id=uid, role=role)
    return c


@pytest.mark.parametrize("endpoint", sorted(MAPA))
def test_sem_a_pagina_recebe_403(app_falso, monkeypatch, endpoint):
    outras = set(ALL_PAGES) - set(MAPA[endpoint])
    c = _cliente(app_falso, monkeypatch, paginas=outras)
    r = c.get(app_falso.rota_de[endpoint])
    assert r.status_code == 403
    assert r.get_json()["error"] == permissoes.SEM_PERMISSAO


@pytest.mark.parametrize("endpoint", sorted(ep for ep, p in MAPA.items() if p))
def test_com_qualquer_uma_das_paginas_passa(app_falso, monkeypatch, endpoint):
    for pagina in MAPA[endpoint]:
        c = _cliente(app_falso, monkeypatch, paginas={pagina})
        assert c.get(app_falso.rota_de[endpoint]).status_code == 200, pagina


@pytest.mark.parametrize("endpoint", sorted(MAPA))
def test_admin_passa(app_falso, monkeypatch, endpoint):
    c = _cliente(app_falso, monkeypatch, role="admin")
    assert c.get(app_falso.rota_de[endpoint]).status_code == 200


def test_login_de_emergencia_passa(app_falso, monkeypatch):
    c = _cliente(app_falso, monkeypatch, role="admin", uid=0)
    ep = "inadimplencia.api_inadimplencia_list"
    assert c.get(app_falso.rota_de[ep]).status_code == 200


def test_aplicar_mapa_avisa_endpoint_que_nao_existe(monkeypatch):
    a = Flask("vazio")
    monkeypatch.setattr(permissoes, "PAGINAS_POR_ENDPOINT", {"nao.existe": ("config",)})
    assert permissoes.aplicar_mapa(a) == ["nao.existe"]


def test_aplicar_mapa_duas_vezes_nao_embrulha_de_novo(app_falso):
    assert permissoes.aplicar_mapa(app_falso) == []
    ep = "inadimplencia.api_inadimplencia_list"
    assert getattr(app_falso.view_functions[ep], "paginas_exigidas", None) == ("inadimplencia",)
    assert not hasattr(app_falso.view_functions[ep].__wrapped__, "paginas_exigidas")
