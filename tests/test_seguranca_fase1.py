"""Fase 1: SECRET_KEY, cookies, hash de senha e permissão por página no servidor."""
import hashlib
import re
from pathlib import Path

import pytest
from flask import Flask

import permissoes
import seguranca
from seguranca import (
    CHAVE_ANTIGA_PUBLICA,
    chave_da_sessao,
    conferir_senha,
    configurar_cookies,
    eh_hash_antigo,
    gerar_hash,
)

RAIZ = Path(__file__).resolve().parent.parent


# ── SECRET_KEY ───────────────────────────────────────────────────────────

def test_usa_a_secret_key_do_ambiente(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "minha-chave-de-producao")
    assert chave_da_sessao() == "minha-chave-de-producao"


@pytest.mark.parametrize("valor", [None, "", "   ", CHAVE_ANTIGA_PUBLICA])
def test_sem_chave_ou_com_a_publica_gera_aleatoria(monkeypatch, valor):
    if valor is None:
        monkeypatch.delenv("SECRET_KEY", raising=False)
    else:
        monkeypatch.setenv("SECRET_KEY", valor)
    k1, k2 = chave_da_sessao(), chave_da_sessao()
    assert k1 != CHAVE_ANTIGA_PUBLICA
    assert re.fullmatch(r"[0-9a-f]{64}", k1)
    assert k1 != k2


def test_app_nao_tem_mais_a_chave_publica_como_padrao():
    src = (RAIZ / "app.py").read_text(encoding="utf-8")
    assert CHAVE_ANTIGA_PUBLICA not in src
    assert "app.secret_key = chave_da_sessao()" in src


def test_cookie_forjado_com_a_chave_publica_nao_vale(monkeypatch):
    """Quem assina um cookie de admin com a chave antiga não entra mais."""
    from flask.sessions import SecureCookieSessionInterface

    monkeypatch.delenv("SECRET_KEY", raising=False)
    forjador = Flask("forjador")
    forjador.secret_key = CHAVE_ANTIGA_PUBLICA
    cookie = SecureCookieSessionInterface().get_signing_serializer(forjador).dumps(
        {"authenticated": True, "role": "admin", "user_id": 1}
    )

    painel = Flask("painel")
    painel.secret_key = chave_da_sessao()

    @painel.route("/quem")
    def quem():
        from flask import session
        return session.get("role") or "anonimo"

    c = painel.test_client()
    c.set_cookie("session", cookie)
    assert c.get("/quem").data == b"anonimo"


# ── cookies ──────────────────────────────────────────────────────────────

def test_cookies_padrao_localhost(monkeypatch):
    monkeypatch.delenv("SESSION_COOKIE_SECURE", raising=False)
    app = Flask("t")
    configurar_cookies(app)
    assert app.config["SESSION_COOKIE_HTTPONLY"] is True
    assert app.config["SESSION_COOKIE_SAMESITE"] == "Lax"
    assert app.config["SESSION_COOKIE_SECURE"] is False


def test_cookie_secure_por_variavel(monkeypatch):
    monkeypatch.setenv("SESSION_COOKIE_SECURE", "1")
    app = Flask("t")
    configurar_cookies(app)
    assert app.config["SESSION_COOKIE_SECURE"] is True


def test_dockerfile_liga_cookie_secure():
    assert "SESSION_COOKIE_SECURE=1" in (RAIZ / "Dockerfile").read_text(encoding="utf-8")


# ── senhas ───────────────────────────────────────────────────────────────

def _sha256(s):
    return hashlib.sha256(s.encode()).hexdigest()


def test_hash_novo_tem_sal_e_nao_e_sha256():
    h1, h2 = gerar_hash("segredo"), gerar_hash("segredo")
    assert h1 != h2  # sal diferente a cada vez
    assert not eh_hash_antigo(h1)
    assert h1.startswith("scrypt:")


def test_confere_hash_novo():
    h = gerar_hash("segredo")
    assert conferir_senha("segredo", h) == (True, False)
    assert conferir_senha("errada", h) == (False, False)


def test_hash_antigo_confere_e_pede_regravacao():
    h = _sha256("segredo")
    assert conferir_senha("segredo", h) == (True, True)
    assert conferir_senha("errada", h) == (False, False)


@pytest.mark.parametrize("h", [None, "", "lixo", "scrypt:quebrado"])
def test_hash_vazio_ou_invalido_nao_entra(h):
    assert conferir_senha("qualquer", h) == (False, False)


def test_helpers_e_auth_gravam_hash_novo():
    import helpers
    from routes import auth
    for f in (helpers._hash_pw, auth._hash_pw):
        h = f("x")
        assert not eh_hash_antigo(h) and conferir_senha("x", h)[0]


class _CurLogin:
    def __init__(self, row, executados):
        self.row, self.executados = row, executados
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None): self.executados.append((sql, params))
    def fetchone(self): return self.row


class _ConnLogin:
    def __init__(self, row):
        self.row, self.executados, self.commits = row, [], 0
    def cursor(self, *a, **k): return _CurLogin(self.row, self.executados)
    def commit(self): self.commits += 1
    def rollback(self): pass
    def close(self): pass


def test_login_com_hash_antigo_entra_e_regrava(monkeypatch):
    from routes import auth
    conn = _ConnLogin({"id": 7, "username": "ana", "pw_hash": _sha256("segredo"), "role": "viewer"})
    monkeypatch.setattr(auth, "get_conn", lambda: conn)
    u = auth._db_auth("ana", "segredo")
    assert u and u["id"] == 7
    updates = [p for s, p in conn.executados if s.startswith("UPDATE app_users SET pw_hash")]
    assert len(updates) == 1 and conn.commits == 1
    novo, uid = updates[0]
    assert uid == 7 and conferir_senha("segredo", novo) == (True, False)


def test_login_com_senha_errada_nao_entra_nem_regrava(monkeypatch):
    from routes import auth
    conn = _ConnLogin({"id": 7, "username": "ana", "pw_hash": _sha256("segredo"), "role": "viewer"})
    monkeypatch.setattr(auth, "get_conn", lambda: conn)
    assert auth._db_auth("ana", "errada") is None
    assert not [s for s, _ in conn.executados if s.startswith("UPDATE")]


def test_login_com_hash_novo_nao_regrava(monkeypatch):
    from routes import auth
    conn = _ConnLogin({"id": 7, "username": "ana", "pw_hash": gerar_hash("segredo"), "role": "viewer"})
    monkeypatch.setattr(auth, "get_conn", lambda: conn)
    assert auth._db_auth("ana", "segredo")["id"] == 7
    assert not [s for s, _ in conn.executados if s.startswith("UPDATE")]


def test_usuario_inexistente(monkeypatch):
    from routes import auth
    monkeypatch.setattr(auth, "get_conn", lambda: _ConnLogin(None))
    assert auth._db_auth("ninguem", "x") is None


# ── permissão por página ─────────────────────────────────────────────────

class _CurPerm:
    def __init__(self, paginas_do_usuario):
        self.p, self.res = paginas_do_usuario, None
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params):
        _uid, pedidas = params
        self.res = (1,) if set(pedidas) & self.p else None
    def fetchone(self): return self.res


class _ConnPerm:
    def __init__(self, paginas):
        self.paginas = set(paginas)
    def cursor(self, *a, **k): return _CurPerm(self.paginas)
    def close(self): pass


@pytest.fixture
def app_perm():
    app = Flask("perm")
    app.secret_key = "teste"

    @app.route("/acao", methods=["POST"])
    @permissoes.exige_pagina("config", "schedule")
    def acao():
        return "feito"

    @app.route("/so-admin")
    @permissoes.so_admin
    def admin():
        return "feito"

    return app


def _cliente(app, monkeypatch, sessao, paginas=()):
    monkeypatch.setattr(permissoes, "get_conn", lambda: _ConnPerm(paginas))
    c = app.test_client()
    with c.session_transaction() as s:
        s.update(sessao)
    return c


USUARIO = {"authenticated": True, "user_id": 5, "role": "viewer"}


def test_usuario_sem_a_pagina_recebe_403(app_perm, monkeypatch):
    r = _cliente(app_perm, monkeypatch, USUARIO, paginas={"minha_performance"}).post("/acao")
    assert r.status_code == 403 and r.get_json()["ok"] is False


@pytest.mark.parametrize("pagina", ["config", "schedule"])
def test_usuario_com_qualquer_das_paginas_passa(app_perm, monkeypatch, pagina):
    r = _cliente(app_perm, monkeypatch, USUARIO, paginas={pagina}).post("/acao")
    assert r.status_code == 200 and r.data == b"feito"


@pytest.mark.parametrize("sessao", [
    {"authenticated": True, "user_id": 1, "role": "admin"},
    {"authenticated": True, "user_id": 0, "role": "admin"},  # login de emergência
])
def test_admin_passa_sempre(app_perm, monkeypatch, sessao):
    c = _cliente(app_perm, monkeypatch, sessao, paginas=())
    assert c.post("/acao").status_code == 200
    assert c.get("/so-admin").status_code == 200


def test_sem_sessao_recebe_403(app_perm, monkeypatch):
    assert _cliente(app_perm, monkeypatch, {}, paginas={"config"}).post("/acao").status_code == 403


def test_so_admin_barra_usuario_com_paginas(app_perm, monkeypatch):
    c = _cliente(app_perm, monkeypatch, USUARIO, paginas={"config", "schedule", "update"})
    assert c.get("/so-admin").status_code == 403


# ── quais rotas ficaram protegidas ───────────────────────────────────────

ESPERADO = {
    ("POST", "/api/turmas"): ("config",),
    ("PUT", "/api/turmas/<int:tid>"): ("config",),
    ("DELETE", "/api/turmas/<int:tid>"): ("config",),
    ("POST", "/api/turmas/seed"): ("config",),
    ("POST", "/api/ciclos"): ("config",),
    ("PUT", "/api/ciclos/<int:cid>"): ("config",),
    ("DELETE", "/api/ciclos/<int:cid>"): ("config",),
    ("POST", "/api/ciclos/seed"): ("config",),
    ("POST", "/api/distribuicao"): ("distribuicao",),
    ("POST", "/api/schedules"): ("schedule", "config"),
    ("DELETE", "/api/schedules/<schedule_id>"): ("schedule", "config"),
    ("POST", "/api/schedules/<schedule_id>/toggle"): ("schedule", "config"),
    ("GET", "/api/debug"): (),
    ("POST", "/api/macro-email/consultores"): ("macro_email",),
    ("POST", "/api/upload"): ("update",),
    ("POST", "/api/upload-batch"): ("update",),
    ("POST", "/api/upload-folder"): ("update",),
}

# Leituras que as telas usam e que NÃO podem ganhar trava nesta fase.
LIVRES = [("GET", "/api/turmas"), ("GET", "/api/ciclos"), ("GET", "/api/distribuicao"),
          ("GET", "/api/schedules"), ("GET", "/api/upload/info")]


@pytest.fixture(scope="module")
def rotas():
    from routes.config import config_bp
    from routes.upload import upload_bp
    app = Flask("rotas")
    app.register_blueprint(config_bp)
    app.register_blueprint(upload_bp)
    mapa = {}
    for rule in app.url_map.iter_rules():
        for m in rule.methods - {"HEAD", "OPTIONS"}:
            mapa[(m, rule.rule)] = getattr(app.view_functions[rule.endpoint], "paginas_exigidas", None)
    return mapa


@pytest.mark.parametrize("chave,paginas", ESPERADO.items(), ids=[f"{m} {r}" for m, r in ESPERADO])
def test_rota_de_escrita_exige_a_pagina_certa(rotas, chave, paginas):
    assert rotas[chave] == paginas


@pytest.mark.parametrize("chave", LIVRES, ids=[f"{m} {r}" for m, r in LIVRES])
def test_leituras_continuam_livres(rotas, chave):
    assert rotas[chave] is None


def test_rotas_de_preco_sao_embrulhadas_no_app():
    src = (RAIZ / "app.py").read_text(encoding="utf-8")
    assert '_exige_pagina("atualizar_preco", "precos_bwipo")' in src
    assert "view_func=_so_precos(_preco_app.view_functions[_ep])" in src


def test_post_de_schedule_barrado_de_ponta_a_ponta(monkeypatch):
    """Consultor sem a página não consegue criar agendamento pela API."""
    from routes.config import config_bp
    app = Flask("e2e")
    app.secret_key = "teste"
    app.register_blueprint(config_bp)
    c = _cliente(app, monkeypatch, USUARIO, paginas={"minha_performance"})
    r = c.post("/api/schedules", json={"type": "sync_full", "hour": 3, "minute": 0})
    assert r.status_code == 403
