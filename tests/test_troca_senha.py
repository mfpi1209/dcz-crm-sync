"""Troca de senha pelo próprio usuário e troca obrigatória (26/11).

Banco simulado em memória: nenhum Postgres, nenhum segredo.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
from flask import Flask, jsonify

import troca_senha
from seguranca import conferir_senha, gerar_hash

RAIZ = Path(__file__).resolve().parent.parent
auth = pytest.importorskip("routes.auth")


# ── banco simulado ───────────────────────────────────────────────────────

class _Cursor:
    def __init__(self, banco, dict_rows):
        self.banco = banco
        self.dict_rows = dict_rows
        self._linha = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=()):
        s = " ".join(sql.split())
        self.banco.sqls.append(s)
        users = self.banco.users
        self._linha = None
        if s.startswith("SELECT id, username, pw_hash, role FROM app_users"):
            login = params[0].strip().lower()
            for u in users.values():
                if u["username"].lower() == login:
                    self._linha = {k: u[k] for k in ("id", "username", "pw_hash", "role")}
        elif s.startswith("SELECT precisa_trocar_senha FROM app_users WHERE id"):
            if self.banco.sem_coluna:
                raise RuntimeError('column "precisa_trocar_senha" does not exist')
            u = users.get(params[0])
            self._linha = {"precisa_trocar_senha": u["precisa_trocar_senha"]} if u else None
        elif s.startswith("SELECT pw_hash FROM app_users WHERE id"):
            u = users.get(params[0])
            self._linha = {"pw_hash": u["pw_hash"]} if u else None
        elif s.startswith("UPDATE app_users SET pw_hash = %s, precisa_trocar_senha = FALSE WHERE id"):
            users[params[1]]["pw_hash"] = params[0]
            users[params[1]]["precisa_trocar_senha"] = False
        elif s.startswith("UPDATE app_users SET pw_hash = %s WHERE id"):
            users[params[1]]["pw_hash"] = params[0]
        elif s.startswith("SELECT role FROM app_users WHERE id"):
            u = users.get(params[0])
            self._linha = {"role": u["role"]} if u else None
        elif s.startswith("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS precisa_trocar_senha"):
            pass
        else:
            raise AssertionError(f"SQL inesperado no teste: {s}")

    def fetchone(self):
        if self._linha is None:
            return None
        return dict(self._linha) if self.dict_rows else tuple(self._linha.values())


class _Conn:
    def __init__(self, banco):
        self.banco = banco

    def cursor(self, cursor_factory=None):
        return _Cursor(self.banco, cursor_factory is not None)

    def commit(self):
        self.banco.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass


class _Banco:
    def __init__(self):
        self.sqls: list[str] = []
        self.commits = 0
        self.sem_coluna = False
        self.users = {
            7: {"id": 7, "username": "ana@eduit.com.br", "pw_hash": gerar_hash("SenhaAntiga1"),
                "role": "viewer", "precisa_trocar_senha": True},
            8: {"id": 8, "username": "bia@eduit.com.br", "pw_hash": gerar_hash("OutraSenha9"),
                "role": "viewer", "precisa_trocar_senha": False},
        }


@pytest.fixture
def banco(monkeypatch):
    b = _Banco()
    monkeypatch.setattr(auth, "get_conn", lambda: _Conn(b))
    monkeypatch.delenv("DEV_SKIP_AUTH", raising=False)
    return b


@pytest.fixture
def app(banco):
    a = Flask("painel_teste", template_folder=str(RAIZ / "templates"))
    a.secret_key = "chave-de-teste"
    a.register_blueprint(auth.auth_bp)

    @a.route("/")
    def inicio():
        return "painel"

    @a.route("/api/qualquer")
    def api_qualquer():
        return jsonify({"ok": True})

    return a


def _logado(cliente, uid, flag=None, username="ana@eduit.com.br"):
    with cliente.session_transaction() as s:
        s["authenticated"] = True
        s["user_id"] = uid
        s["username"] = username
        s["role"] = "viewer"
        if flag is not None:
            s["precisa_trocar_senha"] = flag


# ── validação da nova senha ──────────────────────────────────────────────

def test_tamanho_minimo_e_razoavel():
    assert troca_senha.TAMANHO_MINIMO >= 8


@pytest.mark.parametrize(
    "atual,nova,confirmacao,trecho",
    [
        ("", "NovaSenha123", "NovaSenha123", "senha atual"),
        ("SenhaAntiga1", "", "", "nova senha"),
        ("SenhaAntiga1", "curta1", "curta1", "8"),
        ("SenhaAntiga1", "NovaSenha123", "NovaSenha124", "não conferem"),
        ("SenhaAntiga1", "SenhaAntiga1", "SenhaAntiga1", "diferente da atual"),
        ("SenhaAntiga1", "x" * 200, "x" * 200, "128"),
    ],
)
def test_validacao_recusa(atual, nova, confirmacao, trecho):
    erro = troca_senha.validar_nova_senha(atual, nova, confirmacao)
    assert erro and trecho in erro


def test_validacao_aceita():
    assert troca_senha.validar_nova_senha("SenhaAntiga1", "NovaSenha123", "NovaSenha123") is None


# ── troca obrigatória: bloqueio ──────────────────────────────────────────

def test_com_flag_pagina_redireciona_para_troca(app):
    c = app.test_client()
    _logado(c, 7, flag=True)
    r = c.get("/")
    assert r.status_code == 302
    assert r.headers["Location"].endswith("/trocar-senha")


def test_com_flag_api_recebe_403_em_json(app):
    c = app.test_client()
    _logado(c, 7, flag=True)
    r = c.get("/api/qualquer")
    assert r.status_code == 403
    assert r.get_json()["redirect"] == "/trocar-senha"


@pytest.mark.parametrize("caminho", ["/trocar-senha", "/logout", "/health", "/static/js/app.js"])
def test_com_flag_rotas_livres_nao_redirecionam_para_troca(app, caminho):
    c = app.test_client()
    _logado(c, 7, flag=True)
    r = c.get(caminho)
    assert not (r.status_code == 302 and r.headers["Location"].endswith("/trocar-senha"))
    assert r.status_code != 403


def test_sem_flag_navega_normalmente(app):
    c = app.test_client()
    _logado(c, 8, flag=False, username="bia@eduit.com.br")
    assert c.get("/").data == b"painel"
    assert c.get("/api/qualquer").status_code == 200


def test_sessao_antiga_sem_a_chave_nao_e_bloqueada(app):
    c = app.test_client()
    _logado(c, 8, username="bia@eduit.com.br")
    assert c.get("/").data == b"painel"


def test_dev_skip_auth_nao_e_bloqueado(app, monkeypatch):
    monkeypatch.setenv("DEV_SKIP_AUTH", "1")
    c = app.test_client()
    assert c.get("/").data == b"painel"


def test_trocar_senha_sem_login_vai_para_o_login(app):
    r = app.test_client().get("/trocar-senha")
    assert r.status_code == 302
    assert r.headers["Location"].endswith("/login")


# ── login lê a flag ──────────────────────────────────────────────────────

def test_login_de_usuario_com_flag_cai_na_troca(app):
    c = app.test_client()
    r = c.post("/login", data={"username": "ana@eduit.com.br", "password": "SenhaAntiga1"})
    assert r.status_code == 302
    with c.session_transaction() as s:
        assert s["precisa_trocar_senha"] is True
    assert c.get("/").headers["Location"].endswith("/trocar-senha")


def test_login_de_usuario_sem_flag_entra(app):
    c = app.test_client()
    c.post("/login", data={"username": "bia@eduit.com.br", "password": "OutraSenha9"})
    with c.session_transaction() as s:
        assert s["precisa_trocar_senha"] is False
    assert c.get("/").data == b"painel"


def test_login_continua_funcionando_se_a_coluna_nao_existir(app, banco):
    banco.sem_coluna = True
    c = app.test_client()
    c.post("/login", data={"username": "ana@eduit.com.br", "password": "SenhaAntiga1"})
    with c.session_transaction() as s:
        assert s["authenticated"] is True
        assert s["precisa_trocar_senha"] is False
    assert c.get("/").data == b"painel"


# ── tela e gravação ──────────────────────────────────────────────────────

def test_tela_de_troca_abre_com_os_tres_campos(app):
    c = app.test_client()
    _logado(c, 7, flag=True)
    html = c.get("/trocar-senha").get_data(as_text=True)
    for campo in ("senha_atual", "nova_senha", "confirmacao"):
        assert f'name="{campo}"' in html
    assert 'method="POST"' in html
    assert "Trocar senha" in html


def test_troca_certa_grava_scrypt_desliga_flag_e_libera(app, banco):
    c = app.test_client()
    _logado(c, 7, flag=True)
    r = c.post("/trocar-senha", data={
        "senha_atual": "SenhaAntiga1", "nova_senha": "NovaSenha123", "confirmacao": "NovaSenha123"})
    assert r.status_code == 302
    assert r.headers["Location"].endswith("/")
    u = banco.users[7]
    assert u["precisa_trocar_senha"] is False
    assert u["pw_hash"].startswith("scrypt:")
    assert conferir_senha("NovaSenha123", u["pw_hash"]) == (True, False)
    assert banco.commits >= 1
    with c.session_transaction() as s:
        assert not s.get("precisa_trocar_senha")
    assert c.get("/").data == b"painel"


def test_usuario_sem_flag_tambem_pode_trocar(app, banco):
    c = app.test_client()
    _logado(c, 8, flag=False, username="bia@eduit.com.br")
    r = c.post("/trocar-senha", data={
        "senha_atual": "OutraSenha9", "nova_senha": "NovaSenha456", "confirmacao": "NovaSenha456"})
    assert r.status_code == 302
    assert conferir_senha("NovaSenha456", banco.users[8]["pw_hash"])[0]


def test_senha_atual_errada_nao_grava(app, banco):
    antes = banco.users[7]["pw_hash"]
    c = app.test_client()
    _logado(c, 7, flag=True)
    r = c.post("/trocar-senha", data={
        "senha_atual": "errada", "nova_senha": "NovaSenha123", "confirmacao": "NovaSenha123"})
    assert r.status_code == 400
    assert "Senha atual incorreta" in r.get_data(as_text=True)
    assert banco.users[7]["pw_hash"] == antes
    assert banco.users[7]["precisa_trocar_senha"] is True
    with c.session_transaction() as s:
        assert s["precisa_trocar_senha"] is True


def test_confirmacao_diferente_nao_grava(app, banco):
    antes = banco.users[7]["pw_hash"]
    c = app.test_client()
    _logado(c, 7, flag=True)
    r = c.post("/trocar-senha", data={
        "senha_atual": "SenhaAntiga1", "nova_senha": "NovaSenha123", "confirmacao": "Outra12345"})
    assert r.status_code == 400
    assert "não conferem" in r.get_data(as_text=True)
    assert banco.users[7]["pw_hash"] == antes


def test_hash_antigo_sha256_tambem_confere_na_troca(app, banco):
    import hashlib
    banco.users[7]["pw_hash"] = hashlib.sha256(b"SenhaAntiga1").hexdigest()
    c = app.test_client()
    _logado(c, 7, flag=True)
    r = c.post("/trocar-senha", data={
        "senha_atual": "SenhaAntiga1", "nova_senha": "NovaSenha123", "confirmacao": "NovaSenha123"})
    assert r.status_code == 302
    assert banco.users[7]["pw_hash"].startswith("scrypt:")


def test_login_de_emergencia_nao_tem_senha_no_banco(app, banco):
    c = app.test_client()
    with c.session_transaction() as s:
        s.update(authenticated=True, user_id=0, username="admin", role="admin")
    r = c.post("/trocar-senha", data={
        "senha_atual": "x", "nova_senha": "NovaSenha123", "confirmacao": "NovaSenha123"})
    assert r.status_code == 400
    assert "emergência" in r.get_data(as_text=True)
    assert not any(s.startswith("UPDATE") for s in banco.sqls)


# ── migração, login e menu ───────────────────────────────────────────────

def test_migracao_so_cria_a_coluna_sem_marcar_ninguem():
    sql = " ".join(troca_senha.SQL_COLUNA.split())
    assert sql == (
        "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS precisa_trocar_senha BOOLEAN NOT NULL DEFAULT FALSE"
    )


def test_garantir_coluna_roda_o_alter(banco):
    troca_senha.garantir_coluna(lambda: _Conn(banco))
    assert any(s.startswith("ALTER TABLE app_users ADD COLUMN IF NOT EXISTS precisa_trocar_senha") for s in banco.sqls)
    assert banco.commits == 1


def test_garantir_coluna_nao_derruba_o_boot():
    def _falha():
        raise RuntimeError("sem banco")
    troca_senha.garantir_coluna(_falha)  # não levanta


def test_app_chama_garantir_coluna_no_boot():
    src = (RAIZ / "app.py").read_text(encoding="utf-8")
    assert re.search(r"troca_senha\.garantir_coluna\(", src)


def test_login_html_continua_com_o_mesmo_formulario(app):
    html = app.test_client().get("/login").get_data(as_text=True)
    assert 'name="username"' in html and 'name="password"' in html
    assert "Entrar no Sistema" in html


def test_menu_do_perfil_tem_link_para_trocar_senha():
    html = (RAIZ / "templates" / "partials" / "_profile.html").read_text(encoding="utf-8")
    assert 'href="/trocar-senha"' in html
