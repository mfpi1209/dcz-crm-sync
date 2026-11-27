"""Admin "Entrar como" outro usuário, sem senha, e "Voltar para o admin" (26/11).

Banco simulado (o de test_troca_senha): nenhum Postgres, nenhum segredo.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest
from flask import Flask, render_template_string, session

from test_troca_senha import _Banco, _Conn  # noqa: E402  (mesma pasta)

RAIZ = Path(__file__).resolve().parent.parent
auth = pytest.importorskip("routes.auth")


@pytest.fixture
def banco(monkeypatch):
    b = _Banco()
    b.users[1] = {"id": 1, "username": "raphael", "pw_hash": "x", "role": "admin", "precisa_trocar_senha": False}
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
        return f"{session.get('user_id')}|{session.get('username')}|{session.get('role')}"

    return a


def _como(cliente, uid, username, role, **extra):
    with cliente.session_transaction() as s:
        s.update(authenticated=True, user_id=uid, username=username, role=role, **extra)


# ── entrar como ──────────────────────────────────────────────────────────

def test_admin_entra_como_consultor(app, caplog):
    c = app.test_client()
    _como(c, 1, "raphael", "admin")
    with caplog.at_level(logging.INFO):
        r = c.post("/api/users/7/entrar-como")
    assert r.status_code == 200
    assert r.get_json() == {"ok": True, "redirect": "/"}
    assert c.get("/").data == b"7|ana@eduit.com.br|viewer"
    with c.session_transaction() as s:
        assert s["admin_original"] == {"user_id": 1, "username": "raphael", "role": "admin"}
        assert s["precisa_trocar_senha"] is False
    # registro: quem entrou como quem
    assert any("raphael" in m and "ana@eduit.com.br" in m and "entrou como" in m for m in caplog.messages)


def test_entrar_como_nao_e_bloqueado_pela_flag_do_usuario(app, banco):
    assert banco.users[7]["precisa_trocar_senha"] is True
    c = app.test_client()
    _como(c, 1, "raphael", "admin")
    c.post("/api/users/7/entrar-como")
    assert c.get("/").status_code == 200


@pytest.mark.parametrize("role", ["viewer", "editor"])
def test_quem_nao_e_admin_nao_entra_como(app, role):
    c = app.test_client()
    _como(c, 8, "bia@eduit.com.br", role)
    r = c.post("/api/users/7/entrar-como")
    assert r.status_code == 403
    assert c.get("/").data == f"8|bia@eduit.com.br|{role}".encode()


def test_usuario_inexistente(app):
    c = app.test_client()
    _como(c, 1, "raphael", "admin")
    assert c.post("/api/users/999/entrar-como").status_code == 404


def test_nao_entra_como_si_mesmo(app):
    c = app.test_client()
    _como(c, 1, "raphael", "admin")
    assert c.post("/api/users/1/entrar-como").status_code == 400


def test_get_nao_entra_como(app):
    c = app.test_client()
    _como(c, 1, "raphael", "admin")
    assert c.get("/api/users/7/entrar-como").status_code == 405


def test_sem_login_recebe_401(app):
    assert app.test_client().post("/api/users/7/entrar-como").status_code == 401


def test_admin_como_outro_admin_guarda_o_primeiro_original(app, banco):
    banco.users[9] = {"id": 9, "username": "marcelo", "pw_hash": "x", "role": "admin", "precisa_trocar_senha": False}
    c = app.test_client()
    _como(c, 1, "raphael", "admin")
    c.post("/api/users/9/entrar-como")
    c.post("/api/users/7/entrar-como")
    with c.session_transaction() as s:
        assert s["admin_original"]["username"] == "raphael"
    c.post("/voltar-admin")
    assert c.get("/").data == b"1|raphael|admin"


# ── voltar para o admin ──────────────────────────────────────────────────

def test_voltar_para_o_admin(app, caplog):
    c = app.test_client()
    _como(c, 1, "raphael", "admin")
    c.post("/api/users/7/entrar-como")
    with caplog.at_level(logging.INFO):
        r = c.post("/voltar-admin")
    assert r.status_code == 302
    assert r.headers["Location"].endswith("/")
    assert c.get("/").data == b"1|raphael|admin"
    with c.session_transaction() as s:
        assert "admin_original" not in s
    assert any("voltou" in m and "raphael" in m for m in caplog.messages)


def test_voltar_sem_ter_entrado_nao_muda_nada(app):
    c = app.test_client()
    _como(c, 8, "bia@eduit.com.br", "viewer")
    r = c.post("/voltar-admin")
    assert r.status_code == 302
    assert c.get("/").data == b"8|bia@eduit.com.br|viewer"


def test_voltar_continua_livre_se_houver_flag_de_troca(app):
    """Mesmo que algo ligue a flag na sessão, o admin consegue voltar."""
    c = app.test_client()
    _como(c, 7, "ana@eduit.com.br", "viewer", precisa_trocar_senha=True,
          admin_original={"user_id": 1, "username": "raphael", "role": "admin"})
    c.post("/voltar-admin")
    assert c.get("/").data == b"1|raphael|admin"


def test_logout_encerra_tudo(app):
    c = app.test_client()
    _como(c, 1, "raphael", "admin")
    c.post("/api/users/7/entrar-como")
    c.get("/logout")
    with c.session_transaction() as s:
        assert "admin_original" not in s and not s.get("authenticated")


# ── faixa e botão ────────────────────────────────────────────────────────

def _faixa(app, sessao):
    with app.test_request_context("/"):
        session.update(sessao)
        return render_template_string("{% include 'partials/_entrar_como.html' %}")


def test_faixa_aparece_so_quando_esta_como_outro(app):
    html = _faixa(app, {"username": "ana@eduit.com.br",
                        "admin_original": {"user_id": 1, "username": "raphael", "role": "admin"}})
    assert "ana@eduit.com.br" in html
    assert 'action="/voltar-admin"' in html and 'method="POST"' in html
    assert "Voltar para o admin" in html
    assert _faixa(app, {"username": "raphael"}).strip() == ""


def test_faixa_escapa_o_nome(app):
    html = _faixa(app, {"username": "<script>x</script>",
                        "admin_original": {"user_id": 1, "username": "raphael", "role": "admin"}})
    assert "<script>x</script>" not in html


def test_index_inclui_a_faixa():
    src = (RAIZ / "templates" / "index.html").read_text(encoding="utf-8")
    assert "{% include 'partials/_entrar_como.html' %}" in src


def test_tela_de_usuarios_tem_o_botao_entrar_como():
    js = (RAIZ / "static" / "js" / "config.js").read_text(encoding="utf-8")
    assert "entrarComoUser(" in js
    assert "/entrar-como" in js
