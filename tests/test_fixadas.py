"""Fase 4, etapa 3 (27/11): telas fixadas por usuário no banco (/api/me/fixadas)."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import flask
import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

import navegacao  # noqa: E402
import permissoes  # noqa: E402
import routes.fixadas as fx  # noqa: E402


class _Cur:
    def __init__(self, conn):
        self.conn = conn
        self._ult = ""

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._ult = sql
        self.conn.chamadas.append((sql, params))
        if self.conn.erro:
            raise RuntimeError("banco fora")

    def fetchall(self):
        if "page_views" in self._ult:
            return list(self.conn.views)
        return [(p,) for p in self.conn.fixadas]


class _Conn:
    def __init__(self, fixadas=(), views=(), erro=False):
        self.fixadas, self.views, self.erro = list(fixadas), list(views), erro
        self.chamadas, self.commits, self.fechada = [], 0, False

    def cursor(self, *a, **k):
        return _Cur(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.fechada = True


def _cliente(conn, role="viewer", pages=("repasse", "logs", "bwipo_comercial"), categoria="Supervisor Comercial", user_id=7):
    app = flask.Flask(__name__)
    app.secret_key = "t"
    app.register_blueprint(fx.fixadas_bp)
    menu = navegacao.Menu(role, set(pages), categoria, "f@x")
    patches = [
        patch.object(fx, "get_conn", return_value=conn),
        patch.object(fx.navegacao, "menu_do_usuario", return_value=menu),
    ]
    for p in patches:
        p.start()
    cli = app.test_client()
    with cli.session_transaction() as s:
        s.update(authenticated=True, user_id=user_id, role=role, username="f@x")
    return cli, patches


def _parar(patches):
    for p in patches:
        p.stop()


def _get(conn, **kw):
    cli, patches = _cliente(conn, **kw)
    try:
        return cli.get("/api/me/fixadas")
    finally:
        _parar(patches)


def _put(conn, corpo, **kw):
    cli, patches = _cliente(conn, **kw)
    try:
        return cli.put("/api/me/fixadas", json=corpo)
    finally:
        _parar(patches)


def test_get_devolve_fixadas_em_ordem():
    conn = _Conn(fixadas=["repasse", "bwipo_comercial"])
    r = _get(conn)
    assert r.status_code == 200
    assert r.get_json()["fixadas"] == ["repasse", "bwipo_comercial"]
    sql, params = conn.chamadas[0]
    assert "nav_fixadas" in sql and "ORDER BY" in sql and "ordem" in sql
    assert params == (7,)


def test_get_some_fixada_sem_permissao():
    conn = _Conn(fixadas=["repasse", "logs"])
    r = _get(conn, pages=("repasse",))
    assert r.get_json()["fixadas"] == ["repasse"]


def test_get_mais_usadas_top3_30_dias():
    views = [("repasse", 9), ("bwipo_comercial", 8), ("avisos", 7), ("profile", 6), ("xyz", 5)]
    conn = _Conn(views=views)
    r = _get(conn)
    assert r.get_json()["mais_usadas"] == ["repasse", "bwipo_comercial", "avisos"]
    sql = next(s for s, _ in conn.chamadas if "page_views" in s)
    assert "30 days" in sql and "America/Sao_Paulo" in sql
    assert "LIMIT" not in sql


def test_mais_usadas_respeita_visivel():
    conn = _Conn(views=[("page_views", 9), ("repasse", 5)])
    r = _get(conn, pages=("repasse", "page_views"))
    assert r.get_json()["mais_usadas"] == ["repasse"]
    r = _get(_Conn(views=[("page_views", 9), ("repasse", 5)]), role="admin")
    assert r.get_json()["mais_usadas"] == ["page_views", "repasse"]


def test_get_erro_de_banco_devolve_listas_vazias():
    r = _get(_Conn(erro=True))
    assert r.status_code == 200
    assert r.get_json() == {"fixadas": [], "mais_usadas": []}


def test_put_substitui_lista():
    conn = _Conn()
    r = _put(conn, {"paginas": ["repasse", "bwipo_comercial"]})
    assert r.status_code == 200
    assert r.get_json() == {"fixadas": ["repasse", "bwipo_comercial"]}
    sqls = [(s, p) for s, p in conn.chamadas]
    assert "DELETE FROM nav_fixadas" in sqls[0][0] and sqls[0][1] == (7,)
    inserts = [p for s, p in sqls if "INSERT INTO nav_fixadas" in s]
    assert inserts == [(7, "repasse", 0), (7, "bwipo_comercial", 1)]
    assert conn.commits == 1


def test_put_lista_vazia_desafixa_tudo():
    conn = _Conn()
    r = _put(conn, {"paginas": []})
    assert r.status_code == 200 and r.get_json() == {"fixadas": []}
    assert conn.commits == 1


@pytest.mark.parametrize("corpo", [
    {"paginas": "repasse"},
    {"paginas": [1, 2]},
    {},
    {"paginas": ["repasse", "repasse"]},
    {"paginas": ["repasse"] * 9},
    {"paginas": ["xyz"]},
    {"paginas": ["config"]},  # sem permissão
])
def test_put_valida(corpo):
    conn = _Conn()
    r = _put(conn, corpo)
    assert r.status_code == 400
    assert r.get_json()["error"]
    assert conn.commits == 0 and conn.chamadas == []


def test_put_nove_itens_distintos_recusa():
    nove = list(navegacao.CONHECIDAS[:9])
    r = _put(_Conn(), {"paginas": nove}, role="admin")
    assert r.status_code == 400
    assert fx.MAX_FIXADAS == 8


def test_put_recusa_tela_de_grupo_oculto():
    conn = _Conn()
    r = _put(conn, {"paginas": ["page_views"]}, pages=("page_views",))
    assert r.status_code == 400
    assert conn.commits == 0


def test_usuario_zero():
    conn = _Conn()
    r = _get(conn, role="admin", user_id=0)
    assert r.get_json() == {"fixadas": [], "mais_usadas": []}
    r = _put(conn, {"paginas": ["repasse"]}, role="admin", user_id=0)
    assert r.status_code == 400
    assert r.get_json()["error"] == "Este login não guarda telas fixadas."
    assert conn.chamadas == []


def test_rota_e_do_proprio_usuario():
    assert "/api/me/fixadas" in permissoes.ROTAS_DO_PROPRIO_USUARIO


def test_tabela_criada_no_boot():
    app_src = (RAIZ / "app.py").read_text(encoding="utf-8")
    db_src = (RAIZ / "db.py").read_text(encoding="utf-8")
    assert "_ensure_nav_fixadas_table()" in app_src
    assert "CREATE TABLE IF NOT EXISTS nav_fixadas" in db_src
    assert "ON DELETE CASCADE" in db_src
    assert "app.register_blueprint(fixadas_bp)" in app_src
    assert app_src.index("register_blueprint(fixadas_bp)") < app_src.index("_aplicar_mapa_permissoes(app)")
