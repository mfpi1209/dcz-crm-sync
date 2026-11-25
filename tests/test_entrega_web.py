"""Fase 0: compressão e cache dos estáticos (entrega_web.py)."""
import gzip
from pathlib import Path

import pytest
from flask import Flask, jsonify

from entrega_web import (
    CACHE_LONGO,
    CACHE_REVALIDAR,
    configurar_entrega,
    versao_estaticos,
)

RAIZ = Path(__file__).resolve().parent.parent
JS = "function f(){return 1}\n" * 200  # ~4,6 KB, compressível


@pytest.fixture
def static_dir(tmp_path):
    d = tmp_path / "static"
    (d / "js").mkdir(parents=True)
    (d / "js" / "a.js").write_bytes(JS.encode())
    (d / "pagina.html").write_text("<p>ok</p>", encoding="utf-8")
    return d


def _app(static_dir, autorefresh=False):
    app = Flask(__name__, static_folder=None)

    @app.route("/")
    def index():
        return "<html>" + ("<div>linha</div>" * 500) + "</html>"

    @app.route("/api/dados")
    def dados():
        return jsonify(itens=[{"n": i, "nome": "x" * 10} for i in range(200)])

    @app.route("/api/csv")
    def csv():
        return app.response_class("a;b\n" * 500, mimetype="text/csv")

    versao = versao_estaticos(static_dir)
    configurar_entrega(app, static_dir, versao, autorefresh=autorefresh)
    return app, versao


# ── versão do conteúdo ───────────────────────────────────────────────────

def test_versao_estavel_e_muda_com_o_conteudo(static_dir):
    v1 = versao_estaticos(static_dir)
    assert v1 == versao_estaticos(static_dir)
    (static_dir / "js" / "a.js").write_text(JS + "//x", encoding="utf-8")
    assert versao_estaticos(static_dir) != v1


def test_versao_ignora_arquivos_comprimidos_do_build(static_dir):
    v1 = versao_estaticos(static_dir)
    (static_dir / "js" / "a.js.gz").write_bytes(gzip.compress(JS.encode()))
    (static_dir / "js" / "a.js.br").write_bytes(b"qualquer")
    assert versao_estaticos(static_dir) == v1


# ── compressão das respostas do Flask ────────────────────────────────────

def test_html_comprimido_quando_aceito(static_dir):
    app, _ = _app(static_dir)
    r = app.test_client().get("/", headers={"Accept-Encoding": "gzip"})
    assert r.headers.get("Content-Encoding") == "gzip"
    assert "Accept-Encoding" in r.headers.get("Vary", "")
    assert gzip.decompress(r.data).startswith(b"<html>")


def test_html_sem_compressao_quando_nao_aceito(static_dir):
    app, _ = _app(static_dir)
    r = app.test_client().get("/")
    assert "Content-Encoding" not in r.headers
    assert r.data.startswith(b"<html>")


def test_json_comprimido_com_brotli(static_dir):
    app, _ = _app(static_dir)
    r = app.test_client().get("/api/dados", headers={"Accept-Encoding": "br, gzip"})
    assert r.headers.get("Content-Encoding") == "br"


def test_csv_fica_sem_compressao(static_dir):
    app, _ = _app(static_dir)
    r = app.test_client().get("/api/csv", headers={"Accept-Encoding": "gzip"})
    assert "Content-Encoding" not in r.headers


# ── estáticos pelo WhiteNoise ────────────────────────────────────────────

def test_estatico_com_versao_atual_fica_em_cache_longo(static_dir):
    app, versao = _app(static_dir)
    r = app.test_client().get(f"/static/js/a.js?v={versao}")
    assert r.status_code == 200
    assert r.headers["Cache-Control"] == CACHE_LONGO
    assert r.data.decode() == JS


def test_versao_com_sufixo_manual_do_index_vale(static_dir):
    app, versao = _app(static_dir)
    r = app.test_client().get(f"/static/js/a.js?v={versao}-hide-crgm")
    assert r.headers["Cache-Control"] == CACHE_LONGO


@pytest.mark.parametrize("qs", ["", "?v=", "?v=outra-versao", "?x=1"])
def test_sem_versao_ou_versao_de_outro_deploy_revalida(static_dir, qs):
    app, _ = _app(static_dir)
    r = app.test_client().get("/static/js/a.js" + qs)
    assert r.status_code == 200
    assert r.headers["Cache-Control"] == CACHE_REVALIDAR


def test_revalidacao_devolve_304(static_dir):
    app, _ = _app(static_dir)
    c = app.test_client()
    etag = c.get("/static/pagina.html").headers["ETag"]
    r = c.get("/static/pagina.html", headers={"If-None-Match": etag})
    assert r.status_code == 304


def test_estatico_usa_versao_gz_gerada_no_build(static_dir):
    (static_dir / "js" / "a.js.gz").write_bytes(gzip.compress(JS.encode()))
    app, versao = _app(static_dir)
    r = app.test_client().get(f"/static/js/a.js?v={versao}", headers={"Accept-Encoding": "gzip"})
    assert r.headers.get("Content-Encoding") == "gzip"
    assert gzip.decompress(r.data).decode() == JS
    assert r.headers["Cache-Control"] == CACHE_LONGO


def test_modo_desenvolvimento_nunca_usa_cache_longo(static_dir):
    app, versao = _app(static_dir, autorefresh=True)
    r = app.test_client().get(f"/static/js/a.js?v={versao}")
    assert r.headers["Cache-Control"] == CACHE_REVALIDAR


def test_estatico_inexistente_cai_no_flask(static_dir):
    app, _ = _app(static_dir)
    assert app.test_client().get("/static/js/nao_existe.js").status_code == 404


def test_rotas_fora_de_static_nao_recebem_cache_control_de_estatico(static_dir):
    app, _ = _app(static_dir)
    r = app.test_client().get("/api/dados")
    assert r.headers.get("Cache-Control") not in (CACHE_LONGO, CACHE_REVALIDAR)


# ── ligação com o painel ─────────────────────────────────────────────────

def test_index_nao_sobrescreve_a_versao_com_numero_aleatorio():
    html = (RAIZ / "templates" / "index.html").read_text(encoding="utf-8")
    assert "| random" not in html
    assert "?v={{_v}}" in html


def test_app_liga_a_entrega_com_a_versao_do_conteudo():
    src = (RAIZ / "app.py").read_text(encoding="utf-8")
    assert "configurar_entrega(app" in src
    assert "versao_estaticos(" in src


def test_dockerfile_gera_os_comprimidos_e_desliga_o_autorefresh():
    df = (RAIZ / "Dockerfile").read_text(encoding="utf-8")
    assert "whitenoise.compress" in df
    assert "STATIC_AUTOREFRESH=0" in df


def test_todo_js_do_index_vem_com_versao():
    """Cache longo só é seguro porque todo /static/js do index leva ?v={{_v}}."""
    import re
    html = (RAIZ / "templates" / "index.html").read_text(encoding="utf-8")
    refs = re.findall(r"filename='(js/[^']+)'\)\s*}}([^\"]*)\"", html)
    assert len(refs) >= 50
    assert all(sufixo.startswith("?v={{_v}}") for _, sufixo in refs)
