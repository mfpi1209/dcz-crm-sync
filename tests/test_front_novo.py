"""Fase 4, etapa 2 (27/11): front novo servido pelo Flask em /app/."""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from unittest.mock import patch

import flask
import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

import navegacao  # noqa: E402
import routes.front_novo as fn  # noqa: E402


@pytest.fixture
def dist(tmp_path, monkeypatch):
    d = tmp_path / "dist"
    (d / "assets").mkdir(parents=True)
    (d / "index.html").write_text("<!doctype html><div id=raiz></div>", encoding="utf-8")
    (d / "assets" / "index-abc123.js").write_text("console.log(1)", encoding="utf-8")
    (tmp_path / "segredo.txt").write_text("nao", encoding="utf-8")
    monkeypatch.setattr(fn, "DIST", d)
    return d


def _cliente(logado=True):
    import routes.auth as auth

    app = flask.Flask(__name__)
    app.secret_key = "t"
    app.register_blueprint(auth.auth_bp)
    app.register_blueprint(fn.front_novo_bp)
    cli = app.test_client()
    if logado:
        with cli.session_transaction() as s:
            s.update(authenticated=True, user_id=7, role="viewer", username="f")
    return cli


@pytest.fixture
def cli():
    import routes.auth as auth

    with patch.object(auth.troca_senha, "bloqueio", return_value=None), \
         patch.dict(os.environ, {"DEV_SKIP_AUTH": ""}):
        yield _cliente()


def test_index_sem_cache(dist, cli):
    r = cli.get("/app/")
    assert r.status_code == 200
    assert b'id=raiz' in r.data
    assert r.headers["Cache-Control"] == "no-cache"


@pytest.mark.parametrize("caminho", ["/app/page_views", "/app/qualquer/coisa/aqui"])
def test_rotas_do_react_devolvem_o_index(dist, cli, caminho):
    r = cli.get(caminho)
    assert r.status_code == 200 and b"id=raiz" in r.data


def test_assets_com_cache_longo(dist, cli):
    r = cli.get("/app/assets/index-abc123.js")
    assert r.status_code == 200
    assert "immutable" in r.headers["Cache-Control"]
    assert "max-age=31536000" in r.headers["Cache-Control"]


def test_asset_que_nao_existe_e_404_e_nao_o_index(dist, cli):
    assert cli.get("/app/assets/sumiu.js").status_code == 404


def test_nao_sai_da_pasta_do_build(dist, cli):
    r = cli.get("/app/assets/../../segredo.txt")
    assert b"nao" not in r.data


def test_sem_barra_redireciona(dist, cli):
    r = cli.get("/app")
    assert r.status_code in (301, 308)
    assert r.headers["Location"].endswith("/app/")


def test_sem_build_explica(tmp_path, monkeypatch, cli):
    monkeypatch.setattr(fn, "DIST", tmp_path / "nao_existe")
    r = cli.get("/app/")
    assert r.status_code == 503
    assert "npm run build" in r.get_data(as_text=True)


def test_sem_login_vai_para_o_login(dist):
    import routes.auth as auth

    with patch.dict(os.environ, {"DEV_SKIP_AUTH": ""}):
        cli = _cliente(logado=False)
        r = cli.get("/app/")
        assert r.status_code == 302 and "/login" in r.headers["Location"]
        # o JS e o CSS do front também ficam atrás do login
        assert cli.get("/app/assets/index-abc123.js").status_code == 302


def test_app_registra_o_blueprint():
    src = (RAIZ / "app.py").read_text(encoding="utf-8")
    assert "app.register_blueprint(front_novo_bp)" in src
    # antes do mapa de permissões, como os demais
    assert src.index("register_blueprint(front_novo_bp)") < src.index("_aplicar_mapa_permissoes(app)")


# ── Nomes das telas e grupos do menu ───────────────────────────────────────
def _titulos_ts() -> dict[str, str]:
    txt = (RAIZ / "frontend" / "src" / "titulos.ts").read_text(encoding="utf-8")
    return dict(re.findall(r'^\s*"([^"]+)":\s*"([^"]*)",?$', txt, re.M))


def test_titulos_iguais_aos_do_painel_atual():
    utils = (RAIZ / "static" / "js" / "utils.js").read_text(encoding="utf-8")
    linha = re.search(r"^const PAGE_TITLES = (\{.*\});$", utils, re.M).group(1)
    antigos = {(a or b): t for a, b, t in re.findall(r"(?:'([^']+)'|([A-Za-z_][\w-]*))\s*:\s*'([^']*)'", linha)}
    assert _titulos_ts() == antigos


def test_toda_tela_do_menu_tem_nome_no_front():
    """Tela do menu sem nome em titulos.ts sumiria do menu do front novo."""
    usadas = set()
    for f in (RAIZ / "templates").rglob("*.html"):
        usadas |= set(re.findall(r"nav_can\('([^']+)'\)", f.read_text(encoding="utf-8")))
    for paginas in navegacao.GRUPOS.values():
        usadas |= set(paginas)
    falta = usadas - set(_titulos_ts())
    assert not falta, sorted(falta)


def test_grupos_do_front_existem_no_servidor():
    txt = (RAIZ / "frontend" / "src" / "menu.ts").read_text(encoding="utf-8")
    nomes = re.findall(r'\{ nome: "([a-z_]+)", titulo:', txt)
    assert nomes and set(nomes) <= set(navegacao.GRUPOS)


def test_api_traz_as_telas_de_cada_grupo():
    m = navegacao.Menu("viewer", {"repasse", "bwipo_comercial", "page_views"}, "Supervisor Comercial", "f@x")
    g = m.para_api()["grupos"]
    assert set(g) == set(navegacao.GRUPOS)
    assert g["comercial"] == ["bwipo_comercial", "repasse"]   # ordem do GRUPOS
    assert g["ti"] == ["page_views"]                          # a seção decide se aparece
    assert m.para_api()["secoes"]["ti"] is False


# ── Build na imagem e no CI ────────────────────────────────────────────────
def test_dockerfile_compila_o_front_e_leva_so_o_dist():
    docker = (RAIZ / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"^FROM node:(\d+)-slim AS front$", docker, re.M)
    assert "RUN npm ci" in docker and "RUN npm run build" in docker
    assert "COPY --from=front /front/dist ./frontend/dist" in docker
    # a etapa final continua sendo Python (sem Node em produção)
    finais = re.findall(r"^FROM (\S+)", docker, re.M)
    assert finais[-1].startswith("python:")


def test_mesma_versao_do_node_no_ci_e_no_dockerfile():
    docker = (RAIZ / "Dockerfile").read_text(encoding="utf-8")
    wf = (RAIZ / ".github" / "workflows" / "deploy-easypanel.yml").read_text(encoding="utf-8")
    v = re.search(r"^FROM node:(\d+)-slim", docker, re.M).group(1)
    assert f'node-version: "{v}"' in wf


def test_build_do_front_fora_do_git_e_da_imagem_de_contexto():
    assert "frontend/node_modules/" in (RAIZ / ".gitignore").read_text(encoding="utf-8")
    assert "frontend/dist/" in (RAIZ / ".gitignore").read_text(encoding="utf-8")
    di = (RAIZ / ".dockerignore").read_text(encoding="utf-8")
    assert "frontend/node_modules" in di


def test_versoes_exatas_no_package_json():
    pkg = json.loads((RAIZ / "frontend" / "package.json").read_text(encoding="utf-8"))
    for secao in ("dependencies", "devDependencies"):
        for nome, versao in pkg[secao].items():
            assert re.fullmatch(r"\d+\.\d+\.\d+", versao), f"{nome}@{versao}"
    assert (RAIZ / "frontend" / "package-lock.json").is_file()


# ── Telas migradas: o menu do painel atual aponta para o front novo ────────
def _telas_novas() -> set[str]:
    txt = (RAIZ / "frontend" / "src" / "telas.tsx").read_text(encoding="utf-8")
    bloco = txt.split("TELAS_NOVAS: Record<string, ComponentType> = {", 1)[1].split("};", 1)[0]
    return set(re.findall(r"^\s*([a-z_-]+):", bloco, re.M))


def test_ha_tela_migrada():
    assert "page_views" in _telas_novas()


def test_repasse_migrado_mas_menu_antigo_ainda_na_tela_antiga():
    """Repasse está no front novo, mas o link do menu antigo só troca após a aprovação."""
    assert "repasse" in _telas_novas()
    assert "repasse" not in TELAS_APROVADAS


def test_dashboard_migrado_mas_menu_antigo_ainda_na_tela_antiga():
    """Dashboard (home) está no front novo; o menu antigo só troca após a aprovação."""
    assert "dashboard" in _telas_novas()
    assert "dashboard" not in TELAS_APROVADAS


# Roteiro de cada tela (AGENTS.md, 28/11): 1) entra em TELAS_NOVAS e é publicada;
# 2) o usuário compara em produção e aprova; 3) só então o item do _sidebar.html
# passa a /app/<tela>. Por isso a regra do link vale só para as telas já
# aprovadas, listadas aqui à mão (uma tela recém-migrada, como o Repasse, fica
# de fora até a aprovação). Ao aprovar, acrescente a tela aqui no mesmo PR que
# troca o link do menu antigo.
TELAS_APROVADAS = {"page_views"}


def test_telas_aprovadas_estao_no_front_novo():
    assert TELAS_APROVADAS <= _telas_novas()


def test_menu_antigo_leva_as_telas_migradas_ao_front_novo():
    """Tela aprovada: o item do _sidebar.html abre /app/<tela>, sem o navigate() antigo."""
    html = (RAIZ / "templates" / "partials" / "_sidebar.html").read_text(encoding="utf-8")
    for tela in TELAS_APROVADAS:
        links = re.findall(rf'<a [^>]*data-page="{re.escape(tela)}"[^>]*>', html)
        assert links, tela
        for a in links:
            assert f'href="/app/{tela}"' in a, a
            assert "onclick=" not in a, a   # sem o navigate() do painel antigo
