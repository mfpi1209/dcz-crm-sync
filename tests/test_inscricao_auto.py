"""Inscrição - Automática: proxy autenticado (routes/inscricao_auto.py) e registro da página."""
import json
from pathlib import Path

import pytest
import requests
from flask import Flask

import routes.inscricao_auto as ia

RAIZ = Path(__file__).resolve().parent.parent


class _Resp:
    def __init__(self, status=200, corpo=b'{"ok":true}', headers=None):
        self.status_code = status
        self.content = corpo
        self.headers = headers or {"Content-Type": "application/json; charset=utf-8"}


class _Cur:
    def __init__(self, paginas):
        self.paginas = paginas

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        self.params = params

    def fetchall(self):
        return [(p,) for p in self.paginas]


class _Conn:
    def __init__(self, paginas):
        self.paginas = paginas

    def cursor(self):
        return _Cur(self.paginas)

    def close(self):
        pass


@pytest.fixture
def ambiente(monkeypatch):
    """App só com o blueprint; requests.request gravado em `chamadas`."""
    monkeypatch.setenv("INSCRICAO_API_URL", "http://servico:8788/")
    monkeypatch.setenv("INSCRICAO_API_TOKEN", "tok-painel")
    monkeypatch.delenv("INSCRICAO_API_TIMEOUT_S", raising=False)
    chamadas = []
    resposta = {"r": _Resp()}

    def fake_request(metodo, url, headers=None, data=None, timeout=None):
        chamadas.append({"metodo": metodo, "url": url, "headers": headers, "data": data, "timeout": timeout})
        r = resposta["r"]
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(ia._requests, "request", fake_request)
    paginas = {"p": set()}
    monkeypatch.setattr(ia, "get_conn", lambda: _Conn(paginas["p"]))

    app = Flask("t")
    app.secret_key = "x"
    app.register_blueprint(ia.inscricao_auto_bp)
    return app, chamadas, resposta, paginas


def _cliente(app, *, role="viewer", uid=7, username="maria.silva@eduit.com.br"):
    c = app.test_client()
    with c.session_transaction() as s:
        s["authenticated"] = True
        s["role"] = role
        s["user_id"] = uid
        s["username"] = username
    return c


# ── autenticação e papel ─────────────────────────────────────────────────

def test_sem_login_401(ambiente):
    app, chamadas, *_ = ambiente
    assert app.test_client().get("/api/inscricao-auto/orq/health").status_code == 401
    assert chamadas == []


def test_sem_permissao_403(ambiente):
    app, chamadas, _, paginas = ambiente
    paginas["p"] = {"dashboard"}
    assert _cliente(app).get("/api/inscricao-auto/orq/health").status_code == 403
    assert chamadas == []


def test_operar_le_e_repassa_cabecalhos(ambiente):
    app, chamadas, _, paginas = ambiente
    paginas["p"] = {"inscricao_automatica"}
    r = _cliente(app).get("/api/inscricao-auto/orq/fila?limite=300&status=na_fila,executando")
    assert r.status_code == 200
    c = chamadas[0]
    assert c["metodo"] == "GET"
    assert c["url"] == "http://servico:8788/api/orq/fila?limite=300&status=na_fila,executando"
    assert c["headers"]["X-Painel-Token"] == "tok-painel"
    assert c["headers"]["X-Inscricao-Papel"] == "operar"
    assert c["headers"]["X-Usuario"] == "Maria Silva"
    assert "Content-Type" not in c["headers"]
    assert c["timeout"] == 20


def test_admin_do_painel_e_login_de_emergencia_administram(ambiente):
    app, chamadas, *_ = ambiente
    _cliente(app, role="admin").get("/api/inscricao-auto/status")
    _cliente(app, role="viewer", uid=0).get("/api/inscricao-auto/status")
    assert [c["headers"]["X-Inscricao-Papel"] for c in chamadas] == ["admin", "admin"]


def test_sub_permissao_admin_administra(ambiente):
    app, chamadas, _, paginas = ambiente
    paginas["p"] = {"inscricao_automatica_admin"}
    r = _cliente(app).put("/api/inscricao-auto/orq/config", data=json.dumps({"fila": {"concorrencia": 1}}))
    assert r.status_code == 200
    assert chamadas[0]["headers"]["X-Inscricao-Papel"] == "admin"


@pytest.mark.parametrize("metodo,rota", [
    ("PUT", "campanha"),
    ("PUT", "atendente"),
    ("DELETE", "atendente"),
    ("POST", "atendente/testar"),
    ("PUT", "polos"),
    ("PUT", "polos/google"),
    ("PUT", "afiliados"),
    ("PUT", "afiliados/chave"),
    ("POST", "crm/token"),
    ("PUT", "crm/modo"),
    ("PUT", "manutencao/ia"),
    ("POST", "manutencao/aplicar"),
    ("POST", "orq/automacao/ligar"),
    ("POST", "orq/automacao/desligar"),
    ("PUT", "orq/config"),
    ("POST", "orq/engines/api/reset"),
    ("PUT", "orq/siaa/sessao"),
    ("POST", "orq/conferencia/siaa"),
    ("POST", "orq/agente/verificar"),
    ("POST", "orq/crm1/retorno/reenviar"),
    ("POST", "orq/crm1/retorno/devolver/J000001"),
    ("PUT", "orq/simulacao"),
    ("POST", "orq/inscricao/simulada"),
])
def test_operar_nao_chama_rota_de_admin(ambiente, metodo, rota):
    app, chamadas, _, paginas = ambiente
    paginas["p"] = {"inscricao_automatica"}
    r = _cliente(app).open(f"/api/inscricao-auto/{rota}", method=metodo, data="{}")
    assert r.status_code == 403
    assert chamadas == []


@pytest.mark.parametrize("metodo,rota", [
    ("POST", "cupom/validar"),
    ("POST", "polos/testar"),
    ("POST", "manutencao/diagnosticos"),
    ("POST", "orq/fila/J000123/reprocessar"),
    ("POST", "orq/fila/J000123/cancelar"),
    ("GET", "orq/relatorio/inscricoes"),
    ("GET", "orq/conferencia/siaa"),
    ("GET", "inscricoes/0f8fad5b-d9cb-469f-a165-70867728950e"),
    # abas Painel de inscrições, Integração CRM, Afiliados e Manutenção (leitura)
    ("GET", "painel"),
    ("GET", "crm"),
    ("GET", "afiliados"),
    ("GET", "manutencao"),
    ("GET", "manutencao/diagnosticos/0f8fad5b-d9cb-469f-a165-70867728950e"),
])
def test_operar_pode(ambiente, metodo, rota):
    app, chamadas, _, paginas = ambiente
    paginas["p"] = {"inscricao_automatica"}
    r = _cliente(app).open(f"/api/inscricao-auto/{rota}", method=metodo, data="{}" if metodo != "GET" else None)
    assert r.status_code == 200
    assert len(chamadas) == 1


def test_inscricao_real_exige_admin(ambiente):
    app, chamadas, _, paginas = ambiente
    paginas["p"] = {"inscricao_automatica"}
    c = _cliente(app)
    assert c.post("/api/inscricao-auto/inscricoes", data=json.dumps({"modo": "real"})).status_code == 403
    assert chamadas == []
    for modo in ("validar", "ensaio"):
        assert c.post("/api/inscricao-auto/inscricoes", data=json.dumps({"modo": modo})).status_code == 200
    assert len(chamadas) == 2
    paginas["p"] = {"inscricao_automatica_admin"}
    assert c.post("/api/inscricao-auto/inscricoes", data=json.dumps({"modo": "real"})).status_code == 200


def test_rota_fora_da_lista_404_mesmo_para_admin(ambiente):
    app, chamadas, *_ = ambiente
    c = _cliente(app, role="admin")
    for metodo, rota in [("POST", "crm/inscricoes"), ("GET", "crm/inscricoes/abc"), ("POST", "orq/inscricao"),
                         ("GET", "orq/fila/J1"), ("DELETE", "campanha"), ("GET", "../../etc/passwd")]:
        assert c.open(f"/api/inscricao-auto/{rota}", method=metodo, data="{}").status_code == 404, rota
    assert chamadas == []


def test_nao_get_vai_sempre_com_json(ambiente):
    app, chamadas, *_ = ambiente
    c = _cliente(app, role="admin")
    # sem corpo: manda {} e Content-Type JSON (o serviço devolve 415 sem ele)
    c.post("/api/inscricao-auto/orq/automacao/ligar")
    c.post("/api/inscricao-auto/orq/fila/J000001/reprocessar?force=true", data='{"x":1}', content_type="text/plain")
    assert chamadas[0]["headers"]["Content-Type"] == "application/json"
    assert chamadas[0]["data"] == b"{}"
    assert chamadas[1]["headers"]["Content-Type"] == "application/json"
    assert chamadas[1]["url"].endswith("/api/orq/fila/J000001/reprocessar?force=true")
    assert chamadas[1]["data"] == b'{"x":1}'


def test_json_invalido_400(ambiente):
    app, chamadas, *_ = ambiente
    r = _cliente(app, role="admin").post("/api/inscricao-auto/cupom/validar", data="{nao é json")
    assert r.status_code == 400
    assert chamadas == []


def test_status_do_servico_passa_direto(ambiente):
    app, _, resposta, _ = ambiente
    resposta["r"] = _Resp(409, b'{"ok":false,"erro":"ocupado"}')
    r = _cliente(app, role="admin").post("/api/inscricao-auto/inscricoes", data='{"modo":"ensaio"}')
    assert r.status_code == 409
    assert r.get_json()["erro"] == "ocupado"


def test_401_do_servico_vira_502(ambiente):
    """401 do serviço = token do painel errado; repassar 401 mandaria o usuário para o /login."""
    app, _, resposta, _ = ambiente
    resposta["r"] = _Resp(401, b'{"erro":"Acesso negado"}')
    r = _cliente(app, role="admin").get("/api/inscricao-auto/status")
    assert r.status_code == 502
    assert "INSCRICAO_API_TOKEN" in r.get_json()["erro"]


def test_csv_repassado_com_content_disposition(ambiente):
    app, chamadas, resposta, paginas = ambiente
    paginas["p"] = {"inscricao_automatica"}
    resposta["r"] = _Resp(200, "a;b\n1;2\n".encode(), {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": 'attachment; filename="inscricoes-siaa.csv"',
    })
    r = _cliente(app).get("/api/inscricao-auto/orq/relatorio/inscricoes.csv?de=2026-11-01")
    assert r.status_code == 200
    assert r.headers["Content-Type"].startswith("text/csv")
    assert r.headers["Content-Disposition"] == 'attachment; filename="inscricoes-siaa.csv"'
    assert r.data == b"a;b\n1;2\n"
    assert chamadas[0]["url"].endswith("/api/orq/relatorio/inscricoes.csv?de=2026-11-01")


@pytest.mark.parametrize("erro,status", [
    (requests.exceptions.Timeout(), 504),
    (requests.exceptions.ConnectionError(), 503),
    (RuntimeError("x"), 502),
])
def test_erros_de_rede(ambiente, erro, status):
    app, _, resposta, _ = ambiente
    resposta["r"] = erro
    assert _cliente(app, role="admin").get("/api/inscricao-auto/orq/health").status_code == status


def test_timeout_longo_so_nas_rotas_lentas(ambiente):
    app, chamadas, *_ = ambiente
    c = _cliente(app, role="admin")
    c.post("/api/inscricao-auto/orq/agente/verificar?corrigir=true")
    c.post("/api/inscricao-auto/atendente/testar")
    c.get("/api/inscricao-auto/orq/config")
    # a conferência no SIAA responde 202 na hora (roda no serviço): timeout padrão
    c.post("/api/inscricao-auto/orq/conferencia/siaa")
    c.get("/api/inscricao-auto/orq/conferencia/siaa")
    assert [x["timeout"] for x in chamadas] == [60, 60, 20, 20, 20]


def test_conferencia_siaa_202_e_status_repassados(ambiente):
    app, chamadas, resposta, paginas = ambiente
    resposta["r"] = _Resp(202, b'{"ok":true,"emAndamento":true,"desde":"2026-11-26T10:00:00.000Z"}')
    r = _cliente(app, role="admin").post("/api/inscricao-auto/orq/conferencia/siaa")
    assert r.status_code == 202 and r.get_json()["emAndamento"] is True
    resposta["r"] = _Resp(409, b'{"ok":false,"motivo":"conferencia em andamento"}')
    r = _cliente(app, role="admin").post("/api/inscricao-auto/orq/conferencia/siaa")
    assert r.status_code == 409 and r.get_json()["motivo"]
    paginas["p"] = {"inscricao_automatica"}
    resposta["r"] = _Resp(200, b'{"ok":true,"emAndamento":false,"ultima":{"ok":true,"contagem":{"confirmado":1}}}')
    r = _cliente(app).get("/api/inscricao-auto/orq/conferencia/siaa")
    assert r.status_code == 200 and r.get_json()["ultima"]["contagem"]["confirmado"] == 1


def test_sem_token_nao_manda_cabecalho(ambiente, monkeypatch):
    app, chamadas, *_ = ambiente
    monkeypatch.setenv("INSCRICAO_API_TOKEN", "")
    _cliente(app, role="admin").get("/api/inscricao-auto/status")
    assert "X-Painel-Token" not in chamadas[0]["headers"]


def test_usuario_so_latin1(ambiente):
    app, chamadas, *_ = ambiente
    _cliente(app, role="admin", username="joão🎉.souza").get("/api/inscricao-auto/status")
    assert chamadas[0]["headers"]["X-Usuario"] == "João Souza"
    chamadas[0]["headers"]["X-Usuario"].encode("latin-1")


def test_url_padrao_rede_interna(monkeypatch):
    monkeypatch.delenv("INSCRICAO_API_URL", raising=False)
    assert ia._base_url() == "http://banco-inscricao:8788"


# ── registro da página nos lugares do painel ─────────────────────────────

def test_pagina_registrada_em_todos_os_lugares():
    from helpers import ALL_PAGES
    assert "inscricao_automatica" in ALL_PAGES and "inscricao_automatica_admin" in ALL_PAGES
    app_src = (RAIZ / "app.py").read_text(encoding="utf-8")
    assert "app.register_blueprint(inscricao_auto_bp)" in app_src
    assert '"inscricao_automatica_admin" in pages' in app_src
    utils = (RAIZ / "static/js/utils.js").read_text(encoding="utf-8")
    assert "'inscricao_automatica'" in utils and "inscricao_automatica: 'Inscrição - Automática'" in utils
    assert "loadInscricaoAutomatica()" in utils
    sidebar = (RAIZ / "templates/partials/_sidebar.html").read_text(encoding="utf-8")
    assert "nav_can('inscricao_automatica')" in sidebar and 'data-page="inscricao_automatica"' in sidebar
    index = (RAIZ / "templates/index.html").read_text(encoding="utf-8")
    assert "partials/_inscricao_automatica.html" in index
    assert "js/inscricao_automatica.js') }}?v={{_v}}" in index
    config = (RAIZ / "static/js/config.js").read_text(encoding="utf-8")
    assert "inscricao_automatica_admin: 'Inscrição - Automática · administrar'" in config
    assert "'inscricao_automatica', 'inscricao_automatica_admin'," in config
    partial = (RAIZ / "templates/partials/_inscricao_automatica.html").read_text(encoding="utf-8")
    assert 'id="page-inscricao_automatica" class="hidden"' in partial


def test_abas_agrupadas_numa_linha_e_secoes_na_pagina():
    """6 abas (grupos) numa linha; cada seção da interface do serviço fica num grupo (sub-aba)."""
    import re as _re
    partial = (RAIZ / "templates/partials/_inscricao_automatica.html").read_text(encoding="utf-8")
    grupos = _re.findall(r'id="insa-tab-([a-z]+)" data-grupo=', partial)
    assert grupos == ["visao", "fila", "nova", "relatorios", "manutencao", "config"]
    js = (RAIZ / "static/js/inscricao_automatica.js").read_text(encoding="utf-8")
    secoes = ["visao", "fila", "nova", "painel", "cadastros", "afiliados", "siaa",
              "agente", "manutencao", "config", "eventos", "simulacao"]
    assert "const PAINEIS = { visao, fila, nova, painel, cadastros, afiliados, siaa, agente, manutencao, config, eventos, simulacao };" in js
    bloco = js[js.index("const GRUPOS = {"):js.index("};", js.index("const GRUPOS = {"))]
    for sec in secoes:
        assert f'id="insa-sec-{sec}"' in partial, sec
        assert f"'{sec}'" in bloco, f"seção {sec} fora de GRUPOS"
    # Integração CRM antiga (POST /api/crm/inscricoes) saiu da tela: o crm1 entra pelo webhook
    assert 'id="insa-sec-crm"' not in partial


def test_todas_as_rotas_que_a_tela_chama_estao_liberadas():
    """Cada chamar('<rota>') da página bate com alguma regra de ROTAS (sem a query)."""
    import re as _re
    js = (RAIZ / "static/js/inscricao_automatica.js").read_text(encoding="utf-8")
    brutas = _re.findall(r"chamar\(['`]([^'`?]+)", js)
    # interpolações viram exemplos reais: ${id} de job (J000001), de execução/diagnóstico (uuid), motor
    exemplos = {"id": "J000001", "encodeURIComponent(b.dataset.motor)": "api"}
    uuid = "0f8fad5b-d9cb-469f-a165-70867728950e"

    def trocar(m):
        expr = m.group(1)
        if expr in exemplos:
            return exemplos[expr]
        return "J000001" if "acao" in expr else ""

    rotas = set()
    for b in brutas:
        b = _re.sub(r"\$\{[^}]*$", "", b)  # ${cond ? '?x=1' : ''} cortado no '?': é só query
        if b.startswith("inscricoes/${") or b.startswith("manutencao/diagnosticos/${"):
            rotas.add(b.split("${")[0] + uuid)
        elif "${acao}" in b:
            rotas.update(b.replace("${id}", "J000001").replace("${acao}", x).split("${")[0] for x in ("reprocessar", "cancelar"))
        else:
            rotas.add(_re.sub(r"\$\{([^}]*)\}", trocar, b))
    assert {"painel", "afiliados", "afiliados/chave", "manutencao",
            "manutencao/diagnosticos", "manutencao/aplicar", "manutencao/ia", "orq/conferencia/siaa"} <= rotas
    metodos = ["GET", "POST", "PUT", "DELETE"]
    for rota in rotas:
        assert any(ia.achar_rota(m, rota) for m in metodos), rota
