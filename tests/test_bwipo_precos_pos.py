"""Pós-Graduação no CRM (18/11): a planilha traz SKU_CRM; o produto de Pós do CRM
casa pelo SKU e só tem o preço atualizado (nunca cria). Sem HTTP, sem banco."""

from __future__ import annotations

import io
import os
import sys

import openpyxl
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from routes import bwipo_precos as bp  # noqa: E402
from openpyxl.reader.excel import load_workbook as _real_load_workbook  # noqa: E402


@pytest.fixture(autouse=True)
def _openpyxl_real(monkeypatch):
    monkeypatch.setattr(bp.openpyxl, "load_workbook", _real_load_workbook)
    monkeypatch.setattr(bp.time, "sleep", lambda s: None)


HEAD = ["PRODUTO", "CURSOS", "CERTIFICADORA", "ÁREA DE ATUAÇÃO", "CANAL", "DURAÇÃO", "PARCELAS",
        "PREÇO SIAA", "DESCONTO", "VALOR COM DESCONTO", "OBS", "COD_CURSO", "SKU_CRM", "STATUS_CRM"]
CERT = "Cruzeiro - Pós Graduação Ead"
RH6 = "ADMINISTRACAO DE RECURSOS HUMANOSPós6 meses"
RH9 = "ADMINISTRACAO DE RECURSOS HUMANOSPós9 meses"


def _row(curso, dur, parc, siaa, desc, sku, status="Ativo", canal="ESQUENTA BLACK"):
    return ["PÓS EAD", curso, CERT, "GESTÃO", canal, dur, parc, siaa, desc, round(siaa * (1 - desc), 3), "\xa0", "1", sku, status]


def _xlsx(rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(HEAD)
    for r in rows:
        ws.append(r)
    ws.append([None] * 13 + [None])
    ws.append(["Filtros aplicados:\nPRODUTO é PÓS EAD"] + [None] * 13)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


ROWS = [
    _row("Administração De Recursos Humanos", "6 meses", "12 parcelas", 209.9, 0.35, RH6),
    _row("Administração De Recursos Humanos", "9 meses", "15 parcelas", 169.9, 0.35, RH9),
    _row("Curso Velho", "6 meses", "12 parcelas", 100, 0, "CURSO VELHOPós6 meses", status="Inativo"),
    _row("Sem Sku", "6 meses", "12 parcelas", 100, 0, None),
]


def _prod(pid, sku, level="POST_GRADUATION", **cc):
    base = {"level": level, "mode": "EAD", "grau": None, "semester": None, "gradeUrl": None,
            "postSalePipelineId": "pipe", "channel": "Antigo", "discountPercent": 10.0,
            "pricingOptions": [{"price": 200.0, "months": 6, "channel": "Antigo", "installments": 12, "discountPercent": 10.0}]}
    base.update(cc)
    return {"id": pid, "name": "Administração De Recursos Humanos", "sku": sku, "description": "d",
            "price": "200.00", "unit": "matrícula", "type": "PRODUCT", "kind": "COURSE", "isActive": True,
            "courseConfig": base}


def test_parse_pos_agrupa_por_sku_e_separa_inativo_e_sem_sku():
    p = bp.parse_pos_workbook(_xlsx(ROWS))
    assert [c["sku"] for c in p["cursos"]] == [RH6, RH9]
    rh6 = p["cursos"][0]
    assert (rh6["curso"], rh6["duracao"], rh6["meses"], rh6["parcelas"]) == ("Administração De Recursos Humanos", "6 meses", 6, 12)
    cota = rh6["canais"]["ESQUENTA BLACK"]
    assert (float(cota["base"]), float(cota["desconto"]), float(cota["final"])) == (209.9, 35.0, 136.44)
    assert [i["sku"] for i in p["inativos"]] == ["CURSO VELHOPós6 meses"]
    assert p["sem_sku"] == ["Sem Sku · 6 meses"]
    assert [c["canal"] for c in p["canais"]] == ["ESQUENTA BLACK"]


def test_parse_pos_sem_coluna_sku_erro():
    wb = openpyxl.Workbook()
    wb.active.append(["CURSOS", "CANAL", "DURAÇÃO", "PREÇO SIAA", "DESCONTO", "VALOR COM DESCONTO"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    with pytest.raises(ValueError, match="SKU_CRM"):
        bp.parse_pos_workbook(buf)


def test_indice_pos_casa_sku_sem_acento_caixa_espaco_e_ignora_graduacao():
    prods = [_prod("a", "administracao de recursos humanosPos6 Meses"),
             _prod("g", RH9, level="GRADUATION")]
    idx = bp._pos_index(prods)
    assert idx[bp._sku_key(RH6)]["id"] == "a"
    assert bp._sku_key(RH9) not in idx


def test_apply_pos_atualiza_preco_e_cota_preservando_o_resto(monkeypatch):
    chamadas = []
    monkeypatch.setattr(bp, "bwipo_request", lambda m, p, body=None: chamadas.append((m, p, body)) or {})
    item = bp.parse_pos_workbook(_xlsx(ROWS))["cursos"][0]
    prod = _prod("a", RH6, extraCampo="fica")
    r = bp._apply_prices_pos(prod, item, {"ESQUENTA BLACK"})
    assert r == {"acao": "atualizado", "id": "a"}
    [(m, path, body)] = chamadas
    assert (m, path) == ("PUT", "/api/products/a")
    assert body["sku"] == RH6 and body["price"] == "209.90" and body["name"] == prod["name"]
    c = body["course"]
    assert c["level"] == "POST_GRADUATION" and c["semester"] is None and c["extraCampo"] == "fica"
    assert c["postSalePipelineId"] == "pipe"
    assert c["channel"] == "Esquenta Black" and c["discountPercent"] == 35.0
    opts = {o["channel"]: o for o in c["pricingOptions"]}
    assert opts["Esquenta Black"] == {"price": 209.9, "months": 6, "channel": "Esquenta Black",
                                      "installments": 12, "discountPercent": 35.0}
    assert opts["Antigo"]["price"] == 209.9      # cota não escolhida: só o preço base


def test_apply_pos_sem_cota_escolhida_erro(monkeypatch):
    monkeypatch.setattr(bp, "bwipo_request", lambda *a, **k: pytest.fail("não devia chamar o CRM"))
    item = bp.parse_pos_workbook(_xlsx(ROWS))["cursos"][0]
    with pytest.raises(bp.BwipoComercialError):
        bp._apply_prices_pos(_prod("a", RH6), item, set())


@pytest.fixture(autouse=True)
def _job_sincrono(monkeypatch):
    monkeypatch.setattr(bp, "_rodar_em_segundo_plano", lambda fn: fn())
    monkeypatch.setattr(bp, "_POS_JOBS", {})


def _status(client, r):
    body = r.get_json()
    assert r.status_code == 200, body
    st = client.get(f"/api/bwipo-precos/pos/aplicar/{body['job_id']}")
    assert st.status_code == 200
    return st.get_json()


@pytest.fixture
def client():
    from flask import Flask
    app = Flask(__name__)
    app.secret_key = "t"
    app.register_blueprint(bp.bwipo_precos_bp)
    c = app.test_client()
    with c.session_transaction() as s:
        s["authenticated"] = True
        s["role"] = "admin"
        s["username"] = "teste"
    return c


def test_rota_preview_pos(client, monkeypatch):
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("a", RH6)])
    r = client.post("/api/bwipo-precos/pos/preview", data={"file": (_xlsx(ROWS), "p.xlsx")},
                    content_type="multipart/form-data")
    body = r.get_json()
    assert r.status_code == 200, body
    assert (body["cursos"], body["atualizar"], body["sem_produto"]) == (2, 1, 1)
    acoes = {l["sku"]: l["acao"] for l in body["linhas"]}
    assert acoes == {RH6: "atualizar", RH9: "sem_produto"}
    assert body["inativos"] == 1 and body["sem_sku"] == ["Sem Sku · 6 meses"]
    assert body["amostra"]["level"] == "POST_GRADUATION"


def test_rota_aplicar_pos_so_atualiza_e_nunca_cria(client, monkeypatch):
    chamadas = []
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("a", RH6)])
    monkeypatch.setattr(bp, "bwipo_request", lambda m, p, body=None: chamadas.append((m, p)) or {})
    r = client.post("/api/bwipo-precos/pos/aplicar", data={
        "file": (_xlsx(ROWS), "p.xlsx"), "canais": '["ESQUENTA BLACK"]',
    }, content_type="multipart/form-data")
    body = _status(client, r)
    assert chamadas == [("PUT", "/api/products/a")]
    assert body["status"] == "fim" and (body["feitos"], body["total"]) == (2, 2)
    assert {x["sku"]: x["acao"] for x in body["resultados"]} == {RH6: "atualizado", RH9: "sem_produto"}
    assert body["resumo"] == {"cursos": 2, "ok": 1, "erro": 0, "sem_produto": 1, "inativos": 1}


def test_rota_aplicar_pos_teste_em_um_sku(client, monkeypatch):
    chamadas = []
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("a", RH6), _prod("b", RH9)])
    monkeypatch.setattr(bp, "bwipo_request", lambda m, p, body=None: chamadas.append((m, p)) or {})
    r = client.post("/api/bwipo-precos/pos/aplicar", data={
        "file": (_xlsx(ROWS), "p.xlsx"), "canais": '["ESQUENTA BLACK"]', "so_sku": RH9,
    }, content_type="multipart/form-data")
    assert _status(client, r)["status"] == "fim" and chamadas == [("PUT", "/api/products/b")]
    r = client.post("/api/bwipo-precos/pos/aplicar", data={
        "file": (_xlsx(ROWS), "p.xlsx"), "canais": '["ESQUENTA BLACK"]', "so_sku": "NAO EXISTE",
    }, content_type="multipart/form-data")
    assert r.status_code == 400


def test_rota_aplicar_pos_exige_cota(client, monkeypatch):
    monkeypatch.setattr(bp, "_list_products", lambda: pytest.fail("não devia ler o CRM"))
    r = client.post("/api/bwipo-precos/pos/aplicar", data={"file": (_xlsx(ROWS), "p.xlsx"), "canais": "[]"},
                    content_type="multipart/form-data")
    assert r.status_code == 400


def _post_todos(client):
    return client.post("/api/bwipo-precos/pos/aplicar", data={
        "file": (_xlsx(ROWS), "p.xlsx"), "canais": '["ESQUENTA BLACK"]',
    }, content_type="multipart/form-data")


def test_job_tenta_de_novo_erro_passageiro(client, monkeypatch):
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("a", RH6), _prod("b", RH9)])
    falhas = {"a": [TimeoutError("timed out"), bp.BwipoComercialError("lento", 504)]}
    chamadas = []

    def fake(m, p, body=None):
        chamadas.append(p)
        pid = p.rsplit("/", 1)[-1]
        if falhas.get(pid):
            raise falhas[pid].pop(0)
        return {}

    monkeypatch.setattr(bp, "bwipo_request", fake)
    body = _status(client, _post_todos(client))
    assert chamadas == ["/api/products/a"] * 3 + ["/api/products/b"]
    assert body["resumo"]["ok"] == 2 and body["resumo"]["erro"] == 0


def test_job_erro_inesperado_vira_linha_e_segue(client, monkeypatch):
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("a", RH6), _prod("b", RH9)])

    def fake(m, p, body=None):
        if p.endswith("/a"):
            raise ConnectionResetError("caiu")
        return {}

    monkeypatch.setattr(bp, "bwipo_request", fake)
    body = _status(client, _post_todos(client))
    linhas = {x["sku"]: x for x in body["resultados"]}
    assert linhas[RH6]["acao"] == "erro" and "ConnectionResetError" in linhas[RH6]["erro"]
    assert linhas[RH9]["acao"] == "atualizado"
    assert (body["resumo"]["ok"], body["resumo"]["erro"]) == (1, 1)


def test_job_erro_4xx_nao_tenta_de_novo(client, monkeypatch):
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("a", RH6)])
    chamadas = []

    def fake(m, p, body=None):
        chamadas.append(p)
        raise bp.BwipoComercialError("campo inválido", 400)

    monkeypatch.setattr(bp, "bwipo_request", fake)
    body = _status(client, _post_todos(client))
    assert chamadas == ["/api/products/a"] and body["resumo"]["erro"] == 1


def test_job_falha_ao_ler_crm_fica_no_status(client, monkeypatch):
    def quebra():
        raise bp.BwipoComercialError("sem token", 503)

    monkeypatch.setattr(bp, "_list_products", quebra)
    body = _status(client, _post_todos(client))
    assert body["status"] == "erro" and "sem token" in body["erro"]


def test_um_job_por_vez_e_status_desconhecido(client, monkeypatch):
    bp._POS_JOBS["x"] = {"status": "rodando"}
    assert _post_todos(client).status_code == 409
    assert client.get("/api/bwipo-precos/pos/aplicar/nao-existe").status_code == 404


def test_status_desde_devolve_so_o_que_falta(client, monkeypatch):
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("a", RH6)])
    monkeypatch.setattr(bp, "bwipo_request", lambda m, p, body=None: {})
    job_id = _post_todos(client).get_json()["job_id"]
    r = client.get(f"/api/bwipo-precos/pos/aplicar/{job_id}?desde=1").get_json()
    assert [x["sku"] for x in r["resultados"]] == [RH9]
