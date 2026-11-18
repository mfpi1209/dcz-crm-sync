"""Variantes "Cst Em X" / "X (4.0)" / "X (4.0I)" (18/11): não criam produto no CRM,
casam com o produto "X" que já existe, e as que sobraram no CRM podem ser
apagadas. Sem HTTP, sem banco."""

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


def _xlsx(linhas):
    """linhas: (curso, grau, sub, preco)."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["CÓDIGO", "CURSOS", "DURAÇÃO", "GRAU", "SUBMODALIDADE", "CANAL",
               "PREÇO SIAA", "DESCONTO 1 SEMESTRE", "VALOR COM DESCONTO"])
    for curso, grau, sub, preco in linhas:
        ws.append(["1", curso, 5, grau, sub, "BALCÃO ESTRATÉGICO", preco, 10, preco * 0.9])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def _prod(pid, nome, grau="Tecnólogo", level="GRADUATION", sku=None):
    """Sem sku: o formato que _sku() gera, ou seja, produto criado pelo painel."""
    return {"id": pid, "name": nome, "sku": sku if sku is not None else nome.replace(" ", "") + "EAD",
            "price": "100.00",
            "courseConfig": {"level": level, "grau": grau, "pricingOptions": []}}


ADS = "Análise E Desenvolvimento De Sistemas"


@pytest.mark.parametrize("nome", [
    "Cst Em Análise E Desenvolvimento De Sistemas",
    "Cst Em Análise E Desenvolvimento De Sistemas (4.0I)",
    "Análise E Desenvolvimento De Sistemas  (4.0)",
    "Análise E Desenvolvimento De Sistemas (4.0I Mais )",
    "CST EM Análise E Desenvolvimento De Sistemas (Ead)",
])
def test_nome_base_tira_prefixo_e_sufixo(nome):
    assert bp.nome_base(nome) == ADS
    assert bp.eh_variante(nome)


def test_nome_normal_nao_e_variante():
    assert bp.nome_base(ADS) == ADS
    assert not bp.eh_variante(ADS)
    assert not bp.eh_variante("Engenharia Civil")


def test_parse_planilha_com_x_e_variantes_fica_so_x():
    parsed = bp.parse_workbook(_xlsx([
        (f"Cst Em {ADS}", "Tecnólogo", "Digital", 204),
        (ADS, "Tecnólogo", "Digital", 204.01),
        (f"Cst Em {ADS} (4.0I)", "Tecnólogo", "Ao Vivo", 237.10),
    ]))
    assert [(c["curso"], float(c["base"]["base"])) for c in parsed["cursos"]] == [(ADS, 204.01)]
    assert sorted(i["curso"] for i in parsed["ignorados"]) == [f"Cst Em {ADS}", f"Cst Em {ADS} (4.0I)"]


def test_parse_so_variantes_prefere_a_sem_sufixo_e_usa_nome_base():
    parsed = bp.parse_workbook(_xlsx([
        ("Cst Em Estética E Cosmética (4.0)", "Tecnólogo", "Semipresencial", 347.40),
        ("Cst Em Estética E Cosmética", "Tecnólogo", "Digital", 300),
    ]))
    [item] = parsed["cursos"]
    assert item["curso"] == "Estética E Cosmética"
    assert item["variante"] is True
    assert item["nome_planilha"] == "Cst Em Estética E Cosmética"
    assert float(item["base"]["base"]) == 300
    assert [i["curso"] for i in parsed["ignorados"]] == ["Cst Em Estética E Cosmética (4.0)"]


def test_indice_casa_variante_com_produto_existente_e_prefere_o_original():
    idx = bp._grad_index([
        _prod("cst", f"Cst Em {ADS}"),
        _prod("orig", ADS),
        _prod("v40", f"Cst Em {ADS} (4.0I)"),
    ])
    assert idx[(bp._fold(ADS), bp._fold("Tecnólogo"))]["id"] == "orig"


def test_variante_sem_produto_no_crm_e_pulada_nunca_criada():
    item = {"curso": "Estética E Cosmética", "variante": True}
    assert bp._acao_item(item, None) == "pular"
    assert bp._acao_item({"curso": ADS, "variante": False}, None) == "criar"
    assert bp._acao_item(item, _prod("x", "Estética E Cosmética")) == "atualizar"


def test_duplicatas_so_variantes_com_original_no_mesmo_grau():
    prods = [
        _prod("orig", ADS),
        _prod("cst", f"Cst Em {ADS}"),
        _prod("v40", f"Cst Em {ADS} (4.0I)"),
        _prod("sozinha", "Engenharia Ambiental  (4.0)", grau="Licenciatura"),
        _prod("outro_grau", f"Cst Em {ADS}", grau="Bacharelado"),
        _prod("pos", f"Cst Em {ADS}", level="POSTGRADUATE"),
        _prod("normal", "Engenharia Civil", grau="Bacharelado"),
    ]
    d = bp._duplicatas(prods)
    assert sorted(x["id"] for x in d["apagar"]) == ["cst", "v40"]
    assert all(x["mantem_id"] == "orig" and x["mantem_nome"] == ADS for x in d["apagar"])
    assert sorted(x["id"] for x in d["sem_base"]) == ["outro_grau", "sozinha"]


def test_apagar_so_apaga_id_que_ainda_e_duplicata(monkeypatch):
    prods = [_prod("orig", ADS), _prod("cst", f"Cst Em {ADS}"), _prod("sozinha", "Agronomia (4.0)", grau="Bacharelado")]
    chamadas = []

    def fake(method, path, body=None):
        chamadas.append((method, path))
        return {}

    monkeypatch.setattr(bp, "bwipo_request", fake)
    monkeypatch.setattr(bp.time, "sleep", lambda s: None)
    r = bp._apagar_duplicatas(["cst", "orig", "sozinha", "nao_existe"], prods)
    assert chamadas == [("DELETE", "/api/products/cst")]
    assert [x["id"] for x in r if x["ok"]] == ["cst"]
    assert sorted(x["id"] for x in r if not x["ok"]) == ["nao_existe", "orig", "sozinha"]


def test_apagar_erro_do_crm_vira_linha_de_erro(monkeypatch):
    prods = [_prod("orig", ADS), _prod("cst", f"Cst Em {ADS}")]

    def fake(method, path, body=None):
        raise bp.BwipoComercialError("em uso", 409)

    monkeypatch.setattr(bp, "bwipo_request", fake)
    monkeypatch.setattr(bp.time, "sleep", lambda s: None)
    [r] = bp._apagar_duplicatas(["cst"], prods)
    assert r["ok"] is False and "em uso" in r["erro"]


@pytest.fixture
def client(monkeypatch):
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


def test_aplicar_variante_sem_produto_pula_e_nao_cria(client, monkeypatch):
    chamadas = []
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("orig", ADS)])
    monkeypatch.setattr(bp, "bwipo_request", lambda m, p, body=None: chamadas.append((m, p)) or {"product": {"id": "x"}})
    monkeypatch.setattr(bp, "get_conn", lambda: type("C", (), {"close": lambda self: None})())
    monkeypatch.setattr(bp, "_save_snapshot", lambda *a, **k: 7)
    monkeypatch.setattr(bp.time, "sleep", lambda s: None)
    xlsx = _xlsx([
        (f"Cst Em {ADS}", "Tecnólogo", "Digital", 204),
        ("Cst Em Podologia", "Tecnólogo", "Digital", 300),
    ])
    r = client.post("/api/bwipo-precos/aplicar", data={
        "file": (xlsx, "p.xlsx"), "canais": '["BALCÃO ESTRATÉGICO"]',
    }, content_type="multipart/form-data")
    body = r.get_json()
    assert r.status_code == 200, body
    assert [m for m, _ in chamadas] == ["PUT"]          # só atualiza ADS; nada de POST
    assert chamadas[0][1] == "/api/products/orig"
    por_curso = {x["curso"]: x["acao"] for x in body["resultados"]}
    assert por_curso == {ADS: "atualizado", "Podologia": "pulado"}
    assert body["resumo"]["pulado"] == 1 and body["resumo"]["erro"] == 0


def test_rota_apagar_exige_ids_e_rele_o_crm(client, monkeypatch):
    assert client.post("/api/bwipo-precos/duplicatas/apagar", json={}).status_code == 400
    chamadas = []
    monkeypatch.setattr(bp, "_list_products", lambda: [_prod("orig", ADS), _prod("cst", f"Cst Em {ADS}")])
    monkeypatch.setattr(bp, "bwipo_request", lambda m, p, body=None: chamadas.append((m, p)) or {})
    monkeypatch.setattr(bp.time, "sleep", lambda s: None)
    r = client.post("/api/bwipo-precos/duplicatas/apagar", json={"ids": ["cst", "orig"]}).get_json()
    assert chamadas == [("DELETE", "/api/products/cst")]
    assert (r["apagados"], r["erros"]) == (1, 1)
    lista = client.get("/api/bwipo-precos/duplicatas").get_json()
    assert [x["id"] for x in lista["apagar"]] == ["cst"]


def test_variante_que_nao_foi_criada_pelo_painel_nao_entra_em_apagar():
    # "Pedagogia (4.0)" legítimo, de antes da planilha ruim: SKU de outro formato.
    prods = [
        _prod("orig", "Pedagogia", grau="Licenciatura", sku="Pedagogia EAD"),
        _prod("legit", "Pedagogia (4.0)", grau="Licenciatura", sku="PED-SEMI-01"),
        _prod("painel", "Pedagogia  (4.0)", grau="Licenciatura", sku="Pedagogia(4.0)HIBRIDO"),
    ]
    d = bp._duplicatas(prods)
    assert [x["id"] for x in d["apagar"]] == ["painel"]
    assert [x["id"] for x in d["outros"]] == ["legit"]
    assert bp._apagar_duplicatas(["legit"], prods)[0]["ok"] is False


def test_indice_nao_casa_com_variante_quando_falta_o_original():
    # CRM só tem "X (4.0)"; planilha "X" não pode sobrescrever a variante.
    idx = bp._grad_index([_prod("v40", f"{ADS} (4.0)")])
    assert (bp._fold(ADS), bp._fold("Tecnólogo")) not in idx


def test_indice_casa_original_com_espaco_duplo():
    idx = bp._grad_index([_prod("orig", "Análise E  Desenvolvimento De Sistemas")])
    assert idx[(bp._fold(ADS), bp._fold("Tecnólogo"))]["id"] == "orig"


def test_original_com_espaco_duplo_conta_como_original_na_limpeza():
    prods = [_prod("orig", "Análise E  Desenvolvimento De Sistemas"), _prod("cst", f"Cst Em {ADS}")]
    assert [x["id"] for x in bp._duplicatas(prods)["apagar"]] == ["cst"]
