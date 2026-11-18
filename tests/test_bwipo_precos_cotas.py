"""Cotas livres no CRM (17/11): nenhuma cota obrigatória; Estratégico só manda
quando escolhido. Sem HTTP, sem banco."""

from __future__ import annotations

import io
import os
import sys

import openpyxl
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from routes import bwipo_precos as bp  # noqa: E402

# tests/test_caminhos_siaa.py troca openpyxl.load_workbook por um stub no import;
# aqui precisamos do leitor real.
from openpyxl.reader.excel import load_workbook as _real_load_workbook  # noqa: E402


@pytest.fixture(autouse=True)
def _openpyxl_real(monkeypatch):
    monkeypatch.setattr(bp.openpyxl, "load_workbook", _real_load_workbook)

CANAIS = ["BALCÃO ESTRATÉGICO", "BALCÃO|COMERCIAL 10%PS/5%DS", "ESQUENTA BLACK"]


def _xlsx(canais):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["CÓDIGO", "CURSOS", "DURAÇÃO", "GRAU", "SUBMODALIDADE", "CANAL",
               "PREÇO SIAA", "DESCONTO 1 SEMESTRE", "VALOR COM DESCONTO"])
    for i, canal in enumerate(canais):
        ws.append(["1", "Administração", 8, "Bacharelado", "EAD", canal, 500, 10 + i * 5, 500 * (1 - (10 + i * 5) / 100)])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def _item(canais):
    return bp.parse_workbook(_xlsx(canais))["cursos"][0]


def test_parse_aceita_planilha_sem_estrategico():
    item = _item(CANAIS[1:])
    assert item["base"]["canal"] == "BALCÃO|COMERCIAL 10%PS/5%DS"
    assert float(item["base"]["base"]) == 500


def test_parse_prefere_estrategico_como_base_quando_existe():
    item = _item(CANAIS)
    assert item["base"]["canal"] == "BALCÃO ESTRATÉGICO"


def test_rows_para_subir_so_as_escolhidas_e_estrategico_primeiro():
    item = _item(CANAIS)
    rows = bp._rows_para_subir(item, {"ESQUENTA BLACK", "BALCÃO ESTRATÉGICO"})
    assert [r["canal"] for r in rows] == ["BALCÃO ESTRATÉGICO", "ESQUENTA BLACK"]
    rows = bp._rows_para_subir(item, {"ESQUENTA BLACK"})
    assert [r["canal"] for r in rows] == ["ESQUENTA BLACK"]


def test_apply_prices_sem_cota_escolhida_falha_antes_do_crm():
    item = _item(CANAIS)
    with pytest.raises(bp.BwipoComercialError):
        bp._apply_prices(None, item, set())


def test_apply_prices_canal_principal_e_o_primeiro_escolhido(monkeypatch):
    item = _item(CANAIS)
    chamadas = []

    def fake_request(method, path, body=None):
        chamadas.append((method, path, body))
        return {"product": {"id": "p1"}}

    monkeypatch.setattr(bp, "bwipo_request", fake_request)
    bp._apply_prices(None, item, {"ESQUENTA BLACK", "BALCÃO|COMERCIAL 10%PS/5%DS"})
    post = next(c for c in chamadas if c[0] == "POST")
    put = next(c for c in chamadas if c[0] == "PUT")
    assert post[2]["price"] == "500.00"
    canais = [o["channel"] for o in put[2]["course"]["pricingOptions"]]
    assert len(canais) == 2 and "Balcão Estratégico" not in canais
    assert put[2]["course"]["channel"] == canais[0]
