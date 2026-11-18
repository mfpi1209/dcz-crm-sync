"""Opção "Preço SIAA" do site e da IA (18/11): canal '__SIAA__' usa o PREÇO SIAA
da planilha, sem desconto, em vez do VALOR COM DESCONTO de um canal."""
import io

import openpyxl
import pytest

# O módulo carrega um .pyc de Python 3.14 (produção); em outra versão o teste é pulado.
ap = pytest.importorskip("routes.atualizar_preco_app", exc_type=ImportError)

HEAD = ["CURSO", "GRAU", "SUBMODALIDADE", "CANAL", "PREÇO SIAA", "% DESCONTO", "VALOR COM DESCONTO"]
ROWS = [
    ["Administração", "Bacharelado", "EAD", "BALCÃO ESTRATÉGICO", 410.5, 40, 246.3],
    ["Administração", "Bacharelado", "EAD", "APROVADOS", 410.5, 50, 205.25],
]


def _xlsx():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(HEAD)
    for r in ROWS:
        ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_sentinela_igual_ao_do_front():
    assert ap.SIAA_SENTINEL == "__SIAA__"


def test_ia_usa_preco_siaa():
    headers, rows = ap._read_xlsx_rows(_xlsx())
    canal_col = ap._find_col(headers, "CANAL")
    siaa_col = ap._find_col(headers, "PREÇO SIAA")
    idx = ap._build_grad_index_canal(rows, headers, ap.SIAA_SENTINEL, canal_col, siaa_col)
    assert set(idx.values()) == {410.5}
    idx_canal = ap._build_grad_index_canal(rows, headers, "APROVADOS", canal_col, siaa_col)
    assert set(idx_canal.values()) == {205.25}


def test_site_usa_preco_siaa():
    out = ap._build_canal_xlsx(_xlsx(), ap.SIAA_SENTINEL)
    ws = openpyxl.load_workbook(io.BytesIO(out)).active
    precos = {row[3] for row in ws.iter_rows(min_row=2, values_only=True)}
    assert precos == {410.5}
