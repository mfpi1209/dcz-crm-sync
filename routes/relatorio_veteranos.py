import re
import logging
from io import BytesIO
from xml.etree import ElementTree as ET

import requests
from flask import Blueprint, request, session, send_file, jsonify
from openpyxl import Workbook

logger = logging.getLogger(__name__)
relatorio_veteranos_bp = Blueprint("relatorio_veteranos_bp", __name__)

GRID = "https://siaa.cruzeirodosul.edu.br/siaa_financeiro/secure/fin/wtesger01/XML/XMLgridRel1.jsp"
COMBO = "https://siaa.cruzeirodosul.edu.br/siaa_financeiro/secure/fin/wtesger01/XML/XMLcomboPolo.jsp"
HEADERS = {
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://siaa.cruzeirodosul.edu.br/siaa_financeiro/secure/fin/wtesger01/index.jsp",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0"
    ),
}
COLS = [
    "campus", "rgm", "nome", "fonere", "fonecom", "fonecel", "email",
    "curso", "serie", "qtddoc", "dtpag", "valorpgto", "valortit",
    "ende", "cidade", "estado", "cep",
]
ROTULOS = {
    "polo": "Polo", "campus": "Campus", "rgm": "RGM", "nome": "Nome",
    "fonere": "Fone Res.", "fonecom": "Fone Com.", "fonecel": "Fone Cel.",
    "email": "Email", "curso": "Curso", "serie": "Série", "qtddoc": "Qtd. Doc.",
    "dtpag": "Data Pagamento", "valorpgto": "Valor Pago", "valortit": "Valor Título",
    "ende": "Endereço", "cidade": "Cidade", "estado": "Estado", "cep": "CEP",
}


class SessaoExpirada(RuntimeError):
    pass


def extrair_cookie(raw: str) -> str:
    """Aceita cURL bash completo, header 'Cookie: ...' ou valor puro.
    Exige que contenha JSESSIONID."""
    m = re.search(r"-b\s+'([^']*)'", raw) or re.search(r'(?:-b|--cookie)\s+"([^"]*)"', raw)
    val = m.group(1) if m else raw
    m2 = re.search(r'[Cc]ookie:\s*(.+)', val)
    if m2:
        val = m2.group(1)
    val = re.sub(r"\s+", " ", val).strip()
    if "JSESSIONID" not in val:
        raise ValueError("cookie inválido (precisa conter JSESSIONID)")
    return val


def _listar_polos(cookie: str) -> list:
    r = requests.get(
        COMBO,
        params={"codEmpr": "12"},
        headers={**HEADERS, "Cookie": cookie},
        timeout=30,
        allow_redirects=False,
    )
    if r.status_code != 200:
        raise SessaoExpirada()
    txt = r.content.decode("iso-8859-1", "replace")
    return re.findall(r'<option\s+value="(\d+)"[^>]*>\s*(.*?)\s*</option>', txt, re.DOTALL)


def _baixar_polo(cookie: str, id_polo: str, ano: str) -> list:
    params = {
        "anoLeti": ano,
        "descEmpr": "GRADUAÇÃO EAD (CSED)",
        "codEmpr": "12",
        "idPolo": id_polo,
        "codCurso": "0",
    }
    r = requests.get(
        GRID,
        params=params,
        headers={**HEADERS, "Cookie": cookie},
        timeout=90,
        allow_redirects=False,
    )
    if r.status_code in (301, 302, 303, 307, 308) or "access_denied" in (r.headers.get("Location") or ""):
        raise SessaoExpirada()
    root = ET.fromstring(r.content.decode("iso-8859-1", "replace"))
    linhas = []
    for row in root.findall("row"):
        cells = row.findall("cell")
        linhas.append({
            COLS[i] if i < len(COLS) else f"c{i}": (c.text or "").strip()
            for i, c in enumerate(cells)
        })
    return linhas


def _gerar_excel(cookie: str, ano: str) -> bytes:
    polos = _listar_polos(cookie)
    wb = Workbook()
    ws = wb.active
    ws.title = "Veteranos"
    ordem = ["polo"] + COLS
    ws.append([ROTULOS.get(c, c) for c in ordem])
    for id_polo, nome in polos:
        for reg in _baixar_polo(cookie, id_polo, ano):
            reg["polo"] = nome
            ws.append([reg.get(c, "") for c in ordem])
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


@relatorio_veteranos_bp.route("/ti/relatorio-veteranos", methods=["POST"])
def gerar():
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "forbidden"}), 403
    ano = (request.form.get("ano") or "2026/2").strip()
    raw_cookie = request.form.get("cookie") or ""
    try:
        cookie = extrair_cookie(raw_cookie)
    except ValueError as e:
        return str(e), 400
    try:
        xlsx = _gerar_excel(cookie, ano)
    except SessaoExpirada:
        return "Sessão do SIAA expirada — pegue o cookie de novo e tente outra vez.", 409
    except Exception:
        logger.exception("Falha ao gerar relatório de veteranos (ano=%s)", ano)
        return "Erro interno ao gerar o relatório.", 500

    filename = f"relatorio_veteranos_{ano.replace('/', '-')}.xlsx"
    return send_file(
        BytesIO(xlsx),
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
