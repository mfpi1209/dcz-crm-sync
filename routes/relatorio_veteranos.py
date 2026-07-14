import re
import logging
from datetime import date
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
    """Levantada quando o SIAA nao aceita o cookie enviado.

    O atributo `debug` traz status_code, Location e um preview curto do body
    (sem cookie) — logado no servidor, nao exposto no response ao cliente.
    """

    def __init__(self, debug: dict | None = None):
        super().__init__("sessao_expirada")
        self.debug = debug or {}


def _resp_preview(r) -> dict:
    """Extrai informacoes seguras da resposta para diagnostico (sem cookie)."""
    try:
        body = r.content.decode("iso-8859-1", "replace")
    except Exception:
        body = ""
    return {
        "status_code": r.status_code,
        "location": r.headers.get("Location"),
        "content_type": r.headers.get("Content-Type"),
        "body_len": len(body),
        "body_preview": body[:400],
    }


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
    dbg = _resp_preview(r)
    logger.info("relatorio_veteranos: COMBO status=%s ct=%s len=%s", dbg["status_code"], dbg["content_type"], dbg["body_len"])
    if r.status_code != 200:
        logger.warning("relatorio_veteranos: COMBO nao-200 diag=%s", dbg)
        raise SessaoExpirada(debug={"step": "listar_polos", **dbg})
    txt = r.content.decode("iso-8859-1", "replace")
    polos = re.findall(r'<option\s+value="(\d+)"[^>]*>\s*(.*?)\s*</option>', txt, re.DOTALL)
    if not polos:
        # 200 mas sem <option> = provavelmente HTML de login, WAF ou pagina de erro
        logger.warning("relatorio_veteranos: COMBO sem <option> diag=%s", dbg)
        raise SessaoExpirada(debug={"step": "listar_polos_sem_polos", **dbg})
    return polos


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
    dbg = _resp_preview(r)
    if r.status_code in (301, 302, 303, 307, 308) or "access_denied" in (r.headers.get("Location") or ""):
        logger.warning("relatorio_veteranos: GRID polo=%s redirect diag=%s", id_polo, dbg)
        raise SessaoExpirada(debug={"step": "baixar_polo", "id_polo": id_polo, **dbg})
    if r.status_code != 200:
        logger.warning("relatorio_veteranos: GRID polo=%s nao-200 diag=%s", id_polo, dbg)
        raise SessaoExpirada(debug={"step": "baixar_polo", "id_polo": id_polo, **dbg})
    try:
        root = ET.fromstring(r.content.decode("iso-8859-1", "replace"))
    except ET.ParseError:
        logger.warning("relatorio_veteranos: GRID polo=%s xml invalido diag=%s", id_polo, dbg)
        raise SessaoExpirada(debug={"step": "baixar_polo_xml_invalido", "id_polo": id_polo, **dbg})
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


@relatorio_veteranos_bp.route("/ti/relatorio-veteranos/diag", methods=["POST"])
def diag():
    """Retorna diagnostico da chamada ao SIAA sem gerar Excel.

    Uso: mesmo form-encoded (`cookie`, opcional `ano`). Ideal pra rodar
    em producao e comparar com o local quando o resultado difere.
    Nao inclui o valor do cookie na resposta.
    """
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "forbidden"}), 403
    raw_cookie = request.form.get("cookie") or ""
    try:
        cookie = extrair_cookie(raw_cookie)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    out: dict = {"ok": True, "cookie_len": len(cookie)}
    # 1. Testa COMBO
    try:
        r = requests.get(
            COMBO,
            params={"codEmpr": "12"},
            headers={**HEADERS, "Cookie": cookie},
            timeout=30,
            allow_redirects=False,
        )
        out["combo"] = _resp_preview(r)
        if r.status_code == 200:
            txt = r.content.decode("iso-8859-1", "replace")
            polos = re.findall(r'<option\s+value="(\d+)"[^>]*>\s*(.*?)\s*</option>', txt, re.DOTALL)
            out["combo"]["polos_count"] = len(polos)
            out["combo"]["polos_amostra"] = polos[:3]
    except Exception as e:
        out["combo"] = {"error": str(e)}
    # 2. Testa GRID no primeiro polo (se listamos algum)
    id_polo_teste = None
    if isinstance(out.get("combo"), dict) and out["combo"].get("polos_amostra"):
        id_polo_teste = out["combo"]["polos_amostra"][0][0]
    if id_polo_teste:
        try:
            r = requests.get(
                GRID,
                params={
                    "anoLeti": (request.form.get("ano") or "2026/2").strip(),
                    "descEmpr": "GRADUAÇÃO EAD (CSED)",
                    "codEmpr": "12",
                    "idPolo": id_polo_teste,
                    "codCurso": "0",
                },
                headers={**HEADERS, "Cookie": cookie},
                timeout=60,
                allow_redirects=False,
            )
            out["grid"] = _resp_preview(r)
            out["grid"]["id_polo"] = id_polo_teste
        except Exception as e:
            out["grid"] = {"error": str(e)}
    return jsonify(out)


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
    except SessaoExpirada as se:
        logger.warning("relatorio_veteranos: SIAA rejeitou requisicao debug=%s", se.debug)
        step = se.debug.get("step") if isinstance(se.debug, dict) else None
        status = se.debug.get("status_code") if isinstance(se.debug, dict) else None
        detalhe = f" (step={step}, status={status})" if step else ""
        return (
            "Sessão do SIAA expirada ou requisição bloqueada — pegue o cookie de novo "
            f"e tente outra vez.{detalhe}"
        ), 409
    except Exception:
        logger.exception("Falha ao gerar relatório de veteranos (ano=%s)", ano)
        return "Erro interno ao gerar o relatório.", 500

    hoje = date.today().strftime("%Y-%m-%d")
    filename = f"relatorio_veteranos_{ano.replace('/', '-')}_{hoje}.xlsx"
    return send_file(
        BytesIO(xlsx),
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
