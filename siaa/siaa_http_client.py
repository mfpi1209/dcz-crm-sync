"""
Client HTTP experimental para consulta SIAA (JSF / PrimeFaces AJAX).

Fluxo em 4 etapas:
  1. GET  — carrega a página e obtém ViewState
  2. POST — busca por RGM (btnBuscar), confirma found=true
  3. POST — clica em "Carregar Aluno" (alunoProva), obtém formPanel + TabView
  4. GET  — ativa abas lazy do TabView e busca cada iframe descoberto

Uso:
    python run.py --rgm 39982564 --from-siaa-http --debug

Cookies: variável SIAA_COOKIE ou cookies/academico.txt (formato header Cookie).
         Módulos separados: SIAA_ACADEMICO_COOKIE / cookies/academico.txt
                            SIAA_FINANCEIRO_COOKIE / cookies/financeiro.txt
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests

from siaa.env_loader import get_project_root, load_project_env

_PROJECT_ROOT = get_project_root()
load_project_env()

log = logging.getLogger("siaa_http")

ENDPOINT = (
    "https://siaa.cruzeirodosul.edu.br/siaa_academico/secure/academico/"
    "consulta/wacdcon18/consultaAcademico.jsf"
)
ORIGIN = "https://siaa.cruzeirodosul.edu.br"
CAPTURAS_DIR = _PROJECT_ROOT / "capturas_siaa"
COOKIES_FILE = _PROJECT_ROOT / "siaa_cookies.txt"
COOKIES_FILE_ACADEMICO = _PROJECT_ROOT / "cookies" / "academico.txt"
COOKIES_FILE_FINANCEIRO = _PROJECT_ROOT / "cookies" / "financeiro.txt"

# Módulo financeiro
FIN_BASE = "https://siaa.cruzeirodosul.edu.br/siaa_financeiro/secure/fin/wteseve02/"
FIN_CONSULTA = FIN_BASE + "consultaTesouraria.jsf"
FIN_VENCIDOS = FIN_BASE + "vencidos.xhtml"
FIN_AVENCER  = FIN_BASE + "aVencer.xhtml"
FIN_PAGOS    = FIN_BASE + "pagos.xhtml"
FIN_COD_EMPR_DEFAULT = "12"
FIN_COD_INST_DEFAULT = "18"

# Row index usada no botão da tabela de resultados (primeira linha = índice 0)
RESULT_ROW_INDEX = 0

VIEWSTATE_INPUT_RE = re.compile(
    r'<input[^>]+name=["\']javax\.faces\.ViewState["\'][^>]*value=["\']([^"\']+)["\']',
    re.IGNORECASE | re.DOTALL,
)
VIEWSTATE_INPUT_RE_ALT = re.compile(
    r'<input[^>]+value=["\']([^"\']+)["\'][^>]*name=["\']javax\.faces\.ViewState["\']',
    re.IGNORECASE | re.DOTALL,
)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0"
)

BROWSER_BASE_HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "sec-ch-ua": '"Microsoft Edge";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

GET_EXTRA_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}

DEFAULT_HEADERS = {k: v for k, v in BROWSER_BASE_HEADERS.items()}

# Base URL usada para resolver hrefs relativos de iframes
IFRAME_BASE_URL = (
    "https://siaa.cruzeirodosul.edu.br/siaa_academico/secure/academico/"
    "consulta/wacdcon18/"
)

# Abas do TabView que interessam (índice → label esperado).
# Abas sem iframe (Doc. Pessoais, Endereço, Histórico, Vestibular,
# Matriz Curricular) são puladas se não retornarem iframe.
TABS_DE_INTERESSE = {0, 3, 6}   # Dados Cadastrais, Matrícula, Doc. Pendentes

# URLs de iframe conhecidas desta tela (relativas a IFRAME_BASE_URL).
# O PrimeFaces TabView atualiza o src do iframe via JavaScript no browser,
# não via AJAX — por isso as buscamos diretamente com GET após carregar o aluno.
KNOWN_IFRAME_SRCS: list[dict[str, str]] = [
    {"id": "dados",             "src": "dados.xhtml"},
    {"id": "matricula",         "src": "matricula.xhtml"},
    {"id": "documentos",        "src": "documentos.xhtml"},
    {"id": "documentoPendente", "src": "documentoPendente.xhtml"},
]


# ---------------------------------------------------------------------------
# Utilitários internos
# ---------------------------------------------------------------------------

def _extract_view_state_from_html(html: str) -> Optional[str]:
    if not html:
        return None
    for pattern in (VIEWSTATE_INPUT_RE, VIEWSTATE_INPUT_RE_ALT):
        match = pattern.search(html)
        if match:
            return match.group(1)
    return None


def _extract_view_state_from_xml_cdata(xml_text: str) -> Optional[str]:
    """
    O ViewState pode vir como conteúdo literal de um <update id="...ViewState...">
    sem ser um input HTML — extrai o valor bruto do texto do nó.
    """
    if not xml_text:
        return None
    match = re.search(
        r'<update[^>]+id=["\'][^"\']*ViewState[^"\']*["\'][^>]*>\s*([^<\s][^<]*)',
        xml_text,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()
    return None


def _load_cookie_header(module: str = "academico") -> str:
    """
    Carrega o header Cookie para o módulo indicado.

    Ordem de precedência:
      1. Variável de ambiente específica  (SIAA_ACADEMICO_COOKIE / SIAA_FINANCEIRO_COOKIE)
      2. Arquivo específico               (cookies/academico.txt / cookies/financeiro.txt)
      3. Variável de ambiente geral       (SIAA_COOKIE)
      4. Arquivo geral                    (siaa_cookies.txt)

    Parâmetro module: "academico" | "financeiro"
    """
    module = module.lower()

    env_specific   = {"academico": "SIAA_ACADEMICO_COOKIE", "financeiro": "SIAA_FINANCEIRO_COOKIE"}.get(module)
    file_specific  = {"academico": COOKIES_FILE_ACADEMICO,  "financeiro": COOKIES_FILE_FINANCEIRO }.get(module)

    # 0. Servidor de sessão remoto (SIAA_SESSION_URL)
    session_url = os.environ.get("SIAA_SESSION_URL", "").strip().rstrip("/")
    if session_url:
        try:
            headers: dict[str, str] = {}
            tok = os.environ.get("SIAA_SESSION_TOKEN", "").strip()
            if tok:
                headers["Authorization"] = f"Bearer {tok}"
            r = requests.get(
                f"{session_url}/session/{module}",
                headers=headers,
                timeout=6,
            )
            if r.status_code == 200:
                cookie = (r.json().get("cookie") or "").strip()
                if cookie:
                    try:
                        fp = file_specific
                        if fp:
                            fp.parent.mkdir(parents=True, exist_ok=True)
                            fp.write_text(cookie, encoding="utf-8")
                    except Exception:
                        pass
                    log.info("[Cookies] Usando cookie %s do servidor de sessao remoto", module)
                    return cookie
        except Exception as exc:
            log.warning("[Cookies] Falha ao puxar sessao remota (%s); usando fallback local", exc)

    # 1. Env específica
    if env_specific:
        cookie_str = os.environ.get(env_specific, "").strip()
        if cookie_str:
            log.info("[Cookies] Usando cookie %s de env %s", module, env_specific)
            return cookie_str

    # 2. Arquivo específico
    if file_specific and file_specific.exists():
        cookie_str = file_specific.read_text(encoding="utf-8").strip()
        if cookie_str:
            log.info("[Cookies] Usando cookie %s de arquivo %s", module, file_specific)
            return cookie_str

    # 3. Env geral
    cookie_str = os.environ.get("SIAA_COOKIE", "").strip()
    if cookie_str:
        log.info("[Cookies] Usando cookie %s de env SIAA_COOKIE (fallback geral)", module)
        return cookie_str

    # 4. Arquivo geral
    if COOKIES_FILE.exists():
        cookie_str = COOKIES_FILE.read_text(encoding="utf-8").strip()
        if cookie_str:
            log.info("[Cookies] Usando cookie %s de arquivo %s (fallback geral)", module, COOKIES_FILE)
            return cookie_str

    raise ValueError(
        f"Cookies SIAA ({module}) não configurados. "
        f"Defina SIAA_{module.upper()}_COOKIE ou crie {file_specific} "
        f"(ou SIAA_COOKIE / {COOKIES_FILE} como fallback)."
    )


def _apply_cookies(session: requests.Session, cookie_header: str) -> None:
    session.headers["Cookie"] = cookie_header
    for part in cookie_header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, _, value = part.partition("=")
        name, value = name.strip(), value.strip()
        if name:
            session.cookies.set(name, value, domain=".cruzeirodosul.edu.br")


def _ajax_headers() -> dict[str, str]:
    return {
        **BROWSER_BASE_HEADERS,
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/xml, text/xml, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": ORIGIN,
        "Referer": ENDPOINT,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }


def _parse_partial_response(xml_text: str) -> dict[str, str]:
    """
    Parseia o partial-response JSF e retorna {id: conteúdo} para cada <update>.
    Tolera CDATA e resposta malformada.
    """
    updates: dict[str, str] = {}
    if not xml_text or not xml_text.strip():
        return updates

    # Tenta parse XML puro primeiro
    try:
        root = ET.fromstring(xml_text.strip())
        for node in root.iter("update"):
            uid = node.attrib.get("id", "")
            content = node.text or ""
            if uid:
                updates[uid] = content
        return updates
    except ET.ParseError:
        pass

    # Fallback: extração via regex quando o XML tem CDATA não-escapado
    log.debug("Parse XML falhou, usando extração regex")
    for match in re.finditer(
        r'<update\b[^>]*\bid=["\']([^"\']+)["\'][^>]*>(.*?)</update>',
        xml_text,
        re.DOTALL | re.IGNORECASE,
    ):
        uid = match.group(1)
        content = match.group(2)
        # Remove wrappers CDATA se presentes
        content = re.sub(r"^\s*<!\[CDATA\[", "", content)
        content = re.sub(r"\]\]>\s*$", "", content)
        updates[uid] = content.strip()
    return updates


def _view_state_from_updates(updates: dict[str, str], raw_xml: str = "") -> Optional[str]:
    # 1. update cujo id contém "ViewState" com conteúdo bruto (não HTML)
    for uid, content in updates.items():
        if "ViewState" in uid:
            text = content.strip()
            if text and "<" not in text:   # conteúdo puro, não HTML
                return text

    # 2. Procura <input javax.faces.ViewState> dentro do HTML dos updates
    for content in updates.values():
        vs = _extract_view_state_from_html(content)
        if vs:
            return vs

    # 3. Regex direta no XML raw como último recurso
    if raw_xml:
        vs = _extract_view_state_from_xml_cdata(raw_xml)
        if vs:
            return vs

    return None


def _pick_updates(updates: dict[str, str]) -> dict[str, str]:
    """Seleciona updates relevantes: formPrincipal, formPanel e ViewState."""
    picked: dict[str, str] = {}
    for uid, content in updates.items():
        if uid in ("formPrincipal", "formPanel") or "ViewState" in uid:
            picked[uid] = content
    return picked


def _combine_html_parts(parts: list[tuple[str, str]]) -> str:
    """
    Recebe lista de (label, html) e une com comentários separadores.
    Ignora entradas de ViewState.
    """
    segments: list[str] = []
    for label, html in parts:
        if not html:
            continue
        segments.append(f"<!-- {label} -->\n{html}")
    return "\n\n".join(segments)


def _has_jsf_error(xml_text: str) -> bool:
    """Detecta <error> ou NullPointerException dentro de um partial-response JSF."""
    if not xml_text:
        return False
    lower = xml_text.lower()
    return (
        "<error>" in lower
        or "<error-name>" in lower
        or "nullpointerexception" in lower
    )


def _detect_found(rgm: str, html: str) -> bool:
    if not html or rgm not in html:
        return False
    lower = html.lower()
    if "j_username" in lower and "j_password" in lower:
        return False
    not_found_markers = (
        "nenhum registro encontrado",
        "nenhum aluno encontrado",
        "nenhum resultado",
        "sem registros",
    )
    if any(marker in lower for marker in not_found_markers):
        return False
    if "tabelaListaAlunos" in html or "ui-datatable-data" in html:
        return True
    return rgm in html


def _detect_loaded(html: str) -> bool:
    """
    Verifica se o segundo POST trouxe dados detalhados do aluno
    (painéis acadêmico/financeiro carregados).
    """
    if not html:
        return False
    lower = html.lower()
    loaded_markers = (
        "alunocarregado",
        "dadosaluno",
        "panelaluno",
        "formPanel",
        "tabview",
        "ui-tabs",
        "abaacademico",
        "abadoc",
        "abafinanceiro",
        "consulta-detalhe",
        "siaa_academico",
        "situacao",
        "periodo",
        "matricula",
        "turma",
        "documentopendente",
        "vencidos",
        "avencer",
        "pagos",
    )
    return any(m.lower() in lower for m in loaded_markers)


def _save_debug(stem: str, raw_xml: str, combined_html: str) -> None:
    CAPTURAS_DIR.mkdir(parents=True, exist_ok=True)
    xml_path = CAPTURAS_DIR / f"{stem}_response.xml"
    html_path = CAPTURAS_DIR / f"{stem}_combined.html"
    xml_path.write_text(raw_xml or "", encoding="utf-8")
    html_path.write_text(combined_html or "", encoding="utf-8")
    log.info("Debug salvo: %s", xml_path)
    log.info("Debug salvo: %s", html_path)


# ---------------------------------------------------------------------------
# Parsing de TabView e iframes
# ---------------------------------------------------------------------------

def _parse_tabview(html: str) -> Optional[dict[str, Any]]:
    """
    Detecta um PrimeFaces TabView no HTML e retorna sua estrutura:
    {
      "tabview_id": "formPanel:j_idt141",
      "form_id": "formPanel",
      "tabs": [{"index": 0, "panel_id": "...", "label": "...", "loaded": True/False}]
    }
    Retorna None se não encontrar.
    """
    tv_m = re.search(
        r'<div\s+id="([^"]+)"[^>]+class="ui-tabs\s[^"]*"[^>]*>',
        html,
        re.IGNORECASE,
    )
    if not tv_m:
        return None
    tabview_id: str = tv_m.group(1)
    form_id = tabview_id.split(":")[0]

    tabs: list[dict[str, Any]] = []
    for m in re.finditer(
        r'data-index="(\d+)"><a href="#([^"]+)"[^>]*>([^<]+)</a>',
        html,
    ):
        idx = int(m.group(1))
        panel_id = m.group(2)
        label = m.group(3).strip()

        # Verifica se o panel já tem conteúdo (não é lazy)
        panel_re = re.search(
            rf'<div\s+id="{re.escape(panel_id)}"[^>]*>(.*?)</div>',
            html,
            re.DOTALL | re.IGNORECASE,
        )
        loaded = bool(panel_re and panel_re.group(1).strip())
        tabs.append({"index": idx, "panel_id": panel_id, "label": label, "loaded": loaded})

    return {"tabview_id": tabview_id, "form_id": form_id, "tabs": tabs}


def _parse_iframes(html: str) -> list[dict[str, str]]:
    """Extrai lista de {id, src} de todos os <iframe> no HTML."""
    frames: list[dict[str, str]] = []
    for m in re.finditer(r'<iframe\b([^>]*)>', html, re.IGNORECASE):
        attrs = m.group(1)
        src_m = re.search(r'\bsrc="([^"]+)"', attrs, re.IGNORECASE)
        id_m = re.search(r'\bid="([^"]+)"', attrs, re.IGNORECASE)
        if src_m:
            frames.append({
                "id": id_m.group(1) if id_m else "",
                "src": src_m.group(1),
            })
    return frames


def _build_tab_activate_form(
    tabview_id: str,
    form_id: str,
    panel_id: str,
    tab_index: int,
    view_state: str,
) -> dict[str, str]:
    """Monta o form para ativar uma aba lazy do PrimeFaces TabView."""
    return {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": tabview_id,
        "javax.faces.partial.execute": tabview_id,
        "javax.faces.partial.render": tabview_id,
        form_id: form_id,
        f"{tabview_id}_newTab": panel_id,
        f"{tabview_id}_tabindex": str(tab_index),
        "javax.faces.ViewState": view_state,
    }


def _ativar_tab(
    session: requests.Session,
    tabview_id: str,
    form_id: str,
    panel_id: str,
    tab_index: int,
    view_state: str,
    *,
    label: str = "",
    debug: bool = False,
) -> dict[str, Any]:
    """
    POST para ativar uma aba lazy do TabView.
    Retorna {tab_html, view_state, iframes, error}.
    Nunca levanta exceção.
    """
    form = _build_tab_activate_form(tabview_id, form_id, panel_id, tab_index, view_state)
    log.info("POST ativar aba [%d] %r (panel=%s)", tab_index, label, panel_id)
    try:
        resp = session.post(ENDPOINT, data=form, headers=_ajax_headers(), timeout=60)
    except Exception as exc:
        log.error("POST aba [%d] falhou (rede): %s", tab_index, exc)
        return {"tab_html": "", "view_state": view_state, "iframes": [], "error": str(exc)}

    log.debug("POST aba [%d] status=%d xml_len=%d", tab_index, resp.status_code, len(resp.text))
    if resp.status_code != 200:
        return {
            "tab_html": "",
            "view_state": view_state,
            "iframes": [],
            "error": f"HTTP {resp.status_code}",
        }

    updates = _parse_partial_response(resp.text)
    new_vs = _view_state_from_updates(updates, resp.text) or view_state

    # O update relevante é o do panel da aba (ou o tabview inteiro)
    tab_html = ""
    for uid, content in updates.items():
        if panel_id in uid or tabview_id in uid:
            if content:
                tab_html = content
                break
    # fallback: qualquer update com conteúdo HTML
    if not tab_html:
        for uid, content in updates.items():
            if "ViewState" not in uid and content:
                tab_html = content
                break

    # Extrai iframes SOMENTE do painel desta aba (não do TabView inteiro)
    panel_m = re.search(
        rf'<div\s[^>]*id="{re.escape(panel_id)}"[^>]*>(.*?)</div>',
        tab_html,
        re.DOTALL | re.IGNORECASE,
    )
    panel_content = panel_m.group(1) if panel_m else tab_html
    iframes = _parse_iframes(panel_content)
    log.debug(
        "Aba [%d] %r → painel extraído (%d chars), iframes: %s",
        tab_index, label, len(panel_content), [f["src"] for f in iframes],
    )

    # Salva debug do XML e HTML da aba somente quando debug=True
    if debug:
        CAPTURAS_DIR.mkdir(parents=True, exist_ok=True)
        (CAPTURAS_DIR / f"_tab{tab_index}_{label[:20].replace(' ','_')}_raw.html").write_text(
            tab_html, encoding="utf-8", errors="replace"
        )

    return {"tab_html": tab_html, "panel_content": panel_content, "view_state": new_vs, "iframes": iframes, "error": None}

# ---------------------------------------------------------------------------
# Formulários JSF
# ---------------------------------------------------------------------------

def _build_search_form(rgm: str, view_state: str) -> dict[str, str]:
    return {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "formPrincipal:btnBuscar",
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "formPrincipal",
        "formPrincipal": "formPrincipal",
        "formPrincipal:btnBuscar": "formPrincipal:btnBuscar",
        "formPrincipal:empresas_input": "12",
        "formPrincipal:filterAluno": rgm,
        "formPrincipal:tabelaListaAlunos_rppDD": "10",
        "formPrincipal:tipoEnade_input": "1",
        "javax.faces.ViewState": view_state,
    }


def _build_load_form(rgm: str, view_state: str, row_index: int = RESULT_ROW_INDEX) -> dict[str, str]:
    btn = f"formPrincipal:tabelaListaAlunos:{row_index}:alunoProva"
    return {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": btn,
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "formPrincipal formPanel",
        "formPrincipal": "formPrincipal",
        btn: btn,
        "formPrincipal:empresas_input": "12",
        "formPrincipal:filterAluno": rgm,
        "formPrincipal:tabelaListaAlunos_rppDD": "10",
        "formPrincipal:tipoEnade_input": "1",
        "javax.faces.ViewState": view_state,
    }


# ---------------------------------------------------------------------------
# Funções públicas
# ---------------------------------------------------------------------------

def carregar_aluno(
    session: requests.Session,
    rgm: str,
    view_state: str,
    *,
    debug: bool = False,
) -> dict[str, Any]:
    """
    Segundo POST: simula clique em "Carregar Aluno" (alunoProva) na tabela de resultados.

    Retorna dict com load_html, load_raw_xml, view_state, loaded, error (ou None).
    Nunca levanta exceção — erros ficam em result["error"].
    """
    form = _build_load_form(rgm, view_state)
    btn = form["javax.faces.source"]
    log.info("POST carregar aluno RGM=%s btn=%s", rgm, btn)

    try:
        resp = session.post(
            ENDPOINT,
            data=form,
            headers=_ajax_headers(),
            timeout=60,
        )
    except Exception as exc:
        log.error("POST carregar falhou (rede): %s", exc)
        return {
            "load_html": "",
            "load_raw_xml": "",
            "view_state": view_state,
            "loaded": False,
            "error": f"Erro de rede no segundo POST: {exc}",
        }

    raw_xml = resp.text
    log.debug("POST carregar status=%d xml_len=%d", resp.status_code, len(raw_xml))

    if resp.status_code != 200:
        CAPTURAS_DIR.mkdir(parents=True, exist_ok=True)
        err_path = CAPTURAS_DIR / f"{rgm}_load_error.html"
        err_path.write_text(raw_xml, encoding="utf-8", errors="replace")
        msg = f"POST carregar retornou HTTP {resp.status_code}"
        log.error(msg)
        return {
            "load_html": raw_xml,
            "load_raw_xml": raw_xml,
            "view_state": view_state,
            "loaded": False,
            "error": msg,
        }

    updates = _parse_partial_response(raw_xml)
    picked = _pick_updates(updates)
    for uid in picked:
        log.debug("Load update: id=%s (%d chars)", uid, len(picked[uid]))

    new_vs = _view_state_from_updates(updates, raw_xml) or view_state
    if new_vs != view_state:
        log.debug("ViewState atualizado após POST carregar")

    load_html = _combine_html_parts([
        (uid, content)
        for uid, content in picked.items()
        if "ViewState" not in uid
    ])

    _save_debug(f"{rgm}_load", raw_xml, load_html)

    return {
        "load_html": load_html,
        "load_raw_xml": raw_xml,
        "view_state": new_vs,
        "loaded": _detect_loaded(load_html),
        "error": None,
    }


def buscar_iframes_aluno(
    session: requests.Session,
    rgm: str,
    load_html: str,
    view_state: str,
    *,
    base_url: str = IFRAME_BASE_URL,
    debug: bool = False,
) -> dict[str, Any]:
    """
    Etapa 4: coleta iframes do aluno carregado.

    Estratégia:
      1. Detecta iframes presentes em load_html (tab 0 = dados.xhtml)
      2. Complementa com KNOWN_IFRAME_SRCS (matricula, documentos, documentoPendente)
         que o SIAA carrega via JavaScript no browser — não chegam via AJAX
      3. GET de cada URL única resolvida
      4. Retorna lista detalhada + HTML combinado

    Retorna:
      {"iframes": [...], "iframe_html_combined": "...", "view_state": view_state}
    """
    CAPTURAS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Passo A: iframes do load_html ---
    seen_srcs: set[str] = set()
    raw_frames: list[dict[str, str]] = []

    for frame in _parse_iframes(load_html):
        src = frame["src"]
        if src and src not in seen_srcs:
            seen_srcs.add(src)
            raw_frames.append(frame)
            log.debug("Iframe do load_html: id=%r src=%r", frame.get("id"), src)

    # --- Passo B: adiciona iframes conhecidos que não vieram no HTML ---
    for known in KNOWN_IFRAME_SRCS:
        if known["src"] not in seen_srcs:
            seen_srcs.add(known["src"])
            raw_frames.append(known)
            log.debug("Iframe conhecido adicionado: id=%r src=%r", known["id"], known["src"])

    log.info("Total de iframes a buscar: %d → %s", len(raw_frames), [f["src"] for f in raw_frames])

    # --- Passo C: GET de cada iframe ---
    iframes: list[dict[str, Any]] = []
    iframe_parts: list[tuple[str, str]] = []

    for frame in raw_frames:
        src = frame["src"]
        url = urljoin(base_url, src)
        frame_id = frame.get("id") or re.sub(r"[^a-zA-Z0-9_.-]", "_", src)

        log.info("GET iframe id=%r src=%r → %s", frame_id, src, url)
        try:
            iframe_resp = session.get(url, headers={**GET_EXTRA_HEADERS}, timeout=60)
            status = iframe_resp.status_code
            iframe_html = iframe_resp.text if status == 200 else ""
            if status != 200:
                log.warning("GET iframe %r retornou %d", src, status)
        except Exception as exc:
            log.error("GET iframe %r falhou: %s", src, exc)
            iframe_html = ""
            status = 0

        # Salva debug
        safe_name = re.sub(r"[^a-zA-Z0-9_.-]", "_", src)[:60]
        debug_path = CAPTURAS_DIR / f"{rgm}_iframe_{safe_name}.html"
        debug_path.write_text(iframe_html, encoding="utf-8", errors="replace")
        log.info(
            "Debug iframe salvo: %s (%d chars)", debug_path, len(iframe_html)
        )

        entry: dict[str, Any] = {
            "id": frame_id,
            "src": src,
            "url": url,
            "status_code": status,
            "html": iframe_html,
        }
        iframes.append(entry)
        if iframe_html:
            iframe_parts.append((f"SIAA IFRAME {src}", iframe_html))

    iframe_html_combined = _combine_html_parts(iframe_parts)
    return {
        "iframes": iframes,
        "iframe_html_combined": iframe_html_combined,
        "view_state": view_state,
    }


# ---------------------------------------------------------------------------
# Etapa 5 — Módulo financeiro: tela de vencidos
# ---------------------------------------------------------------------------

def _extract_cod_inst_from_html(html: str) -> Optional[str]:
    """Tenta extrair cod_inst de URLs ou campos no HTML acadêmico carregado."""
    # Procura por padrões como cod_inst=18 em atributos href, action, JS etc.
    m = re.search(r'cod_inst[=:&"\s]+(\d+)', html, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def _is_access_denied_html(html: str) -> bool:
    """Detecta se o HTML retornado é a página de acesso negado do SIAA."""
    indicators = [
        "access_denied",
        "Funcionalidade indispon",
        "realizar o logoff e logon",
        "codigoErro=",
    ]
    return any(ind.lower() in html.lower() for ind in indicators)


def buscar_financeiro_aluno(
    session: requests.Session,
    rgm: str,
    *,
    load_html: str = "",
    cod_empr: str = FIN_COD_EMPR_DEFAULT,
    cod_inst: Optional[str] = None,
    debug: bool = False,
) -> dict[str, Any]:
    """
    Acessa o módulo financeiro do SIAA para buscar títulos vencidos, a vencer e pagos.

    Fluxo:
      1. GET consultaTesouraria.jsf?init=1&cod_empr=<e>&cod_inst=<i>&rgm_alun=<rgm>
         (estabelece contexto de sessão do módulo financeiro)
      2. GET vencidos.xhtml  (iframe com tabela de vencidos)
      3. GET aVencer.xhtml   (iframe com tabela a vencer)
      4. GET pagos.xhtml     (iframe com tabela de pagos)

    Retorna dict com chave "financeiro".
    """
    rgm = str(rgm).strip()

    # Tenta extrair cod_inst do HTML acadêmico; fallback para padrão
    if cod_inst is None:
        extracted = _extract_cod_inst_from_html(load_html) if load_html else None
        cod_inst = extracted or FIN_COD_INST_DEFAULT
        fonte_inst = "extraído do HTML" if extracted else "default"
    else:
        fonte_inst = "parâmetro"
    log.info(
        "[Fin] RGM=%s cod_empr=%s cod_inst=%s (%s)",
        rgm, cod_empr, cod_inst, fonte_inst,
    )

    consulta_url = (
        f"{FIN_CONSULTA}?init=1&cod_empr={cod_empr}"
        f"&cod_inst={cod_inst}&rgm_alun={rgm}"
    )

    nav_headers = {
        **GET_EXTRA_HEADERS,
        "Referer": ORIGIN + "/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    }
    iframe_headers = {
        **GET_EXTRA_HEADERS,
        "Referer": consulta_url,
        "Sec-Fetch-Dest": "iframe",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    }

    CAPTURAS_DIR.mkdir(parents=True, exist_ok=True)
    error: Optional[str] = None
    consulta_status = 0
    vencidos_status = 0
    avencer_status = 0
    pagos_status = 0
    consulta_html = ""
    vencidos_html = ""
    avencer_html = ""
    pagos_html = ""

    # ── Aplicar cookies do módulo financeiro ────────────────────────────────
    # siaa_financeiro é um WAR separado com JSESSIONID próprio.
    # Tenta carregar cookies/financeiro.txt (ou fallback geral).
    try:
        fin_cookie_header = _load_cookie_header(module="financeiro")
        _apply_cookies(session, fin_cookie_header)
        log.info("[OK] Cookies financeiros carregados (%d chars)", len(fin_cookie_header))
    except ValueError as exc:
        log.warning("[AVISO] Cookies financeiros não encontrados — %s", exc)

    # ── Etapa 0: inicializar sessão do módulo financeiro ─────────────────────
    fin_base_init = FIN_BASE + "index.jsf"
    try:
        log.info("[Fin] GET base init: %s", fin_base_init)
        session.get(fin_base_init, headers=nav_headers, timeout=15)
    except Exception:
        pass  # Não quebramos por isso; pode não existir

    # ── Etapa 1: tela-mãe ───────────────────────────────────────────────────
    try:
        log.info("[Fin] GET consultaTesouraria RGM=%s", rgm)
        resp = session.get(consulta_url, headers=nav_headers, timeout=30)
        consulta_status = resp.status_code
        consulta_html = resp.text
        log.info("[Fin] consultaTesouraria status=%d html_len=%d", consulta_status, len(consulta_html))
        if _is_access_denied_html(consulta_html):
            error = (
                "Módulo financeiro retornou acesso negado (access_denied). "
                "Os cookies de cookies/academico.txt cobrem apenas o módulo acadêmico. "
                "Para acessar o financeiro, capture os cookies da sessão siaa_financeiro "
                "(DevTools > Network > request a consultaTesouraria.jsf > cabeçalho Cookie) "
                "e salve em cookies/financeiro.txt."
            )
            log.warning("[Fin] %s", error)
            if debug:
                path = CAPTURAS_DIR / f"{rgm}_financeiro_consultaTesouraria.html"
                path.write_text(consulta_html, encoding="utf-8", errors="replace")
            return {"financeiro": {
                "consulta_url": consulta_url,
                "vencidos_url": FIN_VENCIDOS,
                "consulta_status": consulta_status,
                "vencidos_status": 0,
                "vencidos_html": "",
                "avencer_url": FIN_AVENCER,
                "avencer_status": 0,
                "avencer_html": "",
                "pagos_url": FIN_PAGOS,
                "pagos_status": 0,
                "pagos_html": "",
                "cod_inst": cod_inst,
                "error": error,
            }}
        if debug:
            path = CAPTURAS_DIR / f"{rgm}_financeiro_consultaTesouraria.html"
            path.write_text(consulta_html, encoding="utf-8", errors="replace")
            log.info("[Fin] Debug salvo: %s", path)
    except Exception as exc:
        error = f"consultaTesouraria falhou: {exc}"
        log.error("[Fin] %s", error)
        return {"financeiro": {
            "consulta_url": consulta_url,
            "vencidos_url": FIN_VENCIDOS,
            "consulta_status": consulta_status,
            "vencidos_status": 0,
            "vencidos_html": "",
            "avencer_url": FIN_AVENCER,
            "avencer_status": 0,
            "avencer_html": "",
            "pagos_url": FIN_PAGOS,
            "pagos_status": 0,
            "pagos_html": "",
            "cod_inst": cod_inst,
            "error": error,
        }}

    # ── Etapa 2: iframe vencidos ─────────────────────────────────────────────
    try:
        log.info("[Fin] GET vencidos.xhtml RGM=%s", rgm)
        vresp = session.get(FIN_VENCIDOS, headers=iframe_headers, timeout=30)
        vencidos_status = vresp.status_code
        vencidos_html = vresp.text
        log.info("[Fin] vencidos.xhtml status=%d html_len=%d", vencidos_status, len(vencidos_html))
        if _is_access_denied_html(vencidos_html):
            log.warning("[Fin] vencidos.xhtml retornou acesso negado")
            error = error or "vencidos.xhtml: acesso negado"
            vencidos_html = ""
        if debug:
            path = CAPTURAS_DIR / f"{rgm}_financeiro_vencidos.html"
            path.write_text(vresp.text, encoding="utf-8", errors="replace")
            log.info("[Fin] Debug salvo: %s", path)
    except Exception as exc:
        error = f"vencidos.xhtml falhou: {exc}"
        log.warning("[Fin] %s", error)

    # ── Etapa 3: iframe a vencer ─────────────────────────────────────────────
    try:
        log.info("[Fin] GET aVencer.xhtml RGM=%s", rgm)
        aresp = session.get(FIN_AVENCER, headers=iframe_headers, timeout=30)
        avencer_status = aresp.status_code
        avencer_html = aresp.text
        log.info("[Fin] aVencer.xhtml status=%d html_len=%d", avencer_status, len(avencer_html))
        if _is_access_denied_html(avencer_html):
            log.warning("[Fin] aVencer.xhtml retornou acesso negado")
            avencer_html = ""
        if debug:
            path = CAPTURAS_DIR / f"{rgm}_financeiro_avencer.html"
            path.write_text(aresp.text, encoding="utf-8", errors="replace")
            log.info("[Fin] Debug salvo: %s", path)
    except Exception as exc:
        log.warning("[Fin] aVencer.xhtml falhou: %s", exc)

    # ── Etapa 4: iframe pagos ────────────────────────────────────────────────
    try:
        log.info("[Fin] GET pagos.xhtml RGM=%s", rgm)
        presp = session.get(FIN_PAGOS, headers=iframe_headers, timeout=30)
        pagos_status = presp.status_code
        pagos_html = presp.text
        log.info("[Fin] pagos.xhtml status=%d html_len=%d", pagos_status, len(pagos_html))
        if _is_access_denied_html(pagos_html):
            log.warning("[Fin] pagos.xhtml retornou acesso negado")
            pagos_html = ""
        if debug:
            path = CAPTURAS_DIR / f"{rgm}_financeiro_pagos.html"
            path.write_text(presp.text, encoding="utf-8", errors="replace")
            log.info("[Fin] Debug salvo: %s", path)
    except Exception as exc:
        log.warning("[Fin] pagos.xhtml falhou: %s", exc)

    return {"financeiro": {
        "consulta_url": consulta_url,
        "vencidos_url": FIN_VENCIDOS,
        "consulta_status": consulta_status,
        "vencidos_status": vencidos_status,
        "vencidos_html": vencidos_html,
        "avencer_url": FIN_AVENCER,
        "avencer_status": avencer_status,
        "avencer_html": avencer_html,
        "pagos_url": FIN_PAGOS,
        "pagos_status": pagos_status,
        "pagos_html": pagos_html,
        "cod_inst": cod_inst,
        "error": error,
    }}


def buscar_aluno(rgm: str, *, debug: bool = False) -> dict[str, Any]:
    """
    Fluxo completo: GET → POST busca → POST carregar aluno → GET iframes.

    Retorna dict com:
      rgm, status_code, view_state,
      search_html, load_html, iframe_html_combined,
      html (search + load + iframes combinados),
      raw_xml, load_raw_xml,
      found, loaded, load_error,
      iframes (lista detalhada)
    """
    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(levelname)s %(name)s: %(message)s",
        )

    rgm = str(rgm).strip()
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    cookie_header = _load_cookie_header(module="academico")
    _apply_cookies(session, cookie_header)
    log.info("[OK] Cookies acadêmicos carregados (%d chars)", len(cookie_header))

    # ------------------------------------------------------------------
    # Etapa 1 — GET inicial
    # ------------------------------------------------------------------
    log.info("GET inicial: %s", ENDPOINT)
    get_resp = session.get(
        ENDPOINT,
        headers={**GET_EXTRA_HEADERS},
        timeout=60,
    )

    if get_resp.status_code != 200:
        CAPTURAS_DIR.mkdir(parents=True, exist_ok=True)
        err_path = CAPTURAS_DIR / "debug_get_error.html"
        err_path.write_text(get_resp.text, encoding="utf-8", errors="replace")
        log.error("GET inicial falhou — status %d", get_resp.status_code)
        log.error("Response headers: %s", dict(get_resp.headers))
        log.error("Primeiros 500 chars: %s", get_resp.text[:500])
        print(f"[GET ERRO] status_code={get_resp.status_code}", file=sys.stderr)
        print(f"[GET ERRO] headers={dict(get_resp.headers)}", file=sys.stderr)
        print(f"[GET ERRO] body[:500]={get_resp.text[:500]}", file=sys.stderr)
        print(f"[GET ERRO] debug salvo em {err_path}", file=sys.stderr)
        raise RuntimeError(f"GET inicial retornou {get_resp.status_code}")

    initial_html = get_resp.text
    view_state = _extract_view_state_from_html(initial_html)
    if not view_state:
        raise RuntimeError("ViewState não encontrado no GET inicial")

    log.debug("ViewState inicial (%d chars)", len(view_state))

    # ------------------------------------------------------------------
    # Etapa 2 — POST busca
    # ------------------------------------------------------------------
    log.info("POST busca RGM=%s", rgm)
    search_resp = session.post(
        ENDPOINT,
        data=_build_search_form(rgm, view_state),
        headers=_ajax_headers(),
        timeout=60,
    )
    search_xml = search_resp.text
    search_updates = _parse_partial_response(search_xml)
    search_picked = _pick_updates(search_updates)

    for uid in search_picked:
        log.debug("Search update: id=%s (%d chars)", uid, len(search_picked[uid]))

    view_state = _view_state_from_updates(search_updates, search_xml) or view_state
    log.debug("ViewState após busca (%d chars)", len(view_state))

    search_html = _combine_html_parts([
        ("GET initial", initial_html),
        *[
            (f'search update id="{uid}"', content)
            for uid, content in search_picked.items()
            if "ViewState" not in uid
        ],
    ])
    search_jsf_error = _has_jsf_error(search_xml)
    if search_jsf_error:
        log.error(
            "Busca RGM=%s retornou erro JSF (SIAA server): %s",
            rgm, search_xml[:300].replace("\n", " "),
        )
    found = (not search_jsf_error) and _detect_found(rgm, search_html)
    log.info(
        "Busca: found=%s status=%d jsf_error=%s",
        found, search_resp.status_code, search_jsf_error,
    )

    _save_debug(f"{rgm}", search_xml, search_html)

    # ------------------------------------------------------------------
    # Etapa 3 — POST carregar aluno (somente se found=True)
    # ------------------------------------------------------------------
    load_result: dict[str, Any] = {
        "load_html": "",
        "load_raw_xml": "",
        "loaded": False,
        "error": "Aluno não encontrado na busca — POST carregar pulado",
    }
    if found:
        load_result = carregar_aluno(session, rgm, view_state, debug=debug)
        view_state = load_result.get("view_state") or view_state
    else:
        log.warning("RGM=%s não encontrado — segundo POST ignorado", rgm)

    load_html = load_result.get("load_html", "")

    # ------------------------------------------------------------------
    # Etapa 4 — GET iframes (abas lazy + iframe URLs)
    # ------------------------------------------------------------------
    iframe_result: dict[str, Any] = {
        "iframes": [],
        "iframe_html_combined": "",
        "view_state": view_state,
    }
    if load_result.get("loaded") and load_html:
        log.info("Iniciando busca de iframes para RGM=%s", rgm)
        iframe_result = buscar_iframes_aluno(
            session, rgm, load_html, view_state, debug=debug
        )
        view_state = iframe_result.get("view_state") or view_state
    else:
        log.info("Pulando etapa de iframes (loaded=%s)", load_result.get("loaded"))

    iframe_html_combined = iframe_result.get("iframe_html_combined", "")

    # ------------------------------------------------------------------
    # Etapa 5 — Módulo financeiro: tela de vencidos
    # Só busca se o aluno realmente foi carregado no módulo acadêmico
    # (senão o financeiro retorna dados stale da sessão do último aluno).
    # ------------------------------------------------------------------
    financeiro: dict[str, Any] = {}
    if load_result.get("loaded"):
        fin_result = buscar_financeiro_aluno(
            session, rgm,
            load_html=load_html,
            debug=debug,
        )
        financeiro = fin_result.get("financeiro", {}) or {}
    else:
        log.warning(
            "Pulando módulo financeiro (loaded=False) para evitar dados stale — RGM=%s",
            rgm,
        )
    vencidos_html = financeiro.get("vencidos_html", "") if financeiro else ""

    # HTML combinado final = busca + detalhe + iframes + vencidos
    combined_html = _combine_html_parts([
        ("SIAA SEARCH_HTML", search_html),
        ("SIAA LOAD_HTML", load_html),
        ("SIAA IFRAMES", iframe_html_combined),
        ("SIAA FINANCEIRO VENCIDOS", vencidos_html),
    ])

    search_error = None
    if search_jsf_error:
        search_error = "SIAA retornou erro (NullPointerException) na busca — sessão pode estar inválida ou RGM inválido"
    return {
        "rgm": rgm,
        "status_code": search_resp.status_code,
        "view_state": view_state,
        "search_html": search_html,
        "load_html": load_html,
        "iframe_html_combined": iframe_html_combined,
        "html": combined_html,
        "raw_xml": search_xml,
        "load_raw_xml": load_result.get("load_raw_xml", ""),
        "found": found,
        "loaded": load_result.get("loaded", False),
        "load_error": load_result.get("error"),
        "search_error": search_error,
        "iframes": iframe_result.get("iframes", []),
        "financeiro": financeiro,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Client HTTP experimental SIAA")
    parser.add_argument("rgm", help="RGM do aluno")
    parser.add_argument("--debug", action="store_true", help="Logs detalhados")
    args = parser.parse_args(argv)

    try:
        result = buscar_aluno(args.rgm, debug=args.debug)
    except Exception as exc:
        log.error("%s", exc)
        if args.debug:
            raise
        print(f"Erro: {exc}", file=sys.stderr)
        return 1

    summary = {k: v for k, v in result.items() if k not in ("html", "search_html", "load_html", "iframe_html_combined", "raw_xml", "load_raw_xml", "view_state", "iframes")}
    summary["view_state_len"] = len(result.get("view_state") or "")
    summary["search_html_len"] = len(result.get("search_html") or "")
    summary["load_html_len"] = len(result.get("load_html") or "")
    summary["iframe_html_combined_len"] = len(result.get("iframe_html_combined") or "")
    summary["html_len"] = len(result.get("html") or "")
    summary["raw_xml_len"] = len(result.get("raw_xml") or "")
    summary["load_raw_xml_len"] = len(result.get("load_raw_xml") or "")
    summary["iframes_count"] = len(result.get("iframes") or [])
    summary["iframes"] = [
        {"id": f["id"], "src": f["src"], "status_code": f["status_code"], "html_len": len(f.get("html") or "")}
        for f in (result.get("iframes") or [])
    ]
    fin = result.get("financeiro") or {}
    summary["financeiro"] = {
        "cod_inst": fin.get("cod_inst"),
        "consulta_status": fin.get("consulta_status"),
        "vencidos_status": fin.get("vencidos_status"),
        "vencidos_html_len": len(fin.get("vencidos_html") or ""),
        "error": fin.get("error"),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
