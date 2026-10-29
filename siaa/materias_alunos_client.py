"""SIAA Academico - consulta de materias por RGM (via cookie da sessao).

Baseado no fluxo validado:
  GET  wacdcon18/consultaAcademico.jsf?init=true   -> ViewState
  POST wacdcon18/consultaAcademico.jsf             -> carrega aluno (buscar)
  GET  wacdcon18/dados.xhtml                       -> dados cadastrais (e-mails)
  GET  wacdcon18/historico.xhtml                   -> grid inicial + turmas
  POST wacdcon18/historico.xhtml                   -> troca de turma (AJAX)

Cookie carregado do servidor de sessao (SIAA_SESSION_URL /session/academico).
"""
from __future__ import annotations

import html
import logging
import os
import re
from urllib.parse import quote

import requests

log = logging.getLogger(__name__)

CONS_BASE = "https://siaa.cruzeirodosul.edu.br/siaa_academico/secure/academico/consulta/wacdcon18"
CONS_JSF  = CONS_BASE + "/consultaAcademico.jsf"
CONS_HIST = CONS_BASE + "/historico.xhtml"
CONS_DADOS = CONS_BASE + "/dados.xhtml"   # aba "Dados Cadastrais" do TabView

# Dominios que caracterizam o e-mail academico/institucional (sufixos, csv via env).
_DOMINIOS_ACADEMICOS = tuple(
    d.strip().lower().lstrip("@")
    for d in os.getenv("SIAA_EMAIL_ACADEMICO_DOMINIOS", "cruzeirodosul.edu.br,.edu.br").split(",")
    if d.strip()
)
# Locais de e-mail que sao contato institucional da pagina, nao do aluno.
_EMAIL_LOCAL_BLOCKLIST = frozenset({
    "suporte", "contato", "atendimento", "noreply", "no-reply", "nao-responda",
    "sac", "ouvidoria", "secretaria", "financeiro", "cobranca", "webmaster",
})
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0")

# Fingerprint de browser identico ao siaa_http_client (Consulta SIAA), que
# funciona em producao. O SIAA/WAF exige esses headers para nao redirecionar.
_BROWSER_BASE = {
    "User-Agent": _UA,
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "sec-ch-ua": '"Microsoft Edge";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

HDR_NAV = {
    **_BROWSER_BASE,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,image/apng,*/*;q=0.8"),
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}
HDR_AJAX = {
    **_BROWSER_BASE,
    "Accept": "application/xml, text/xml, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Faces-Request": "partial/ajax",
    "Origin": "https://siaa.cruzeirodosul.edu.br",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

SESSAO_EXPIRADA_MSG = (
    "Sessão do SIAA (Acadêmico) expirada ou ausente. "
    "Suba a cURL em 'Atualizar sessão SIAA' (acadêmico) e tente novamente."
)


class SessaoExpirada(RuntimeError):
    """Sinaliza que o cookie SIAA (Academico) expirou/e ausente."""
    def __init__(self, msg: str = SESSAO_EXPIRADA_MSG):
        super().__init__(msg)


# ---------- Cookie loader ----------

def _load_cookie_academico() -> str:
    """Carrega o cookie Academico usando a mesma cadeia de fallbacks do Consulta SIAA
    (servidor remoto -> env -> arquivo). Isso garante paridade de comportamento em prod."""
    try:
        # reusar a funcao ja usada pelo Consulta SIAA para 100% de paridade
        from siaa.siaa_http_client import _load_cookie_header
        cookie = (_load_cookie_header("academico") or "").strip()
        if cookie:
            return cookie
        raise SessaoExpirada()
    except SessaoExpirada:
        raise
    except Exception as e:
        raise SessaoExpirada(f"Falha ao obter cookie: {e}. " + SESSAO_EXPIRADA_MSG)


# ---------- Helpers ----------

def _sessao_cookie(cookie_str: str) -> requests.Session:
    """Monta a sessao igual ao siaa_http_client (Consulta SIAA):
    header Cookie bruto + cookies no jar com dominio amplo .cruzeirodosul.edu.br."""
    s = requests.Session()
    s.headers.update(_BROWSER_BASE)
    # header Cookie bruto (garante envio de todos os cookies, igual ao que funciona)
    s.headers["Cookie"] = cookie_str
    for parte in cookie_str.split(";"):
        parte = parte.strip()
        if "=" in parte:
            k, v = parte.split("=", 1)
            s.cookies.set(k.strip(), v.strip(), domain=".cruzeirodosul.edu.br")
    return s


def _redirecionou(r: requests.Response) -> bool:
    if r.status_code in (301, 302, 303, 307, 308):
        return True
    loc = r.headers.get("Location") or ""
    return "access_denied" in loc


def _viewstate(text: str) -> str | None:
    m = (re.search(r'<update id="[^"]*ViewState[^"]*"><!\[CDATA\[(.*?)\]\]></update>', text, re.DOTALL)
         or re.search(r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"', text))
    return m.group(1) if m else None


def _parse_grid(text: str) -> list[dict]:
    out = []
    for row in re.findall(r'<tr[^>]*data-ri="\d+"[^>]*>(.*?)</tr>', text, re.DOTALL):
        mn = re.search(r'nome_disciplina"[^>]*?title="([^"]*)"[^>]*>\s*([^<]*)', row, re.DOTALL)
        if not mn:
            continue
        mr = re.search(r'tipo_resultado"[^>]*?title="([^"]*)"', row)
        # coluna "Seq. Oferta" = periodo/data da materia (ex.: "01/08/2026 a 18/12/2026")
        md = re.search(r'idSeqOferta"[^>]*>\s*([^<]*)', row)
        out.append({
            "sigla":      html.unescape(mn.group(2)).strip(),
            "disciplina": html.unescape(mn.group(1)).strip(),
            "resultado":  html.unescape(mr.group(1)).strip() if mr else "",
            "data":       html.unescape(md.group(1)).strip() if md else "",
        })
    return out


def _eh_academico(email: str) -> bool:
    dom = email.rsplit("@", 1)[-1].lower()
    for d in _DOMINIOS_ACADEMICOS:
        if dom == d.lstrip(".") or dom.endswith("." + d.lstrip(".")):
            return True
    return False


def _extrair_emails(text: str) -> dict:
    """Extrai e-mails da aba Dados Cadastrais.

    1) Pares rotulo/valor cujo rotulo contenha "mail" (reusa o parser do Consulta SIAA);
    2) fallback: todos os e-mails do HTML, ignorando contatos institucionais da pagina.
    Retorna {email_academico, email_pessoal, emails: [todos, em ordem]}.
    """
    if not text:
        return {"email_academico": None, "email_pessoal": None, "emails": []}
    try:
        from siaa.parser import _extract_label_values
        labels = _extract_label_values(text)
    except Exception as e:  # parser e best-effort; nao derruba a consulta
        log.warning("materias: parser de rotulos falhou: %s", e)
        labels = {}

    ordenados: list[str] = []
    vistos: set[str] = set()
    por_rotulo: dict[str, str] = {}   # "ad" | "pessoal" -> email

    def _add(v: str) -> str | None:
        first = None
        for m in _EMAIL_RE.findall(html.unescape(v or "")):
            m = m.strip().lower()
            if m.split("@", 1)[0] in _EMAIL_LOCAL_BLOCKLIST:
                continue
            first = first or m
            if m not in vistos:
                vistos.add(m)
                ordenados.append(m)
        return first

    # 1) rotulos com "mail". No SIAA (dados.xhtml): "E-mail AD:" = institucional
    #    (Active Directory, ex. @cs.unicid.edu.br) e "E-mail:" = pessoal.
    for k, v in labels.items():
        if "mail" not in k:
            continue
        e = _add(v)
        if not e:
            continue
        k2 = re.sub(r"[^a-z]+", " ", k).strip()
        if re.search(r"\bad\b|institucional|academic", k2):
            por_rotulo.setdefault("ad", e)
        elif k2 in ("e mail", "email", "mail", "e mail pessoal", "email pessoal"):
            por_rotulo.setdefault("pessoal", e)
    # 2) fallback: varredura do HTML inteiro
    if not ordenados:
        _add(text)

    academico = por_rotulo.get("ad") or next((e for e in ordenados if _eh_academico(e)), None)
    pessoal = por_rotulo.get("pessoal") if por_rotulo.get("pessoal") != academico else None
    if not pessoal:
        pessoal = next((e for e in ordenados if e != academico), None)
    return {"email_academico": academico, "email_pessoal": pessoal, "emails": ordenados}


def _turmas(text: str) -> tuple[str | None, list[str]]:
    m = re.search(r'id="formNota:turmas_input".*?</select>', text, re.DOTALL)
    bloco = m.group(0) if m else text
    turmas, sel = [], None
    for val, attrs, _lbl in re.findall(r'<option value="(\d+)"([^>]*)>([^<]*)</option>', bloco):
        turmas.append(val)
        if "selected" in attrs:
            sel = val
    return sel, turmas


def _troca_turma(s: requests.Session, vs: str, val: str) -> str:
    body = ("javax.faces.partial.ajax=true&javax.faces.source=formNota%3Aturmas"
            "&javax.faces.partial.execute=formNota%3Aturmas"
            "&javax.faces.partial.render=formNota+formDadosDisciplina"
            "&javax.faces.behavior.event=change&javax.faces.partial.event=change"
            f"&formNota=formNota&formNota%3Aturmas_input={val}"
            f"&javax.faces.ViewState={quote(vs, safe='')}")
    r = s.post(CONS_HIST, data=body, headers={**HDR_AJAX, "Referer": CONS_HIST}, timeout=90)
    if _redirecionou(r):
        raise SessaoExpirada()
    return r.text


# ---------- Public API ----------

def buscar_dados_cadastrais(s: requests.Session) -> str:
    """GET da aba Dados Cadastrais do aluno ja carregado na sessao. Retorna o HTML."""
    rd = s.get(CONS_DADOS,
               headers={**HDR_NAV, "Referer": CONS_JSF, "Sec-Fetch-Dest": "iframe"},
               timeout=60, allow_redirects=False)
    if _redirecionou(rd):
        raise SessaoExpirada()
    return rd.text or ""


def buscar_materias(cookie_str: str, rgm: str, empresa: str = "12") -> dict:
    """Consulta as materias de um RGM.

    Retorna {aluno, rgm, email_academico, email_pessoal,
             materias: [{sigla, disciplina, resultado, data}]}.

    Levanta SessaoExpirada se o SIAA redirecionar (cookie invalido/expirado).
    A busca de e-mails (aba Dados Cadastrais) e best-effort: falha de rede/parse
    nao derruba a consulta das materias.
    """
    rgm = re.sub(r"\D", "", rgm or "")
    if not rgm:
        raise ValueError("RGM invalido")

    s = _sessao_cookie(cookie_str)

    r0 = s.get(CONS_JSF, params={"init": "true"}, headers=HDR_NAV,
               timeout=60, allow_redirects=False)
    if _redirecionou(r0):
        raise SessaoExpirada()
    vs = _viewstate(r0.text)
    if not vs:
        raise SessaoExpirada()

    body = ("javax.faces.partial.ajax=true&javax.faces.source=formPrincipal%3AbtnBuscar"
            "&javax.faces.partial.execute=%40all&javax.faces.partial.render=formPrincipal"
            "&formPrincipal%3AbtnBuscar=formPrincipal%3AbtnBuscar&formPrincipal=formPrincipal"
            f"&formPrincipal%3Aempresas_focus=&formPrincipal%3Aempresas_input={empresa}"
            f"&formPrincipal%3AfilterAluno={rgm}&formPrincipal%3AtabelaListaAlunos_rppDD=10"
            "&formPrincipal%3AtipoEnade_focus=&formPrincipal%3AtipoEnade_input=1"
            f"&javax.faces.ViewState={quote(vs, safe='')}")
    rb = s.post(CONS_JSF, data=body,
                headers={**HDR_AJAX, "Referer": CONS_JSF + "?init=true"},
                timeout=60, allow_redirects=False)
    if _redirecionou(rb):
        raise SessaoExpirada()

    mn = re.search(r'filterAluno"[^>]*value="([^"]*)"', rb.text)
    aluno = html.unescape(mn.group(1)).strip() if mn else rgm

    # Dados Cadastrais -> e-mails (best-effort, exceto sessao expirada)
    emails = {"email_academico": None, "email_pessoal": None, "emails": []}
    try:
        emails = _extrair_emails(buscar_dados_cadastrais(s))
    except SessaoExpirada:
        raise
    except Exception as e:
        log.warning("materias: falha ao obter dados cadastrais rgm=%s: %s", rgm, e)

    rh = s.get(CONS_HIST,
               headers={**HDR_NAV, "Referer": CONS_JSF, "Sec-Fetch-Dest": "iframe"},
               timeout=60, allow_redirects=False)
    if _redirecionou(rh):
        raise SessaoExpirada()

    vs_h = _viewstate(rh.text)
    sel, turmas = _turmas(rh.text)
    materias = _parse_grid(rh.text)
    vistos = {sel}
    for val in turmas:
        if val in vistos or not vs_h:
            continue
        vistos.add(val)
        try:
            txt = _troca_turma(s, vs_h, val)
        except SessaoExpirada:
            raise
        except Exception as e:
            log.warning("materias: falha ao trocar turma %s rgm=%s: %s", val, rgm, e)
            continue
        vs_h = _viewstate(txt) or vs_h
        materias += _parse_grid(txt)

    # dedupe por (sigla, disciplina, resultado, data)
    uniq, seen = [], set()
    for m in materias:
        k = (m["sigla"], m["disciplina"], m["resultado"], m.get("data", ""))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(m)

    return {
        "aluno": aluno,
        "rgm": rgm,
        "email_academico": emails.get("email_academico"),
        "email_pessoal": emails.get("email_pessoal"),
        "materias": uniq,
    }
