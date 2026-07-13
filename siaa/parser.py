"""Parser de capturas SIAA (iframes principais + internos) → Supabase."""
from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse, urlencode as _urlencode

import requests

from siaa.env_loader import get_project_root, load_project_env

_PROJECT_ROOT = get_project_root()
load_project_env()

log = logging.getLogger("eduit_insights.parser")

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
TABLE_RE = re.compile(r"<table\b[^>]*>.*?</table>", re.IGNORECASE | re.DOTALL)
ROW_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_RE = re.compile(r"<t[hd]\b[^>]*>(.*?)</t[hd]>", re.IGNORECASE | re.DOTALL)

# PrimeFaces scrollable DataTable: thead e tbody ficam em <table> separados.
# Os headers ficam em <th aria-label="..."> e as linhas em <tr data-ri="N">.
_PF_TH_ARIA_RE = re.compile(r'<th\b[^>]+aria-label="([^"]+)"', re.IGNORECASE)
# Extrai o primeiro bloco scrollable-header-box para pegar só os headers da tabela principal
_PF_HEADER_BOX_RE = re.compile(
    r'class="[^"]*ui-datatable-scrollable-header(?:-box)?[^"]*"[^>]*>(.*?)</table>',
    re.IGNORECASE | re.DOTALL,
)
_PF_TR_RI_RE = re.compile(r'<tr\b[^>]*\bdata-ri="\d+"[^>]*>(.*?)</tr>', re.IGNORECASE | re.DOTALL)
_PF_TD_RE = re.compile(r'<td\b[^>]*>(.*?)</td>', re.IGNORECASE | re.DOTALL)

# Regex para diagnóstico financeiro
_MONEY_RE_SEARCH = re.compile(r'\b\d{1,3}(?:\.\d{3})*,\d{2}\b')
_DATE_BR_RE_SEARCH = re.compile(r'\b\d{2}/\d{2}/\d{4}\b')
RGM_LABEL_RE = re.compile(r"RGM\s*[:\-]?\s*(\d{8,10})", re.IGNORECASE)
RGM_URL_RE = re.compile(
    r"(?:rgm_alun|rgm|filterAluno)[=:](\d{8,10})",
    re.IGNORECASE,
)

ACADEMIC_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "rgm": ("rgm", "n rgm", "numero rgm", "nº rgm", "cod rgm"),
    "nome": ("nome", "nome do aluno", "aluno", "nome aluno"),
    "curso": ("curso",),
    "situacao_academica": ("situacao", "situação", "situacao academica", "situação acadêmica", "situação do aluno"),
    "serie": ("serie", "série", "semestre"),
    "periodo": ("periodo", "período", "turno", "modalidade"),
    "data_matricula": ("data matricula", "data matrícula", "dt matricula", "dt matrícula"),
    "cod_turma": ("turma", "cod turma", "código turma", "codigo turma"),
}

DOC_HEADER_MAP = {
    "codigo": ("codigo", "código", "cod"),
    "descricao": ("descricao", "descrição", "documento"),
    "obrigatorio": ("obrigatorio", "obrigatório", "obrig"),
    "situacao": ("situacao", "situação", "status"),
}

FIN_HEADER_MAP = {
    "tipo_titulo": ("tipo titulo", "tipo título", "tipo de titulo"),
    "numero_titulo": ("nr titulo", "nr. titulo", "numero titulo", "nº titulo", "número titulo"),
    "vencimento": ("vencimento", "dt vencimento", "data vencimento"),
    "atraso": ("atraso", "dias atraso", "dias"),
    "valor": ("valor", "vlr"),
    "desconto": ("desconto",),
    "multa_juros": ("multa", "juros", "multa/juros", "multa juros"),
    "total": ("total", "vl total", "valor/total"),
    "data_pagamento": ("data pagamento", "data de pagamento", "dt pagamento", "data pgto"),
    "tipo_pagamento": ("tipo pagamento", "tipo de pagamento", "forma pagamento", "forma de pagamento"),
    "valor_pago": ("valor pago", "vlr pago", "valor/pago", "valor liquido", "valor líquido"),
}

NOME_BLOCKLIST = frozenset({
    "appinicio", "inicio", "siaa", "home", "consulta", "academico", "financeiro",
    "documentos", "matricula", "dados", "aluno", "cruzeiro", "ead",
    "salvar", "buscar", "pesquisar", "filtrar", "limpar", "voltar", "cancelar",
})


@dataclass
class ParsedCaptura:
    rgm: Optional[str]
    captura: dict[str, Any]
    documentos: list[dict[str, Any]] = field(default_factory=list)
    titulos: list[dict[str, Any]] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)


def _plog(msg: str, logs: list[str], level: int = logging.INFO) -> None:
    line = f"[Parser] {msg}"
    logs.append(line)
    if level >= logging.WARNING:
        log.log(level, line)
    else:
        log.info(line)


def _norm_key(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().strip()
    return WS_RE.sub(" ", text)


def _clean_html(value: str) -> str:
    text = unescape(TAG_RE.sub(" ", value or ""))
    return WS_RE.sub(" ", text).strip()


def _normalize_rgm(value: str) -> str:
    return re.sub(r"\D", "", (value or "").strip())


def _is_valid_rgm(digits: str) -> bool:
    return 8 <= len(digits) <= 10


def _frame_html(frame: dict) -> str:
    if not isinstance(frame, dict):
        return _as_html(frame)
    for key in ("html", "outerHTML", "innerHTML", "content", "raw"):
        val = frame.get(key)
        if val:
            return str(val)
    return ""


def _as_html(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("html", "outerHTML", "innerHTML", "content", "raw"):
            if value.get(key):
                return str(value[key])
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "\n".join(_as_html(v) for v in value if v)
    return str(value)


def _flatten_iframes(nodes: Any, out: Optional[list[dict]] = None) -> list[dict]:
    if out is None:
        out = []
    if not nodes:
        return out
    if isinstance(nodes, dict):
        nodes = [nodes]
    if not isinstance(nodes, list):
        return out
    for node in nodes:
        if not isinstance(node, dict):
            continue
        out.append(node)
        for key in ("iframes", "innerIframes", "inner_iframes", "children", "frames"):
            child = node.get(key)
            if child:
                _flatten_iframes(child, out)
    return out


def _iframe_kind(src: str, html: str = "") -> str:
    src_l = (src or "").lower()
    if "documentopendente" in src_l:
        return "documentos"
    if "documentos.xhtml" in src_l:
        return "documentos_lista"
    if "dadoscadastrais" in src_l:
        return "financeiro_dados"
    if "dados.xhtml" in src_l:
        return "dados"
    if "matricula.xhtml" in src_l:
        return "matricula"
    if "vencidos" in src_l:
        return "financeiro_vencidos"
    if "avencer" in src_l:
        return "financeiro_a_vencer"
    if "pagos" in src_l:
        return "financeiro_pagos"
    if "financeiro" in src_l or "/caf/" in src_l or "/fin/" in src_l:
        return "financeiro"
    if "academ" in src_l or "wacdcon" in src_l:
        return "academico"
    blob = _norm_key(html[:800])
    if "documentopendente" in blob:
        return "documentos"
    return "outro"


def _iframe_label(kind: str) -> str:
    return {
        "dados": "iframe dados",
        "matricula": "iframe matricula",
        "documentos": "iframe documentos",
        "documentos_lista": "iframe documentos",
        "financeiro": "iframe financeiro",
        "financeiro_vencidos": "iframe financeiro",
        "financeiro_a_vencer": "iframe financeiro",
        "financeiro_pagos": "iframe financeiro",
        "academico": "iframe academico",
    }.get(kind, "iframe")


def _parse_tables(html: str) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    for table_html in TABLE_RE.findall(html or ""):
        rows: list[list[str]] = []
        for row_html in ROW_RE.findall(table_html):
            cells = [_clean_html(cell) for cell in CELL_RE.findall(row_html)]
            cells = [c for c in cells if c]
            if cells:
                rows.append(cells)
        if rows:
            tables.append(rows)
    return tables


def _parse_primefaces_scrollable(html: str) -> tuple[list[str], list[list[str]]]:
    """
    Extrai (headers, rows) de um PrimeFaces DataTable com layout scrollable.

    Esse layout separa thead e tbody em duas <table> distintas:
      - Headers: lidos do primeiro bloco ui-datatable-scrollable-header-box
                 (restringe ao datatable principal, evita headers de dialogs)
      - Rows:    lidas de <tr data-ri="N"> na scrollable-body

    A coluna "Zoom" (row-toggler) é descartada automaticamente.
    """
    # ── Headers: apenas da primeira scrollable-header-box ──────────────────
    header_box_m = _PF_HEADER_BOX_RE.search(html)
    header_source = header_box_m.group(1) if header_box_m else html
    all_headers = _PF_TH_ARIA_RE.findall(header_source)

    # Descarta coluna "Zoom" (row-toggler)
    zoom_offset = 0
    if all_headers and _norm_key(all_headers[0]) == "zoom":
        headers = all_headers[1:]
        zoom_offset = 1
    else:
        headers = all_headers

    if not headers:
        return [], []

    # ── Rows: <tr data-ri="N"> ─────────────────────────────────────────────
    rows: list[list[str]] = []
    for row_m in _PF_TR_RI_RE.finditer(html):
        row_html = row_m.group(1)
        cells = [_clean_html(c.group(1)) for c in _PF_TD_RE.finditer(row_html)]
        if zoom_offset and cells:
            cells = cells[zoom_offset:]
        if cells:
            rows.append(cells)

    return headers, rows


def _header_index(headers: list[str], aliases: tuple[str, ...]) -> Optional[int]:
    norm_headers = [_norm_key(h) for h in headers]
    for idx, header in enumerate(norm_headers):
        if header in aliases:
            return idx
    best_idx: Optional[int] = None
    best_len = 0
    for idx, header in enumerate(norm_headers):
        for alias in aliases:
            if header.startswith(alias) or alias in header:
                if len(alias) > best_len:
                    best_len = len(alias)
                    best_idx = idx
    return best_idx


def _clean_cell(value: str) -> str:
    text = re.sub(r"\s+ui-button\s*$", "", (value or "").strip(), flags=re.IGNORECASE)
    return text.strip()


def _align_table_row(headers: list[str], cells: list[str]) -> list[str]:
    if not headers or not cells:
        return cells
    if _norm_key(headers[0]) == "zoom":
        first = cells[0].strip() if cells else ""
        if first and (re.match(r"^\d+\s*[-–]", first) or re.match(r"^\d+/", first)):
            return [""] + list(cells)
        if len(cells) == len(headers) - 1:
            return [""] + list(cells)
    return cells


def _is_finance_placeholder_row(cells: list[str]) -> bool:
    blob = _norm_key(" ".join(cells))
    placeholders = (
        "nenhum titulo", "nenhum registro", "sem registros", "nenhum desconto",
        "nenhum titulo vencido", "nenhum titulo a vencer",
    )
    return any(p in blob for p in placeholders)


def _map_row(headers: list[str], cells: list[str], mapping: dict[str, tuple[str, ...]]) -> dict[str, str]:
    cells = _align_table_row(headers, cells)
    row: dict[str, str] = {}
    for fld, aliases in mapping.items():
        idx = _header_index(headers, aliases)
        if idx is not None and idx < len(cells):
            row[fld] = _clean_cell(cells[idx])
    if not row and len(cells) >= 2 and len(headers) >= 2:
        for i, header in enumerate(headers):
            if i < len(cells):
                row[_norm_key(header)] = _clean_cell(cells[i])
    return row


def _extract_input_values(html: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for match in re.finditer(
        r'<input\b[^>]*(?:id|name)\s*=\s*["\']([^"\']+)["\'][^>]*value\s*=\s*["\']([^"\']*)["\']',
        html or "",
        re.IGNORECASE,
    ):
        field_id = _norm_key(match.group(1))
        value = unescape(match.group(2)).strip()
        if value:
            found[field_id] = value
    for match in re.finditer(
        r'<input\b[^>]*value\s*=\s*["\']([^"\']*)["\'][^>]*(?:id|name)\s*=\s*["\']([^"\']+)["\']',
        html or "",
        re.IGNORECASE,
    ):
        value = unescape(match.group(1)).strip()
        field_id = _norm_key(match.group(2))
        if value and field_id not in found:
            found[field_id] = value
    return found


def _extract_label_values(html: str) -> dict[str, str]:
    found: dict[str, str] = {}
    patterns = [
        re.compile(
            r"<label[^>]*>(?P<label>.*?)</label>\s*</td>\s*<td[^>]*>(?P<value>.*?)</td>",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"ui-outputlabel[^>]*>(?P<label>.*?)</label>.*?<[^>]+>(?P<value>.*?)</",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"<span[^>]*class=\"[^\"]*(?:label|rotulo)[^\"]*\"[^>]*>(?P<label>.*?)</span>\s*<span[^>]*>(?P<value>.*?)</span>",
            re.IGNORECASE | re.DOTALL,
        ),
        # SIAA: <span style="font-weight:bold">Label:</span></td><td>...<input value="VALUE">
        # Limite: não cruzar </td> (evita capturar value de outro campo)
        re.compile(
            r"<span[^>]*>(?P<label>[^<]{3,50})</span>\s*</td>\s*<td[^>]*>"
            r"(?:(?!</?td\b).)*?<input[^>]+value=\"(?P<value>[^\"]{1,120})\"",
            re.IGNORECASE | re.DOTALL,
        ),
    ]
    for pattern in patterns:
        for match in pattern.finditer(html or ""):
            label = _clean_html(match.group("label"))
            value = _clean_html(match.group("value"))
            if label and value:
                found[_norm_key(label)] = value

    for table in _parse_tables(html):
        if len(table) == 1 and len(table[0]) == 2:
            found[_norm_key(table[0][0])] = table[0][1]
        for row in table:
            if len(row) == 2:
                found[_norm_key(row[0])] = row[1]

    for field_id, value in _extract_input_values(html).items():
        if "nome" in field_id and _is_plausible_nome(value):
            found.setdefault("nome", value)
        elif "rgm" in field_id and _is_valid_rgm(_normalize_rgm(value)):
            found.setdefault("rgm", _normalize_rgm(value))
        elif "curso" in field_id:
            found.setdefault("curso", value)
        elif "situac" in field_id:
            found.setdefault("situacao academica", value)
        elif "serie" in field_id or "semestre" in field_id:
            found.setdefault("serie", value)
        elif "turma" in field_id:
            found.setdefault("turma", value)
        elif "matric" in field_id and "data" in field_id:
            found.setdefault("data matricula", value)
    return found


def _pick_academic_field(label_map: dict[str, str], fld: str) -> str:
    for alias in ACADEMIC_FIELD_ALIASES[fld]:
        for key, value in label_map.items():
            if alias in key or key in alias:
                return value.strip()
    return ""


def _is_plausible_nome(nome: str) -> bool:
    text = (nome or "").strip()
    if len(text) < 5 or len(text) > 80:
        return False
    norm = _norm_key(text)
    if norm in NOME_BLOCKLIST:
        return False
    menu_tokens = {
        "cadastros", "consultas", "rotinas", "acompanhamento", "processos",
        "solicitacoes", "ocorrencias", "diarias", "setor", "caa", "aluno",
    }
    words = set(norm.split())
    if words & menu_tokens:
        return False
    if len(text.split()) > 8:
        return False
    if " " not in text and text.isupper() and len(text) < 20:
        return False
    if not re.search(r"[A-Za-zÀ-ú]{3,}", text):
        return False
    return True


def _extract_nome_from_html(html: str) -> str:
    labels = _extract_label_values(html)
    for alias in ACADEMIC_FIELD_ALIASES["nome"]:
        for key, value in labels.items():
            if alias in key and _is_plausible_nome(value):
                return value.strip()

    patterns = [
        r"Nome(?:\s+do\s+Aluno)?[:\s]+([A-ZÀ-Ú][A-ZÀ-Úa-zà-ú\s\.'-]{4,})",
        r"Aluno[:\s]+([A-ZÀ-Ú][A-ZÀ-Úa-zà-ú\s\.'-]{4,})",
    ]
    clean = _clean_html(html)
    for pattern in patterns:
        m = re.search(pattern, clean, re.I)
        if m and _is_plausible_nome(m.group(1)):
            return m.group(1).strip()
    return ""


def _extract_rgm_from_html(html: str) -> str:
    if not html:
        return ""
    for pattern in (RGM_LABEL_RE, RGM_URL_RE):
        m = pattern.search(html)
        if m and _is_valid_rgm(m.group(1)):
            return m.group(1)
    labels = _extract_label_values(html)
    for key, value in labels.items():
        if "rgm" in key:
            digits = _normalize_rgm(value)
            if _is_valid_rgm(digits):
                return digits
    return ""


def _extract_rgm_from_url(url: str) -> str:
    if not url:
        return ""
    for pattern in (RGM_URL_RE, re.compile(r"[?&]rgm=(\d{8,10})", re.I)):
        m = pattern.search(url)
        if m and _is_valid_rgm(m.group(1)):
            return m.group(1)
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        for key in ("rgm_alun", "rgm", "filterAluno"):
            for val in qs.get(key, []):
                digits = _normalize_rgm(val)
                if _is_valid_rgm(digits):
                    return digits
    except Exception:
        pass
    return ""


def _resolve_rgm(
    payload: dict[str, Any],
    frames: list[dict],
    root_html: str,
    academic: dict[str, str],
    logs: list[str],
) -> Optional[str]:
    candidates: list[tuple[str, str]] = []

    for source, raw in (
        ("payload.rgm", str(payload.get("rgm", ""))),
        ("payload.RGM", str(payload.get("RGM", ""))),
        ("campo rgm do JSON", json.dumps(payload, ensure_ascii=False)[:50000]),
    ):
        if source.startswith("campo"):
            digits = _extract_rgm_from_html(raw)
        else:
            digits = _normalize_rgm(raw)
        if _is_valid_rgm(digits):
            candidates.append((source, digits))

    url = str(payload.get("url") or payload.get("meta", {}).get("url") or "")
    url_rgm = _extract_rgm_from_url(url)
    if url_rgm:
        candidates.append(("URL", url_rgm))

    main_rgm = _extract_rgm_from_html(root_html)
    if main_rgm:
        candidates.append(("HTML principal", main_rgm))

    for frame in frames:
        src = str(frame.get("src") or "")
        html = _frame_html(frame)
        kind = _iframe_kind(src, html)
        label = _iframe_label(kind)

        src_rgm = _extract_rgm_from_url(src)
        if src_rgm:
            candidates.append((f"URL do {label}", src_rgm))

        html_rgm = _extract_rgm_from_html(html)
        if html_rgm:
            candidates.append((label, html_rgm))

    acad_rgm = _normalize_rgm(academic.get("rgm", ""))
    if _is_valid_rgm(acad_rgm):
        candidates.append(("dados acadêmicos", acad_rgm))

    if candidates:
        source, digits = candidates[0]
        _plog(f"RGM encontrado em {source}: {digits}", logs)
        return digits

    _plog("RGM não encontrado — continuando com rgm=null", logs, logging.WARNING)
    return None


def _resolve_nome(
    payload: dict[str, Any],
    frames: list[dict],
    root_html: str,
    academic: dict[str, str],
    logs: list[str],
) -> Optional[str]:
    for frame in frames:
        src = str(frame.get("src") or "")
        if "dados.xhtml" not in src.lower() or "financeiro" in src.lower():
            continue
        html = _frame_html(frame)
        dados = _parse_academic_from_html(html)
        if _is_plausible_nome(dados.get("nome", "")):
            _plog(f"Nome encontrado em iframe dados: {dados['nome']}", logs)
            return dados["nome"]

    if _is_plausible_nome(academic.get("nome", "")):
        _plog(f"Nome encontrado em dados acadêmicos: {academic['nome']}", logs)
        return academic["nome"]

    for key in ("nome", "nome_aluno", "aluno"):
        val = str(payload.get(key, "")).strip()
        if _is_plausible_nome(val):
            _plog(f"Nome encontrado em payload.{key}: {val}", logs)
            return val

    main_nome = _extract_nome_from_html(root_html)
    if main_nome:
        _plog(f"Nome encontrado em HTML principal: {main_nome}", logs)
        return main_nome

    for frame in frames:
        src = str(frame.get("src") or "")
        html = _frame_html(frame)
        kind = _iframe_kind(src, html)
        nome = _extract_nome_from_html(html)
        if nome:
            _plog(f"Nome encontrado em {_iframe_label(kind)}: {nome}", logs)
            return nome

    _plog("Nome do aluno não encontrado — continuando com nome=null", logs, logging.WARNING)
    return None


def _dedupe_text(value: str) -> str:
    text = WS_RE.sub(" ", (value or "").strip())
    if not text:
        return text
    words = text.split()
    for size in range(len(words) // 2, 0, -1):
        if len(words) % size:
            continue
        chunk = words[:size]
        if all(words[i : i + size] == chunk for i in range(0, len(words), size)):
            return " ".join(chunk)
    for i in range(1, len(text) // 2 + 1):
        unit = text[:i]
        if unit and len(text) % len(unit) == 0 and unit * (len(text) // len(unit)) == text:
            return unit.strip()
    return text


def _extract_curso_from_selected_options(html: str) -> Optional[str]:
    """
    Extrai o nome do curso a partir de <option selected> no HTML.
    Formato esperado: "CODE - NOME (MODALIDADE)" ou "CODE - NOME" (sem parêntese).
    Exclui: opções de empresa (2 siglas entre parênteses) e de aluno (RGM 8+ dígitos).
    """
    if not html:
        return None
    for m in re.finditer(
        r'selected[^>]*>(\d{1,5}\s*-\s*[^<]{4,120})</option>',
        html,
        re.IGNORECASE,
    ):
        text = m.group(1).strip()
        code_m = re.match(r'(\d+)\s*-', text)
        if not code_m:
            continue
        code = code_m.group(1)
        # Exclui RGM-like codes (6+ dígitos) → é o aluno, não o curso
        if len(code) >= 6:
            continue
        # Exclui padrão de empresa: "12 - GRADUAÇÃO EAD (CSED) (G-EAD)" — 2 grupos sigla
        if re.search(r'\([A-Z/-]{2,10}\)\s*\([A-Z/-]{2,10}\)\s*$', text):
            continue
        return text
    return None


def _normalize_curso(raw: str) -> str:
    """
    Remove código numérico inicial se presente:
    "316 - ENFERMAGEM (BACHARELADO) (4.0)" → "ENFERMAGEM (BACHARELADO)"
    Mantém versão numérica apenas se for a única info em parênteses.
    """
    if not raw:
        return raw
    # Remove código inicial: "316 - TEXT" → "TEXT"
    text = re.sub(r'^\d+\s*-\s*', '', raw).strip()
    # Remove versão numérica no final: "ENFERMAGEM (BACHARELADO) (4.0)" → "ENFERMAGEM (BACHARELADO)"
    text = re.sub(r'\s*\(\d+\.\d+\)\s*$', '', text).strip()
    return text or raw


def _parse_academic_from_html(html: str) -> dict[str, str]:
    labels = _extract_label_values(html)
    data = {fld: _pick_academic_field(labels, fld) for fld in ACADEMIC_FIELD_ALIASES}
    rgm = _extract_rgm_from_html(html)
    if rgm:
        data["rgm"] = rgm
    nome = _extract_nome_from_html(html)
    if nome:
        data["nome"] = nome
    if data.get("curso"):
        data["curso"] = _dedupe_text(data["curso"])
    # Normaliza situação: remove prefixo numérico "0 - Em Curso" → "Em Curso"
    if data.get("situacao_academica"):
        data["situacao_academica"] = re.sub(
            r'^\d+\s*-\s*', '', data["situacao_academica"]
        ).strip() or data["situacao_academica"]
    return {k: v for k, v in data.items() if v}


def _financeiro_categoria(kind: str, src: str) -> str:
    if kind == "financeiro_vencidos" or "vencidos" in src.lower():
        return "vencidos"
    if kind == "financeiro_a_vencer" or "avencer" in src.lower():
        return "a_vencer"
    if kind == "financeiro_pagos" or "pagos" in src.lower():
        return "pagos"
    return "geral"


def _parse_documentos_from_html(html: str, rgm: Optional[str], logs: list[str]) -> list[dict[str, str]]:
    docs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for table in _parse_tables(html):
        if len(table) < 2:
            continue
        headers = table[0]
        header_blob = _norm_key(" ".join(headers))
        if not any(token in header_blob for token in ("codigo", "código", "descricao", "descrição", "situacao", "situação")):
            continue
        for row in table[1:]:
            mapped = _map_row(headers, row, DOC_HEADER_MAP)
            if not mapped.get("descricao") and not mapped.get("codigo"):
                continue
            situacao = mapped.get("situacao", "")
            if situacao and _norm_key(situacao) in {"situação", "situacao"}:
                continue
            key = (mapped.get("codigo", ""), mapped.get("descricao", ""))
            if key in seen:
                continue
            seen.add(key)
            doc = {
                "rgm": rgm,
                "codigo": mapped.get("codigo", ""),
                "descricao": mapped.get("descricao", ""),
                "obrigatorio": mapped.get("obrigatorio", ""),
                "situacao": situacao,
            }
            docs.append(doc)
            _plog(
                f"Documento extraído: [{doc.get('codigo')}] {doc.get('descricao')} -> {doc.get('situacao')}",
                logs,
            )
    return docs


def _parse_titulos_from_html(
    html: str,
    rgm: Optional[str],
    categoria: str,
    logs: list[str],
) -> list[dict[str, Any]]:
    titulos: list[dict[str, Any]] = []

    def _extract_from_headers_and_rows(
        headers: list[str], data_rows: list[list[str]]
    ) -> None:
        header_blob = _norm_key(" ".join(headers))
        if "vencimento" not in header_blob and "valor" not in header_blob and "total" not in header_blob:
            return
        for row in data_rows:
            if _is_finance_placeholder_row(row):
                continue
            mapped = _map_row(headers, row, FIN_HEADER_MAP)
            if not any(mapped.get(k) for k in ("vencimento", "valor", "total", "numero_titulo", "data_pagamento", "valor_pago")):
                continue
            tipo = mapped.get("tipo_titulo", "")
            if _norm_key(tipo) in {"nenhum registro encontrado", "sem registros", "nenhum registro"}:
                continue
            valor_parsed = _parse_money(mapped.get("valor", ""))
            total_parsed = _parse_money(mapped.get("total", ""))
            vencimento = mapped.get("vencimento", "")
            numero = mapped.get("numero_titulo", "")
            if (
                not vencimento
                and valor_parsed is None
                and total_parsed is None
                and not numero
                and not mapped.get("data_pagamento")
                and mapped.get("valor_pago") is None
            ):
                continue
            tit = {
                "rgm": rgm,
                "categoria": categoria,
                "tipo_titulo": mapped.get("tipo_titulo", ""),
                "numero_titulo": numero,
                "vencimento": vencimento,
                "atraso": mapped.get("atraso", ""),
                "valor": valor_parsed,
                "desconto": _parse_money(mapped.get("desconto", "")),
                "multa_juros": _parse_money(mapped.get("multa_juros", "")),
                "total": total_parsed,
                "data_pagamento": mapped.get("data_pagamento", ""),
                "tipo_pagamento": mapped.get("tipo_pagamento", ""),
                "valor_pago": _parse_money(mapped.get("valor_pago", "")),
            }
            titulos.append(tit)
            _plog(
                f"Título financeiro extraído [{categoria}]: venc={tit.get('vencimento')} "
                f"total={tit.get('total')} tipo={tit.get('tipo_titulo')}",
                logs,
            )

    # ── Caminho 1: tabelas HTML normais (<thead> e <tbody> no mesmo <table>) ──
    for table in _parse_tables(html):
        if len(table) < 2:
            continue
        _extract_from_headers_and_rows(table[0], table[1:])

    # ── Caminho 2: PrimeFaces scrollable DataTable (<thead> e <tbody> em
    #    <table> distintos; headers via aria-label, linhas via data-ri) ────────
    pf_headers, pf_rows = _parse_primefaces_scrollable(html)
    if pf_headers and pf_rows:
        _extract_from_headers_and_rows(pf_headers, pf_rows)

    return titulos


def _parse_money(value: str) -> Optional[float]:
    if not value:
        return None
    text = value.strip().replace(".", "").replace(",", ".")
    text = re.sub(r"[^\d.\-]", "", text)
    try:
        return float(text)
    except ValueError:
        return None


def _parse_rows_as_documentos(rows: Any, rgm: Optional[str], logs: list[str]) -> list[dict[str, str]]:
    docs: list[dict[str, str]] = []
    if not isinstance(rows, list):
        return docs
    for row in rows:
        if not isinstance(row, dict):
            continue
        doc = {
            "rgm": rgm,
            "codigo": str(row.get("codigo") or row.get("Código") or row.get("cod") or ""),
            "descricao": str(row.get("descricao") or row.get("Descrição") or row.get("documento") or ""),
            "obrigatorio": str(row.get("obrigatorio") or row.get("Obrigatório") or ""),
            "situacao": str(row.get("situacao") or row.get("Situação") or row.get("status") or ""),
        }
        if doc.get("descricao") or doc.get("codigo"):
            docs.append(doc)
            _plog(f"Documento extraído: [{doc.get('codigo')}] {doc.get('descricao')}", logs)
    return docs


def _parse_rows_as_titulos(rows: Any, rgm: Optional[str], categoria: str, logs: list[str]) -> list[dict[str, Any]]:
    titulos: list[dict[str, Any]] = []
    if not isinstance(rows, list):
        return titulos
    for row in rows:
        if not isinstance(row, dict):
            continue
        tit = {
            "rgm": rgm,
            "categoria": categoria,
            "tipo_titulo": str(row.get("tipo") or row.get("tipo_titulo") or ""),
            "numero_titulo": str(row.get("numero") or row.get("numero_titulo") or row.get("titulo") or ""),
            "vencimento": str(row.get("vencimento") or row.get("Vencimento") or ""),
            "atraso": str(row.get("atraso") or row.get("Atraso") or ""),
            "valor": _parse_money(str(row.get("valor") or row.get("Valor") or "")),
            "desconto": _parse_money(str(row.get("desconto") or row.get("Desconto") or "")),
            "multa_juros": _parse_money(str(row.get("multa_juros") or row.get("multa") or "")),
            "total": _parse_money(str(row.get("total") or row.get("Total") or "")),
            "data_pagamento": str(row.get("data_pagamento") or row.get("data pagamento") or ""),
            "tipo_pagamento": str(row.get("tipo_pagamento") or row.get("tipo pagamento") or ""),
            "valor_pago": _parse_money(str(row.get("valor_pago") or row.get("valor pago") or "")),
        }
        if any(tit.get(k) for k in ("vencimento", "valor", "total", "numero_titulo", "data_pagamento", "valor_pago")):
            titulos.append(tit)
            _plog(f"Título financeiro extraído [{categoria}]: total={tit.get('total')}", logs)
    return titulos


def _merge_academic(target: dict[str, str], incoming: dict[str, str]) -> None:
    for key, value in incoming.items():
        if not value:
            continue
        if key == "nome" and not _is_plausible_nome(value):
            continue
        if not target.get(key):
            target[key] = value


def parse_captura_payload(payload: dict[str, Any]) -> ParsedCaptura:
    logs: list[str] = []
    academic: dict[str, str] = {}
    documentos: list[dict[str, str]] = []
    titulos: list[dict[str, Any]] = []

    frames = _flatten_iframes(payload.get("iframes"))
    _plog(f"Iframes achatados: {len(frames)} bloco(s)", logs)

    root_html = _as_html(payload.get("html"))
    if root_html:
        _merge_academic(academic, _parse_academic_from_html(root_html))

    for frame in frames:
        src = str(frame.get("src") or "")
        html = _frame_html(frame)
        kind = _iframe_kind(src, html)
        _plog(
            f"Processando {_iframe_label(kind)} src={src[:100] or '(sem src)'} html={len(html)} chars",
            logs,
        )

        if not html:
            continue

        if kind == "dados":
            dados = _parse_academic_from_html(html)
            if dados:
                _merge_academic(academic, dados)
                if dados.get("nome"):
                    _plog(f"Nome encontrado em iframe dados: {dados['nome']}", logs)
        else:
            frame_data = _parse_academic_from_html(html)
            if frame_data:
                _merge_academic(academic, frame_data)
                if frame_data.get("situacao_academica") and not academic.get("_situacao_logged"):
                    _plog(
                        f"Situação encontrada em iframe {src or kind}: "
                        f"{frame_data['situacao_academica']!r}",
                        logs,
                    )
                    academic["_situacao_logged"] = "1"
                if frame_data.get("curso") and not academic.get("_curso_logged"):
                    _plog(
                        f"Curso encontrado em iframe {src or kind}: "
                        f"{frame_data['curso']!r}",
                        logs,
                    )
                    academic["_curso_logged"] = "1"

        documentos.extend(_parse_documentos_from_html(html, None, logs))
        cat = _financeiro_categoria(kind, src)
        titulos.extend(_parse_titulos_from_html(html, None, cat, logs))

    academico = payload.get("academico") if isinstance(payload.get("academico"), dict) else {}
    if academico:
        _plog("Payload estruturado academico detectado", logs)
        for block_key in ("dados", "matricula", "documentos"):
            block = academico.get(block_key)
            html = _as_html(block)
            if html:
                _merge_academic(academic, _parse_academic_from_html(html))
                if block_key == "documentos":
                    documentos.extend(_parse_documentos_from_html(html, None, logs))
            documentos.extend(_parse_rows_as_documentos(block, None, logs))

    financeiro = payload.get("financeiro") if isinstance(payload.get("financeiro"), dict) else {}
    if financeiro:
        _plog("Payload estruturado financeiro detectado", logs)
        for cat in ("vencidos", "a_vencer", "pagos"):
            block = financeiro.get(cat)
            titulos.extend(_parse_titulos_from_html(_as_html(block), None, cat, logs))
            titulos.extend(_parse_rows_as_titulos(block, None, cat, logs))

    # ---------------------------------------------------------------
    # Extração de curso via selected options (load_html do SIAA HTTP)
    # ---------------------------------------------------------------
    load_html_raw = _as_html(payload.get("load_html"))
    if load_html_raw and not academic.get("curso"):
        raw_curso = _extract_curso_from_selected_options(load_html_raw)
        if raw_curso:
            academic["curso"] = _normalize_curso(raw_curso)
            academic.setdefault("curso_raw", raw_curso)
            _plog(f"Curso encontrado em load_html: {academic['curso']!r}", logs)

    rgm = _resolve_rgm(payload, frames, root_html, academic, logs)
    nome = _resolve_nome(payload, frames, root_html, academic, logs)
    if nome:
        academic["nome"] = nome

    # Remove flags internas de logging (não são campos acadêmicos reais)
    academic.pop("_situacao_logged", None)
    academic.pop("_curso_logged", None)

    doc_seen: set[tuple[str, str, str]] = set()
    unique_docs: list[dict[str, str]] = []
    for doc in documentos:
        key = (doc.get("codigo", ""), doc.get("descricao", ""), doc.get("situacao", ""))
        if key in doc_seen:
            continue
        doc_seen.add(key)
        doc["rgm"] = rgm
        unique_docs.append(doc)

    tit_seen: set[tuple[str, ...]] = set()
    unique_titulos: list[dict[str, Any]] = []
    for tit in titulos:
        key = (
            tit.get("categoria", ""),
            tit.get("numero_titulo", ""),
            tit.get("vencimento", ""),
            tit.get("data_pagamento", ""),
            str(tit.get("total", "")),
        )
        if key in tit_seen:
            continue
        tit_seen.add(key)
        tit["rgm"] = rgm
        unique_titulos.append(tit)

    capturado_em = payload.get("capturado_em") or payload.get("meta", {}).get("captured_at")
    if not capturado_em:
        capturado_em = datetime.now(timezone.utc).isoformat()

    has_data = bool(nome or unique_docs or unique_titulos or any(academic.values()))
    captura_row = {
        "rgm": rgm,
        "nome": nome,
        "curso": academic.get("curso"),
        "situacao_academica": academic.get("situacao_academica"),
        "serie": academic.get("serie"),
        "periodo": academic.get("periodo"),
        "data_matricula": academic.get("data_matricula"),
        "cod_turma": academic.get("cod_turma"),
        "status": "sucesso" if has_data and rgm else ("parcial" if has_data else "vazio"),
        "erro": None if rgm else "RGM não identificado na captura",
        "capturado_em": capturado_em,
        "fonte": payload.get("source") or payload.get("fonte") or "iframe_export",
        "raw_payload": payload,
    }

    _plog(
        f"Resumo acadêmico: rgm={rgm!r} nome={nome!r} curso={academic.get('curso')!r} "
        f"situacao={academic.get('situacao_academica')!r}",
        logs,
    )
    _plog(f"Total documentos: {len(unique_docs)} | Total títulos: {len(unique_titulos)}", logs)

    return ParsedCaptura(
        rgm=rgm,
        captura=captura_row,
        documentos=unique_docs,
        titulos=unique_titulos,
        logs=logs,
    )


def load_captura_file(path: Path | str) -> dict[str, Any]:
    file_path = Path(path)
    raw = file_path.read_text(encoding="utf-8").strip()
    if not raw:
        raise ValueError(f"Arquivo vazio: {file_path}")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON inválido em {file_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Captura deve ser um objeto JSON: {file_path}")
    return data


def _supabase_headers() -> dict[str, str]:
    url = (os.environ.get("SUPABASE_URL", "") or "").rstrip("/")
    key = os.environ.get("SUPABASE_KEY", "") or ""
    if not url or not key:
        raise RuntimeError("SUPABASE_URL e SUPABASE_KEY são obrigatórios")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _supabase_post(table: str, rows: list[dict] | dict) -> list[dict]:
    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    resp = requests.post(
        f"{base}/rest/v1/{table}",
        headers=_supabase_headers(),
        json=rows,
        timeout=30,
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Supabase {table} POST {resp.status_code}: {(resp.text or '')[:500]}")
    data = resp.json()
    return data if isinstance(data, list) else [data]


def _supabase_get(table: str, params: dict[str, str]) -> list[dict]:
    """GET /rest/v1/{table} com filtros PostgREST."""
    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    qs = _urlencode(params)
    resp = requests.get(
        f"{base}/rest/v1/{table}?{qs}",
        headers=_supabase_headers(),
        timeout=30,
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Supabase {table} GET {resp.status_code}: {(resp.text or '')[:500]}")
    data = resp.json()
    return data if isinstance(data, list) else [data]


def _supabase_count(table: str, captura_id: Any) -> int:
    """Conta linhas em uma tabela filtrando por captura_id."""
    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    qs = _urlencode({"captura_id": f"eq.{captura_id}", "select": "id"})
    headers = {**_supabase_headers(), "Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"}
    resp = requests.get(f"{base}/rest/v1/{table}?{qs}", headers=headers, timeout=30)
    cr = resp.headers.get("Content-Range", "")
    # Content-Range: 0-0/TOTAL  ou  */TOTAL
    m = re.search(r"/(\d+)$", cr)
    return int(m.group(1)) if m else 0


def fetch_latest_captura(rgm: str) -> dict[str, Any]:
    """
    Busca no Supabase a última captura do RGM, enriquece com contagens de
    documentos e títulos, calcula status_geral e próxima ação.

    Retorna dict com:
      captura_id, created_at, rgm, nome, curso, situacao_academica,
      serie, periodo, status, documentos_count, titulos_count,
      status_geral, proxima_acao, fonte, raw_payload (omitido por padrão)
    """
    rgm_norm = re.sub(r"\D", "", (rgm or "").strip())
    if not rgm_norm:
        raise ValueError("RGM inválido")

    rows = _supabase_get(
        "siaa_capturas",
        {"rgm": f"eq.{rgm_norm}", "order": "created_at.desc", "limit": "1"},
    )
    if not rows:
        return {"ok": False, "error": f"Nenhuma captura encontrada para RGM={rgm_norm}"}

    row = rows[0]
    captura_id = row.get("id")
    docs_count = _supabase_count("siaa_documentos", captura_id) if captura_id else 0
    tits_count = _supabase_count("siaa_titulos_financeiros", captura_id) if captura_id else 0

    # Status geral e próxima ação
    db_status: str = row.get("status") or "vazio"
    situacao: str = row.get("situacao_academica") or ""
    nome: str = row.get("nome") or ""
    curso: str = row.get("curso") or ""

    if db_status == "sucesso" and nome and curso:
        status_geral = "OK"
    elif db_status in ("sucesso", "parcial") and nome:
        status_geral = "PARCIAL"
    else:
        status_geral = "INCOMPLETO"

    proxima_acoes: list[str] = []
    if not nome:
        proxima_acoes.append("Re-capturar: nome não extraído")
    elif not curso:
        proxima_acoes.append("Re-capturar: curso não identificado")

    if situacao:
        sit_lower = situacao.lower()
        if "cancelado" in sit_lower or "cancelad" in sit_lower:
            proxima_acoes.append("Verificar motivação do cancelamento")
        elif "cursando" in sit_lower or "em curso" in sit_lower or "ativo" in sit_lower:
            if docs_count:
                proxima_acoes.append(f"Verificar {docs_count} documento(s) pendente(s)")
            if tits_count:
                proxima_acoes.append(f"Verificar {tits_count} título(s) financeiro(s)")
    if not proxima_acoes:
        if tits_count:
            proxima_acoes.append(f"Analisar {tits_count} título(s) financeiro(s)")
        else:
            proxima_acoes.append("Captura OK — nenhuma ação imediata")

    return {
        "ok": True,
        "captura_id": captura_id,
        "created_at": row.get("created_at"),
        "capturado_em": row.get("capturado_em"),
        "rgm": row.get("rgm"),
        "nome": nome or None,
        "curso": curso or None,
        "situacao_academica": situacao or None,
        "serie": row.get("serie"),
        "periodo": row.get("periodo"),
        "status": db_status,
        "documentos_count": docs_count,
        "titulos_count": tits_count,
        "fonte": row.get("fonte"),
        "status_geral": status_geral,
        "proxima_acao": " | ".join(proxima_acoes),
    }


def persist_parsed_to_supabase(parsed: ParsedCaptura) -> dict[str, Any]:
    captura_rows = _supabase_post("siaa_capturas", parsed.captura)
    captura_id = captura_rows[0]["id"]
    log.info("[Parser] Captura salva no Supabase id=%s rgm=%s", captura_id, parsed.rgm)
    print(f"[OK] Supabase atualizado — captura_id={captura_id} rgm={parsed.rgm}")

    docs_saved = 0
    if parsed.documentos:
        rows = [{**doc, "captura_id": captura_id} for doc in parsed.documentos]
        _supabase_post("siaa_documentos", rows)
        docs_saved = len(rows)
        log.info("[Parser] Documentos salvos: %d", docs_saved)

    titulos_saved = 0
    if parsed.titulos:
        rows = [{**tit, "captura_id": captura_id} for tit in parsed.titulos]
        _supabase_post("siaa_titulos_financeiros", rows)
        titulos_saved = len(rows)
        log.info("[Parser] Títulos financeiros salvos: %d", titulos_saved)

    print(f"[OK] Documentos salvos: {docs_saved} | Títulos salvos: {titulos_saved}")
    return {
        "captura_id": captura_id,
        "rgm": parsed.rgm,
        "documentos": docs_saved,
        "titulos": titulos_saved,
    }


def process_captura_file(path: Path | str, *, save_supabase: bool = True) -> dict[str, Any]:
    path = Path(path)
    print(f"[...] Processando captura: {path.name}")
    payload = load_captura_file(path)
    parsed = parse_captura_payload(payload)
    for line in parsed.logs:
        log.info(line)

    print(f"[OK] Parser concluído — rgm={parsed.rgm} docs={len(parsed.documentos)} títulos={len(parsed.titulos)}")

    result: dict[str, Any] = {
        "rgm": parsed.rgm,
        "captura": parsed.captura,
        "documentos_count": len(parsed.documentos),
        "titulos_count": len(parsed.titulos),
        "logs": parsed.logs,
    }
    if save_supabase:
        result["supabase"] = persist_parsed_to_supabase(parsed)
    return result


def process_captura_rgm(rgm: str, capturas_dir: Path | str, **kwargs: Any) -> dict[str, Any]:
    rgm_norm = _normalize_rgm(rgm)
    path = Path(capturas_dir) / f"{rgm_norm}.json"
    return process_captura_file(path, **kwargs)


def _pick_primary_html(http_result: dict[str, Any]) -> str:
    """
    Escolhe o HTML principal para passar ao parser como outerHTML.
    Preferência: dados.xhtml (inputs limpos) → load_html → html combinado.
    """
    for frame in http_result.get("iframes", []):
        if frame.get("src") == "dados.xhtml" and frame.get("html"):
            return frame["html"]
    return http_result.get("load_html", "") or http_result.get("html", "")


def _dump_academic_debug(rgm: str, http_result: dict[str, Any], payload: dict[str, Any]) -> None:
    """
    Gera capturas_siaa/<rgm>_debug_academic_fields.txt com:
    - iframes recebidos (nome, status, tamanho)
    - todas as ocorrências de "curso" com contexto ±300 chars
    - todos os <option selected> encontrados
    - todos os inputs readonly com value
    """
    out_dir = _PROJECT_ROOT / "capturas_siaa"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{rgm}_debug_academic_fields.txt"

    lines: list[str] = []
    sep = "=" * 72

    def _section(title: str) -> None:
        lines.append(f"\n{sep}\n  {title}\n{sep}")

    # ── 1. Iframes recebidos ─────────────────────────────────────────────────
    _section("IFRAMES RECEBIDOS (siaa_http_client)")
    raw_iframes = http_result.get("iframes") or []
    if not raw_iframes:
        lines.append("  (nenhum iframe retornado)")
    for f in raw_iframes:
        status = f.get("status_code", "?")
        size = len(f.get("html") or "")
        lines.append(f"  [{status}]  src={f.get('src', '—')}  html_len={size}")

    # also report what ended up in the parser payload
    _section("IFRAMES PASSADOS AO PARSER (payload)")
    parser_iframes = payload.get("iframes") or []
    if not parser_iframes:
        lines.append("  (nenhum)")
    for f in parser_iframes:
        size = len(f.get("html") or f.get("outerHTML") or "")
        lines.append(f"  src={f.get('src', '—')}  html_len={size}")

    # ── helper: scan HTML block ──────────────────────────────────────────────
    def _scan_html(label: str, html: str) -> None:
        if not html:
            lines.append(f"\n  [{label}] html vazio / não disponível")
            return
        lines.append(f"\n  [{label}]  total_len={len(html)}")

        # 2. Ocorrências de "curso" com contexto ±300 chars
        curso_hits = list(re.finditer(r'(?i)curso', html))
        lines.append(f"    Ocorrências de 'curso': {len(curso_hits)}")
        for m in curso_hits[:10]:
            start = max(0, m.start() - 300)
            end = min(len(html), m.end() + 300)
            snippet = html[start:end].replace("\n", " ").replace("\r", "")
            lines.append(f"    --- pos {m.start()} ---")
            lines.append(f"    {snippet}")

        # 3. <option selected ...>...</option>
        opts = re.findall(r'<option[^>]+selected[^>]*>([^<]{1,200})</option>', html, re.IGNORECASE)
        lines.append(f"    <option selected>: {len(opts)} encontrado(s)")
        for o in opts[:20]:
            lines.append(f"    OPTION: {o.strip()}")

        # 4. inputs readonly com value
        inputs = re.findall(
            r'<input[^>]+readonly[^>]*value=["\']([^"\']{1,200})["\'][^>]*/?>',
            html, re.IGNORECASE
        )
        inputs += re.findall(
            r'<input[^>]+value=["\']([^"\']{1,200})["\'][^>]+readonly[^>]*/?>',
            html, re.IGNORECASE
        )
        inputs = list(dict.fromkeys(inputs))  # dedupe preserving order
        lines.append(f"    inputs readonly com value: {len(inputs)}")
        for v in inputs[:30]:
            lines.append(f"    INPUT: {v.strip()}")

    # ── 2. Scan: load_html ───────────────────────────────────────────────────
    _section("SCAN: load_html")
    _scan_html("load_html", http_result.get("load_html") or "")

    # ── 3. Scan: cada iframe ─────────────────────────────────────────────────
    _section("SCAN: iframes individuais")
    for f in raw_iframes:
        _scan_html(f.get("src") or "iframe_sem_src", f.get("html") or "")

    # ── 4. Scan: outerHTML (primary html passado ao parser) ──────────────────
    _section("SCAN: outerHTML (primary html para o parser)")
    _scan_html("outerHTML", payload.get("outerHTML") or "")

    # ── escreve arquivo ──────────────────────────────────────────────────────
    content = "\n".join(lines)
    out_path.write_text(content, encoding="utf-8", errors="replace")
    log.info("[SiaaHttp] Debug fields salvo em %s", out_path)
    print(f"[Debug] Arquivo de diagnóstico: {out_path}")


def _dump_financeiro_debug(rgm: str, vencidos_html: str) -> None:
    """
    Gera capturas_siaa/<rgm>_debug_financeiro_tables.txt com:
    - todas as tabelas encontradas via _parse_tables (headers + linhas)
    - tabela PrimeFaces scrollable (headers via aria-label + linhas data-ri)
    - valores monetários encontrados por regex
    - datas encontradas por regex
    """
    out_dir = _PROJECT_ROOT / "capturas_siaa"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{rgm}_debug_financeiro_tables.txt"

    lines: list[str] = []
    sep = "=" * 72

    lines.append(f"{sep}")
    lines.append(f"  DEBUG FINANCEIRO VENCIDOS — RGM {rgm}")
    lines.append(f"  HTML: {len(vencidos_html)} chars")
    lines.append(sep)

    # ── Tabelas via _parse_tables ────────────────────────────────────────────
    tables = _parse_tables(vencidos_html)
    lines.append(f"\n{'─'*60}")
    lines.append(f"  _parse_tables: {len(tables)} tabela(s) encontrada(s)")
    lines.append(f"{'─'*60}")
    for i, table in enumerate(tables):
        lines.append(f"\nTabela {i+1}: {len(table)} linha(s)")
        if table:
            lines.append(f"  Headers (linha 0): {table[0]}")
            header_blob = _norm_key(" ".join(table[0]))
            has_fin = any(k in header_blob for k in ("vencimento", "valor", "total"))
            lines.append(f"  Header blob: {header_blob!r}")
            lines.append(f"  Tem vencimento/valor/total: {has_fin}")
        for j, row in enumerate(table[1:], 1):
            lines.append(f"  Linha {j}: {row}")

    # ── PrimeFaces scrollable ────────────────────────────────────────────────
    pf_headers, pf_rows = _parse_primefaces_scrollable(vencidos_html)
    lines.append(f"\n{'─'*60}")
    lines.append(f"  PrimeFaces Scrollable: {len(pf_headers)} col(s), {len(pf_rows)} linha(s)")
    lines.append(f"{'─'*60}")
    lines.append(f"  Headers: {pf_headers}")
    for i, row in enumerate(pf_rows):
        lines.append(f"  Row {i}: {row}")

    # ── Valores monetários ───────────────────────────────────────────────────
    moneys = _MONEY_RE_SEARCH.findall(vencidos_html)
    lines.append(f"\n{'─'*60}")
    lines.append(f"  Valores monetários (regex): {len(moneys)} ocorrência(s)")
    lines.append(f"{'─'*60}")
    lines.append(f"  {moneys[:40]}")

    # ── Datas ────────────────────────────────────────────────────────────────
    dates = _DATE_BR_RE_SEARCH.findall(vencidos_html)
    lines.append(f"\n{'─'*60}")
    lines.append(f"  Datas BR (regex): {len(dates)} ocorrência(s)")
    lines.append(f"{'─'*60}")
    lines.append(f"  {dates[:20]}")

    content = "\n".join(lines) + "\n"
    out_path.write_text(content, encoding="utf-8", errors="replace")
    log.info("[SiaaHttp] Debug financeiro salvo em %s", out_path)
    print(f"[Debug] Diagnóstico financeiro: {out_path}")


def process_siaa_http_rgm(
    rgm: str,
    *,
    save_supabase: bool = True,
    debug: bool = False,
) -> dict[str, Any]:
    """
    Busca o aluno diretamente no SIAA via HTTP (JSF AJAX), parseia o HTML
    retornado e opcionalmente persiste no Supabase.

    Requer siaa_http_client disponível no mesmo diretório (ou no PYTHONPATH)
    e cookies configurados via SIAA_COOKIE ou siaa_cookies.txt.

    Retorna o mesmo formato de process_captura_file().
    """
    try:
        from siaa.siaa_http_client import buscar_aluno
    except ImportError:
        try:
            from siaa_http_client import buscar_aluno  # fallback: mesmo diretório
        except ImportError as exc:
            raise RuntimeError(
                "siaa_http_client não encontrado. "
                "Execute via 'python run.py' a partir da raiz do projeto."
            ) from exc

    rgm_norm = _normalize_rgm(rgm)

    log.info("[SiaaHttp] Buscando RGM=%s no SIAA...", rgm_norm)
    print(f"[...] Buscando RGM {rgm_norm} no SIAA via HTTP...")
    http_result = buscar_aluno(rgm_norm, debug=debug)

    if not http_result.get("found"):
        log.warning("[SiaaHttp] Aluno RGM=%s não encontrado na resposta SIAA", rgm_norm)
        print(f"[AVISO] RGM {rgm_norm} não encontrado na resposta SIAA (verifique cookies)")
    else:
        print(f"[OK] Consulta SIAA realizada — RGM {rgm_norm} encontrado")

    payload: dict[str, Any] = {
        "rgm": rgm_norm,
        "source": "siaa_http",
        "url": "https://siaa.cruzeirodosul.edu.br/siaa_academico/secure/academico/"
               "consulta/wacdcon18/consultaAcademico.jsf",
        # Usa dados.xhtml como HTML primário (campos de input limpos, sem JS da busca).
        # Fallback para load_html e html combinado se dados.xhtml não estiver disponível.
        "outerHTML": _pick_primary_html(http_result),
        "html": _pick_primary_html(http_result),
        "raw_xml": http_result.get("raw_xml", ""),
        "capturado_em": datetime.now(timezone.utc).isoformat(),
        # load_html contém os selected options com curso e outros campos do formPrincipal
        "load_html": http_result.get("load_html", ""),
        # Mapeia iframes HTTP → formato esperado pelo parser
        # (parser lê frame["outerHTML"] ou frame["html"] via _frame_html)
        "iframes": [
            {
                "src": f["src"],
                "outerHTML": f["html"],
                "html": f["html"],
                "iframes": [],
            }
            for f in http_result.get("iframes", [])
            if f.get("html") and f.get("status_code") == 200
        ],
    }

    # Adiciona iframes financeiros ao payload, se disponíveis
    fin = http_result.get("financeiro") or {}
    vencidos_html = fin.get("vencidos_html") or ""
    if vencidos_html and fin.get("vencidos_status") == 200:
        payload["iframes"].append({
            "src": "financeiro/vencidos.xhtml",
            "outerHTML": vencidos_html,
            "html": vencidos_html,
            "iframes": [],
        })
        log.info("[SiaaHttp] Iframe financeiro/vencidos.xhtml adicionado (%d chars)", len(vencidos_html))

    avencer_html = fin.get("avencer_html") or ""
    if avencer_html and fin.get("avencer_status") == 200:
        payload["iframes"].append({
            "src": "financeiro/aVencer.xhtml",
            "outerHTML": avencer_html,
            "html": avencer_html,
            "iframes": [],
        })
        log.info("[SiaaHttp] Iframe financeiro/aVencer.xhtml adicionado (%d chars)", len(avencer_html))

    pagos_html = fin.get("pagos_html") or ""
    if pagos_html and fin.get("pagos_status") == 200:
        payload["iframes"].append({
            "src": "financeiro/pagos.xhtml",
            "outerHTML": pagos_html,
            "html": pagos_html,
            "iframes": [],
        })
        log.info("[SiaaHttp] Iframe financeiro/pagos.xhtml adicionado (%d chars)", len(pagos_html))

    log.info(
        "[SiaaHttp] Payload montado: %d iframes para o parser",
        len(payload["iframes"]),
    )
    print(f"[OK] Financeiro carregado — {len(payload['iframes'])} iframe(s) prontos para o parser")

    if debug:
        _dump_academic_debug(rgm_norm, http_result, payload)
        _vencidos_html = (http_result.get("financeiro") or {}).get("vencidos_html") or ""
        if _vencidos_html:
            _dump_financeiro_debug(rgm_norm, _vencidos_html)

    parsed = parse_captura_payload(payload)
    for line in parsed.logs:
        log.info(line)

    print(f"[OK] Parser concluído — rgm={parsed.rgm} docs={len(parsed.documentos)} títulos={len(parsed.titulos)}")

    result: dict[str, Any] = {
        "rgm": parsed.rgm,
        "captura": parsed.captura,
        "documentos_count": len(parsed.documentos),
        "titulos_count": len(parsed.titulos),
        "logs": parsed.logs,
        "siaa_http": {
            "status_code": http_result.get("status_code"),
            "found": http_result.get("found"),
            "html_len": len(http_result.get("html") or ""),
            "raw_xml_len": len(http_result.get("raw_xml") or ""),
        },
    }
    if save_supabase:
        result["supabase"] = persist_parsed_to_supabase(parsed)
    return result
