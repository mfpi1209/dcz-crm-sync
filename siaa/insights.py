"""
Regras de negócio + leitura do Supabase para a tela "Consulta SIAA".

Este módulo é autossuficiente: fala com o Supabase via REST (requests) e
gera o objeto `insights` no formato exato consumido pelo painel.

Funções públicas:
    build_insights(captura, documentos, titulos) -> dict
    analisar_pagamentos(titulos) -> dict
    fetch_latest_captura(rgm) -> (captura|None, erro|None)
    fetch_documentos(captura_id) -> (list, erro|None)
    fetch_titulos(captura_id) -> (list, erro|None)
    supabase_configured() -> bool
    normalize_rgm(value) -> str

Env vars usadas:
    SUPABASE_URL, SUPABASE_KEY
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime
from typing import Any, Optional

import requests


# ---------------------------------------------------------------------------
# Supabase REST
# ---------------------------------------------------------------------------

def _get_supabase_url() -> str:
    return (os.environ.get("SUPABASE_URL", "") or "").rstrip("/")


def _get_supabase_key() -> str:
    return os.environ.get("SUPABASE_KEY", "") or ""


def _supabase_headers() -> dict[str, str]:
    key = _get_supabase_key()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }


def supabase_configured() -> bool:
    return bool(_get_supabase_url() and _get_supabase_key())


def _supabase_get(table: str, params: list[tuple[str, str]]) -> tuple[list[dict], Optional[str]]:
    if not supabase_configured():
        return [], "SUPABASE_URL e SUPABASE_KEY não configurados."

    url = f"{_get_supabase_url()}/rest/v1/{table}"
    try:
        resp = requests.get(url, headers=_supabase_headers(), params=params, timeout=20)
    except requests.RequestException as exc:
        return [], f"Erro de rede ao consultar Supabase: {exc}"

    if resp.status_code >= 300:
        body = (resp.text or "")[:400]
        return [], f"Supabase retornou {resp.status_code}: {body}"

    data = resp.json()
    return (data if isinstance(data, list) else []), None


def normalize_rgm(value: str) -> str:
    return re.sub(r"\D", "", (value or "").strip())


def fetch_latest_captura(rgm: str) -> tuple[Optional[dict], Optional[str]]:
    rgm_norm = normalize_rgm(rgm)
    if not rgm_norm:
        return None, "Informe um RGM válido."

    for order_col in ("created_at.desc", "capturado_em.desc", "id.desc"):
        rows, err = _supabase_get(
            "siaa_capturas",
            [
                ("select", "*"),
                ("rgm", f"eq.{rgm_norm}"),
                ("order", order_col),
                ("limit", "1"),
            ],
        )
        if err:
            return None, err
        if rows:
            return rows[0], None

    return None, None


def fetch_documentos(captura_id: Any) -> tuple[list[dict], Optional[str]]:
    return _supabase_get(
        "siaa_documentos",
        [
            ("select", "*"),
            ("captura_id", f"eq.{captura_id}"),
            ("order", "id.asc"),
        ],
    )


def fetch_titulos(captura_id: Any) -> tuple[list[dict], Optional[str]]:
    return _supabase_get(
        "siaa_titulos_financeiros",
        [
            ("select", "*"),
            ("captura_id", f"eq.{captura_id}"),
            ("order", "id.asc"),
        ],
    )


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------

def _pick(row: dict, *keys: str, default: str = "") -> str:
    for key in keys:
        val = row.get(key)
        if val is not None and str(val).strip() != "":
            return str(val).strip()
    return default


def _norm_situacao(value: str) -> str:
    return (value or "").strip().casefold()


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _to_num(value: Any) -> float:
    """Coage valores numéricos que podem vir como str do Supabase (ex.: '302.11' ou '302,11')."""
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if re.match(r"^\d+(\.\d+)?$", s):
        try:
            return float(s)
        except (ValueError, TypeError):
            return 0.0
    if re.match(r"^\d+,\d+$", s):
        try:
            return float(s.replace(",", "."))
        except (ValueError, TypeError):
            return 0.0
    try:
        return float(s.replace(".", "").replace(",", "."))
    except (ValueError, TypeError):
        return 0.0


def _fmt_datetime(value: Any) -> str:
    """Formata ISO datetime para DD/MM/AAAA às HH:MM (fuso local BRT, UTC-3)."""
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        from datetime import timezone as _tz, timedelta as _td
        brt = _tz(offset=_td(hours=-3))
        dt_local = dt.astimezone(brt)
        return dt_local.strftime("%d/%m/%Y às %H:%M")
    except (ValueError, TypeError):
        return str(value)[:16].replace("T", " ")


def _doc_label(doc: dict) -> str:
    return _pick(doc, "tipo", "nome", "documento", "descricao", default="Documento")


def _situacao_resumo_phrase(situacao: str) -> str:
    sit = (situacao or "não informada").strip().lower()
    if sit in {"em curso", "matriculado", "ativo"}:
        return f"Aluno {sit}"
    return f"Aluno com situação {sit}"


def _build_status_geral(recusados: list[dict], pendentes: list[dict], vencidos: list[dict]) -> dict[str, str]:
    if recusados or vencidos:
        return {"label": "Crítico", "emoji": "🔴", "tone": "critical"}
    if pendentes:
        return {"label": "Atenção", "emoji": "🟡", "tone": "warning"}
    return {"label": "Regular", "emoji": "🟢", "tone": "ok"}


def _build_resumo_atendimento(situacao_acad: str, recusados: list[dict], pendentes: list[dict], vencidos: list[dict]) -> str:
    sit = _situacao_resumo_phrase(situacao_acad)
    fin = "financeiro em atenção" if vencidos else "financeiro regular"
    if recusados:
        doc = "com documentação recusada que exige ação imediata"
    elif pendentes:
        doc = "com pendência documental que exige ação"
    else:
        doc = "sem pendências críticas identificadas"
    return f"{sit}, {fin}, {doc}."


# ---------------------------------------------------------------------------
# Análise de pagamentos (só mensalidades pagas)
# ---------------------------------------------------------------------------

def analisar_pagamentos(titulos: list[dict]) -> dict:
    from collections import Counter

    mensalidades = [
        t for t in titulos
        if t.get("categoria") == "pagos"
        and "mensalidade" in (t.get("tipo_titulo") or "").lower()
    ]
    qtd_analisada = len(mensalidades)

    dias: list[int] = []
    for t in mensalidades:
        d = _parse_date(t.get("data_pagamento"))
        if d is not None:
            dias.append(d.day)

    if len(dias) >= 2:
        dia_min = min(dias)
        dia_max = max(dias)
        dia_medio = round(sum(dias) / len(dias))
        amplitude = dia_max - dia_min
        if amplitude <= 3:
            consistencia = "Muito consistente"
        elif amplitude <= 7:
            consistencia = "Consistente"
        else:
            consistencia = "Irregular"
        if consistencia == "Muito consistente" and dia_min == dia_max:
            frase = f"Sempre paga no dia {dia_min}."
        else:
            frase = f"Costuma pagar entre o dia {dia_min} e {dia_max} do mês (média: dia {dia_medio})."
    else:
        dia_min = dia_max = dia_medio = amplitude = None
        consistencia = "Sem dados"
        frase = "Histórico de pagamentos insuficiente para identificar um padrão de dia."

    valores = [_to_num(t.get("valor_pago")) for t in mensalidades if t.get("valor_pago") is not None]
    if valores:
        sorted_v = sorted(valores)
        mid = len(sorted_v) // 2
        valor_tipico: Optional[float] = (
            sorted_v[mid]
            if len(sorted_v) % 2 == 1
            else (sorted_v[mid - 1] + sorted_v[mid]) / 2
        )
    else:
        valor_tipico = None

    def _strip_code(tp: str) -> str:
        return re.sub(r"^\d+-", "", (tp or "").strip())

    formas = [
        _strip_code(t.get("tipo_pagamento") or "")
        for t in mensalidades
        if (t.get("tipo_pagamento") or "").strip()
    ]
    forma_predominante = Counter(formas).most_common(1)[0][0] if formas else "—"
    if not forma_predominante:
        forma_predominante = "—"

    atrasos_num: list[int] = []
    em_dia = 0
    for t in mensalidades:
        try:
            a = int(t.get("atraso") or 0)
            atrasos_num.append(a)
            if a == 0:
                em_dia += 1
        except (ValueError, TypeError):
            pass

    pct_em_dia = round(100 * em_dia / len(atrasos_num)) if atrasos_num else 0
    atraso_medio = round(sum(atrasos_num) / len(atrasos_num), 1) if atrasos_num else 0.0

    return {
        "frase": frase,
        "consistencia": consistencia,
        "dia_min": dia_min,
        "dia_max": dia_max,
        "dia_medio": dia_medio,
        "amplitude": amplitude,
        "valor_tipico": valor_tipico,
        "forma_predominante": forma_predominante,
        "qtd_analisada": qtd_analisada,
        "pct_em_dia": pct_em_dia,
        "atraso_medio": atraso_medio,
    }


# ---------------------------------------------------------------------------
# Insights
# ---------------------------------------------------------------------------

def build_insights(
    captura: Optional[dict],
    documentos: list[dict],
    titulos: list[dict],
) -> dict[str, Any]:
    _NA = "Não identificado na consulta atual"

    if not captura:
        return {
            "status_geral": {"label": "Sem dados", "tone": "neutral", "detail": "Nenhuma captura encontrada para este RGM."},
            "risco": {"title": "Risco", "value": "—", "detail": "", "tone": "neutral", "status_label": ""},
            "resumo_atendimento": "Nenhuma captura encontrada para este RGM. Verifique o número ou aguarde nova sincronização.",
            "aluno": {
                "title": "Aluno",
                "value": _NA,
                "fields": [],
                "tone": "neutral",
                "status_label": "",
            },
            "documentacao": {"title": "Documentação", "value": _NA, "detail": "", "tone": "neutral", "status_label": ""},
            "financeiro": {"title": "Financeiro", "value": _NA, "detail": "", "tone": "neutral", "status_label": ""},
            "proxima_acao": {
                "title": "Próxima Melhor Ação",
                "value": "Verificar RGM",
                "detail": "Confira o RGM informado ou aguarde nova captura no SIAA.",
                "tone": "neutral",
                "status_label": "",
            },
            "siaa_online": False,
            "meta": None,
            "financeiro_detalhe": None,
        }

    nome = _pick(captura, "nome", "nome_aluno", "aluno", default="")
    curso = _pick(captura, "curso", "curso_nome", default="")
    situacao_acad = _pick(captura, "situacao_academica", "situacao", "status_academico", default="")
    rgm_txt = _pick(captura, "rgm", default="")
    serie = _pick(captura, "serie", default="")
    periodo = _pick(captura, "periodo", default="")
    fonte = _pick(captura, "fonte", default="")

    created_raw = _pick(captura, "created_at", "capturado_em", default="")
    ultima_consulta = _fmt_datetime(created_raw) or _NA

    _bad = {"", "none", "null", "nan", "(nome não informado)", "não informada"}

    def _display(v: str) -> str:
        return v.strip() if v and v.strip().lower() not in _bad else _NA

    nome_d = _display(nome)
    curso_d = _display(curso)
    sit_d = _display(situacao_acad)
    rgm_d = rgm_txt if rgm_txt else _NA

    aluno_fields: list[dict] = [
        {"label": "Nome", "value": nome_d},
        {"label": "RGM", "value": rgm_d},
        {"label": "Curso", "value": curso_d},
        {"label": "Situação acadêmica", "value": sit_d},
    ]
    if serie or periodo:
        aluno_fields.append({"label": "Série / Período", "value": f"{serie or '—'} / {periodo or '—'}"})
    aluno_fields.append({"label": "Última consulta", "value": ultima_consulta})

    sit_lower = (situacao_acad or "").strip().lower()
    if sit_lower in ("em curso", "matriculado", "ativo"):
        aluno_tone = "ok"
    elif sit_lower in ("cancelado", "cancelada", "trancado", "trancada"):
        aluno_tone = "critical"
    else:
        aluno_tone = "neutral"

    recusados = [d for d in documentos if _norm_situacao(_pick(d, "situacao", "status")) == "recusado"]
    pendentes = [d for d in documentos if _norm_situacao(_pick(d, "situacao", "status")) == "pendente"]
    vencidos_lst = [t for t in titulos if t.get("categoria") == "vencidos"]
    avencer_lst = [t for t in titulos if t.get("categoria") == "a_vencer"]
    pagos_lst = [t for t in titulos if t.get("categoria") == "pagos"]
    vencidos = vencidos_lst

    if recusados:
        doc_value = "Problema crítico"
        doc_tone = "critical"
        doc_detail = "Recusados: " + ", ".join(_doc_label(d) for d in recusados[:5])
        if len(recusados) > 5:
            doc_detail += f" (+{len(recusados) - 5})"
        doc_label_pill = "Crítico"
    elif pendentes:
        doc_value = "Pendência"
        doc_tone = "warning"
        doc_detail = "Pendentes: " + ", ".join(_doc_label(d) for d in pendentes[:5])
        if len(pendentes) > 5:
            doc_detail += f" (+{len(pendentes) - 5})"
        doc_label_pill = "Atenção"
    elif documentos:
        doc_value = "Regular"
        doc_tone = "ok"
        doc_detail = f"{len(documentos)} documento(s) analisado(s) sem pendências críticas."
        doc_label_pill = "Regular"
    else:
        doc_value = _NA
        doc_tone = "neutral"
        doc_detail = ""
        doc_label_pill = ""

    if vencidos_lst:
        fin_value = "Em atenção"
        fin_tone = "critical"
        fin_detail = (
            f"{len(vencidos_lst)} vencido(s) · "
            f"{len(avencer_lst)} a vencer · "
            f"{len(pagos_lst)} pago(s)"
        )
        fin_label_pill = "Crítico"
    elif titulos:
        fin_value = "Regular"
        fin_tone = "ok"
        fin_detail = (
            f"{len(vencidos_lst)} vencido(s) · "
            f"{len(avencer_lst)} a vencer · "
            f"{len(pagos_lst)} pago(s)"
        )
        fin_label_pill = "Regular"
    else:
        fin_value = "Regular"
        fin_tone = "ok"
        fin_detail = "Nenhum título financeiro vinculado à captura."
        fin_label_pill = "Regular"

    if recusados or vencidos:
        risco_value = "Alto"
        risco_tone = "critical"
        risco_detail = "Documentação recusada ou títulos vencidos requerem ação imediata."
        risco_pill = "Crítico"
    elif pendentes:
        risco_value = "Moderado"
        risco_tone = "warning"
        risco_detail = "Pendências documentais identificadas. Ação recomendada em breve."
        risco_pill = "Atenção"
    else:
        risco_value = "Baixo"
        risco_tone = "ok"
        risco_detail = "Nenhuma pendência crítica identificada na consulta atual."
        risco_pill = "Regular"

    if recusados:
        prox_value = "Reenvio de documentação"
        prox_detail = "Entrar em contato para orientar o aluno no reenvio do documento recusado."
        prox_tone = "critical"
        prox_pill = "Urgente"
    elif pendentes:
        prox_value = "Envio de documentação"
        prox_detail = "Solicitar envio da documentação pendente para evitar bloqueios futuros."
        prox_tone = "warning"
        prox_pill = "Atenção"
    elif vencidos:
        prox_value = "Regularização financeira"
        prox_detail = "Orientar regularização financeira para evitar impacto na jornada acadêmica."
        prox_tone = "critical"
        prox_pill = "Urgente"
    else:
        prox_value = "Acompanhamento padrão"
        prox_detail = "Aluno sem fricção crítica identificada. Manter acompanhamento de rotina."
        prox_tone = "ok"
        prox_pill = "Padrão"

    status_geral = _build_status_geral(recusados, pendentes, vencidos)
    status_geral["detail"] = _build_resumo_atendimento(situacao_acad, recusados, pendentes, vencidos)
    resumo_atendimento = status_geral["detail"]

    return {
        "status_geral": status_geral,
        "resumo_atendimento": resumo_atendimento,
        "aluno": {
            "title": "Dados do Aluno",
            "fields": aluno_fields,
            "tone": aluno_tone,
            "status_label": sit_d if sit_d != _NA else "",
        },
        "risco": {
            "title": "Risco",
            "value": risco_value,
            "detail": risco_detail,
            "tone": risco_tone,
            "status_label": risco_pill,
        },
        "documentacao": {
            "title": "Documentação",
            "value": doc_value,
            "detail": doc_detail,
            "tone": doc_tone,
            "status_label": doc_label_pill,
        },
        "financeiro": {
            "title": "Financeiro",
            "value": fin_value,
            "detail": fin_detail,
            "tone": fin_tone,
            "status_label": fin_label_pill,
        },
        "proxima_acao": {
            "title": "Próxima Melhor Ação",
            "value": prox_value,
            "detail": prox_detail,
            "tone": prox_tone,
            "status_label": prox_pill,
        },
        "siaa_online": fonte == "siaa_http",
        "meta": {
            "captura_id": captura.get("id"),
            "documentos_count": len(documentos),
            "titulos_count": len(titulos),
            "ultima_consulta": ultima_consulta,
        },
        "financeiro_detalhe": {
            "vencidos": {
                "tem": bool(vencidos_lst),
                "count": len(vencidos_lst),
                "total": sum(_to_num(t.get("total")) for t in vencidos_lst),
                "itens": [
                    {
                        "tipo_titulo": t.get("tipo_titulo", ""),
                        "numero_titulo": t.get("numero_titulo", ""),
                        "vencimento": t.get("vencimento", ""),
                        "atraso": t.get("atraso", ""),
                        "total": t.get("total"),
                    }
                    for t in vencidos_lst
                ],
            },
            "a_vencer": {
                "count": len(avencer_lst),
                "total": sum(_to_num(t.get("total")) for t in avencer_lst),
                "itens": [
                    {
                        "tipo_titulo": t.get("tipo_titulo", ""),
                        "numero_titulo": t.get("numero_titulo", ""),
                        "vencimento": t.get("vencimento", ""),
                        "total": t.get("total"),
                    }
                    for t in avencer_lst
                ],
            },
            "pagos": {
                "count": len(pagos_lst),
                "itens": [
                    {
                        "tipo_titulo": t.get("tipo_titulo", ""),
                        "numero_titulo": t.get("numero_titulo", ""),
                        "data_pagamento": t.get("data_pagamento", ""),
                        "valor_pago": t.get("valor_pago"),
                        "tipo_pagamento": t.get("tipo_pagamento", ""),
                        "atraso": t.get("atraso", ""),
                    }
                    for t in pagos_lst
                ],
                "analise": analisar_pagamentos(titulos),
            },
        },
    }
