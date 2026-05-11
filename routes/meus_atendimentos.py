"""Painel "Meus Atendimentos" — agrega feedback de consultores (webhook n8n).

Hierarquia:
  - Admin / Supervisor Acadêmico → veem tudo, podem filtrar por consultor.
  - Demais → só veem seus próprios dados (forçado no servidor).

Otimizações:
  - Cache em memória (TTL 3 min) por (start, end, consultor) para evitar
    pressionar repetidamente o webhook do n8n.
  - Unificação de nomes duplicados (ex: "Felipe" e "Felipe Guimarães"
    representam a mesma pessoa). Quando o filtro pede o canônico,
    paralelizamos as chamadas para cada alias e agregamos.
"""

import os
import time
import logging
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Blueprint, request, jsonify, session

from routes.comercial_rgm import (
    _dist_consultor_kommo_uid_for_session,
    _dist_consultor_name_for_kommo_uid,
)

logger = logging.getLogger(__name__)

meus_atendimentos_bp = Blueprint("meus_atendimentos", __name__)

FB_WEBHOOK_URL = os.getenv(
    "FB_WEBHOOK_URL",
    "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/feedback",
)


# ---------------------------------------------------------------------------
# Alias map — nomes que o webhook devolve em formas diferentes para a mesma
# pessoa. Chave: nome canônico (o que aparece para o usuário). Valor: lista
# de variantes que o webhook conhece (consultas paralelas são feitas para
# cada variante).
# ---------------------------------------------------------------------------
CONSULTOR_ALIASES: dict[str, list[str]] = {
    "Felipe Guimarães": ["Felipe Guimarães", "Felipe"],
    "Marilia Souza": ["Marilia Souza", "Marilia"],
}


# Fallback de identidade: quando o usuário não tem kommo_user_id mapeado em
# app_users, resolvemos o nome do consultor a partir do username (email).
# Chave: prefixo do email em minúsculas (antes de "@") → nome canônico que o
# webhook do n8n conhece.
USERNAME_TO_CONSULTOR: dict[str, str] = {
    "beatriz.andrade":   "Beatriz Andrade",
    "danubia.sousa":     "Danubia",
    "debora.moreira":    "Debora Mani Moreira",
    "felipe.guimaraes":  "Felipe Guimarães",
    "joyce.pereira":     "Joyce",
    "julia.rodrigues":   "Julia",
    "mariana.vecoso":    "Mariana",
    "marilia.nascimento": "Marilia Souza",
    "wesley.guerreiro":  "Wesley Guerreiro",
    "camila.santos1876": "Camila Ferreira",
}


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s or "")
        if unicodedata.category(c) != "Mn"
    )


def _norm_name(n: str) -> str:
    return _strip_accents((n or "").strip().lower())


# Reverse map: cada variante → canônico
_ALIAS_REVERSE: dict[str, str] = {}
for canon, variants in CONSULTOR_ALIASES.items():
    _ALIAS_REVERSE[_norm_name(canon)] = canon
    for v in variants:
        _ALIAS_REVERSE[_norm_name(v)] = canon


def _canon_consultor(nome: str | None) -> str | None:
    if not nome:
        return nome
    return _ALIAS_REVERSE.get(_norm_name(nome), nome)


# ---------------------------------------------------------------------------
# Cache em memória — chave (start, end, consultor) → (timestamp, payload).
# Mantém o backend responsivo após a primeira consulta e amortece os tempos
# de timeout do n8n.
# ---------------------------------------------------------------------------
_CACHE_TTL = 180  # segundos
_CACHE: dict[tuple, tuple[float, dict]] = {}
_CACHE_LOCK = threading.Lock()


def _cache_get(key: tuple) -> dict | None:
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
    if not entry:
        return None
    ts, data = entry
    if (time.time() - ts) > _CACHE_TTL:
        with _CACHE_LOCK:
            _CACHE.pop(key, None)
        return None
    return data


def _cache_set(key: tuple, data: dict) -> None:
    with _CACHE_LOCK:
        _CACHE[key] = (time.time(), data)


# ---------------------------------------------------------------------------
# Chamada bruta ao webhook (com retry / timeout escalonado).
# ---------------------------------------------------------------------------
def _call_webhook(start: str, end: str, consultor: str | None,
                  tipo: str | None = None, top_n: int = 5) -> dict:
    cache_key = (start, end, (consultor or "").strip(), tipo or "", int(top_n))
    cached = _cache_get(cache_key)
    if cached is not None:
        return {**cached, "_cached": True}

    params: dict = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if tipo:
        params["tipo"] = tipo
    params["topN"] = top_n
    if consultor:
        params["consultor"] = consultor

    last_exc: Exception | None = None
    for attempt in (1, 2):
        try:
            timeout = 60 if attempt == 1 else 90
            r = requests.get(FB_WEBHOOK_URL, params=params, timeout=timeout, verify=False)
            r.raise_for_status()
            try:
                data = r.json()
            except ValueError:
                data = {"raw": r.text}
            if not isinstance(data, dict):
                data = {"raw": data}
            data["_attempt"] = attempt
            _cache_set(cache_key, data)
            return data
        except requests.Timeout as e:
            last_exc = e
            logger.warning("meus-atendimentos webhook timeout (try %d/2): %s", attempt, e)
            continue
        except requests.RequestException as e:
            last_exc = e
            logger.warning("meus-atendimentos webhook proxy: %s", e)
            break

    raise last_exc or RuntimeError("Falha no webhook")


# ---------------------------------------------------------------------------
# Agregadores — quando precisamos somar vários "consultores" (aliases).
# ---------------------------------------------------------------------------
def _safe_num(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _agg_metrics(items: list[dict]) -> dict:
    """Agrega listas de métricas (estilo `global` ou `metricas` por consultor)."""
    total = sum(_safe_num(i.get("total_atendimentos")) for i in items)
    notas = sum(_safe_num(i.get("notas_informadas")) for i in items)
    nota_sum = sum(
        _safe_num(i.get("nota_media")) * _safe_num(i.get("notas_informadas"))
        for i in items
    )
    tr_sum = sum(
        _safe_num(i.get("tempo_medio_resposta_min")) * _safe_num(i.get("total_atendimentos"))
        for i in items
    )
    ta_sum = sum(
        _safe_num(i.get("tempo_medio_atendimento_min")) * _safe_num(i.get("total_atendimentos"))
        for i in items
    )
    return {
        "total_atendimentos": int(total),
        "notas_informadas": int(notas),
        "nota_media": (nota_sum / notas) if notas > 0 else None,
        "tempo_medio_resposta_min": (tr_sum / total) if total > 0 else None,
        "tempo_medio_atendimento_min": (ta_sum / total) if total > 0 else None,
    }


def _agg_series(series_list: list[list[dict]]) -> list[dict]:
    """Mescla várias `serie_dia` em uma única, somando atendimentos por data."""
    bucket: dict[str, dict] = {}
    for serie in series_list:
        for p in (serie or []):
            d = p.get("date") or p.get("data") or p.get("dia")
            if not d:
                continue
            cur = bucket.setdefault(d, {
                "date": d, "atendimentos": 0, "notas_informadas": 0,
                "_nota_sum": 0.0, "_tr_sum": 0.0, "_ta_sum": 0.0,
            })
            at = _safe_num(p.get("atendimentos") or p.get("total") or p.get("qtd"))
            ni = _safe_num(p.get("notas_informadas"))
            cur["atendimentos"] += int(at)
            cur["notas_informadas"] += int(ni)
            if p.get("nota_media") is not None:
                cur["_nota_sum"] += _safe_num(p.get("nota_media")) * ni
            if p.get("tempo_medio_resposta_min") is not None:
                cur["_tr_sum"] += _safe_num(p.get("tempo_medio_resposta_min")) * at
            if p.get("tempo_medio_atendimento_min") is not None:
                cur["_ta_sum"] += _safe_num(p.get("tempo_medio_atendimento_min")) * at

    out: list[dict] = []
    for d in sorted(bucket.keys()):
        c = bucket[d]
        out.append({
            "date": d,
            "atendimentos": c["atendimentos"],
            "notas_informadas": c["notas_informadas"],
            "nota_media": (c["_nota_sum"] / c["notas_informadas"]) if c["notas_informadas"] else None,
            "tempo_medio_resposta_min": (c["_tr_sum"] / c["atendimentos"]) if c["atendimentos"] else None,
            "tempo_medio_atendimento_min": (c["_ta_sum"] / c["atendimentos"]) if c["atendimentos"] else None,
        })
    return out


def _merge_payloads_for_canon(payloads: list[dict], canon_name: str) -> dict:
    """Unifica respostas do webhook (uma por alias) em um único payload."""
    metrics_inputs: list[dict] = []
    series_inputs: list[list[dict]] = []
    feedback_geral, feedback_pos, feedback_neg = [], [], []

    for p in payloads:
        if not isinstance(p, dict):
            continue
        # Quando o webhook é chamado com ?consultor=, ele devolve o detalhe
        # diretamente em `global` (e a `serie_dia` no topo). Quando vem em
        # `consultor_detalhe.metricas`, usamos isso.
        det = p.get("consultor_detalhe") or p.get("detalhe")
        if det:
            m = det.get("metricas") or det
            metrics_inputs.append(m)
            sd = det.get("serie_dia") or (det.get("metricas") or {}).get("serie_dia") or p.get("serie_dia")
            if sd:
                series_inputs.append(sd)
            for tgt, key in ((feedback_geral, "feedback_geral"),
                             (feedback_pos, "feedback_positivo"),
                             (feedback_neg, "feedback_negativo")):
                v = m.get(key) or det.get(key)
                if v:
                    tgt.append(str(v))
        else:
            g = p.get("global") or {}
            metrics_inputs.append(g)
            sd = p.get("serie_dia")
            if sd:
                series_inputs.append(sd)
            for tgt, key in ((feedback_geral, "feedback_geral"),
                             (feedback_pos, "feedback_positivo"),
                             (feedback_neg, "feedback_negativo")):
                v = g.get(key)
                if v:
                    tgt.append(str(v))

    merged_metrics = _agg_metrics(metrics_inputs)
    merged_serie = _agg_series(series_inputs)

    return {
        "ok": True,
        "filtros": {"consultor_filtro": canon_name},
        "global": merged_metrics,
        "serie_dia": merged_serie,
        "consultor_detalhe": {
            "consultor": canon_name,
            "metricas": merged_metrics,
            "serie_dia": merged_serie,
            "feedback_geral": " ".join(feedback_geral) if feedback_geral else None,
            "feedback_positivo": " ".join(feedback_pos) if feedback_pos else None,
            "feedback_negativo": " ".join(feedback_neg) if feedback_neg else None,
        },
        "_merged_from": [v for c, vs in CONSULTOR_ALIASES.items() if c == canon_name for v in vs],
    }


def _dedupe_consultores_global(payload: dict) -> dict:
    """Quando NÃO há filtro de consultor, agrega `consultores[]` aplicando
    o mapa de aliases (ex.: "Felipe" + "Felipe Guimarães" → "Felipe Guimarães")."""
    if not isinstance(payload, dict):
        return payload
    consultores = payload.get("consultores")
    if not isinstance(consultores, list) or not consultores:
        return payload

    groups: dict[str, list[dict]] = {}
    for c in consultores:
        if not isinstance(c, dict):
            continue
        canon = _canon_consultor(c.get("consultor"))
        groups.setdefault(canon, []).append(c)

    out_list: list[dict] = []
    for canon, items in groups.items():
        if len(items) == 1 and canon == items[0].get("consultor"):
            out_list.append(items[0])
            continue
        # Agrega múltiplas variantes em um único registro canônico.
        metrics_inputs = [(i.get("metricas") or i) for i in items]
        series_inputs = [
            (i.get("metricas") or {}).get("serie_dia") or i.get("serie_dia")
            for i in items
        ]
        series_inputs = [s for s in series_inputs if s]
        merged = _agg_metrics(metrics_inputs)
        sample = items[0]
        out_list.append({
            **{k: v for k, v in sample.items() if k not in (
                "consultor", "metricas", "serie_dia",
                "total_atendimentos", "notas_informadas", "nota_media",
                "tempo_medio_resposta_min", "tempo_medio_atendimento_min",
            )},
            "consultor": canon,
            "metricas": merged,
            **merged,  # facilita acesso direto também
            "serie_dia": _agg_series(series_inputs) if series_inputs else None,
            "_aliases": [i.get("consultor") for i in items if i.get("consultor") != canon],
        })

    out_list.sort(key=lambda x: x.get("total_atendimentos") or 0, reverse=True)
    payload = {**payload, "consultores": out_list}
    return payload


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------
def _ma_categoria() -> str:
    return (session.get("categoria") or "").strip().lower()


def _ma_is_supervisor_academico() -> bool:
    return _ma_categoria() in ("supervisor acadêmico", "supervisor academico")


def _ma_is_privileged() -> bool:
    return session.get("role") == "admin" or _ma_is_supervisor_academico()


def _consultor_name_from_username(username: str | None) -> str | None:
    """Fallback: extrai o consultor a partir do email/username do app_users.

    Útil quando o usuário ainda não tem `kommo_user_id` populado e por isso
    `_dist_consultor_name_for_kommo_uid` não consegue resolver."""
    if not username:
        return None
    prefix = username.split("@", 1)[0].strip().lower()
    if not prefix:
        return None
    mapped = USERNAME_TO_CONSULTOR.get(prefix)
    if mapped:
        return mapped
    # heurística: usa o primeiro nome do prefixo como tentativa
    first = prefix.replace(".", " ").split()[0]
    return first.capitalize() if first else None


def _ma_session_identity() -> dict:
    privileged = _ma_is_privileged()
    uid = _dist_consultor_kommo_uid_for_session()
    nome = None
    if not privileged:
        nome = _dist_consultor_name_for_kommo_uid(uid)
        if not nome:
            nome = _consultor_name_from_username(session.get("username"))
    return {
        "is_admin": privileged,
        "kommo_user_id": uid,
        "consultor_nome": nome,
        "categoria": session.get("categoria") or None,
        "username": session.get("username") or None,
    }


@meus_atendimentos_bp.route("/api/meus-atendimentos/me")
def meus_atendimentos_me():
    info = _ma_session_identity()
    return jsonify({
        "ok": True,
        "is_admin": info["is_admin"],
        "kommo_user_id": info["kommo_user_id"],
        "consultor_nome": _canon_consultor(info["consultor_nome"]),
        "categoria": info["categoria"],
    })


@meus_atendimentos_bp.route("/api/meus-atendimentos")
def meus_atendimentos_data():
    info = _ma_session_identity()
    consultor_arg = (request.args.get("consultor") or "").strip()

    if info["is_admin"]:
        consultor = _canon_consultor(consultor_arg) if consultor_arg else None
    else:
        nome = info["consultor_nome"]
        if not nome:
            return jsonify({
                "ok": True,
                "forced": True,
                "consultor": None,
                "global": {},
                "consultores": [],
                "consultor_detalhe": None,
                "_acl_message": "Usuário sem kommo_user_id mapeado.",
            })
        consultor = _canon_consultor(nome)

    start = (request.args.get("start") or "").strip()
    end = (request.args.get("end") or "").strip()
    tipo = (request.args.get("tipo") or "").strip() or None
    try:
        top_n = int(request.args.get("topN") or 5)
    except ValueError:
        top_n = 5

    try:
        if consultor and consultor in CONSULTOR_ALIASES:
            variants = CONSULTOR_ALIASES[consultor]
            with ThreadPoolExecutor(max_workers=max(2, len(variants))) as ex:
                results = list(ex.map(
                    lambda v: _call_webhook(start, end, v, tipo, top_n),
                    variants,
                ))
            data = _merge_payloads_for_canon(results, consultor)
        else:
            data = _call_webhook(start, end, consultor, tipo, top_n)
            if not consultor:
                data = _dedupe_consultores_global(data)
            else:
                # Garante o filtros.consultor_filtro consistente para o front.
                data = {**data, "filtros": {**(data.get("filtros") or {}), "consultor_filtro": consultor}}

        data = {**data}
        data.setdefault("ok", True)
        data["_acl_consultor"] = consultor
        data["_acl_forced"] = not info["is_admin"]
        return jsonify(data)
    except requests.Timeout as e:
        logger.warning("meus-atendimentos final timeout: %s", e)
        return jsonify({
            "ok": False,
            "error": str(e),
            "hint": "O webhook do n8n demorou para responder. Tente um período menor (até 7 dias) ou filtre por consultor.",
        }), 502
    except requests.RequestException as e:
        return jsonify({"ok": False, "error": str(e)}), 502
    except Exception as e:
        logger.exception("meus-atendimentos unexpected: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500
