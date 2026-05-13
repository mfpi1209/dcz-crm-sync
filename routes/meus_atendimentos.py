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
from datetime import date as _date, datetime, timedelta

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
#
# IMPORTANTE: o webhook do n8n passou a devolver corpo vazio quando recebe
# o parâmetro `consultor`. Por isso sempre buscamos o payload global (sem
# filtro) — que já traz `consultores[]` com `metricas` e `serie_dia` por
# consultor — e fazemos o filtro localmente. Como bônus, isso permite que
# múltiplos consultores no mesmo período compartilhem o mesmo cache.
# ---------------------------------------------------------------------------
def _call_webhook(start: str, end: str, consultor: str | None = None,
                  tipo: str | None = None, top_n: int = 5) -> dict:
    # `consultor` é mantido na assinatura por compatibilidade, mas NÃO é
    # enviado ao n8n. O filtro acontece em `_extract_consultor_payload`.
    _ = consultor  # noqa: F841 (intencional)
    cache_key = (start, end, "__GLOBAL__", tipo or "", int(top_n))
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
    """Agrega listas de métricas (estilo `global` ou `metricas` por consultor).

    Os campos `tempo_medio_*` são ponderados pelo `total_atendimentos` apenas
    dos itens em que estão definidos — assim, se nenhum item informa o tempo,
    o resultado fica `None` (em vez de zero, que seria interpretado pelo
    frontend como "dentro da meta").
    """
    items = [i for i in items if isinstance(i, dict)]
    total = sum(_safe_num(i.get("total_atendimentos")) for i in items)
    notas = sum(_safe_num(i.get("notas_informadas")) for i in items)
    nota_sum = sum(
        _safe_num(i.get("nota_media")) * _safe_num(i.get("notas_informadas"))
        for i in items
    )

    def _weighted(field: str) -> float | None:
        with_field = [i for i in items if i.get(field) is not None]
        if not with_field:
            return None
        w = sum(_safe_num(i.get("total_atendimentos")) for i in with_field)
        if w <= 0:
            return None
        s = sum(_safe_num(i.get(field)) * _safe_num(i.get("total_atendimentos")) for i in with_field)
        return s / w

    return {
        "total_atendimentos": int(total),
        "notas_informadas": int(notas),
        "nota_media": (nota_sum / notas) if notas > 0 else None,
        "tempo_medio_resposta_min": _weighted("tempo_medio_resposta_min"),
        "tempo_medio_atendimento_min": _weighted("tempo_medio_atendimento_min"),
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


def _parse_iso_date(s: str) -> _date | None:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _metrics_from_serie(serie: list[dict]) -> dict:
    """Recomputa métricas agregadas a partir de uma `serie_dia` filtrada.

    `tempo_medio_resposta_min` e `tempo_medio_atendimento_min` não estão
    detalhados por dia na resposta do n8n; ficam `None` no recorte.
    """
    total = sum(_safe_num(p.get("atendimentos") or p.get("total") or p.get("qtd")) for p in (serie or []))
    notas = sum(_safe_num(p.get("notas_informadas")) for p in (serie or []))
    nota_sum = sum(
        _safe_num(p.get("nota_media")) * _safe_num(p.get("notas_informadas"))
        for p in (serie or [])
    )
    return {
        "total_atendimentos": int(total),
        "notas_informadas": int(notas),
        "nota_media": (nota_sum / notas) if notas > 0 else None,
        "tempo_medio_resposta_min": None,
        "tempo_medio_atendimento_min": None,
    }


def _filter_payload_by_range(payload: dict, start: str, end: str) -> dict:
    """Recorta `serie_dia` (global e por consultor) para [start, end] inclusive.

    Útil quando precisamos expandir o range pedido ao webhook como workaround
    (ex.: o n8n devolve corpo vazio quando `start == end`).
    """
    if not isinstance(payload, dict):
        return payload
    d0, d1 = _parse_iso_date(start), _parse_iso_date(end)
    if not d0 or not d1:
        return payload

    def _in_range(raw: str | None) -> bool:
        if not raw:
            return False
        d = _parse_iso_date(raw[:10])
        return bool(d and d0 <= d <= d1)

    out = {**payload}

    sg = payload.get("serie_dia_global")
    if isinstance(sg, list):
        out["serie_dia_global"] = [p for p in sg if _in_range(p.get("date") or p.get("data") or p.get("dia"))]

    cons = payload.get("consultores")
    if isinstance(cons, list):
        new_cons: list[dict] = []
        for c in cons:
            if not isinstance(c, dict):
                continue
            sd = c.get("serie_dia") or (c.get("metricas") or {}).get("serie_dia") or []
            filtered = [p for p in sd if _in_range(p.get("date") or p.get("data") or p.get("dia"))]
            orig_metrics = c.get("metricas") or {}

            # Caso feliz: o filtro não removeu nenhum dia → as métricas
            # originais já refletem exatamente o range pedido (preserva
            # `tempo_medio_*` que a serie por dia não detalha).
            if filtered and len(filtered) == len(sd) and orig_metrics:
                metrics_out = orig_metrics
            else:
                recomputed = _metrics_from_serie(filtered)
                if recomputed["total_atendimentos"] <= 0:
                    continue
                # Aproveita os tempos médios originais quando a série filtrada
                # ainda representa a maior parte dos atendimentos do consultor.
                # É uma aproximação melhor do que `None` na maioria dos casos.
                if orig_metrics.get("tempo_medio_resposta_min") is not None:
                    recomputed["tempo_medio_resposta_min"] = orig_metrics["tempo_medio_resposta_min"]
                if orig_metrics.get("tempo_medio_atendimento_min") is not None:
                    recomputed["tempo_medio_atendimento_min"] = orig_metrics["tempo_medio_atendimento_min"]
                metrics_out = recomputed

            if (metrics_out.get("total_atendimentos") or 0) <= 0:
                continue

            new_cons.append({
                **{k: v for k, v in c.items() if k not in ("metricas", "serie_dia")},
                "consultor": c.get("consultor"),
                "metricas": metrics_out,
                "serie_dia": filtered,
            })
        out["consultores"] = new_cons

    # Recomputa o `global` agregado a partir dos consultores filtrados.
    g_metrics = _agg_metrics([c.get("metricas") for c in out.get("consultores") or []])
    out["global"] = g_metrics

    out["_filtered_range"] = {"start": start, "end": end}
    return out


def _call_webhook_with_fallback(start: str, end: str,
                                tipo: str | None = None, top_n: int = 5) -> dict:
    """Wrapper que normaliza o range para o webhook do n8n.

    O webhook trata `end` como **exclusivo** (não inclui o último dia).
    Para o usuário, o esperado é que `[start, end]` seja **inclusivo** nas
    duas pontas — em especial quando `start == end` (consulta de um único
    dia), em que o webhook devolveria corpo vazio.

    Estratégia: chamamos o webhook com `end + 1d` e depois filtramos
    localmente para `[start, end]` inclusive.
    """
    d0, d1 = _parse_iso_date(start), _parse_iso_date(end)
    if not d0 or not d1:
        return _call_webhook(start, end, None, tipo, top_n)

    ext_end = (d1 + timedelta(days=1)).strftime("%Y-%m-%d")
    data = _call_webhook(start, ext_end, None, tipo, top_n)
    if not isinstance(data, dict) or not (data.get("consultores") or data.get("global")):
        return data
    return _filter_payload_by_range(data, start, end)


def _extract_consultor_payload(global_payload: dict, canon_name: str) -> dict:
    """A partir do payload global do n8n, monta a resposta filtrada por consultor.

    Funciona com aliases: se o canônico tiver variantes (ex.: "Felipe" e
    "Felipe Guimarães"), agrega todas elas em um único registro.
    """
    consultores = (global_payload.get("consultores") if isinstance(global_payload, dict) else None) or []
    target_names = {_norm_name(v) for v in CONSULTOR_ALIASES.get(canon_name, [canon_name])}
    target_names.add(_norm_name(canon_name))

    matches: list[dict] = []
    for c in consultores:
        if not isinstance(c, dict):
            continue
        if _norm_name(c.get("consultor")) in target_names:
            matches.append(c)

    if not matches:
        return {
            "ok": True,
            "filtros": {"consultor_filtro": canon_name},
            "global": {
                "total_atendimentos": 0,
                "notas_informadas": 0,
                "nota_media": None,
                "tempo_medio_resposta_min": None,
                "tempo_medio_atendimento_min": None,
            },
            "serie_dia": [],
            "consultor_detalhe": {
                "consultor": canon_name,
                "metricas": {
                    "total_atendimentos": 0,
                    "notas_informadas": 0,
                    "nota_media": None,
                    "tempo_medio_resposta_min": None,
                    "tempo_medio_atendimento_min": None,
                },
                "serie_dia": [],
                "feedback_geral": None,
                "feedback_positivo": None,
                "feedback_negativo": None,
            },
            "_no_match": True,
        }

    metrics_inputs = [(c.get("metricas") or c) for c in matches]
    series_inputs = [c.get("serie_dia") or (c.get("metricas") or {}).get("serie_dia") for c in matches]
    series_inputs = [s for s in series_inputs if s]
    merged_metrics = _agg_metrics(metrics_inputs)
    merged_serie = _agg_series(series_inputs) if series_inputs else []

    fb_geral, fb_pos, fb_neg = [], [], []
    for c in matches:
        m = c.get("metricas") or {}
        for tgt, key in ((fb_geral, "feedback_geral"),
                         (fb_pos, "feedback_positivo"),
                         (fb_neg, "feedback_negativo")):
            v = m.get(key) or c.get(key)
            if v:
                tgt.append(str(v))

    return {
        "ok": True,
        "filtros": {"consultor_filtro": canon_name},
        "global": merged_metrics,
        "serie_dia": merged_serie,
        "consultor_detalhe": {
            "consultor": canon_name,
            "metricas": merged_metrics,
            "serie_dia": merged_serie,
            "feedback_geral": " ".join(fb_geral) if fb_geral else None,
            "feedback_positivo": " ".join(fb_pos) if fb_pos else None,
            "feedback_negativo": " ".join(fb_neg) if fb_neg else None,
        },
        "_merged_from": [c.get("consultor") for c in matches],
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
        # Sempre buscamos o payload global (sem filtro) — o webhook do n8n
        # devolve corpo vazio quando recebe `?consultor=`, então fazemos o
        # filtro localmente a partir de `consultores[]`.
        # Também usa fallback para o bug do n8n de devolver vazio quando
        # `start == end` (consulta de um único dia).
        global_data = _call_webhook_with_fallback(start, end, tipo, top_n)

        if consultor:
            data = _extract_consultor_payload(global_data, consultor)
            # Mantém a lista global de consultores no payload para que o
            # frontend continue populando o seletor de consultor (admin).
            cons_list = (global_data.get("consultores") if isinstance(global_data, dict) else None) or []
            if cons_list:
                data["consultores"] = cons_list
        else:
            data = _dedupe_consultores_global(global_data)

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
