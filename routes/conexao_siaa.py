"""Conexão SIAA: status, histórico, agenda e pausa manual da session-api.

O navegador nunca fala com a session-api nem vê o token: estas rotas repassam
a chamada com `Authorization: Bearer <SIAA_SESSION_TOKEN>` e devolvem só os
campos do contrato (lista branca). Ler status/histórico NÃO consulta o SIAA;
a session-api só devolve o que o keepalive registrou.

GET  /api/conexao-siaa/status
GET  /api/conexao-siaa/historico?modulo=&limite=&antes=
GET  /api/conexao-siaa/agenda
PUT  /api/conexao-siaa/agenda          {ativa, dias, inicio, fim, intervalo_s, versao}
POST /api/conexao-siaa/pausa-manual    {ativa, motivo, versao}

Env: SIAA_SESSION_URL, SIAA_SESSION_TOKEN (os mesmos de routes/siaa.py).
Só admin (mesma regra das demais páginas de TI).
"""
from __future__ import annotations

import logging
import os
import re

import requests
from flask import Blueprint, jsonify, request, session

logger = logging.getLogger(__name__)
conexao_siaa_bp = Blueprint("conexao_siaa_bp", __name__)

# (conexão, leitura) em segundos: curto, o painel atualiza a cada ~30 s.
_TIMEOUT = (3, 8)
_MODULOS = ("academico", "financeiro")
_ISO_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$")
_HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


# ---------------------------------------------------------------------------
# Lista branca
# ---------------------------------------------------------------------------

def _s(v, n=200):
    return v[:n] if isinstance(v, str) else None


def _i(v):
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _b(v):
    return v if isinstance(v, bool) else None


def _dias(v):
    if not isinstance(v, list):
        return []
    return [d for d in v if isinstance(d, int) and not isinstance(d, bool) and 0 <= d <= 6][:7]


def _strs(v):
    if not isinstance(v, list):
        return []
    return [x[:80] for x in v if isinstance(x, str)][:20]


def _pick(obj, spec):
    obj = obj if isinstance(obj, dict) else {}
    return {k: fn(obj.get(k)) for k, fn in spec.items()}


def _sub(spec):
    return lambda v: _pick(v, spec)


_RESULTADO = {
    "em": _s, "classe": _s, "motivo": _s, "etapa": _s, "modo": _s,
    "status_http": _i, "duracao_ms": _i, "origem": _s,
}
_MODULO = {
    "estado": _s, "ultima_confirmacao": _s, "ultima_verificacao": _s,
    "proxima_execucao": _s, "modo_usado": _s, "precisa_renovacao": _b,
    "ultimo_resultado": _sub(_RESULTADO),
}
_AGENDA = {
    "ativa": _b, "dias": _dias, "inicio": _s, "fim": _s, "fuso": _s,
    "intervalo_s": _i, "versao": _i, "atualizada_em": _s,
    "aberta_agora": _b, "proxima_mudanca": _s, "ilegivel": _b,
}
_PAUSA = {"ativa": _b, "desde": _s, "motivo": _s, "versao": _i, "ilegivel": _b}
_STATUS = {
    "gerado_em": _s,
    "worker": _sub({"visto_em": _s, "iniciado_em": _s, "modo": _s, "intervalo_s": _i}),
    "agenda": _sub(_AGENDA),
    "pausa_manual": _sub(_PAUSA),
    "disponibilidade": _sub({"estado": _s, "motivo": _s}),
    "modulos": _sub({m: _sub(_MODULO) for m in _MODULOS}),
    "fora_da_agenda": _strs,
}


def _lista_historico(d):
    itens = d.get("itens") if isinstance(d, dict) else None
    spec = dict(_RESULTADO, modulo=_s)
    out = [_pick(it, spec) for it in (itens if isinstance(itens, list) else [])[:200]]
    return out


# ---------------------------------------------------------------------------
# Chamada à session-api
# ---------------------------------------------------------------------------

def _forbidden():
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return None


def _erro(msg, status):
    return jsonify({"ok": False, "error": msg}), status


def _chamar(method, caminho, params=None, corpo=None):
    """Devolve (status_http_da_api, json|None) ou (None, mensagem_amigavel)."""
    base = (os.environ.get("SIAA_SESSION_URL", "") or "").rstrip("/")
    token = os.environ.get("SIAA_SESSION_TOKEN", "") or ""
    if not base or not token:
        return None, ("Conexão com a session-api não configurada no painel.", 503)
    try:
        resp = requests.request(
            method, f"{base}/conexao/{caminho}",
            params=params, json=corpo,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=_TIMEOUT,
        )
    except requests.Timeout:
        return None, ("A session-api demorou demais para responder.", 504)
    except Exception as exc:  # sem URL/token/corpo no log
        logger.warning("conexao_siaa: falha de rede (%s)", type(exc).__name__)
        return None, ("Não foi possível falar com a session-api.", 502)
    try:
        dados = resp.json()
    except Exception:
        dados = None
    return resp.status_code, dados


def _responder(status, dados, montar):
    """Traduz a resposta da session-api; `montar(dados)` devolve o corpo saneado."""
    if status is None:
        msg, http = dados
        return _erro(msg, http)
    if status == 409:
        atual = _i(dados.get("atual")) if isinstance(dados, dict) else None
        return jsonify({"ok": False, "error": "versao", "atual": atual,
                        "message": "Outra pessoa alterou esta configuração. Os dados foram recarregados."}), 409
    if status == 400:
        det = _s(dados.get("error"), 160) if isinstance(dados, dict) else None
        return _erro(det or "Dados inválidos.", 400)
    if status in (401, 403):
        return _erro("O painel não está autorizado na session-api.", 502)
    if status == 404:
        return _erro("A session-api ainda não oferece este recurso (versão desatualizada).", 502)
    if status != 200 or not isinstance(dados, (dict, list)):
        return _erro("A session-api respondeu de forma inesperada.", 502)
    corpo = montar(dados)
    corpo["ok"] = True
    return jsonify(corpo)


# ---------------------------------------------------------------------------
# Validação de entrada
# ---------------------------------------------------------------------------

def _int_estrito(v):
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _validar_agenda(d):
    if not isinstance(d, dict):
        return None, "corpo: JSON esperado"
    if not isinstance(d.get("ativa"), bool):
        return None, "ativa: booleano esperado"
    dias = d.get("dias")
    if not isinstance(dias, list) or any(_int_estrito(x) is None or not 0 <= x <= 6 for x in dias):
        return None, "dias: valores de 0 a 6"
    if len(set(dias)) != len(dias):
        return None, "dias: repetidos"
    for campo in ("inicio", "fim"):
        valor = d.get(campo)
        if valor is None and not d["ativa"]:
            continue  # agenda desativada pode ficar sem horário
        if not isinstance(valor, str) or not _HHMM.match(valor):
            return None, f"{campo}: use HH:MM"
    iv = d.get("intervalo_s")
    if iv is not None and (_int_estrito(iv) is None or not 60 <= iv <= 1800):
        return None, "intervalo_s: vazio ou de 60 a 1800"
    if d["ativa"] and (not dias or d.get("inicio") == d.get("fim")):
        return None, "ativa: exige ao menos um dia e início diferente do fim"
    if _int_estrito(d.get("versao")) is None:
        return None, "versao: inteiro esperado"
    return {"ativa": d["ativa"], "dias": sorted(dias), "inicio": d.get("inicio"), "fim": d.get("fim"),
            "intervalo_s": iv, "versao": d["versao"]}, None


def _validar_pausa(d):
    if not isinstance(d, dict):
        return None, "corpo: JSON esperado"
    if not isinstance(d.get("ativa"), bool):
        return None, "ativa: booleano esperado"
    motivo = d.get("motivo")
    if motivo is not None and (not isinstance(motivo, str) or len(motivo) > 200):
        return None, "motivo: texto de até 200 caracteres"
    if _int_estrito(d.get("versao")) is None:
        return None, "versao: inteiro esperado"
    return {"ativa": d["ativa"], "motivo": (motivo or "").strip(), "versao": d["versao"]}, None


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@conexao_siaa_bp.route("/api/conexao-siaa/status", methods=["GET"])
def api_conexao_status():
    if (f := _forbidden()) is not None:
        return f
    st, d = _chamar("GET", "status")
    return _responder(st, d, lambda x: _pick(x, _STATUS))


@conexao_siaa_bp.route("/api/conexao-siaa/historico", methods=["GET"])
def api_conexao_historico():
    if (f := _forbidden()) is not None:
        return f
    params = {}
    modulo = (request.args.get("modulo") or "").strip().lower()
    if modulo:
        if modulo not in _MODULOS:
            return _erro("modulo: use academico ou financeiro.", 400)
        params["modulo"] = modulo
    limite = request.args.get("limite")
    if limite not in (None, ""):
        try:
            n = int(limite)
        except ValueError:
            return _erro("limite: número de 1 a 200.", 400)
        if not 1 <= n <= 200:
            return _erro("limite: número de 1 a 200.", 400)
        params["limite"] = n
    antes = (request.args.get("antes") or "").strip()
    if antes:
        if not _ISO_Z.match(antes):
            return _erro("antes: data ISO 8601 em UTC (…Z).", 400)
        params["antes"] = antes
    st, d = _chamar("GET", "historico", params=params)

    def montar(x):
        prox = _s(x.get("proximo_antes")) if isinstance(x, dict) else None
        return {"itens": _lista_historico(x), "proximo_antes": prox}

    return _responder(st, d, montar)


@conexao_siaa_bp.route("/api/conexao-siaa/agenda", methods=["GET"])
def api_conexao_agenda_get():
    if (f := _forbidden()) is not None:
        return f
    st, d = _chamar("GET", "agenda")
    return _responder(st, d, lambda x: {"agenda": _pick(x.get("agenda") if isinstance(x, dict) else None, _AGENDA)})


@conexao_siaa_bp.route("/api/conexao-siaa/agenda", methods=["PUT"])
def api_conexao_agenda_put():
    if (f := _forbidden()) is not None:
        return f
    corpo, erro = _validar_agenda(request.get_json(silent=True))
    if erro:
        return _erro(erro, 400)
    st, d = _chamar("PUT", "agenda", corpo=corpo)

    def montar(x):
        bruto = x.get("agenda") if isinstance(x, dict) and isinstance(x.get("agenda"), dict) else x
        return {"agenda": _pick(bruto, _AGENDA)}

    return _responder(st, d, montar)


@conexao_siaa_bp.route("/api/conexao-siaa/pausa-manual", methods=["POST"])
def api_conexao_pausa_manual():
    if (f := _forbidden()) is not None:
        return f
    corpo, erro = _validar_pausa(request.get_json(silent=True))
    if erro:
        return _erro(erro, 400)
    st, d = _chamar("POST", "pausa-manual", corpo=corpo)

    def montar(x):
        bruto = x.get("pausa_manual") if isinstance(x, dict) and isinstance(x.get("pausa_manual"), dict) else x
        return {"pausa_manual": _pick(bruto, _PAUSA)}

    return _responder(st, d, montar)
