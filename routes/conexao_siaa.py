"""Conexão SIAA: status, histórico, agenda e pausa manual da session-api.

O navegador nunca fala com a session-api nem vê o token: estas rotas repassam
a chamada com `Authorization: Bearer <SIAA_SESSION_TOKEN>` e devolvem só os
campos do contrato (lista branca). Ler status/histórico NÃO consulta o SIAA;
a session-api só devolve o que o keepalive registrou.

GET  /api/conexao-siaa/status
GET  /api/conexao-siaa/historico?modulo=&limite=&antes=
GET  /api/conexao-siaa/agenda
PUT  /api/conexao-siaa/agenda          {ativa, dias, inicio, fim, intervalo_s, versao,
                                        intervalo_modo, intervalo_min_min, intervalo_max_min}
POST /api/conexao-siaa/pausa-manual    {ativa, motivo, versao}
GET  /api/conexao-siaa/diagnostico?periodo=6h|24h|3d|7d   (download JSON)

Renovação automática (repasse de /renovacao/* da session-api; as rotas do
executor NUNCA são expostas aqui):
GET  /api/conexao-siaa/renovacao/status
GET  /api/conexao-siaa/renovacao/execucoes?limite=&antes=
PUT  /api/conexao-siaa/renovacao/config      {ativa, versao}
POST /api/conexao-siaa/renovacao/suspensao   {suspensa, versao}
POST /api/conexao-siaa/renovacao/solicitar   {pedido_id, confirmacao}
POST /api/conexao-siaa/renovacao/liberar     {episodio_id, versao}

Renovação seletiva por módulo (aditivo, contrato 2026-11-19): status ganha
`alvos`, episódio `modulos_estado`, execução `modulos_solicitados`/`resultado`
e `modulos[m].situacao`, liberar `modulos_liberados`. Ausentes (backend antigo)
continuam null. Corpos dos POSTs não mudam.

Env: SIAA_SESSION_URL, SIAA_SESSION_TOKEN (os mesmos de routes/siaa.py).
Só admin (mesma regra das demais páginas de TI).
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import requests
from flask import Blueprint, Response, jsonify, request, session

logger = logging.getLogger(__name__)
conexao_siaa_bp = Blueprint("conexao_siaa_bp", __name__)

# (conexão, leitura) em segundos: curto, o painel atualiza a cada ~30 s.
_TIMEOUT = (3, 8)
# Renovação automática: só acadêmico e financeiro.
_MODULOS = ("academico", "financeiro")
# Estado/histórico/diagnóstico da conexão: inclui o módulo manual de inscrições.
_MODULOS_STATUS = ("academico", "financeiro", "inscricoes")
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
    "intervalo_modo": lambda v: _enum(v), "intervalo_min_min": _i, "intervalo_max_min": _i,
    "aberta_agora": _b, "proxima_mudanca": _s, "ilegivel": _b,
}
_PAUSA = {"ativa": _b, "desde": _s, "motivo": _s, "versao": _i, "ilegivel": _b}
_STATUS = {
    "gerado_em": _s,
    "worker": _sub({
        "visto_em": _s, "iniciado_em": _s, "modo": _s, "intervalo_s": _i,
        "intervalo_modo": lambda v: _enum(v), "intervalo_min_s": _i, "intervalo_max_s": _i,
        "intervalo_sorteado_s": _i, "proxima_execucao": _s,
    }),
    "agenda": _sub(_AGENDA),
    "pausa_manual": _sub(_PAUSA),
    "disponibilidade": _sub({"estado": _s, "motivo": _s}),
    "modulos": _sub({m: _sub(_MODULO) for m in _MODULOS_STATUS}),
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


def _chamar(method, caminho, params=None, corpo=None, prefixo="conexao", timeout=_TIMEOUT):
    """Devolve (status_http_da_api, json|None) ou (None, mensagem_amigavel)."""
    base = (os.environ.get("SIAA_SESSION_URL", "") or "").rstrip("/")
    token = os.environ.get("SIAA_SESSION_TOKEN", "") or ""
    if not base or not token:
        return None, ("Conexão com a session-api não configurada no painel.", 503)
    try:
        resp = requests.request(
            method, f"{base}/{prefixo}/{caminho}",
            params=params, json=corpo,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            timeout=timeout,
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


_MSG_409 = {
    "execucao_ativa": "Já existe uma renovação em andamento.",
    "pausa_manual": "A pausa manual está ativa; desative-a antes de renovar.",
}


def _responder(status, dados, montar, aceitos=(200,), generico_400=False):
    """Traduz a resposta da session-api; `montar(dados)` devolve o corpo saneado."""
    if status is None:
        msg, http = dados
        return _erro(msg, http)
    if status == 409:
        d = dados if isinstance(dados, dict) else {}
        codigo = d.get("error") if isinstance(d.get("error"), str) else "versao"
        if codigo in _MSG_409:
            return jsonify({"ok": False, "error": codigo, "message": _MSG_409[codigo]}), 409
        return jsonify({"ok": False, "error": "versao", "atual": _i(d.get("atual")),
                        "message": "Outra pessoa alterou esta configuração. Os dados foram recarregados."}), 409
    if status == 400:
        if generico_400:
            return _erro("Dados inválidos.", 400)
        det = _s(dados.get("error"), 160) if isinstance(dados, dict) else None
        return _erro(det or "Dados inválidos.", 400)
    if status in (401, 403):
        return _erro("O painel não está autorizado na session-api.", 502)
    if status == 404:
        return _erro("A session-api ainda não oferece este recurso (versão desatualizada).", 502)
    if status not in aceitos or not isinstance(dados, (dict, list)):
        return _erro("A session-api respondeu de forma inesperada.", 502)
    corpo = montar(dados)
    corpo["ok"] = True
    resp = jsonify(corpo)
    resp.status_code = status
    return resp


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
    corpo = {"ativa": d["ativa"], "dias": sorted(dias), "inicio": d.get("inicio"), "fim": d.get("fim"),
             "intervalo_s": iv, "versao": d["versao"]}
    # Intervalo aleatório (aditivo): sem `intervalo_modo` o corpo é o de sempre (fixo).
    modo = d.get("intervalo_modo")
    if modo is None and "intervalo_modo" not in d:
        if d.get("intervalo_min_min") is not None or d.get("intervalo_max_min") is not None:
            return None, "intervalo_modo: informe fixo ou aleatorio"
        return corpo, None
    if modo not in ("fixo", "aleatorio"):
        return None, "intervalo_modo: use fixo ou aleatorio"
    mn, mx = d.get("intervalo_min_min"), d.get("intervalo_max_min")
    if modo == "aleatorio":
        if _int_estrito(mn) is None or _int_estrito(mx) is None:
            return None, "intervalo aleatório: mínimo e máximo em minutos (inteiros)"
        if not (1 <= mn <= 30 and 1 <= mx <= 30):
            return None, "intervalo aleatório: de 1 a 30 minutos"
        if mn > mx:
            return None, "intervalo aleatório: o mínimo não pode passar do máximo"
    else:  # fixo: valores guardados só seguem se válidos; senão null (ignorados pelo serviço)
        ok = (_int_estrito(mn) is not None and _int_estrito(mx) is not None
              and 1 <= mn <= 30 and 1 <= mx <= 30 and mn <= mx)
        mn, mx = (mn, mx) if ok else (None, None)
    corpo.update({"intervalo_modo": modo, "intervalo_min_min": mn, "intervalo_max_min": mx})
    return corpo, None


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
        if modulo not in _MODULOS_STATUS:
            return _erro("modulo: use academico, financeiro ou inscricoes.", 400)
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


# ---------------------------------------------------------------------------
# Renovação automática: lista branca estrita (por tipo)
# ---------------------------------------------------------------------------

_ENUM = re.compile(r"^[a-z0-9_]{1,40}$")
_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_PEDIDO = re.compile(r"^[0-9a-f]{32}$")


def _enum(v):
    """Só aceita rótulos de lista fixa (minúsculas, dígitos e _): valor com cara de cookie é descartado."""
    return v if isinstance(v, str) and _ENUM.match(v) else None


def _id(v):
    return v if isinstance(v, str) and _ID.match(v) else None


def _iso(v):
    return v if isinstance(v, str) and _ISO_Z.match(v) else None


def _mods(v):
    if not isinstance(v, list):
        return []
    out = []
    for m in v:
        if m in _MODULOS and m not in out:
            out.append(m)
    return out


def _mods_opt(v):
    """Lista de módulos que pode faltar: ausente (backend antigo) continua null, nunca vira []."""
    return _mods(v) if isinstance(v, list) else None


# Renovação seletiva (aditivo): estado por módulo do episódio e alvos de cada controle.
_R_MOD_ESTADO = {
    "tentativas_automaticas": _i, "bloqueado": _b, "bloqueio_motivo": _enum,
    "resolvido": _b, "resolvido_em": _iso, "liberado_em": _iso,
}


def _modulos_estado(v):
    if not isinstance(v, dict):
        return None
    return {m: _pick(v[m], _R_MOD_ESTADO) for m in _MODULOS if isinstance(v.get(m), dict)}


def _alvos(v):
    if not isinstance(v, dict):
        return None
    return {k: _mods_opt(v.get(k)) for k in ("renovar_agora", "liberar", "automatica")}


def _opt(spec):
    """Sub-objeto que pode ser null: não vira dicionário de nulos."""
    return lambda v: _pick(v, spec) if isinstance(v, dict) else None


_R_MOD = {"coleta": _enum, "http": _i, "publicacao": _enum, "vinculo": _enum, "sessao_validada": _b,
          "situacao": _enum, "motivo_situacao": _enum}
_R_EXEC = {
    "id": _id, "tipo": _enum, "estado": _enum,
    "gatilho": _sub({"modulos": _mods, "motivo": _enum}),
    "criada_em": _iso, "inicio": _iso, "fim": _iso, "duracao_ms": _i,
    "ultimo_progresso": _opt({"etapa": _enum, "em": _iso}),
    "janela_fechou_durante": _b, "login": _enum,
    "modulos": _sub({m: _sub(_R_MOD) for m in _MODULOS}),
    "erro": _enum, "proxima_acao": _enum,
    "modulos_solicitados": _mods_opt, "resultado": _enum,
}
_R_CONFIG = {"ativa": _b, "suspensa": _b, "versao": _i, "atualizada_em": _iso}
_R_EPISODIO = {
    "id": _id, "desde": _iso, "modulos": _mods, "motivo": _enum, "tentativas_automaticas": _i,
    "bloqueado": _b, "bloqueio_motivo": _enum, "liberado_em": _iso,
    "modulos_estado": _modulos_estado,
}
_R_STATUS = {
    "gerado_em": _iso, "estado": _enum, "motivo_estado": _s,
    "config": _sub(_R_CONFIG),
    "executor": _sub({"visto_em": _iso, "ocupado": _b}),
    "episodio": _opt(_R_EPISODIO),
    "execucao_atual": _opt(_R_EXEC),
    "ultima_execucao": _opt(_R_EXEC),
    "ultima_renovacao_ok": _iso, "proxima_acao": _enum,
    "alvos": _alvos,
}
_MOTIVO_ESTADO_MAX = 120


def _montar_status(x):
    out = _pick(x, _R_STATUS)
    if isinstance(out.get("motivo_estado"), str):
        out["motivo_estado"] = out["motivo_estado"][:_MOTIVO_ESTADO_MAX]
    return out


def _montar_execucoes(x):
    itens = x.get("itens") if isinstance(x, dict) else None
    lista = [_pick(it, _R_EXEC) for it in (itens if isinstance(itens, list) else [])[:100]]
    return {"itens": lista, "proximo_antes": _iso(x.get("proximo_antes")) if isinstance(x, dict) else None}


def _aninhado(x, chave, spec):
    bruto = x.get(chave) if isinstance(x, dict) and isinstance(x.get(chave), dict) else x
    return {chave: _pick(bruto, spec)}


def _montar_liberar(x):
    out = _aninhado(x, "episodio", _R_EPISODIO)
    lib = x.get("modulos_liberados") if isinstance(x, dict) else None
    if lib is None and isinstance(x, dict) and isinstance(x.get("episodio"), dict):
        lib = x["episodio"].get("modulos_liberados")
    out["modulos_liberados"] = _mods_opt(lib)
    return out


def _validar_config_renov(d, campo):
    if not isinstance(d, dict):
        return None, "corpo: JSON esperado"
    if not isinstance(d.get(campo), bool):
        return None, f"{campo}: booleano esperado"
    if _int_estrito(d.get("versao")) is None or d["versao"] < 0:
        return None, "versao: inteiro esperado"
    return {campo: d[campo], "versao": d["versao"]}, None


def _validar_solicitar(d):
    if not isinstance(d, dict):
        return None, "corpo: JSON esperado"
    pid = d.get("pedido_id")
    if not isinstance(pid, str) or not _PEDIDO.match(pid.lower()):
        return None, "pedido_id: 32 caracteres hexadecimais"
    if d.get("confirmacao") != "substituir_sessoes":
        return None, "confirmacao: confirme a substituição das sessões"
    return {"pedido_id": pid.lower(), "confirmacao": "substituir_sessoes"}, None


def _validar_liberar(d):
    if not isinstance(d, dict):
        return None, "corpo: JSON esperado"
    ep = d.get("episodio_id")
    if not isinstance(ep, str) or not _ID.match(ep):
        return None, "episodio_id: identificador inválido"
    if _int_estrito(d.get("versao")) is None or d["versao"] < 0:
        return None, "versao: inteiro esperado"
    return {"episodio_id": ep, "versao": d["versao"]}, None


# ---------------------------------------------------------------------------
# Renovação automática: rotas (admin; nada de /renovacao/executor/*)
# ---------------------------------------------------------------------------

def _renov(method, caminho, params=None, corpo=None):
    return _chamar(method, caminho, params=params, corpo=corpo, prefixo="renovacao")


@conexao_siaa_bp.route("/api/conexao-siaa/renovacao/status", methods=["GET"])
def api_renov_status():
    if (f := _forbidden()) is not None:
        return f
    st, d = _renov("GET", "status")
    return _responder(st, d, _montar_status, generico_400=True)


@conexao_siaa_bp.route("/api/conexao-siaa/renovacao/execucoes", methods=["GET"])
def api_renov_execucoes():
    if (f := _forbidden()) is not None:
        return f
    params = {}
    limite = request.args.get("limite")
    if limite not in (None, ""):
        try:
            n = int(limite)
        except ValueError:
            return _erro("limite: número de 1 a 100.", 400)
        if not 1 <= n <= 100:
            return _erro("limite: número de 1 a 100.", 400)
        params["limite"] = n
    antes = (request.args.get("antes") or "").strip()
    if antes:
        if not _ISO_Z.match(antes):
            return _erro("antes: data ISO 8601 em UTC (…Z).", 400)
        params["antes"] = antes
    st, d = _renov("GET", "execucoes", params=params)
    return _responder(st, d, _montar_execucoes, generico_400=True)


@conexao_siaa_bp.route("/api/conexao-siaa/renovacao/config", methods=["PUT"])
def api_renov_config():
    if (f := _forbidden()) is not None:
        return f
    corpo, erro = _validar_config_renov(request.get_json(silent=True), "ativa")
    if erro:
        return _erro(erro, 400)
    st, d = _renov("PUT", "config", corpo=corpo)
    return _responder(st, d, lambda x: _aninhado(x, "config", _R_CONFIG), generico_400=True)


@conexao_siaa_bp.route("/api/conexao-siaa/renovacao/suspensao", methods=["POST"])
def api_renov_suspensao():
    if (f := _forbidden()) is not None:
        return f
    corpo, erro = _validar_config_renov(request.get_json(silent=True), "suspensa")
    if erro:
        return _erro(erro, 400)
    st, d = _renov("POST", "suspensao", corpo=corpo)
    return _responder(st, d, lambda x: _aninhado(x, "config", _R_CONFIG), generico_400=True)


@conexao_siaa_bp.route("/api/conexao-siaa/renovacao/solicitar", methods=["POST"])
def api_renov_solicitar():
    if (f := _forbidden()) is not None:
        return f
    corpo, erro = _validar_solicitar(request.get_json(silent=True))
    if erro:
        return _erro(erro, 400)
    st, d = _renov("POST", "solicitar", corpo=corpo)
    return _responder(st, d, lambda x: {"execucao": _pick(x.get("execucao") if isinstance(x, dict) else None, _R_EXEC)},
                      aceitos=(200, 201), generico_400=True)


@conexao_siaa_bp.route("/api/conexao-siaa/renovacao/liberar", methods=["POST"])
def api_renov_liberar():
    if (f := _forbidden()) is not None:
        return f
    corpo, erro = _validar_liberar(request.get_json(silent=True))
    if erro:
        return _erro(erro, 400)
    st, d = _renov("POST", "liberar", corpo=corpo)
    return _responder(st, d, _montar_liberar, generico_400=True)


# ---------------------------------------------------------------------------
# Exportação de diagnóstico (download JSON, lista branca estrita)
# ---------------------------------------------------------------------------

_PERIODOS = {"6h": timedelta(hours=6), "24h": timedelta(hours=24), "3d": timedelta(days=3), "7d": timedelta(days=7)}
_TIMEOUT_DIAG = (3, 30)
_TOKENISH = re.compile(r"[A-Za-z0-9+/_=-]{24,}|[A-Za-z0-9_.-]+=[^\s]+|Bearer|JSESSIONID|ViewState", re.I)


def _texto(v):
    """Frase curta e legível; descarta o que parece cookie, token ou chave=valor."""
    if not isinstance(v, str) or len(v) > 160 or _TOKENISH.search(v):
        return None
    return v


def _formato(v):
    return v if v == "conexao-siaa-diagnostico" else None


def _hhmm(v):
    return v if isinstance(v, str) and _HHMM.match(v) else None


def _fuso(v):
    return v if isinstance(v, str) and re.match(r"^[A-Za-z_]+/[A-Za-z_]+$", v) else None


def _lista(spec, maximo):
    return lambda v: [_pick(x, spec) for x in v[:maximo]] if isinstance(v, list) else []


_D_RES = {"em": _iso, "classe": _enum, "motivo": _enum, "etapa": _enum, "modo": _enum,
          "status_http": _i, "duracao_ms": _i, "origem": _enum}
_D_MODULO = {
    "estado": _enum, "ultima_confirmacao": _iso, "ultima_verificacao": _iso, "proxima_execucao": _iso,
    "modo_usado": _enum, "precisa_renovacao": _b, "ultimo_resultado": _opt(_D_RES),
}
_D_SONDA = dict(_D_RES, modulo=_enum)
_D_REN_MOD = {"coleta": _enum, "http": _i, "publicacao": _enum, "vinculo": _enum, "sessao_validada": _b,
              "situacao": _enum, "motivo_situacao": _enum}


def _d_gatilho(v):
    if isinstance(v, dict):
        return _pick(v, {"modulos": _mods, "motivo": _enum})
    return _enum(v)


def _d_modulos_ren(v):
    if isinstance(v, dict):
        return {m: _opt(_D_REN_MOD)(v.get(m)) for m in _MODULOS if isinstance(v.get(m), dict)}
    return _mods(v)


_D_RENOVACAO_ITEM = {
    "criada_em": _iso, "inicio": _iso, "fim": _iso, "tipo": _enum, "estado": _enum, "gatilho": _d_gatilho,
    "duracao_ms": _i, "login": _enum, "modulos": _d_modulos_ren, "erro": _enum, "janela_fechou_durante": _b,
    "modulos_solicitados": _mods_opt, "resultado": _enum,
}
_DIAGNOSTICO = {
    "formato": _formato, "versao_formato": _i, "exportado_em": _iso,
    "periodo": _sub({"desde": _iso, "ate": _iso}),
    "agenda": _sub({
        "ativa": _b, "dias": _dias, "inicio": _hhmm, "fim": _hhmm, "fuso": _fuso, "intervalo_s": _i,
        "intervalo_modo": _enum, "intervalo_min_min": _i, "intervalo_max_min": _i, "versao": _i,
        "atualizada_em": _iso, "aberta_agora": _b, "proxima_mudanca": _iso, "ilegivel": _b,
    }),
    "intervalos": _sub({"modo": _enum, "fixo_s": _i, "min_s": _i, "max_s": _i, "sorteado_s": _i,
                        "proxima_execucao": _iso}),
    "worker": _sub({"visto_em": _iso, "iniciado_em": _iso, "modo": _enum}),
    "pausa_manual": _sub({"ativa": _b, "desde": _iso}),
    "disponibilidade": _sub({"estado": _enum, "motivo": _enum}),
    "modulos": _sub({m: _sub(_D_MODULO) for m in _MODULOS_STATUS}),
    "renovacao": _sub({
        "config": _sub({"ativa": _b, "suspensa": _b, "versao": _i}),
        "estado": _enum, "motivo_estado": _texto, "ultima_renovacao_ok": _iso,
        "episodio": _opt({"desde": _iso, "modulos": _mods, "motivo": _enum, "tentativas_automaticas": _i,
                          "bloqueado": _b, "bloqueio_motivo": _enum, "modulos_estado": _modulos_estado}),
    }),
    "sondas": _lista(_D_SONDA, 5000),
    "renovacoes": _lista(_D_RENOVACAO_ITEM, 500),
    "limites": _sub({"max_sondas": _i, "max_renovacoes": _i, "truncado": _b}),
}


def _agora_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def _fmt_z(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


@conexao_siaa_bp.route("/api/conexao-siaa/diagnostico", methods=["GET"])
def api_conexao_diagnostico():
    if (f := _forbidden()) is not None:
        return f
    periodo = (request.args.get("periodo") or "24h").strip().lower()
    if periodo not in _PERIODOS:
        return _erro("periodo: use 6h, 24h, 3d ou 7d.", 400)
    ate = _agora_utc()
    desde = ate - _PERIODOS[periodo]
    st, d = _chamar("GET", "diagnostico", params={"desde": _fmt_z(desde), "ate": _fmt_z(ate)},
                    timeout=_TIMEOUT_DIAG)
    if st == 200 and not isinstance(d, dict):
        return _erro("A session-api respondeu de forma inesperada.", 502)
    if st != 200:
        return _responder(st, d, lambda x: {}, generico_400=True)
    corpo = json.dumps(_pick(d, _DIAGNOSTICO), ensure_ascii=False, indent=2)
    nome = "conexao-siaa-diagnostico-" + ate.strftime("%Y%m%dT%H%M%SZ") + ".json"
    resp = Response(corpo, status=200, mimetype="application/json")
    resp.headers["Content-Disposition"] = f'attachment; filename="{nome}"'
    resp.headers["Cache-Control"] = "no-store"
    return resp
