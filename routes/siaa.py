"""SIAA: scraping direto ao SIAA + persistência no Supabase.

GET /api/siaa/aluno/<rgm>?refresh=0|1
    Consulta dados do aluno no sistema SIAA.
    - refresh=1  força nova captura ao vivo via HTTP (pode demorar ~60 s)
    - refresh=0  retorna ultima captura em cache do Supabase (default)

Env vars usadas:
    SUPABASE_URL, SUPABASE_KEY     — obrigatórias para leitura/escrita
    SIAA_SESSION_URL               — usado pelo siaa_http_client para buscar cookies
    SIAA_SESSION_TOKEN             — token de autenticação do servidor de sessão
"""
from __future__ import annotations

import logging
import os
import re

import requests
from flask import Blueprint, jsonify, request, session

from siaa.availability import (
    consultar_availability,
    data_da_captura,
    decidir_consulta,
    mensagem_preservada,
)
from siaa.parser import process_siaa_http_rgm
from siaa.insights import (
    build_insights, fetch_latest_captura, fetch_documentos,
    fetch_titulos, supabase_configured, normalize_rgm,
)

logger = logging.getLogger(__name__)
siaa_bp = Blueprint("siaa_bp", __name__)

SIAA_URL = (os.environ.get("SIAA_SESSION_URL", "") or "").rstrip("/")
SIAA_TOKEN = os.environ.get("SIAA_SESSION_TOKEN", "") or ""


@siaa_bp.route("/api/siaa/aluno/<rgm>", methods=["GET"])
def api_siaa_aluno(rgm):
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "forbidden"}), 403
    rgm_norm = normalize_rgm(rgm)
    if not rgm_norm:
        return jsonify({"ok": False, "error": "rgm invalido"}), 400
    if not supabase_configured():
        return jsonify({"ok": False, "error": "SUPABASE_URL e SUPABASE_KEY nao configurados"}), 500

    do_refresh = request.args.get("refresh", "0") == "1"
    refresh_status = None
    refresh_message = None
    situacao_availability = None
    if do_refresh:
        situacao_availability = consultar_availability(
            (os.environ.get("SIAA_SESSION_URL", "") or "").rstrip("/"),
            os.environ.get("SIAA_SESSION_TOKEN", "") or "",
        )
        if decidir_consulta(situacao_availability) == "preservar":
            refresh_status = "preservado"
        else:
            try:
                process_siaa_http_rgm(rgm_norm, save_supabase=True, debug=False)
                refresh_status = "ok"
                refresh_message = "Consulta online ao SIAA realizada automaticamente."
            except Exception as exc:
                logger.warning("api_siaa_aluno refresh falhou rgm=%s: %s", rgm_norm, exc)
                refresh_status = "fail"
                refresh_message = "Nao foi possivel consultar o SIAA agora. Exibindo a ultima captura disponivel."

    captura, err = fetch_latest_captura(rgm_norm)
    if err:
        return jsonify({"ok": False, "error": err, "rgm": rgm_norm}), 502

    if refresh_status == "preservado":
        refresh_message = mensagem_preservada(
            situacao_availability or {},
            data_da_captura(captura),
        )
    if not captura:
        return jsonify({
            "ok": True,
            "found": False,
            "rgm": rgm_norm,
            "refresh_status": refresh_status,
            "refresh_message": refresh_message,
            "siaa_consultado": refresh_status == "ok",
        })

    captura_id = captura.get("id")
    documentos, doc_err = fetch_documentos(captura_id)
    titulos, tit_err = fetch_titulos(captura_id)
    if doc_err or tit_err:
        return jsonify({
            "ok": False,
            "error": " | ".join([e for e in (doc_err, tit_err) if e]),
            "rgm": rgm_norm,
        }), 502

    return jsonify({
        "ok": True,
        "found": True,
        "rgm": rgm_norm,
        "refresh_status": refresh_status,
        "refresh_message": refresh_message,
        "capturado_em": data_da_captura(captura) if refresh_status == "preservado" else None,
        "siaa_consultado": refresh_status == "ok",
        "insights": build_insights(captura, documentos, titulos),
    })


# ---------------------------------------------------------------------------
# Sessao SIAA — salvar / testar cookies (admin-only)
# ---------------------------------------------------------------------------

def _siaa_sessao_forbidden():
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return None


_MODULOS_SESSAO = ("academico", "financeiro", "inscricoes")
# Módulos atualizados por cURL completo: a session-api valida destino (HTTPS, host,
# página) e extrai só o cookie. O painel só repassa, sem registrar o conteúdo.
_MODULOS_CURL = ("inscricoes",)


def _validar_sessao_payload(data):
    module = (data.get("module") or "").strip().lower()
    cookie = (data.get("cookie") or "").strip()
    if module not in _MODULOS_SESSAO:
        return None, None, (jsonify({"ok": False, "error": "module invalido"}), 400)
    if not cookie:
        return None, None, (jsonify({"ok": False, "error": "cookie ausente"}), 400)
    return module, cookie, None


def _curl_sem_https(module):
    """O cURL leva cookies e a busca original: só trafega para a session-api em HTTPS."""
    if module in _MODULOS_CURL and not SIAA_URL.lower().startswith("https://"):
        return jsonify({"ok": False, "error": "servidor de sessão sem HTTPS; envio do cURL recusado"}), 500
    return None


def _timeout_sessao(module):
    # Testar: GET (até 60 s) + ping (até 40 s) na session-api; Salvar: gravação + GET.
    return (5, 120) if module in _MODULOS_CURL else 60


def _corpo_sessao(module, cookie):
    return {"curl": cookie} if module in _MODULOS_CURL else {"cookie": cookie}


def _txt(v, n=200):
    return v[:n] if isinstance(v, str) else None


def _rotulo(v):
    return v if isinstance(v, str) and re.fullmatch(r"[a-z0-9_]{1,40}", v) else None


def _int(v):
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _bool(v):
    return v if isinstance(v, bool) else None


_ERRO_SUSPEITO = re.compile(r"=|\d{6,}|jsessionid|ts01|viewstate|curl\s+['\"]?https?:", re.IGNORECASE)


def _erro_seguro(v):
    """Mensagem de erro só se não parecer conter cookie, CPF, ViewState ou o cURL."""
    texto = _txt(v)
    if texto is None:
        return None
    return "Não foi possível concluir a operação." if _ERRO_SUSPEITO.search(texto) else texto


def _sem_none(d):
    return {k: v for k, v in d.items() if v is not None}


def _resposta_curl(data, status_code):
    """Lista branca da resposta da session-api para módulos por cURL: nada de cookie ou ViewState."""
    d = data if isinstance(data, dict) else {}
    sonda = d.get("sonda") if isinstance(d.get("sonda"), dict) else None
    pub = d.get("publicacao") if isinstance(d.get("publicacao"), dict) else None
    ids = d.get("formularios_encontrados")
    out = {
        "ok": _bool(d.get("ok")) if "ok" in d else status_code < 400,
        "module": _rotulo(d.get("module")),
        "error": _erro_seguro(d.get("error")),
        "codigo": _rotulo(d.get("codigo")),
        "alive": _bool(d.get("alive")),
        "status": _int(d.get("status")),
        "classe": _rotulo(d.get("classe")),
        "etapa": _rotulo(d.get("etapa")),
        "motivo": _rotulo(d.get("motivo")),
        "formulario": d.get("formulario") if isinstance(d.get("formulario"), str)
        and re.fullmatch(r"[A-Za-z][A-Za-z0-9_:\-]{0,63}", d.get("formulario")) else None,
        "formularios_encontrados": [x for x in ids if isinstance(x, str)
                                    and re.fullmatch(r"[A-Za-z][A-Za-z0-9_:\-]{0,63}", x)][:10]
        if isinstance(ids, list) else None,
        "estado": _rotulo(d.get("estado")),
        "length": _int(d.get("length")),
        "sonda": None if sonda is None else _sem_none({
            "alive": _bool(sonda.get("alive")), "status": _int(sonda.get("status")),
            "classe": _rotulo(sonda.get("classe")), "adiado": _bool(sonda.get("adiado")),
            "etapa": _rotulo(sonda.get("etapa")), "motivo": _rotulo(sonda.get("motivo")),
        }),
        "publicacao": None if pub is None else _sem_none({
            "gravada": _bool(pub.get("gravada")), "sonda_validou": _bool(pub.get("sonda_validou")),
            "vinculo": _rotulo(pub.get("vinculo")),
        }),
    }
    return {k: v for k, v in out.items() if v is not None}


@siaa_bp.route("/siaa-sessao/salvar", methods=["POST"])
def siaa_sessao_salvar():
    forbidden = _siaa_sessao_forbidden()
    if forbidden is not None:
        return forbidden

    if not SIAA_URL or not SIAA_TOKEN:
        return jsonify({"ok": False, "error": "siaa sessao nao configurado"}), 500

    data = request.get_json(silent=True) or {}
    module, cookie, err = _validar_sessao_payload(data)
    if err is not None:
        return err
    if (sem_https := _curl_sem_https(module)) is not None:
        return sem_https

    try:
        r = requests.post(
            f"{SIAA_URL}/session/{module}",
            headers={"Authorization": f"Bearer {SIAA_TOKEN}"},
            json=_corpo_sessao(module, cookie),
            timeout=_timeout_sessao(module),
        )
    except requests.Timeout:
        logger.error("siaa_sessao_salvar: timeout module=%s", module)
        if module in _MODULOS_CURL:
            return jsonify({"ok": False, "error": "Sem resposta do servidor de sessão a tempo. A sessão pode ter sido gravada: confira em Conexão SIAA antes de salvar de novo."}), 504
        return jsonify({"ok": False, "error": "timeout"}), 504
    except requests.RequestException as e:
        logger.error("siaa_sessao_salvar: erro de rede module=%s", module)
        if module in _MODULOS_CURL:
            return jsonify({"ok": False, "error": "falha de rede ao falar com o servidor de sessão"}), 502
        return jsonify({"ok": False, "error": str(e)}), 502

    logger.info("siaa_sessao_salvar: module=%s len=%d status=%d", module, len(cookie), r.status_code)

    try:
        resp_data = r.json()
    except ValueError:
        resp_data = {"ok": r.status_code < 400, "raw": r.text[:300]}
    if module in _MODULOS_CURL:
        return jsonify(_resposta_curl(resp_data, r.status_code)), r.status_code

    return jsonify(resp_data), r.status_code


@siaa_bp.route("/siaa-sessao/testar", methods=["POST"])
def siaa_sessao_testar():
    forbidden = _siaa_sessao_forbidden()
    if forbidden is not None:
        return forbidden

    if not SIAA_URL or not SIAA_TOKEN:
        return jsonify({"ok": False, "error": "siaa sessao nao configurado"}), 500

    data = request.get_json(silent=True) or {}
    module, cookie, err = _validar_sessao_payload(data)
    if err is not None:
        return err
    if (sem_https := _curl_sem_https(module)) is not None:
        return sem_https

    try:
        r = requests.post(
            f"{SIAA_URL}/session/{module}/test",
            headers={"Authorization": f"Bearer {SIAA_TOKEN}"},
            json=_corpo_sessao(module, cookie),
            timeout=_timeout_sessao(module),
        )
    except requests.Timeout:
        logger.error("siaa_sessao_testar: timeout module=%s", module)
        if module in _MODULOS_CURL:
            return jsonify({"ok": False, "error": "Sem resposta do servidor de sessão a tempo. Tente testar de novo."}), 504
        return jsonify({"ok": False, "error": "timeout"}), 504
    except requests.RequestException as e:
        logger.error("siaa_sessao_testar: erro de rede module=%s", module)
        if module in _MODULOS_CURL:
            return jsonify({"ok": False, "error": "falha de rede ao falar com o servidor de sessão"}), 502
        return jsonify({"ok": False, "error": str(e)}), 502

    logger.info("siaa_sessao_testar: module=%s len=%d status=%d", module, len(cookie), r.status_code)

    try:
        resp_data = r.json()
    except ValueError:
        resp_data = {"ok": r.status_code < 400, "raw": r.text[:300]}
    if module in _MODULOS_CURL:
        return jsonify(_resposta_curl(resp_data, r.status_code)), r.status_code

    return jsonify(resp_data), r.status_code
