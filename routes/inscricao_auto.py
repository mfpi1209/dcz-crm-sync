"""
eduit. — Inscrição - Automática (proxy autenticado para o serviço de inscrição).

A página "Inscrição - Automática" do painel fala só com /api/inscricao-auto/<rota>.
Este blueprint confere a sessão e a permissão do painel e repassa a chamada para
{INSCRICAO_API_URL}/api/<rota> (ui-server do serviço de inscrição, rede interna do
EasyPanel; padrão http://banco-inscricao:8788).

Permissões (user_permissions):
  - inscricao_automatica        → operar: leituras, fila (reprocessar/cancelar),
                                  nova inscrição em validar/ensaio, testes sem efeito.
  - inscricao_automatica_admin  → administrar: tudo (inclui inscrição REAL, disjuntor,
                                  configuração, cadastros, sessão SIAA, agente).
  Papel `admin` do painel e o login de emergência (user_id 0) administram.

Cada rota liberada está na lista ROTAS (método + regex + papel mínimo). Rota fora
da lista → 404, mesmo para admin: o painel só repassa o que a tela usa.

Cabeçalhos repassados ao serviço:
  - X-Painel-Token: INSCRICAO_API_TOKEN (o navegador nunca vê);
  - X-Usuario: nome de quem está logado (auditoria do serviço);
  - X-Inscricao-Papel: admin | operar (o serviço recusa ação de admin para operar);
  - Content-Type: application/json em todo não-GET (o serviço devolve 415 sem ele).

Timeouts curtos (gunicorn 1 worker / 4 threads): 20 s por padrão, 60 s nas rotas
lentas. A conferência no SIAA e o agente continuam no serviço mesmo se o painel
desistir de esperar; a tela avisa e consulta o resultado depois.
"""

import json
import logging
import os
import re
from pathlib import Path

import requests as _requests
from dotenv import load_dotenv
from flask import Blueprint, Response, jsonify, request, session

from db import get_conn
from helpers import display_name_from_login

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logger = logging.getLogger(__name__)

inscricao_auto_bp = Blueprint("inscricao_auto_bp", __name__)

PERM_OPERAR = "inscricao_automatica"
PERM_ADMIN = "inscricao_automatica_admin"

TIMEOUT_LENTO_S = 60


def _base_url():
    return (os.getenv("INSCRICAO_API_URL") or "http://banco-inscricao:8788").strip().rstrip("/")


def _token():
    return (os.getenv("INSCRICAO_API_TOKEN") or "").strip()


def _timeout_padrao():
    try:
        return max(1, int(os.getenv("INSCRICAO_API_TIMEOUT_S") or "20"))
    except ValueError:
        return 20


_J = r"J\d{6}"
_UUID = r"[0-9a-f-]{36}"

# (método, regex da rota depois de /api/, papel mínimo, lenta?)
ROTAS = [
    # ── leitura (operar) ──────────────────────────────────────────────
    ("GET", r"status", "operar", False),
    ("GET", r"campanha", "operar", False),
    ("GET", r"catalogo/(cursos|polos)", "operar", False),
    ("GET", r"inscricoes", "operar", False),
    ("GET", rf"inscricoes/{_UUID}", "operar", False),
    ("GET", r"atendente", "operar", False),
    ("GET", r"polos", "operar", False),
    ("GET", r"painel", "operar", False),
    ("GET", r"afiliados", "operar", False),
    ("GET", r"manutencao", "operar", False),
    ("GET", rf"manutencao/diagnosticos/{_UUID}", "operar", False),
    ("GET", r"crm", "operar", False),
    ("GET", r"orq/health", "operar", False),
    ("GET", r"orq/automacao", "operar", False),
    ("GET", r"orq/config", "operar", False),
    ("GET", r"orq/config/historico", "operar", False),
    ("GET", r"orq/eventos", "operar", False),
    ("GET", r"orq/fila", "operar", False),
    ("GET", rf"orq/fila/{_J}", "operar", False),
    ("GET", r"orq/crm1/retorno", "operar", False),
    ("GET", r"orq/relatorio/inscricoes(\.csv)?", "operar", False),
    ("GET", r"orq/siaa/sessao", "operar", False),
    ("GET", r"orq/agente", "operar", False),
    ("GET", r"orq/simulacao", "operar", False),
    # ── ações sem efeito na loja / da operação do dia a dia (operar) ──
    # POST inscricoes: modo "real" exige admin (conferido no corpo, ver _papel_exigido).
    ("POST", r"inscricoes", "operar", False),
    ("POST", r"cupom/validar", "operar", False),
    ("POST", r"polos/testar", "operar", False),
    ("POST", r"manutencao/diagnosticos", "operar", False),
    ("POST", rf"orq/fila/{_J}/(reprocessar|cancelar)", "operar", False),
    # ── administração ─────────────────────────────────────────────────
    ("PUT", r"campanha", "admin", False),
    ("PUT", r"atendente", "admin", False),
    ("DELETE", r"atendente", "admin", False),
    ("POST", r"atendente/testar", "admin", True),
    ("PUT", r"polos", "admin", False),
    ("PUT", r"polos/google", "admin", False),
    ("PUT", r"afiliados(/chave)?", "admin", False),
    ("POST", r"crm/token", "admin", False),
    ("PUT", r"crm/modo", "admin", False),
    ("PUT", r"manutencao/ia", "admin", False),
    ("POST", r"manutencao/aplicar", "admin", False),
    ("POST", r"orq/automacao/(ligar|desligar)", "admin", False),
    ("PUT", r"orq/config", "admin", False),
    ("POST", r"orq/engines/[a-z]+/reset", "admin", False),
    ("PUT", r"orq/siaa/sessao", "admin", False),
    ("POST", r"orq/conferencia/siaa", "admin", True),
    ("POST", r"orq/agente/verificar", "admin", True),
    ("POST", rf"orq/crm1/retorno/(reenviar|devolver/{_J})", "admin", False),
    ("PUT", r"orq/simulacao", "admin", False),
    ("POST", r"orq/inscricao/simulada", "admin", False),
]
_ROTAS_RE = [(m, re.compile(rf"^{rx}$"), papel, lenta) for m, rx, papel, lenta in ROTAS]


def achar_rota(metodo, rota):
    """(papel mínimo, lenta) da rota liberada, ou None se não está na lista."""
    for m, rx, papel, lenta in _ROTAS_RE:
        if m == metodo and rx.match(rota):
            return papel, lenta
    return None


def papel_exigido(metodo, rota, corpo):
    """Papel mínimo para esta chamada, olhando o corpo quando a rota depende dele."""
    achado = achar_rota(metodo, rota)
    if achado is None:
        return None
    papel = achado[0]
    if metodo == "POST" and rota == "inscricoes" and isinstance(corpo, dict):
        if str(corpo.get("modo") or "").strip() == "real":
            papel = "admin"
    return papel


def papel_do_usuario():
    """(papel | None, status, payload): 'admin', 'operar' ou sem acesso.

    Admin do painel e o login de emergência (user_id 0) administram. Os demais
    precisam de inscricao_automatica (operar) ou inscricao_automatica_admin.
    """
    if not session.get("authenticated"):
        return None, 401, {"error": "Não autenticado"}
    role = (session.get("role") or "").strip().lower()
    uid = session.get("user_id")
    if role == "admin" or uid == 0:
        return "admin", 200, None
    if uid is None:
        return None, 403, {"error": "Usuário sem permissão para esta página"}
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT page FROM user_permissions WHERE user_id = %s AND page = ANY(%s)",
                    (uid, [PERM_OPERAR, PERM_ADMIN]),
                )
                paginas = {r[0] for r in cur.fetchall()}
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Falha verificando permissão inscricao_automatica uid=%s: %s", uid, e)
        return None, 500, {"error": "Erro interno verificando permissão"}
    if PERM_ADMIN in paginas:
        return "admin", 200, None
    if PERM_OPERAR in paginas:
        return "operar", 200, None
    return None, 403, {"error": "Usuário sem permissão para esta página"}


def _latin1(s, limite=80):
    """Cabeçalho HTTP só aceita Latin-1: acentos passam, emoji e afins saem."""
    s = "".join(c for c in str(s or "") if 0x20 <= ord(c) <= 0x7E or 0xA0 <= ord(c) <= 0xFF)
    return s.strip()[:limite]


def _usuario():
    nome = display_name_from_login(session.get("username") or "", session.get("email") or "")
    return _latin1(nome) or "painel"


@inscricao_auto_bp.route(
    "/api/inscricao-auto/<path:rota>", methods=["GET", "POST", "PUT", "DELETE"]
)
def proxy(rota):
    papel, status, payload = papel_do_usuario()
    if papel is None:
        return jsonify(payload), status

    metodo = request.method
    corpo_bytes = b""
    corpo = None
    if metodo != "GET":
        corpo_bytes = request.get_data(cache=False) or b""
        if corpo_bytes.strip():
            try:
                corpo = json.loads(corpo_bytes.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                return jsonify({"erro": "JSON inválido"}), 400
        else:
            corpo_bytes = b"{}"
            corpo = {}

    exigido = papel_exigido(metodo, rota, corpo)
    if exigido is None:
        return jsonify({"erro": "Rota não liberada no painel."}), 404
    if exigido == "admin" and papel != "admin":
        return jsonify({
            "erro": "Sem permissão: esta ação é de quem administra a Inscrição - Automática."
        }), 403

    lenta = achar_rota(metodo, rota)[1]
    timeout = max(_timeout_padrao(), TIMEOUT_LENTO_S) if lenta else _timeout_padrao()

    headers = {
        "Accept": request.headers.get("Accept") or "application/json",
        "X-Usuario": _usuario(),
        "X-Inscricao-Papel": papel,
    }
    token = _token()
    if token:
        headers["X-Painel-Token"] = token
    if metodo != "GET":
        headers["Content-Type"] = "application/json"

    url = f"{_base_url()}/api/{rota}"
    qs = request.query_string.decode("utf-8", "replace")
    if qs:
        url = f"{url}?{qs}"

    if metodo != "GET":
        logger.info(
            "[inscricao_auto] %s /api/%s user_id=%s usuario=%s papel=%s",
            metodo, rota, session.get("user_id"), headers["X-Usuario"], papel,
        )

    try:
        r = _requests.request(
            metodo, url, headers=headers,
            data=corpo_bytes if metodo != "GET" else None,
            timeout=timeout,
        )
    except _requests.exceptions.Timeout:
        return jsonify({
            "erro": f"O serviço de inscrição não respondeu em {timeout}s.",
            "timeout": True,
        }), 504
    except _requests.exceptions.ConnectionError:
        return jsonify({
            "erro": "Serviço de inscrição indisponível.",
            "foraDoAr": True,
        }), 503
    except Exception as e:
        logger.warning("Erro no proxy inscricao_auto %s /api/%s: %s", metodo, rota, e)
        return jsonify({"erro": "Falha ao falar com o serviço de inscrição."}), 502

    tipo = r.headers.get("Content-Type") or "application/json"
    if r.status_code == 401:
        # 401 do serviço = token do painel errado/ausente (não é o usuário que precisa logar de novo;
        # repassar 401 faria o api() do front mandar para /login).
        return jsonify({
            "erro": "O serviço de inscrição recusou o token do painel (confira INSCRICAO_API_TOKEN)."
        }), 502
    resp = Response(r.content, status=r.status_code, content_type=tipo)
    disp = r.headers.get("Content-Disposition")
    if disp:
        resp.headers["Content-Disposition"] = disp
    resp.headers["Cache-Control"] = "no-store"
    return resp

