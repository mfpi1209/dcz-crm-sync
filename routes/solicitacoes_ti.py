"""Chamados internos (TI e Marketing) persistidos no Postgres.

Páginas
-------
solicitacoes_ti     formulário de abertura (qualquer autenticado, _NAV_ALWAYS)
meus_chamados_ti    tickets do próprio usuário (read-only, _NAV_ALWAYS)
chamados_ti         fila operacional: altera status (permissão / admin)

Status: Pendente (abertura) → Em andamento → Concluído.

Departamento (`ti_chamado.departamento`): `TI` mantém os campos originais;
`Marketing` usa o formulário de briefing de design, gravado em `briefing`
(JSONB) — a fila é escopada por departamento via permissão.
"""
from __future__ import annotations

import json
import logging
import re
import uuid
from typing import Any

import psycopg2.extras
from flask import Blueprint, g, jsonify, request, session

from db import get_conn
from helpers import criar_aviso_para_usuarios, criar_aviso_por_permissao, display_name_from_login

logger = logging.getLogger(__name__)
solicitacoes_ti_bp = Blueprint("solicitacoes_ti_bp", __name__)

DEPTO_TI = "TI"
DEPTO_MKT = "Marketing"
DEPARTAMENTOS = (DEPTO_TI, DEPTO_MKT)

PAGE_FILA = "chamados_ti"
PAGE_FILA_MKT = "chamados_marketing"
# Permissão que dá acesso à fila de cada departamento.
DEPTO_PAGE = {DEPTO_TI: PAGE_FILA, DEPTO_MKT: PAGE_FILA_MKT}

STATUS_PENDENTE = "Pendente"
STATUS_ANDAMENTO = "Em andamento"
STATUS_CONCLUIDO = "Concluído"
STATUS_VALIDOS = (STATUS_PENDENTE, STATUS_ANDAMENTO, STATUS_CONCLUIDO)
STATUS_ABERTOS = (STATUS_PENDENTE, STATUS_ANDAMENTO)

URGENCIAS = ("Baixa", "Média", "Alta", "Crítica")
SETORES = ("Marketing", "Comercial", "Acadêmico", "TI", "Financeiro")
CATEGORIAS = ("Erros/Bugs", "Processos Novos", "Ideias Novas")
# Marketing: "categoria" é o formato da peça (briefing de design).
CATEGORIAS_MKT = ("Imagem", "Vídeo", "UX / UI", "E-book", "Brinde", "Outro")
CATEGORIAS_POR_DEPTO = {DEPTO_TI: CATEGORIAS, DEPTO_MKT: CATEGORIAS_MKT}
BANDEIRAS = ("Cruzeiro do Sul", "DNA WORK", "Sumaré")

SETOR_MAX = 80
CATEGORIA_MAX = 80
TITULO_MAX = 200
DESCRICAO_MAX = 4000
OBS_MAX = 300
NOTA_MAX = 500
SOLICITANTE_MAX = 120
BRIEF_TEXT_MAX = 2000
BRIEF_LIST_MAX = 12

# Campos do briefing de design aceitos no JSONB. Chaves fora desta lista são
# descartadas — o payload vem do front, não vale gravar cru.
BRIEF_COMUNS = (
    "bandeira", "publico_alvo", "assunto_tema", "objetivo_peca",
    "tem_telefone", "telefone_contato", "outras_informacoes",
)
BRIEF_SPECS = {
    "Imagem": ("formatos", "dimensao_especifica", "observacoes"),
    "Vídeo": (
        "tem_material_bruto", "materiais_disponiveis", "material_outro",
        "link_material_bruto", "tem_roteiro", "link_roteiro", "ideia_roteiro",
        "formato_video", "formato_video_outro", "duracao_estimada",
    ),
    "UX / UI": (
        "o_que_sera_desenvolvido", "quantidade_telas", "quais_telas",
        "referencias_links", "observacoes",
    ),
    "E-book": (
        "tema_titulo", "tem_texto", "link_conteudo_textual", "conceito_texto",
        "quantidade_paginas", "formato_entrega", "formato_entrega_outro",
        "observacoes",
    ),
    "Brinde": ("qual_brinde", "quantidade", "link_referencia", "observacoes"),
    "Outro": (
        "tipo_personalizado", "especificacoes_tecnicas", "formato_entrega",
        "observacoes",
    ),
}
# Rótulos usados na renderização (front) — servidos junto do config para o
# JS não duplicar as opções do briefing.
BRIEF_OPCOES = {
    "Imagem": {
        "formatos": [
            ["feed", "Feed (1:1 / 4:5)"],
            ["stories", "Stories (9:16)"],
            ["banner", "Banner Digital"],
            ["email", "E-mail Marketing"],
        ],
    },
    "Vídeo": {
        "materiais_disponiveis": [
            ["videos", "Vídeos / Clipes gravados"],
            ["fotos", "Fotos em alta resolução"],
            ["audios", "Áudios / Locuções / Efeitos"],
            ["logo", "Logo vetorial / PNG com transparência"],
            ["outros", "Outros arquivos"],
        ],
        "formato_video": [
            ["vertical", "Vertical (9:16)"],
            ["horizontal", "Horizontal (16:9)"],
            ["quadrado", "Quadrado (1:1)"],
            ["outro", "Outro"],
        ],
    },
    "E-book": {
        "formato_entrega": [
            ["pdf", "PDF Interativo"],
            ["digital", "Digital Web / Flipbook"],
            ["impressao", "Fechamento para Gráfica"],
            ["outro", "Outro Formato"],
        ],
    },
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _norm_depto(value: Any) -> str:
    v = str(value or "").strip().lower()
    for d in DEPARTAMENTOS:
        if v == d.lower():
            return d
    return DEPTO_TI


def _sanitize_briefing(raw: Any, categoria: str) -> dict[str, Any] | None:
    """Mantém só as chaves conhecidas, com tipo e tamanho controlados."""
    if not isinstance(raw, dict):
        return None
    permitidas = set(BRIEF_COMUNS) | set(BRIEF_SPECS.get(categoria, ()))
    out: dict[str, Any] = {}
    for key in permitidas:
        if key not in raw:
            continue
        val = raw[key]
        if isinstance(val, bool):
            out[key] = val
        elif isinstance(val, (int, float)):
            out[key] = str(val)
        elif isinstance(val, list):
            itens = [str(x).strip()[:120] for x in val[:BRIEF_LIST_MAX] if str(x).strip()]
            if itens:
                out[key] = itens
        elif isinstance(val, str):
            txt = val.strip()[:BRIEF_TEXT_MAX]
            if txt:
                out[key] = txt
    return out or None


def _require_auth():
    if not session.get("authenticated"):
        return jsonify({"ok": False, "status": "error", "message": "Não autenticado"}), 401
    return None


def _current_user() -> tuple[int | None, str, str]:
    raw = session.get("user_id")
    try:
        uid = int(raw) if raw is not None else None
    except (TypeError, ValueError):
        uid = None
    if uid is not None and uid <= 0:
        uid = None
    username = (session.get("username") or "").strip()
    role = (session.get("role") or "").strip()
    return uid, username, role


def _display_name(username: str) -> str:
    return display_name_from_login(username) or username or "Usuário"


def _fila_departamentos() -> list[str]:
    """Departamentos cuja fila o usuário pode ver (memoizado por request).

    Admin vê os dois; os demais dependem da permissão de cada fila
    (`chamados_ti` / `chamados_marketing`, allowlists reconciliadas no boot).
    """
    cached = getattr(g, "_chamados_deptos", None)
    if cached is not None:
        return cached
    uid, _username, role = _current_user()
    if role == "admin" or session.get("user_id") == 0:
        deptos = list(DEPARTAMENTOS)
    elif not uid:
        deptos = []
    else:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT page FROM user_permissions WHERE user_id = %s AND page = ANY(%s)",
                    (uid, [PAGE_FILA, PAGE_FILA_MKT]),
                )
                pages = {r[0] for r in cur.fetchall()}
        finally:
            conn.close()
        deptos = [d for d in DEPARTAMENTOS if DEPTO_PAGE[d] in pages]
    g._chamados_deptos = deptos
    return deptos


def _has_fila_perm(departamento: str | None = None) -> bool:
    """Acesso à fila: qualquer departamento, ou um específico."""
    deptos = _fila_departamentos()
    if departamento is None:
        return bool(deptos)
    return departamento in deptos


def _iso(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _row_public(row: dict[str, Any], *, include_body: bool = True) -> dict[str, Any]:
    out = {
        "id": row["id"],
        "protocolo": row["protocolo"],
        "departamento": row.get("departamento") or DEPTO_TI,
        "prazo_desejado": _iso(row.get("prazo_desejado")),
        "solicitante": row["solicitante"],
        "solicitante_user_id": row.get("solicitante_user_id"),
        "solicitante_username": row.get("solicitante_username") or "",
        "setor": row["setor"],
        "categoria": row["categoria"],
        "urgencia": row["urgencia"],
        "titulo": row["titulo"],
        "status": row["status"],
        "status_nota": row.get("status_nota") or "",
        "created_at": _iso(row.get("created_at")),
        "updated_at": _iso(row.get("updated_at")),
        "status_updated_at": _iso(row.get("status_updated_at")),
        "status_updated_by_nome": row.get("status_updated_by_nome") or "",
    }
    if include_body:
        out["descricao"] = row.get("descricao") or ""
        out["observacoes"] = row.get("observacoes") or ""
        brief = row.get("briefing")
        if isinstance(brief, str):
            try:
                brief = json.loads(brief)
            except ValueError:
                brief = None
        out["briefing"] = brief if isinstance(brief, dict) else None
    return out


def _owns(row: dict[str, Any], uid: int | None, username: str) -> bool:
    if uid and row.get("solicitante_user_id") == uid:
        return True
    snap = (row.get("solicitante_username") or "").strip().lower()
    if username and snap and snap == username.strip().lower():
        return True
    return False


# ---------------------------------------------------------------------------
# Config / submit
# ---------------------------------------------------------------------------

@solicitacoes_ti_bp.route("/api/solicitacoes_ti/config", methods=["GET"])
def get_config():
    deny = _require_auth()
    if deny:
        return deny
    uid, username, _role = _current_user()
    return jsonify({
        "ok": True,
        "storage": "postgres",
        "default_solicitante": _display_name(username),
        "can_manage": _has_fila_perm(),
        "user_id": uid,
        "setores": list(SETORES),
        "categorias": list(CATEGORIAS),
        "urgencias": list(URGENCIAS),
        "status_valores": list(STATUS_VALIDOS),
        "departamentos": list(DEPARTAMENTOS),
        "categorias_por_departamento": {k: list(v) for k, v in CATEGORIAS_POR_DEPTO.items()},
        "bandeiras": list(BANDEIRAS),
        "briefing_opcoes": BRIEF_OPCOES,
        "fila_departamentos": _fila_departamentos(),
    })


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/submit", methods=["POST"])
def submit_ticket():
    deny = _require_auth()
    if deny:
        return deny

    body = request.get_json(silent=True) or {}
    departamento = _norm_depto(body.get("departamento"))

    required = ["solicitante", "setor", "categoria", "titulo", "descricao"]
    missing = [f for f in required if not str(body.get(f) or "").strip()]
    if missing:
        return jsonify({
            "ok": False,
            "status": "error",
            "message": f"Preencha os campos obrigatórios: {', '.join(missing)}",
        }), 400

    urgencia = str(body.get("urgencia") or "Média").strip()
    if urgencia not in URGENCIAS:
        urgencia = "Média"

    setor = str(body.get("setor") or "").strip()[:SETOR_MAX]
    categoria = str(body.get("categoria") or "").strip()[:CATEGORIA_MAX]
    if setor not in SETORES:
        return jsonify({"ok": False, "status": "error", "message": "Setor inválido."}), 400
    if categoria not in CATEGORIAS_POR_DEPTO[departamento]:
        return jsonify({
            "ok": False,
            "status": "error",
            "message": f"Categoria inválida para {departamento}.",
        }), 400

    briefing = None
    if departamento == DEPTO_MKT:
        briefing = _sanitize_briefing(body.get("briefing"), categoria) or {}
        falta_brief = [
            label for key, label in (("bandeira", "bandeira / empresa"), ("publico_alvo", "público-alvo"))
            if not str(briefing.get(key) or "").strip()
        ]
        if falta_brief:
            return jsonify({
                "ok": False,
                "status": "error",
                "message": f"Preencha os campos obrigatórios: {', '.join(falta_brief)}",
            }), 400

    # Prazo é opcional, mas se vier tem que ser ISO — melhor recusar do que
    # gravar NULL e o solicitante achar que a data foi registrada.
    prazo = str(body.get("prazo_desejado") or "").strip()
    if prazo and not _DATE_RE.match(prazo):
        return jsonify({
            "ok": False,
            "status": "error",
            "message": "Prazo desejado inválido.",
        }), 400

    uid, username, _role = _current_user()
    solicitante = str(body.get("solicitante") or "").strip()[:SOLICITANTE_MAX]
    titulo = str(body.get("titulo") or "").strip()[:TITULO_MAX]
    descricao = str(body.get("descricao") or "").strip()[:DESCRICAO_MAX]
    observacoes = str(body.get("observacoes") or "").strip()[:OBS_MAX]

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO ti_chamado (
                    protocolo, departamento, solicitante, solicitante_user_id,
                    solicitante_username, setor, categoria, urgencia, titulo,
                    descricao, observacoes, status, briefing, prazo_desejado
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id
                """,
                (
                    f"TMP-{uuid.uuid4().hex[:12]}",
                    departamento,
                    solicitante, uid, username or None,
                    setor, categoria, urgencia, titulo, descricao, observacoes or None,
                    STATUS_PENDENTE,
                    json.dumps(briefing, ensure_ascii=False) if briefing else None,
                    prazo or None,
                ),
            )
            new_id = cur.fetchone()["id"]
            protocolo = f"CH-{new_id:05d}"
            cur.execute(
                "UPDATE ti_chamado SET protocolo = %s WHERE id = %s",
                (protocolo, new_id),
            )
            cur.execute(
                """
                INSERT INTO ti_chamado_evento (
                    chamado_id, status_anterior, status_novo, autor_user_id, autor_nome, nota
                ) VALUES (%s, NULL, %s, %s, %s, %s)
                """,
                (new_id, STATUS_PENDENTE, uid, _display_name(username) or solicitante, "Abertura do chamado"),
            )
            cur.execute("SELECT * FROM ti_chamado WHERE id = %s", (new_id,))
            row = dict(cur.fetchone())
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("solicitacoes_ti: falha ao gravar chamado")
        return jsonify({
            "ok": False,
            "status": "error",
            "message": "Não foi possível gravar o chamado. Tente novamente.",
        }), 500
    finally:
        conn.close()

    ticket = _row_public(row)
    try:
        prio = "urgente" if urgencia == "Crítica" else ("importante" if urgencia == "Alta" else "normal")
        criar_aviso_por_permissao(
            DEPTO_PAGE[departamento],
            titulo=f"Novo chamado {departamento} {protocolo}",
            corpo=(
                f"{solicitante} ({setor}) abriu o chamado {protocolo} para {departamento}: "
                f"{titulo}. Prioridade: {urgencia}."
            ),
            prioridade=prio,
            excluir_user_ids=[uid] if uid else None,
            created_by=uid,
        )
    except Exception:
        logger.exception("solicitacoes_ti: aviso de abertura falhou")

    return jsonify({
        "ok": True,
        "status": "success",
        "message": "Chamado registrado.",
        "ticket": ticket,
    })


# ---------------------------------------------------------------------------
# Meus chamados
# ---------------------------------------------------------------------------

def _list_where_meus(uid: int | None, username: str) -> tuple[str, list[Any]]:
    clauses = []
    params: list[Any] = []
    if uid:
        clauses.append("solicitante_user_id = %s")
        params.append(uid)
    if username:
        clauses.append("LOWER(TRIM(solicitante_username)) = %s")
        params.append(username.strip().lower())
    if not clauses:
        return "FALSE", []
    return "(" + " OR ".join(clauses) + ")", params


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/meus", methods=["GET"])
def list_meus():
    deny = _require_auth()
    if deny:
        return deny
    uid, username, _role = _current_user()
    own_where, own_params = _list_where_meus(uid, username)
    status = (request.args.get("status") or "").strip()
    busca = (request.args.get("q") or "").strip()
    extra = []
    extra_params: list[Any] = []
    if status == "abertos":
        extra.append("status = ANY(%s)")
        extra_params.append(list(STATUS_ABERTOS))
    elif status in STATUS_VALIDOS:
        extra.append("status = %s")
        extra_params.append(status)
    if busca:
        extra.append("(protocolo ILIKE %s OR titulo ILIKE %s OR descricao ILIKE %s)")
        like = f"%{busca}%"
        extra_params.extend([like, like, like])
    depto_req = (request.args.get("departamento") or "").strip()
    if depto_req in DEPARTAMENTOS:
        extra.append("COALESCE(departamento, 'TI') = %s")
        extra_params.append(depto_req)
    extra_sql = (" AND " + " AND ".join(extra)) if extra else ""
    list_params = list(own_params) + extra_params

    try:
        limit = min(200, max(1, int(request.args.get("limit") or 80)))
    except (TypeError, ValueError):
        limit = 80
    try:
        offset = max(0, int(request.args.get("offset") or 0))
    except (TypeError, ValueError):
        offset = 0

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT COUNT(*) AS n FROM ti_chamado WHERE {own_where}{extra_sql}",
                list_params,
            )
            total = int(cur.fetchone()["n"])
            cur.execute(
                f"""
                SELECT * FROM ti_chamado
                 WHERE {own_where}{extra_sql}
                 ORDER BY created_at DESC
                 LIMIT %s OFFSET %s
                """,
                list_params + [limit, offset],
            )
            items = [_row_public(dict(r), include_body=False) for r in cur.fetchall()]
            cur.execute(
                f"""
                SELECT status, COUNT(*) AS n
                  FROM ti_chamado
                 WHERE {own_where}
                 GROUP BY status
                """,
                own_params,
            )
            kpis = {s: 0 for s in STATUS_VALIDOS}
            for r in cur.fetchall():
                kpis[r["status"]] = int(r["n"])
            cur.execute(
                f"""
                SELECT COALESCE(departamento, 'TI') AS d, COUNT(*) AS n
                  FROM ti_chamado
                 WHERE {own_where}
                 GROUP BY 1
                """,
                own_params,
            )
            por_depto = {d: 0 for d in DEPARTAMENTOS}
            for r in cur.fetchall():
                if r["d"] in por_depto:
                    por_depto[r["d"]] = int(r["n"])
    finally:
        conn.close()

    return jsonify({
        "ok": True, "items": items, "total": total,
        "kpis": kpis, "por_departamento": por_depto,
    })


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/meus/<int:chamado_id>", methods=["GET"])
def get_meu(chamado_id: int):
    deny = _require_auth()
    if deny:
        return deny
    uid, username, _role = _current_user()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM ti_chamado WHERE id = %s", (chamado_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"ok": False, "message": "Chamado não encontrado."}), 404
            row = dict(row)
            if not _owns(row, uid, username) and not _has_fila_perm(_norm_depto(row.get("departamento"))):
                return jsonify({"ok": False, "message": "Sem permissão."}), 403
            cur.execute(
                """
                SELECT id, status_anterior, status_novo, autor_nome, nota, created_at
                  FROM ti_chamado_evento
                 WHERE chamado_id = %s
                 ORDER BY created_at ASC, id ASC
                """,
                (chamado_id,),
            )
            eventos = []
            for ev in cur.fetchall():
                eventos.append({
                    "id": ev["id"],
                    "status_anterior": ev["status_anterior"],
                    "status_novo": ev["status_novo"],
                    "autor_nome": ev["autor_nome"],
                    "nota": ev["nota"] or "",
                    "created_at": _iso(ev["created_at"]),
                })
    finally:
        conn.close()
    return jsonify({"ok": True, "ticket": _row_public(row), "eventos": eventos})


# ---------------------------------------------------------------------------
# Fila operacional
# ---------------------------------------------------------------------------

@solicitacoes_ti_bp.route("/api/solicitacoes_ti/chamados", methods=["GET"])
def list_chamados():
    deny = _require_auth()
    if deny:
        return deny
    deptos_ok = _fila_departamentos()
    if not deptos_ok:
        return jsonify({"ok": False, "message": "Sem permissão para a fila de chamados."}), 403

    # Escopo por departamento: o usuário só enxerga as filas que pode ver.
    depto_req = (request.args.get("departamento") or "").strip()
    if depto_req in DEPARTAMENTOS:
        if depto_req not in deptos_ok:
            return jsonify({
                "ok": False,
                "message": f"Sem permissão para a fila de {depto_req}.",
            }), 403
        deptos_scope = [depto_req]
    else:
        deptos_scope = list(deptos_ok)

    status = (request.args.get("status") or "abertos").strip()
    urgencia = (request.args.get("urgencia") or "").strip()
    setor = (request.args.get("setor") or "").strip()
    categoria = (request.args.get("categoria") or "").strip()
    busca = (request.args.get("q") or "").strip()
    clauses: list[str] = ["COALESCE(departamento, 'TI') = ANY(%s)"]
    params: list[Any] = [deptos_scope]
    if status in ("todos", "all", "*"):
        pass
    elif status == "abertos":
        clauses.append("status = ANY(%s)")
        params.append(list(STATUS_ABERTOS))
    elif status in STATUS_VALIDOS:
        clauses.append("status = %s")
        params.append(status)
    if urgencia in URGENCIAS:
        clauses.append("urgencia = %s")
        params.append(urgencia)
    if setor:
        clauses.append("setor = %s")
        params.append(setor)
    if categoria:
        clauses.append("categoria = %s")
        params.append(categoria)
    if busca:
        clauses.append(
            "(protocolo ILIKE %s OR titulo ILIKE %s OR solicitante ILIKE %s OR descricao ILIKE %s)"
        )
        like = f"%{busca}%"
        params.extend([like, like, like, like])
    where = "WHERE " + " AND ".join(clauses)

    try:
        limit = min(300, max(1, int(request.args.get("limit") or 100)))
    except (TypeError, ValueError):
        limit = 100
    try:
        offset = max(0, int(request.args.get("offset") or 0))
    except (TypeError, ValueError):
        offset = 0

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM ti_chamado {where}", params)
            total = int(cur.fetchone()["n"])
            cur.execute(
                f"""
                SELECT * FROM ti_chamado
                {where}
                ORDER BY
                    CASE urgencia
                        WHEN 'Crítica' THEN 0
                        WHEN 'Alta' THEN 1
                        WHEN 'Média' THEN 2
                        ELSE 3
                    END,
                    created_at ASC
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            items = [_row_public(dict(r), include_body=False) for r in cur.fetchall()]
            # KPIs seguem o mesmo escopo de departamento da listagem.
            cur.execute(
                """
                SELECT status, COUNT(*) AS n
                  FROM ti_chamado
                 WHERE COALESCE(departamento, 'TI') = ANY(%s)
                 GROUP BY status
                """,
                (deptos_scope,),
            )
            kpis = {s: 0 for s in STATUS_VALIDOS}
            for r in cur.fetchall():
                kpis[r["status"]] = int(r["n"])
            cur.execute(
                """
                SELECT COALESCE(departamento, 'TI') AS d, COUNT(*) AS n
                  FROM ti_chamado
                 WHERE COALESCE(departamento, 'TI') = ANY(%s)
                   AND status = ANY(%s)
                 GROUP BY 1
                """,
                (list(deptos_ok), list(STATUS_ABERTOS)),
            )
            abertos_por_depto = {d: 0 for d in deptos_ok}
            for r in cur.fetchall():
                if r["d"] in abertos_por_depto:
                    abertos_por_depto[r["d"]] = int(r["n"])
    finally:
        conn.close()
    return jsonify({
        "ok": True, "items": items, "total": total, "kpis": kpis,
        "fila_departamentos": list(deptos_ok),
        "abertos_por_departamento": abertos_por_depto,
    })


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/chamados/<int:chamado_id>", methods=["GET"])
def get_chamado(chamado_id: int):
    deny = _require_auth()
    if deny:
        return deny
    uid, username, _role = _current_user()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM ti_chamado WHERE id = %s", (chamado_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"ok": False, "message": "Chamado não encontrado."}), 404
            row = dict(row)
            row_depto = _norm_depto(row.get("departamento"))
            if not _has_fila_perm(row_depto) and not _owns(row, uid, username):
                return jsonify({"ok": False, "message": "Sem permissão."}), 403
            cur.execute(
                """
                SELECT id, status_anterior, status_novo, autor_nome, nota, created_at
                  FROM ti_chamado_evento
                 WHERE chamado_id = %s
                 ORDER BY created_at ASC, id ASC
                """,
                (chamado_id,),
            )
            eventos = [{
                "id": ev["id"],
                "status_anterior": ev["status_anterior"],
                "status_novo": ev["status_novo"],
                "autor_nome": ev["autor_nome"],
                "nota": ev["nota"] or "",
                "created_at": _iso(ev["created_at"]),
            } for ev in cur.fetchall()]
    finally:
        conn.close()
    return jsonify({
        "ok": True,
        "ticket": _row_public(row),
        "eventos": eventos,
        "can_manage": _has_fila_perm(row_depto),
    })


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/chamados/<int:chamado_id>/status", methods=["PATCH"])
def patch_status(chamado_id: int):
    deny = _require_auth()
    if deny:
        return deny
    if not _has_fila_perm():
        return jsonify({"ok": False, "message": "Sem permissão para alterar o status."}), 403
    # A checagem por departamento acontece depois de carregar a linha.

    body = request.get_json(silent=True) or {}
    novo = (body.get("status") or "").strip()
    if novo not in STATUS_VALIDOS:
        return jsonify({
            "ok": False,
            "message": f"Status inválido. Use: {', '.join(STATUS_VALIDOS)}.",
        }), 400
    nota = str(body.get("nota") or body.get("status_nota") or "").strip()[:NOTA_MAX]

    uid, username, _role = _current_user()
    autor = _display_name(username) or username or "Equipe"

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM ti_chamado WHERE id = %s FOR UPDATE", (chamado_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"ok": False, "message": "Chamado não encontrado."}), 404
            row = dict(row)
            if not _has_fila_perm(_norm_depto(row.get("departamento"))):
                conn.rollback()
                return jsonify({
                    "ok": False,
                    "message": "Sem permissão para alterar chamados deste departamento.",
                }), 403
            anterior = row["status"]
            if anterior == novo and not nota:
                return jsonify({
                    "ok": True,
                    "unchanged": True,
                    "ticket": _row_public(row),
                    "message": "Status já estava nesse valor.",
                })
            cur.execute(
                """
                UPDATE ti_chamado
                   SET status = %s,
                       status_nota = %s,
                       updated_at = NOW(),
                       status_updated_at = NOW(),
                       status_updated_by = %s,
                       status_updated_by_nome = %s
                 WHERE id = %s
                """,
                (novo, nota or None, uid, autor, chamado_id),
            )
            cur.execute(
                """
                INSERT INTO ti_chamado_evento (
                    chamado_id, status_anterior, status_novo, autor_user_id, autor_nome, nota
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (chamado_id, anterior, novo, uid, autor, nota or None),
            )
            cur.execute("SELECT * FROM ti_chamado WHERE id = %s", (chamado_id,))
            updated = dict(cur.fetchone())
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("solicitacoes_ti: falha ao atualizar status")
        return jsonify({"ok": False, "message": "Falha ao atualizar o status."}), 500
    finally:
        conn.close()

    opener_id = updated.get("solicitante_user_id")
    if opener_id and opener_id != uid:
        try:
            corpo = (
                f"Seu chamado {updated['protocolo']} ({updated['titulo']}) "
                f"passou de {anterior} para {novo}."
            )
            if nota:
                corpo += f" Observação do {_norm_depto(updated.get('departamento'))}: {nota}"
            criar_aviso_para_usuarios(
                [opener_id],
                titulo=f"Chamado {updated['protocolo']}: {novo}",
                corpo=corpo,
                prioridade="normal",
                created_by=uid,
            )
        except Exception:
            logger.exception("solicitacoes_ti: aviso de status falhou")

    return jsonify({
        "ok": True,
        "ticket": _row_public(updated),
        "message": f"Status atualizado para {novo}.",
    })
