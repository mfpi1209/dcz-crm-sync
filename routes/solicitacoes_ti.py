"""Solicitações TI — chamados persistidos no Postgres.

Páginas
-------
solicitacoes_ti     formulário de abertura (qualquer autenticado, _NAV_ALWAYS)
meus_chamados_ti    tickets do próprio usuário (read-only, _NAV_ALWAYS)
chamados_ti         fila operacional: TI altera status (permissão / admin)

Status: Pendente (abertura) → Em andamento → Concluído.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

import psycopg2.extras
from flask import Blueprint, jsonify, request, session

from db import get_conn
from helpers import criar_aviso_para_usuarios, criar_aviso_por_permissao, display_name_from_login

logger = logging.getLogger(__name__)
solicitacoes_ti_bp = Blueprint("solicitacoes_ti_bp", __name__)

PAGE_FILA = "chamados_ti"

STATUS_PENDENTE = "Pendente"
STATUS_ANDAMENTO = "Em andamento"
STATUS_CONCLUIDO = "Concluído"
STATUS_VALIDOS = (STATUS_PENDENTE, STATUS_ANDAMENTO, STATUS_CONCLUIDO)
STATUS_ABERTOS = (STATUS_PENDENTE, STATUS_ANDAMENTO)

URGENCIAS = ("Baixa", "Média", "Alta", "Crítica")
SETORES = ("Marketing", "Comercial", "Acadêmico", "TI", "Financeiro")
CATEGORIAS = ("Erros/Bugs", "Processos Novos", "Ideias Novas")

SETOR_MAX = 80
CATEGORIA_MAX = 80
TITULO_MAX = 200
DESCRICAO_MAX = 4000
OBS_MAX = 300
NOTA_MAX = 500
SOLICITANTE_MAX = 120


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


def _has_fila_perm() -> bool:
    uid, _username, role = _current_user()
    if role == "admin" or session.get("user_id") == 0:
        return True
    if not uid:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM user_permissions WHERE user_id = %s AND page = %s",
                (uid, PAGE_FILA),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


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
    })


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/submit", methods=["POST"])
def submit_ticket():
    deny = _require_auth()
    if deny:
        return deny

    body = request.get_json(silent=True) or {}
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
    if categoria not in CATEGORIAS:
        return jsonify({"ok": False, "status": "error", "message": "Categoria inválida."}), 400

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
                    protocolo, solicitante, solicitante_user_id, solicitante_username,
                    setor, categoria, urgencia, titulo, descricao, observacoes, status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id
                """,
                (
                    f"TMP-{uuid.uuid4().hex[:12]}",
                    solicitante, uid, username or None,
                    setor, categoria, urgencia, titulo, descricao, observacoes or None,
                    STATUS_PENDENTE,
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
            PAGE_FILA,
            titulo=f"Novo chamado {protocolo}",
            corpo=(
                f"{solicitante} ({setor}) abriu o chamado {protocolo}: {titulo}. "
                f"Prioridade: {urgencia}."
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
    finally:
        conn.close()

    return jsonify({"ok": True, "items": items, "total": total, "kpis": kpis})


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
            if not _owns(row, uid, username) and not _has_fila_perm():
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
    if not _has_fila_perm():
        return jsonify({"ok": False, "message": "Sem permissão para a fila de chamados."}), 403

    status = (request.args.get("status") or "abertos").strip()
    urgencia = (request.args.get("urgencia") or "").strip()
    setor = (request.args.get("setor") or "").strip()
    busca = (request.args.get("q") or "").strip()
    clauses: list[str] = []
    params: list[Any] = []
    if status == "abertos":
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
    if busca:
        clauses.append(
            "(protocolo ILIKE %s OR titulo ILIKE %s OR solicitante ILIKE %s OR descricao ILIKE %s)"
        )
        like = f"%{busca}%"
        params.extend([like, like, like, like])
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

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
            cur.execute("SELECT status, COUNT(*) AS n FROM ti_chamado GROUP BY status")
            kpis = {s: 0 for s in STATUS_VALIDOS}
            for r in cur.fetchall():
                kpis[r["status"]] = int(r["n"])
    finally:
        conn.close()
    return jsonify({"ok": True, "items": items, "total": total, "kpis": kpis})


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
            if not _has_fila_perm() and not _owns(row, uid, username):
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
        "can_manage": _has_fila_perm(),
    })


@solicitacoes_ti_bp.route("/api/solicitacoes_ti/chamados/<int:chamado_id>/status", methods=["PATCH"])
def patch_status(chamado_id: int):
    deny = _require_auth()
    if deny:
        return deny
    if not _has_fila_perm():
        return jsonify({"ok": False, "message": "Sem permissão para alterar o status."}), 403

    body = request.get_json(silent=True) or {}
    novo = (body.get("status") or "").strip()
    if novo not in STATUS_VALIDOS:
        return jsonify({
            "ok": False,
            "message": f"Status inválido. Use: {', '.join(STATUS_VALIDOS)}.",
        }), 400
    nota = str(body.get("nota") or body.get("status_nota") or "").strip()[:NOTA_MAX]

    uid, username, _role = _current_user()
    autor = _display_name(username) or username or "TI"

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM ti_chamado WHERE id = %s FOR UPDATE", (chamado_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"ok": False, "message": "Chamado não encontrado."}), 404
            row = dict(row)
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
                corpo += f" Observação do TI: {nota}"
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
