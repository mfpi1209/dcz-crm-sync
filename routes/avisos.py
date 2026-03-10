import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, session
from db import DB_DSN

avisos_bp = Blueprint("avisos_bp", __name__)


def _get_conn():
    return psycopg2.connect(**DB_DSN)


def _require_admin():
    if session.get("role") != "admin":
        return jsonify({"error": "Acesso negado"}), 403
    return None


_VISIBLE_WHERE = """
    a.active = TRUE
    AND (a.expires_at IS NULL OR a.expires_at > NOW())
    AND (a.target_role = 'todos' OR a.target_role = %(role)s)
    AND (a.target_user_ids = '{}' OR %(user_id)s = ANY(a.target_user_ids))
"""


@avisos_bp.route("/api/avisos")
def listar_avisos():
    """All visible notices for the logged-in user, with read flag."""
    uid = session.get("user_id", 0)
    role = session.get("role", "viewer")

    conn = _get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT a.*, u.username AS autor,
                   CASE WHEN al.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS lido
            FROM avisos a
            LEFT JOIN app_users u ON u.id = a.created_by
            LEFT JOIN aviso_lido al ON al.aviso_id = a.id AND al.user_id = %(user_id)s
            WHERE {_VISIBLE_WHERE}
            ORDER BY a.created_at DESC
        """, {"user_id": uid, "role": role})
        rows = cur.fetchall()
    conn.close()

    for r in rows:
        r["created_at"] = r["created_at"].isoformat() if r["created_at"] else None
        r["expires_at"] = r["expires_at"].isoformat() if r["expires_at"] else None
        if r.get("read_at"):
            r["read_at"] = r["read_at"].isoformat()

    return jsonify(rows)


@avisos_bp.route("/api/avisos/nao-lidos")
def nao_lidos():
    """Unread notices for the logged-in user (popup + badge)."""
    uid = session.get("user_id", 0)
    role = session.get("role", "viewer")

    conn = _get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT a.id, a.titulo, a.corpo, a.prioridade, a.created_at,
                   u.username AS autor
            FROM avisos a
            LEFT JOIN app_users u ON u.id = a.created_by
            WHERE {_VISIBLE_WHERE}
              AND a.id NOT IN (
                  SELECT aviso_id FROM aviso_lido WHERE user_id = %(user_id)s
              )
            ORDER BY
                CASE a.prioridade WHEN 'urgente' THEN 0 WHEN 'importante' THEN 1 ELSE 2 END,
                a.created_at DESC
        """, {"user_id": uid, "role": role})
        rows = cur.fetchall()
    conn.close()

    for r in rows:
        r["created_at"] = r["created_at"].isoformat() if r["created_at"] else None

    return jsonify({"count": len(rows), "avisos": rows})


@avisos_bp.route("/api/avisos/<int:aviso_id>/lido", methods=["POST"])
def marcar_lido(aviso_id):
    uid = session.get("user_id", 0)
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO aviso_lido (aviso_id, user_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (aviso_id, uid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@avisos_bp.route("/api/avisos/marcar-todos-lidos", methods=["POST"])
def marcar_todos_lidos():
    uid = session.get("user_id", 0)
    role = session.get("role", "viewer")
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO aviso_lido (aviso_id, user_id)
            SELECT a.id, %(user_id)s
            FROM avisos a
            WHERE {_VISIBLE_WHERE}
              AND a.id NOT IN (
                  SELECT aviso_id FROM aviso_lido WHERE user_id = %(user_id)s
              )
        """, {"user_id": uid, "role": role})
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ── Admin CRUD ──────────────────────────────────────────────────────────────

@avisos_bp.route("/api/avisos/admin")
def admin_listar():
    check = _require_admin()
    if check:
        return check

    conn = _get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT a.*, u.username AS autor
            FROM avisos a
            LEFT JOIN app_users u ON u.id = a.created_by
            ORDER BY a.created_at DESC
        """)
        rows = cur.fetchall()
    conn.close()

    for r in rows:
        r["created_at"] = r["created_at"].isoformat() if r["created_at"] else None
        r["expires_at"] = r["expires_at"].isoformat() if r["expires_at"] else None

    return jsonify(rows)


@avisos_bp.route("/api/avisos", methods=["POST"])
def criar_aviso():
    check = _require_admin()
    if check:
        return check

    data = request.get_json(force=True)
    titulo = (data.get("titulo") or "").strip()
    corpo = (data.get("corpo") or "").strip()
    if not titulo or not corpo:
        return jsonify({"error": "Título e corpo são obrigatórios"}), 400

    prioridade = data.get("prioridade", "normal")
    target_role = data.get("target_role", "todos")
    target_user_ids = data.get("target_user_ids") or []
    expires_at = data.get("expires_at") or None
    created_by = session.get("user_id", 0)

    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO avisos (titulo, corpo, prioridade, target_role, target_user_ids,
                                created_by, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (titulo, corpo, prioridade, target_role, target_user_ids, created_by, expires_at))
        new_id = cur.fetchone()[0]
    conn.commit()
    conn.close()

    return jsonify({"ok": True, "id": new_id}), 201


@avisos_bp.route("/api/avisos/<int:aviso_id>", methods=["PUT"])
def editar_aviso(aviso_id):
    check = _require_admin()
    if check:
        return check

    data = request.get_json(force=True)
    titulo = (data.get("titulo") or "").strip()
    corpo = (data.get("corpo") or "").strip()
    if not titulo or not corpo:
        return jsonify({"error": "Título e corpo são obrigatórios"}), 400

    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE avisos SET titulo = %s, corpo = %s, prioridade = %s,
                              target_role = %s, target_user_ids = %s, expires_at = %s
            WHERE id = %s
        """, (
            titulo, corpo,
            data.get("prioridade", "normal"),
            data.get("target_role", "todos"),
            data.get("target_user_ids") or [],
            data.get("expires_at") or None,
            aviso_id,
        ))
    conn.commit()
    conn.close()

    return jsonify({"ok": True})


@avisos_bp.route("/api/avisos/<int:aviso_id>", methods=["DELETE"])
def desativar_aviso(aviso_id):
    check = _require_admin()
    if check:
        return check

    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE avisos SET active = FALSE WHERE id = %s", (aviso_id,))
    conn.commit()
    conn.close()

    return jsonify({"ok": True})


@avisos_bp.route("/api/avisos/usuarios")
def listar_usuarios():
    """List users for the target_user_ids multi-select."""
    check = _require_admin()
    if check:
        return check

    conn = _get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, username, role FROM app_users ORDER BY username")
        rows = cur.fetchall()
    conn.close()

    return jsonify(rows)
