import hashlib
import psycopg2
import psycopg2.extras
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, session
from db import get_conn, DB_DSN
from helpers import ALL_PAGES, APP_USER_FALLBACK, APP_PASS_FALLBACK, to_brt

auth_bp = Blueprint("auth_bp", __name__)


def _hash_pw(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _db_auth(username, password):
    """Authenticate against app_users table. Returns dict or None."""
    import sys
    try:
        print(f"[AUTH] Tentando autenticar usuario: {username}", file=sys.stderr, flush=True)
        print(f"[AUTH] DB_DSN host: {DB_DSN.get('host')}, dbname: {DB_DSN.get('dbname')}", file=sys.stderr, flush=True)
        conn = psycopg2.connect(**DB_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, username, pw_hash, role FROM app_users WHERE username = %s",
                        (username,))
            row = cur.fetchone()
        conn.close()
        if row:
            input_hash = _hash_pw(password)
            print(f"[AUTH] Usuario encontrado: {row['username']}", file=sys.stderr, flush=True)
            print(f"[AUTH] Hash match: {row['pw_hash'] == input_hash}", file=sys.stderr, flush=True)
            if row["pw_hash"] == input_hash:
                return dict(row)
        else:
            print(f"[AUTH] Usuario nao encontrado: {username}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[AUTH] Erro: {e}", file=sys.stderr, flush=True)
    return None


def _get_user_permissions(user_id):
    """Returns list of page slugs the user can access."""
    try:
        conn = psycopg2.connect(**DB_DSN)
        with conn.cursor() as cur:
            cur.execute("SELECT page FROM user_permissions WHERE user_id = %s", (user_id,))
            pages = [r[0] for r in cur.fetchall()]
        conn.close()
        return pages
    except Exception:
        return []


@auth_bp.route("/health")
def health():
    return "ok", 200


@auth_bp.before_app_request
def require_auth():
    if request.path in ("/login", "/health"):
        return
    if request.path.startswith("/static/"):
        return
    if not session.get("authenticated"):
        if request.path.startswith("/api/"):
            return jsonify({"error": "Não autenticado"}), 401
        return redirect(url_for("auth_bp.login"))
    if "role" not in session:
        uid = session.get("user_id")
        if uid and uid != 0:
            try:
                conn = get_conn()
                with conn.cursor() as cur:
                    cur.execute("SELECT role FROM app_users WHERE id = %s", (uid,))
                    row = cur.fetchone()
                conn.close()
                if row:
                    session["role"] = row[0]
                    return
            except Exception:
                pass
        session.clear()
        if request.path.startswith("/api/"):
            return jsonify({"error": "Sessão expirada, faça login novamente"}), 401
        return redirect(url_for("auth_bp.login"))


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    import sys
    error = None
    if request.method == "POST":
        user = request.form.get("username", "")
        pwd = request.form.get("password", "")
        print(f"[LOGIN] Tentando login: user='{user}', pwd_len={len(pwd)}", file=sys.stderr, flush=True)
        db_user = _db_auth(user, pwd)
        print(f"[LOGIN] db_user result: {db_user}", file=sys.stderr, flush=True)
        if db_user:
            session["authenticated"] = True
            session["user_id"] = db_user["id"]
            session["username"] = db_user["username"]
            session["role"] = db_user["role"]
            print(f"[LOGIN] Sucesso! Redirecionando para /", file=sys.stderr, flush=True)
            return redirect("/")
        print(f"[LOGIN] DB auth falhou, tentando fallback...", file=sys.stderr, flush=True)
        print(f"[LOGIN] APP_USER_FALLBACK='{APP_USER_FALLBACK}', APP_PASS_FALLBACK='{APP_PASS_FALLBACK}'", file=sys.stderr, flush=True)
        if APP_PASS_FALLBACK and user == APP_USER_FALLBACK and pwd == APP_PASS_FALLBACK:
            session["authenticated"] = True
            session["user_id"] = 0
            session["username"] = APP_USER_FALLBACK
            session["role"] = "admin"
            return redirect("/")
        error = "Usuário ou senha incorretos."
    return render_template("login.html", error=error)


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth_bp.login"))


@auth_bp.route("/api/me")
def api_me():
    """Returns current user info + permissions for sidebar rendering."""
    uid = session.get("user_id", 0)
    role = session.get("role", "admin")
    if role == "admin":
        pages = list(ALL_PAGES)
    else:
        pages = _get_user_permissions(uid)
    return jsonify({
        "user_id": uid,
        "username": session.get("username", ""),
        "role": role,
        "pages": pages,
    })


# ---------------------------------------------------------------------------
# Gestão de usuários
# ---------------------------------------------------------------------------

def _is_admin_or_bootstrap():
    """Allow access if admin role or no users exist yet (first-time setup)."""
    if session.get("role") == "admin":
        return True
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM app_users")
            count = cur.fetchone()[0]
        conn.close()
        return count == 0
    except Exception:
        return False


@auth_bp.route("/api/users", methods=["GET"])
def api_users_list():
    if not _is_admin_or_bootstrap():
        return jsonify({"error": "Sem permissão"}), 403
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT u.id, u.username, u.role, u.created_at,
                   ARRAY(SELECT p.page FROM user_permissions p WHERE p.user_id = u.id ORDER BY p.page) AS pages
            FROM app_users u ORDER BY u.id
        """)
        users = cur.fetchall()
    conn.close()
    for u in users:
        u["created_at"] = to_brt(u["created_at"])
    return jsonify({"users": users, "all_pages": ALL_PAGES})


@auth_bp.route("/api/users", methods=["POST"])
def api_users_create():
    if not _is_admin_or_bootstrap():
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    username = (body.get("username") or "").strip()
    password = body.get("password", "")
    role = body.get("role", "viewer")
    pages = body.get("pages", [])
    if not username or not password:
        return jsonify({"error": "Usuário e senha são obrigatórios"}), 400
    if role not in ("admin", "viewer"):
        role = "viewer"
    is_bootstrap = session.get("role") != "admin"
    if is_bootstrap:
        role = "admin"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO app_users (username, pw_hash, role) VALUES (%s, %s, %s) RETURNING id",
                (username, _hash_pw(password), role),
            )
            uid = cur.fetchone()[0]
            if role == "admin":
                pages = list(ALL_PAGES)
            for pg in pages:
                if pg in ALL_PAGES:
                    cur.execute("INSERT INTO user_permissions (user_id, page) VALUES (%s, %s)",
                                (uid, pg))
        conn.commit()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        conn.close()
        return jsonify({"error": "Usuário já existe"}), 409
    conn.close()
    if is_bootstrap:
        session["user_id"] = uid
        session["username"] = username
        session["role"] = "admin"
    return jsonify({"ok": True, "id": uid})


@auth_bp.route("/api/users/<int:uid>", methods=["PUT"])
def api_users_update(uid):
    if session.get("role") != "admin":
        return jsonify({"error": "Sem permissão"}), 403
    body = request.json or {}
    role = body.get("role")
    pages = body.get("pages")
    password = body.get("password")
    conn = get_conn()
    with conn.cursor() as cur:
        if password:
            cur.execute("UPDATE app_users SET pw_hash = %s WHERE id = %s",
                        (_hash_pw(password), uid))
        if role and role in ("admin", "viewer"):
            cur.execute("UPDATE app_users SET role = %s WHERE id = %s", (role, uid))
        if pages is not None:
            if role == "admin":
                pages = list(ALL_PAGES)
            cur.execute("DELETE FROM user_permissions WHERE user_id = %s", (uid,))
            for pg in pages:
                if pg in ALL_PAGES:
                    cur.execute("INSERT INTO user_permissions (user_id, page) VALUES (%s, %s)",
                                (uid, pg))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@auth_bp.route("/api/users/<int:uid>", methods=["DELETE"])
def api_users_delete(uid):
    if session.get("role") != "admin":
        return jsonify({"error": "Sem permissão"}), 403
    if uid == session.get("user_id"):
        return jsonify({"error": "Não é possível deletar o próprio usuário"}), 400
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM app_users WHERE id = %s", (uid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})
