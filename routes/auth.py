import os
import hashlib
import psycopg2
import psycopg2.extras
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, session
from db import get_conn, DB_DSN
from helpers import ALL_PAGES, APP_USER_FALLBACK, APP_PASS_FALLBACK, to_brt

KOMMO_DB_DSN = dict(
    host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
    port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
    user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
    password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
    dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
)

auth_bp = Blueprint("auth_bp", __name__)


def _hash_pw(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _db_auth(username, password):
    """Authenticate against app_users table. Returns dict or None."""
    try:
        conn = psycopg2.connect(**DB_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, username, pw_hash, role FROM app_users WHERE username = %s",
                        (username,))
            row = cur.fetchone()
        conn.close()
        if row and row["pw_hash"] == _hash_pw(password):
            return dict(row)
    except Exception:
        pass
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
    error = None
    if request.method == "POST":
        user = request.form.get("username", "")
        pwd = request.form.get("password", "")
        db_user = _db_auth(user, pwd)
        if db_user:
            session["authenticated"] = True
            session["user_id"] = db_user["id"]
            session["username"] = db_user["username"]
            session["role"] = db_user["role"]
            return redirect("/")
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
    kommo_user_id = None
    categoria = None
    if uid and uid != 0:
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT kommo_user_id, categoria FROM app_users WHERE id = %s", (uid,))
                row = cur.fetchone()
                if row:
                    kommo_user_id = row[0]
                    categoria = row[1]
            conn.close()
        except Exception:
            pass
    return jsonify({
        "user_id": uid,
        "username": session.get("username", ""),
        "role": role,
        "pages": pages,
        "kommo_user_id": kommo_user_id,
        "categoria": categoria,
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
            SELECT u.id, u.username, u.role, u.kommo_user_id, u.email_cruzeiro,
                   u.categoria, u.datacrazy_user_id, u.created_at,
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
    kommo_user_id = body.get("kommo_user_id")
    email_cruzeiro = (body.get("email_cruzeiro") or "").strip() or None
    categoria = (body.get("categoria") or "").strip() or None
    datacrazy_user_id = (body.get("datacrazy_user_id") or "").strip() or None
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
                "INSERT INTO app_users (username, pw_hash, role, kommo_user_id, email_cruzeiro, categoria, datacrazy_user_id) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (username, _hash_pw(password), role, kommo_user_id or None, email_cruzeiro, categoria, datacrazy_user_id),
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
    kommo_user_id = body.get("kommo_user_id")
    conn = get_conn()
    with conn.cursor() as cur:
        if password:
            cur.execute("UPDATE app_users SET pw_hash = %s WHERE id = %s",
                        (_hash_pw(password), uid))
        if role and role in ("admin", "viewer"):
            cur.execute("UPDATE app_users SET role = %s WHERE id = %s", (role, uid))
        if "kommo_user_id" in body:
            cur.execute("UPDATE app_users SET kommo_user_id = %s WHERE id = %s",
                        (kommo_user_id or None, uid))
        if "email_cruzeiro" in body:
            cur.execute("UPDATE app_users SET email_cruzeiro = %s WHERE id = %s",
                        ((body["email_cruzeiro"] or "").strip() or None, uid))
        if "categoria" in body:
            cur.execute("UPDATE app_users SET categoria = %s WHERE id = %s",
                        ((body["categoria"] or "").strip() or None, uid))
        if "datacrazy_user_id" in body:
            cur.execute("UPDATE app_users SET datacrazy_user_id = %s WHERE id = %s",
                        ((body["datacrazy_user_id"] or "").strip() or None, uid))
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


@auth_bp.route("/api/users/import-kommo", methods=["POST"])
def api_users_import_kommo():
    """Import Kommo users as app_users via Kommo API v4.

    Uses email as username (login). If no email, generates slug from name.
    Default password: eduit2026, role: viewer, permission: minha_performance.
    Also updates existing users' usernames to email if they had slug-based names.
    """
    if session.get("role") != "admin":
        return jsonify({"error": "Sem permissão"}), 403

    import re
    import unicodedata
    import requests as req

    DEFAULT_PW = "eduit2026"
    DEFAULT_PAGES = ["minha_performance"]
    KOMMO_BASE = os.getenv("KOMMO_BASE_URL", "https://admamoeduitcombr.kommo.com").rstrip("/")
    KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")

    if not KOMMO_TOKEN:
        return jsonify({"ok": False, "error": "KOMMO_TOKEN não configurado"}), 500

    def _slug(name):
        nfkd = unicodedata.normalize("NFKD", name)
        ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
        slug = re.sub(r"[^a-z0-9]+", ".", ascii_only.lower()).strip(".")
        return slug or "user"

    kommo_users = []
    try:
        headers = {"Authorization": f"Bearer {KOMMO_TOKEN}"}
        page = 1
        while True:
            resp = req.get(
                f"{KOMMO_BASE}/api/v4/users",
                headers=headers,
                params={"page": page, "limit": 250},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            users_page = data.get("_embedded", {}).get("users", [])
            if not users_page:
                break
            for u in users_page:
                kommo_users.append({
                    "id": u.get("id"),
                    "name": u.get("name", ""),
                    "email": u.get("email", ""),
                })
            if len(users_page) < 250:
                break
            page += 1
    except Exception as e:
        return jsonify({"ok": False, "error": f"Erro ao buscar usuários do Kommo API: {e}"}), 500

    if not kommo_users:
        return jsonify({"ok": True, "summary": "Nenhum usuário encontrado na API", "created": [], "updated": [], "skipped": [], "errors": []})

    conn = get_conn()
    created = []
    updated = []
    skipped = []
    errors = []

    with conn.cursor() as cur:
        cur.execute("SELECT id, kommo_user_id, username FROM app_users WHERE kommo_user_id IS NOT NULL")
        existing_kommo = {r[1]: {"id": r[0], "username": r[2]} for r in cur.fetchall()}

        cur.execute("SELECT username FROM app_users")
        existing_usernames = {r[0] for r in cur.fetchall()}

        for ku in kommo_users:
            kid = ku["id"]
            email = ku["email"].strip().lower() if ku["email"] else ""
            name = ku["name"] or ""

            if kid in existing_kommo:
                ex = existing_kommo[kid]
                if email and ex["username"] != email and email not in existing_usernames:
                    try:
                        cur.execute("UPDATE app_users SET username = %s WHERE id = %s", (email, ex["id"]))
                        existing_usernames.discard(ex["username"])
                        existing_usernames.add(email)
                        updated.append({"kommo_id": kid, "name": name, "old": ex["username"], "new": email})
                    except Exception as e:
                        errors.append({"kommo_id": kid, "name": name, "error": f"update: {e}"})
                else:
                    skipped.append({"kommo_id": kid, "name": name, "reason": f"Já vinculado como {ex['username']}"})
                continue

            username = email if email else _slug(name)
            if username in existing_usernames:
                skipped.append({"kommo_id": kid, "name": name, "reason": f"Username '{username}' já existe"})
                continue

            try:
                cur.execute(
                    "INSERT INTO app_users (username, pw_hash, role, kommo_user_id) VALUES (%s, %s, %s, %s) RETURNING id",
                    (username, _hash_pw(DEFAULT_PW), "viewer", kid),
                )
                uid = cur.fetchone()[0]
                for pg in DEFAULT_PAGES:
                    if pg in ALL_PAGES:
                        cur.execute("INSERT INTO user_permissions (user_id, page) VALUES (%s, %s)", (uid, pg))
                created.append({"id": uid, "kommo_id": kid, "name": name, "username": username})
                existing_usernames.add(username)
            except Exception as e:
                errors.append({"kommo_id": kid, "name": name, "error": str(e)})

    conn.commit()
    conn.close()

    parts = []
    if created: parts.append(f"{len(created)} criados (login = email)")
    if updated: parts.append(f"{len(updated)} atualizados p/ email")
    if skipped: parts.append(f"{len(skipped)} já existiam")
    if errors: parts.append(f"{len(errors)} erros")

    return jsonify({
        "ok": True,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "summary": ", ".join(parts) or "Nenhum usuário encontrado",
    })


@auth_bp.route("/api/users/import-datacrazy", methods=["POST"])
def api_users_import_datacrazy():
    """Import DataCrazy CRM users as app_users.

    Fetches from DataCrazy accounts API /api/accounts/company-users (paginated).
    Uses email as username (login). Default password: eduit2026, role: viewer.
    Also updates existing users' usernames to email if they had slug-based names.
    """
    if session.get("role") != "admin":
        return jsonify({"error": "Sem permissão"}), 403

    import requests as req

    API_BASE = "https://accounts.g1.datacrazy.io"
    API_TOKEN = os.getenv("DATACRAZY_API_TOKEN", "")
    if not API_TOKEN:
        return jsonify({"ok": False, "error": "DATACRAZY_API_TOKEN não configurado"}), 500

    DEFAULT_PW = "eduit2026"
    DEFAULT_PAGES = ["minha_performance"]
    PAGE_SIZE = 50

    all_users = []
    try:
        headers = {
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json",
            "x-language": "pt",
            "x-timezone": "America/Sao_Paulo",
        }
        skip = 0
        while True:
            resp = req.get(
                f"{API_BASE}/api/accounts/company-users",
                headers=headers,
                params={"skip": skip, "take": PAGE_SIZE, "url": "/company-users"},
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
            data = body.get("data", body) if isinstance(body, dict) else body
            if isinstance(data, list):
                all_users.extend(data)
                total = body.get("count", body.get("total", len(data))) if isinstance(body, dict) else len(data)
                skip += PAGE_SIZE
                if skip >= total or len(data) < PAGE_SIZE:
                    break
            else:
                break
    except Exception as e:
        return jsonify({"ok": False, "error": f"Erro ao conectar ao DataCrazy: {e}"}), 500

    if not all_users:
        return jsonify({"ok": True, "summary": "Nenhum usuário encontrado na API", "created": [], "updated": [], "skipped": [], "errors": []})

    conn = get_conn()
    created = []
    updated = []
    skipped = []
    errors = []

    with conn.cursor() as cur:
        cur.execute("SELECT id, datacrazy_user_id, username FROM app_users WHERE datacrazy_user_id IS NOT NULL")
        existing_dc = {r[1]: {"id": r[0], "username": r[2]} for r in cur.fetchall()}

        cur.execute("SELECT username FROM app_users")
        existing_usernames = {r[0] for r in cur.fetchall()}

        for u in all_users:
            uid_dc = str(u.get("id") or u.get("userId") or "")
            name = u.get("name") or u.get("fullName") or u.get("displayName") or ""
            email = (u.get("email") or "").strip().lower()

            if not uid_dc:
                continue

            if uid_dc in existing_dc:
                ex = existing_dc[uid_dc]
                if email and ex["username"] != email and email not in existing_usernames:
                    try:
                        cur.execute("UPDATE app_users SET username = %s WHERE id = %s", (email, ex["id"]))
                        existing_usernames.discard(ex["username"])
                        existing_usernames.add(email)
                        updated.append({"dc_id": uid_dc, "name": name, "old": ex["username"], "new": email})
                    except Exception as e:
                        errors.append({"dc_id": uid_dc, "name": name, "error": f"update: {e}"})
                else:
                    skipped.append({"dc_id": uid_dc, "name": name, "reason": f"Já vinculado como {ex['username']}"})
                continue

            username = email if email else name.lower().replace(" ", ".")
            if not username:
                skipped.append({"dc_id": uid_dc, "name": name, "reason": "Sem email/nome"})
                continue
            if username in existing_usernames:
                skipped.append({"dc_id": uid_dc, "name": name, "reason": f"Username '{username}' já existe"})
                continue

            try:
                cur.execute(
                    "INSERT INTO app_users (username, pw_hash, role, datacrazy_user_id) VALUES (%s, %s, %s, %s) RETURNING id",
                    (username, _hash_pw(DEFAULT_PW), "viewer", uid_dc),
                )
                new_id = cur.fetchone()[0]
                for pg in DEFAULT_PAGES:
                    if pg in ALL_PAGES:
                        cur.execute("INSERT INTO user_permissions (user_id, page) VALUES (%s, %s)", (new_id, pg))
                created.append({"id": new_id, "dc_id": uid_dc, "name": name, "username": username})
                existing_usernames.add(username)
            except Exception as e:
                errors.append({"dc_id": uid_dc, "name": name, "error": str(e)})

    conn.commit()
    conn.close()

    parts = []
    if created: parts.append(f"{len(created)} criados (login = email)")
    if updated: parts.append(f"{len(updated)} atualizados p/ email")
    if skipped: parts.append(f"{len(skipped)} já existiam")
    if errors: parts.append(f"{len(errors)} erros")

    return jsonify({
        "ok": True,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "summary": ", ".join(parts) or "Nenhum usuário encontrado",
    })
