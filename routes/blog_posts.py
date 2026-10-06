"""Subir Blog: publica posts na tabela public.blog_posts do Supabase.

Mesmo projeto Supabase acadêmico (SUPABASE_ACADEMICO_URL / SUPABASE_ACADEMICO_KEY).
O site público só faz SELECT — aqui gravamos apenas os campos, sem URLs de página.

GET  /api/blog/posts                  lista posts publicados (mais novos primeiro)
POST /api/blog/posts                  cria post publicado
GET  /api/blog/posts/<id>             post completo (para edição)
PUT  /api/blog/posts/<id>             atualiza post (id imutável; trata destaque)
DELETE /api/blog/posts/<id>           apaga post
GET/POST /api/blog/scheduled          lista / cria post programado (Postgres local)
GET/PUT/DELETE /api/blog/scheduled/<id>
POST /api/blog/scheduled/<id>/publish publica agora um programado
POST /api/blog/upload-image           upload da capa p/ Storage bucket 'blog' → URL pública
"""
from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Blueprint, jsonify, request, session
from psycopg2.extras import Json, RealDictCursor

from db import get_conn
from helpers import can_access_subir_blog

logger = logging.getLogger(__name__)
blog_posts_bp = Blueprint("blog_posts_bp", __name__)

_TABLE = "blog_posts"
_BUCKET = "blog"
_TZ = ZoneInfo("America/Sao_Paulo")
_AGENDADOS_READY = False

CATEGORIAS = [
    "Cursos de Graduação",
    "Cursos de Pós-Graduação",
    "Curiosidades/dicas",
    "Financeiro",
    "Duvidas Academicas",
]

_MESES = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
          "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
_DATE_RE = re.compile(
    r"^\d{2} (Jan|Fev|Mar|Abr|Mai|Jun|Jul|Ago|Set|Out|Nov|Dez) \d{4}$"
)
_WRAP_OPEN = '<div style="overflow-wrap:anywhere;word-break:break-word">'
_WRAP_CLOSE = "</div>"
_ZWSP = "\u200b"
_WRAP_CHUNK = 40


def _unwrap_content(content: str) -> str:
    s = content or ""
    if s.startswith(_WRAP_OPEN) and s.endswith(_WRAP_CLOSE):
        s = s[len(_WRAP_OPEN) : -len(_WRAP_CLOSE)]
    return s.replace(_ZWSP, "")


def _break_long_runs(text: str) -> str:
    """Insere ZWSP a cada N chars em sequências sem espaço — o site público
    renderiza texto puro, então CSS no HTML não vale; o ZWSP força a quebra."""
    out: list[str] = []
    buf: list[str] = []

    def flush() -> None:
        s = "".join(buf)
        buf.clear()
        if len(s) <= _WRAP_CHUNK:
            out.append(s)
            return
        parts = [s[i : i + _WRAP_CHUNK] for i in range(0, len(s), _WRAP_CHUNK)]
        out.append(_ZWSP.join(parts))

    for ch in text:
        if ch.isspace() or ch in "-–—/\\":
            flush()
            out.append(ch)
        else:
            buf.append(ch)
    flush()
    return "".join(out)


def _prepare_content(content: str) -> str:
    s = _unwrap_content(content)
    if re.search(r"<(?:br|b|strong|i|em|u|p|div|h2|h3|ul|ol|li|blockquote|a)\b", s, re.I):
        return s
    parts = re.split(r"(<[^>]+>)", s)
    return "".join(p if p.startswith("<") else _break_long_runs(p) for p in parts)


def _require_blog_access():
    if not can_access_subir_blog(session.get("role") or "", session.get("username") or ""):
        return jsonify({"error": "Sem permissão"}), 403
    return None


def _ensure_blog_agendados_table():
    global _AGENDADOS_READY
    if _AGENDADOS_READY:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS blog_posts_agendados (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    content TEXT NOT NULL,
                    category TEXT NOT NULL,
                    badge TEXT,
                    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
                    date TEXT NOT NULL,
                    read_time TEXT,
                    image TEXT NOT NULL,
                    is_featured BOOLEAN NOT NULL DEFAULT FALSE,
                    author_name TEXT,
                    author_role TEXT,
                    author_avatar TEXT,
                    scheduled_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_blog_agendados_at "
                "ON blog_posts_agendados (scheduled_at ASC)"
            )
        conn.commit()
        _AGENDADOS_READY = True
    except Exception:
        conn.rollback()
        logger.exception("blog: falha ao criar blog_posts_agendados")
        raise
    finally:
        conn.close()


def _parse_scheduled_at(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_TZ)
    return dt


def _plain_text(content: str) -> str:
    return re.sub(r"<[^>]+>", " ", content or "").strip()


def _sanitize_content(content: str) -> str:
    s = content or ""
    s = re.sub(r"(?is)<script[^>]*>.*?</script>", "", s)
    s = re.sub(r"(?is)<style[^>]*>.*?</style>", "", s)
    return s


def _insert_to_supabase(row: dict) -> str:
    base, key = _cfg()
    slug = _unique_slug(base, key, row["title"])
    if row.get("is_featured"):
        _req(
            "PATCH",
            f"{base}/rest/v1/{_TABLE}?is_featured=eq.true",
            key,
            {"is_featured": False},
            {"Prefer": "return=minimal"},
        )
    payload = dict(row)
    payload["id"] = slug
    _req("POST", f"{base}/rest/v1/{_TABLE}", key, payload, {"Prefer": "return=minimal"})
    return slug


def _agendado_to_row(rec: dict) -> dict:
    tags = rec.get("tags") or []
    if isinstance(tags, str):
        tags = json.loads(tags)
    return {
        "title": rec["title"],
        "summary": rec["summary"],
        "content": rec["content"],
        "category": rec["category"],
        "badge": rec.get("badge") or rec["category"],
        "tags": list(tags),
        "date": rec["date"],
        "read_time": rec.get("read_time") or "",
        "image": rec["image"],
        "is_featured": bool(rec.get("is_featured")),
        "author_name": rec.get("author_name") or "Eduit",
        "author_role": rec.get("author_role") or "Blog Eduit",
        "author_avatar": rec.get("author_avatar"),
    }


def _agendado_public(rec: dict) -> dict:
    tags = rec.get("tags") or []
    if isinstance(tags, str):
        tags = json.loads(tags)
    sat = rec.get("scheduled_at")
    if isinstance(sat, datetime):
        sat = sat.astimezone(_TZ).isoformat(timespec="minutes")
    return {
        "id": rec["id"],
        "title": rec["title"],
        "summary": rec.get("summary") or "",
        "content": _unwrap_content(rec.get("content") or ""),
        "category": rec["category"],
        "tags": list(tags),
        "date": rec.get("date") or "",
        "read_time": rec.get("read_time") or "",
        "image": rec.get("image") or "",
        "is_featured": bool(rec.get("is_featured")),
        "author_name": rec.get("author_name") or "",
        "author_role": rec.get("author_role") or "",
        "scheduled_at": sat,
    }


def _cfg() -> tuple[str, str]:
    base = os.getenv("SUPABASE_ACADEMICO_URL", "").rstrip("/")
    key = os.getenv("SUPABASE_ACADEMICO_KEY", "")
    if not base or not key:
        raise RuntimeError("SUPABASE_ACADEMICO_URL/SUPABASE_ACADEMICO_KEY não configurados no .env")
    return base, key


def _req(method: str, url: str, key: str, body=None, extra_headers: dict | None = None):
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "User-Agent": "dcz-crm-sync/1.0",
    }
    if extra_headers:
        headers.update(extra_headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read().decode("utf-8")
    return json.loads(raw) if raw else None


def _slugify(title: str) -> str:
    s = unicodedata.normalize("NFKD", title or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _slug_exists(base: str, key: str, slug: str) -> bool:
    url = f"{base}/rest/v1/{_TABLE}?select=id&id=eq.{urllib.parse.quote(slug, safe='')}"
    rows = _req("GET", url, key)
    return bool(rows)


def _unique_slug(base: str, key: str, title: str) -> str:
    slug = _slugify(title)
    if not slug:
        raise ValueError("Título inválido para gerar o slug.")
    candidate, n = slug, 2
    while _slug_exists(base, key, candidate):
        candidate = f"{slug}-{n}"
        n += 1
    return candidate


def _fmt_date(dt: datetime) -> str:
    return f"{dt.day:02d} {_MESES[dt.month - 1]} {dt.year}"


def _date_sort_key(date_str: str) -> datetime:
    m = _DATE_RE.match(date_str or "")
    if not m:
        return datetime.min
    day_s, mon, year_s = (date_str or "").split()
    return datetime(int(year_s), _MESES.index(mon) + 1, int(day_s))


def _read_time(content: str) -> str:
    text = re.sub(r"<[^>]+>", " ", content or "")
    words = len(text.split())
    minutes = max(1, round(words / 200))
    return f"{minutes} min de leitura"


@blog_posts_bp.route("/api/blog/posts", methods=["GET"])
def list_posts():
    denied = _require_blog_access()
    if denied:
        return denied
    try:
        base, key = _cfg()
        url = (f"{base}/rest/v1/{_TABLE}"
               "?select=id,title,category,date,read_time,image,is_featured,created_at"
               "&order=created_at.desc")
        rows: list[dict] = []
        offset = 0
        while True:
            batch = _req("GET", url, key,
                         extra_headers={"Range-Unit": "items",
                                        "Range": f"{offset}-{offset + 999}"})
            rows.extend(batch or [])
            if not batch or len(batch) < 1000:
                break
            offset += 1000
        rows.sort(
            key=lambda r: (_date_sort_key(r.get("date") or ""), r.get("created_at") or ""),
            reverse=True,
        )
    except Exception as e:
        logger.exception("blog: falha ao listar posts")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "categorias": CATEGORIAS, "posts": rows})


_IMAGE_EXT = re.compile(r"\.(jpe?g|png|webp|gif)(?:$|\.)", re.I)


def _direct_image_url(url: str) -> str:
    """Link de resultado do Google Imagens não é arquivo. A foto está em imgurl."""
    raw = (url or "").strip()
    try:
        parsed = urllib.parse.urlparse(raw)
    except Exception:
        return raw
    if "google." in parsed.netloc.lower() and parsed.path.rstrip("/") == "/imgres":
        inner = (urllib.parse.parse_qs(parsed.query).get("imgurl") or [""])[0].strip()
        if inner.startswith("https://"):
            return inner
    return raw


def _url_is_image(url: str) -> bool:
    path = urllib.parse.urlparse(url).path
    if _IMAGE_EXT.search(path):
        return True
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0", "Range": "bytes=0-16"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return (r.headers.get("Content-Type") or "").lower().startswith("image/")
    except Exception:
        return False


def _validate_payload(body: dict) -> tuple[dict | None, str | None]:
    """Valida e normaliza o payload. Retorna (row, erro)."""
    title = (body.get("title") or "").strip()
    summary = (body.get("summary") or "").strip()
    content = _sanitize_content(body.get("content") or "").strip()
    category = (body.get("category") or "").strip()
    tags = body.get("tags") or []
    image = _direct_image_url(body.get("image") or "")
    read_time = (body.get("read_time") or "").strip()
    author_name = (body.get("author_name") or "").strip() or "Eduit"
    author_role = (body.get("author_role") or "").strip() or "Blog Eduit"
    author_avatar = (body.get("author_avatar") or "").strip() or None

    raw_date = (body.get("date") or "").strip()
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", raw_date):
            date_str = _fmt_date(datetime.strptime(raw_date, "%Y-%m-%d"))
        elif raw_date:
            date_str = raw_date
        else:
            date_str = _fmt_date(datetime.now())
    except ValueError:
        return None, "Data inválida."

    if not title or not summary or not _plain_text(content):
        return None, "Título, resumo e conteúdo são obrigatórios."
    if category not in CATEGORIAS:
        return None, f"Categoria inválida. Use exatamente uma de: {', '.join(CATEGORIAS)}"
    if not isinstance(tags, list):
        return None, "Tags inválidas."
    tags = [t for t in tags if isinstance(t, str) and t in CATEGORIAS]
    if not tags:
        return None, "Selecione ao menos uma categoria nas tags."
    if not _DATE_RE.match(date_str):
        return None, "Data fora do formato esperado (ex.: 26 Ago 2026)."
    if not image.startswith("https://"):
        return None, "Imagem deve ser uma URL pública https:// (faça upload ou cole o link)."
    if not _url_is_image(image):
        return None, (
            "Esse link não é o arquivo da imagem. O endereço do Google Imagens abre uma página, "
            "não a foto. Envie o arquivo ou cole o link direto, que termina em .jpg, .png, .webp ou .gif."
        )
    if not read_time:
        read_time = _read_time(content)

    return {
        "title": title,
        "summary": summary,
        "content": _prepare_content(content),
        "category": category,
        "badge": category,
        "tags": tags,
        "date": date_str,
        "read_time": read_time,
        "image": image,
        "is_featured": bool(body.get("is_featured")),
        "author_name": author_name,
        "author_role": author_role,
        "author_avatar": author_avatar,
    }, None


@blog_posts_bp.route("/api/blog/posts", methods=["POST"])
def create_post():
    denied = _require_blog_access()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    row, err = _validate_payload(body)
    if err:
        return jsonify({"error": err}), 400

    try:
        slug = _insert_to_supabase(row)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception("blog: falha ao criar post")
        return jsonify({"error": str(e)}), 502

    return jsonify({"ok": True, "id": slug, "is_featured": row["is_featured"]})


@blog_posts_bp.route("/api/blog/posts/<path:post_id>", methods=["GET"])
def get_post(post_id: str):
    denied = _require_blog_access()
    if denied:
        return denied
    try:
        base, key = _cfg()
        url = f"{base}/rest/v1/{_TABLE}?select=*&id=eq.{urllib.parse.quote(post_id, safe='')}"
        rows = _req("GET", url, key)
    except Exception as e:
        logger.exception("blog: falha ao ler post")
        return jsonify({"error": str(e)}), 502
    if not rows:
        return jsonify({"error": "Post não encontrado."}), 404
    post = rows[0]
    post["content"] = _unwrap_content(post.get("content") or "")
    return jsonify({"ok": True, "post": post})


@blog_posts_bp.route("/api/blog/posts/<path:post_id>", methods=["PUT"])
def update_post(post_id: str):
    denied = _require_blog_access()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    row, err = _validate_payload(body)
    if err:
        return jsonify({"error": err}), 400

    try:
        base, key = _cfg()
        q = urllib.parse.quote(post_id, safe="")
        if row["is_featured"]:
            # só 1 destaque por vez: desmarca os outros (exceto este)
            _req("PATCH", f"{base}/rest/v1/{_TABLE}?is_featured=eq.true&id=neq.{q}", key,
                 {"is_featured": False}, {"Prefer": "return=minimal"})
        _req("PATCH", f"{base}/rest/v1/{_TABLE}?id=eq.{q}", key, row,
             {"Prefer": "return=minimal"})
    except Exception as e:
        logger.exception("blog: falha ao atualizar post")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "id": post_id, "is_featured": row["is_featured"]})


@blog_posts_bp.route("/api/blog/posts/<path:post_id>", methods=["DELETE"])
def delete_post(post_id: str):
    denied = _require_blog_access()
    if denied:
        return denied
    try:
        base, key = _cfg()
        _req("DELETE", f"{base}/rest/v1/{_TABLE}?id=eq.{urllib.parse.quote(post_id, safe='')}",
             key, None, {"Prefer": "return=minimal"})
    except Exception as e:
        logger.exception("blog: falha ao apagar post")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "id": post_id})


@blog_posts_bp.route("/api/blog/scheduled", methods=["GET"])
def list_scheduled():
    denied = _require_blog_access()
    if denied:
        return denied
    try:
        _ensure_blog_agendados_table()
        conn = get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, title, category, date, read_time, image, is_featured,
                           scheduled_at, created_at
                    FROM blog_posts_agendados
                    ORDER BY scheduled_at DESC
                    """
                )
                rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as e:
        logger.exception("blog: falha ao listar agendados")
        return jsonify({"error": str(e)}), 502
    posts = []
    for rec in rows:
        sat = rec.get("scheduled_at")
        if isinstance(sat, datetime):
            sat = sat.astimezone(_TZ).isoformat(timespec="minutes")
        posts.append({
            "id": rec["id"],
            "title": rec["title"],
            "category": rec["category"],
            "date": rec.get("date") or "",
            "read_time": rec.get("read_time") or "",
            "image": rec.get("image") or "",
            "is_featured": bool(rec.get("is_featured")),
            "scheduled_at": sat,
        })
    return jsonify({"ok": True, "posts": posts})


@blog_posts_bp.route("/api/blog/scheduled", methods=["POST"])
def create_scheduled():
    denied = _require_blog_access()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    row, err = _validate_payload(body)
    if err:
        return jsonify({"error": err}), 400
    sat = _parse_scheduled_at(body.get("scheduled_at") or "")
    if not sat:
        return jsonify({"error": "Informe data e hora do agendamento."}), 400
    if sat <= datetime.now(_TZ):
        return jsonify({"error": "O horário do agendamento precisa ser no futuro."}), 400
    try:
        _ensure_blog_agendados_table()
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO blog_posts_agendados (
                        title, summary, content, category, badge, tags, date,
                        read_time, image, is_featured, author_name, author_role,
                        author_avatar, scheduled_at
                    ) VALUES (
                        %(title)s, %(summary)s, %(content)s, %(category)s, %(badge)s,
                        %(tags)s, %(date)s, %(read_time)s, %(image)s, %(is_featured)s,
                        %(author_name)s, %(author_role)s, %(author_avatar)s, %(scheduled_at)s
                    ) RETURNING id
                    """,
                    {**row, "tags": Json(row["tags"]), "scheduled_at": sat},
                )
                sid = cur.fetchone()[0]
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.exception("blog: falha ao agendar post")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "id": sid, "scheduled_at": sat.isoformat(timespec="minutes")})


@blog_posts_bp.route("/api/blog/scheduled/<int:sid>", methods=["GET"])
def get_scheduled(sid: int):
    denied = _require_blog_access()
    if denied:
        return denied
    try:
        _ensure_blog_agendados_table()
        conn = get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM blog_posts_agendados WHERE id = %s", (sid,))
                rec = cur.fetchone()
        finally:
            conn.close()
    except Exception as e:
        logger.exception("blog: falha ao ler agendado")
        return jsonify({"error": str(e)}), 502
    if not rec:
        return jsonify({"error": "Post programado não encontrado."}), 404
    return jsonify({"ok": True, "post": _agendado_public(rec)})


@blog_posts_bp.route("/api/blog/scheduled/<int:sid>", methods=["PUT"])
def update_scheduled(sid: int):
    denied = _require_blog_access()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    row, err = _validate_payload(body)
    if err:
        return jsonify({"error": err}), 400
    sat = _parse_scheduled_at(body.get("scheduled_at") or "")
    if not sat:
        return jsonify({"error": "Informe data e hora do agendamento."}), 400
    if sat <= datetime.now(_TZ):
        return jsonify({"error": "O horário do agendamento precisa ser no futuro."}), 400
    try:
        _ensure_blog_agendados_table()
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE blog_posts_agendados SET
                        title=%(title)s, summary=%(summary)s, content=%(content)s,
                        category=%(category)s, badge=%(badge)s, tags=%(tags)s,
                        date=%(date)s, read_time=%(read_time)s, image=%(image)s,
                        is_featured=%(is_featured)s, author_name=%(author_name)s,
                        author_role=%(author_role)s, author_avatar=%(author_avatar)s,
                        scheduled_at=%(scheduled_at)s, updated_at=NOW()
                    WHERE id=%(id)s
                    """,
                    {**row, "tags": Json(row["tags"]), "scheduled_at": sat, "id": sid},
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    return jsonify({"error": "Post programado não encontrado."}), 404
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.exception("blog: falha ao atualizar agendado")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "id": sid, "scheduled_at": sat.isoformat(timespec="minutes")})


@blog_posts_bp.route("/api/blog/scheduled/<int:sid>", methods=["DELETE"])
def delete_scheduled(sid: int):
    denied = _require_blog_access()
    if denied:
        return denied
    try:
        _ensure_blog_agendados_table()
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM blog_posts_agendados WHERE id = %s", (sid,))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.exception("blog: falha ao apagar agendado")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "id": sid})


@blog_posts_bp.route("/api/blog/scheduled/<int:sid>/publish", methods=["POST"])
def publish_scheduled_now(sid: int):
    denied = _require_blog_access()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    if body:
        row, err = _validate_payload(body)
        if err:
            return jsonify({"error": err}), 400
        sat = _parse_scheduled_at(body.get("scheduled_at") or "") or datetime.now(_TZ)
        try:
            _ensure_blog_agendados_table()
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE blog_posts_agendados SET
                            title=%(title)s, summary=%(summary)s, content=%(content)s,
                            category=%(category)s, badge=%(badge)s, tags=%(tags)s,
                            date=%(date)s, read_time=%(read_time)s, image=%(image)s,
                            is_featured=%(is_featured)s, author_name=%(author_name)s,
                            author_role=%(author_role)s, author_avatar=%(author_avatar)s,
                            scheduled_at=%(scheduled_at)s, updated_at=NOW()
                        WHERE id=%(id)s
                        """,
                        {**row, "tags": Json(row["tags"]), "scheduled_at": sat, "id": sid},
                    )
                    if cur.rowcount == 0:
                        conn.rollback()
                        return jsonify({"error": "Post programado não encontrado."}), 404
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.exception("blog: falha ao atualizar agendado antes de publicar")
            return jsonify({"error": str(e)}), 502
    try:
        slug = _publish_agendado(sid)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404 if "não encontrado" in str(e).lower() else 400
    except Exception as e:
        logger.exception("blog: falha ao publicar agendado")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "id": slug})


def _publish_agendado(sid: int) -> str:
    _ensure_blog_agendados_table()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM blog_posts_agendados WHERE id = %s", (sid,))
            rec = cur.fetchone()
        if not rec:
            raise ValueError("Post programado não encontrado.")
        slug = _insert_to_supabase(_agendado_to_row(rec))
        with conn.cursor() as cur:
            cur.execute("DELETE FROM blog_posts_agendados WHERE id = %s", (sid,))
        conn.commit()
        return slug
    finally:
        conn.close()


def publish_due_blog_posts():
    try:
        _ensure_blog_agendados_table()
        conn = get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT id FROM blog_posts_agendados "
                    "WHERE scheduled_at <= NOW() ORDER BY scheduled_at ASC"
                )
                due = [r["id"] for r in cur.fetchall()]
        finally:
            conn.close()
        for sid in due:
            try:
                slug = _publish_agendado(sid)
                logger.info("blog: publicado agendado #%s -> %s", sid, slug)
            except Exception:
                logger.exception("blog: falha ao publicar agendado #%s", sid)
    except Exception:
        logger.exception("blog: falha no job de agendados")


def register_blog_schedule_job(sched) -> None:
    from apscheduler.triggers.interval import IntervalTrigger
    try:
        sched.remove_job("blog_scheduled_publish")
    except Exception:
        pass
    sched.add_job(
        publish_due_blog_posts,
        trigger=IntervalTrigger(minutes=1),
        id="blog_scheduled_publish",
        replace_existing=True,
        misfire_grace_time=120,
        max_instances=1,
    )
    logger.info("blog: job de agendados registrado (interval=1min)")


@blog_posts_bp.route("/api/blog/upload-image", methods=["POST"])
def upload_image():
    denied = _require_blog_access()
    if denied:
        return denied
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "Envie um arquivo de imagem."}), 400
    ext = (f.filename.rsplit(".", 1)[-1] or "").lower()
    if ext not in ("jpg", "jpeg", "png", "webp", "gif"):
        return jsonify({"error": "Formato inválido. Use jpg, png, webp ou gif."}), 400
    mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
            "webp": "image/webp", "gif": "image/gif"}[ext]

    try:
        base, key = _cfg()
        path = f"capas/{datetime.now():%Y%m}/{uuid.uuid4().hex[:12]}.{ext}"
        url = f"{base}/storage/v1/object/{_BUCKET}/{path}"
        req = urllib.request.Request(
            url,
            data=f.read(),
            method="POST",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": mime,
                "x-upsert": "true",
                "User-Agent": "dcz-crm-sync/1.0",
            },
        )
        with urllib.request.urlopen(req, timeout=60):
            pass
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "ignore")[:300]
        logger.warning("blog: upload falhou (%s): %s", e.code, detail)
        if e.code in (400, 404):
            return jsonify({"error": f"Bucket '{_BUCKET}' não encontrado ou sem permissão. "
                                     "Crie um bucket público chamado 'blog' no Storage do Supabase."}), 502
        return jsonify({"error": f"Falha no upload (HTTP {e.code})."}), 502
    except Exception as e:
        logger.exception("blog: falha no upload da imagem")
        return jsonify({"error": str(e)}), 502

    public_url = f"{base}/storage/v1/object/public/{_BUCKET}/{path}"
    return jsonify({"ok": True, "url": public_url})
