"""Subir Blog: publica posts na tabela public.blog_posts do Supabase.

Mesmo projeto Supabase acadêmico (SUPABASE_ACADEMICO_URL / SUPABASE_ACADEMICO_KEY).
O site público só faz SELECT — aqui gravamos apenas os campos, sem URLs de página.

GET  /api/blog/posts                  lista TODOS os posts (mais novos primeiro)
POST /api/blog/posts                  cria post (valida, gera slug único, trata destaque)
GET  /api/blog/posts/<id>             post completo (para edição)
PUT  /api/blog/posts/<id>             atualiza post (id imutável; trata destaque)
DELETE /api/blog/posts/<id>           apaga post
POST /api/blog/upload-image           upload da capa p/ Storage bucket 'blog' → URL pública
"""
from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
import urllib.parse
import urllib.request
import uuid
from datetime import datetime

from flask import Blueprint, jsonify, request, session

from helpers import can_access_subir_blog

logger = logging.getLogger(__name__)
blog_posts_bp = Blueprint("blog_posts_bp", __name__)

_TABLE = "blog_posts"
_BUCKET = "blog"

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
    parts = re.split(r"(<[^>]+>)", s)
    return "".join(p if p.startswith("<") else _break_long_runs(p) for p in parts)


def _require_blog_access():
    if not can_access_subir_blog(session.get("role") or "", session.get("username") or ""):
        return jsonify({"error": "Sem permissão"}), 403
    return None


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
    except Exception as e:
        logger.exception("blog: falha ao listar posts")
        return jsonify({"error": str(e)}), 502
    return jsonify({"ok": True, "categorias": CATEGORIAS, "posts": rows})


def _validate_payload(body: dict) -> tuple[dict | None, str | None]:
    """Valida e normaliza o payload. Retorna (row, erro)."""
    title = (body.get("title") or "").strip()
    summary = (body.get("summary") or "").strip()
    content = (body.get("content") or "").strip()
    category = (body.get("category") or "").strip()
    tags = body.get("tags") or []
    image = (body.get("image") or "").strip()
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

    if not title or not summary or not content:
        return None, "Título, resumo e conteúdo são obrigatórios."
    if category not in CATEGORIAS:
        return None, f"Categoria inválida. Use exatamente uma de: {', '.join(CATEGORIAS)}"
    if not isinstance(tags, list) or not tags:
        return None, "Informe ao menos uma tag (a categoria principal)."
    bad = [t for t in tags if t not in CATEGORIAS]
    if bad:
        return None, f"Tags inválidas: {', '.join(map(str, bad))}"
    if category not in tags:
        tags = [category] + tags
    if not _DATE_RE.match(date_str):
        return None, "Data fora do formato esperado (ex.: 26 Ago 2026)."
    if not image.startswith("https://"):
        return None, "Imagem deve ser uma URL pública https:// (faça upload ou cole o link)."
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
        base, key = _cfg()
        slug = _unique_slug(base, key, row["title"])
        if row["is_featured"]:
            # só 1 destaque por vez: desmarca o anterior
            _req("PATCH", f"{base}/rest/v1/{_TABLE}?is_featured=eq.true", key,
                 {"is_featured": False}, {"Prefer": "return=minimal"})
        row["id"] = slug
        _req("POST", f"{base}/rest/v1/{_TABLE}", key, row,
             {"Prefer": "return=minimal"})
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
