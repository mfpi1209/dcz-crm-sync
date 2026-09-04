"""Inscrições: lê public.inscricoes_logs no Supabase de feedback/inscrições.

GET /api/inscricao?view=home|errors&from=YYYY-MM-DD&to=YYYY-MM-DD&limit=&offset=
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)
inscricao_bp = Blueprint("inscricao_bp", __name__)

_TABLE = "inscricoes_logs"
_PAGE = 1000


def _cfg() -> tuple[str, str]:
    base = (os.getenv("SUPABASE_INSCRICOES_URL") or "").rstrip("/")
    key = os.getenv("SUPABASE_INSCRICOES_KEY") or ""
    if not base or not key:
        raise RuntimeError("SUPABASE_INSCRICOES_URL/SUPABASE_INSCRICOES_KEY não configurados no .env")
    return base, key


def _day_bounds(from_s: str, to_s: str) -> tuple[str | None, str | None]:
    frm = (from_s or "").strip()
    to = (to_s or "").strip()
    if frm and len(frm) == 10:
        frm = f"{frm}T00:00:00.000Z"
    if to and len(to) == 10:
        to = f"{to}T23:59:59.999Z"
    return frm or None, to or None


def _iso_now_bounds_default() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    to = now.date().isoformat()
    frm = (now.date() - timedelta(days=7)).isoformat()
    return frm, to


def _fetch_logs(frm: str | None, to: str | None) -> list[dict]:
    base, key = _cfg()
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "User-Agent": "dcz-crm-sync/1.0",
        "Prefer": "count=exact",
    }
    rows: list[dict] = []
    offset = 0
    while True:
        params = {
            "select": "*",
            "order": "created_at.desc",
            "limit": str(_PAGE),
            "offset": str(offset),
        }
        if frm:
            params["created_at"] = f"gte.{frm}"
        url = f"{base}/rest/v1/{_TABLE}?{urllib.parse.urlencode(params)}"
        if to:
            url += f"&created_at=lte.{urllib.parse.quote(to)}"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            chunk = json.loads(raw) if raw else []
        if not isinstance(chunk, list):
            raise RuntimeError("Resposta inesperada do Supabase")
        rows.extend(chunk)
        if len(chunk) < _PAGE:
            break
        offset += _PAGE
        if offset > 20000:
            break
    return rows


def _is_ok(row: dict) -> bool:
    v = row.get("ok")
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("true", "1", "t", "yes")


def _duration_ms(row: dict) -> float | None:
    v = row.get("duration_ms")
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _hhmmss(seconds: float | None) -> str | None:
    if seconds is None:
        return None
    s = max(0, int(round(seconds)))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


def _public_row(row: dict) -> dict:
    ms = _duration_ms(row)
    return {
        "id": row.get("id"),
        "execution_id": row.get("id"),
        "created_at": row.get("created_at"),
        "data_inicio": row.get("created_at"),
        "data_fim": row.get("created_at"),
        "duration_ms": ms,
        "ok": _is_ok(row),
        "forma_ingresso": row.get("forma_ingresso") or "",
        "tipo_inscricao": row.get("forma_ingresso") or "",
        "department": row.get("department") or "",
        "etapa_erro": row.get("error_code") or "",
        "error_code": row.get("error_code") or "",
        "erro_mensagem": row.get("error_message") or "",
        "error_message": row.get("error_message") or "",
        "lead_id": row.get("lead_id"),
        "cpf": row.get("cpf") or "",
        "email": row.get("email") or "",
        "curso": row.get("curso") or "",
        "polo": row.get("polo") or "",
        "polo_km": row.get("polo_km"),
        "order_id": row.get("order_id") or "",
        "inscricao_siaa": row.get("inscricao_siaa") or "",
        "telefone": row.get("telefone") or row.get("phone") or row.get("celular") or "",
    }


def _digits(s: str) -> str:
    return "".join(c for c in str(s or "") if c.isdigit())


def _row_matches_q(row: dict, q: str) -> bool:
    needle = _digits(q)
    if len(needle) < 8:
        return False
    hay = " ".join(
        str(row.get(k) or "")
        for k in ("cpf", "telefone", "phone", "celular", "email", "lead_id")
    )
    return needle in _digits(hay)


def _home(rows: list[dict], filters: dict) -> dict:
    timed_ok = [r for r in rows if _is_ok(r) and _duration_ms(r) is not None]
    secs = [(_duration_ms(r) or 0) / 1000.0 for r in timed_ok]
    avg = (sum(secs) / len(secs)) if secs else None
    ok_n = sum(1 for r in rows if _is_ok(r))
    formas = Counter((r.get("forma_ingresso") or "—") for r in rows)
    depts = Counter((r.get("department") or "—") for r in rows)
    return {
        "view": "home",
        "filters": filters,
        "metrics": {
            "avg_execution_seconds": avg,
            "avg_execution_hhmmss": _hhmmss(avg),
            "total_com_inicio_fim": len(timed_ok),
            "total": len(rows),
            "total_ok": ok_n,
            "total_erro": len(rows) - ok_n,
        },
        "tipo_inscricao": [
            {"tipo_inscricao": k, "total": v}
            for k, v in formas.most_common()
        ],
        "department": [
            {"department": k, "total": v}
            for k, v in depts.most_common()
        ],
        "recent": [_public_row(r) for r in rows[:50]],
    }


def _errors(rows: list[dict], filters: dict, limit: int, offset: int) -> dict:
    failed = [r for r in rows if not _is_ok(r)]
    page = failed[offset:offset + limit]
    return {
        "view": "errors",
        "filters": filters,
        "pagination": {"limit": limit, "offset": offset},
        "total_returned": len(page),
        "total_erros": len(failed),
        "rows": [_public_row(r) for r in page],
    }


@inscricao_bp.route("/api/inscricao", methods=["GET"])
def api_inscricao():
    view = (request.args.get("view") or "home").strip().lower()
    if view not in ("home", "errors", "search"):
        view = "home"
    q = (request.args.get("q") or "").strip()
    frm_q = (request.args.get("from") or "").strip()
    to_q = (request.args.get("to") or "").strip()
    if view == "search":
        frm, to = None, None
    else:
        if not frm_q and not to_q:
            frm_q, to_q = _iso_now_bounds_default()
        frm, to = _day_bounds(frm_q, to_q)
    try:
        limit = max(1, min(500, int(request.args.get("limit") or 200)))
    except ValueError:
        limit = 200
    try:
        offset = max(0, int(request.args.get("offset") or 0))
    except ValueError:
        offset = 0
    filters = {"from": frm, "to": to, "q": q or None}
    try:
        rows = _fetch_logs(frm, to)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")[:400]
        logger.exception("inscricoes_logs HTTP %s", e.code)
        return jsonify({"error": f"Supabase HTTP {e.code}: {body}"}), 502
    except Exception as e:
        logger.exception("inscricoes_logs")
        return jsonify({"error": str(e)}), 500
    if view == "search":
        if len(_digits(q)) < 8:
            return jsonify({"error": "Informe um CPF ou telefone com pelo menos 8 dígitos"}), 400
        found = [_public_row(r) for r in rows if _row_matches_q(r, q)]
        return jsonify({
            "view": "search",
            "filters": filters,
            "q": q,
            "total_returned": len(found),
            "rows": found,
        })
    if view == "errors":
        return jsonify(_errors(rows, filters, limit, offset))
    return jsonify(_home(rows, filters))
