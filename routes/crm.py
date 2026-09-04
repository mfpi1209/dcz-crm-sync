from datetime import datetime

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify

from db import get_conn
from helpers import (
    to_brt, SEARCH_QUERY, SYNC_STATE_QUERY, RECENT_BIZ_UPDATES_QUERY,
    _normalize_digits,
)

crm_bp = Blueprint("crm", __name__)

# Compat: dashboard lê flags de processo (sempre false após desativação DataCrazy)
_sync_running = False
_update_running = False


# ---------------------------------------------------------------------------
# Rotas — Debug
# ---------------------------------------------------------------------------

@crm_bp.route("/api/debug/address")
def api_debug_address():
    """Debug DataCrazy desativado (CRM acadêmico descontinuado)."""
    return jsonify({"error": "DataCrazy desativado"}), 410


# ---------------------------------------------------------------------------
# Rotas — Busca
# ---------------------------------------------------------------------------

@crm_bp.route("/api/search")
def api_search():
    cpf = request.args.get("cpf", "").strip()
    rgm = request.args.get("rgm", "").strip()
    telefone = request.args.get("telefone", "").strip()

    if not cpf and not rgm and not telefone:
        return jsonify({"results": [], "error": "Informe pelo menos um critério de busca."})

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SEARCH_QUERY, {"cpf": cpf, "rgm": rgm, "telefone": telefone})
            rows = cur.fetchall()
            results = []
            for r in rows:
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                results.append(row)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"results": [], "error": str(e)}), 500
    finally:
        conn.close()


@crm_bp.route("/api/search-xl")
def api_search_xl():
    cpf = _normalize_digits(request.args.get("cpf", ""))
    rgm = _normalize_digits(request.args.get("rgm", ""))
    telefone = _normalize_digits(request.args.get("telefone", ""))
    snapshot_id = request.args.get("snapshot_id", "")
    tipo = request.args.get("tipo", "").strip().lower()

    if not cpf and not rgm and not telefone:
        return jsonify({"results": [], "snapshot": None})

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if snapshot_id:
                cur.execute("SELECT id, tipo, filename, row_count, uploaded_at FROM xl_snapshots WHERE id = %s", (snapshot_id,))
            elif tipo:
                cur.execute("SELECT id, tipo, filename, row_count, uploaded_at FROM xl_snapshots WHERE tipo = %s ORDER BY id DESC LIMIT 1", (tipo,))
            else:
                cur.execute("SELECT id, tipo, filename, row_count, uploaded_at FROM xl_snapshots ORDER BY id DESC LIMIT 1")
            snap = cur.fetchone()
            if not snap:
                return jsonify({"results": [], "snapshot": None})

            snap_info = {
                "id": snap["id"],
                "tipo": snap["tipo"],
                "filename": snap["filename"],
                "row_count": snap["row_count"],
                "uploaded_at": to_brt(snap["uploaded_at"]),
            }
            sid = snap["id"]

            conditions = []
            params_list = [sid]

            if cpf:
                conditions.append("data->>'cpf_digits' LIKE '%%' || %s || '%%'")
                params_list.append(cpf)
            if rgm:
                conditions.append("data->>'rgm' LIKE '%%' || %s || '%%'")
                params_list.append(rgm)
            if telefone:
                conditions.append("""(
                    EXISTS (SELECT 1 FROM jsonb_array_elements_text(data->'phones_digits') ph WHERE ph LIKE '%%' || %s || '%%')
                )""")
                params_list.append(telefone)

            where = " OR ".join(conditions)
            cur.execute(
                f"SELECT data FROM xl_rows WHERE snapshot_id = %s AND ({where}) LIMIT 20",
                params_list,
            )
            rows = cur.fetchall()

        results = []
        for r in rows:
            d = r["data"]
            results.append({k: v for k, v in d.items()
                            if k not in ("cpf_digits", "rgm_digits", "phones_digits")})

        return jsonify({"results": results, "snapshot": snap_info})
    except Exception as e:
        return jsonify({"results": [], "snapshot": None, "error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Sync State (read-only espelho local)
# ---------------------------------------------------------------------------

@crm_bp.route("/api/sync-state")
def api_sync_state():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SYNC_STATE_QUERY)
            states = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                states.append(row)

            cur.execute(RECENT_BIZ_UPDATES_QUERY)
            recent = []
            for r in cur.fetchall():
                row = dict(r)
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = to_brt(v)
                recent.append(row)

        return jsonify({"states": states, "recent_updates": recent})
    except Exception as e:
        return jsonify({"states": [], "recent_updates": [], "error": str(e)}), 500
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Sync (DataCrazy desativado — stubs)
# ---------------------------------------------------------------------------

@crm_bp.route("/api/sync/<mode>", methods=["POST"])
def api_sync(mode):
    if mode not in ("delta", "full"):
        return jsonify({"error": "Modo inválido. Use 'delta' ou 'full'."}), 400
    return jsonify({"error": "DataCrazy desativado"}), 410


@crm_bp.route("/api/sync/logs")
def api_sync_logs():
    since = int(request.args.get("since", 0))
    return jsonify({"lines": [], "total": 0, "running": False})


@crm_bp.route("/api/sync/status")
def api_sync_status():
    return jsonify({"running": False})


@crm_bp.route("/api/sync/stop", methods=["POST"])
def api_sync_stop():
    return jsonify({"ok": True})
