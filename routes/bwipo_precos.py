"""Preços de curso no CRM Bwipo (Graduação).

Planilha de update → snapshot no Postgres → PUT /api/products.
Cota padrão: BALCÃO ESTRATÉGICO, gravada em Title Case (Balcão Estratégico).
Outros canais só entram se marcados na subida. Grade, imagem e descrição ficam.
"""
from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from decimal import Decimal, ROUND_HALF_UP

import openpyxl
from flask import Blueprint, jsonify, request, session

from db import get_conn
from services.bwipo_comercial import BwipoComercialError, request as bwipo_request

logger = logging.getLogger(__name__)
bwipo_precos_bp = Blueprint("bwipo_precos_bp", __name__)

PAGE = "precos_bwipo"
BASE_CANAL = "BALCÃO ESTRATÉGICO"
APROVADOS_CANAL = "APROVADOS"
MAR_ABERTO = "Mar Aberto"
_MONEY = Decimal("0.01")
_PCT = Decimal("0.01")


def _fold(s) -> str:
    n = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(c for c in n if not unicodedata.combining(c)).lower().strip()


def _money(v) -> Decimal:
    return Decimal(str(v)).quantize(_MONEY, rounding=ROUND_HALF_UP)


def _pct(v) -> Decimal:
    d = Decimal(str(v))
    if d <= 1:
        d = d * 100
    return d.quantize(_PCT, rounding=ROUND_HALF_UP)


def channel_label(raw: str) -> str:
    """BALCÃO ESTRATÉGICO → Balcão Estratégico. Trecho com dígito ou % fica como veio."""
    parts = []
    for word in str(raw or "").strip().split():
        bits = []
        for bit in word.split("|"):
            if any(c.isdigit() for c in bit) or "%" in bit:
                bits.append(bit)
            elif bit:
                bits.append(bit[:1].upper() + bit[1:].lower())
            else:
                bits.append(bit)
        parts.append("|".join(bits))
    return " ".join(parts)


def _is_base(canal: str) -> bool:
    return "estrateg" in _fold(canal)


def _is_aprovados(canal: str) -> bool:
    return _fold(canal) == "aprovados"


def _is_fixa(canal: str) -> bool:
    return _is_base(canal) or _is_aprovados(canal)


def crm_channel_name(canal: str, label: str) -> str:
    if _is_base(canal):
        return "Balcão Estratégico"
    if _is_aprovados(canal):
        return MAR_ABERTO
    return label


def _mode_from_sub(sub: str) -> str:
    return "HYBRID" if "semi" in _fold(sub) else "EAD"


def _sku(name: str, mode: str) -> str:
    base = re.sub(r"\s+", "", name or "")
    return base + ("EAD" if mode == "EAD" else "HIBRIDO")


def _allowed() -> bool:
    if not session.get("authenticated"):
        return False
    if (session.get("role") or "") == "admin" or session.get("user_id", 0) == 0:
        return True
    uid = session.get("user_id")
    if not uid:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM user_permissions WHERE user_id = %s AND page = %s",
                (uid, PAGE),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _gate():
    if _allowed():
        return None
    return jsonify({"ok": False, "error": "Sem permissão para preços do Bwipo."}), 403


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bwipo_preco_snapshot (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                created_by TEXT,
                filename TEXT,
                nivel TEXT NOT NULL DEFAULT 'GRADUATION',
                canais_aplicados TEXT[],
                aplicado_em TIMESTAMPTZ,
                resumo JSONB
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bwipo_preco_item (
                id SERIAL PRIMARY KEY,
                snapshot_id INTEGER NOT NULL
                    REFERENCES bwipo_preco_snapshot(id) ON DELETE CASCADE,
                curso TEXT NOT NULL,
                grau TEXT NOT NULL,
                submodalidade TEXT,
                duracao INTEGER,
                canal TEXT NOT NULL,
                canal_label TEXT NOT NULL,
                preco_base NUMERIC(12,2) NOT NULL,
                desconto_pct NUMERIC(8,2) NOT NULL,
                valor_final NUMERIC(12,2) NOT NULL,
                UNIQUE (snapshot_id, curso, grau, canal)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_bwipo_preco_item_curso
            ON bwipo_preco_item (curso, grau, canal)
        """)
    conn.commit()


def _header_index(header) -> dict:
    want = {
        "cursos": "curso",
        "grau": "grau",
        "canal": "canal",
        "preco siaa": "base",
        "desconto 1 semestre": "desconto",
        "valor com desconto": "final",
        "duracao": "duracao",
        "submodalidade": "sub",
    }
    out = {}
    for i, cell in enumerate(header or []):
        key = want.get(_fold(cell))
        if key:
            out[key] = i
    missing = [k for k in ("curso", "grau", "canal", "base", "desconto", "final") if k not in out]
    if missing:
        raise ValueError("Planilha sem coluna: " + ", ".join(missing))
    return out


def parse_workbook(file_obj) -> dict:
    wb = openpyxl.load_workbook(file_obj, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        raise ValueError("Planilha vazia")
    idx = _header_index(rows[0])
    cursos = {}
    conflitos = []
    for r in rows[1:]:
        curso = r[idx["curso"]]
        if not curso or not isinstance(curso, str):
            continue
        grau = str(r[idx["grau"]] or "").strip()
        canal = str(r[idx["canal"]] or "").strip()
        if not grau or not canal:
            continue
        try:
            base = _money(r[idx["base"]])
            desconto = _pct(r[idx["desconto"]])
            final = _money(r[idx["final"]])
        except Exception:
            continue
        sub = str(r[idx["sub"]] or "").strip() if "sub" in idx else ""
        dur = None
        if "duracao" in idx and r[idx["duracao"]] not in (None, ""):
            try:
                dur = int(float(r[idx["duracao"]]))
            except (TypeError, ValueError):
                dur = None
        key = (_fold(curso), _fold(grau))
        bucket = cursos.setdefault(key, {
            "curso": str(curso).strip(),
            "grau": grau,
            "sub": sub,
            "duracao": dur,
            "canais": {},
        })
        if sub and not bucket["sub"]:
            bucket["sub"] = sub
        if dur and not bucket["duracao"]:
            bucket["duracao"] = dur
        prev = bucket["canais"].get(canal)
        if prev and (prev["base"] != base or prev["desconto"] != desconto or prev["final"] != final):
            conflitos.append(f"{curso} / {grau} / {canal}")
            continue
        bucket["canais"][canal] = {
            "canal": canal,
            "label": crm_channel_name(canal, channel_label(canal)),
            "base": base,
            "desconto": desconto,
            "final": final,
        }
    lista = []
    for bucket in cursos.values():
        base_row = next((c for c in bucket["canais"].values() if _is_base(c["canal"])), None)
        if not base_row:
            continue
        lista.append({**bucket, "base": base_row})
    lista.sort(key=lambda x: (_fold(x["curso"]), _fold(x["grau"])))
    canais = {}
    for item in lista:
        for c in item["canais"].values():
            canais.setdefault(c["canal"], {"canal": c["canal"], "label": c["label"], "base": _is_base(c["canal"]), "cursos": 0})
            canais[c["canal"]]["cursos"] += 1
    ordem = sorted(canais.values(), key=lambda c: (0 if _is_base(c["canal"]) else 1 if _is_aprovados(c["canal"]) else 2, c["label"]))
    return {"cursos": lista, "canais": ordem, "conflitos": conflitos[:30]}


def _list_products() -> list[dict]:
    out = []
    page = 1
    while True:
        raw = bwipo_request("GET", f"/api/products?page={page}&perPage=100")
        items = raw.get("products") or []
        if not items:
            break
        out.extend(items)
        total = int(raw.get("total") or 0)
        if page * 100 >= total:
            break
        page += 1
    return out


def _grad_index(products: list[dict]) -> dict:
    idx = {}
    for p in products:
        cc = p.get("courseConfig") or {}
        if cc.get("level") != "GRADUATION":
            continue
        idx[(_fold(p.get("name")), _fold(cc.get("grau")))] = p
    return idx


def _similar_name(item: dict, products: list[dict]) -> str | None:
    fc = _fold(item["curso"])
    fg = _fold(item["grau"])
    for p in products:
        cc = p.get("courseConfig") or {}
        if cc.get("level") != "GRADUATION":
            continue
        if _fold(cc.get("grau")) != fg:
            continue
        fn = _fold(p.get("name"))
        if fn == fc:
            continue
        if fc in fn or fn in fc:
            return p.get("name") or ""
    return None


def _pick_option(options: list[dict], canal: str, label: str):
    target = _fold(label)
    raw = _fold(canal)
    if _is_base(canal):
        for o in options:
            if _fold(o.get("channel")) in ("balcao", "balcao estrategico"):
                return o
        return None
    if _is_aprovados(canal):
        for o in options:
            if _fold(o.get("channel")) in ("mar aberto", "aprovados"):
                return o
        return None
    for o in options:
        if _fold(o.get("channel")) == target:
            return o
    m = re.search(r"(\d+)\s*%", canal or "")
    if m and "comercial" in raw:
        num = m.group(1)
        for o in options:
            f = _fold(o.get("channel"))
            if "comercial" in f and num in f:
                return o
    if "corporativ" in raw:
        for o in options:
            if "corporativ" in _fold(o.get("channel")):
                return o
    return None


def _apply_prices(product: dict | None, item: dict, selected: set[str]) -> dict:
    base_row = item["base"]
    base_num = float(base_row["base"])
    selected_rows = [c for c in item["canais"].values() if c["canal"] in selected or _is_base(c["canal"])]
    if product:
        cc = product.get("courseConfig") or {}
        options = [dict(o) for o in (cc.get("pricingOptions") or []) if isinstance(o, dict)]
        for o in options:
            o["price"] = base_num
        for row in selected_rows:
            opt = _pick_option(options, row["canal"], row["label"])
            payload = {
                "price": base_num,
                "months": None,
                "channel": crm_channel_name(row["canal"], row["label"]),
                "installments": None,
                "discountPercent": float(row["desconto"]),
            }
            if opt is None:
                options.append(payload)
            else:
                opt.clear()
                opt.update(payload)
        body = {
            "name": product.get("name"),
            "description": product.get("description") or "",
            "sku": product.get("sku"),
            "price": f"{base_row['base']:.2f}",
            "unit": product.get("unit") or "matrícula",
            "type": product.get("type") or "PRODUCT",
            "kind": product.get("kind") or "COURSE",
            "isActive": product.get("isActive", True),
            "course": {
                "mode": cc.get("mode") or _mode_from_sub(item.get("sub")),
                "level": cc.get("level") or "GRADUATION",
                "grau": cc.get("grau") or item["grau"],
                "semester": cc.get("semester") if cc.get("semester") is not None else item.get("duracao"),
                "gradeUrl": cc.get("gradeUrl"),
                "postSalePipelineId": cc.get("postSalePipelineId"),
                "channel": base_row["label"],
                "discountPercent": float(base_row["desconto"]),
                "pricingOptions": options,
            },
        }
        bwipo_request("PUT", f"/api/products/{product['id']}", body)
        return {"acao": "atualizado", "id": product["id"]}

    mode = _mode_from_sub(item.get("sub"))
    options = [{
        "price": base_num,
        "months": None,
        "channel": crm_channel_name(row["canal"], row["label"]),
        "installments": None,
        "discountPercent": float(row["desconto"]),
    } for row in selected_rows]
    body = {
        "name": item["curso"],
        "description": "",
        "sku": _sku(item["curso"], mode),
        "price": f"{base_row['base']:.2f}",
        "unit": "matrícula",
        "type": "PRODUCT",
        "kind": "COURSE",
        "isActive": True,
    }
    created = bwipo_request("POST", "/api/products", body)
    prod = (created or {}).get("product") or created or {}
    new_id = prod.get("id")
    if not new_id:
        raise BwipoComercialError("O CRM criou o produto sem devolver o id.", 502)
    body["course"] = {
        "mode": mode,
        "level": "GRADUATION",
        "grau": item["grau"],
        "semester": item.get("duracao"),
        "gradeUrl": None,
        "channel": base_row["label"],
        "discountPercent": float(base_row["desconto"]),
        "pricingOptions": options,
    }
    try:
        bwipo_request("PUT", f"/api/products/{new_id}", body)
    except BwipoComercialError:
        try:
            bwipo_request("DELETE", f"/api/products/{new_id}")
        except BwipoComercialError:
            logger.warning("produto %s ficou criado sem preço", new_id)
        raise
    return {"acao": "criado", "id": new_id}


def _save_snapshot(conn, filename: str, parsed: dict, canais_aplicados: list[str] | None, resumo: dict) -> int:
    ensure_tables(conn)
    user = session.get("username") or session.get("display_name") or ""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bwipo_preco_snapshot
                (created_by, filename, canais_aplicados, aplicado_em, resumo)
            VALUES (%s, %s, %s, CASE WHEN %s THEN NOW() ELSE NULL END, %s::jsonb)
            RETURNING id
            """,
            (
                user,
                filename,
                canais_aplicados,
                bool(canais_aplicados),
                json.dumps(resumo, default=str),
            ),
        )
        snap_id = cur.fetchone()[0]
        for item in parsed["cursos"]:
            for c in item["canais"].values():
                cur.execute(
                    """
                    INSERT INTO bwipo_preco_item (
                        snapshot_id, curso, grau, submodalidade, duracao,
                        canal, canal_label, preco_base, desconto_pct, valor_final
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        snap_id, item["curso"], item["grau"], item.get("sub"), item.get("duracao"),
                        c["canal"], c["label"], c["base"], c["desconto"], c["final"],
                    ),
                )
    conn.commit()
    return snap_id


@bwipo_precos_bp.route("/api/bwipo-precos/preview", methods=["POST"])
def api_preview():
    denied = _gate()
    if denied:
        return denied
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Envie a planilha."}), 400
    try:
        parsed = parse_workbook(f)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("preview precos")
        return jsonify({"ok": False, "error": f"Não li a planilha: {e}"}), 400
    try:
        products = _list_products()
    except BwipoComercialError as e:
        return jsonify({"ok": False, "error": str(e)}), e.status
    index = _grad_index(products)
    linhas = []
    n_up = n_new = 0
    for item in parsed["cursos"]:
        prod = index.get((_fold(item["curso"]), _fold(item["grau"])))
        b = item["base"]
        if prod:
            n_up += 1
            acao = "atualizar"
            aviso = None
        else:
            n_new += 1
            acao = "criar"
            aviso = _similar_name(item, products)
        linhas.append({
            "chave": f"{_fold(item['curso'])}|{_fold(item['grau'])}",
            "curso": item["curso"],
            "grau": item["grau"],
            "sub": item.get("sub") or "",
            "duracao": item.get("duracao"),
            "base": float(b["base"]),
            "desconto": float(b["desconto"]),
            "final": float(b["final"]),
            "acao": acao,
            "aviso": aviso,
            "produto": (prod or {}).get("name"),
            "cotas": [{
                "canal": c["canal"],
                "label": crm_channel_name(c["canal"], c["label"]),
                "desconto": float(c["desconto"]),
                "final": float(c["final"]),
                "fixa": _is_fixa(c["canal"]),
            } for c in item["canais"].values()],
        })
    return jsonify({
        "ok": True,
        "cursos": len(linhas),
        "atualizar": n_up,
        "criar": n_new,
        "canais": parsed["canais"],
        "conflitos": parsed["conflitos"],
        "linhas": linhas,
    })


@bwipo_precos_bp.route("/api/bwipo-precos/aplicar", methods=["POST"])
def api_aplicar():
    denied = _gate()
    if denied:
        return denied
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Envie a planilha."}), 400
    somente_historico = str(request.form.get("somente_historico") or "") in ("1", "true", "sim")
    try:
        selected = set(json.loads(request.form.get("canais") or "[]"))
    except json.JSONDecodeError:
        selected = set()
    try:
        por_curso = json.loads(request.form.get("por_curso") or "{}")
    except json.JSONDecodeError:
        por_curso = {}
    if not isinstance(por_curso, dict):
        por_curso = {}
    selected.add(BASE_CANAL)
    selected.add(APROVADOS_CANAL)
    try:
        parsed = parse_workbook(f)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    file_canais = {c["canal"] for c in parsed["canais"]}
    selected = {c for c in selected if c in file_canais or _is_base(c)}
    if not any(_is_base(c) for c in file_canais):
        return jsonify({"ok": False, "error": "A planilha não tem BALCÃO ESTRATÉGICO."}), 400

    resultados = []
    if not somente_historico:
        try:
            products = _list_products()
        except BwipoComercialError as e:
            return jsonify({"ok": False, "error": str(e)}), e.status
        index = _grad_index(products)
        for item in parsed["cursos"]:
            chave = f"{_fold(item['curso'])}|{_fold(item['grau'])}"
            extras = por_curso.get(chave) or []
            if not isinstance(extras, list):
                extras = []
            do_curso = {c for c in (selected | set(extras)) if c in item["canais"]}
            prod = index.get((_fold(item["curso"]), _fold(item["grau"])))
            try:
                done = _apply_prices(prod, item, do_curso)
                resultados.append({"curso": item["curso"], "grau": item["grau"], "ok": True, **done})
            except BwipoComercialError as e:
                resultados.append({
                    "curso": item["curso"], "grau": item["grau"],
                    "ok": False, "acao": "erro", "erro": str(e)[:300],
                })
            time.sleep(0.2)

    resumo = {
        "cursos": len(parsed["cursos"]),
        "ok": sum(1 for r in resultados if r.get("ok")),
        "erro": sum(1 for r in resultados if not r.get("ok")),
        "somente_historico": somente_historico,
    }
    conn = get_conn()
    try:
        snap_id = _save_snapshot(
            conn,
            f.filename or "",
            parsed,
            None if somente_historico else sorted(selected),
            resumo,
        )
    finally:
        conn.close()
    return jsonify({"ok": True, "snapshot_id": snap_id, "resumo": resumo, "resultados": resultados})


@bwipo_precos_bp.route("/api/bwipo-precos/painel")
def api_painel():
    denied = _gate()
    if denied:
        return denied
    canal = (request.args.get("canal") or BASE_CANAL).strip()
    conn = get_conn()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("""
                WITH ranked AS (
                    SELECT i.curso, i.grau, i.canal_label, i.preco_base, i.desconto_pct, i.valor_final,
                           s.created_at,
                           row_number() OVER (
                               PARTITION BY i.curso, i.grau ORDER BY s.created_at DESC, s.id DESC
                           ) AS rn
                    FROM bwipo_preco_item i
                    JOIN bwipo_preco_snapshot s ON s.id = i.snapshot_id
                    WHERE i.canal = %s
                ),
                menor AS (
                    SELECT i.curso, i.grau, MIN(i.valor_final) AS menor
                    FROM bwipo_preco_item i
                    JOIN bwipo_preco_snapshot s ON s.id = i.snapshot_id
                    WHERE i.canal = %s
                      AND s.created_at >= NOW() - INTERVAL '6 months'
                    GROUP BY i.curso, i.grau
                )
                SELECT a.curso, a.grau, a.canal_label, a.preco_base, a.desconto_pct, a.valor_final,
                       a.created_at, b.valor_final, m.menor
                FROM ranked a
                LEFT JOIN ranked b ON b.curso = a.curso AND b.grau = a.grau AND b.rn = 2
                LEFT JOIN menor m ON m.curso = a.curso AND m.grau = a.grau
                WHERE a.rn = 1
                ORDER BY a.curso, a.grau
            """, (canal, canal))
            rows = cur.fetchall()
            cur.execute("""
                SELECT id, created_at, created_by, filename, canais_aplicados, aplicado_em
                FROM bwipo_preco_snapshot
                ORDER BY id DESC
                LIMIT 12
            """)
            snaps = cur.fetchall()
            cur.execute("SELECT DISTINCT canal, canal_label FROM bwipo_preco_item ORDER BY canal")
            canais = [{"canal": r[0], "label": r[1]} for r in cur.fetchall()]
    finally:
        conn.close()

    linhas = []
    subiu = caiu = igual = 0
    for curso, grau, label, base, desc, final, quando, anterior, menor in rows:
        final_f = float(final)
        ant = float(anterior) if anterior is not None else None
        delta = None
        if ant and ant != 0:
            delta = round((final_f - ant) / ant * 100, 1)
            if delta < -0.05:
                caiu += 1
            elif delta > 0.05:
                subiu += 1
            else:
                igual += 1
        elif ant is not None:
            igual += 1
        linhas.append({
            "curso": curso,
            "grau": grau,
            "label": label,
            "base": float(base),
            "desconto": float(desc),
            "final": final_f,
            "anterior": ant,
            "delta": delta,
            "menor_6m": float(menor) if menor is not None else final_f,
            "quando": quando.isoformat() if quando else None,
        })
    return jsonify({
        "ok": True,
        "canal": canal,
        "subiu": subiu,
        "caiu": caiu,
        "igual": igual,
        "linhas": linhas,
        "canais": canais,
        "snapshots": [{
            "id": s[0],
            "created_at": s[1].isoformat() if s[1] else None,
            "created_by": s[2],
            "filename": s[3],
            "canais": s[4] or [],
            "aplicado_em": s[5].isoformat() if s[5] else None,
        } for s in snaps],
    })
