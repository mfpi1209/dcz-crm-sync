"""Preços de curso no CRM Bwipo (Graduação).

Planilha de update → snapshot no Postgres → PUT /api/products.
As cotas que sobem são as escolhidas na tela (globais + por curso); nenhuma é
obrigatória (17/11). O preço base é o PREÇO SIAA, igual em todos os canais do
curso; o canal principal do produto é o Balcão Estratégico quando escolhido,
senão a primeira cota escolhida. Grade, imagem e descrição ficam.
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


# Variantes do mesmo curso que não podem virar produto próprio no CRM (18/11):
# "Cst Em X", "X (4.0)", "X (4.0I)", "X (4.0I Mais)", "X (Ead)". Todas são "X".
_RE_CST = re.compile(r"^\s*cst\s+em\s+", re.IGNORECASE)
_RE_SUFIXO = re.compile(r"\s*\(\s*(?:4\.0[^)]*|ead)\s*\)\s*$", re.IGNORECASE)


def nome_base(nome) -> str:
    n = _RE_CST.sub("", str(nome or ""))
    n = _RE_SUFIXO.sub("", n)
    return " ".join(n.split())


def eh_variante(nome) -> bool:
    return nome_base(nome) != " ".join(str(nome or "").split())


def _rank_variante(nome) -> int:
    """0 = nome limpo; 1 = só o prefixo Cst Em; 2 = com sufixo (4.0…)/(Ead)."""
    if not eh_variante(nome):
        return 0
    return 2 if _RE_SUFIXO.search(str(nome or "")) else 1


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
    # Variantes (Cst Em / 4.0) viram o curso base. Se a planilha também traz o
    # nome limpo, ele manda; senão fica a variante sem sufixo. As outras são
    # ignoradas e voltam em "ignorados".
    escolhido: dict = {}
    ignorados = []
    for bucket in cursos.values():
        if not bucket["canais"]:
            continue
        key = (_fold(nome_base(bucket["curso"])), _fold(bucket["grau"]))
        atual = escolhido.get(key)
        if atual is None or _rank_variante(bucket["curso"]) < _rank_variante(atual["curso"]):
            if atual is not None:
                ignorados.append(atual)
            escolhido[key] = bucket
        else:
            ignorados.append(bucket)
    lista = []
    for bucket in escolhido.values():
        lista.append({
            **bucket,
            "curso": nome_base(bucket["curso"]),
            "nome_planilha": bucket["curso"],
            "variante": eh_variante(bucket["curso"]),
            "base": _linha_principal(bucket["canais"].values()),
        })
    ignorados_out = [{
        "curso": b["curso"],
        "grau": b["grau"],
        "sub": b["sub"],
        "fica": nome_base(b["curso"]),
    } for b in ignorados]
    lista.sort(key=lambda x: (_fold(x["curso"]), _fold(x["grau"])))
    canais = {}
    for item in lista:
        for c in item["canais"].values():
            canais.setdefault(c["canal"], {"canal": c["canal"], "label": c["label"], "base": _is_base(c["canal"]), "cursos": 0})
            canais[c["canal"]]["cursos"] += 1
    ordem = sorted(canais.values(), key=lambda c: (0 if _is_base(c["canal"]) else 1 if _is_aprovados(c["canal"]) else 2, c["label"]))
    return {"cursos": lista, "canais": ordem, "conflitos": conflitos[:30], "ignorados": ignorados_out}


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
    """(nome, grau) → produto, só produtos de nome limpo. Uma variante do CRM
    ("Cst Em X", "X (4.0)") nunca é alvo: sem o "X", a linha cria ou é pulada."""
    idx = {}
    for p in products:
        cc = p.get("courseConfig") or {}
        if cc.get("level") != "GRADUATION" or eh_variante(p.get("name")):
            continue
        idx[(_fold(nome_base(p.get("name"))), _fold(cc.get("grau")))] = p
    return idx


def _criado_pelo_painel(p: dict) -> bool:
    """SKU no formato de _sku(): nome sem espaços + EAD/HIBRIDO."""
    sku = str(p.get("sku") or "")
    return sku in (_sku(p.get("name"), "EAD"), _sku(p.get("name"), "HYBRID"))


def _acao_item(item: dict, product: dict | None) -> str:
    """atualizar | criar | pular. Variante (Cst Em / 4.0) nunca cria produto."""
    if product:
        return "atualizar"
    return "pular" if item.get("variante") else "criar"


def _duplicatas(products: list[dict]) -> dict:
    """Produtos de graduação com nome de variante.
    "apagar": criado pelo painel (SKU de _sku()) e há o de nome limpo no mesmo grau.
    "outros": há o original, mas o SKU não é do painel; pode ser legítimo, não apaga.
    "sem_base": não há o original; análise manual."""
    grad = [p for p in products if (p.get("courseConfig") or {}).get("level") == "GRADUATION"]
    originais = {}
    for p in grad:
        if not eh_variante(p.get("name")):
            key = (_fold(nome_base(p.get("name"))), _fold((p.get("courseConfig") or {}).get("grau")))
            originais.setdefault(key, p)
    apagar, outros, sem_base = [], [], []
    for p in grad:
        if not eh_variante(p.get("name")):
            continue
        grau = (p.get("courseConfig") or {}).get("grau") or ""
        orig = originais.get((_fold(nome_base(p.get("name"))), _fold(grau)))
        linha = {"id": p.get("id"), "nome": p.get("name") or "", "sku": p.get("sku") or "",
                 "preco": p.get("price"), "grau": grau,
                 "mantem_id": (orig or {}).get("id"), "mantem_nome": (orig or {}).get("name")}
        if not orig:
            sem_base.append(linha)
        elif _criado_pelo_painel(p):
            apagar.append(linha)
        else:
            outros.append(linha)
    for lista in (apagar, outros, sem_base):
        lista.sort(key=lambda x: _fold(x["nome"]))
    return {"apagar": apagar, "outros": outros, "sem_base": sem_base}


def _apagar_duplicatas(ids: list, products: list[dict]) -> list[dict]:
    """Apaga só os ids que, na lista atual do CRM, ainda são duplicata com original."""
    validos = {str(x["id"]): x for x in _duplicatas(products)["apagar"]}
    out = []
    for raw in ids:
        pid = str(raw)
        dup = validos.get(pid)
        if not dup:
            out.append({"id": pid, "nome": "", "ok": False, "erro": "Não é duplicata com original no CRM; não apaguei."})
            continue
        try:
            bwipo_request("DELETE", f"/api/products/{pid}")
            out.append({"id": pid, "nome": dup["nome"], "mantem_nome": dup["mantem_nome"], "ok": True})
        except BwipoComercialError as e:
            out.append({"id": pid, "nome": dup["nome"], "ok": False, "erro": str(e)[:300]})
        time.sleep(0.2)
    return out


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


def _linha_principal(rows) -> dict:
    """Balcão Estratégico quando presente; senão a primeira linha. O PREÇO SIAA
    é o mesmo em todos os canais do curso, então qualquer uma serve de base."""
    rows = list(rows)
    return next((c for c in rows if _is_base(c["canal"])), rows[0])


def _rows_para_subir(item: dict, selected: set[str]) -> list[dict]:
    """Cotas escolhidas, na ordem da planilha, Estratégico primeiro se escolhido."""
    rows = [c for c in item["canais"].values() if c["canal"] in selected]
    rows.sort(key=lambda c: 0 if _is_base(c["canal"]) else 1)
    return rows


def _apply_prices(product: dict | None, item: dict, selected: set[str]) -> dict:
    selected_rows = _rows_para_subir(item, selected)
    if not selected_rows:
        raise BwipoComercialError("Nenhuma cota escolhida para este curso.", 400)
    base_row = selected_rows[0]
    base_num = float(base_row["base"])
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


# ── Pós-Graduação (18/11) ─────────────────────────────────────────────────
# Planilha de Pós: uma linha por curso × duração × canal, com SKU_CRM (o SKU do
# produto de Pós no CRM) e STATUS_CRM. O produto casa pelo SKU; a subida só
# atualiza preço e cota, nunca cria produto. STATUS_CRM "Inativo" fica de fora.
_POS_COURSE_FIXOS = ("mode", "level", "grau", "semester", "gradeUrl", "postSalePipelineId")
_POS_COURSE_FORA = {"id", "productId", "product", "createdAt", "updatedAt", "channel",
                    "discountPercent", "pricingOptions"}


def _sku_key(s) -> str:
    """SKU comparável: sem acento, minúsculo, sem espaço."""
    return re.sub(r"\s+", "", _fold(s))


def _primeiro_int(v) -> int | None:
    m = re.search(r"\d+", str(v or ""))
    return int(m.group()) if m else None


def _header_index_pos(header) -> dict:
    want = {
        "cursos": "curso", "curso": "curso",
        "canal": "canal",
        "preco siaa": "base",
        "desconto": "desconto",
        "valor com desconto": "final",
        "duracao": "duracao",
        "parcelas": "parcelas",
        "sku_crm": "sku",
        "status_crm": "status",
    }
    out = {}
    for i, cell in enumerate(header or []):
        key = want.get(_fold(cell))
        if key and key not in out:
            out[key] = i
    missing = [k for k in ("curso", "canal", "base", "desconto", "final", "duracao", "sku") if k not in out]
    if missing:
        nomes = {"curso": "CURSOS", "canal": "CANAL", "base": "PREÇO SIAA", "desconto": "DESCONTO",
                 "final": "VALOR COM DESCONTO", "duracao": "DURAÇÃO", "sku": "SKU_CRM"}
        raise ValueError("Planilha de Pós sem coluna: " + ", ".join(nomes[k] for k in missing))
    return out


def parse_pos_workbook(file_obj) -> dict:
    wb = openpyxl.load_workbook(file_obj, data_only=True)
    rows = list(wb.active.iter_rows(values_only=True))
    if not rows:
        raise ValueError("Planilha vazia")
    idx = _header_index_pos(rows[0])
    itens: dict = {}
    inativos: dict = {}
    sem_sku, conflitos = [], []
    for r in rows[1:]:
        curso = r[idx["curso"]] if idx["curso"] < len(r) else None
        if not curso or not isinstance(curso, str):
            continue
        canal = str(r[idx["canal"]] or "").strip()
        duracao = str(r[idx["duracao"]] or "").strip()
        if not canal:
            continue
        try:
            base = _money(r[idx["base"]])
            desconto = _pct(r[idx["desconto"]] or 0)
            final = _money(r[idx["final"]])
        except Exception:
            continue
        sku = str(r[idx["sku"]] or "").strip()
        if not sku:
            rotulo = f"{curso.strip()} · {duracao}"
            if rotulo not in sem_sku:
                sem_sku.append(rotulo)
            continue
        status = str(r[idx["status"]] or "").strip() if "status" in idx else ""
        destino = inativos if _fold(status) == "inativo" else itens
        key = _sku_key(sku)
        bucket = destino.setdefault(key, {
            "sku": sku,
            "curso": curso.strip(),
            "duracao": duracao,
            "meses": _primeiro_int(duracao),
            "parcelas": _primeiro_int(r[idx["parcelas"]]) if "parcelas" in idx else None,
            "grau": "",
            "sub": "EAD",
            "canais": {},
        })
        prev = bucket["canais"].get(canal)
        if prev and (prev["base"] != base or prev["desconto"] != desconto or prev["final"] != final):
            conflitos.append(f"{sku} / {canal}")
            continue
        bucket["canais"][canal] = {
            "canal": canal,
            "label": crm_channel_name(canal, channel_label(canal)),
            "base": base,
            "desconto": desconto,
            "final": final,
        }
    lista = sorted(itens.values(), key=lambda x: (_fold(x["curso"]), x["meses"] or 0))
    for item in lista:
        item["base"] = _linha_principal(item["canais"].values())
    canais = {}
    for item in lista:
        for c in item["canais"].values():
            canais.setdefault(c["canal"], {"canal": c["canal"], "label": c["label"], "base": _is_base(c["canal"]), "cursos": 0})
            canais[c["canal"]]["cursos"] += 1
    return {
        "cursos": lista,
        "canais": sorted(canais.values(), key=lambda c: c["label"]),
        "inativos": [{"sku": i["sku"], "curso": i["curso"], "duracao": i["duracao"]} for i in inativos.values()],
        "sem_sku": sem_sku,
        "conflitos": conflitos[:30],
    }


def _pos_index(products: list[dict]) -> dict:
    """SKU → produto, só produtos que não são de graduação."""
    idx = {}
    for p in products:
        cc = p.get("courseConfig") or {}
        if cc.get("level") == "GRADUATION" or not p.get("sku"):
            continue
        idx[_sku_key(p.get("sku"))] = p
    return idx


def _apply_prices_pos(product: dict, item: dict, selected: set[str]) -> dict:
    """Atualiza preço e cotas do produto de Pós. O resto do courseConfig fica como
    está no CRM (o que não é preço volta igual no PUT)."""
    selected_rows = _rows_para_subir(item, selected)
    if not selected_rows:
        raise BwipoComercialError("Nenhuma cota escolhida para este curso.", 400)
    base_row = selected_rows[0]
    base_num = float(base_row["base"])
    cc = product.get("courseConfig") or {}
    options = [dict(o) for o in (cc.get("pricingOptions") or []) if isinstance(o, dict)]
    for o in options:
        o["price"] = base_num
    for row in selected_rows:
        opt = _pick_option(options, row["canal"], row["label"])
        payload = {
            "price": base_num,
            "months": item.get("meses"),
            "channel": crm_channel_name(row["canal"], row["label"]),
            "installments": item.get("parcelas"),
            "discountPercent": float(row["desconto"]),
        }
        if opt is None:
            options.append(payload)
        else:
            opt.clear()
            opt.update(payload)
    course = {k: cc.get(k) for k in _POS_COURSE_FIXOS}
    course.update({k: v for k, v in cc.items() if k not in _POS_COURSE_FORA and k not in course})
    course.update({
        "channel": base_row["label"],
        "discountPercent": float(base_row["desconto"]),
        "pricingOptions": options,
    })
    body = {
        "name": product.get("name"),
        "description": product.get("description") or "",
        "sku": product.get("sku"),
        "price": f"{base_row['base']:.2f}",
        "unit": product.get("unit") or "matrícula",
        "type": product.get("type") or "PRODUCT",
        "kind": product.get("kind") or "COURSE",
        "isActive": product.get("isActive", True),
        "course": course,
    }
    bwipo_request("PUT", f"/api/products/{product['id']}", body)
    return {"acao": "atualizado", "id": product["id"]}


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
    n_up = n_new = n_pular = 0
    for item in parsed["cursos"]:
        prod = index.get((_fold(item["curso"]), _fold(item["grau"])))
        b = item["base"]
        acao = _acao_item(item, prod)
        aviso = None
        if acao == "atualizar":
            n_up += 1
        elif acao == "criar":
            n_new += 1
            aviso = _similar_name(item, products)
        else:
            n_pular += 1
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
            "nome_planilha": item.get("nome_planilha") or item["curso"],
            "produto": (prod or {}).get("name"),
            "cotas": [{
                "canal": c["canal"],
                "label": crm_channel_name(c["canal"], c["label"]),
                "desconto": float(c["desconto"]),
                "final": float(c["final"]),
            } for c in item["canais"].values()],
        })
    return jsonify({
        "ok": True,
        "cursos": len(linhas),
        "atualizar": n_up,
        "criar": n_new,
        "pular": n_pular,
        "canais": parsed["canais"],
        "conflitos": parsed["conflitos"],
        "ignorados": parsed["ignorados"],
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
    try:
        parsed = parse_workbook(f)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    file_canais = {c["canal"] for c in parsed["canais"]}
    selected = {c for c in selected if c in file_canais}
    tem_por_curso = any(isinstance(v, list) and v for v in por_curso.values())
    if not somente_historico and not selected and not tem_por_curso:
        return jsonify({"ok": False, "error": "Escolha ao menos uma cota para subir."}), 400

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
            if _acao_item(item, prod) == "pular":
                resultados.append({
                    "curso": item["curso"], "grau": item["grau"], "ok": False, "acao": "pulado",
                    "erro": f"Pulado: não há \"{item['curso']}\" no CRM e variante não cria produto.",
                })
                continue
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
        "erro": sum(1 for r in resultados if r.get("acao") == "erro"),
        "pulado": sum(1 for r in resultados if r.get("acao") == "pulado"),
        "ignorados": len(parsed["ignorados"]),
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


@bwipo_precos_bp.route("/api/bwipo-precos/duplicatas")
def api_duplicatas():
    """Só lista: variantes Cst Em / (4.0) que têm o produto de nome limpo no CRM."""
    denied = _gate()
    if denied:
        return denied
    try:
        products = _list_products()
    except BwipoComercialError as e:
        return jsonify({"ok": False, "error": str(e)}), e.status
    return jsonify({"ok": True, **_duplicatas(products)})


@bwipo_precos_bp.route("/api/bwipo-precos/duplicatas/apagar", methods=["POST"])
def api_duplicatas_apagar():
    """Apaga no CRM os ids pedidos, mas só os que, relidos agora, ainda são
    duplicata com original. O original nunca entra."""
    denied = _gate()
    if denied:
        return denied
    ids = (request.get_json(silent=True) or {}).get("ids")
    if not isinstance(ids, list) or not ids:
        return jsonify({"ok": False, "error": "Nenhum produto escolhido."}), 400
    try:
        products = _list_products()
    except BwipoComercialError as e:
        return jsonify({"ok": False, "error": str(e)}), e.status
    resultados = _apagar_duplicatas(ids, products)
    user = session.get("username") or session.get("display_name") or ""
    logger.warning("duplicatas apagadas por %s: %s", user,
                   [(r["id"], r.get("nome")) for r in resultados if r["ok"]])
    return jsonify({
        "ok": True,
        "apagados": sum(1 for r in resultados if r["ok"]),
        "erros": sum(1 for r in resultados if not r["ok"]),
        "resultados": resultados,
    })


@bwipo_precos_bp.route("/api/bwipo-precos/pos/preview", methods=["POST"])
def api_pos_preview():
    denied = _gate()
    if denied:
        return denied
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Envie a planilha de Pós."}), 400
    try:
        parsed = parse_pos_workbook(f)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("preview precos pos")
        return jsonify({"ok": False, "error": f"Não li a planilha de Pós: {e}"}), 400
    try:
        products = _list_products()
    except BwipoComercialError as e:
        return jsonify({"ok": False, "error": str(e)}), e.status
    index = _pos_index(products)
    linhas, amostra = [], None
    for item in parsed["cursos"]:
        prod = index.get(_sku_key(item["sku"]))
        b = item["base"]
        if prod and amostra is None:
            cc = prod.get("courseConfig") or {}
            amostra = {"produto": prod.get("name"), "sku": prod.get("sku"), "level": cc.get("level"),
                       "campos": sorted(cc.keys()),
                       "cotas": [o.get("channel") for o in cc.get("pricingOptions") or [] if isinstance(o, dict)]}
        linhas.append({
            "sku": item["sku"], "curso": item["curso"], "duracao": item["duracao"], "parcelas": item["parcelas"],
            "base": float(b["base"]), "desconto": float(b["desconto"]), "final": float(b["final"]),
            "acao": "atualizar" if prod else "sem_produto",
            "produto": (prod or {}).get("name"),
        })
    n_up = sum(1 for l in linhas if l["acao"] == "atualizar")
    return jsonify({
        "ok": True,
        "cursos": len(linhas),
        "atualizar": n_up,
        "sem_produto": len(linhas) - n_up,
        "inativos": len(parsed["inativos"]),
        "sem_sku": parsed["sem_sku"],
        "conflitos": parsed["conflitos"],
        "canais": parsed["canais"],
        "amostra": amostra,
        "linhas": linhas,
    })


@bwipo_precos_bp.route("/api/bwipo-precos/pos/aplicar", methods=["POST"])
def api_pos_aplicar():
    denied = _gate()
    if denied:
        return denied
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Envie a planilha de Pós."}), 400
    try:
        selected = set(json.loads(request.form.get("canais") or "[]"))
    except json.JSONDecodeError:
        selected = set()
    try:
        parsed = parse_pos_workbook(f)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    selected &= {c["canal"] for c in parsed["canais"]}
    if not selected:
        return jsonify({"ok": False, "error": "Escolha ao menos uma cota para subir."}), 400
    so_sku = _sku_key(request.form.get("so_sku") or "")
    if so_sku:  # "Testar em 1 produto": só esse SKU
        parsed["cursos"] = [i for i in parsed["cursos"] if _sku_key(i["sku"]) == so_sku]
        if not parsed["cursos"]:
            return jsonify({"ok": False, "error": "Esse SKU não está na planilha."}), 400
    try:
        products = _list_products()
    except BwipoComercialError as e:
        return jsonify({"ok": False, "error": str(e)}), e.status
    index = _pos_index(products)
    resultados = []
    for item in parsed["cursos"]:
        info = {"sku": item["sku"], "curso": item["curso"], "duracao": item["duracao"]}
        prod = index.get(_sku_key(item["sku"]))
        if not prod:
            resultados.append({**info, "ok": False, "acao": "sem_produto",
                               "erro": "Sem produto com esse SKU no CRM; Pós não cria produto."})
            continue
        do_curso = {c for c in selected if c in item["canais"]}
        try:
            done = _apply_prices_pos(prod, item, do_curso)
            resultados.append({**info, "ok": True, **done})
        except BwipoComercialError as e:
            resultados.append({**info, "ok": False, "acao": "erro", "erro": str(e)[:300]})
        time.sleep(0.2)
    resumo = {
        "cursos": len(parsed["cursos"]),
        "ok": sum(1 for r in resultados if r.get("ok")),
        "erro": sum(1 for r in resultados if r.get("acao") == "erro"),
        "sem_produto": sum(1 for r in resultados if r.get("acao") == "sem_produto"),
        "inativos": len(parsed["inativos"]),
    }
    logger.info("precos pos aplicados por %s: %s", session.get("username") or "", resumo)
    return jsonify({"ok": True, "resumo": resumo, "resultados": resultados})
