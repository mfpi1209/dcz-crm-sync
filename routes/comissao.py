"""Comissão dos consultores a partir das matrículas do Dashboard Comercial Bwipo.

A quantidade é a mesma do ranking (em curso, no padrão de RGM) no De/Até da aba.
A faixa usa a meta que o ranking já mostra. O R$ por matrícula depende do grupo
(Alta Performance ou Impulso) e de graduação ou pós.
"""
from __future__ import annotations

import logging

from flask import Blueprint, jsonify, request, session

from db import get_conn
from helpers import fold_name

logger = logging.getLogger(__name__)

comissao_bp = Blueprint("comissao_bp", __name__)
PAGE = "comissao"

# R$ por matrícula na faixa atingida. Abaixo de Inter não paga.
TABELAS = {
    "alta": {
        "label": "Alta Performance",
        "inter": {"grad": 10, "pos": 10},
        "meta": {"grad": 15, "pos": 10},
        "super": {"grad": 20, "pos": 20},
    },
    "impulso": {
        "label": "Impulso",
        "inter": {"grad": 8, "pos": 8},
        "meta": {"grad": 12, "pos": 10},
        "super": {"grad": 16, "pos": 16},
    },
}
FAIXA_LABEL = {"super": "Super", "meta": "Meta", "inter": "Inter", "abaixo": "Abaixo"}


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


def _tabela_do_grupo(nome: str) -> str | None:
    folded = fold_name(nome or "")
    if "alta" in folded:
        return "alta"
    if "impulso" in folded:
        return "impulso"
    return None


def _faixa(qtd: int, inter: float, meta: float, super_: float) -> str:
    if super_ and qtd >= super_:
        return "super"
    if meta and qtd >= meta:
        return "meta"
    if inter and qtd >= inter:
        return "inter"
    return "abaixo"


def _grupos(campanha_id: int) -> list[dict]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, nome FROM premiacao_grupo WHERE campanha_id = %s ORDER BY nome",
            (campanha_id,),
        )
        grupos = []
        for gid, nome in cur.fetchall():
            cur.execute(
                "SELECT kommo_user_id FROM premiacao_grupo_membro WHERE grupo_id = %s",
                (gid,),
            )
            grupos.append({
                "id": int(gid),
                "nome": nome,
                "tabela": _tabela_do_grupo(nome),
                "membros": [int(r[0]) for r in cur.fetchall()],
            })
        return grupos
    finally:
        conn.close()


def _calcular(dt_ini: str, dt_fim: str, campanha_id: int) -> dict:
    from routes.bwipo_sync import _painel_payload

    payload = _painel_payload(dt_ini, dt_fim, "", None, "", None, None)
    if not payload.get("ok"):
        raise RuntimeError(payload.get("error") or "painel indisponível")

    ranking = {item.get("user_id"): item for item in payload.get("ranking") or [] if item.get("user_id")}
    grupos = _grupos(campanha_id)
    no_grupo: set[int] = set()
    linhas = []

    for grupo in grupos:
        for uid in grupo["membros"]:
            no_grupo.add(uid)
            item = ranking.get(uid) or {}
            nome = item.get("agente") or f"#{uid}"
            qtd = int(item.get("matriculas") or 0)
            pos = int(item.get("pos") or 0)
            grad = max(0, qtd - pos)
            inter = float(item.get("intermediaria") or 0)
            meta = float(item.get("meta") or 0)
            super_ = float(item.get("supermeta") or 0)
            faixa = _faixa(qtd, inter, meta, super_)
            tabela = grupo["tabela"]
            rates = (TABELAS.get(tabela) or {}).get(faixa) or {}
            valor_grad = rates.get("grad") or 0
            valor_pos = rates.get("pos") or 0
            comissao = (grad * valor_grad) + (pos * valor_pos) if faixa != "abaixo" and tabela else 0
            linhas.append({
                "user_id": uid,
                "agente": nome,
                "grupo": grupo["nome"],
                "tabela": tabela,
                "matriculas": qtd,
                "grad": grad,
                "pos": pos,
                "intermediaria": inter,
                "meta": meta,
                "supermeta": super_,
                "faixa": faixa,
                "faixa_label": FAIXA_LABEL[faixa],
                "valor_grad": valor_grad,
                "valor_pos": valor_pos,
                "comissao": comissao,
            })

    sem_grupo = []
    for item in payload.get("ranking") or []:
        uid = item.get("user_id")
        if not isinstance(uid, int) or uid in no_grupo:
            continue
        if not item.get("matriculas"):
            continue
        sem_grupo.append({
            "user_id": uid,
            "agente": item.get("agente") or "",
            "matriculas": int(item.get("matriculas") or 0),
            "intermediaria": float(item.get("intermediaria") or 0),
            "meta": float(item.get("meta") or 0),
            "supermeta": float(item.get("supermeta") or 0),
        })

    linhas.sort(key=lambda r: (-r["comissao"], -r["matriculas"], r["agente"]))
    sem_grupo.sort(key=lambda r: (-r["matriculas"], r["agente"]))
    kpis = payload.get("kpis") or {}
    return {
        "ok": True,
        "dt_ini": dt_ini,
        "dt_fim": dt_fim,
        "em_curso": int(kpis.get("em_curso") or 0),
        "linhas": linhas,
        "sem_grupo": sem_grupo,
        "total": sum(r["comissao"] for r in linhas),
        "tabelas": {
            chave: {
                "label": tabela["label"],
                "inter": tabela["inter"],
                "meta": tabela["meta"],
                "super": tabela["super"],
            }
            for chave, tabela in TABELAS.items()
        },
    }


@comissao_bp.route("/api/comissao")
def api_comissao():
    if not _allowed():
        return jsonify({"ok": False, "error": "Sem permissão para Comissão."}), 403
    dt_ini = (request.args.get("dt_ini") or "").strip()
    dt_fim = (request.args.get("dt_fim") or "").strip()
    try:
        campanha_id = int(request.args.get("campanha_id") or 0)
    except ValueError:
        campanha_id = 0
    if len(dt_ini) != 10 or len(dt_fim) != 10 or not campanha_id:
        return jsonify({"ok": False, "error": "Informe o período e a campanha dos grupos."}), 400
    try:
        return jsonify(_calcular(dt_ini, dt_fim, campanha_id))
    except Exception as e:
        logger.exception("comissao")
        return jsonify({"ok": False, "error": str(e)}), 500
