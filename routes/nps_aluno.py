"""Página "NPS do Aluno": resultado das pesquisas respondidas no CRM acadêmico.

Só lê as tabelas nps_. A pesquisa é enviada e respondida no CRM; a coleta
(`nps.servico.coletar`) copia o resultado para cá pela API pública, fora de
request. Esta rota não cria tabela e não escreve no CRM.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request, session

from db import get_conn
from nps import VERSAO, razao_pg
from nps.calculo import N_MIN_COMPARAR, N_MIN_PUBLICAR, por_recorte, resumir
from nps.coleta import BRT
from nps.crm import CrmLeitura
from nps.servico import coleta_habilitada, coletar

logger = logging.getLogger(__name__)
nps_aluno_bp = Blueprint("nps_aluno_bp", __name__)

PAGE = "nps_aluno"
PAGE_ATENDENTES = "nps_aluno_atendentes"
MAX_DIAS = 400


def _is_admin() -> bool:
    return (session.get("role") or "") == "admin" or session.get("user_id", 0) == 0


def _allowed(page: str = PAGE) -> bool:
    if not session.get("authenticated"):
        return False
    if _is_admin():
        return True
    uid = session.get("user_id")
    if not uid:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM user_permissions WHERE user_id = %s AND page = %s", (uid, page))
            return cur.fetchone() is not None
    finally:
        conn.close()


def _gate():
    if _allowed():
        return None
    return jsonify({"ok": False, "error": "Sem permissão para o NPS do Aluno."}), 403


def _periodo() -> tuple[datetime, datetime]:
    hoje = datetime.now(BRT).replace(hour=0, minute=0, second=0, microsecond=0)

    def dia(nome: str, padrao: datetime) -> datetime:
        try:
            return datetime.strptime(request.args.get(nome, ""), "%Y-%m-%d").replace(tzinfo=BRT)
        except ValueError:
            return padrao

    ate = dia("ate", hoje)
    de = dia("de", ate - timedelta(days=29))
    if de > ate:
        de = ate
    if ate - de > timedelta(days=MAX_DIAS):
        de = ate - timedelta(days=MAX_DIAS)
    return de, ate + timedelta(days=1)


def _iso(valor):
    return valor.astimezone(BRT).isoformat(timespec="seconds") if isinstance(valor, datetime) else valor


@nps_aluno_bp.route("/api/nps/resumo")
def api_nps_resumo():
    denied = _gate()
    if denied:
        return denied
    de, ate = _periodo()
    base = {
        "ok": True, "versao": VERSAO, "de": de.date().isoformat(), "ate": (ate - timedelta(days=1)).date().isoformat(),
        "regras": {"n_min_publicar": N_MIN_PUBLICAR, "n_min_comparar": N_MIN_COMPARAR},
        "coleta_habilitada": coleta_habilitada(), "admin": _is_admin(),
    }
    conn = get_conn()
    try:
        if not razao_pg.tabelas_instaladas(conn):
            return jsonify({**base, "instalado": False})
        linhas = razao_pg.linhas(conn, de, ate)
        coleta = razao_pg.ultima_coleta(conn)
    finally:
        conn.close()

    respostas = [l for l in linhas if l["nota"] is not None]
    geral = resumir(linhas)
    distribuicao = None
    if geral.respostas >= N_MIN_PUBLICAR:
        distribuicao = [sum(1 for l in respostas if l["nota"] == n) for n in range(11)]
    saida = {
        **base, "instalado": True,
        "coleta": {k: _iso(v) for k, v in coleta.items()} if coleta else None,
        "geral": geral.como_dict(),
        "por_depto": por_recorte(linhas, "depto", "(sem departamento)"),
        "por_semana": sorted(por_recorte(linhas, "semana"), key=lambda r: r["recorte"]),
        "distribuicao": distribuicao,
        "resolvido_nao_lido": sum(1 for l in respostas if l["resolvido"] is None and l["resolvido_bruto"]),
        "ve_atendentes": _allowed(PAGE_ATENDENTES),
    }
    if saida["ve_atendentes"]:
        com_dono = [l for l in linhas if l["nota"] is not None and l["atendente"]]
        saida["por_atendente"] = [
            {**r, "depto": next((l["depto"] for l in com_dono if l["atendente"] == r["recorte"]), "")}
            for r in por_recorte(com_dono, "atendente")
        ]
    return jsonify(saida)


@nps_aluno_bp.route("/api/nps/motivos")
def api_nps_motivos():
    """Comentários dos alunos (já sem e-mail e sem números longos), com nota e departamento. Sem id de aluno."""
    denied = _gate()
    if denied:
        return denied
    de, ate = _periodo()
    conn = get_conn()
    try:
        if not razao_pg.tabelas_instaladas(conn):
            return jsonify({"ok": True, "instalado": False, "motivos": []})
        linhas = razao_pg.linhas(conn, de, ate)
    finally:
        conn.close()
    so = (request.args.get("classe") or "").strip()
    faixa = {"detrator": range(0, 7), "neutro": range(7, 9), "promotor": range(9, 11)}.get(so, range(0, 11))
    motivos = [
        {"nota": l["nota"], "depto": l["depto"], "resolvido": l["resolvido"], "motivo": l["motivo"]}
        for l in linhas
        if l["nota"] is not None and l["nota"] in faixa and l["motivo"]
    ]
    motivos.sort(key=lambda m: m["nota"])
    return jsonify({"ok": True, "instalado": True, "motivos": motivos[:300], "total": len(motivos)})


_coleta_thread: threading.Thread | None = None


def _rodar_coleta(disparada_por: str) -> None:
    try:
        resultado = coletar(get_conn, CrmLeitura(), disparada_por)
        logger.info("nps: coleta (%s) → %s", disparada_por, resultado)
    except Exception:  # noqa: BLE001
        logger.exception("nps: coleta (%s) quebrou", disparada_por)


@nps_aluno_bp.route("/api/nps/coletar", methods=["POST"])
def api_nps_coletar():
    """Dispara uma coleta agora, em segundo plano. Só admin, e só com NPS_COLETA_ENABLED=1."""
    global _coleta_thread
    if not session.get("authenticated") or not _is_admin():
        return jsonify({"ok": False, "error": "Só administrador dispara a coleta."}), 403
    if not coleta_habilitada():
        return jsonify({"ok": False, "error": "Coleta desligada (NPS_COLETA_ENABLED=0)."}), 409
    if not CrmLeitura().configurado():
        return jsonify({"ok": False, "error": "EDUIT_CRM_TOKEN não configurado."}), 409
    if _coleta_thread and _coleta_thread.is_alive():
        return jsonify({"ok": False, "error": "Já existe uma coleta em andamento."}), 409
    quem = str(session.get("username") or "admin")
    _coleta_thread = threading.Thread(target=_rodar_coleta, args=(f"manual:{quem}",), daemon=True, name="nps-coleta")
    _coleta_thread.start()
    return jsonify({"ok": True})


def register_nps_job(scheduler) -> None:
    """Coleta diária às 03h20 (Brasília). Não registra nada com NPS_COLETA_ENABLED diferente de 1."""
    if not coleta_habilitada():
        logger.info("nps: coleta desligada (NPS_COLETA_ENABLED=0); job não registrado")
        return
    from apscheduler.triggers.cron import CronTrigger

    scheduler.add_job(
        _rodar_coleta,
        trigger=CronTrigger(hour=3, minute=20, timezone="America/Sao_Paulo"),
        args=("agendador",),
        id="nps_coleta_diaria",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=3600,
    )
    logger.info("nps: coleta diária registrada (03:20 BRT)")
