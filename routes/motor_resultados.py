"""Resultados, comportamento e textos na página "Motor de Ativações".

Só lê as tabelas motor_ do painel. Não fala com o CRM e não grava nada.
"""
from __future__ import annotations

import logging
import re

import psycopg2.extras
from flask import Blueprint, jsonify, request

from db import get_conn
from motor.calendario import agora_brt, ciclo_de, semestre_de
from motor.config import carregar
from motor.previsao import comportamento
from motor.razao_pg import tabelas_instaladas
from motor.resultados import resultado_do_ciclo
from motor.textos import catalogo
from routes.motor_ativacoes import _gate

logger = logging.getLogger(__name__)
motor_resultados_bp = Blueprint("motor_resultados_bp", __name__)


def _linhas_do_semestre(cur, ciclo: str) -> list[dict]:
    ano, sem = semestre_de(ciclo).split("/")
    primeiro = f"{ano}-{'01' if sem == '1' else '07'}"
    cur.execute(
        "SELECT contact_id, ciclo, baixa_dia10_em, controle FROM motor_aluno_ciclo WHERE ciclo >= %s AND ciclo <= %s",
        (primeiro, ciclo),
    )
    return [dict(r) for r in cur.fetchall()]


@motor_resultados_bp.route("/api/motor/resultados")
def api_motor_resultados():
    negado = _gate()
    if negado:
        return negado
    cfg = carregar()
    hoje = agora_brt().date()
    atual = ciclo_de(hoje)
    conn = get_conn()
    try:
        if not tabelas_instaladas(conn):
            return jsonify({"ok": True, "instalado": False, "textos": catalogo()})
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT DISTINCT ciclo FROM motor_aluno_ciclo ORDER BY ciclo DESC LIMIT 12")
            ciclos = [r["ciclo"] for r in cur.fetchall()]
            pedido = (request.args.get("ciclo") or "").strip()
            ciclo = pedido if re.fullmatch(r"\d{4}-\d{2}", pedido) and pedido in ciclos else (ciclos[0] if ciclos else atual)
            linhas = _linhas_do_semestre(cur, ciclo)
            cur.execute(
                "SELECT contact_id, mensagem, variante, status, classificacao, respondeu_em FROM motor_mensagens "
                "WHERE ciclo = %s AND simulado = FALSE",
                (ciclo,),
            )
            envios = [dict(r) for r in cur.fetchall()]
            linhas_atuais = linhas if semestre_de(ciclo) == semestre_de(atual) and ciclo == atual else _linhas_do_semestre(cur, atual)
    finally:
        conn.close()
    return jsonify({
        "ok": True, "instalado": True, "ciclos": ciclos, "ciclo_atual": atual,
        "resultado": resultado_do_ciclo(ciclo, linhas, envios, hoje=hoje, folga_du=cfg.folga_baixa_du, feriados=cfg.feriados),
        "comportamento": comportamento(linhas_atuais, hoje=hoje, folga_du=cfg.folga_baixa_du, feriados=cfg.feriados),
        "textos": catalogo(),
    })
