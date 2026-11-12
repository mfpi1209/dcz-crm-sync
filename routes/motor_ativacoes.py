"""Página "Motor de Ativações": estado do motor, plano do dia, lotes, diário e parada.

Só lê as tabelas motor_ e grava as chaves liga/desliga/parada em
`motor_controle`. Quem executa é o worker (`python -m motor.worker`), em
serviço separado. Esta rota não fala com o CRM e não cria tabela.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import psycopg2.extras
from flask import Blueprint, jsonify, request, session

from db import get_conn
from motor import VERSAO
from motor.agenda import proximos_dias_de_envio
from motor.calendario import agora_brt, para_brt
from motor.config import carregar
from motor.modelos import ESTADO_ROTULO
from motor.razao_pg import tabelas_instaladas

logger = logging.getLogger(__name__)
motor_ativacoes_bp = Blueprint("motor_ativacoes_bp", __name__)

PAGE = "motor_ativacoes"
ACOES = {"ligar", "desligar", "parar", "retomar"}


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
            cur.execute("SELECT 1 FROM user_permissions WHERE user_id = %s AND page = %s", (uid, PAGE))
            return cur.fetchone() is not None
    finally:
        conn.close()


def _gate():
    if _allowed():
        return None
    return jsonify({"ok": False, "error": "Sem permissão para o Motor de Ativações."}), 403


def _iso(dt):
    return para_brt(dt).isoformat(timespec="seconds") if dt else None


def _linhas(cur) -> list[dict]:
    out = []
    for r in cur.fetchall():
        d = dict(r)
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                d[k] = _iso(v) if hasattr(v, "hour") else v.isoformat()
        out.append(d)
    return out


def _agenda(cfg) -> list[dict]:
    return [
        {"dia": d.isoformat(), "mensagens": [a.mensagem.value for a in acoes]}
        for d, acoes in proximos_dias_de_envio(agora_brt().date(), cfg.feriados)
    ]


@motor_ativacoes_bp.route("/api/motor/estado")
def api_motor_estado():
    denied = _gate()
    if denied:
        return denied
    cfg = carregar()
    agora = agora_brt()
    base = {"ok": True, "versao": VERSAO, "agora": _iso(agora), "agenda": _agenda(cfg), "estados": {e.value: r for e, r in ESTADO_ROTULO.items()}}
    conn = get_conn()
    try:
        if not tabelas_instaladas(conn):
            return jsonify({**base, "instalado": False})
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT ligado, parada_geral, parada_motivo, atualizado_por, atualizado_em FROM motor_controle WHERE id = 1")
            controle = (_linhas(cur) or [{}])[0]
            cur.execute("SELECT host, versao, iniciado_em, ultimo_tick_em, modo, enabled, dry_run, ultimo_erro, config FROM motor_worker WHERE id = 1")
            worker = (_linhas(cur) or [None])[0]
            cur.execute("SELECT * FROM motor_cargas WHERE completa ORDER BY id DESC LIMIT 1")
            carga = (_linhas(cur) or [None])[0]
            cur.execute("SELECT id, dia, ciclo, piloto, status, resumo, criado_em FROM motor_planos WHERE dia = %s AND simulado ORDER BY id DESC LIMIT 1", (agora.date(),))
            plano = (_linhas(cur) or [None])[0]
            cur.execute(
                """
                SELECT ordem, frente, mensagem, tamanho_previsto, tamanho, status, motivo, liberado_em
                FROM motor_lotes WHERE dia = %s AND simulado ORDER BY ordem
                """,
                (agora.date(),),
            )
            lotes = _linhas(cur)
            cur.execute(
                """
                SELECT lida_em, ok, aguardando, maior_espera_s, atendentes_online, decisao, motivo, erro
                FROM motor_fila_leituras ORDER BY id DESC LIMIT 40
                """
            )
            fila = _linhas(cur)
    finally:
        conn.close()

    vivo = False
    if worker and worker.get("ultimo_tick_em"):
        ultimo = datetime.fromisoformat(worker["ultimo_tick_em"])
        vivo = (agora - ultimo) < timedelta(seconds=3 * cfg.tick_s + 30)
    return jsonify({
        **base, "instalado": True, "controle": controle, "worker": worker, "worker_vivo": vivo,
        "carga": carga, "plano": plano, "lotes": lotes, "fila": fila,
    })


@motor_ativacoes_bp.route("/api/motor/diario")
def api_motor_diario():
    denied = _gate()
    if denied:
        return denied
    try:
        limite = max(1, min(300, int(request.args.get("limit", 100))))
    except ValueError:
        limite = 100
    conn = get_conn()
    try:
        if not tabelas_instaladas(conn):
            return jsonify({"ok": True, "instalado": False, "itens": []})
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, criado_em, tipo, autor, texto FROM motor_diario ORDER BY id DESC LIMIT %s", (limite,))
            return jsonify({"ok": True, "instalado": True, "itens": _linhas(cur)})
    finally:
        conn.close()


@motor_ativacoes_bp.route("/api/motor/controle", methods=["POST"])
def api_motor_controle():
    """Liga, desliga, aciona ou libera a parada geral. O worker lê no tick seguinte."""
    denied = _gate()
    if denied:
        return denied
    corpo = request.get_json(silent=True) or {}
    acao = str(corpo.get("acao") or "").strip().lower()
    motivo = str(corpo.get("motivo") or "").strip()[:300]
    if acao not in ACOES:
        return jsonify({"ok": False, "error": "Ação inválida."}), 400
    usuario = (session.get("username") or "admin").strip()[:120]
    mudanca = {
        "ligar": ("ligado = TRUE", "Motor ligado na página."),
        "desligar": ("ligado = FALSE", "Motor desligado na página."),
        "parar": ("parada_geral = TRUE, parada_motivo = %(motivo)s", "PARADA GERAL acionada."),
        "retomar": ("parada_geral = FALSE, parada_motivo = ''", "Parada geral liberada."),
    }[acao]
    conn = get_conn()
    try:
        if not tabelas_instaladas(conn):
            return jsonify({"ok": False, "error": "Tabelas do motor ainda não foram criadas."}), 409
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE motor_controle SET {mudanca[0]}, atualizado_por = %(usuario)s, atualizado_em = NOW() WHERE id = 1",
                {"motivo": motivo, "usuario": usuario},
            )
            cur.execute(
                "INSERT INTO motor_diario (tipo, autor, texto) VALUES ('controle', %s, %s)",
                (f"usuario:{usuario}", mudanca[1] + (f" Motivo: {motivo}" if motivo else "")),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("motor: falha ao gravar o controle")
        return jsonify({"ok": False, "error": "Não consegui gravar. Tente de novo."}), 500
    finally:
        conn.close()
    return jsonify({"ok": True, "acao": acao})
