"""Página "Motor de Ativações": estado do motor, plano do dia, lotes, diário e parada.

Só lê as tabelas motor_ e grava as chaves liga/desliga/parada em
`motor_controle`. Quem executa é o worker (`python -m motor.worker`), em
serviço separado. Esta rota não fala com o CRM e não cria tabela.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2.extras
from flask import Blueprint, jsonify, request, session

from db import get_conn
from motor import VERSAO
from motor.agenda import proximos_dias_de_envio
from motor.calendario import agora_brt, para_brt
from motor.classificador import ROTULOS_DE_EXPERIENCIA
from motor.config import carregar
from motor.modelos import ESTADO_ROTULO
from motor.razao_pg import RazaoPg, tabelas_instaladas
from motor.respostas import assinatura_valida, interpretar

logger = logging.getLogger(__name__)
motor_ativacoes_bp = Blueprint("motor_ativacoes_bp", __name__)

PAGE = "motor_ativacoes"
WEBHOOK_RESPOSTAS = "/webhooks/motor/respostas"  # liberado sem sessão em routes/auth.py; vale a assinatura
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
        for d, acoes in proximos_dias_de_envio(agora_brt().date(), cfg.feriados, n_dias=cfg.dias_antes_vencimento)
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
            # Plano real, se existir; senão o simulado.
            cur.execute(
                """
                SELECT id, dia, simulado, ciclo, piloto, status, resumo, criado_em, aprovado_por, aprovado_em
                FROM motor_planos WHERE dia = %s ORDER BY simulado ASC, id DESC LIMIT 1
                """,
                (agora.date(),),
            )
            plano = (_linhas(cur) or [None])[0]
            simulado = plano["simulado"] if plano else True
            cur.execute(
                """
                SELECT l.ordem, l.frente, l.mensagem, l.variante, l.tamanho_previsto, l.tamanho, l.status, l.motivo, l.liberado_em,
                       l.etiquetados, l.enviados, l.entregues, l.lidos, l.respondidos, l.falhas, l.crm_status,
                       (SELECT COUNT(DISTINCT r.contact_id) FROM motor_respostas r
                         WHERE r.lote_id = l.id AND r.rotulo = ANY(%s)) AS parar_reclamar
                FROM motor_lotes l WHERE l.dia = %s AND l.simulado = %s ORDER BY l.ordem
                """,
                (sorted(ROTULOS_DE_EXPERIENCIA), agora.date(), simulado),
            )
            lotes = _linhas(cur)
            cur.execute(
                """
                SELECT COALESCE(rotulo, 'aguardando') AS rotulo, COUNT(*) AS n, COUNT(*) FILTER (WHERE revisar) AS revisar
                FROM motor_respostas WHERE dia = %s GROUP BY 1 ORDER BY 2 DESC
                """,
                (agora.date(),),
            )
            respostas = _linhas(cur)
            cur.execute(
                "SELECT COUNT(DISTINCT contact_id) AS casos FROM motor_respostas WHERE dia = %s AND rotulo = ANY(%s)",
                (agora.date(), sorted(ROTULOS_DE_EXPERIENCIA)),
            )
            casos = cur.fetchone()["casos"]
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
    liberados = [l for l in lotes if l["liberado_em"]]
    entregues = sum(l["entregues"] or 0 for l in liberados) or sum(l["tamanho"] or 0 for l in liberados)
    experiencia = {
        "casos": casos, "entregues": entregues,
        "por_mil": round(casos * 1000.0 / entregues, 1) if entregues else 0.0,
        "segura_por_mil": cfg.exp_segura_por_mil, "encerra_por_mil": cfg.exp_encerra_por_mil, "min_casos": cfg.exp_min_casos,
    }
    return jsonify({
        **base, "instalado": True, "controle": controle, "worker": worker, "worker_vivo": vivo,
        "carga": carga, "plano": plano, "lotes": lotes, "fila": fila, "respostas": respostas, "experiencia": experiencia,
    })


@motor_ativacoes_bp.route("/api/motor/plano/aprovar", methods=["POST"])
def api_motor_plano_aprovar():
    """Aprova o plano de hoje (o real, se existir; senão o simulado). Em modo real nenhum lote sai sem isso."""
    denied = _gate()
    if denied:
        return denied
    usuario = (session.get("username") or "admin").strip()[:120]
    hoje = agora_brt().date()
    conn = get_conn()
    try:
        if not tabelas_instaladas(conn):
            return jsonify({"ok": False, "error": "Tabelas do motor ainda não foram criadas."}), 409
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE motor_planos SET status = 'aprovado', aprovado_por = %s, aprovado_em = NOW()
                WHERE id = (SELECT id FROM motor_planos WHERE dia = %s ORDER BY simulado ASC, id DESC LIMIT 1)
                  AND status = 'proposto'
                RETURNING simulado, resumo->>'total', resumo->>'lotes'
                """,
                (usuario, hoje),
            )
            linha = cur.fetchone()
            if not linha:
                conn.rollback()
                return jsonify({"ok": False, "error": "Não há plano de hoje aguardando aprovação."}), 409
            cur.execute(
                "INSERT INTO motor_diario (tipo, autor, texto) VALUES ('plano', %s, %s)",
                (f"usuario:{usuario}", f"Plano {'simulado' if linha[0] else 'REAL'} de {hoje:%d/%m} aprovado: {linha[1]} alunos em {linha[2]} lotes."),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("motor: falha ao aprovar o plano")
        return jsonify({"ok": False, "error": "Não consegui gravar. Tente de novo."}), 500
    finally:
        conn.close()
    return jsonify({"ok": True})


@motor_ativacoes_bp.route(WEBHOOK_RESPOSTAS, methods=["POST"])
def webhook_motor_respostas():
    """Webhook `message_received` do CRM. Sem sessão: vale a assinatura HMAC do corpo.

    Responde rápido e não chama serviço externo: grava a resposta (botão já
    classificado; texto livre mascarado) e o worker classifica depois.
    """
    segredo = (os.getenv("MOTOR_WEBHOOK_SECRET") or "").strip()
    if not segredo:
        return jsonify({"ok": False, "error": "webhook não configurado"}), 503
    corpo = request.get_data(cache=False) or b""
    if len(corpo) > 64 * 1024 or not assinatura_valida(corpo, request.headers.get("X-Eduit-Signature", ""), segredo):
        return jsonify({"ok": False}), 401
    try:
        recebida = interpretar(json.loads(corpo.decode("utf-8")))
    except (ValueError, UnicodeDecodeError):
        return jsonify({"ok": False}), 400
    organizacao = (os.getenv("MOTOR_CRM_ORG_ID") or "").strip()
    if recebida is None or (organizacao and recebida.organizacao != organizacao):
        return jsonify({"ok": True, "gravada": False})
    try:
        razao = RazaoPg(get_conn)
        b = recebida.botao
        novo = razao.registrar_resposta(
            contact_id=recebida.contact_id, dia=recebida.dia, wa_message_id=recebida.wa_message_id,
            texto_pendente=recebida.texto_mascarado, rotulo=b.rotulo if b else None,
            confianca=b.confianca if b else None, origem=b.origem if b else None,
            observar=(os.getenv("MOTOR_RESPOSTAS_OBSERVAR") or "").strip() == "1",
        )
        if novo and b:
            razao.aplicar_efeito(recebida.contact_id, b.rotulo, recebida.dia)
    except Exception:
        logger.exception("motor: falha ao gravar resposta do webhook")  # sem o corpo: não registrar texto de aluno
        return jsonify({"ok": False}), 500
    return jsonify({"ok": True, "gravada": bool(novo)})


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
