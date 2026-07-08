"""
Distribuicao Comercial — Agendamento de troca de turno (Dia/Noite).

Ver AGENTS.md 2026-07-07 (segunda entrada — modelo de janela).

Modelo de JANELA:
- Cada regra descreve uma JANELA HORARIA em que um turno fica ativo:
  {hora_inicio, hora_fim, turno_alvo, enabled}.
- Ex: turno_alvo='noite', hora_inicio=17:00, hora_fim=22:00 significa:
    das 17:00 as 22:00 -> Modo NOITE ativo (dia inativo, noite conforme snapshot)
    as 22:00           -> aplica Modo DIA (turno oposto: noite inativo, dia
                           conforme snapshot). Ou seja, fora da janela volta
                           automaticamente pro turno oposto.
- Portanto UMA regra dispara 2 vezes por dia: hora_inicio (turno_alvo) e
  hora_fim (turno oposto). last_run_inicio_date e last_run_fim_date evitam
  dupla execucao no mesmo dia.

Snapshots (inalterados em relacao a versao anterior):
- turno_map[id_lead] = 'dia' | 'noite' — divisao global compartilhada.
- snapshot['noite'] = {'id_lead': 'ATIVO' | 'INATIVO', ...} — tirado quando
  o gestor clica TURNO NOITE + SALVAR. Guarda quem estava ativo/inativo dos
  do noturno naquela aplicacao manual. Idem 'dia'.

Ao aplicar turno_alvo='noite':
- Todos os id_leads com turno='dia' viram INATIVO.
- Todos os id_leads no snapshot['noite'] recebem o status do snapshot.
- id_leads com turno='noite' que ainda nao estao no snapshot nao sao tocados.

Endpoint POST /apply/<turno> continua chamando internamente a mesma logica
que o job cron usa — reduz superficie de bug.
"""
from __future__ import annotations

import os
import json
import logging
import threading
from datetime import datetime, date, time as dtime, timezone, timedelta

import psycopg2
import psycopg2.extras
import requests as _requests
from flask import Blueprint, request, jsonify, session

from db import get_conn

logger = logging.getLogger(__name__)

dist_comercial_schedule_bp = Blueprint("dist_comercial_schedule_bp", __name__)

# Mesmos endpoints usados pelo frontend (dist_comercial.js). Persistir aqui
# permite chamar do backend quando o job cron dispara sem o gestor logado.
N8N_LOAD = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/distribuicaocomercial"
N8N_SAVE = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/edicao_distrib"

# Slug de permissao (mesma pagina do painel principal)
PAGE_DIST = "dist_comercial"

# Evita disparos concorrentes (job cron + POST manual chegando ao mesmo tempo)
_apply_lock = threading.Lock()

# Timezone BRT (UTC-3 fixo — Brasil nao observa DST desde 2019). Alinhado com
# routes/kommo_sync.py:_BRT. Usado no _run_scheduled_apply pra comparar wall
# clock BRT contra os TIMEs naive gravados no Postgres pelo usuario.
_BRT = timezone(timedelta(hours=-3))


# ---------------------------------------------------------------------------
# Auth helpers (padrao dos outros blueprints)
# ---------------------------------------------------------------------------

def _session_user() -> tuple[int | None, str, str]:
    if not session.get("authenticated"):
        return None, "", ""
    return (
        session.get("user_id"),
        (session.get("username") or "").strip(),
        (session.get("role") or "").strip(),
    )


def _has_dist_permission() -> tuple[bool, int, dict | None]:
    uid, _uname, role = _session_user()
    if uid is None:
        return False, 401, {"error": "Nao autenticado"}
    if role == "admin" or uid == 0:
        return True, 200, None
    if not uid:
        return False, 403, {"error": "Sem permissao"}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM user_permissions WHERE user_id = %s AND page = %s LIMIT 1",
                (uid, PAGE_DIST),
            )
            if cur.fetchone():
                return True, 200, None
    finally:
        conn.close()
    return False, 403, {"error": "Sem permissao pra Distribuicao Comercial"}


def _current_actor() -> str:
    _uid, uname, role = _session_user()
    if role == "admin":
        return uname or "admin"
    return uname or "sistema"


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

def _list_rules() -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id,
                       hora_inicio::text AS hora_inicio,
                       hora_fim::text    AS hora_fim,
                       turno_alvo, enabled,
                       last_run_inicio_date::text AS last_run_inicio_date,
                       last_run_fim_date::text    AS last_run_fim_date,
                       last_run_at, last_run_result,
                       created_at, updated_at
                FROM dist_comercial_schedule
                ORDER BY hora_inicio, id
            """)
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        r["hora_inicio"] = (r.get("hora_inicio") or "")[:5]
        r["hora_fim"] = (r.get("hora_fim") or "")[:5]
        for k in ("last_run_at", "created_at", "updated_at"):
            if r.get(k):
                r[k] = r[k].isoformat()
        out.append(dict(r))
    return out


def _get_turno_map() -> dict[str, str]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id_lead::text, turno FROM dist_comercial_turno_map")
            return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()


def _get_snapshots() -> dict[str, dict]:
    """Retorna {'dia': {...}, 'noite': {...}} com taken_at e payload por turno."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT turno, payload, taken_at, taken_by
                FROM dist_comercial_snapshot
            """)
            rows = cur.fetchall()
    finally:
        conn.close()
    out = {"dia": None, "noite": None}
    for turno, payload, taken_at, taken_by in rows:
        out[turno] = {
            "payload": payload if isinstance(payload, dict) else json.loads(payload or "{}"),
            "taken_at": taken_at.isoformat() if taken_at else None,
            "taken_by": taken_by or "",
        }
    return out


# ---------------------------------------------------------------------------
# Core: aplicar turno
# ---------------------------------------------------------------------------

def _fetch_cadastro_from_n8n() -> list[dict]:
    """Retorna a lista atual do CRM (mesma que o front consulta em DIST_API_LOAD).

    Precisamos disso pra montar o payload de SALVAR com nome + status_anterior,
    que sao campos exigidos pelo webhook `edicao_distrib`.
    """
    r = _requests.post(N8N_LOAD, headers={"Content-Type": "application/json"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Resposta inesperada de {N8N_LOAD}: {type(data).__name__}")
    return data


def _post_alteracoes_n8n(alteracoes: list[dict]) -> None:
    if not alteracoes:
        return
    r = _requests.post(
        N8N_SAVE,
        headers={"Content-Type": "application/json"},
        json={"alteracoes": alteracoes, "timestamp": datetime.utcnow().isoformat() + "Z"},
        timeout=30,
    )
    r.raise_for_status()


def _compute_target_statuses(
    turno_alvo: str,
    turno_map: dict[str, str],
    snapshots: dict[str, dict],
) -> dict[str, str]:
    """Calcula {id_lead_str: status_alvo} conforme regra do turno.

    turno_alvo='noite': dia -> INATIVO; noite -> aplica snapshot_noite se existir.
    turno_alvo='dia':   noite -> INATIVO; dia -> aplica snapshot_dia se existir.

    Consultores fora do turno_map nao entram (nao tocamos em quem nao foi
    categorizado).
    """
    if turno_alvo not in ("dia", "noite"):
        raise ValueError(f"turno_alvo invalido: {turno_alvo}")

    outro = "noite" if turno_alvo == "dia" else "dia"
    snap_alvo = (snapshots.get(turno_alvo) or {}).get("payload") or {}

    target: dict[str, str] = {}

    for id_lead, turno in turno_map.items():
        key = str(id_lead)
        if turno == outro:
            # Turno oposto ao alvo: forca INATIVO. Se ja estiver INATIVO, o
            # diff a posteriori vai ignorar (nao entra em `alteracoes`).
            target[key] = "INATIVO"
        elif turno == turno_alvo:
            if key in snap_alvo:
                v = str(snap_alvo[key]).upper()
                if v not in ("ATIVO", "INATIVO"):
                    v = "INATIVO"
                target[key] = v
            # else: nao no snapshot ainda -> nao toca

    return target


def _apply_turno(
    turno_alvo: str,
    *,
    origem: str,
    autor: str,
    schedule_id: int | None = None,
    dry_run: bool = False,
) -> dict:
    """Executa a troca de turno.

    origem: 'manual' (POST via UI) ou 'auto' (job APScheduler).
    Retorna resumo pra logging/UI.
    """
    if turno_alvo not in ("dia", "noite"):
        return {"ok": False, "error": "turno_alvo invalido"}

    with _apply_lock:
        try:
            turno_map = _get_turno_map()
            snapshots = _get_snapshots()

            if not turno_map:
                msg = "Nenhum consultor categorizado como Dia/Noite ainda."
                _log_apply(turno_alvo, origem, schedule_id, autor, None, "noop", msg)
                return {"ok": True, "applied": 0, "reason": msg}

            snap_alvo = (snapshots.get(turno_alvo) or {}).get("payload") or {}
            if not snap_alvo:
                # Sem snapshot pro alvo -> aplicar so o "outro=INATIVO" mesmo
                # assim seria brusco (inativa todo mundo). Melhor abortar com
                # aviso claro (o usuario precisa clicar TURNO ALVO + SALVAR ao
                # menos 1x pra criar o snapshot).
                msg = (
                    f"Snapshot do turno '{turno_alvo}' ainda nao existe. "
                    f"Clique em TURNO {turno_alvo.upper()} + SALVAR ao menos "
                    f"uma vez para criar o snapshot antes de agendar."
                )
                _log_apply(turno_alvo, origem, schedule_id, autor, None, "no_snapshot", msg)
                return {"ok": False, "error": msg}

            target = _compute_target_statuses(turno_alvo, turno_map, snapshots)
            if not target:
                msg = "Nada a aplicar (mapa vazio ou sem correspondencia)."
                _log_apply(turno_alvo, origem, schedule_id, autor, None, "noop", msg)
                return {"ok": True, "applied": 0, "reason": msg}

            cadastro = _fetch_cadastro_from_n8n()
            by_id: dict[str, dict] = {}
            for row in cadastro:
                idl = row.get("id_lead")
                if idl is not None:
                    by_id[str(idl)] = row

            alteracoes: list[dict] = []
            skipped_no_row = 0
            unchanged = 0

            for id_lead_str, novo_status in target.items():
                row = by_id.get(id_lead_str)
                if not row:
                    skipped_no_row += 1
                    continue
                atual = (row.get("status") or "").upper()
                if atual == novo_status:
                    unchanged += 1
                    continue
                alteracoes.append({
                    "id_lead": int(id_lead_str) if id_lead_str.isdigit() else id_lead_str,
                    "nome": row.get("nome") or "",
                    "campo": "status",
                    "valorAnterior": atual or "(vazio)",
                    "valorNovo": novo_status,
                    "status": novo_status,
                })

            resumo = {
                "ok": True,
                "turno_alvo": turno_alvo,
                "applied": 0 if dry_run else len(alteracoes),
                "would_apply": len(alteracoes),
                "unchanged": unchanged,
                "skipped_no_row": skipped_no_row,
                "origem": origem,
                "autor": autor,
                "dry_run": bool(dry_run),
            }

            if dry_run:
                return resumo

            if alteracoes:
                _post_alteracoes_n8n(alteracoes)

            _log_apply(
                turno_alvo, origem, schedule_id, autor,
                {"alteracoes": alteracoes, "resumo": {k: v for k, v in resumo.items() if k != "alteracoes"}},
                "ok",
                f"{len(alteracoes)} status alterado(s); {unchanged} inalterado(s); {skipped_no_row} sem correspondencia no CRM.",
            )
            return resumo

        except Exception as e:
            logger.exception("Falha ao aplicar turno %s (origem=%s)", turno_alvo, origem)
            _log_apply(turno_alvo, origem, schedule_id, autor, None, "error", str(e))
            return {"ok": False, "error": str(e)}


def _log_apply(
    turno_alvo: str,
    origem: str,
    schedule_id: int | None,
    autor: str,
    payload: dict | None,
    resultado: str,
    mensagem: str,
) -> None:
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dist_comercial_apply_log
                    (turno_alvo, origem, schedule_id, autor, payload, resultado, mensagem)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (turno_alvo, origem, schedule_id, autor,
                 json.dumps(payload) if payload else None, resultado, mensagem),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Falha ao gravar apply_log: %s", e)


# ---------------------------------------------------------------------------
# Rotas — Regras
# ---------------------------------------------------------------------------

@dist_comercial_schedule_bp.route("/api/dist-comercial/rules", methods=["GET"])
def api_rules_list():
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status
    return jsonify({"ok": True, "rules": _list_rules()})


def _parse_hora(hora_raw: str) -> str:
    """Valida e normaliza 'HH:MM' (aceita 'H:MM' tambem)."""
    if not hora_raw:
        raise ValueError("hora obrigatoria")
    parts = hora_raw.split(":")
    if len(parts) < 2:
        raise ValueError("hora invalida (use HH:MM)")
    try:
        h = int(parts[0])
        m = int(parts[1])
    except ValueError:
        raise ValueError("hora invalida (use HH:MM)")
    if not (0 <= h < 24 and 0 <= m < 60):
        raise ValueError("hora fora do range 00:00 - 23:59")
    return f"{h:02d}:{m:02d}:00"


@dist_comercial_schedule_bp.route("/api/dist-comercial/rules", methods=["POST"])
def api_rules_create():
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status

    body = request.get_json(silent=True) or {}
    try:
        hora_inicio = _parse_hora((body.get("hora_inicio") or "").strip())
        hora_fim = _parse_hora((body.get("hora_fim") or "").strip())
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if hora_inicio == hora_fim:
        return jsonify({"error": "hora_inicio e hora_fim nao podem ser iguais"}), 400

    turno_alvo = (body.get("turno_alvo") or "").strip().lower()
    if turno_alvo not in ("dia", "noite"):
        return jsonify({"error": "turno_alvo deve ser 'dia' ou 'noite'"}), 400

    enabled = bool(body.get("enabled", True))

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dist_comercial_schedule (hora_inicio, hora_fim, turno_alvo, enabled)
                VALUES (%s::time, %s::time, %s, %s)
                RETURNING id
                """,
                (hora_inicio, hora_fim, turno_alvo, enabled),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "id": new_id})


@dist_comercial_schedule_bp.route("/api/dist-comercial/rules/<int:rule_id>", methods=["PATCH"])
def api_rules_update(rule_id: int):
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status

    body = request.get_json(silent=True) or {}
    sets = []
    params: list = []

    if "hora_inicio" in body:
        try:
            hi = _parse_hora((body.get("hora_inicio") or "").strip())
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        sets.append("hora_inicio = %s::time")
        params.append(hi)
    if "hora_fim" in body:
        try:
            hf = _parse_hora((body.get("hora_fim") or "").strip())
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        sets.append("hora_fim = %s::time")
        params.append(hf)
    if "turno_alvo" in body:
        ta = (body.get("turno_alvo") or "").strip().lower()
        if ta not in ("dia", "noite"):
            return jsonify({"error": "turno_alvo deve ser 'dia' ou 'noite'"}), 400
        sets.append("turno_alvo = %s")
        params.append(ta)
    if "enabled" in body:
        sets.append("enabled = %s")
        params.append(bool(body["enabled"]))
    if not sets:
        return jsonify({"error": "nada a atualizar"}), 400

    sets.append("updated_at = NOW()")
    params.append(rule_id)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE dist_comercial_schedule SET {', '.join(sets)} WHERE id = %s",
                params,
            )
            if cur.rowcount == 0:
                return jsonify({"error": "regra nao encontrada"}), 404
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True})


@dist_comercial_schedule_bp.route("/api/dist-comercial/rules/<int:rule_id>", methods=["DELETE"])
def api_rules_delete(rule_id: int):
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dist_comercial_schedule WHERE id = %s", (rule_id,))
            if cur.rowcount == 0:
                return jsonify({"error": "regra nao encontrada"}), 404
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Turno map
# ---------------------------------------------------------------------------

@dist_comercial_schedule_bp.route("/api/dist-comercial/turno-map", methods=["GET"])
def api_turno_map_get():
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status
    return jsonify({"ok": True, "map": _get_turno_map()})


@dist_comercial_schedule_bp.route("/api/dist-comercial/turno-map", methods=["PUT"])
def api_turno_map_put():
    """Recebe {'map': {'id_lead': 'dia' | 'noite', ...}} e substitui o mapa inteiro.

    Idempotente. Consultores omitidos do payload sao removidos do mapa (voltam
    ao estado "sem categorizacao"). Frontend envia o mapa completo apos
    qualquer toggle pra manter consistencia entre gestores.
    """
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status

    body = request.get_json(silent=True) or {}
    incoming = body.get("map") or {}
    if not isinstance(incoming, dict):
        return jsonify({"error": "map deve ser um objeto {id_lead: turno}"}), 400

    validated: list[tuple[int, str]] = []
    for k, v in incoming.items():
        try:
            id_lead = int(k)
        except (TypeError, ValueError):
            continue
        turno = str(v or "").lower().strip()
        if turno not in ("dia", "noite"):
            continue
        validated.append((id_lead, turno))

    actor = _current_actor()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dist_comercial_turno_map")
            if validated:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO dist_comercial_turno_map (id_lead, turno, updated_by) VALUES %s",
                    [(idl, t, actor) for idl, t in validated],
                )
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "count": len(validated)})


# ---------------------------------------------------------------------------
# Rotas — Snapshot
# ---------------------------------------------------------------------------

@dist_comercial_schedule_bp.route("/api/dist-comercial/snapshot", methods=["GET"])
def api_snapshot_get():
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status
    return jsonify({"ok": True, "snapshots": _get_snapshots()})


@dist_comercial_schedule_bp.route("/api/dist-comercial/snapshot", methods=["POST"])
def api_snapshot_post():
    """Body: {'turno': 'dia'|'noite', 'payload': {'id_lead': 'ATIVO'|'INATIVO', ...}}.

    Upsert. Chamado pelo frontend depois de SALVAR bem-sucedido quando o
    gestor aplicou Modo Dia/Noite manualmente.
    """
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status

    body = request.get_json(silent=True) or {}
    turno = (body.get("turno") or "").strip().lower()
    if turno not in ("dia", "noite"):
        return jsonify({"error": "turno deve ser 'dia' ou 'noite'"}), 400

    payload = body.get("payload") or {}
    if not isinstance(payload, dict):
        return jsonify({"error": "payload deve ser um objeto {id_lead: status}"}), 400

    clean: dict[str, str] = {}
    for k, v in payload.items():
        try:
            idl = int(k)
        except (TypeError, ValueError):
            continue
        vv = str(v or "").upper().strip()
        if vv not in ("ATIVO", "INATIVO"):
            continue
        clean[str(idl)] = vv

    actor = _current_actor()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dist_comercial_snapshot (turno, payload, taken_by)
                VALUES (%s, %s::jsonb, %s)
                ON CONFLICT (turno) DO UPDATE
                SET payload = EXCLUDED.payload,
                    taken_at = NOW(),
                    taken_by = EXCLUDED.taken_by
                """,
                (turno, json.dumps(clean), actor),
            )
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "count": len(clean)})


@dist_comercial_schedule_bp.route("/api/dist-comercial/snapshot/<turno>", methods=["DELETE"])
def api_snapshot_delete(turno: str):
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status
    turno = (turno or "").strip().lower()
    if turno not in ("dia", "noite"):
        return jsonify({"error": "turno invalido"}), 400
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dist_comercial_snapshot WHERE turno = %s", (turno,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Rotas — Apply (manual/dry-run)
# ---------------------------------------------------------------------------

@dist_comercial_schedule_bp.route("/api/dist-comercial/apply/<turno>", methods=["POST"])
def api_apply(turno: str):
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status
    turno = (turno or "").strip().lower()
    body = request.get_json(silent=True) or {}
    dry_run = bool(body.get("dry_run", False))
    result = _apply_turno(
        turno,
        origem="manual",
        autor=_current_actor(),
        schedule_id=None,
        dry_run=dry_run,
    )
    return jsonify(result), (200 if result.get("ok") else 400)


@dist_comercial_schedule_bp.route("/api/dist-comercial/apply-log", methods=["GET"])
def api_apply_log():
    ok, status, err = _has_dist_permission()
    if not ok:
        return jsonify(err), status
    limit = 50
    try:
        limit = int(request.args.get("limit", 50))
        limit = max(1, min(200, limit))
    except (TypeError, ValueError):
        limit = 50

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, turno_alvo, origem, schedule_id, autor,
                       resultado, mensagem, executed_at
                FROM dist_comercial_apply_log
                ORDER BY executed_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        d = dict(r)
        if d.get("executed_at"):
            d["executed_at"] = d["executed_at"].isoformat()
        out.append(d)
    return jsonify({"ok": True, "log": out})


# ---------------------------------------------------------------------------
# APScheduler job — dispara as regras ao minuto certo
# ---------------------------------------------------------------------------

def _run_scheduled_apply() -> None:
    """Executa a cada minuto. Cada regra dispara ate 2 vezes por dia:

    - Em `hora_inicio`: aplica turno_alvo (Modo NOITE ou Modo DIA).
    - Em `hora_fim`: aplica turno OPOSTO (fim da janela, volta ao complemento).

    Dedup por dia (por gatilho): `last_run_inicio_date` e `last_run_fim_date`
    guardam a data da ultima execucao de cada extremidade. Se hoje ja rodou,
    nao repete.

    Grace: dispara se `agora >= hora_gatilho` e `last_run_*_date != hoje` —
    cobre casos onde o servidor estava parado no minuto exato (misfire).
    """
    try:
        # BRT explicito: o usuario digita horarios pensando em Sao Paulo. Se o
        # server rodar em UTC (Easypanel default), datetime.now() naive traria
        # UTC e disparaia 3h cedo demais. .time() de um aware retorna naive
        # (wall clock BRT), que compara corretamente contra o TIME naive gravado
        # pelo usuario em hora_inicio/hora_fim.
        now = datetime.now(_BRT)
        today = now.date()
        current_t = now.time().replace(tzinfo=None)

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, hora_inicio, hora_fim, turno_alvo,
                           last_run_inicio_date, last_run_fim_date
                    FROM dist_comercial_schedule
                    WHERE enabled = TRUE
                    ORDER BY hora_inicio, id
                    """
                )
                rules = cur.fetchall()
        finally:
            conn.close()

        if not rules:
            return

        # Monta lista de gatilhos pendentes: (rule_id, edge, turno_a_aplicar).
        # edge='inicio' -> aplica turno_alvo; edge='fim' -> aplica turno oposto.
        pending: list[tuple[int, str, str]] = []
        for rid, hi, hf, turno_alvo, last_inicio, last_fim in rules:
            if hi and current_t >= hi and (last_inicio is None or last_inicio < today):
                pending.append((rid, "inicio", turno_alvo))
            if hf and current_t >= hf and (last_fim is None or last_fim < today):
                oposto = "noite" if turno_alvo == "dia" else "dia"
                pending.append((rid, "fim", oposto))

        if not pending:
            return

        for rid, edge, turno_a_aplicar in pending:
            logger.info(
                "dist_comercial_schedule: disparando regra id=%s edge=%s turno=%s",
                rid, edge, turno_a_aplicar,
            )
            result = _apply_turno(
                turno_a_aplicar,
                origem="auto",
                autor="scheduler",
                schedule_id=rid,
            )
            col = "last_run_inicio_date" if edge == "inicio" else "last_run_fim_date"
            try:
                conn = get_conn()
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE dist_comercial_schedule
                        SET {col} = %s,
                            last_run_at = NOW(),
                            last_run_result = %s
                        WHERE id = %s
                        """,
                        (
                            today,
                            (f"{edge}: ok" if result.get("ok")
                             else f"{edge}: error - {result.get('error') or 'unknown'}"),
                            rid,
                        ),
                    )
                conn.commit()
                conn.close()
            except Exception as e:
                logger.warning("Falha ao marcar %s: %s", col, e)
    except Exception as e:
        logger.exception("dist_comercial_schedule: erro no job de check: %s", e)


def register_dist_comercial_schedule_job(sched) -> None:
    """Registra o job que roda a cada 1 minuto pra checar regras a disparar."""
    from apscheduler.triggers.interval import IntervalTrigger
    try:
        sched.remove_job("dist_comercial_schedule_check")
    except Exception:
        pass
    sched.add_job(
        _run_scheduled_apply,
        trigger=IntervalTrigger(minutes=1),
        id="dist_comercial_schedule_check",
        replace_existing=True,
        misfire_grace_time=120,
        max_instances=1,
    )
    logger.info("dist_comercial_schedule job registrado (interval=1min)")
