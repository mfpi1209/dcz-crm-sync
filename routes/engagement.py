import os
import json
import logging
import threading
from datetime import datetime, timezone, timedelta, date

import psycopg2
import psycopg2.extras
import csv
import io
import requests as _requests
from flask import Blueprint, request, jsonify, current_app, Response

from db import get_conn
from helpers import BRT, to_brt

_log = logging.getLogger(__name__)

engagement_bp = Blueprint("engagement", __name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

N8N_COMM_WEBHOOK = os.getenv(
    "N8N_COMM_WEBHOOK",
    "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/comm-engagement",
)

NEW_STUDENT_DAYS = 30
RECENCY_WINDOW_DAYS = 45

# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------


def _parse_date_flexible(val):
    """Parse a date string in multiple formats, return datetime or None."""
    if not val:
        return None
    if isinstance(val, (datetime, date)):
        return val if isinstance(val, datetime) else datetime.combine(val, datetime.min.time())
    val = str(val).strip()
    if not val:
        return None
    clean = val.replace(" AM", "").replace(" PM", "").replace(" am", "").replace(" pm", "")
    for fmt in (
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M",
    ):
        try:
            return datetime.strptime(clean[:len(fmt.replace('%', 'X'))], fmt)
        except (ValueError, IndexError):
            continue
    # M/D/YYYY fallback (handles single-digit month/day)
    import re as _re
    m = _re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", val)
    if m:
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if month > 12 and day <= 12:
            month, day = day, month
        try:
            return datetime(year, month, day)
        except ValueError:
            pass
    return None


def calculate_engagement_scores():
    """Cross-reference matriculados x acesso_ava and compute engagement scores."""
    conn = get_conn()
    today = datetime.now().date()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM xl_snapshots WHERE tipo='matriculados' ORDER BY id DESC LIMIT 1")
            snap_mat = cur.fetchone()
            cur.execute("SELECT id FROM xl_snapshots WHERE tipo='acesso_ava' ORDER BY id DESC LIMIT 1")
            snap_ava = cur.fetchone()

            if not snap_mat:
                return {"error": "Nenhum snapshot de matriculados encontrado", "processed": 0}

            cur.execute("""
                SELECT data FROM xl_rows WHERE snapshot_id = %s
                AND COALESCE(data->>'rgm_digits', '') != ''
            """, (snap_mat["id"],))
            mat_rows = {r["data"]["rgm_digits"]: r["data"] for r in cur.fetchall()}

            ava_rows = {}
            if snap_ava:
                cur.execute("""
                    SELECT data FROM xl_rows WHERE snapshot_id = %s
                    AND COALESCE(data->>'rgm_digits', '') != ''
                """, (snap_ava["id"],))
                ava_rows = {r["data"]["rgm_digits"]: r["data"] for r in cur.fetchall()}

            all_interactions = []
            all_minutes = []
            for ad in ava_rows.values():
                try:
                    all_interactions.append(int(float(ad.get("interacoes", 0) or 0)))
                except (ValueError, TypeError):
                    pass
                try:
                    all_minutes.append(float(ad.get("minutos", 0) or 0))
                except (ValueError, TypeError):
                    pass
            avg_interactions = sum(all_interactions) / max(len(all_interactions), 1)
            avg_minutes = sum(all_minutes) / max(len(all_minutes), 1)

            processed = 0
            with_ava = 0
            without_ava = 0
            for rgm, mat_data in mat_rows.items():
                sit = (mat_data.get("situacao") or "").strip().lower()
                if sit and sit not in ("em curso", "ativo", "matriculado"):
                    continue

                dt_mat = _parse_date_flexible(mat_data.get("data_mat"))
                days_enrolled = (today - dt_mat.date()).days if dt_mat else None
                is_new = days_enrolled is not None and days_enrolled <= NEW_STUDENT_DAYS

                ava_data = ava_rows.get(rgm)
                if ava_data:
                    with_ava += 1
                else:
                    without_ava += 1
                if ava_data:
                    dt_access = _parse_date_flexible(ava_data.get("ultimo_acesso"))
                    days_no_access = (today - dt_access.date()).days if dt_access else None
                    try:
                        interactions = int(float(ava_data.get("interacoes", 0) or 0))
                    except (ValueError, TypeError):
                        interactions = 0
                    try:
                        minutes = float(ava_data.get("minutos", 0) or 0)
                    except (ValueError, TypeError):
                        minutes = 0.0
                else:
                    days_no_access = None
                    interactions = 0
                    minutes = 0.0

                recency = 0
                if days_no_access is not None:
                    recency = max(0, 100 - int(days_no_access * 100 / RECENCY_WINDOW_DAYS))
                elif ava_data is None:
                    recency = 0

                depth = 0
                if avg_interactions > 0 or avg_minutes > 0:
                    int_ratio = min(interactions / max(avg_interactions, 1), 2.0) * 50
                    min_ratio = min(minutes / max(avg_minutes, 1), 2.0) * 50
                    depth = int((int_ratio + min_ratio) / 2)
                elif interactions > 0 or minutes > 0:
                    depth = 50

                frequency = 0
                if ava_data and days_no_access is not None and days_no_access <= 14:
                    frequency = max(0, 100 - days_no_access * 4)
                elif ava_data and days_no_access is not None:
                    frequency = max(0, 30 - (days_no_access - 14) * 2)

                phase_penalty = 0
                if is_new and ava_data is None:
                    phase_penalty = -30
                elif is_new and days_no_access is not None and days_no_access > 3:
                    phase_penalty = -15

                score = int(
                    recency * 0.40
                    + frequency * 0.25
                    + depth * 0.20
                    + max(0, 100 + phase_penalty) * 0.15
                )
                score = max(0, min(100, score))

                if score >= 75:
                    risk = "engajado"
                elif score >= 55:
                    risk = "atencao"
                elif score >= 46:
                    risk = "em_risco"
                else:
                    risk = "critico"

                detail = {
                    "recency": recency, "frequency": frequency,
                    "depth": depth, "phase_penalty": phase_penalty,
                    "is_new": is_new, "nome": mat_data.get("nome", ""),
                    "curso": mat_data.get("curso", ""),
                    "polo": mat_data.get("polo", ""),
                    "email": mat_data.get("email", ""),
                }

                cur.execute("""
                    INSERT INTO ava_engagement
                        (rgm, snapshot_date, score, risk_level, days_since_enrollment,
                         days_since_last_access, access_count, interaction_count,
                         total_minutes, detail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (rgm, snapshot_date)
                    DO UPDATE SET score=EXCLUDED.score, risk_level=EXCLUDED.risk_level,
                        days_since_enrollment=EXCLUDED.days_since_enrollment,
                        days_since_last_access=EXCLUDED.days_since_last_access,
                        access_count=EXCLUDED.access_count,
                        interaction_count=EXCLUDED.interaction_count,
                        total_minutes=EXCLUDED.total_minutes,
                        detail=EXCLUDED.detail
                """, (
                    rgm, today, score, risk, days_enrolled, days_no_access,
                    1 if ava_data else 0, interactions, minutes,
                    json.dumps(detail, ensure_ascii=False),
                ))
                processed += 1

        conn.commit()
        return {
            "processed": processed,
            "date": str(today),
            "with_ava": with_ava,
            "without_ava": without_ava,
            "has_ava_snapshot": snap_ava is not None,
            "ava_rows_total": len(ava_rows),
            "mat_rows_total": len(mat_rows),
        }
    except Exception as e:
        conn.rollback()
        current_app.logger.error("Engagement score error: %s", e)
        return {"error": str(e), "processed": 0}
    finally:
        conn.close()


def _render_template(template, data):
    """Replace {{var}} placeholders in a template string."""
    result = template
    for key, val in data.items():
        result = result.replace("{{" + key + "}}", str(val or ""))
    return result


def evaluate_comm_triggers():
    """Daily job: recalculate scores, evaluate rules, enqueue communications."""
    _log.info("[ENGAGEMENT] Iniciando avaliação diária de gatilhos")
    calc_result = calculate_engagement_scores()
    current_app.logger.info("[ENGAGEMENT] Scores recalculados: %s", calc_result)

    conn = get_conn()
    today = datetime.now(BRT)
    today_date = today.date()
    weekday = today.weekday()
    if weekday >= 5:
        current_app.logger.info("[ENGAGEMENT] Fim de semana — disparos adiados")
        conn.close()
        return

    try:
        enqueued = 0
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM comm_rules WHERE enabled = TRUE ORDER BY priority")
            rules = cur.fetchall()
            if not rules:
                current_app.logger.info("[ENGAGEMENT] Nenhuma regra ativa")
                conn.close()
                return

            cur.execute("""
                SELECT rgm, score, risk_level, days_since_enrollment,
                       days_since_last_access, detail
                FROM ava_engagement
                WHERE snapshot_date = %s AND risk_level IN ('critico', 'em_risco', 'atencao')
            """, (today_date,))
            students = cur.fetchall()

            for student in students:
                rgm = student["rgm"]
                detail = student["detail"] or {}
                days_enrolled = student["days_since_enrollment"]
                days_no_access = student["days_since_last_access"]
                is_new = days_enrolled is not None and days_enrolled <= NEW_STUDENT_DAYS

                cur.execute("""
                    SELECT COUNT(*) as cnt FROM comm_queue
                    WHERE rgm = %s AND created_at >= %s - INTERVAL '7 days'
                    AND status IN ('pendente', 'enviado')
                """, (rgm, today))
                week_count = cur.fetchone()["cnt"]

                cur.execute("""
                    SELECT MAX(created_at) as last_comm FROM comm_queue
                    WHERE rgm = %s AND status IN ('pendente', 'enviado')
                """, (rgm,))
                last_comm_row = cur.fetchone()
                last_comm = last_comm_row["last_comm"] if last_comm_row else None

                for rule in rules:
                    if rule["audience"] == "novo_aluno" and not is_new:
                        continue
                    if rule["audience"] == "veterano" and is_new:
                        continue

                    if week_count >= rule["max_per_week"]:
                        continue

                    if last_comm and (today - last_comm).days < rule["cooldown_days"]:
                        continue

                    trigger_days = rule["trigger_days"]
                    matches = False
                    if rule["trigger_type"] == "sem_acesso_inicial":
                        if is_new and days_enrolled is not None and days_enrolled >= trigger_days:
                            if days_no_access is None or days_no_access >= trigger_days:
                                matches = True
                    elif rule["trigger_type"] == "inatividade":
                        if days_no_access is not None and days_no_access >= trigger_days:
                            matches = True
                        elif days_no_access is None and days_enrolled and days_enrolled >= trigger_days:
                            matches = True
                    elif rule["trigger_type"] == "score_baixo":
                        if student["score"] <= trigger_days:
                            matches = True

                    if not matches:
                        continue

                    cur.execute("""
                        SELECT id FROM comm_queue
                        WHERE rgm = %s AND rule_id = %s
                        AND created_at >= %s - INTERVAL '30 days'
                        AND status IN ('pendente', 'enviado')
                    """, (rgm, rule["id"], today))
                    if cur.fetchone():
                        continue

                    nome = detail.get("nome", "")
                    primeiro_nome = nome.split()[0] if nome else ""
                    tpl_data = {
                        "nome": nome,
                        "primeiro_nome": primeiro_nome,
                        "curso": detail.get("curso", ""),
                        "polo": detail.get("polo", ""),
                        "email": detail.get("email", ""),
                        "dias_sem_acesso": str(days_no_access or "N/A"),
                        "score": str(student["score"]),
                        "rgm": rgm,
                    }
                    message = _render_template(rule["message_template"], tpl_data)

                    payload = {
                        "event": "ava_engagement_alert",
                        "channel": rule["channel"],
                        "student": {
                            "name": nome,
                            "first_name": primeiro_nome,
                            "email": detail.get("email", ""),
                            "rgm": rgm,
                            "curso": detail.get("curso", ""),
                            "polo": detail.get("polo", ""),
                        },
                        "rule": {
                            "id": rule["id"],
                            "name": rule["name"],
                        },
                        "message": message,
                        "context": {
                            "days_since_enrollment": days_enrolled,
                            "days_since_last_access": days_no_access,
                            "engagement_score": student["score"],
                            "risk_level": student["risk_level"],
                        },
                    }

                    cur.execute("""
                        INSERT INTO comm_queue (rgm, rule_id, channel, status, payload, scheduled_for)
                        VALUES (%s, %s, %s, 'pendente', %s, NOW())
                    """, (rgm, rule["id"], rule["channel"], json.dumps(payload, ensure_ascii=False)))
                    enqueued += 1
                    break

        conn.commit()
        current_app.logger.info("[ENGAGEMENT] %d comunicações enfileiradas", enqueued)

        _dispatch_pending_comms()

    except Exception as e:
        conn.rollback()
        current_app.logger.error("[ENGAGEMENT] Erro na avaliação: %s", e)
    finally:
        conn.close()


def _dispatch_pending_comms():
    """Send pending communications to n8n webhook."""
    conn = get_conn()
    sent = 0
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, rgm, rule_id, channel, payload
                FROM comm_queue
                WHERE status = 'pendente'
                ORDER BY created_at
                LIMIT 50
            """)
            pending = cur.fetchall()

            for item in pending:
                try:
                    payload = item["payload"] or {}
                    payload["callback_url"] = request.host_url.rstrip("/") + "/api/comm/callback" if hasattr(request, "host_url") else ""

                    r = _requests.post(N8N_COMM_WEBHOOK, json=payload, timeout=15)
                    n8n_resp = {}
                    try:
                        n8n_resp = r.json() if r.text.strip() else {}
                    except Exception:
                        n8n_resp = {"status_code": r.status_code}

                    if r.ok:
                        cur.execute("""
                            UPDATE comm_queue SET status='enviado', sent_at=NOW(), n8n_response=%s
                            WHERE id=%s
                        """, (json.dumps(n8n_resp), item["id"]))
                        msg_preview = (payload.get("message") or "")[:200]
                        cur.execute("""
                            INSERT INTO comm_log (rgm, rule_id, channel, message_preview, status, metadata)
                            VALUES (%s, %s, %s, %s, 'enviado', %s)
                        """, (item["rgm"], item["rule_id"], item["channel"],
                              msg_preview, json.dumps(n8n_resp)))
                        sent += 1
                    else:
                        cur.execute("""
                            UPDATE comm_queue SET status='falha', n8n_response=%s WHERE id=%s
                        """, (json.dumps(n8n_resp), item["id"]))
                        cur.execute("""
                            INSERT INTO comm_log (rgm, rule_id, channel, message_preview, status, metadata)
                            VALUES (%s, %s, %s, %s, 'falha', %s)
                        """, (item["rgm"], item["rule_id"], item["channel"],
                              "FALHA NO ENVIO", json.dumps(n8n_resp)))

                except Exception as e:
                    current_app.logger.warning("[COMM] Erro ao enviar para n8n (queue %d): %s", item["id"], e)
                    cur.execute("UPDATE comm_queue SET status='falha' WHERE id=%s", (item["id"],))

        conn.commit()
        current_app.logger.info("[COMM] %d/%d comunicações enviadas ao n8n", sent, len(pending))
    except Exception as e:
        conn.rollback()
        current_app.logger.error("[COMM] Erro no dispatch: %s", e)
    finally:
        conn.close()


def register_engagement_job(scheduler):
    """Register the daily engagement evaluation job.

    Accepts the APScheduler instance as parameter so this module
    doesn't need to import the global scheduler directly.
    """
    from apscheduler.triggers.cron import CronTrigger

    try:
        scheduler.remove_job("engagement_daily")
    except Exception:
        pass
    trigger = CronTrigger(hour=8, minute=0, timezone="America/Sao_Paulo")
    scheduler.add_job(
        evaluate_comm_triggers,
        trigger=trigger,
        id="engagement_daily",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    _log.info("Engagement daily job registered (08:00 BRT)")


# ---------------------------------------------------------------------------
# Rotas — Engagement Scores
# ---------------------------------------------------------------------------

@engagement_bp.route("/api/engagement/recalculate", methods=["POST"])
def api_engagement_recalculate():
    result = calculate_engagement_scores()
    return jsonify(result)


@engagement_bp.route("/api/engagement/scores")
def api_engagement_scores():
    risk = request.args.get("risk", "").strip()
    polo = request.args.get("polo", "").strip()
    curso = request.args.get("curso", "").strip()
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    offset = (page - 1) * per_page

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            conditions = ["snapshot_date = (SELECT MAX(snapshot_date) FROM ava_engagement)"]
            params = []
            if risk:
                conditions.append("risk_level = %s")
                params.append(risk)
            if polo:
                conditions.append("detail->>'polo' ILIKE %s")
                params.append(f"%{polo}%")
            if curso:
                conditions.append("detail->>'curso' ILIKE %s")
                params.append(f"%{curso}%")

            where = " AND ".join(conditions)

            cur.execute(f"SELECT COUNT(*) as cnt FROM ava_engagement WHERE {where}", params)
            total = cur.fetchone()["cnt"]

            cur.execute(f"""
                SELECT rgm, score, risk_level, days_since_enrollment,
                       days_since_last_access, interaction_count, total_minutes,
                       detail, snapshot_date
                FROM ava_engagement WHERE {where}
                ORDER BY score ASC, rgm
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            rows = cur.fetchall()

            for r in rows:
                r["snapshot_date"] = str(r["snapshot_date"]) if r["snapshot_date"] else None

            cur.execute("""
                SELECT risk_level, COUNT(*) as cnt
                FROM ava_engagement
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM ava_engagement)
                GROUP BY risk_level
            """)
            summary = {r["risk_level"]: r["cnt"] for r in cur.fetchall()}

            cur.execute("SELECT id FROM xl_snapshots WHERE tipo='acesso_ava' ORDER BY id DESC LIMIT 1")
            has_ava = cur.fetchone() is not None

            cur.execute("""
                SELECT COUNT(*) as cnt FROM ava_engagement
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM ava_engagement)
                  AND days_since_last_access IS NULL
            """)
            sem_ava_count = cur.fetchone()["cnt"]

        return jsonify({
            "scores": rows,
            "total": total,
            "page": page,
            "per_page": per_page,
            "summary": summary,
            "has_ava_snapshot": has_ava,
            "sem_ava_count": sem_ava_count,
        })
    finally:
        conn.close()


@engagement_bp.route("/api/engagement/export-sem-ava")
def api_engagement_export_sem_ava():
    """Exporta CSV dos alunos sem dados no AVA (days_since_last_access IS NULL)."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT rgm, score, risk_level,
                       detail->>'nome' as nome,
                       detail->>'curso' as curso,
                       detail->>'polo' as polo,
                       detail->>'email' as email
                FROM ava_engagement
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM ava_engagement)
                  AND days_since_last_access IS NULL
                ORDER BY detail->>'nome'
            """)
            rows = cur.fetchall()

        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=';')
        writer.writerow(["Nome", "RGM", "Curso", "Polo", "Email", "Score", "Risco"])
        for r in rows:
            writer.writerow([
                r["nome"] or "", r["rgm"] or "", r["curso"] or "",
                r["polo"] or "", r["email"] or "",
                r["score"], r["risk_level"] or "",
            ])

        output = buf.getvalue()
        return Response(
            output,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=alunos_sem_ava.csv"},
        )
    finally:
        conn.close()


@engagement_bp.route("/api/engagement/timeline")
def api_engagement_timeline():
    days = int(request.args.get("days", 90))
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT snapshot_date,
                       ROUND(AVG(score)) as avg_score,
                       COUNT(*) as total,
                       SUM(CASE WHEN risk_level='engajado' THEN 1 ELSE 0 END) as engajados,
                       SUM(CASE WHEN risk_level='atencao' THEN 1 ELSE 0 END) as atencao,
                       SUM(CASE WHEN risk_level='em_risco' THEN 1 ELSE 0 END) as em_risco,
                       SUM(CASE WHEN risk_level='critico' THEN 1 ELSE 0 END) as criticos
                FROM ava_engagement
                WHERE snapshot_date >= CURRENT_DATE - %s
                GROUP BY snapshot_date
                ORDER BY snapshot_date
            """, (days,))
            points = cur.fetchall()
            for p in points:
                p["snapshot_date"] = str(p["snapshot_date"])
        return jsonify({"points": points, "days": days})
    finally:
        conn.close()


@engagement_bp.route("/api/engagement/student/<rgm>")
def api_engagement_student(rgm):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM ava_engagement WHERE rgm = %s
                ORDER BY snapshot_date DESC LIMIT 30
            """, (rgm,))
            history = cur.fetchall()
            for h in history:
                h["snapshot_date"] = str(h["snapshot_date"]) if h["snapshot_date"] else None

            cur.execute("""
                SELECT * FROM comm_log WHERE rgm = %s
                ORDER BY sent_at DESC LIMIT 20
            """, (rgm,))
            comms = cur.fetchall()
            for c in comms:
                c["sent_at"] = to_brt(c["sent_at"])

        return jsonify({"history": history, "communications": comms})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Régua de Comunicação (CRUD)
# ---------------------------------------------------------------------------

@engagement_bp.route("/api/comm/rules", methods=["GET"])
def api_comm_rules_list():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM comm_rules ORDER BY priority, id")
            rules = cur.fetchall()
            for r in rules:
                r["created_at"] = to_brt(r["created_at"])
                r["updated_at"] = to_brt(r["updated_at"])
        return jsonify({"rules": rules})
    finally:
        conn.close()


@engagement_bp.route("/api/comm/rules", methods=["POST"])
def api_comm_rules_create():
    b = request.json or {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO comm_rules (name, description, audience, trigger_type, trigger_days,
                    channel, escalation_channel, escalation_after_days, message_template,
                    cooldown_days, max_per_week, priority, enabled)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (
                b.get("name", "Nova Regra"), b.get("description", ""),
                b.get("audience", "todos"), b.get("trigger_type", "inatividade"),
                int(b.get("trigger_days", 7)), b.get("channel", "email"),
                b.get("escalation_channel"), b.get("escalation_after_days"),
                b.get("message_template", ""), int(b.get("cooldown_days", 3)),
                int(b.get("max_per_week", 2)), int(b.get("priority", 0)),
                b.get("enabled", True),
            ))
            new_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({"ok": True, "id": new_id})
    finally:
        conn.close()


@engagement_bp.route("/api/comm/rules/<int:rid>", methods=["PUT"])
def api_comm_rules_update(rid):
    b = request.json or {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE comm_rules SET
                    name=%s, description=%s, audience=%s, trigger_type=%s, trigger_days=%s,
                    channel=%s, escalation_channel=%s, escalation_after_days=%s,
                    message_template=%s, cooldown_days=%s, max_per_week=%s,
                    priority=%s, enabled=%s, updated_at=NOW()
                WHERE id=%s
            """, (
                b.get("name"), b.get("description"), b.get("audience"),
                b.get("trigger_type"), int(b.get("trigger_days", 7)),
                b.get("channel"), b.get("escalation_channel"),
                b.get("escalation_after_days"), b.get("message_template"),
                int(b.get("cooldown_days", 3)), int(b.get("max_per_week", 2)),
                int(b.get("priority", 0)), b.get("enabled", True), rid,
            ))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@engagement_bp.route("/api/comm/rules/<int:rid>", methods=["DELETE"])
def api_comm_rules_delete(rid):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM comm_rules WHERE id = %s", (rid,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Rotas — Motor de Régua (evaluate, dispatch, callback, queue, log)
# ---------------------------------------------------------------------------

@engagement_bp.route("/api/comm/evaluate", methods=["POST"])
def api_comm_evaluate():
    """Manually trigger the engagement evaluation."""
    def _run():
        with current_app._get_current_object().app_context():
            evaluate_comm_triggers()
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Avaliação iniciada em background"})


@engagement_bp.route("/api/comm/queue")
def api_comm_queue():
    status = request.args.get("status", "").strip()
    limit = int(request.args.get("limit", 50))
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if status:
                cur.execute("""
                    SELECT q.*, r.name as rule_name FROM comm_queue q
                    LEFT JOIN comm_rules r ON r.id = q.rule_id
                    WHERE q.status = %s ORDER BY q.created_at DESC LIMIT %s
                """, (status, limit))
            else:
                cur.execute("""
                    SELECT q.*, r.name as rule_name FROM comm_queue q
                    LEFT JOIN comm_rules r ON r.id = q.rule_id
                    ORDER BY q.created_at DESC LIMIT %s
                """, (limit,))
            items = cur.fetchall()
            for i in items:
                i["created_at"] = to_brt(i["created_at"])
                i["sent_at"] = to_brt(i["sent_at"])
                i["scheduled_for"] = to_brt(i["scheduled_for"])
        return jsonify({"queue": items})
    finally:
        conn.close()


@engagement_bp.route("/api/comm/log")
def api_comm_log_list():
    limit = int(request.args.get("limit", 50))
    channel = request.args.get("channel", "").strip()
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if channel:
                cur.execute("""
                    SELECT l.*, r.name as rule_name FROM comm_log l
                    LEFT JOIN comm_rules r ON r.id = l.rule_id
                    WHERE l.channel = %s ORDER BY l.sent_at DESC LIMIT %s
                """, (channel, limit))
            else:
                cur.execute("""
                    SELECT l.*, r.name as rule_name FROM comm_log l
                    LEFT JOIN comm_rules r ON r.id = l.rule_id
                    ORDER BY l.sent_at DESC LIMIT %s
                """, (limit,))
            items = cur.fetchall()
            for i in items:
                i["sent_at"] = to_brt(i["sent_at"])
        return jsonify({"log": items})
    finally:
        conn.close()


@engagement_bp.route("/api/comm/callback", methods=["POST"])
def api_comm_callback():
    """Receive delivery status from n8n."""
    b = request.json or {}
    rgm = b.get("rgm", "")
    status = b.get("status", "")
    queue_id = b.get("queue_id")

    if not rgm and not queue_id:
        return jsonify({"error": "rgm or queue_id required"}), 400

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if queue_id:
                cur.execute("UPDATE comm_queue SET status=%s WHERE id=%s", (status, queue_id))
            if rgm and status:
                cur.execute("""
                    UPDATE comm_log SET status=%s
                    WHERE rgm=%s AND id = (
                        SELECT id FROM comm_log WHERE rgm=%s ORDER BY sent_at DESC LIMIT 1
                    )
                """, (status, rgm, rgm))

            if status == "respondido" and rgm:
                cur.execute("""
                    UPDATE comm_queue SET status='cancelado'
                    WHERE rgm=%s AND status='pendente'
                """, (rgm,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@engagement_bp.route("/api/comm/dispatch", methods=["POST"])
def api_comm_dispatch_manual():
    """Manually dispatch pending communications."""
    try:
        _dispatch_pending_comms()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
