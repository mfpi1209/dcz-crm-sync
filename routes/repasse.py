import os
import re
import logging

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, session

logger = logging.getLogger(__name__)

repasse_bp = Blueprint("repasse", __name__)

DB_DSN = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    dbname=os.getenv("DB_NAME", "dcz_sync"),
)

KOMMO_DB_DSN = dict(
    host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
    port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
    user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
    password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
    dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
)


def _pg():
    return psycopg2.connect(**DB_DSN)


def _pg_kommo():
    return psycopg2.connect(**KOMMO_DB_DSN)


def _normalize_rgm(val):
    if not val:
        return None
    digits = re.sub(r"\D", "", str(val))
    return digits if digits else None


def _is_admin():
    return session.get("role") == "admin"


def _require_login():
    # user_id pode ser 0 no login de emergência (APP_USER/APP_PASS em auth.py)
    return bool(session.get("authenticated")) and session.get("user_id") is not None


def _get_kommo_uid():
    """Retorna o kommo_user_id do usuário logado."""
    uid = session.get("user_id", 0)
    if not uid:
        return None
    try:
        conn = _pg()
        with conn.cursor() as cur:
            cur.execute("SELECT kommo_user_id FROM app_users WHERE id = %s", (uid,))
            row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def _resolve_kommo_uid(args_uid=None):
    """Admin pode passar ?kommo_uid=X. Viewer sempre recebe o próprio."""
    if _is_admin() and args_uid:
        try:
            return int(args_uid)
        except (ValueError, TypeError):
            pass
    return _get_kommo_uid()


# ---------------------------------------------------------------------------
# GET /api/repasse/taxa — retorna taxa atual
# PUT /api/repasse/taxa — admin salva nova taxa
# ---------------------------------------------------------------------------
@repasse_bp.route("/api/repasse/taxa", methods=["GET"])
def api_repasse_taxa_get():
    if not _require_login():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT valor FROM app_config WHERE chave = 'taxa_repasse'")
        row = cur.fetchone()
        cur.close(); conn.close()
        taxa = float(row[0]) if row else 30.0
        return jsonify({"taxa": taxa})
    except Exception as e:
        logger.error("api_repasse_taxa_get: %s", e)
        return jsonify({"taxa": 30.0})


@repasse_bp.route("/api/repasse/taxa", methods=["PUT"])
def api_repasse_taxa_put():
    if not _require_login():
        return jsonify({"error": "Sem permissão"}), 403
    if not _is_admin():
        return jsonify({"error": "Apenas admin pode alterar a taxa"}), 403
    body = request.get_json(silent=True) or {}
    try:
        taxa = float(body.get("taxa", 30))
        if taxa < 0 or taxa > 100:
            return jsonify({"error": "Taxa inválida"}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "Taxa inválida"}), 400
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO app_config (chave, valor, atualizado_em)
            VALUES ('taxa_repasse', %s, NOW())
            ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
        """, (str(taxa),))
        conn.commit()
        cur.close(); conn.close()
        return jsonify({"ok": True, "taxa": taxa})
    except Exception as e:
        logger.error("api_repasse_taxa_put: %s", e)
        return jsonify({"error": str(e)}), 500


# GET /api/repasse/filtros — meses e ciclos disponíveis
# ---------------------------------------------------------------------------
@repasse_bp.route("/api/repasse/filtros")
def api_repasse_filtros():
    if not _require_login():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT ciclo
            FROM comercial_pagamentos
            WHERE ciclo IS NOT NULL AND ciclo != ''
            ORDER BY ciclo DESC
        """)
        ciclos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT tipo_pagamento
            FROM comercial_pagamentos
            WHERE tipo_pagamento IS NOT NULL AND tipo_pagamento != ''
            ORDER BY tipo_pagamento
        """)
        tipos = [r[0] for r in cur.fetchall()]

        # Turmas agrupadas por ciclo para filtro dinâmico
        cur.execute("""
            SELECT ciclo, turma
            FROM comercial_pagamentos
            WHERE ciclo IS NOT NULL AND ciclo != ''
              AND turma IS NOT NULL AND turma != ''
            GROUP BY ciclo, turma
            ORDER BY ciclo, turma
        """)
        turmas_por_ciclo = {}
        for ciclo_val, turma_val in cur.fetchall():
            turmas_por_ciclo.setdefault(ciclo_val, [])
            if turma_val not in turmas_por_ciclo[ciclo_val]:
                turmas_por_ciclo[ciclo_val].append(turma_val)

        cur.close()
        conn.close()
        return jsonify({"ok": True, "ciclos": ciclos, "tipos": tipos, "turmas_por_ciclo": turmas_por_ciclo})
    except Exception as e:
        logger.error("repasse filtros error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# GET /api/repasse/agentes — totais por agente para o mês/ciclo selecionado
# ---------------------------------------------------------------------------
@repasse_bp.route("/api/repasse/agentes")
def api_repasse_agentes():
    if not _require_login():
        return jsonify({"error": "Sem permissão"}), 403

    ciclo    = request.args.get("ciclo", "")
    tipo     = request.args.get("tipo", "")
    turma    = request.args.get("turma", "")
    is_admin = _is_admin()
    # Viewer: força os dados para o próprio agente
    viewer_uid = None if is_admin else _get_kommo_uid()

    try:
        # ── 1. Carrega mapeamento rgm→agente do Kommo ─────────────────────
        kconn = _pg_kommo()
        kcur = kconn.cursor()

        if viewer_uid:
            # Viewer: busca apenas os RGMs do próprio agente
            kcur.execute("""
                SELECT DISTINCT ON (rgm_norm)
                    regexp_replace(COALESCE(v.rgm, ''), '[^0-9]', '', 'g') AS rgm_norm,
                    l.responsible_user_id,
                    COALESCE(u.name, 'Sem nome') AS agent_name
                FROM vw_leads_rgm v
                JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                LEFT JOIN users u ON u.id = l.responsible_user_id
                WHERE v.rgm IS NOT NULL AND v.rgm != ''
                  AND l.responsible_user_id = %s
                ORDER BY rgm_norm, l.id DESC
            """, (viewer_uid,))
        else:
            # Admin: carrega todos os mapeamentos
            kcur.execute("""
                SELECT DISTINCT ON (rgm_norm)
                    regexp_replace(COALESCE(v.rgm, ''), '[^0-9]', '', 'g') AS rgm_norm,
                    l.responsible_user_id,
                    COALESCE(u.name, 'Sem nome') AS agent_name
                FROM vw_leads_rgm v
                JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                LEFT JOIN users u ON u.id = l.responsible_user_id
                WHERE v.rgm IS NOT NULL AND v.rgm != ''
                ORDER BY rgm_norm, l.id DESC
            """)

        rgm_agent = {}
        for rgm_norm, uid, aname in kcur.fetchall():
            if rgm_norm and rgm_norm not in rgm_agent:
                rgm_agent[rgm_norm] = (uid, aname)

        kcur.close()
        kconn.close()

        # ── 2. Busca pagamentos — viewer filtrado pelos próprios RGMs ─────
        conn = _pg()
        cur = conn.cursor()

        wheres = []
        params = []
        if ciclo:
            wheres.append("ciclo = %s")
            params.append(ciclo)
        if tipo:
            wheres.append("tipo_pagamento ILIKE %s")
            params.append(tipo)
        if turma:
            wheres.append("turma ILIKE %s")
            params.append(turma)

        # Viewer: limita aos RGMs mapeados ao seu agente
        if viewer_uid and rgm_agent:
            wheres.append("regexp_replace(COALESCE(rgm, ''), '[^0-9]', '', 'g') = ANY(%s)")
            params.append(list(rgm_agent.keys()))
        elif viewer_uid and not rgm_agent:
            # Agente sem RGMs mapeados — retorna vazio
            cur.close(); conn.close()
            return jsonify({"ok": True, "is_admin": is_admin, "agentes": [],
                            "totais": {"valor": 0, "alunos": 0}, "agentes_count": 0})

        w = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        cur.execute(f"""
            SELECT rgm, valor_pago
            FROM comercial_pagamentos
            {w}
        """, params)

        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Totais por RGM
        all_rgms = set()
        total_valor = 0.0
        rgm_valor = {}
        for rgm_raw, valor in rows:
            rgm = _normalize_rgm(rgm_raw)
            if not rgm:
                continue
            all_rgms.add(rgm)
            v = float(valor or 0)
            total_valor += v
            rgm_valor[rgm] = rgm_valor.get(rgm, 0.0) + v

        if not all_rgms:
            return jsonify({
                "ok": True,
                "agentes": [],
                "totais": {"valor": 0, "alunos": 0},
            })

        # ── 3. Agrega por agente (ignora RGMs sem mapeamento) ─────────────
        agent_data = {}

        for rgm, valor in rgm_valor.items():
            if rgm not in rgm_agent:
                continue  # ignora RGMs sem agente identificado
            uid, aname = rgm_agent[rgm]
            key = uid or aname
            if key not in agent_data:
                agent_data[key] = {
                    "id": uid,
                    "nome": aname,
                    "qtd_alunos": 0,
                    "total_valor": 0.0,
                }
            agent_data[key]["qtd_alunos"] += 1
            agent_data[key]["total_valor"] += valor

        agentes = sorted(agent_data.values(), key=lambda x: x["total_valor"], reverse=True)

        # Formata valores
        for a in agentes:
            a["total_valor"] = round(a["total_valor"], 2)
            a["media_por_aluno"] = round(a["total_valor"] / a["qtd_alunos"], 2) if a["qtd_alunos"] else 0

        # Total recalculado apenas sobre RGMs mapeados
        total_mapeado = sum(a["total_valor"] for a in agentes)
        alunos_mapeados = sum(a["qtd_alunos"] for a in agentes)

        return jsonify({
            "ok": True,
            "is_admin": is_admin,
            "agentes": agentes,
            "totais": {
                "valor": round(total_mapeado, 2),
                "alunos": alunos_mapeados,
                "agentes": len(agent_data),
            },
        })

    except Exception as e:
        logger.error("repasse agentes error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# GET /api/repasse/detalhe — alunos de um agente específico
# ---------------------------------------------------------------------------
@repasse_bp.route("/api/repasse/detalhe")
def api_repasse_detalhe():
    if not _require_login():
        return jsonify({"error": "Sem permissão"}), 403

    ciclo     = request.args.get("ciclo", "")
    tipo      = request.args.get("tipo", "")
    turma     = request.args.get("turma", "")
    kommo_uid = request.args.get("kommo_uid")

    try:
        # RGMs do agente via Kommo
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        if kommo_uid:
            kcur.execute("""
                SELECT DISTINCT regexp_replace(COALESCE(v.rgm, ''), '[^0-9]', '', 'g')
                FROM vw_leads_rgm v
                JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                WHERE l.responsible_user_id = %s
                  AND v.rgm IS NOT NULL
            """, (int(kommo_uid),))
            agent_rgms = {r[0] for r in kcur.fetchall() if r[0]}
        else:
            agent_rgms = None  # sem agente

        kcur.close()
        kconn.close()

        # Busca recebimentos
        conn = _pg()
        cur = conn.cursor()
        wheres = []
        params = []
        if ciclo:
            wheres.append("ciclo = %s")
            params.append(ciclo)
        if tipo:
            wheres.append("tipo_pagamento ILIKE %s")
            params.append(tipo)
        if turma:
            wheres.append("turma ILIKE %s")
            params.append(turma)

        w = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        cur.execute(f"""
            SELECT rgm, valor_pago, tipo_pagamento, turma, ciclo
            FROM comercial_pagamentos
            {w}
            ORDER BY valor_pago DESC
        """, params)

        alunos = []
        seen = set()
        for rgm_raw, valor, tipo_pag, turma, ciclo_val in cur.fetchall():
            rgm = _normalize_rgm(rgm_raw)
            if not rgm or rgm in seen:
                continue
            # Filtra pelo agente
            if agent_rgms is not None and rgm not in agent_rgms:
                continue
            seen.add(rgm)
            alunos.append({
                "rgm": rgm,
                "valor": round(float(valor or 0), 2),
                "tipo_pagamento": tipo_pag or "",
                "turma": turma or "",
                "ciclo": ciclo_val or "",
            })

        cur.close()
        conn.close()
        return jsonify({"ok": True, "alunos": alunos, "total": sum(a["valor"] for a in alunos)})
    except Exception as e:
        logger.error("repasse detalhe error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500
