import os
import re
import logging
from datetime import date

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, session, Response

from helpers import normalize_polo_display

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

# Upload em Premiação grava em comercial_recebimentos. comercial_pagamentos é a
# cópia de 20/03/2026 desse histórico: o mesmo RGM e o mesmo valor já estão no
# recebimento. O card do consultor ainda lê as duas. A planilha exportada não,
# senão as 15 mil linhas antigas saem de novo.
_REPASSE_FONT = """
(
    SELECT rgm,
           valor_pago::double precision AS valor_pago,
           turma,
           tipo_pagamento,
           ciclo
    FROM comercial_pagamentos
    UNION ALL
    SELECT rgm,
           valor::double precision AS valor_pago,
           turma,
           tipo_pagamento,
           ciclo
    FROM comercial_recebimentos
) AS repasse_fonte
"""

_PLANILHA_FONT = """
(
    SELECT rgm,
           valor::double precision AS valor_pago,
           turma,
           tipo_pagamento,
           ciclo
    FROM comercial_recebimentos
) AS repasse_fonte
"""


def _ciclo_repasse_valido(val):
    """Exclui N/A, #N/A e vazios dos filtros de ciclo/turma."""
    if val is None:
        return False
    s = str(val).strip()
    if not s:
        return False
    n = s.lower().replace("#", "").replace(" ", "").replace(".", "")
    return n not in ("n/a", "na", "null", "-")


# Venda com matrícula até 30/09/2026 fica no responsável do Kommo.
# De 01/10/2026 em diante o valor vai para o owner_id do negócio no Bwipo.
_CORTE_REPASSE = date(2026, 10, 1)


def _rgms_venda_nova(cur):
    """RGMs cuja matrícula (SIAA, tabela do painel) é de 01/10/2026 em diante."""
    cur.execute("SELECT to_regclass('public.bwipo_painel_base')")
    if not cur.fetchone()[0]:
        return set()
    cur.execute(
        """
        SELECT rgm
        FROM bwipo_painel_base
        WHERE data_matricula >= %s
          AND rgm IS NOT NULL AND rgm <> ''
        """,
        (_CORTE_REPASSE,),
    )
    return {r[0] for r in cur.fetchall() if r[0]}


def _owners_bwipo(cur, rgms):
    """RGM → (owner_id, nome) no Pipeline Principal. Um negócio por RGM."""
    if not rgms:
        return {}
    cur.execute(
        """
        SELECT DISTINCT ON (d.rgm_norm)
               d.rgm_norm,
               d.owner_id,
               COALESCE(NULLIF(d.owner_name, ''), 'Sem nome')
        FROM bwipo_deals d
        LEFT JOIN bwipo_stages s ON s.id = d.stage_id
        WHERE NOT d.is_deleted
          AND d.rgm_norm = ANY(%s)
          AND COALESCE(d.owner_id, '') <> ''
          AND (
                position('principal' in lower(COALESCE(d.pipeline_name, ''))) > 0
                OR COALESCE(d.pipeline_name, '') = ''
              )
        ORDER BY d.rgm_norm,
                 CASE WHEN s.is_won THEN 0 ELSE 1 END,
                 d.updated_at DESC NULLS LAST
        """,
        (list(rgms),),
    )
    return {rgm: (owner_id, nome) for rgm, owner_id, nome in cur.fetchall()}


def _depara_owner(cur):
    """owner_id do Bwipo → (kommo_user_id, nome). Só junta o card da mesma pessoa."""
    cur.execute("SELECT to_regclass('public.bwipo_kommo_user_depara')")
    if not cur.fetchone()[0]:
        return {}
    cur.execute(
        """
        SELECT bwipo_user_id, kommo_user_id, COALESCE(kommo_name, '')
        FROM bwipo_kommo_user_depara
        """
    )
    out = {}
    for bwipo_id, kommo_uid, nome in cur.fetchall():
        if bwipo_id and kommo_uid is not None:
            out[bwipo_id] = (int(kommo_uid), nome)
    return out


def _dono_repasse(rgm, novas, kommo_map, bwipo_map):
    """Dono de uma venda. Uma fonte só: Bwipo se a matrícula é de 01/10 em diante, senão Kommo."""
    if rgm in novas:
        hit = bwipo_map.get(rgm)
        if not hit:
            return None
        owner_id, owner_name = hit
        return owner_id, owner_name, owner_id
    hit = kommo_map.get(rgm)
    if not hit:
        return None
    uid, nome = hit
    return (uid or nome), nome, None


def _owner_ids_do_usuario(depara, kommo_uid):
    if kommo_uid is None:
        return set()
    return {bwipo_id for bwipo_id, (uid, _nome) in depara.items() if uid == kommo_uid}


def _repasse_filtros_sql():
    """Filtros da tela (ciclo, turma, tipo) mais busca por RGM."""
    ciclo = request.args.get("ciclo", "")
    tipo = request.args.get("tipo", "")
    turma = request.args.get("turma", "")
    q = re.sub(r"\D", "", request.args.get("q") or "")
    wheres, params = [], []
    if ciclo:
        wheres.append("ciclo = %s")
        params.append(ciclo)
    _append_filtro_tipo(wheres, params, tipo)
    if turma:
        wheres.append("turma ILIKE %s")
        params.append(turma)
    if q:
        wheres.append("regexp_replace(COALESCE(rgm, ''), '[^0-9]', '', 'g') LIKE %s")
        params.append(f"%{q}%")
    where = ("WHERE " + " AND ".join(wheres)) if wheres else ""
    return where, params


def _fmt_valor_planilha(valor):
    return f"{float(valor or 0):.2f}".replace(".", ",")


def _fmt_data_planilha(valor):
    if not valor:
        return ""
    return valor.strftime("%d/%m/%Y")


def _append_filtro_tipo(wheres, params, tipo):
    """Repasse: UI só oferece Mensalidade — filtra qualquer variação no texto."""
    if not (tipo or "").strip():
        return
    if tipo.strip().lower() == "mensalidade":
        wheres.append("tipo_pagamento ILIKE %s")
        params.append("%mensalidade%")
    else:
        wheres.append("tipo_pagamento ILIKE %s")
        params.append(tipo)


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
        taxa = float(row[0]) if row else 20.0
        return jsonify({"taxa": taxa})
    except Exception as e:
        logger.error("api_repasse_taxa_get: %s", e)
        return jsonify({"taxa": 20.0})


@repasse_bp.route("/api/repasse/taxa", methods=["PUT"])
def api_repasse_taxa_put():
    if not _require_login():
        return jsonify({"error": "Sem permissão"}), 403
    if not _is_admin():
        return jsonify({"error": "Apenas admin pode alterar a taxa"}), 403
    body = request.get_json(silent=True) or {}
    try:
        taxa = float(body.get("taxa", 20))
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

        cur.execute(f"""
            SELECT DISTINCT ciclo
            FROM {_REPASSE_FONT}
            WHERE ciclo IS NOT NULL AND ciclo != ''
            ORDER BY ciclo DESC
        """)
        ciclos = [r[0] for r in cur.fetchall() if _ciclo_repasse_valido(r[0])]

        # Só "Mensalidade" no filtro (variações no banco são tratadas no ILIKE ao buscar)
        tipos = ["Mensalidade"]

        # Turmas agrupadas por ciclo para filtro dinâmico
        cur.execute(f"""
            SELECT ciclo, turma
            FROM {_REPASSE_FONT}
            WHERE ciclo IS NOT NULL AND ciclo != ''
              AND turma IS NOT NULL AND turma != ''
            GROUP BY ciclo, turma
            ORDER BY ciclo, turma
        """)
        turmas_por_ciclo = {}
        for ciclo_val, turma_val in cur.fetchall():
            if not _ciclo_repasse_valido(ciclo_val):
                continue
            if not _ciclo_repasse_valido(turma_val):
                continue
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

        # ── 2. Vendas de 01/10/2026 em diante: dono = owner_id do Bwipo ──
        conn = _pg()
        cur = conn.cursor()
        novas = _rgms_venda_nova(cur)
        bwipo_map = _owners_bwipo(cur, novas)
        depara = _depara_owner(cur)

        wheres = []
        params = []
        if ciclo:
            wheres.append("ciclo = %s")
            params.append(ciclo)
        _append_filtro_tipo(wheres, params, tipo)
        if turma:
            wheres.append("turma ILIKE %s")
            params.append(turma)

        # Viewer: RGMs antigos dele no Kommo, mais os novos em que ele é o owner.
        if viewer_uid:
            meus = set()
            for rgm in rgm_agent:
                if rgm not in novas:
                    meus.add(rgm)
            meus_owners = _owner_ids_do_usuario(depara, viewer_uid)
            for rgm, (owner_id, _nome) in bwipo_map.items():
                if owner_id in meus_owners:
                    meus.add(rgm)
            if not meus:
                cur.close(); conn.close()
                return jsonify({"ok": True, "is_admin": is_admin, "agentes": [],
                                "totais": {"valor": 0, "alunos": 0}, "agentes_count": 0})
            wheres.append("regexp_replace(COALESCE(rgm, ''), '[^0-9]', '', 'g') = ANY(%s)")
            params.append(list(meus))

        w = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        cur.execute(f"""
            SELECT rgm, valor_pago
            FROM {_REPASSE_FONT}
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

        # ── 3. Agrega por agente. Uma fonte por venda, conforme a data. ──
        agent_data = {}
        meus_owners = _owner_ids_do_usuario(depara, viewer_uid) if viewer_uid else set()

        for rgm, valor in rgm_valor.items():
            dono = _dono_repasse(rgm, novas, rgm_agent, bwipo_map)
            if not dono:
                continue
            key, aname, owner_id = dono
            if viewer_uid and str(key) != str(viewer_uid) and owner_id not in meus_owners:
                continue
            if key not in agent_data:
                agent_data[key] = {
                    "id": key,
                    "nome": aname,
                    "owner_id": owner_id,
                    "qtd_alunos": 0,
                    "total_valor": 0.0,
                }
            elif owner_id and not agent_data[key].get("owner_id"):
                agent_data[key]["owner_id"] = owner_id
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
        conn = _pg()
        cur = conn.cursor()
        novas = _rgms_venda_nova(cur)
        bwipo_map = _owners_bwipo(cur, novas)
        depara = _depara_owner(cur)
        cur.close()
        conn.close()

        if not _is_admin():
            meu = _get_kommo_uid()
            owners = _owner_ids_do_usuario(depara, meu)
            if not kommo_uid or (str(kommo_uid) != str(meu) and str(kommo_uid) not in owners):
                return jsonify({"error": "Sem permissão"}), 403

        agent_rgms = set()
        if kommo_uid:
            pedido = str(kommo_uid)
            try:
                kommo_int = int(pedido)
            except (TypeError, ValueError):
                kommo_int = None

            # Id numérico é consultor do Kommo: só venda anterior a 01/10.
            if kommo_int is not None:
                kconn = _pg_kommo()
                kcur = kconn.cursor()
                kcur.execute("""
                    SELECT DISTINCT regexp_replace(COALESCE(v.rgm, ''), '[^0-9]', '', 'g')
                    FROM vw_leads_rgm v
                    JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
                    WHERE l.responsible_user_id = %s
                      AND v.rgm IS NOT NULL
                """, (kommo_int,))
                agent_rgms = {r[0] for r in kcur.fetchall() if r[0] and r[0] not in novas}
                kcur.close()
                kconn.close()

            for rgm, (owner_id, _nome) in bwipo_map.items():
                if rgm in novas and str(owner_id) == pedido:
                    agent_rgms.add(rgm)
        else:
            agent_rgms = None

        # Busca recebimentos
        conn = _pg()
        cur = conn.cursor()
        wheres = []
        params = []
        if ciclo:
            wheres.append("ciclo = %s")
            params.append(ciclo)
        _append_filtro_tipo(wheres, params, tipo)
        if turma:
            wheres.append("turma ILIKE %s")
            params.append(turma)

        w = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        cur.execute(f"""
            SELECT rgm, valor_pago, tipo_pagamento, turma, ciclo
            FROM {_REPASSE_FONT}
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


_PLANILHA_PAGE = 50


def _planilha_rows(cur, where, params, limit=None, offset=0):
    lim = ""
    qparams = list(params)
    if limit is not None:
        lim = " LIMIT %s OFFSET %s"
        qparams.extend([limit, offset])
    cur.execute(
        f"""
        SELECT regexp_replace(COALESCE(rgm, ''), '[^0-9]', '', 'g') AS rgm,
               valor_pago,
               COALESCE(turma, '') AS turma,
               COALESCE(tipo_pagamento, '') AS beleza,
               COALESCE(ciclo, '') AS ciclo
        FROM {_PLANILHA_FONT}
        {where}
        ORDER BY rgm, valor_pago DESC
        {lim}
        """,
        qparams,
    )
    return cur.fetchall()


@repasse_bp.route("/api/repasse/planilha")
def api_repasse_planilha():
    """Tabela crua de recebimentos, só admin. Mesmas colunas da planilha."""
    if not _require_login() or not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        page = int(request.args.get("page") or 1)
    except (TypeError, ValueError):
        page = 1
    page = max(1, page)
    where, params = _repasse_filtros_sql()
    try:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*), COALESCE(SUM(valor_pago), 0) FROM {_PLANILHA_FONT} {where}",
            params,
        )
        total, valor = cur.fetchone()
        total = int(total or 0)
        pages = max(1, (total + _PLANILHA_PAGE - 1) // _PLANILHA_PAGE)
        if page > pages:
            page = pages
        offset = (page - 1) * _PLANILHA_PAGE
        rows = _planilha_rows(cur, where, params, _PLANILHA_PAGE, offset)
        cur.close()
        conn.close()
        return jsonify({
            "ok": True,
            "page": page,
            "pages": pages,
            "total": total,
            "valor": round(float(valor or 0), 2),
            "rows": [
                {
                    "rgm": rgm or "",
                    "valor": round(float(v or 0), 2),
                    "turma": turma or "",
                    "beleza": beleza or "",
                    "ciclo": ciclo or "",
                }
                for rgm, v, turma, beleza, ciclo in rows
            ],
        })
    except Exception as e:
        logger.error("repasse planilha error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@repasse_bp.route("/api/repasse/planilha.csv")
def api_repasse_planilha_csv():
    """CSV no formato da planilha: RGM;Valor Pago;Turma;Beleza;ciclo."""
    if not _require_login() or not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    where, params = _repasse_filtros_sql()
    try:
        conn = _pg()
        cur = conn.cursor()
        rows = _planilha_rows(cur, where, params)
        cur.close()
        conn.close()
        linhas = ["RGM;Valor Pago;Turma;Beleza;ciclo"]
        for rgm, valor, turma, beleza, ciclo in rows:
            linhas.append(";".join([
                rgm or "",
                _fmt_valor_planilha(valor),
                (turma or "").replace(";", " "),
                (beleza or "").replace(";", " "),
                (ciclo or "").replace(";", " "),
            ]))
        corpo = "\ufeff" + "\n".join(linhas)
        return Response(
            corpo,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=repasse_recebimentos.csv"},
        )
    except Exception as e:
        logger.error("repasse planilha csv error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


_MESES_PT = (
    "", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
)


def _ciclo_mais_recente(cur):
    """Ciclo mais novo que ainda está na base. A base lê todos os snapshots, não só o arquivo do dia."""
    cur.execute(
        """
        SELECT ciclo
        FROM bwipo_painel_base
        WHERE ciclo IS NOT NULL AND ciclo <> ''
        GROUP BY ciclo
        ORDER BY ciclo DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    return row[0] if row else ""


def _ingressantes_filtros(ciclo_padrao=""):
    """Recorte da lista de ingressantes. Ciclo sempre entra: sem escolha, vale o mais recente."""
    tipo = (request.args.get("tipo_mat") or "NOVA MATRICULA").strip()
    pago = (request.args.get("pago") or "").strip().lower()
    mes = (request.args.get("mes") or "").strip()
    dia = (request.args.get("dia") or "").strip()
    ciclo = (request.args.get("ciclo_mat") or "").strip() or ciclo_padrao
    nivel = (request.args.get("nivel") or "").strip()
    q = re.sub(r"\D", "", request.args.get("q") or "")
    wheres = ["b.rgm IS NOT NULL", "b.rgm <> ''"]
    params = []
    if tipo and tipo.lower() != "todos":
        wheres.append("b.tipo_matricula = %s")
        params.append(tipo)
    if ciclo:
        wheres.append("b.ciclo = %s")
        params.append(ciclo)
    if mes:
        wheres.append("to_char(b.data_matricula, 'YYYY-MM') = %s")
        params.append(mes)
    if dia:
        wheres.append("b.data_matricula = %s")
        params.append(dia)
    if nivel:
        wheres.append("b.nivel = %s")
        params.append(nivel)
    if q:
        wheres.append("b.rgm LIKE %s")
        params.append(f"%{q}%")
    if pago == "sim":
        wheres.append("p.rgm IS NOT NULL")
    elif pago == "nao":
        wheres.append("p.rgm IS NULL")
    return " AND ".join(wheres), params, tipo, ciclo


def _ingressantes_from():
    return f"""
        FROM bwipo_painel_base b
        LEFT JOIN (
            SELECT regexp_replace(COALESCE(rgm, ''), '[^0-9]', '', 'g') AS rgm,
                   SUM(valor_pago) AS valor,
                   STRING_AGG(DISTINCT NULLIF(tipo_pagamento, ''), ', ') AS tipos
            FROM {_REPASSE_FONT}
            GROUP BY 1
        ) p ON p.rgm = b.rgm
        LEFT JOIN (
            SELECT DISTINCT ON (rgm) rgm, modalidade, turma
            FROM comercial_rgm
            WHERE rgm IS NOT NULL AND rgm <> ''
            ORDER BY rgm, id DESC
        ) c ON c.rgm = b.rgm
        LEFT JOIN LATERAL (
            SELECT tc.nome
            FROM turmas_comercial tc
            WHERE b.data_matricula BETWEEN tc.dt_inicio AND tc.dt_fim
            ORDER BY CASE WHEN tc.nivel = COALESCE(b.nivel, '') THEN 0 ELSE 1 END,
                     tc.dt_inicio DESC
            LIMIT 1
        ) t ON TRUE
    """


def _ingressantes_select():
    return """
        SELECT b.rgm,
               COALESCE(b.polo, '') AS polo,
               COALESCE(b.nivel, '') AS nivel,
               COALESCE(c.modalidade, '') AS modalidade,
               b.data_matricula,
               COALESCE(b.ciclo, '') AS ciclo,
               COALESCE(t.nome, NULLIF(c.turma, ''), '') AS turma,
               COALESCE(p.valor, 0) AS valor,
               COALESCE(p.tipos, '') AS tipo_pagamento,
               (p.rgm IS NOT NULL) AS pagou
    """


def _matriculados_linha(row):
    rgm, polo, nivel, modalidade, data_mat, ciclo, turma, valor, tipo, pagou = row
    return {
        "rgm": rgm or "",
        "polo": normalize_polo_display(polo or "") or (polo or ""),
        "nivel": nivel or "",
        "modalidade": modalidade or "",
        "data_matricula": _fmt_data_planilha(data_mat),
        "ciclo": ciclo or "",
        "turma": turma or "",
        "valor": round(float(valor or 0), 2),
        "tipo_pagamento": tipo or "",
        "pagou": bool(pagou),
    }


def _ingressantes_opcoes(cur, tipo, ciclo):
    where = "WHERE rgm IS NOT NULL AND rgm <> ''"
    params = []
    if tipo and tipo.lower() != "todos":
        where += " AND tipo_matricula = %s"
        params.append(tipo)
    mes_where = where
    mes_params = list(params)
    if ciclo:
        mes_where += " AND ciclo = %s"
        mes_params.append(ciclo)
    cur.execute(
        f"""
        SELECT DISTINCT ciclo
        FROM bwipo_painel_base
        {where} AND ciclo IS NOT NULL AND ciclo <> ''
        ORDER BY ciclo DESC
        """,
        params,
    )
    ciclos = [r[0] for r in cur.fetchall()]
    cur.execute(
        f"""
        SELECT DISTINCT nivel
        FROM bwipo_painel_base
        {where} AND nivel IS NOT NULL AND nivel <> ''
        ORDER BY nivel
        """,
        params,
    )
    niveis = [r[0] for r in cur.fetchall()]
    cur.execute(
        f"""
        SELECT DISTINCT to_char(data_matricula, 'YYYY-MM')
        FROM bwipo_painel_base
        {mes_where} AND data_matricula IS NOT NULL
        ORDER BY 1 DESC
        """,
        mes_params,
    )
    meses = []
    for (chave,) in cur.fetchall():
        if not chave or len(chave) < 7:
            continue
        ano, mes = chave[:4], int(chave[5:7])
        nome = _MESES_PT[mes] if 1 <= mes <= 12 else chave
        meses.append({"id": chave, "label": f"{nome}/{ano}"})
    return {"ciclos": ciclos, "niveis": niveis, "meses": meses, "ciclo": ciclo}


@repasse_bp.route("/api/repasse/matriculados")
def api_repasse_matriculados():
    """Ingressantes da base diária, pagos e não pagos."""
    if not _require_login() or not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        page = int(request.args.get("page") or 1)
    except (TypeError, ValueError):
        page = 1
    page = max(1, page)
    try:
        conn = _pg()
        cur = conn.cursor()
        where, params, tipo, ciclo = _ingressantes_filtros(_ciclo_mais_recente(cur))
        cur.execute(
            f"""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE p.rgm IS NOT NULL),
                   COALESCE(SUM(p.valor), 0)
            {_ingressantes_from()}
            WHERE {where}
            """,
            params,
        )
        total, pagos, valor = cur.fetchone()
        total = int(total or 0)
        pages = max(1, (total + _PLANILHA_PAGE - 1) // _PLANILHA_PAGE)
        if page > pages:
            page = pages
        offset = (page - 1) * _PLANILHA_PAGE
        cur.execute(
            f"""
            {_ingressantes_select()}
            {_ingressantes_from()}
            WHERE {where}
            ORDER BY b.data_matricula DESC NULLS LAST, b.rgm
            LIMIT %s OFFSET %s
            """,
            list(params) + [_PLANILHA_PAGE, offset],
        )
        rows = cur.fetchall()
        opcoes = _ingressantes_opcoes(cur, tipo, ciclo)
        cur.close()
        conn.close()
        return jsonify({
            "ok": True,
            "page": page,
            "pages": pages,
            "total": total,
            "pagos": int(pagos or 0),
            "valor": round(float(valor or 0), 2),
            "filtros": opcoes,
            "rows": [_matriculados_linha(r) for r in rows],
        })
    except Exception as e:
        logger.error("repasse matriculados error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@repasse_bp.route("/api/repasse/matriculados.csv")
def api_repasse_matriculados_csv():
    if not _require_login() or not _is_admin():
        return jsonify({"error": "Sem permissão"}), 403
    try:
        conn = _pg()
        cur = conn.cursor()
        where, params, _tipo, _ciclo = _ingressantes_filtros(_ciclo_mais_recente(cur))
        cur.execute(
            f"""
            {_ingressantes_select()}
            {_ingressantes_from()}
            WHERE {where}
            ORDER BY b.data_matricula DESC NULLS LAST, b.rgm
            """,
            params,
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        linhas = ["RGM;Polo;Nível;Modalidade;Data de Matrícula;Ciclo;Turma;Pagou;Valor Pago;Tipo de Pagamento"]
        for item in (_matriculados_linha(r) for r in rows):
            linhas.append(";".join([
                item["rgm"],
                item["polo"].replace(";", " "),
                item["nivel"].replace(";", " "),
                item["modalidade"].replace(";", " "),
                item["data_matricula"],
                item["ciclo"].replace(";", " "),
                item["turma"].replace(";", " "),
                "Sim" if item["pagou"] else "Não",
                _fmt_valor_planilha(item["valor"]) if item["pagou"] else "",
                item["tipo_pagamento"].replace(";", " "),
            ]))
        corpo = "\ufeff" + "\n".join(linhas)
        return Response(
            corpo,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=repasse_ingressantes.csv"},
        )
    except Exception as e:
        logger.error("repasse matriculados csv error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500
