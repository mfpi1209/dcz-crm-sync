"""
Premiacoes Internas — workflow de aprovacao de premiacoes discricionarias.

Duas paginas independentes controladas por permissao:
- `premiacoes_internas`  — Gestor cria/edita/envia lotes.
- `aprovacao_premiacoes` — Aprovador decide (aprova / reprova / solicita ajuste).

Notificacoes: reaproveitam a tabela `avisos` via helpers em `helpers.py`.
"""
from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

import psycopg2
import psycopg2.extras
from flask import Blueprint, request, jsonify, session

from db import DB_DSN
from helpers import criar_aviso_para_usuarios, criar_aviso_por_permissao

premiacoes_internas_bp = Blueprint("premiacoes_internas_bp", __name__)


# ---------------------------------------------------------------------------
# Helpers de conexao / auth
# ---------------------------------------------------------------------------

PAGE_GESTOR = "premiacoes_internas"
PAGE_APROVADOR = "aprovacao_premiacoes"

STATUS_RASCUNHO = "rascunho"
STATUS_AGUARDANDO = "aguardando_aprovacao"
STATUS_APROVADO = "aprovado"
STATUS_REPROVADO = "reprovado"
STATUS_AJUSTE = "ajuste_solicitado"

STATUS_EDITAVEIS = {STATUS_RASCUNHO, STATUS_AJUSTE}

# Setores válidos — dropdown fechado no front + whitelist server-side.
# Manter em sincronia com PI_SETORES em premiacoes_internas.js / aprovacao_premiacoes.js.
SETORES_VALIDOS = ("Acadêmico", "Comercial", "TI", "Marketing")


def _get_conn():
    return psycopg2.connect(**DB_DSN)


def _session_user():
    """Retorna (uid, username, role) do usuario logado; ou (None, None, None)."""
    if not session.get("authenticated"):
        return None, None, None
    return (
        session.get("user_id"),
        (session.get("username") or "").strip(),
        (session.get("role") or "").strip(),
    )


def _has_permission(page: str) -> tuple[bool, int, dict | None]:
    """Gate server-side por slug de pagina.

    Padrao: admin passa direto; caso contrario, consulta user_permissions.
    Retorna (ok, http_status, error_payload_or_None).
    """
    return _has_any_permission([page])


def _has_any_permission(pages: list[str]) -> tuple[bool, int, dict | None]:
    """Gate para quando qualquer uma das paginas listadas basta."""
    uid, _uname, role = _session_user()
    if uid is None:
        return False, 401, {"error": "Nao autenticado"}
    if role == "admin":
        return True, 200, None
    if not uid:
        return False, 403, {"error": "Usuario sem permissao para esta pagina"}
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM user_permissions WHERE user_id = %s AND page = ANY(%s) LIMIT 1",
                (uid, list(pages)),
            )
            if cur.fetchone():
                return True, 200, None
    finally:
        conn.close()
    return False, 403, {"error": "Usuario sem permissao para esta pagina"}


def _fetch_app_user(uid: int) -> dict | None:
    """Retorna { id, username, email, categoria } ou None."""
    if not uid:
        return None
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, username, email_cruzeiro AS email, categoria
                  FROM app_users
                 WHERE id = %s
                """,
                (uid,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def _resolve_user_name(uid: int) -> str:
    """Snapshot do username no momento da acao (auditoria)."""
    if not uid:
        return ""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT username FROM app_users WHERE id = %s", (uid,))
            row = cur.fetchone()
            return (row[0] if row else "") or ""
    finally:
        conn.close()


def _normalize_uid_for_fk(uid) -> int | None:
    """Retorna `uid` se existir em `app_users`, senao None.

    Motivo: admin logado pelo fallback (APP_USER/APP_PASS) tem uid=0, que nao
    existe em app_users. Persistir 0 em FK quebra a integridade referencial.
    O snapshot em `*_nome` continua registrando quem foi o autor.
    """
    try:
        uid_int = int(uid)
    except (TypeError, ValueError):
        return None
    if uid_int <= 0:
        return None
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM app_users WHERE id = %s", (uid_int,))
            return uid_int if cur.fetchone() else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Serializacao
# ---------------------------------------------------------------------------

def _iso(dt):
    return dt.isoformat() if dt else None


def _dec_to_float(v):
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _serialize_lote(row: dict) -> dict:
    return {
        "id": row["id"],
        "mes_referencia": row["mes_referencia"],
        "setor": row["setor"],
        "gestor_user_id": row["gestor_user_id"],
        "gestor_nome": row["gestor_nome"],
        "observacoes_gerais": row.get("observacoes_gerais") or "",
        "status": row["status"],
        "valor_total": _dec_to_float(row.get("valor_total")),
        "enviado_em": _iso(row.get("enviado_em")),
        "decidido_em": _iso(row.get("decidido_em")),
        "aprovador_user_id": row.get("aprovador_user_id"),
        "aprovador_nome": row.get("aprovador_nome") or "",
        "aprovador_justificativa": row.get("aprovador_justificativa") or "",
        "created_at": _iso(row.get("created_at")),
        "updated_at": _iso(row.get("updated_at")),
    }


def _serialize_colaborador(row: dict) -> dict:
    return {
        "id": row["id"],
        "app_user_id": row.get("app_user_id"),
        "nome": row["nome"],
        "email": row.get("email") or "",
        "cargo": row["cargo"],
        "setor": row["setor"],
        "valor": _dec_to_float(row.get("valor")),
        "justificativa": row.get("justificativa") or "",
        "observacoes": row.get("observacoes") or "",
        "is_auto_premiacao": bool(row.get("is_auto_premiacao")),
        "ordem": row.get("ordem", 0),
    }


def _serialize_evento(row: dict) -> dict:
    return {
        "id": row["id"],
        "tipo": row["tipo"],
        "status_anterior": row.get("status_anterior"),
        "status_novo": row.get("status_novo"),
        "autor_user_id": row["autor_user_id"],
        "autor_nome": row["autor_nome"],
        "justificativa": row.get("justificativa") or "",
        "created_at": _iso(row.get("created_at")),
    }


# ---------------------------------------------------------------------------
# Validacao de payload
# ---------------------------------------------------------------------------

def _parse_decimal(v, campo: str) -> Decimal:
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"Campo '{campo}' invalido")


def _validate_lote_payload(data: dict, *, submit: bool) -> tuple[dict, list[dict], list[str]]:
    """Valida payload de criacao/edicao de lote.

    Retorna (lote_fields, colaboradores_normalizados, erros).
    Quando submit=True, exige justificativa e demais campos obrigatorios.
    """
    errors: list[str] = []
    data = data or {}

    mes = (data.get("mes_referencia") or "").strip()
    setor = (data.get("setor") or "").strip()
    observacoes = (data.get("observacoes_gerais") or "").strip() or None
    colaboradores = data.get("colaboradores") or []

    if not mes:
        errors.append("Campo 'mes_referencia' obrigatorio (formato YYYY-MM).")
    elif len(mes) != 7 or mes[4] != "-":
        errors.append("Campo 'mes_referencia' deve estar no formato YYYY-MM.")

    if not setor:
        errors.append("Campo 'setor' obrigatorio.")
    elif setor not in SETORES_VALIDOS:
        errors.append(
            f"Setor '{setor}' invalido. Use um destes: {', '.join(SETORES_VALIDOS)}."
        )

    if not isinstance(colaboradores, list) or not colaboradores:
        errors.append("Adicione pelo menos um colaborador.")

    normalizados: list[dict] = []
    total = Decimal("0")

    for idx, c in enumerate(colaboradores or [], start=1):
        c = c or {}
        nome = (c.get("nome") or "").strip()
        cargo = (c.get("cargo") or "").strip()
        email = (c.get("email") or "").strip() or None
        c_setor = (c.get("setor") or "").strip() or setor
        justificativa = (c.get("justificativa") or "").strip()
        obs = (c.get("observacoes") or "").strip() or None
        is_auto = bool(c.get("is_auto_premiacao"))

        # app_user_id opcional — se vier, valida existencia e usa como
        # fonte canonica para nome/email (evita divergencia UI vs banco).
        app_user_id = c.get("app_user_id")
        if app_user_id in ("", 0, "0"):
            app_user_id = None
        if app_user_id is not None:
            try:
                app_user_id = int(app_user_id)
            except (TypeError, ValueError):
                errors.append(f"Colaborador #{idx}: app_user_id invalido.")
                app_user_id = None
        if app_user_id:
            db_user = _fetch_app_user(app_user_id)
            if not db_user:
                errors.append(f"Colaborador #{idx}: usuario nao encontrado no sistema.")
                app_user_id = None
            else:
                # snapshot canonico do server
                nome = nome or db_user["username"] or ""
                if not email:
                    email = db_user.get("email") or None

        if not nome:
            errors.append(f"Colaborador #{idx}: nome obrigatorio.")
        if not cargo:
            errors.append(f"Colaborador #{idx}: cargo obrigatorio.")
        if c_setor and c_setor not in SETORES_VALIDOS:
            errors.append(
                f"Colaborador #{idx}: setor '{c_setor}' invalido. "
                f"Use um destes: {', '.join(SETORES_VALIDOS)}."
            )

        try:
            valor = _parse_decimal(c.get("valor"), f"Colaborador #{idx} valor")
        except ValueError as e:
            errors.append(str(e))
            valor = Decimal("0")

        if valor <= 0:
            errors.append(f"Colaborador #{idx}: valor deve ser maior que zero.")

        if submit and not justificativa:
            errors.append(f"Colaborador #{idx}: justificativa obrigatoria para envio.")

        normalizados.append({
            "app_user_id": app_user_id,
            "nome": nome,
            "email": email,
            "cargo": cargo,
            "setor": c_setor,
            "valor": valor,
            "justificativa": justificativa,
            "observacoes": obs,
            "is_auto_premiacao": is_auto,
            "ordem": idx - 1,
        })
        total += valor

    lote_fields = {
        "mes_referencia": mes,
        "setor": setor,
        "observacoes_gerais": observacoes,
        "valor_total": total,
    }
    return lote_fields, normalizados, errors


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

def _fetch_lote(conn, lote_id: int) -> dict | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM premiacao_interna_lote WHERE id = %s", (lote_id,))
        return cur.fetchone()


def _fetch_colaboradores(conn, lote_id: int) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM premiacao_interna_colaborador WHERE lote_id = %s ORDER BY ordem, id",
            (lote_id,),
        )
        return list(cur.fetchall())


def _fetch_eventos(conn, lote_id: int) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM premiacao_interna_evento WHERE lote_id = %s ORDER BY created_at, id",
            (lote_id,),
        )
        return list(cur.fetchall())


def _replace_colaboradores(conn, lote_id: int, colaboradores: list[dict]):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM premiacao_interna_colaborador WHERE lote_id = %s", (lote_id,))
        for c in colaboradores:
            cur.execute(
                """
                INSERT INTO premiacao_interna_colaborador (
                    lote_id, app_user_id, nome, email, cargo, setor, valor,
                    justificativa, observacoes, is_auto_premiacao, ordem
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    lote_id, c.get("app_user_id"), c["nome"], c.get("email"),
                    c["cargo"], c["setor"], c["valor"],
                    c["justificativa"], c["observacoes"], c["is_auto_premiacao"],
                    c["ordem"],
                ),
            )


def _insert_evento(conn, *, lote_id, tipo, status_anterior, status_novo,
                    autor_user_id, autor_nome, justificativa=None,
                    payload_diff=None):
    autor_fk = _normalize_uid_for_fk(autor_user_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO premiacao_interna_evento (
                lote_id, tipo, status_anterior, status_novo,
                autor_user_id, autor_nome, justificativa, payload_diff
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                lote_id, tipo, status_anterior, status_novo,
                autor_fk, autor_nome, justificativa,
                json.dumps(payload_diff) if payload_diff is not None else None,
            ),
        )


def _mes_pt(mes_ref: str) -> str:
    """'2026-07' -> 'Julho de 2026'."""
    meses = ["", "Janeiro", "Fevereiro", "Marco", "Abril", "Maio", "Junho",
             "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
    try:
        y, m = mes_ref.split("-")
        return f"{meses[int(m)]} de {y}"
    except Exception:
        return mes_ref


# ---------------------------------------------------------------------------
# Lookup de usuarios — compartilhado gestor + aprovador
# ---------------------------------------------------------------------------

@premiacoes_internas_bp.route("/api/premiacoes-internas/usuarios-disponiveis",
                              methods=["GET"])
def api_usuarios_disponiveis():
    """Lista compacta de app_users para autofill no combobox de colaboradores.

    Acessivel para quem tem `premiacoes_internas` OU `aprovacao_premiacoes`
    (aprovador tambem precisa exibir dados no read-only).
    """
    ok, status, payload = _has_any_permission([PAGE_GESTOR, PAGE_APROVADOR])
    if not ok:
        return jsonify(payload), status

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id,
                       username,
                       email_cruzeiro AS email,
                       categoria
                  FROM app_users
                 ORDER BY LOWER(username)
                """
            )
            rows = list(cur.fetchall())
    finally:
        conn.close()

    return jsonify({"ok": True, "usuarios": rows})


# ---------------------------------------------------------------------------
# Gestor — rotas
# ---------------------------------------------------------------------------

@premiacoes_internas_bp.route("/api/premiacoes-internas/lotes", methods=["GET"])
def api_gestor_listar_lotes():
    ok, status, payload = _has_permission(PAGE_GESTOR)
    if not ok:
        return jsonify(payload), status

    uid, _, role = _session_user()

    mes = (request.args.get("mes") or "").strip()
    setor = (request.args.get("setor") or "").strip()
    status_f = (request.args.get("status") or "").strip()
    q = (request.args.get("q") or "").strip().lower()

    where = []
    params: dict = {}
    if role != "admin":
        where.append("gestor_user_id = %(uid)s")
        params["uid"] = uid
    if mes:
        where.append("mes_referencia = %(mes)s")
        params["mes"] = mes
    if setor:
        where.append("setor = %(setor)s")
        params["setor"] = setor
    if status_f:
        where.append("status = %(st)s")
        params["st"] = status_f

    sql = "SELECT * FROM premiacao_interna_lote"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY updated_at DESC, id DESC"

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())
    finally:
        conn.close()

    lotes = [_serialize_lote(r) for r in rows]
    if q:
        lotes = [
            l for l in lotes
            if q in l["gestor_nome"].lower() or q in l["setor"].lower()
        ]
    return jsonify({"ok": True, "lotes": lotes})


@premiacoes_internas_bp.route("/api/premiacoes-internas/lotes/<int:lote_id>", methods=["GET"])
def api_gestor_detalhe_lote(lote_id: int):
    ok, status, payload = _has_permission(PAGE_GESTOR)
    if not ok:
        return jsonify(payload), status

    uid, _, role = _session_user()
    conn = _get_conn()
    try:
        lote = _fetch_lote(conn, lote_id)
        if not lote:
            return jsonify({"error": "Lote nao encontrado"}), 404
        if role != "admin" and lote["gestor_user_id"] != uid:
            return jsonify({"error": "Sem permissao para acessar este lote"}), 403
        colaboradores = _fetch_colaboradores(conn, lote_id)
        eventos = _fetch_eventos(conn, lote_id)
    finally:
        conn.close()

    return jsonify({
        "ok": True,
        "lote": _serialize_lote(lote),
        "colaboradores": [_serialize_colaborador(c) for c in colaboradores],
        "eventos": [_serialize_evento(e) for e in eventos],
    })


@premiacoes_internas_bp.route("/api/premiacoes-internas/lotes", methods=["POST"])
def api_gestor_criar_lote():
    ok, status, payload = _has_permission(PAGE_GESTOR)
    if not ok:
        return jsonify(payload), status

    uid, uname, _ = _session_user()
    data = request.get_json(silent=True) or {}
    lote_fields, colaboradores, errors = _validate_lote_payload(data, submit=False)
    if errors:
        return jsonify({"error": "Payload invalido", "detalhes": errors}), 400

    autor_nome = _resolve_user_name(uid) or uname or ""
    gestor_fk = _normalize_uid_for_fk(uid)

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO premiacao_interna_lote (
                    mes_referencia, setor, gestor_user_id, gestor_nome,
                    observacoes_gerais, status, valor_total
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    lote_fields["mes_referencia"],
                    lote_fields["setor"],
                    gestor_fk,
                    autor_nome,
                    lote_fields["observacoes_gerais"],
                    STATUS_RASCUNHO,
                    lote_fields["valor_total"],
                ),
            )
            new_id = cur.fetchone()[0]
        _replace_colaboradores(conn, new_id, colaboradores)
        _insert_evento(
            conn,
            lote_id=new_id,
            tipo="criado",
            status_anterior=None,
            status_novo=STATUS_RASCUNHO,
            autor_user_id=uid,
            autor_nome=autor_nome,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"error": f"Erro ao criar lote: {e}"}), 500
    conn.close()

    return jsonify({"ok": True, "id": new_id}), 201


@premiacoes_internas_bp.route("/api/premiacoes-internas/lotes/<int:lote_id>", methods=["PUT"])
def api_gestor_atualizar_lote(lote_id: int):
    ok, status, payload = _has_permission(PAGE_GESTOR)
    if not ok:
        return jsonify(payload), status

    uid, uname, role = _session_user()
    data = request.get_json(silent=True) or {}
    lote_fields, colaboradores, errors = _validate_lote_payload(data, submit=False)
    if errors:
        return jsonify({"error": "Payload invalido", "detalhes": errors}), 400

    conn = _get_conn()
    try:
        lote = _fetch_lote(conn, lote_id)
        if not lote:
            conn.close()
            return jsonify({"error": "Lote nao encontrado"}), 404
        if role != "admin" and lote["gestor_user_id"] != uid:
            conn.close()
            return jsonify({"error": "Sem permissao para editar este lote"}), 403
        if lote["status"] not in STATUS_EDITAVEIS:
            conn.close()
            return jsonify({
                "error": f"Lote no status '{lote['status']}' nao pode ser editado."
            }), 409

        autor_nome = _resolve_user_name(uid) or uname or ""

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE premiacao_interna_lote
                   SET mes_referencia = %s,
                       setor = %s,
                       observacoes_gerais = %s,
                       valor_total = %s,
                       updated_at = NOW()
                 WHERE id = %s
                """,
                (
                    lote_fields["mes_referencia"],
                    lote_fields["setor"],
                    lote_fields["observacoes_gerais"],
                    lote_fields["valor_total"],
                    lote_id,
                ),
            )
        _replace_colaboradores(conn, lote_id, colaboradores)
        _insert_evento(
            conn,
            lote_id=lote_id,
            tipo="editado",
            status_anterior=lote["status"],
            status_novo=lote["status"],
            autor_user_id=uid,
            autor_nome=autor_nome,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"error": f"Erro ao atualizar lote: {e}"}), 500
    conn.close()

    return jsonify({"ok": True})


@premiacoes_internas_bp.route("/api/premiacoes-internas/lotes/<int:lote_id>", methods=["DELETE"])
def api_gestor_deletar_lote(lote_id: int):
    ok, status, payload = _has_permission(PAGE_GESTOR)
    if not ok:
        return jsonify(payload), status

    uid, _, role = _session_user()
    conn = _get_conn()
    try:
        lote = _fetch_lote(conn, lote_id)
        if not lote:
            conn.close()
            return jsonify({"error": "Lote nao encontrado"}), 404
        if role != "admin" and lote["gestor_user_id"] != uid:
            conn.close()
            return jsonify({"error": "Sem permissao para excluir este lote"}), 403
        if lote["status"] != STATUS_RASCUNHO:
            conn.close()
            return jsonify({
                "error": "Apenas rascunhos podem ser excluidos."
            }), 409

        with conn.cursor() as cur:
            cur.execute("DELETE FROM premiacao_interna_lote WHERE id = %s", (lote_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"error": f"Erro ao excluir lote: {e}"}), 500
    conn.close()
    return jsonify({"ok": True})


@premiacoes_internas_bp.route("/api/premiacoes-internas/lotes/<int:lote_id>/enviar",
                              methods=["POST"])
def api_gestor_enviar_lote(lote_id: int):
    ok, status, payload = _has_permission(PAGE_GESTOR)
    if not ok:
        return jsonify(payload), status

    uid, uname, role = _session_user()

    # Payload opcional pra permitir "salvar antes de enviar" numa mesma chamada.
    data = request.get_json(silent=True) or {}
    save_before_submit = bool(data.get("save_before_submit"))

    conn = _get_conn()
    try:
        lote = _fetch_lote(conn, lote_id)
        if not lote:
            conn.close()
            return jsonify({"error": "Lote nao encontrado"}), 404
        if role != "admin" and lote["gestor_user_id"] != uid:
            conn.close()
            return jsonify({"error": "Sem permissao para enviar este lote"}), 403
        if lote["status"] not in STATUS_EDITAVEIS:
            conn.close()
            return jsonify({
                "error": f"Lote no status '{lote['status']}' nao pode ser enviado."
            }), 409

        autor_nome = _resolve_user_name(uid) or uname or ""
        status_anterior = lote["status"]
        primeiro_envio = lote["enviado_em"] is None

        if save_before_submit:
            lote_fields, colaboradores, errors = _validate_lote_payload(data, submit=True)
            if errors:
                conn.close()
                return jsonify({"error": "Payload invalido", "detalhes": errors}), 400
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE premiacao_interna_lote
                       SET mes_referencia = %s,
                           setor = %s,
                           observacoes_gerais = %s,
                           valor_total = %s,
                           updated_at = NOW()
                     WHERE id = %s
                    """,
                    (
                        lote_fields["mes_referencia"],
                        lote_fields["setor"],
                        lote_fields["observacoes_gerais"],
                        lote_fields["valor_total"],
                        lote_id,
                    ),
                )
            _replace_colaboradores(conn, lote_id, colaboradores)
        else:
            # Re-valida colaboradores atuais (garante justificativa preenchida).
            colabs = _fetch_colaboradores(conn, lote_id)
            if not colabs:
                conn.close()
                return jsonify({"error": "Lote sem colaboradores."}), 400
            faltando = [
                f"Colaborador #{i+1}: justificativa obrigatoria."
                for i, c in enumerate(colabs) if not (c.get("justificativa") or "").strip()
            ]
            if faltando:
                conn.close()
                return jsonify({"error": "Payload invalido", "detalhes": faltando}), 400

        # Transiciona pra aguardando_aprovacao
        with conn.cursor() as cur:
            if primeiro_envio:
                cur.execute(
                    """
                    UPDATE premiacao_interna_lote
                       SET status = %s,
                           enviado_em = NOW(),
                           updated_at = NOW()
                     WHERE id = %s
                    """,
                    (STATUS_AGUARDANDO, lote_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE premiacao_interna_lote
                       SET status = %s,
                           updated_at = NOW()
                     WHERE id = %s
                    """,
                    (STATUS_AGUARDANDO, lote_id),
                )

        tipo_evento = "enviado" if primeiro_envio else "reenviado"
        _insert_evento(
            conn,
            lote_id=lote_id,
            tipo=tipo_evento,
            status_anterior=status_anterior,
            status_novo=STATUS_AGUARDANDO,
            autor_user_id=uid,
            autor_nome=autor_nome,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"error": f"Erro ao enviar lote: {e}"}), 500
    conn.close()

    # Notifica aprovadores (fora da transacao)
    try:
        gestor_id = lote["gestor_user_id"]
        gestor_nome_snap = lote["gestor_nome"] or autor_nome
        mes_txt = _mes_pt(lote["mes_referencia"])
        setor = lote["setor"]
        criar_aviso_por_permissao(
            PAGE_APROVADOR,
            "Nova premiacao aguardando aprovacao",
            f"O gestor {gestor_nome_snap} enviou a premiacao do setor {setor} "
            f"referente a {mes_txt}.",
            prioridade="importante",
            excluir_user_ids=[gestor_id],
            created_by=uid,
        )
    except Exception as e:
        # Nao derruba a transacao ja committada; loga no server.
        import logging
        logging.getLogger(__name__).warning(
            "Falha ao criar aviso pos-envio de lote %s: %s", lote_id, e
        )

    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Aprovador — rotas
# ---------------------------------------------------------------------------

@premiacoes_internas_bp.route("/api/premiacoes-internas/aprovacao/pendentes",
                              methods=["GET"])
def api_aprovacao_listar():
    ok, status, payload = _has_permission(PAGE_APROVADOR)
    if not ok:
        return jsonify(payload), status

    incluir_decididos = request.args.get("incluir_decididos") == "1"
    mes = (request.args.get("mes") or "").strip()
    setor = (request.args.get("setor") or "").strip()
    q = (request.args.get("q") or "").strip().lower()

    where = []
    params: dict = {}
    if incluir_decididos:
        where.append("status IN %(sts)s")
        params["sts"] = (
            STATUS_AGUARDANDO, STATUS_APROVADO, STATUS_REPROVADO, STATUS_AJUSTE,
        )
    else:
        where.append("status = %(sts)s")
        params["sts"] = STATUS_AGUARDANDO
    if mes:
        where.append("mes_referencia = %(mes)s")
        params["mes"] = mes
    if setor:
        where.append("setor = %(setor)s")
        params["setor"] = setor

    sql = (
        "SELECT * FROM premiacao_interna_lote WHERE "
        + " AND ".join(where)
        + " ORDER BY enviado_em DESC NULLS LAST, updated_at DESC, id DESC"
    )

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())
    finally:
        conn.close()

    lotes = [_serialize_lote(r) for r in rows]
    if q:
        lotes = [
            l for l in lotes
            if q in l["gestor_nome"].lower() or q in l["setor"].lower()
        ]

    kpis = _compute_kpis()
    return jsonify({"ok": True, "lotes": lotes, "kpis": kpis})


def _compute_kpis() -> dict:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = %s) AS pendentes,
                    COUNT(*) FILTER (
                        WHERE status = %s AND decidido_em::date = CURRENT_DATE
                    ) AS aprovadas_hoje,
                    COUNT(*) FILTER (
                        WHERE status = %s AND decidido_em::date = CURRENT_DATE
                    ) AS reprovadas_hoje,
                    COUNT(*) FILTER (
                        WHERE status = %s AND decidido_em::date = CURRENT_DATE
                    ) AS ajustes_hoje,
                    COALESCE(SUM(valor_total) FILTER (WHERE status = %s), 0) AS valor_pendente
                  FROM premiacao_interna_lote
                """,
                (STATUS_AGUARDANDO, STATUS_APROVADO, STATUS_REPROVADO,
                 STATUS_AJUSTE, STATUS_AGUARDANDO),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    return {
        "pendentes": int(row[0] or 0),
        "aprovadas_hoje": int(row[1] or 0),
        "reprovadas_hoje": int(row[2] or 0),
        "ajustes_hoje": int(row[3] or 0),
        "valor_pendente": _dec_to_float(row[4]),
    }


@premiacoes_internas_bp.route("/api/premiacoes-internas/aprovacao/lotes/<int:lote_id>",
                              methods=["GET"])
def api_aprovacao_detalhe(lote_id: int):
    ok, status, payload = _has_permission(PAGE_APROVADOR)
    if not ok:
        return jsonify(payload), status

    conn = _get_conn()
    try:
        lote = _fetch_lote(conn, lote_id)
        if not lote:
            return jsonify({"error": "Lote nao encontrado"}), 404
        colaboradores = _fetch_colaboradores(conn, lote_id)
        eventos = _fetch_eventos(conn, lote_id)
    finally:
        conn.close()

    return jsonify({
        "ok": True,
        "lote": _serialize_lote(lote),
        "colaboradores": [_serialize_colaborador(c) for c in colaboradores],
        "eventos": [_serialize_evento(e) for e in eventos],
    })


@premiacoes_internas_bp.route(
    "/api/premiacoes-internas/aprovacao/lotes/<int:lote_id>/decidir",
    methods=["POST"],
)
def api_aprovacao_decidir(lote_id: int):
    ok, status, payload = _has_permission(PAGE_APROVADOR)
    if not ok:
        return jsonify(payload), status

    uid, uname, role = _session_user()
    data = request.get_json(silent=True) or {}
    decisao = (data.get("decisao") or "").strip().lower()
    justificativa = (data.get("justificativa") or "").strip()

    if decisao not in ("aprovado", "reprovado", "ajuste_solicitado"):
        return jsonify({
            "error": "Decisao invalida (use aprovado | reprovado | ajuste_solicitado)."
        }), 400
    if decisao in ("reprovado", "ajuste_solicitado") and not justificativa:
        return jsonify({
            "error": "Justificativa obrigatoria para reprovar ou solicitar ajuste."
        }), 400

    conn = _get_conn()
    try:
        lote = _fetch_lote(conn, lote_id)
        if not lote:
            conn.close()
            return jsonify({"error": "Lote nao encontrado"}), 404
        if lote["status"] != STATUS_AGUARDANDO:
            conn.close()
            return jsonify({
                "error": f"Lote no status '{lote['status']}' nao pode ser decidido."
            }), 409
        if lote["gestor_user_id"] == uid and role != "admin":
            conn.close()
            return jsonify({
                "error": "Voce nao pode aprovar/decidir a propria premiacao."
            }), 403

        aprovador_nome = _resolve_user_name(uid) or uname or ""
        aprovador_fk = _normalize_uid_for_fk(uid)

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE premiacao_interna_lote
                   SET status = %s,
                       decidido_em = NOW(),
                       aprovador_user_id = %s,
                       aprovador_nome = %s,
                       aprovador_justificativa = %s,
                       updated_at = NOW()
                 WHERE id = %s
                """,
                (
                    decisao, aprovador_fk, aprovador_nome,
                    justificativa or None, lote_id,
                ),
            )
        _insert_evento(
            conn,
            lote_id=lote_id,
            tipo=decisao,
            status_anterior=lote["status"],
            status_novo=decisao,
            autor_user_id=uid,
            autor_nome=aprovador_nome,
            justificativa=justificativa or None,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({"error": f"Erro ao registrar decisao: {e}"}), 500
    conn.close()

    # Notifica gestor
    try:
        gestor_id = lote["gestor_user_id"]
        setor = lote["setor"]
        mes_txt = _mes_pt(lote["mes_referencia"])
        if decisao == "aprovado":
            titulo = "Premiacao aprovada"
            corpo = (f"Sua premiacao do setor {setor} referente a {mes_txt} "
                     f"foi aprovada por {aprovador_nome}.")
            prio = "normal"
        elif decisao == "reprovado":
            titulo = "Premiacao reprovada"
            corpo = (f"Sua premiacao do setor {setor} referente a {mes_txt} "
                     f"foi reprovada por {aprovador_nome}. Motivo: {justificativa}")
            prio = "importante"
        else:
            titulo = "Ajuste solicitado na premiacao"
            corpo = (f"{aprovador_nome} solicitou ajustes na premiacao do "
                     f"setor {setor} referente a {mes_txt}. Motivo: {justificativa}")
            prio = "importante"

        criar_aviso_para_usuarios(
            [gestor_id],
            titulo,
            corpo,
            prioridade=prio,
            created_by=uid,
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            "Falha ao criar aviso pos-decisao lote %s: %s", lote_id, e
        )

    return jsonify({"ok": True, "status": decisao})
