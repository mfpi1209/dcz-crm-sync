"""Sync Comercial — CRM Bwipo (org Comercial Cruzeiro).

Espelha pipelines/stages/contacts/deals no dcz_sync e monta depara Kommo → Bwipo.
Não substitui o sync Kommo: os dois convivem até a migração dos leads antigos.
"""
from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import psycopg2
import psycopg2.extras
from flask import Blueprint, jsonify, request, session

from db import get_conn
from helpers import fold_name
from services.bwipo_comercial import (
    BwipoComercialError,
    api_base,
    configured,
    delta_since_iso,
    flatten_contact,
    flatten_deal,
    get_deal,
    list_page,
    list_pipelines,
    rgm_norm,
    sleep_page,
    web_base,
)

logger = logging.getLogger(__name__)

bwipo_bp = Blueprint("bwipo_bp", __name__)

_KOMMO_DSN = dict(
    host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
    port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
    user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
    password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
    dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
)

_tasks: dict[str, dict] = {}
_sync_lock = threading.Lock()


def _pg_kommo():
    return psycopg2.connect(**_KOMMO_DSN)


def _set_meta(cur, entity: str, status: str, records: int = 0, error: str | None = None, full: bool = False):
    now = datetime.now(timezone.utc)
    cur.execute(
        """
        INSERT INTO bwipo_sync_metadata (entity_type, last_sync_at, last_full_sync_at,
                                         records_synced, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (entity_type) DO UPDATE SET
            last_sync_at = EXCLUDED.last_sync_at,
            last_full_sync_at = CASE WHEN %s THEN EXCLUDED.last_full_sync_at
                                     ELSE bwipo_sync_metadata.last_full_sync_at END,
            records_synced = EXCLUDED.records_synced,
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message
        """,
        (entity, now, now if full else None, records, status, error, full),
    )


def _task_log(task: dict, msg: str, progress: Optional[int] = None):
    task["log"].append({"time": datetime.now().strftime("%H:%M:%S"), "msg": msg})
    task["message"] = msg
    if progress is not None:
        task["progress"] = max(0, min(100, int(progress)))
    logger.info("bwipo_sync: %s", msg)


def _cancelled(task: dict) -> bool:
    return bool(task.get("cancelled"))


def _upsert_user(cur, uid: str, name: str | None, email: str | None, raw: dict | None = None):
    if not uid:
        return
    cur.execute(
        """
        INSERT INTO bwipo_users (id, name, email, raw_json, synced_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = COALESCE(EXCLUDED.name, bwipo_users.name),
            email = COALESCE(EXCLUDED.email, bwipo_users.email),
            raw_json = CASE WHEN EXCLUDED.raw_json <> '{}'::jsonb THEN EXCLUDED.raw_json
                            ELSE bwipo_users.raw_json END,
            synced_at = NOW()
        """,
        (uid, name, email, psycopg2.extras.Json(raw or {})),
    )


def _last_sync_at(cur):
    cur.execute(
        """
        SELECT last_sync_at FROM bwipo_sync_metadata
        WHERE entity_type IN ('deals', 'run') AND last_sync_at IS NOT NULL
        ORDER BY last_sync_at DESC LIMIT 1
        """
    )
    row = cur.fetchone()
    return row[0] if row else None


def _resolve_sync_window(cur, mode: str):
    """Incremental só com last_sync — senão vira Full, igual ao Kommo."""
    if mode == "full":
        return True, None
    last = _last_sync_at(cur)
    if last is None:
        return True, None
    return False, delta_since_iso(last)


def _upsert_contact(cur, row: dict) -> bool:
    if not row.get("id"):
        return False
    cur.execute(
        """
        INSERT INTO bwipo_contacts (
            id, number, name, email, phone, phone_norm, email_norm, source,
            assigned_to_id, assigned_to_name, cpf_norm, rgm_norm, custom_fields,
            is_deleted, created_at, updated_at, raw_json, synced_at
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, FALSE, %s, %s, %s, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            number = EXCLUDED.number, name = EXCLUDED.name, email = EXCLUDED.email,
            phone = EXCLUDED.phone, phone_norm = EXCLUDED.phone_norm,
            email_norm = EXCLUDED.email_norm, source = EXCLUDED.source,
            assigned_to_id = EXCLUDED.assigned_to_id,
            assigned_to_name = EXCLUDED.assigned_to_name,
            cpf_norm = COALESCE(EXCLUDED.cpf_norm, bwipo_contacts.cpf_norm),
            rgm_norm = COALESCE(EXCLUDED.rgm_norm, bwipo_contacts.rgm_norm),
            custom_fields = EXCLUDED.custom_fields, is_deleted = FALSE,
            created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
            raw_json = EXCLUDED.raw_json, synced_at = NOW()
        """,
        (
            row["id"], row["number"], row["name"], row["email"], row["phone"],
            row["phone_norm"], row["email_norm"], row["source"],
            row["assigned_to_id"], row["assigned_to_name"],
            row["cpf_norm"], row["rgm_norm"],
            psycopg2.extras.Json(row["custom_fields"]),
            row["created_at"], row["updated_at"],
            psycopg2.extras.Json(row["raw"]),
        ),
    )
    if row.get("assigned_to_id"):
        _upsert_user(cur, row["assigned_to_id"], row.get("assigned_to_name"), None)
    return True


def _sync_pipelines(cur, task: dict, full: bool) -> int:
    _task_log(task, "Sincronizando pipelines e etapas…", 5)
    pipes = list_pipelines()
    n_stages = 0
    for p in pipes:
        if _cancelled(task):
            return n_stages
        cur.execute(
            """
            INSERT INTO bwipo_pipelines (id, name, slug, number, is_default, organization_id, raw_json, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name, slug = EXCLUDED.slug, number = EXCLUDED.number,
                is_default = EXCLUDED.is_default, organization_id = EXCLUDED.organization_id,
                raw_json = EXCLUDED.raw_json, synced_at = NOW()
            """,
            (
                p.get("id"), p.get("name"), p.get("slug"), p.get("number"),
                bool(p.get("isDefault")), p.get("organizationId"),
                psycopg2.extras.Json(p),
            ),
        )
        for s in p.get("stages") or []:
            cur.execute(
                """
                INSERT INTO bwipo_stages (
                    id, pipeline_id, name, slug, number, position, color,
                    is_won, is_lost, is_incoming, deal_count, raw_json, synced_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (id) DO UPDATE SET
                    pipeline_id = EXCLUDED.pipeline_id, name = EXCLUDED.name,
                    slug = EXCLUDED.slug, number = EXCLUDED.number, position = EXCLUDED.position,
                    color = EXCLUDED.color, is_won = EXCLUDED.is_won, is_lost = EXCLUDED.is_lost,
                    is_incoming = EXCLUDED.is_incoming, deal_count = EXCLUDED.deal_count,
                    raw_json = EXCLUDED.raw_json, synced_at = NOW()
                """,
                (
                    s.get("id"), p.get("id"), s.get("name"), s.get("slug"),
                    s.get("number"), s.get("position"), s.get("color"),
                    bool(s.get("isWon")), bool(s.get("isLost")), bool(s.get("isIncoming")),
                    s.get("dealCount"), psycopg2.extras.Json(s),
                ),
            )
            n_stages += 1
    _set_meta(cur, "pipelines", "completed", len(pipes), full=full)
    _set_meta(cur, "stages", "completed", n_stages, full=full)
    _task_log(task, f"Pipelines {len(pipes)} · etapas {n_stages}", 12)
    return n_stages


def _paginate(path: str, task: dict, label: str, extra: Optional[dict] = None):
    page = 1
    total_seen = 0
    total_hint = None
    while not _cancelled(task):
        payload = list_page(path, page=page, per_page=100, extra=extra)
        items = payload.get("items") or []
        if not items:
            break
        total_hint = payload.get("total") or total_hint
        total_pages = payload.get("totalPages")
        total_seen += len(items)
        yield items, total_seen, total_hint
        if total_pages and page >= total_pages:
            break
        if len(items) < 100:
            break
        page += 1
        sleep_page()


def _sync_contacts(cur, task: dict, full: bool, run_start, extra=None, only_items=None) -> int:
    _task_log(task, "Sincronizando contatos…", 15)
    n = 0
    if only_items is not None:
        batches = [(only_items, len(only_items), len(only_items))]
    else:
        batches = _paginate("/api/contacts", task, "contacts", extra=extra)
    for items, seen, hint in batches:
        for raw in items:
            if _upsert_contact(cur, flatten_contact(raw)):
                n += 1
        pct = 15 + int(min(25, (seen / max(hint or seen, 1)) * 25))
        _task_log(task, f"Contatos {seen}" + (f" / {hint}" if hint else ""), pct)
    if full:
        cur.execute(
            "UPDATE bwipo_contacts SET is_deleted = TRUE WHERE synced_at < %s AND NOT is_deleted",
            (run_start,),
        )
    _set_meta(cur, "contacts", "completed", n, full=full)
    return n


def _write_deal(cur, raw: dict) -> dict | None:
    """Grava um negócio da API. Não apaga RGM/CPF que já estavam na coluna."""
    row = flatten_deal(raw)
    if not row.get("id"):
        return None
    cur.execute(
                """
                INSERT INTO bwipo_deals (
                    id, number, title, value, status, deal_role, contact_id,
                    stage_id, stage_name, stage_slug, pipeline_id, pipeline_name,
                    owner_id, owner_name, lost_reason, phone_norm, email_norm,
                    rgm_norm, cpf_norm, data_matricula, situacao, curso, polo, origem,
                    custom_fields, is_deleted, created_at, updated_at, closed_at,
                    raw_json, synced_at
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s, FALSE, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    number = EXCLUDED.number, title = EXCLUDED.title, value = EXCLUDED.value,
                    status = EXCLUDED.status, deal_role = EXCLUDED.deal_role,
                    contact_id = EXCLUDED.contact_id, stage_id = EXCLUDED.stage_id,
                    stage_name = EXCLUDED.stage_name, stage_slug = EXCLUDED.stage_slug,
                    pipeline_id = EXCLUDED.pipeline_id, pipeline_name = EXCLUDED.pipeline_name,
                    owner_id = EXCLUDED.owner_id, owner_name = EXCLUDED.owner_name,
                    lost_reason = EXCLUDED.lost_reason, phone_norm = EXCLUDED.phone_norm,
                    email_norm = EXCLUDED.email_norm,
                    rgm_norm = COALESCE(EXCLUDED.rgm_norm, bwipo_deals.rgm_norm),
                    cpf_norm = COALESCE(EXCLUDED.cpf_norm, bwipo_deals.cpf_norm),
                    data_matricula = COALESCE(EXCLUDED.data_matricula, bwipo_deals.data_matricula),
                    situacao = COALESCE(EXCLUDED.situacao, bwipo_deals.situacao),
                    curso = COALESCE(EXCLUDED.curso, bwipo_deals.curso),
                    polo = COALESCE(EXCLUDED.polo, bwipo_deals.polo),
                    origem = COALESCE(EXCLUDED.origem, bwipo_deals.origem),
                    custom_fields = EXCLUDED.custom_fields, is_deleted = FALSE,
                    created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at,
                    closed_at = EXCLUDED.closed_at, raw_json = EXCLUDED.raw_json,
                    synced_at = NOW()
                """,
                (
                    row["id"], row["number"], row["title"], row["value"], row["status"],
                    row["deal_role"], row["contact_id"], row["stage_id"], row["stage_name"],
                    row["stage_slug"], row["pipeline_id"], row["pipeline_name"],
                    row["owner_id"], row["owner_name"], row["lost_reason"],
                    row["phone_norm"], row["email_norm"], row["rgm_norm"], row["cpf_norm"],
                    row["data_matricula"], row["situacao"], row["curso"], row["polo"],
                    row["origem"], psycopg2.extras.Json(row["custom_fields"]),
                    row["created_at"], row["updated_at"], row["closed_at"],
                    psycopg2.extras.Json(row["raw"]),
                ),
            )
    if row.get("owner_id"):
        owner = row.get("raw", {}).get("owner") if isinstance(row.get("raw"), dict) else None
        _upsert_user(cur, row["owner_id"], row.get("owner_name"), row.get("owner_email"), owner if isinstance(owner, dict) else None)
    contact_raw = raw.get("contact") if isinstance(raw.get("contact"), dict) else None
    if contact_raw and contact_raw.get("id"):
        _upsert_contact(cur, flatten_contact(contact_raw))
    return row


def _sync_deals(cur, task: dict, full: bool, run_start, extra=None) -> tuple[int, list[dict]]:
    _task_log(task, "Sincronizando negócios…", 42)
    n = 0
    contacts: list[dict] = []
    for items, seen, hint in _paginate("/api/deals", task, "deals", extra=extra):
        for raw in items:
            row = _write_deal(cur, raw)
            if not row:
                continue
            contact_raw = raw.get("contact") if isinstance(raw.get("contact"), dict) else None
            if contact_raw and contact_raw.get("id"):
                contacts.append(contact_raw)
            n += 1
        pct = 42 + int(min(40, (seen / max(hint or seen, 1)) * 40))
        _task_log(task, f"Negócios {seen}" + (f" / {hint}" if hint else ""), pct)
    if full:
        cur.execute(
            "UPDATE bwipo_deals SET is_deleted = TRUE WHERE synced_at < %s AND NOT is_deleted",
            (run_start,),
        )
    _set_meta(cur, "deals", "completed", n, full=full)
    cur.execute("SELECT COUNT(*) FROM bwipo_users")
    _set_meta(cur, "users", "completed", int(cur.fetchone()[0]), full=full)
    return n, contacts


def fill_fields_from_json(cur) -> int:
    """Copia RGM/CPF/data do JSON (campo sem nome) para as colunas do espelho."""
    cur.execute(
        """
        UPDATE bwipo_deals d SET
            rgm_norm = COALESCE(d.rgm_norm, NULLIF(regexp_replace(f.rgm, '[^0-9]', '', 'g'), '')),
            cpf_norm = COALESCE(d.cpf_norm, CASE
                WHEN length(regexp_replace(f.cpf, '[^0-9]', '', 'g')) BETWEEN 9 AND 10
                    THEN lpad(regexp_replace(f.cpf, '[^0-9]', '', 'g'), 11, '0')
                WHEN length(regexp_replace(f.cpf, '[^0-9]', '', 'g')) = 11
                    THEN regexp_replace(f.cpf, '[^0-9]', '', 'g')
                ELSE NULL END),
            data_matricula = COALESCE(d.data_matricula, NULLIF(btrim(f.data_mat), '')),
            situacao = COALESCE(d.situacao, NULLIF(btrim(f.situacao), '')),
            curso = COALESCE(d.curso, NULLIF(btrim(f.curso), '')),
            polo = COALESCE(d.polo, NULLIF(btrim(f.polo), ''))
        FROM (
            SELECT id,
                max(value) FILTER (WHERE fid = 'cmu2wz3qt0eqrqn01gv5p1vcr') AS rgm,
                max(value) FILTER (WHERE fid = 'cmu2wwmld0burqn01gu4w1sxm') AS cpf,
                max(value) FILTER (WHERE fid = 'cmu2wyu8l0efdqn01kfew46tt') AS data_mat,
                max(value) FILTER (WHERE fid = 'cmub9ys7k2wxhld01zvujuuiu') AS situacao,
                max(value) FILTER (WHERE fid = 'cmu2ww7lk0be9qn01q3re0hem') AS curso,
                max(value) FILTER (WHERE fid = 'cmub9yel82wunld01uru5t6b0') AS polo
            FROM (
                SELECT d2.id,
                       elem->>'customFieldId' AS fid,
                       elem->>'value' AS value
                FROM bwipo_deals d2
                CROSS JOIN LATERAL jsonb_array_elements(d2.custom_fields) elem
                WHERE NOT d2.is_deleted
                  AND jsonb_typeof(d2.custom_fields) = 'array'
            ) x
            GROUP BY id
        ) f
        WHERE d.id = f.id
          AND (
                (d.rgm_norm IS NULL AND f.rgm IS NOT NULL)
             OR (d.cpf_norm IS NULL AND f.cpf IS NOT NULL)
             OR (d.data_matricula IS NULL AND f.data_mat IS NOT NULL)
             OR (d.situacao IS NULL AND f.situacao IS NOT NULL)
             OR (d.curso IS NULL AND f.curso IS NOT NULL)
             OR (d.polo IS NULL AND f.polo IS NOT NULL)
          )
        """
    )
    return cur.rowcount


def refresh_deal(deal_id: str | None = None, number: int | None = None) -> dict:
    """Busca um negócio na API e grava no espelho. Por id Bwipo ou pelo número do card."""
    raw = None
    if deal_id:
        raw = get_deal(str(deal_id).strip())
    elif number is not None:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM bwipo_deals WHERE number = %s AND NOT is_deleted LIMIT 1",
                (int(number),),
            )
            found = cur.fetchone()
            cur.close()
        finally:
            conn.close()
        if found:
            raw = get_deal(found[0])
        else:
            payload = list_page("/api/deals", page=1, per_page=20, extra={"search": str(number)})
            for item in payload.get("items") or []:
                if str(item.get("number")) == str(number):
                    raw = item
                    break
    if not isinstance(raw, dict) or not raw.get("id"):
        raise BwipoComercialError("Negócio não encontrado no Bwipo.", 404)
    conn = get_conn()
    try:
        cur = conn.cursor()
        row = _write_deal(cur, raw)
        conn.commit()
        cur.close()
    finally:
        conn.close()
    if not row:
        raise BwipoComercialError("Negócio sem id.", 404)
    return row


def _rebuild_depara(cur, task: dict) -> int:
    """Casa Kommo -> Bwipo por RGM > CPF > telefone > e-mail."""
    _task_log(task, "Montando depara Kommo -> Bwipo...", 88)
    cur.execute(
        """
        SELECT d.id, d.contact_id, d.title, d.rgm_norm, d.cpf_norm, d.phone_norm, d.email_norm,
               COALESCE(c.name, d.title) AS nome
        FROM bwipo_deals d
        LEFT JOIN bwipo_contacts c ON c.id = d.contact_id
        WHERE NOT d.is_deleted
        """
    )
    deals = cur.fetchall()
    by_rgm, by_cpf, by_phone, by_email, by_name = {}, {}, {}, {}, {}
    for d in deals:
        deal_id, contact_id, title, rgm, cpf, phone, email, nome = d
        pack = {
            "deal_id": deal_id,
            "contact_id": contact_id,
            "name": nome or title or "",
            "rgm": rgm,
        }
        if rgm and rgm not in by_rgm:
            by_rgm[rgm] = pack
        if cpf and cpf not in by_cpf:
            by_cpf[cpf] = pack
        if phone and phone not in by_phone:
            by_phone[phone] = pack
        if email and email not in by_email:
            by_email[email] = pack
        fn = fold_name(nome or title or "")
        if fn and fn not in by_name:
            by_name[fn] = pack

    if not deals:
        _task_log(task, "Depara: nenhum negócio no espelho ainda.", 92)
        return 0

    # As consultas no Kommo levam minutos; a conexão do dcz_sync não pode
    # ficar "idle in transaction" nesse intervalo, senão o servidor a derruba.
    cur.connection.commit()

    try:
        kconn = _pg_kommo()
    except Exception as e:
        _task_log(task, f"Depara: kommo_sync indisponível ({e}).", 92)
        return 0

    rows: list[tuple] = []

    def _upsert_match(lid, name, pack, mtype, key, rgm_n, conf="exact"):
        rows.append((
            lid, pack["deal_id"], pack["contact_id"], mtype, key,
            name, pack["name"], rgm_n, pack["rgm"], conf,
        ))

    def _flush():
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO bwipo_kommo_depara (
                kommo_lead_id, bwipo_deal_id, bwipo_contact_id, match_type,
                match_key, kommo_name, bwipo_name, kommo_rgm, bwipo_rgm,
                confidence
            ) VALUES %s
            ON CONFLICT (kommo_lead_id) DO UPDATE SET
                bwipo_deal_id = EXCLUDED.bwipo_deal_id,
                bwipo_contact_id = EXCLUDED.bwipo_contact_id,
                match_type = CASE WHEN bwipo_kommo_depara.match_type = 'manual'
                                  THEN bwipo_kommo_depara.match_type
                                  ELSE EXCLUDED.match_type END,
                match_key = CASE WHEN bwipo_kommo_depara.match_type = 'manual'
                                 THEN bwipo_kommo_depara.match_key
                                 ELSE EXCLUDED.match_key END,
                kommo_name = EXCLUDED.kommo_name,
                bwipo_name = EXCLUDED.bwipo_name,
                kommo_rgm = EXCLUDED.kommo_rgm,
                bwipo_rgm = EXCLUDED.bwipo_rgm,
                confidence = CASE WHEN bwipo_kommo_depara.match_type = 'manual'
                                  THEN bwipo_kommo_depara.confidence
                                  ELSE EXCLUDED.confidence END,
                updated_at = NOW()
            """,
            rows,
            page_size=1000,
        )

    matched = 0
    try:
        kcur = kconn.cursor()
        # Só busca no Kommo as chaves que já existem no Bwipo (não varre os 200k).
        queries = [
            ("rgm", list(by_rgm.keys()), "rgm",
             "lower(cf.field_name) = 'rgm'"),
            ("cpf", list(by_cpf.keys()), "cpf",
             "lower(cf.field_name) = 'cpf'"),
            ("email", list(by_email.keys()), "email",
             "lower(cf.field_name) IN ('e-mail', 'email')"),
            ("phone", list(by_phone.keys()), "phone",
             "lower(cf.field_name) LIKE 'telefone%%'"),
        ]
        seen_leads: set[int] = set()
        for mtype, keys, _kind, where_sql in queries:
            if not keys:
                continue
            for i in range(0, len(keys), 400):
                chunk = keys[i:i + 400]
                kcur.execute(
                    f"""
                    SELECT l.id, l.name,
                           regexp_replace((cf.values_json->0)->>'value', '[^0-9]', '', 'g') AS digits,
                           lower(trim((cf.values_json->0)->>'value')) AS raw
                    FROM lead_custom_field_values cf
                    JOIN leads l ON l.id = cf.lead_id AND COALESCE(l.is_deleted, FALSE) = FALSE
                    WHERE {where_sql}
                      AND (
                        regexp_replace((cf.values_json->0)->>'value', '[^0-9]', '', 'g') = ANY(%s)
                        OR lower(trim((cf.values_json->0)->>'value')) = ANY(%s)
                      )
                    """,
                    (chunk, chunk),
                )
                for lid, name, digits, raw in kcur.fetchall():
                    if lid in seen_leads:
                        continue
                    pack = None
                    key = None
                    rgm_n = None
                    if mtype == "rgm":
                        rgm_n = rgm_norm(digits)
                        pack = by_rgm.get(rgm_n)
                        key = rgm_n
                    elif mtype == "cpf":
                        cpf_n = digits if len(digits or "") == 11 else (
                            (digits or "").zfill(11) if digits and 9 <= len(digits) <= 10 else ""
                        )
                        pack = by_cpf.get(cpf_n)
                        key = cpf_n
                    elif mtype == "email":
                        pack = by_email.get(raw)
                        key = raw
                    else:
                        ph = digits or ""
                        if ph.startswith("55") and len(ph) >= 12:
                            ph = ph[2:]
                        ph = ph[-11:] if len(ph) >= 10 else ph
                        pack = by_phone.get(ph)
                        key = ph
                    if not pack:
                        continue
                    _upsert_match(lid, name, pack, mtype, key, rgm_n or pack.get("rgm"))
                    seen_leads.add(lid)
                    matched += 1
        kcur.close()
    finally:
        kconn.close()
    if rows:
        _task_log(task, f"Depara: gravando {len(rows)} casamentos...", 94)
        _flush()
    _set_meta(cur, "depara", "completed", matched, full=True)
    _task_log(task, f"Depara: {matched} leads Kommo casados.", 95)
    return matched


def _rebuild_user_depara(cur, task: dict) -> int:
    """Casa consultor Bwipo → Kommo por e-mail (e nome, se o e-mail não bater)."""
    _task_log(task, "Mapeando consultores Bwipo -> Kommo...", 86)
    cur.execute(
        "SELECT id, name, email FROM bwipo_users WHERE email IS NOT NULL AND email <> ''"
    )
    users = cur.fetchall()
    if not users:
        return 0
    try:
        kconn = _pg_kommo()
    except Exception as e:
        _task_log(task, f"Depara users: kommo_sync indisponível ({e}).", 87)
        return 0
    matched = 0
    try:
        kcur = kconn.cursor()
        kcur.execute("SELECT id, name, email FROM users")
        by_email = {}
        by_name = {}
        for kid, kname, kemail in kcur.fetchall():
            if kemail:
                by_email[(kemail or "").strip().lower()] = (int(kid), kname)
            key = fold_name(kname or "")
            if key and key not in by_name:
                by_name[key] = (int(kid), kname)
        kcur.close()
        for uid, name, email in users:
            email_n = (email or "").strip().lower()
            hit = by_email.get(email_n) if email_n else None
            match_type = "email"
            if not hit and name:
                hit = by_name.get(fold_name(name))
                match_type = "name"
            if not hit:
                continue
            cur.execute(
                """
                INSERT INTO bwipo_kommo_user_depara (
                    bwipo_user_id, kommo_user_id, match_type, email, bwipo_name, kommo_name,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (bwipo_user_id) DO UPDATE SET
                    kommo_user_id = CASE
                        WHEN bwipo_kommo_user_depara.match_type = 'manual'
                        THEN bwipo_kommo_user_depara.kommo_user_id
                        ELSE EXCLUDED.kommo_user_id END,
                    match_type = CASE
                        WHEN bwipo_kommo_user_depara.match_type = 'manual'
                        THEN bwipo_kommo_user_depara.match_type
                        ELSE EXCLUDED.match_type END,
                    email = EXCLUDED.email,
                    bwipo_name = EXCLUDED.bwipo_name,
                    kommo_name = EXCLUDED.kommo_name,
                    updated_at = NOW()
                """,
                (uid, hit[0], match_type, email_n or None, name, hit[1]),
            )
            matched += 1
    finally:
        kconn.close()
    _set_meta(cur, "user_depara", "completed", matched, full=True)
    _task_log(task, f"Consultores mapeados: {matched}.", 87)
    return matched


def _run_sync(task: dict, mode: str):
    run_start = datetime.now(timezone.utc)
    conn = None
    full = mode == "full"
    try:
        if not configured():
            raise BwipoComercialError("BWIPO_COMERCIAL_API_TOKEN não configurado no .env", 503)
        conn = get_conn()
        cur = conn.cursor()
        full, since = _resolve_sync_window(cur, mode)
        extra = {"updatedSince": since} if since else None
        if mode != "full" and full:
            _task_log(task, "Incremental sem last_sync — rodando Full.", 3)
        elif extra:
            _task_log(task, f"Incremental: updatedSince={since}", 3)
        _set_meta(cur, "run", "running", 0, full=full)
        conn.commit()

        _sync_pipelines(cur, task, full)
        conn.commit()
        if _cancelled(task):
            raise InterruptedError("cancelado")

        if full:
            n_c = _sync_contacts(cur, task, True, run_start)
            conn.commit()
            if _cancelled(task):
                raise InterruptedError("cancelado")
            n_d, _ = _sync_deals(cur, task, True, run_start)
            conn.commit()
        else:
            n_d, deal_contacts = _sync_deals(cur, task, False, run_start, extra=extra)
            conn.commit()
            if _cancelled(task):
                raise InterruptedError("cancelado")
            n_c = _sync_contacts(cur, task, False, run_start, only_items=deal_contacts)
            conn.commit()
        if _cancelled(task):
            raise InterruptedError("cancelado")

        try:
            n_u = _rebuild_user_depara(cur, task)
            conn.commit()
        except Exception as ue:
            conn.rollback()
            logger.exception("bwipo user depara: %s", ue)
            n_u = 0
            _task_log(task, f"Depara consultores falhou: {ue}", 87)

        try:
            n_m = _rebuild_depara(cur, task)
            conn.commit()
        except Exception as de:
            conn.rollback()
            logger.exception("bwipo depara: %s", de)
            n_m = 0
            _task_log(task, f"Depara falhou (espelho ok): {de}", 95)

        _set_meta(cur, "run", "completed", n_c + n_d, full=full)
        conn.commit()
        _task_log(
            task,
            f"Concluído: {n_c} contatos, {n_d} negócios, {n_m} deparas, {n_u} consultores.",
            100,
        )
        task["status"] = "completed"
    except InterruptedError:
        if conn:
            conn.rollback()
        task["status"] = "cancelled"
        _task_log(task, "Sincronização cancelada.", task.get("progress") or 0)
    except Exception as e:
        logger.exception("bwipo_sync falhou")
        if conn:
            try:
                cur = conn.cursor()
                _set_meta(cur, "run", "error", 0, error=str(e)[:400], full=full)
                conn.commit()
            except Exception:
                conn.rollback()
        task["status"] = "error"
        _task_log(task, f"Erro: {e}", task.get("progress") or 0)
    finally:
        if conn:
            conn.close()
        if _sync_lock.locked():
            _sync_lock.release()


def register_bwipo_sync_job(sched):
    """Incremental/Full só no botão. O responsável dos leads novos roda a cada 5 min."""
    for job_id in ("bwipo_sync_delta", "bwipo_sync_full"):
        try:
            sched.remove_job(job_id)
        except Exception:
            pass
    try:
        from apscheduler.triggers.interval import IntervalTrigger

        from services.bwipo_owner import assign_recent_unowned

        def _run():
            try:
                stats = assign_recent_unowned()
                logger.info("bwipo owner recente %s", stats)
            except Exception:
                logger.exception("bwipo owner recente")

        sched.add_job(
            _run,
            IntervalTrigger(minutes=5),
            id="bwipo_owner_recente",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        sched.add_job(
            rebuild_painel_base,
            IntervalTrigger(minutes=10),
            id="bwipo_painel_base",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
        logger.info("bwipo owner recente a cada 5 min; base do painel a cada 10 min")
    except Exception:
        logger.exception("bwipo owner recente não agendou")


@bwipo_bp.route("/api/bwipo/connection-test")
def api_bwipo_connection_test():
    if not configured():
        return jsonify({"ok": False, "error": "BWIPO_COMERCIAL_API_TOKEN não configurado."}), 500
    try:
        pipes = list_pipelines()
        stages = []
        for p in pipes:
            stages.extend(p.get("stages") or [])
        return jsonify({
            "ok": True,
            "base": api_base(),
            "web": web_base(),
            "pipelines": len(pipes),
            "stages": len(stages),
            "pipeline_names": [p.get("name") for p in pipes],
        })
    except BwipoComercialError as e:
        return jsonify({"ok": False, "error": str(e)}), e.status


@bwipo_bp.route("/api/bwipo/status")
def api_bwipo_status():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM bwipo_sync_metadata ORDER BY entity_type")
        entities = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) AS cnt FROM bwipo_deals WHERE NOT is_deleted")
        deals = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) AS cnt FROM bwipo_contacts WHERE NOT is_deleted")
        contacts = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) AS cnt FROM bwipo_kommo_depara")
        depara = cur.fetchone()["cnt"]
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM bwipo_deals WHERE NOT is_deleted AND created_at >= date_trunc('day', NOW())"
        )
        new_today = cur.fetchone()["cnt"]
        cur.execute(
            """
            SELECT COUNT(*) AS cnt FROM bwipo_deals d
            JOIN bwipo_stages s ON s.id = d.stage_id
            WHERE NOT d.is_deleted AND s.is_won
            """
        )
        won = cur.fetchone()["cnt"]
        conn.close()
        return jsonify({
            "ok": True,
            "data": {
                "entities": entities,
                "leads_count": deals,
                "contacts_count": contacts,
                "depara_count": depara,
                "new_today": new_today,
                "won_total": won,
                "configured": configured(),
                "base": api_base(),
                "web": web_base(),
            },
        })
    except Exception as e:
        logger.error("bwipo status: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bwipo_bp.route("/api/bwipo/funnel")
def api_bwipo_funnel():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT p.name AS pipeline_name, s.name AS stage_name, s.slug, s.id AS stage_id,
                   s.position, s.is_won, s.is_lost, s.is_incoming, s.color,
                   COUNT(d.id) AS total
            FROM bwipo_stages s
            JOIN bwipo_pipelines p ON p.id = s.pipeline_id
            LEFT JOIN bwipo_deals d ON d.stage_id = s.id AND NOT d.is_deleted
            GROUP BY p.name, s.name, s.slug, s.id, s.position, s.is_won, s.is_lost, s.is_incoming, s.color, p.number
            ORDER BY p.number, s.position
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM bwipo_deals WHERE NOT is_deleted AND created_at >= date_trunc('day', NOW())"
        )
        new_today = cur.fetchone()["cnt"]
        conn.close()
        stages = []
        for r in rows:
            slug = (r.get("slug") or "").replace("-", "_")
            stages.append({
                "key": slug,
                "name": r["stage_name"],
                "pipeline": r["pipeline_name"],
                "count": int(r["total"] or 0),
                "is_won": r["is_won"],
                "is_lost": r["is_lost"],
                "color": r["color"],
            })
        return jsonify({
            "ok": True,
            "data": {
                "stages": stages,
                "new_today": new_today,
                "total": sum(s["count"] for s in stages),
                "by_stage": [
                    {
                        "pipeline_name": r["pipeline_name"],
                        "stage_name": r["stage_name"],
                        "total": int(r["total"] or 0),
                    }
                    for r in rows
                ],
            },
        })
    except Exception as e:
        logger.error("bwipo funnel: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bwipo_bp.route("/api/bwipo/recent-changes")
def api_bwipo_recent_changes():
    hours = request.args.get("hours", 24, type=int)
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT COALESCE(pipeline_name, '—') AS pipeline_name,
                   COALESCE(stage_name, '—') AS stage_name,
                   COUNT(*) AS total
            FROM bwipo_deals
            WHERE synced_at >= NOW() - (%s * INTERVAL '1 hour')
              AND NOT is_deleted
            GROUP BY pipeline_name, stage_name
            ORDER BY COUNT(*) DESC
            """,
            (hours,),
        )
        by_stage = [dict(r) for r in cur.fetchall()]
        cur.execute(
            "SELECT COUNT(*) AS t FROM bwipo_deals WHERE synced_at >= NOW() - (%s * INTERVAL '1 hour')",
            (hours,),
        )
        leads_upd = cur.fetchone()["t"]
        cur.execute(
            "SELECT COUNT(*) AS t FROM bwipo_contacts WHERE synced_at >= NOW() - (%s * INTERVAL '1 hour')",
            (hours,),
        )
        contacts_upd = cur.fetchone()["t"]
        cur.execute(
            """
            SELECT COUNT(*) AS t FROM bwipo_deals
            WHERE created_at >= NOW() - (%s * INTERVAL '1 hour') AND NOT is_deleted
            """,
            (hours,),
        )
        new_leads = cur.fetchone()["t"]
        cur.execute(
            """
            SELECT COUNT(*) AS t FROM bwipo_deals d
            JOIN bwipo_stages s ON s.id = d.stage_id
            WHERE s.is_won AND d.synced_at >= NOW() - (%s * INTERVAL '1 hour')
            """,
            (hours,),
        )
        won = cur.fetchone()["t"]
        conn.close()
        return jsonify({
            "ok": True,
            "data": {
                "hours": hours,
                "leads_updated": leads_upd,
                "contacts_updated": contacts_upd,
                "new_leads": new_leads,
                "won": won,
                "by_stage": by_stage,
            },
        })
    except Exception as e:
        logger.error("bwipo recent: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bwipo_bp.route("/api/bwipo/deal-refresh", methods=["POST"])
def api_bwipo_deal_refresh():
    """Atualiza um negócio: id Bwipo (cuid) ou number do card."""
    if not configured():
        return jsonify({"ok": False, "error": "BWIPO_COMERCIAL_API_TOKEN não configurado no .env"}), 500
    body = request.get_json(silent=True) or {}
    deal_id = (body.get("bwipo_id") or body.get("id") or "").strip()
    number = body.get("bwipo_number", body.get("number"))
    if deal_id and deal_id.isdigit():
        number = deal_id
        deal_id = ""
    try:
        number_i = int(number) if number not in (None, "") else None
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Número do negócio inválido."}), 400
    if not deal_id and number_i is None:
        return jsonify({"ok": False, "error": "Informe o id ou o número do negócio no Bwipo."}), 400
    try:
        row = refresh_deal(deal_id or None, number_i)
    except BwipoComercialError as e:
        return jsonify({"ok": False, "error": str(e)}), e.status if e.status < 500 else 502
    return jsonify({
        "ok": True,
        "fonte": "bwipo",
        "lead_id": row.get("number"),
        "bwipo_id": row.get("id"),
        "nome_card": row.get("title") or row.get("contact_name"),
        "rgm": row.get("rgm_norm"),
        "pipeline": row.get("pipeline_name"),
        "status": row.get("stage_name") or row.get("status"),
        "owner": row.get("owner_name"),
        "msg": "Espelho Bwipo atualizado.",
    })


@bwipo_bp.route("/api/bwipo/sync", methods=["POST"])
def api_bwipo_sync():
    if not configured():
        return jsonify({"ok": False, "error": "BWIPO_COMERCIAL_API_TOKEN não configurado no .env"}), 500
    if not _sync_lock.acquire(blocking=False):
        return jsonify({"ok": False, "error": "Já existe um sync Bwipo em andamento."}), 409
    body = request.get_json(silent=True) or {}
    mode = (body.get("mode") or "delta").strip().lower()
    if mode not in ("delta", "full"):
        mode = "delta"
    task_id = str(uuid.uuid4())[:8]
    task = {
        "id": task_id,
        "mode": mode,
        "status": "running",
        "progress": 0,
        "message": "Iniciando…",
        "log": [],
        "cancelled": False,
        "started_at": time.time(),
    }
    _tasks[task_id] = task
    t = threading.Thread(target=_run_sync, args=(task, mode), daemon=True)
    t.start()
    return jsonify({"ok": True, "task_id": task_id})


@bwipo_bp.route("/api/bwipo/sync/cancel", methods=["POST"])
def api_bwipo_sync_cancel():
    body = request.get_json(silent=True) or {}
    task_id = body.get("task_id")
    task = _tasks.get(task_id) if task_id else None
    if not task:
        return jsonify({"ok": False, "error": "Tarefa não encontrada."}), 404
    task["cancelled"] = True
    return jsonify({"ok": True})


@bwipo_bp.route("/api/bwipo/task/<task_id>")
def api_bwipo_task(task_id):
    task = _tasks.get(task_id)
    if not task:
        return jsonify({"ok": False, "error": "Tarefa não encontrada."}), 404
    return jsonify({"ok": True, "data": {
        "id": task["id"],
        "mode": task["mode"],
        "status": task["status"],
        "progress": task["progress"],
        "message": task["message"],
        "log": task["log"][-80:],
    }})


def _search_bwipo(q: str, limit: int = 15) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    like = f"%{q}%"
    digits = "".join(c for c in q if c.isdigit())
    cur.execute(
        """
        SELECT d.id, d.number, d.title, d.stage_name, d.pipeline_name, d.owner_name,
               d.rgm_norm, d.cpf_norm, d.phone_norm, d.email_norm, d.contact_id,
               d.data_matricula, d.status, d.created_at,
               c.name AS contact_name
        FROM bwipo_deals d
        LEFT JOIN bwipo_contacts c ON c.id = d.contact_id
        WHERE NOT d.is_deleted AND (
            d.id = %s
            OR CAST(d.number AS TEXT) = %s
            OR d.title ILIKE %s
            OR c.name ILIKE %s
            OR d.rgm_norm = %s
            OR d.cpf_norm = %s
            OR d.phone_norm LIKE %s
            OR d.email_norm ILIKE %s
        )
        ORDER BY d.updated_at DESC NULLS LAST
        LIMIT %s
        """,
        (q, q, like, like, digits or None, digits.zfill(11) if digits else None,
         f"%{digits[-11:]}" if len(digits) >= 8 else None, like, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _search_kommo(q: str, limit: int = 15) -> list[dict]:
    try:
        conn = _pg_kommo()
    except Exception:
        return []
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    like = f"%{q}%"
    digits = "".join(c for c in q if c.isdigit())
    lead_id = int(digits) if digits.isdigit() and len(digits) < 12 else None
    cur.execute(
        """
        SELECT l.id, l.name, l.status_id, l.responsible_user_id, l.pipeline_id,
               l.created_at, l.closed_at,
               (
                   SELECT regexp_replace((cf.values_json->0)->>'value', '[^0-9]', '', 'g')
                   FROM lead_custom_field_values cf
                   WHERE cf.lead_id = l.id AND lower(cf.field_name) = 'rgm' LIMIT 1
               ) AS rgm,
               (
                   SELECT regexp_replace((cf.values_json->0)->>'value', '[^0-9]', '', 'g')
                   FROM lead_custom_field_values cf
                   WHERE cf.lead_id = l.id AND lower(cf.field_name) = 'cpf' LIMIT 1
               ) AS cpf
        FROM leads l
        WHERE COALESCE(l.is_deleted, FALSE) = FALSE AND (
            (%s IS NOT NULL AND l.id = %s)
            OR l.name ILIKE %s
            OR EXISTS (
                SELECT 1 FROM lead_custom_field_values cf
                WHERE cf.lead_id = l.id
                  AND lower(cf.field_name) IN ('rgm', 'cpf')
                  AND regexp_replace((cf.values_json->0)->>'value', '[^0-9]', '', 'g') = %s
            )
        )
        ORDER BY l.updated_at DESC NULLS LAST
        LIMIT %s
        """,
        (lead_id, lead_id, like, digits or None, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


@bwipo_bp.route("/api/bwipo/depara")
def api_bwipo_depara_search():
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT kommo_lead_id, bwipo_deal_id, bwipo_contact_id, match_type, match_key,
                   kommo_name, bwipo_name, kommo_rgm, bwipo_rgm, confidence, updated_at
            FROM bwipo_kommo_depara
            ORDER BY updated_at DESC
            LIMIT 40
            """
        )
        rows = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) AS c FROM bwipo_kommo_depara")
        total = cur.fetchone()["c"]
        conn.close()
        return jsonify({"ok": True, "data": {"matches": rows, "total": total, "kommo": [], "bwipo": []}})

    bwipo = _search_bwipo(q)
    kommo = _search_kommo(q)
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    ids = [r["id"] for r in kommo] + [int(x) for x in [q] if q.isdigit()]
    matches = []
    if ids:
        cur.execute(
            """
            SELECT * FROM bwipo_kommo_depara
            WHERE kommo_lead_id = ANY(%s)
               OR bwipo_deal_id = %s
            """,
            (ids, q),
        )
        matches = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify({"ok": True, "data": {"q": q, "kommo": kommo, "bwipo": bwipo, "matches": matches}})


@bwipo_bp.route("/api/bwipo/depara", methods=["POST"])
def api_bwipo_depara_link():
    body = request.get_json(silent=True) or {}
    try:
        kid = int(body.get("kommo_lead_id"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "kommo_lead_id inválido"}), 400
    did = (body.get("bwipo_deal_id") or "").strip()
    if not did:
        return jsonify({"ok": False, "error": "bwipo_deal_id obrigatório"}), 400
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT contact_id, title, rgm_norm FROM bwipo_deals WHERE id = %s", (did,))
    deal = cur.fetchone()
    if not deal:
        conn.close()
        return jsonify({"ok": False, "error": "Negócio Bwipo não encontrado no espelho. Rode o sync."}), 404
    cur.execute(
        """
        INSERT INTO bwipo_kommo_depara (
            kommo_lead_id, bwipo_deal_id, bwipo_contact_id, match_type, match_key,
            kommo_name, bwipo_name, kommo_rgm, bwipo_rgm, confidence, created_at, updated_at
        ) VALUES (%s,%s,%s,'manual','manual',%s,%s,NULL,%s,'exact',NOW(),NOW())
        ON CONFLICT (kommo_lead_id) DO UPDATE SET
            bwipo_deal_id = EXCLUDED.bwipo_deal_id,
            bwipo_contact_id = EXCLUDED.bwipo_contact_id,
            match_type = 'manual', match_key = 'manual',
            bwipo_name = EXCLUDED.bwipo_name, bwipo_rgm = EXCLUDED.bwipo_rgm,
            confidence = 'exact', updated_at = NOW()
        """,
        (kid, did, deal[0], body.get("kommo_name"), deal[1], deal[2]),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


_PRINCIPAL_SQL = (
    "(position('principal' in lower(COALESCE(d.pipeline_name, ''))) > 0"
    " OR COALESCE(d.pipeline_name, '') = '')"
)
_DM_SQL = """(
    CASE
        WHEN d.data_matricula ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN to_date(d.data_matricula, 'DD/MM/YYYY')
        WHEN d.data_matricula ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN left(d.data_matricula, 10)::date
        ELSE NULL
    END
)"""
_NIVEL_SQL = (
    "CASE WHEN COALESCE(d.curso, '') ~* 'p[oó]s|mba|lato' "
    "THEN 'Pós-Graduação' ELSE 'Graduação' END"
)
_EVASAO_SQL = "COALESCE(d.situacao, '') ~* 'cancel|tranc|desist|evad|transfer'"


def _painel_filters(args):
    where = ["NOT d.is_deleted", _PRINCIPAL_SQL]
    params = []
    owner = (args.get("owner") or "").strip()
    if owner == "Sem responsável":
        where.append("(d.owner_id IS NULL OR d.owner_id = '')")
    elif owner:
        where.append("d.owner_name = %s")
        params.append(owner)
    if args.get("ganho") in ("1", "true"):
        where.append("COALESCE(s.is_won, FALSE)")
    dt_ini = (args.get("dt_ini") or "").strip()
    dt_fim = (args.get("dt_fim") or "").strip()
    if dt_ini:
        where.append(f"{_DM_SQL} >= %s")
        params.append(dt_ini)
    if dt_fim:
        where.append(f"{_DM_SQL} <= %s")
        params.append(dt_fim)
    q = (args.get("q") or "").strip()
    if q:
        where.append("(d.title ILIKE %s OR d.rgm_norm = %s OR CAST(d.number AS TEXT) = %s OR d.phone_norm LIKE %s)")
        params.extend([f"%{q}%", q, q, f"%{q[-8:]}%"])
    return " AND ".join(where), params


def _painel_scope(args):
    """Recorte do painel. Data = data de matrícula do negócio, não a data do sync."""
    where = ["NOT d.is_deleted", _PRINCIPAL_SQL]
    params: list = []
    polo = (args.get("polo") or "").strip()
    if polo:
        where.append("COALESCE(NULLIF(d.polo, ''), 'Sem polo') = %s")
        params.append(polo)
    nivel = (args.get("nivel") or "").strip()
    if nivel:
        where.append(f"{_NIVEL_SQL} = %s")
        params.append(nivel)
    owner = (args.get("owner") or "").strip()
    if owner == "Sem responsável":
        where.append("(d.owner_id IS NULL OR d.owner_id = '')")
    elif owner:
        where.append("d.owner_name = %s")
        params.append(owner)
    dt_ini = (args.get("dt_ini") or "").strip()
    dt_fim = (args.get("dt_fim") or "").strip()
    if dt_ini:
        where.append(f"{_DM_SQL} >= %s")
        params.append(dt_ini)
    if dt_fim:
        where.append(f"{_DM_SQL} <= %s")
        params.append(dt_fim)
    return " AND ".join(where), params, dt_ini, dt_fim


_ADMIN_SISTEMA = "Admin Sistema"


def _bwipo_owner_por_rgm(rgms: list[str]) -> dict[str, tuple]:
    """RGM → (kommo_user_id do dono pelo depara, nome no Bwipo). Negócio em Ganho primeiro."""
    if not rgms:
        return {}
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (d.rgm_norm)
            d.rgm_norm, ud.kommo_user_id, NULLIF(btrim(d.owner_name), '')
        FROM bwipo_deals d
        LEFT JOIN bwipo_stages s ON s.id = d.stage_id
        LEFT JOIN bwipo_kommo_user_depara ud ON ud.bwipo_user_id = d.owner_id
        WHERE NOT d.is_deleted
          AND d.rgm_norm = ANY(%s)
          AND NULLIF(btrim(d.owner_id), '') IS NOT NULL
        ORDER BY d.rgm_norm,
                 CASE WHEN COALESCE(s.is_won, FALSE) THEN 0 ELSE 1 END,
                 d.updated_at DESC NULLS LAST
        """,
        (rgms,),
    )
    out = {rgm: (int(uid) if uid else None, name) for rgm, uid, name in cur.fetchall()}
    conn.close()
    return out


def _tombamentos() -> dict[int, Any]:
    """kommo_user_id → data a partir da qual o consultor trabalha no Bwipo."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT kommo_user_id, desde FROM bwipo_consultor_tombamento")
    out = {int(uid): desde for uid, desde in cur.fetchall()}
    conn.close()
    return out


def _as_date(v):
    from datetime import date as _date
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, _date):
        return v
    s = str(v or "")[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        try:
            return datetime.strptime(s, "%d/%m/%Y").date()
        except ValueError:
            return None


def _atribuir_rgms(rgm_datas: dict[str, Any]) -> tuple[dict, dict, dict]:
    """Dono de cada venda: manual > Bwipo (consultor tombado, a partir da data dele) > Kommo > Bwipo > Admin.

    Devolve (rgm → chave do agente, chave → nome, rgm → fonte). A chave é o
    kommo_user_id; dono do Bwipo sem depara vira "bw:<nome>".
    """
    from routes.comercial_rgm import (
        _admin_sistema_uid,
        _consultor_reassign_map,
        _crgm_conflito_overrides,
        _crgm_kommo_lookup_rgms,
        _fetch_kommo_user_names,
        _normalize_rgm,
    )

    rgms = sorted(rgm_datas)
    admin = _admin_sistema_uid()
    manual = _crgm_conflito_overrides()
    kommo, nomes = _crgm_kommo_lookup_rgms(rgms)
    bwipo = _bwipo_owner_por_rgm(rgms)
    tomb = _tombamentos()
    reassign = _consultor_reassign_map()

    dono, fonte = {}, {}
    for rgm in rgms:
        nk = _normalize_rgm(rgm)
        b_uid, b_nome = bwipo.get(rgm, (None, None))
        k_uid = kommo.get(nk)
        dm = _as_date(rgm_datas.get(rgm))
        if nk in manual:
            key, src = manual[nk], "manual"
        elif b_uid and b_uid in tomb and dm and dm >= tomb[b_uid]:
            key, src = b_uid, "bwipo"
        elif k_uid:
            key, src = int(k_uid), "kommo"
        elif b_uid:
            key, src = b_uid, "bwipo"
        elif b_nome:
            key, src = f"bw:{b_nome}", "bwipo"
        else:
            key, src = admin, "sem_dono"
        if isinstance(key, int) and key in reassign:
            key = reassign[key]
        dono[rgm] = key
        fonte[rgm] = src

    uids = {k for k in dono.values() if isinstance(k, int)} | {admin}
    faltando = [u for u in uids if u not in nomes]
    if faltando:
        nomes.update(_fetch_kommo_user_names(faltando))
    nomes = {k: v for k, v in nomes.items() if k in uids}
    for k in dono.values():
        if isinstance(k, str):
            nomes[k] = k[3:]
    nomes[admin] = nomes.get(admin) or _ADMIN_SISTEMA
    return dono, nomes, fonte


def _bwipo_metas_por_uid(uids, dt_ini, dt_fim) -> dict:
    """Mesma meta do Comercial (campanha ativa, senão comercial_metas), por kommo_user_id."""
    conn = get_conn()
    cur = conn.cursor()
    fim = dt_fim or "9999-12-31"
    ini = dt_ini or "1900-01-01"
    metas = {}
    cur.execute(
        """
        SELECT pcm.kommo_user_id, pcm.meta, pcm.meta_intermediaria, pcm.supermeta
        FROM premiacao_campanha_meta pcm
        JOIN premiacao_campanha pc ON pc.id = pcm.campanha_id
        WHERE COALESCE(pc.ativa, TRUE)
          AND pc.dt_inicio <= %s AND pc.dt_fim >= %s
        """,
        (fim, ini),
    )
    for uid, meta, inter, super_ in cur.fetchall():
        metas[int(uid)] = {"meta": float(meta or 0), "intermediaria": float(inter or 0), "supermeta": float(super_ or 0)}
    cur.execute(
        """
        SELECT user_id, meta, COALESCE(meta_intermediaria, 0), COALESCE(supermeta, 0)
        FROM comercial_metas
        WHERE (categoria IS NULL OR categoria = 'matriculas')
          AND dt_inicio <= %s AND dt_fim >= %s
        """,
        (fim, ini),
    )
    for uid, meta, inter, super_ in cur.fetchall():
        uid = int(uid)
        if uid in metas:
            continue
        prev = metas.get(uid, {"meta": 0, "intermediaria": 0, "supermeta": 0})
        prev["meta"] += float(meta or 0)
        prev["intermediaria"] += float(inter or 0)
        prev["supermeta"] += float(super_ or 0)
        metas[uid] = prev
    if dt_ini and dt_fim:
        cur.execute(
            """
            SELECT def_meta_intermediaria, def_meta, def_supermeta
            FROM premiacao_campanha
            WHERE COALESCE(ativa, TRUE)
              AND dt_inicio <= %s::date AND dt_fim >= %s::date
            ORDER BY dt_inicio DESC
            LIMIT 1
            """,
            (dt_fim, dt_ini),
        )
        row = cur.fetchone()
        if row and any(row):
            default = {"intermediaria": float(row[0] or 0), "meta": float(row[1] or 0), "supermeta": float(row[2] or 0)}
            for uid in uids:
                if uid not in metas and any(default.values()):
                    metas[uid] = dict(default)
    conn.close()
    return {uid: metas.get(uid, {"meta": 0, "intermediaria": 0, "supermeta": 0}) for uid in uids}


def _bwipo_ticket_30(rgms: list) -> float:
    if not rgms:
        return 0.0
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (d.rgm_norm) d.value
        FROM bwipo_deals d
        LEFT JOIN bwipo_stages s ON s.id = d.stage_id
        WHERE NOT d.is_deleted
          AND d.rgm_norm = ANY(%s)
          AND d.value IS NOT NULL AND d.value > 0
        ORDER BY d.rgm_norm,
                 CASE WHEN COALESCE(s.is_won, FALSE) THEN 0 ELSE 1 END,
                 d.updated_at DESC NULLS LAST
        """,
        (rgms,),
    )
    prices = [float(r[0]) for r in cur.fetchall() if r[0]]
    conn.close()
    if not prices:
        return 0.0
    return round((sum(prices) / len(prices)) * 0.30, 2)


def _bwipo_leads_criados(dt_ini, dt_fim, admin_uid: int) -> dict:
    """Leads do Pipeline Principal criados no período, dia em BRT. Mesma chave do funil.

    A conversão do ranking é matrícula do período / esses leads, como no painel antigo.
    """
    conds = [
        "NOT d.is_deleted",
        "position('principal' in lower(COALESCE(p.name, d.pipeline_name, ''))) > 0",
    ]
    params: list = []
    if dt_ini:
        conds.append("(d.created_at AT TIME ZONE 'America/Sao_Paulo')::date >= %s")
        params.append(dt_ini)
    if dt_fim:
        conds.append("(d.created_at AT TIME ZONE 'America/Sao_Paulo')::date <= %s")
        params.append(dt_fim)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT ud.kommo_user_id, NULLIF(btrim(d.owner_name), ''), COUNT(*)
        FROM bwipo_deals d
        LEFT JOIN bwipo_pipelines p ON p.id = d.pipeline_id
        LEFT JOIN bwipo_kommo_user_depara ud ON ud.bwipo_user_id = d.owner_id
        WHERE {' AND '.join(conds)}
        GROUP BY 1, 2
        """,
        params,
    )
    out: dict = {}
    for uid, nome, total in cur.fetchall():
        key = int(uid) if uid else (f"bw:{nome}" if nome else admin_uid)
        out[key] = out.get(key, 0) + int(total or 0)
    conn.close()
    return out


def _bwipo_funil_estoque(admin_uid: int) -> dict:
    """Carteira atual do Pipeline Principal por agente (mesma chave do ranking). Não usa a data de matrícula."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ud.kommo_user_id, NULLIF(btrim(d.owner_name), ''),
               COUNT(*) FILTER (WHERE NOT COALESCE(s.is_won, FALSE) AND NOT COALESCE(s.is_lost, FALSE)) AS aberto,
               COUNT(*) FILTER (WHERE COALESCE(s.is_lost, FALSE)) AS perdido
        FROM bwipo_deals d
        LEFT JOIN bwipo_stages s ON s.id = d.stage_id
        LEFT JOIN bwipo_pipelines p ON p.id = d.pipeline_id
        LEFT JOIN bwipo_kommo_user_depara ud ON ud.bwipo_user_id = d.owner_id
        WHERE NOT d.is_deleted
          AND position('principal' in lower(COALESCE(p.name, d.pipeline_name, ''))) > 0
        GROUP BY 1, 2
        """
    )
    out: dict = {}
    for uid, nome, aberto, perdido in cur.fetchall():
        key = int(uid) if uid else (f"bw:{nome}" if nome else admin_uid)
        acc = out.setdefault(key, {"aberto": 0, "perdido": 0})
        acc["aberto"] += int(aberto or 0)
        acc["perdido"] += int(perdido or 0)
    conn.close()
    return out


_PAINEL_CACHE_TTL = 300
_painel_cache: dict = {}
_painel_cache_lock = threading.Lock()
_painel_refreshing: set = set()
_painel_base_lock = threading.Lock()


def clear_painel_response_cache():
    with _painel_cache_lock:
        _painel_cache.clear()
        _painel_refreshing.clear()
    try:
        from routes.minha_performance import _bwipo_periodo_cache, _bwipo_periodo_lock
        with _bwipo_periodo_lock:
            _bwipo_periodo_cache.clear()
    except Exception:
        logger.exception("limpar cache da minha performance")


def _ensure_painel_base_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bwipo_painel_base (
            rgm TEXT PRIMARY KEY,
            nome TEXT NOT NULL DEFAULT '',
            situacao TEXT NOT NULL DEFAULT '',
            data_matricula DATE,
            polo TEXT NOT NULL DEFAULT '',
            nivel TEXT NOT NULL DEFAULT '',
            ciclo TEXT NOT NULL DEFAULT '',
            tipo_matricula TEXT NOT NULL DEFAULT '',
            turma TEXT NOT NULL DEFAULT '',
            sumiu BOOLEAN NOT NULL DEFAULT FALSE,
            dono_key TEXT,
            dono_nome TEXT NOT NULL DEFAULT '',
            fonte TEXT NOT NULL DEFAULT '',
            built_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_bwipo_painel_base_data ON bwipo_painel_base (data_matricula)"
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bwipo_painel_meta (
            id INTEGER PRIMARY KEY,
            ciclo_prefix INTEGER,
            built_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def _painel_base_count() -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.bwipo_painel_base')")
        if not cur.fetchone()[0]:
            return 0
        cur.execute("SELECT COUNT(*) FROM bwipo_painel_base")
        return int(cur.fetchone()[0] or 0)
    except Exception:
        logger.exception("contar bwipo_painel_base")
        return 0
    finally:
        conn.close()


def _painel_ciclo_prefix():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.bwipo_painel_meta')")
        if not cur.fetchone()[0]:
            return None
        cur.execute("SELECT ciclo_prefix FROM bwipo_painel_meta WHERE id = 1")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        logger.exception("prefixo do painel")
        return None
    finally:
        conn.close()


def _painel_dominant_prefix(periodo_rgms):
    from routes.comercial_rgm import _compute_dominant_rgm_prefix, _crgm_ciclo_dominant_prefix

    periodo_pfx = _compute_dominant_rgm_prefix(periodo_rgms)
    ciclo_pfx = _painel_ciclo_prefix()
    if ciclo_pfx is None:
        ciclo_pfx = _crgm_ciclo_dominant_prefix()
    candidatos = [p for p in (periodo_pfx, ciclo_pfx) if p is not None]
    return min(candidatos) if candidatos else None


def _dono_key_value(raw):
    if raw is None or raw == "":
        return None
    text = str(raw)
    return int(text) if text.isdigit() else text


def _rebuild_painel_base_locked() -> bool:
    """Quem chama já está com _painel_base_lock. Não apaga a tabela se a leitura vier vazia."""
    try:
        from routes.comercial_rgm import _crgm_periodo_data_oficial

        from datetime import date as _date
        rows = _crgm_periodo_data_oficial(
            dt_ini="2018-01-01",
            dt_fim=_date.today().isoformat(),
            mark_missing_as_transferido=True,
        ) or []
        if not rows:
            logger.warning("bwipo painel base: leitura vazia, tabela anterior mantida")
            return False
        datas = {row["rgm"]: row.get("data_matricula") for row in rows if row.get("rgm")}
        dono, nomes, fonte = _atribuir_rgms(datas)
        built = datetime.now(timezone.utc)
        payload = []
        for row in rows:
            rgm = row.get("rgm")
            if not rgm:
                continue
            key = dono.get(rgm)
            payload.append((
                rgm,
                row.get("nome") or "",
                row.get("situacao") or "",
                _as_date(row.get("data_matricula")) if row.get("data_matricula") else None,
                row.get("polo") or "",
                row.get("nivel") or "",
                row.get("ciclo") or "",
                row.get("tipo_matricula") or "",
                row.get("turma") or "",
                bool(row.get("sumiu_do_csv")),
                str(key) if key is not None else None,
                nomes.get(key) or "",
                fonte.get(rgm) or "sem_dono",
                built,
            ))
        conn = get_conn()
        try:
            cur = conn.cursor()
            _ensure_painel_base_table(cur)
            cur.execute("DELETE FROM bwipo_painel_base")
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO bwipo_painel_base (
                    rgm, nome, situacao, data_matricula, polo, nivel, ciclo, tipo_matricula,
                    turma, sumiu, dono_key, dono_nome, fonte, built_at
                ) VALUES %s
                """,
                payload,
                page_size=1000,
            )
            from routes.comercial_rgm import _crgm_ciclo_dominant_prefix
            cur.execute(
                """
                INSERT INTO bwipo_painel_meta (id, ciclo_prefix, built_at)
                VALUES (1, %s, %s)
                ON CONFLICT (id) DO UPDATE
                  SET ciclo_prefix = EXCLUDED.ciclo_prefix, built_at = EXCLUDED.built_at
                """,
                (_crgm_ciclo_dominant_prefix(), built),
            )
            conn.commit()
        finally:
            conn.close()
        clear_painel_response_cache()
        logger.info("bwipo painel base: %s matrículas", len(payload))
        return True
    except Exception:
        logger.exception("bwipo painel base")
        return False


def rebuild_painel_base() -> bool:
    """Remonta a tabela do painel. Se outra montagem já estiver rodando, sai."""
    if not _painel_base_lock.acquire(blocking=False):
        return False
    try:
        return _rebuild_painel_base_locked()
    finally:
        _painel_base_lock.release()


def schedule_painel_base_rebuild() -> None:
    threading.Thread(target=rebuild_painel_base, name="bwipo-painel-base", daemon=True).start()


def ensure_painel_base() -> bool:
    if _painel_base_count() > 0:
        return True
    with _painel_base_lock:
        if _painel_base_count() > 0:
            return True
        return _rebuild_painel_base_locked()


def painel_base_note_pin(rgm: str, user_id: int, user_name: str) -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.bwipo_painel_base')")
        if cur.fetchone()[0]:
            cur.execute(
                """
                UPDATE bwipo_painel_base
                   SET dono_key = %s, dono_nome = %s, fonte = 'manual'
                 WHERE rgm = %s
                """,
                (str(int(user_id)), user_name or "", rgm),
            )
        conn.commit()
    except Exception:
        logger.exception("painel base pin")
    finally:
        conn.close()
    clear_painel_response_cache()


def refresh_painel_base_rgm(rgm: str) -> None:
    """Recalcula o dono de um RGM depois que a fixação manual sai."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.bwipo_painel_base')")
        if not cur.fetchone()[0]:
            return
        cur.execute("SELECT data_matricula FROM bwipo_painel_base WHERE rgm = %s", (rgm,))
        found = cur.fetchone()
        if not found:
            return
        data = found[0].isoformat() if found[0] else None
        dono, nomes, fonte = _atribuir_rgms({rgm: data})
        key = dono.get(rgm)
        cur.execute(
            """
            UPDATE bwipo_painel_base
               SET dono_key = %s, dono_nome = %s, fonte = %s
             WHERE rgm = %s
            """,
            (
                str(key) if key is not None else None,
                nomes.get(key) or "",
                fonte.get(rgm) or "sem_dono",
                rgm,
            ),
        )
        conn.commit()
    except Exception:
        logger.exception("refresh painel base rgm")
    finally:
        conn.close()
    clear_painel_response_cache()


def load_painel_base(dt_ini, dt_fim, nivel, turma, ciclo):
    """Linhas do recorte já com dono. None se a tabela ainda não existe."""
    if _painel_base_count() <= 0:
        return None
    conds = []
    params = []
    if dt_ini:
        conds.append("data_matricula >= %s")
        params.append(dt_ini)
    if dt_fim:
        conds.append("data_matricula <= %s")
        params.append(dt_fim)
    if nivel:
        conds.append("nivel = %s")
        params.append(nivel)
    if turma:
        conds.append("turma = %s")
        params.append(turma)
    if ciclo:
        conds.append("ciclo = %s")
        params.append(ciclo)
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT rgm, nome, situacao, data_matricula, polo, nivel, ciclo, tipo_matricula,
                   turma, sumiu, dono_key, dono_nome, fonte
              FROM bwipo_painel_base
              {where}
            """,
            params,
        )
        fetched = cur.fetchall()
    finally:
        conn.close()
    rows = []
    dono = {}
    nomes = {}
    fonte = {}
    for rgm, nome, situacao, dm, polo, nivel_v, ciclo_v, tipo, turma_v, sumiu, key, dono_nome, src in fetched:
        if not rgm:
            continue
        rows.append({
            "rgm": rgm,
            "nome": nome or "",
            "situacao": situacao or "",
            "data_matricula": dm.isoformat() if dm else None,
            "polo": polo or "",
            "nivel": nivel_v or "",
            "ciclo": ciclo_v or "",
            "tipo_matricula": tipo or "",
            "turma": turma_v or "",
            "sumiu_do_csv": bool(sumiu),
        })
        parsed = _dono_key_value(key)
        if parsed is not None:
            dono[rgm] = parsed
            if dono_nome:
                nomes[parsed] = dono_nome
        fonte[rgm] = src or "sem_dono"
    return rows, dono, nomes, fonte


def _painel_bruto_janela(dt_ini, dt_fim, nivel, turma, ciclo, polo, owner):
    """Matrículas do intervalo, com os mesmos filtros da página. None se a tabela ainda não existe."""
    from helpers import normalize_polo_display
    from routes.comercial_rgm import _admin_sistema_uid

    loaded = load_painel_base(dt_ini, dt_fim, nivel, turma, ciclo)
    if loaded is None:
        return None
    rows, dono, nomes, _fonte = loaded
    if polo:
        rows = [
            row for row in rows
            if normalize_polo_display(row.get("polo") or "") == polo or (row.get("polo") or "") == polo
        ]
    if owner:
        admin_nome = nomes.get(_admin_sistema_uid()) or _ADMIN_SISTEMA
        rows = [
            row for row in rows
            if nomes.get(dono.get(row.get("rgm")), admin_nome) == owner
        ]
    total = len({row["rgm"] for row in rows if row.get("rgm")})
    if total or owner:
        return total
    from routes.comercial_rgm import _crgm_count_bruto_from_table, _pg
    conn = _pg()
    try:
        return _crgm_count_bruto_from_table(conn, dt_ini, dt_fim, polo, nivel, turma)
    finally:
        conn.close()


def _painel_cache_key(dt_ini, dt_fim, polo, nivel, owner, turma, ciclo):
    return (
        dt_ini or "", dt_fim or "", polo or "", nivel or "",
        owner or "", turma or "", ciclo or "",
    )


def _refresh_painel_cache(key, args):
    with _painel_cache_lock:
        if key in _painel_refreshing:
            return
        _painel_refreshing.add(key)

    def _run():
        try:
            payload = _painel_payload(*args)
            if payload.get("ok"):
                with _painel_cache_lock:
                    _painel_cache[key] = (time.time(), payload)
        except Exception:
            logger.exception("refresh painel bwipo")
        finally:
            with _painel_cache_lock:
                _painel_refreshing.discard(key)

    threading.Thread(target=_run, name="bwipo-painel-cache", daemon=True).start()


@bwipo_bp.route("/api/bwipo/painel")
def api_bwipo_painel():
    """Mesma matrícula do Dashboard Comercial. Sem responsável no Bwipo → Admin Sistema."""
    dt_ini = (request.args.get("dt_ini") or "").strip() or None
    dt_fim = (request.args.get("dt_fim") or "").strip() or None
    polo = (request.args.get("polo") or "").strip()
    nivel = (request.args.get("nivel") or "").strip() or None
    owner = (request.args.get("owner") or "").strip()
    turma = (request.args.get("turma") or "").strip() or None
    ciclo = (request.args.get("ciclo") or "").strip() or None
    key = _painel_cache_key(dt_ini, dt_fim, polo, nivel, owner, turma, ciclo)
    args = (dt_ini, dt_fim, polo, nivel, owner, turma, ciclo)
    now = time.time()
    with _painel_cache_lock:
        hit = _painel_cache.get(key)
    if hit and now - hit[0] < _PAINEL_CACHE_TTL:
        return jsonify(hit[1])
    if hit:
        _refresh_painel_cache(key, args)
        return jsonify(hit[1])
    payload = _painel_payload(*args)
    if payload.get("ok"):
        with _painel_cache_lock:
            _painel_cache[key] = (time.time(), payload)
        return jsonify(payload)
    return jsonify(payload), 500


def _painel_payload(dt_ini, dt_fim, polo, nivel, owner, turma, ciclo):
    try:
        from collections import defaultdict

        from helpers import normalize_polo_display
        from routes.comercial_rgm import (
            _crgm_build_periodo_sets,
            _crgm_fora_padrao_rows,
            _crgm_periodo_data_oficial,
            _load_outlier_contagem_overrides,
            _rgm_conta_para_venda,
        )

        loaded = load_painel_base(dt_ini, dt_fim, nivel, turma, ciclo)
        if loaded is None and ensure_painel_base():
            loaded = load_painel_base(dt_ini, dt_fim, nivel, turma, ciclo)
        dono = nomes = fonte = None
        if loaded is not None:
            ciclo_all, dono, nomes, fonte = loaded
        else:
            ciclo_all = _crgm_periodo_data_oficial(
                dt_ini=dt_ini, dt_fim=dt_fim, nivel=nivel, turma=turma, ciclo_filter=ciclo,
                mark_missing_as_transferido=True,
            )
        if polo:
            ciclo_all = [
                row for row in ciclo_all
                if normalize_polo_display(row.get("polo") or "") == polo
                or (row.get("polo") or "") == polo
            ]
        (
            periodo_rows, rgms_periodo, rgms_bruto, evasao_rows,
            day_rgms, _day_bruto, polo_rgms,
        ) = _crgm_build_periodo_sets(ciclo_all, dt_ini, dt_fim)
        dom = _painel_dominant_prefix(list(rgms_periodo) or list(rgms_bruto))
        overrides = _load_outlier_contagem_overrides()
        contando = {r for r in rgms_periodo if _rgm_conta_para_venda(r, dom, overrides)}
        fora_rows = _crgm_fora_padrao_rows(periodo_rows, dom, overrides, apenas_nao_conta=True)
        from routes.comercial_rgm import _admin_sistema_uid, _consultor_hidden_uids

        if dono is None:
            rgm_datas = {row["rgm"]: row.get("data_matricula") for row in periodo_rows if row.get("rgm")}
            dono, nomes, fonte = _atribuir_rgms(rgm_datas)
        admin_uid = _admin_sistema_uid()
        admin_nome = nomes.get(admin_uid) or _ADMIN_SISTEMA
        nome_key = {v: k for k, v in nomes.items()}
        ocultos = _consultor_hidden_uids()

        def _agente(rgm: str) -> str:
            return nomes.get(dono.get(rgm), admin_nome)

        agentes = sorted({_agente(r) for r in rgms_bruto} | {admin_nome})
        if owner:
            periodo_rows = [row for row in periodo_rows if _agente(row.get("rgm")) == owner]
            rgms_bruto = {row["rgm"] for row in periodo_rows if row.get("rgm")}
            rgms_periodo = {r for r in rgms_periodo if r in rgms_bruto}
            contando = {r for r in contando if r in rgms_bruto}
            evasao_rows = [row for row in evasao_rows if _agente(row.get("rgm")) == owner]
            fora_rows = [row for row in fora_rows if _agente(row.get("rgm")) == owner]
            day_rgms = {d: s & contando for d, s in day_rgms.items()}
            polo_rgms = {p: s & contando for p, s in polo_rgms.items()}

        ranking_acc = defaultdict(lambda: {"matriculas": 0, "evasao": 0, "pos": 0})
        for row in periodo_rows:
            rgm = row.get("rgm")
            if not rgm:
                continue
            ag = _agente(rgm)
            bucket = ranking_acc[ag]
            if rgm in contando:
                bucket["matriculas"] += 1
                if row.get("nivel") == "Pós-Graduação":
                    bucket["pos"] += 1
            elif row.get("situacao") != "EM CURSO":
                bucket["evasao"] += 1
        ranking = []
        for ag, bucket in ranking_acc.items():
            mat = bucket["matriculas"]
            ev = bucket["evasao"]
            ranking.append({
                "agente": ag,
                "matriculas": mat,
                "aberto": bucket.get("aberto", 0),
                "perdido": bucket.get("perdido", 0),
                "evasao": ev,
                "pos": bucket["pos"],
                "meta": bucket.get("meta", 0),
                "intermediaria": bucket.get("intermediaria", 0),
                "supermeta": bucket.get("supermeta", 0),
                "conv": 0,
            })
        ranking = [r for r in ranking if nome_key.get(r["agente"]) not in ocultos]
        try:
            metas_uid = _bwipo_metas_por_uid(
                [k for k in nomes if isinstance(k, int)], dt_ini, dt_fim,
            )
        except Exception:
            logger.exception("bwipo metas")
            metas_uid = {}
        try:
            funil_key = _bwipo_funil_estoque(admin_uid)
        except Exception:
            logger.exception("bwipo funil")
            funil_key = {}
        try:
            leads_key = _bwipo_leads_criados(dt_ini, dt_fim, admin_uid)
        except Exception:
            logger.exception("bwipo leads criados")
            leads_key = {}
        funil = {}
        for k, v in funil_key.items():
            nm = nomes.get(k)
            if nm is None:
                continue
            acc = funil.setdefault(nm, {"aberto": 0, "perdido": 0})
            acc["aberto"] += v["aberto"]
            acc["perdido"] += v["perdido"]
        if owner:
            funil = {k: v for k, v in funil.items() if k == owner}
        for item in ranking:
            key = nome_key.get(item["agente"])
            item["user_id"] = key if isinstance(key, int) else None
            item["bwipo_owner"] = key[3:] if isinstance(key, str) else None
            meta = metas_uid.get(key) or {"meta": 0, "intermediaria": 0, "supermeta": 0}
            est = funil.get(item["agente"]) or {"aberto": 0, "perdido": 0}
            leads = leads_key.get(key, 0) if key is not None else 0
            item["meta"] = meta["meta"]
            item["intermediaria"] = meta["intermediaria"]
            item["supermeta"] = meta["supermeta"]
            item["aberto"] = est["aberto"]
            item["perdido"] = est["perdido"]
            item["conv"] = round(100 * item["matriculas"] / leads, 1) if leads else 0
        ranking.sort(key=lambda r: (-r["matriculas"], -r["evasao"], r["agente"]))
        dias = len([s for s in day_rgms.values() if s & contando]) or 1
        em_curso = len(contando)
        polos = []
        for nome, rgms in polo_rgms.items():
            n = len(rgms & contando)
            if n:
                polos.append({"polo": nome, "total": n})
        polos.sort(key=lambda p: (-p["total"], p["polo"]))
        evolucao = [
            {"dia": d.isoformat() if hasattr(d, "isoformat") else str(d)[:10], "total": len(s & contando)}
            for d, s in sorted(day_rgms.items())
            if s & contando
        ]
        pfx_acc = defaultdict(int)
        for row in fora_rows:
            pfx_acc[row.get("prefixo") or "?"] += 1
        fora_rgms = {row.get("rgm") for row in fora_rows}
        linhas = []
        for row in periodo_rows:
            rgm = row.get("rgm")
            if not rgm:
                continue
            linhas.append({
                "rgm": rgm,
                "nome": row.get("nome") or "",
                "situacao": row.get("situacao") or "",
                "data": str(row.get("data_matricula") or "")[:10],
                "polo": normalize_polo_display(row.get("polo") or "") or (row.get("polo") or ""),
                "nivel": row.get("nivel") or "",
                "tipo": row.get("tipo_matricula") or "",
                "agente": _agente(rgm),
                "fonte": fonte.get(rgm, "sem_dono"),
                "conta": rgm in contando,
                "fora": rgm in fora_rgms,
            })
        try:
            ticket = _bwipo_ticket_30(sorted(contando))
        except Exception:
            logger.exception("bwipo ticket")
            ticket = 0
        base_total = funil if owner else funil_key
        aberto_total = sum(v["aberto"] for v in base_total.values())
        perdido_total = sum(v["perdido"] for v in base_total.values())
        vendas_6m = vendas_1a = None
        compare_6m = compare_1a = None
        pct_6m = pct_1a = None
        delta_6m = delta_1a = None
        atual = len(rgms_bruto)
        if dt_ini and dt_fim:
            from datetime import date as _date
            from routes.comercial_rgm import _shift_months
            try:
                d_ini = _date.fromisoformat(dt_ini)
                d_fim = _date.fromisoformat(dt_fim)
                i6, f6 = _shift_months(d_ini, -6), _shift_months(d_fim, -6)
                i1, f1 = _shift_months(d_ini, -12), _shift_months(d_fim, -12)
                vendas_6m = _painel_bruto_janela(i6.isoformat(), f6.isoformat(), nivel, turma, ciclo, polo, owner)
                vendas_1a = _painel_bruto_janela(i1.isoformat(), f1.isoformat(), nivel, turma, ciclo, polo, owner)
                compare_6m = f"{i6.strftime('%d/%m/%Y')} a {f6.strftime('%d/%m/%Y')}"
                compare_1a = f"{i1.strftime('%d/%m/%Y')} a {f1.strftime('%d/%m/%Y')}"
                if vendas_6m:
                    pct_6m = round((atual / vendas_6m - 1) * 100, 1)
                    delta_6m = atual - vendas_6m
                if vendas_1a:
                    pct_1a = round((atual / vendas_1a - 1) * 100, 1)
                    delta_1a = atual - vendas_1a
            except Exception:
                logger.exception("bwipo comparativo")
        return {
            "ok": True,
            "kpis": {
                "matriculas": len(rgms_bruto),
                "em_curso": em_curso,
                "evasao": len(evasao_rows) if not owner else sum(r["evasao"] for r in ranking),
                "aberto": aberto_total,
                "perdido": perdido_total,
                "sem_data": 0,
                "fora_padrao": len(fora_rows),
                "media": round(em_curso / dias, 1),
                "dias": dias,
                "ticket": ticket,
                "prefixo": dom or "",
                "vendas_6m": vendas_6m,
                "vendas_1a": vendas_1a,
                "pct_6m": pct_6m,
                "pct_1a": pct_1a,
                "delta_6m": delta_6m,
                "delta_1a": delta_1a,
                "compare_6m_period": compare_6m,
                "compare_1a_period": compare_1a,
            },
            "linhas": linhas,
            "prefixos": [{"pfx": p, "n": n} for p, n in sorted(pfx_acc.items())],
            "ranking": ranking,
            "polos": polos,
            "evolucao": evolucao,
            "opcoes": {
                "polos": sorted({normalize_polo_display(row.get("polo") or "") for row in ciclo_all if row.get("polo")} - {""}),
                "agentes": agentes,
            },
        }
    except Exception as e:
        logger.exception("bwipo painel")
        return {"ok": False, "error": str(e)}


@bwipo_bp.route("/api/bwipo/painel/extras")
def api_bwipo_painel_extras():
    """YTD e inscritos do Match no mesmo recorte do painel antigo. Roda separado para não atrasar o ranking."""
    from datetime import date as _date

    from routes.comercial_rgm import _crgm_count_bruto_compare, _pg

    dt_fim = (request.args.get("dt_fim") or "").strip() or _date.today().isoformat()
    polo = (request.args.get("polo") or "").strip() or None
    nivel = (request.args.get("nivel") or "").strip() or None
    turma = (request.args.get("turma") or "").strip() or None
    dt_ini = (request.args.get("dt_ini") or "").strip() or None
    try:
        ano = int(dt_fim[:4])
    except ValueError:
        return jsonify({"ok": False, "error": "Data inválida"}), 400
    conn = _pg()
    try:
        ytd = _crgm_count_bruto_compare(conn, f"{ano}-01-01", dt_fim, polo, nivel, turma)
        cur = conn.cursor()
        wh, params = [], []
        ini = dt_ini or f"{ano}-01-01"
        wh.append("data_inscr >= %s")
        params.append(ini)
        wh.append("data_inscr <= %s")
        params.append(dt_fim)
        if polo:
            wh.append("polo_normalizado = %s")
            params.append(polo)
        where = "WHERE " + " AND ".join(wh)
        try:
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT cpf) FROM (
                    SELECT cpf FROM mm_inscritos_hist {where}
                    UNION
                    SELECT cpf FROM mm_inscritos {where}
                ) sub WHERE cpf IS NOT NULL
                """,
                params + params,
            )
            inscritos = int(cur.fetchone()[0] or 0)
        except Exception:
            conn.rollback()
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM mm_inscritos_hist {where}", params)
            inscritos = int(cur.fetchone()[0] or 0)
        cur.close()
    finally:
        conn.close()
    return jsonify({"ok": True, "ytd": ytd, "inscritos": inscritos})


@bwipo_bp.route("/api/bwipo/painel/deals")
def api_bwipo_painel_deals():
    try:
        where, params = _painel_filters(request.args)
        limit = min(int(request.args.get("limit") or 80), 200)
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            f"""
            SELECT d.number, d.title, d.stage_name, d.owner_name, d.rgm_norm,
                   d.data_matricula, d.phone_norm, d.status, d.id
            FROM bwipo_deals d
            LEFT JOIN bwipo_stages s ON s.id = d.stage_id
            WHERE {where}
            ORDER BY d.updated_at DESC NULLS LAST
            LIMIT %s
            """,
            (*params, limit),
        )
        rows = []
        for r in cur.fetchall():
            item = dict(r)
            for key, val in list(item.items()):
                if hasattr(val, "isoformat"):
                    item[key] = val.isoformat()
            rows.append(item)
        conn.close()
        return jsonify({"ok": True, "deals": rows})
    except Exception as e:
        logger.exception("bwipo painel deals")
        return jsonify({"ok": False, "error": str(e)}), 500


def _candidatos_rgm(rgms: list[str]) -> dict[str, list[dict]]:
    """RGM → leads do Kommo e negócios do Bwipo que carregam esse RGM, com o dono de cada um."""
    from routes.comercial_rgm import _normalize_rgm

    out: dict[str, list[dict]] = {r: [] for r in rgms}
    if not rgms:
        return out
    kconn = _pg_kommo()
    try:
        kcur = kconn.cursor()
        kcur.execute(
            """
            SELECT v.rgm, l.id, l.responsible_user_id, u.name, l.status_id, ps.name
            FROM vw_leads_rgm v
            JOIN leads l ON l.id = v.lead_id AND NOT l.is_deleted
            LEFT JOIN users u ON u.id = l.responsible_user_id
            LEFT JOIN pipeline_statuses ps ON ps.id = l.status_id
            WHERE l.responsible_user_id IS NOT NULL AND v.rgm = ANY(%s)
            ORDER BY v.rgm, CASE WHEN l.status_id = 142 THEN 0 ELSE 1 END, l.id DESC
            """,
            (rgms,),
        )
        for rgm, lid, uid, nome, st, st_nome in kcur.fetchall():
            nk = _normalize_rgm(rgm)
            if nk not in out:
                continue
            out[nk].append({
                "origem": "kommo",
                "id": lid,
                "user_id": int(uid),
                "agente": nome or f"User #{uid}",
                "etapa": "Ganho" if st == 142 else "Perdido" if st == 143 else (st_nome or str(st)),
                "ganho": st == 142,
            })
        kcur.close()
    finally:
        kconn.close()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT d.rgm_norm, d.id, d.number, ud.kommo_user_id, NULLIF(btrim(d.owner_name), ''),
                   d.stage_name, COALESCE(s.is_won, FALSE)
            FROM bwipo_deals d
            LEFT JOIN bwipo_stages s ON s.id = d.stage_id
            LEFT JOIN bwipo_kommo_user_depara ud ON ud.bwipo_user_id = d.owner_id
            WHERE NOT d.is_deleted AND d.rgm_norm = ANY(%s)
            ORDER BY d.rgm_norm, CASE WHEN COALESCE(s.is_won, FALSE) THEN 0 ELSE 1 END,
                     d.updated_at DESC NULLS LAST
            """,
            (rgms,),
        )
        for rgm, did, number, uid, nome, etapa, won in cur.fetchall():
            if rgm not in out:
                continue
            out[rgm].append({
                "origem": "bwipo",
                "id": did,
                "number": number,
                "user_id": int(uid) if uid else None,
                "agente": nome or "Sem responsável",
                "etapa": etapa or "",
                "ganho": bool(won),
            })
    finally:
        conn.close()
    return out


def _resolucoes(rgms: list[str]) -> dict[str, dict]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT rgm, user_id, user_name, resolved_by, resolved_at FROM comercial_rgm_conflito_resolucao WHERE rgm = ANY(%s)",
        (rgms,),
    )
    out = {
        r[0]: {"user_id": r[1], "user_name": r[2], "por": r[3], "em": r[4].isoformat() if r[4] else None}
        for r in cur.fetchall()
    }
    conn.close()
    return out


def _rgms_body(body) -> list[str]:
    from routes.comercial_rgm import _normalize_rgm

    raw = body.get("rgms") or ([body.get("rgm")] if body.get("rgm") else [])
    return sorted({n for n in (_normalize_rgm(str(r)) for r in raw) if n and len(n) == 8})


@bwipo_bp.route("/api/bwipo/painel/rgm")
def api_bwipo_painel_rgm():
    """Consultar RGM: quem fica com a venda, por qual regra, e todos os leads/negócios com esse RGM."""
    rgms = _rgms_body({"rgm": request.args.get("rgm")})
    if not rgms:
        return jsonify({"ok": False, "error": "Informe um RGM com 8 dígitos."}), 400
    rgm = rgms[0]
    try:
        cands = _candidatos_rgm([rgm])[rgm]
        res = _resolucoes([rgm]).get(rgm)
        dono, nomes, fonte = _atribuir_rgms({rgm: None})
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM comercial_rgm_outlier_contagem WHERE rgm = %s", (rgm,))
        contando = cur.fetchone() is not None
        conn.close()
        return jsonify({
            "ok": True,
            "rgm": rgm,
            "agente": nomes.get(dono.get(rgm)),
            "user_id": dono.get(rgm) if isinstance(dono.get(rgm), int) else None,
            "fonte": fonte.get(rgm),
            "resolucao": res,
            "contar_venda": contando,
            "candidatos": cands,
        })
    except Exception as e:
        logger.exception("bwipo painel rgm")
        return jsonify({"ok": False, "error": str(e)}), 500


@bwipo_bp.route("/api/bwipo/painel/conflitos", methods=["POST"])
def api_bwipo_painel_conflitos():
    """RGMs do recorte com mais de um consultor em Ganho entre Kommo e Bwipo (Admin Sistema não disputa). Body: {rgms: [...]}."""
    rgms = _rgms_body(request.get_json(force=True, silent=True) or {})
    if not rgms:
        return jsonify({"ok": True, "conflitos": [], "total": 0, "total_nao_resolvidos": 0})
    try:
        from routes.comercial_rgm import _admin_sistema_uid

        admin = _admin_sistema_uid()
        cands = _candidatos_rgm(rgms)
        res = _resolucoes(rgms)
        conflitos = []
        for rgm, lst in cands.items():
            donos = {
                c["user_id"] or f"bw:{c['agente']}"
                for c in lst
                if c["ganho"] and c["agente"] != "Sem responsável" and c["user_id"] != admin
            }
            if len(donos) <= 1:
                continue
            conflitos.append({"rgm": rgm, "candidatos": lst, "resolucao": res.get(rgm)})
        conflitos.sort(key=lambda c: (c["resolucao"] is not None, c["rgm"]))
        return jsonify({
            "ok": True,
            "conflitos": conflitos,
            "total": len(conflitos),
            "total_nao_resolvidos": sum(1 for c in conflitos if not c["resolucao"]),
        })
    except Exception as e:
        logger.exception("bwipo painel conflitos")
        return jsonify({"ok": False, "error": str(e)}), 500


@bwipo_bp.route("/api/bwipo/painel/conflitos/resolver", methods=["POST", "DELETE"])
def api_bwipo_painel_conflitos_resolver():
    """POST {rgm, user_id, user_name} fixa a venda no consultor; DELETE {rgm} volta à regra automática."""
    from routes.comercial_rgm import _pin_rgm_attribution, clear_crgm_data_cache

    body = request.get_json(force=True, silent=True) or {}
    rgms = _rgms_body(body)
    if not rgms:
        return jsonify({"ok": False, "error": "RGM inválido."}), 400
    rgm = rgms[0]
    if request.method == "DELETE":
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM comercial_rgm_conflito_resolucao WHERE rgm = %s", (rgm,))
        conn.commit()
        conn.close()
        clear_crgm_data_cache(reason=f"bwipo conflito desfeito rgm={rgm}")
        refresh_painel_base_rgm(rgm)
        return jsonify({"ok": True, "rgm": rgm, "acao": "removido"})
    try:
        uid = int(body.get("user_id"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Escolha um consultor com id no Kommo."}), 400
    if not _pin_rgm_attribution(rgm, uid, str(body.get("user_name") or ""), resolved_by="conflito_bwipo"):
        return jsonify({"ok": False, "error": "Não foi possível gravar a decisão."}), 500
    clear_crgm_data_cache(reason=f"bwipo conflito rgm={rgm}")
    return jsonify({"ok": True, "rgm": rgm, "user_id": uid})


@bwipo_bp.route("/api/bwipo/painel/mini-sync", methods=["POST"])
def api_bwipo_painel_mini_sync():
    """Atualiza um lead Kommo ({lead_id}) ou um negócio Bwipo ({deal_id}|{number}) e, se estiver em
    Ganho com RGM, fixa a venda no responsável dele — mesmo efeito do mini-sync do painel antigo."""
    from routes.comercial_rgm import _pin_rgm_attribution, clear_crgm_data_cache, crgm_kommo_sync_lead

    body = request.get_json(force=True, silent=True) or {}
    if body.get("lead_id"):
        return crgm_kommo_sync_lead()
    try:
        number = int(body["number"]) if body.get("number") else None
        row = refresh_deal(deal_id=body.get("deal_id"), number=number)
    except BwipoComercialError as e:
        return jsonify({"ok": False, "error": str(e)}), getattr(e, "status", 400) or 400
    except Exception as e:
        logger.exception("bwipo mini-sync")
        return jsonify({"ok": False, "error": str(e)}), 500
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT d.number, d.title, d.rgm_norm, d.stage_name, COALESCE(s.is_won, FALSE),
               ud.kommo_user_id, d.owner_name
        FROM bwipo_deals d
        LEFT JOIN bwipo_stages s ON s.id = d.stage_id
        LEFT JOIN bwipo_kommo_user_depara ud ON ud.bwipo_user_id = d.owner_id
        WHERE d.id = %s
        """,
        (row["id"],),
    )
    number, title, rgm, etapa, won, uid, owner = cur.fetchone()
    conn.close()
    pinned = False
    if won and rgm and len(rgm) == 8 and uid:
        pinned = _pin_rgm_attribution(rgm, int(uid), owner or "", resolved_by="mini_sync_bwipo")
        clear_crgm_data_cache(reason=f"bwipo mini-sync rgm={rgm}")
    if pinned:
        msg = f"Negócio #{number} atualizado. Venda do RGM {rgm} fixada em {owner}."
    elif won and rgm and not uid:
        msg = f"Negócio #{number} atualizado, mas {owner or 'o responsável'} não tem depara com o Kommo — a venda não foi fixada."
    else:
        msg = f"Negócio #{number} atualizado ({etapa or 'sem etapa'}). Só negócio em Ganho com RGM fixa a venda."
    return jsonify({
        "ok": True, "number": number, "title": title, "rgm": rgm, "etapa": etapa,
        "agente": owner, "pinned": pinned, "msg": msg,
    })


@bwipo_bp.route("/api/bwipo/painel/contar-venda", methods=["POST", "DELETE"])
def api_bwipo_painel_contar_venda():
    """Admin: faz um RGM fora do padrão contar (POST) ou deixar de contar (DELETE) como venda."""
    from routes.comercial_rgm import clear_crgm_data_cache

    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "Apenas administradores podem executar esta ação"}), 403
    rgms = _rgms_body(request.get_json(force=True, silent=True) or {})
    if not rgms:
        return jsonify({"ok": False, "error": "RGM inválido."}), 400
    rgm = rgms[0]
    conn = get_conn()
    cur = conn.cursor()
    if request.method == "POST":
        cur.execute(
            """
            INSERT INTO comercial_rgm_outlier_contagem (rgm, counted_at, counted_by)
            VALUES (%s, NOW(), %s)
            ON CONFLICT (rgm) DO UPDATE SET counted_at = NOW(), counted_by = EXCLUDED.counted_by
            """,
            (rgm, session.get("username") or "admin"),
        )
    else:
        cur.execute("DELETE FROM comercial_rgm_outlier_contagem WHERE rgm = %s", (rgm,))
    conn.commit()
    conn.close()
    clear_crgm_data_cache(reason=f"bwipo contar-venda {request.method} rgm={rgm}")
    clear_painel_response_cache()
    return jsonify({"ok": True, "rgm": rgm, "acao": "contando" if request.method == "POST" else "removido"})


def _admin_only():
    if session.get("role") != "admin":
        return jsonify({"ok": False, "error": "Apenas administradores podem executar esta ação"}), 403
    return None


@bwipo_bp.route("/api/bwipo/painel/meta-agente", methods=["POST"])
def api_bwipo_painel_meta_agente():
    """Grava a meta do consultor na campanha da Premiação que cobre o período.
    Sem campanha nesse intervalo, cai em comercial_metas (avulsa)."""
    denied = _admin_only()
    if denied:
        return denied
    body = request.get_json(force=True, silent=True) or {}
    try:
        uid = int(body.get("user_id") or 0)
    except (TypeError, ValueError):
        uid = 0
    if not uid:
        return jsonify({"ok": False, "error": "Escolha o consultor."}), 400
    dt_ini = str(body.get("dt_inicio") or "")[:10]
    dt_fim = str(body.get("dt_fim") or "")[:10]
    if not dt_ini or not dt_fim:
        return jsonify({"ok": False, "error": "Informe o período da meta."}), 400
    try:
        inter = float(body.get("meta_intermediaria") or 0)
        meta = float(body.get("meta") or 0)
        super_ = float(body.get("supermeta") or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Valores inválidos."}), 400
    if inter <= 0 and meta <= 0 and super_ <= 0:
        return jsonify({"ok": False, "error": "Preencha ao menos um valor."}), 400
    name = str(body.get("user_name") or "")[:120]
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, nome FROM premiacao_campanha
        WHERE COALESCE(ativa, TRUE)
          AND dt_inicio <= %s::date AND dt_fim >= %s::date
        ORDER BY dt_inicio DESC
        LIMIT 1
        """,
        (dt_fim, dt_ini),
    )
    camp = cur.fetchone()
    if camp:
        cur.execute(
            """
            INSERT INTO premiacao_campanha_meta
                (campanha_id, kommo_user_id, meta, meta_intermediaria, supermeta)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (campanha_id, kommo_user_id) DO UPDATE
              SET meta = EXCLUDED.meta,
                  meta_intermediaria = EXCLUDED.meta_intermediaria,
                  supermeta = EXCLUDED.supermeta
            """,
            (camp[0], uid, meta, inter, super_),
        )
        conn.commit()
        conn.close()
        clear_painel_response_cache()
        return jsonify({"ok": True, "onde": "campanha", "campanha": camp[1], "saved": 1})
    cur.execute(
        """
        INSERT INTO comercial_metas
            (user_id, user_name, meta, meta_intermediaria, supermeta,
             categoria, dt_inicio, dt_fim, descricao)
        VALUES (%s, %s, %s, %s, %s, 'matriculas', %s, %s, '')
        ON CONFLICT (user_id, dt_inicio, dt_fim, categoria) DO UPDATE
          SET meta = EXCLUDED.meta,
              meta_intermediaria = EXCLUDED.meta_intermediaria,
              supermeta = EXCLUDED.supermeta,
              user_name = EXCLUDED.user_name
        """,
        (uid, name, meta, inter, super_, dt_ini, dt_fim),
    )
    conn.commit()
    conn.close()
    clear_painel_response_cache()
    return jsonify({"ok": True, "onde": "avulsa", "saved": 1})


@bwipo_bp.route("/api/bwipo/painel/consultores")
def api_bwipo_painel_consultores():
    """Consultores do Kommo com o usuário Bwipo ligado, ajuste do painel e data de tombamento.
    Também lista donos do Bwipo sem depara (não herdam meta nem histórico)."""
    from routes.comercial_rgm import _admin_sistema_uid, _load_consultor_ajustes

    denied = _admin_only()
    if denied:
        return denied
    try:
        kconn = _pg_kommo()
        kcur = kconn.cursor()
        kcur.execute("SELECT id, name, email FROM users ORDER BY name")
        kommo = [{"id": int(r[0]), "name": r[1] or f"User #{r[0]}", "email": r[2] or ""} for r in kcur.fetchall()]
        kconn.close()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.id, u.name, u.email, ud.kommo_user_id, ud.match_type,
                   (SELECT COUNT(*) FROM bwipo_deals d WHERE d.owner_id = u.id AND NOT d.is_deleted)
            FROM bwipo_users u
            LEFT JOIN bwipo_kommo_user_depara ud ON ud.bwipo_user_id = u.id
            ORDER BY u.name
            """
        )
        bwipo = [
            {"id": r[0], "name": r[1] or "", "email": r[2] or "", "kommo_user_id": int(r[3]) if r[3] else None,
             "match": r[4], "deals": int(r[5] or 0)}
            for r in cur.fetchall()
        ]
        tomb = {uid: d.isoformat() for uid, d in _tombamentos().items()}
        conn.close()
    except Exception as e:
        logger.exception("bwipo consultores")
        return jsonify({"ok": False, "error": str(e)}), 500
    aj = _load_consultor_ajustes()
    por_kommo: dict[int, list] = {}
    for b in bwipo:
        if b["kommo_user_id"]:
            por_kommo.setdefault(b["kommo_user_id"], []).append(b)
    out = []
    for k in kommo:
        a = aj.get(k["id"], {})
        ligados = por_kommo.get(k["id"], [])
        out.append({
            **k,
            "display_name": a.get("display_name"),
            "hidden": bool(a.get("hidden")),
            "excluido": a.get("reassign_to") is not None,
            "bwipo": [{"id": b["id"], "name": b["name"], "match": b["match"], "deals": b["deals"]} for b in ligados],
            "tombado_desde": tomb.get(k["id"]),
        })
    return jsonify({
        "ok": True,
        "consultores": out,
        "bwipo_sem_depara": [b for b in bwipo if not b["kommo_user_id"]],
        "admin_sistema_uid": _admin_sistema_uid(),
    })


@bwipo_bp.route("/api/bwipo/painel/tombamento", methods=["POST"])
def api_bwipo_painel_tombamento():
    """{kommo_user_id, desde: 'YYYY-MM-DD' | null}. Sem data = consultor volta a ser lido do Kommo."""
    from routes.comercial_rgm import clear_crgm_data_cache

    denied = _admin_only()
    if denied:
        return denied
    body = request.get_json(force=True, silent=True) or {}
    try:
        uid = int(body.get("kommo_user_id"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "kommo_user_id inválido"}), 400
    desde = _as_date(body.get("desde")) if body.get("desde") else None
    if body.get("desde") and not desde:
        return jsonify({"ok": False, "error": "Data inválida"}), 400
    conn = get_conn()
    cur = conn.cursor()
    if desde:
        cur.execute(
            """
            INSERT INTO bwipo_consultor_tombamento (kommo_user_id, desde, updated_at, updated_by)
            VALUES (%s, %s, NOW(), %s)
            ON CONFLICT (kommo_user_id) DO UPDATE SET desde = EXCLUDED.desde, updated_at = NOW(),
                updated_by = EXCLUDED.updated_by
            """,
            (uid, desde, session.get("username") or "admin"),
        )
    else:
        cur.execute("DELETE FROM bwipo_consultor_tombamento WHERE kommo_user_id = %s", (uid,))
    conn.commit()
    conn.close()
    clear_crgm_data_cache(reason=f"bwipo tombamento uid={uid}")
    schedule_painel_base_rebuild()
    return jsonify({"ok": True, "kommo_user_id": uid, "desde": desde.isoformat() if desde else None})


@bwipo_bp.route("/api/bwipo/painel/user-depara", methods=["POST"])
def api_bwipo_painel_user_depara():
    """Liga à mão um usuário Bwipo a um consultor Kommo ({bwipo_user_id, kommo_user_id}); kommo_user_id vazio desliga."""
    denied = _admin_only()
    if denied:
        return denied
    body = request.get_json(force=True, silent=True) or {}
    bid = str(body.get("bwipo_user_id") or "").strip()
    if not bid:
        return jsonify({"ok": False, "error": "bwipo_user_id obrigatório"}), 400
    conn = get_conn()
    cur = conn.cursor()
    if not body.get("kommo_user_id"):
        cur.execute("DELETE FROM bwipo_kommo_user_depara WHERE bwipo_user_id = %s", (bid,))
        conn.commit()
        conn.close()
        schedule_painel_base_rebuild()
        return jsonify({"ok": True, "bwipo_user_id": bid, "kommo_user_id": None})
    try:
        kid = int(body["kommo_user_id"])
    except (TypeError, ValueError):
        conn.close()
        return jsonify({"ok": False, "error": "kommo_user_id inválido"}), 400
    cur.execute("SELECT name, email FROM bwipo_users WHERE id = %s", (bid,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Usuário Bwipo não está no espelho"}), 404
    cur.execute(
        """
        INSERT INTO bwipo_kommo_user_depara (bwipo_user_id, kommo_user_id, match_type, email, bwipo_name,
                                             kommo_name, created_at, updated_at)
        VALUES (%s, %s, 'manual', %s, %s, %s, NOW(), NOW())
        ON CONFLICT (bwipo_user_id) DO UPDATE SET kommo_user_id = EXCLUDED.kommo_user_id,
            match_type = 'manual', kommo_name = EXCLUDED.kommo_name, updated_at = NOW()
        """,
        (bid, kid, row[1], row[0], body.get("kommo_name") or None),
    )
    conn.commit()
    conn.close()
    schedule_painel_base_rebuild()
    return jsonify({"ok": True, "bwipo_user_id": bid, "kommo_user_id": kid})


@bwipo_bp.route("/api/bwipo/painel/sync-agentes", methods=["POST"])
def api_bwipo_painel_sync_agentes():
    """Atualiza os usuários do Kommo e refaz o depara de consultores Bwipo → Kommo (manual não é sobrescrito)."""
    from routes.comercial_rgm import crgm_sync_users

    denied = _admin_only()
    if denied:
        return denied
    kommo_n = None
    try:
        r = crgm_sync_users()
        r = r[0] if isinstance(r, tuple) else r
        kommo_n = (r.get_json() or {}).get("synced")
    except Exception as e:
        logger.warning("sync-agentes kommo: %s", e)
    task = {"log": [], "progress": 0}
    conn = get_conn()
    try:
        cur = conn.cursor()
        n = _rebuild_user_depara(cur, task)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM bwipo_users u WHERE NOT EXISTS "
                    "(SELECT 1 FROM bwipo_kommo_user_depara ud WHERE ud.bwipo_user_id = u.id)")
        sem = int(cur.fetchone()[0])
    finally:
        conn.close()
    schedule_painel_base_rebuild()
    return jsonify({"ok": True, "kommo_users": kommo_n, "depara": n, "bwipo_sem_depara": sem})


@bwipo_bp.route("/api/bwipo/painel/duplicatas", methods=["POST"])
def api_bwipo_painel_duplicatas():
    """RGMs do recorte com mais de um negócio ativo no Pipeline Principal do Bwipo. Body: {rgms}."""
    rgms = _rgms_body(request.get_json(force=True, silent=True) or {})
    if not rgms:
        return jsonify({"ok": True, "duplicatas": [], "total": 0})
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT d.rgm_norm, d.id, d.number, d.title, NULLIF(btrim(d.owner_name), ''), d.stage_name,
               COALESCE(s.is_won, FALSE), COALESCE(s.is_lost, FALSE), d.updated_at
        FROM bwipo_deals d
        LEFT JOIN bwipo_stages s ON s.id = d.stage_id
        WHERE NOT d.is_deleted AND {_PRINCIPAL_SQL}
          AND d.rgm_norm IN (
              SELECT rgm_norm FROM bwipo_deals d
              WHERE NOT d.is_deleted AND {_PRINCIPAL_SQL} AND d.rgm_norm = ANY(%s)
              GROUP BY rgm_norm HAVING COUNT(*) > 1
          )
        ORDER BY d.rgm_norm, COALESCE(s.is_won, FALSE) DESC, d.updated_at DESC NULLS LAST
        """,
        (rgms,),
    )
    grupos: dict[str, list] = {}
    for rgm, did, number, title, owner, etapa, won, lost, upd in cur.fetchall():
        grupos.setdefault(rgm, []).append({
            "id": did, "number": number, "title": title or "", "agente": owner or "Sem responsável",
            "etapa": etapa or "", "ganho": bool(won), "perdido": bool(lost),
            "atualizado": upd.isoformat() if upd else None,
        })
    conn.close()
    dups = [{"rgm": r, "count": len(v), "negocios": v} for r, v in grupos.items()]
    dups.sort(key=lambda x: (-sum(1 for n in x["negocios"] if n["ganho"]), -x["count"], x["rgm"]))
    return jsonify({"ok": True, "duplicatas": dups, "total": len(dups)})


@bwipo_bp.route("/api/bwipo/painel/sem-data", methods=["POST"])
def api_bwipo_painel_sem_data():
    """Matrículas do recorte cujo negócio no Bwipo está sem data de matrícula, e as que não têm negócio. Body: {rgms}."""
    rgms = _rgms_body(request.get_json(force=True, silent=True) or {})
    if not rgms:
        return jsonify({"ok": True, "sem_data": [], "sem_negocio": [], "total": 0})
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (d.rgm_norm) d.rgm_norm, d.number, NULLIF(btrim(d.owner_name), ''), d.stage_name,
               NULLIF(btrim(d.data_matricula), '')
        FROM bwipo_deals d
        LEFT JOIN bwipo_stages s ON s.id = d.stage_id
        WHERE NOT d.is_deleted AND d.rgm_norm = ANY(%s)
        ORDER BY d.rgm_norm, (NULLIF(btrim(d.data_matricula), '') IS NULL), COALESCE(s.is_won, FALSE) DESC,
                 d.updated_at DESC NULLS LAST
        """,
        (rgms,),
    )
    achados = {}
    sem_data = []
    for rgm, number, owner, etapa, dm in cur.fetchall():
        achados[rgm] = True
        if not dm:
            sem_data.append({"rgm": rgm, "number": number, "agente": owner or "Sem responsável", "etapa": etapa or ""})
    conn.close()
    sem_negocio = [r for r in rgms if r not in achados]
    return jsonify({"ok": True, "sem_data": sem_data, "sem_negocio": sem_negocio, "total": len(sem_data)})


@bwipo_bp.route("/api/bwipo/painel/agente-carteira")
def api_bwipo_painel_agente_carteira():
    """Carteira atual do consultor no Pipeline Principal, por etapa. ?user_id=<kommo> ou ?owner=<nome Bwipo>."""
    uid = (request.args.get("user_id") or "").strip()
    owner = (request.args.get("owner") or "").strip()
    if uid:
        filtro = ("d.owner_id IN (SELECT bwipo_user_id FROM bwipo_kommo_user_depara WHERE kommo_user_id = %s)",
                  int(uid))
    elif owner:
        filtro = ("btrim(d.owner_name) = %s", owner)
    else:
        return jsonify({"ok": False, "error": "Informe user_id ou owner"}), 400
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COALESCE(d.stage_name, 'Sem etapa'), MIN(COALESCE(s.position, 999)),
               COALESCE(BOOL_OR(s.is_won), FALSE), COALESCE(BOOL_OR(s.is_lost), FALSE), COUNT(*)
        FROM bwipo_deals d
        LEFT JOIN bwipo_stages s ON s.id = d.stage_id
        LEFT JOIN bwipo_pipelines p ON p.id = d.pipeline_id
        WHERE NOT d.is_deleted
          AND position('principal' in lower(COALESCE(p.name, d.pipeline_name, ''))) > 0
          AND {filtro[0]}
        GROUP BY 1 ORDER BY 2, 1
        """,
        (filtro[1],),
    )
    etapas = [{"etapa": r[0], "ganho": bool(r[2]), "perdido": bool(r[3]), "total": int(r[4])} for r in cur.fetchall()]
    conn.close()
    return jsonify({"ok": True, "etapas": etapas, "total": sum(e["total"] for e in etapas)})


@bwipo_bp.route("/api/bwipo/painel/diagnostico")
def api_bwipo_painel_diagnostico():
    """Saúde da ligação Kommo ↔ Bwipo: espelho, depara, donos sem depara e últimos syncs."""
    conn = get_conn()
    cur = conn.cursor()
    q = {
        "negocios": "SELECT COUNT(*) FROM bwipo_deals WHERE NOT is_deleted",
        "negocios_principal": f"SELECT COUNT(*) FROM bwipo_deals d WHERE NOT d.is_deleted AND {_PRINCIPAL_SQL}",
        "negocios_com_rgm": "SELECT COUNT(*) FROM bwipo_deals WHERE NOT is_deleted AND length(rgm_norm) = 8",
        "negocios_sem_dono": "SELECT COUNT(*) FROM bwipo_deals WHERE NOT is_deleted AND NULLIF(btrim(owner_id), '') IS NULL",
        "negocios_dono_sem_depara": (
            "SELECT COUNT(*) FROM bwipo_deals d WHERE NOT d.is_deleted AND NULLIF(btrim(d.owner_id), '') IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM bwipo_kommo_user_depara ud WHERE ud.bwipo_user_id = d.owner_id)"
        ),
        "depara_leads": "SELECT COUNT(*) FROM bwipo_kommo_depara",
        "depara_negocios": "SELECT COUNT(DISTINCT bwipo_deal_id) FROM bwipo_kommo_depara",
        "depara_consultores": "SELECT COUNT(*) FROM bwipo_kommo_user_depara",
        "decisoes_manuais": "SELECT COUNT(*) FROM comercial_rgm_conflito_resolucao",
        "consultores_tombados": "SELECT COUNT(*) FROM bwipo_consultor_tombamento",
    }
    nums = {}
    for k, sql in q.items():
        cur.execute(sql)
        nums[k] = int(cur.fetchone()[0])
    cur.execute("SELECT match_type, COUNT(*) FROM bwipo_kommo_depara GROUP BY 1 ORDER BY 2 DESC")
    por_tipo = {r[0]: int(r[1]) for r in cur.fetchall()}
    cur.execute(
        """
        SELECT u.name, COUNT(d.id)
        FROM bwipo_deals d JOIN bwipo_users u ON u.id = d.owner_id
        WHERE NOT d.is_deleted
          AND NOT EXISTS (SELECT 1 FROM bwipo_kommo_user_depara ud WHERE ud.bwipo_user_id = d.owner_id)
        GROUP BY u.name ORDER BY 2 DESC
        """
    )
    donos_sem_depara = [{"agente": r[0], "negocios": int(r[1])} for r in cur.fetchall()]
    cur.execute("SELECT entity_type, last_sync_at, status, records_synced, error_message FROM bwipo_sync_metadata ORDER BY entity_type")
    syncs = [
        {"entidade": r[0], "em": r[1].isoformat() if r[1] else None, "status": r[2], "registros": r[3], "erro": r[4]}
        for r in cur.fetchall()
    ]
    conn.close()
    return jsonify({"ok": True, "numeros": nums, "depara_por_tipo": por_tipo,
                    "donos_sem_depara": donos_sem_depara, "syncs": syncs})
