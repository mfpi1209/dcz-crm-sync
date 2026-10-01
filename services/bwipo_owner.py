"""Responsável Bwipo = responsável do contato no Kommo.

O lead novo entra no Bwipo por uma integração de fora (formulário / n8n).
Este módulo não cria o negócio: a cada poucos minutos pega os recém-atualizados
sem owner e grava o id Bwipo do mesmo consultor do Kommo.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2

from services.bwipo_comercial import (
    BwipoComercialError,
    list_page,
    phone_norm,
    request,
)

logger = logging.getLogger(__name__)


def _pg_kommo():
    return psycopg2.connect(
        host=os.getenv("KOMMO_PG_HOST", os.getenv("DB_HOST", "localhost")),
        port=os.getenv("KOMMO_PG_PORT", os.getenv("DB_PORT", "5432")),
        user=os.getenv("KOMMO_PG_USER", os.getenv("DB_USER")),
        password=os.getenv("KOMMO_PG_PASS", os.getenv("DB_PASS")),
        dbname=os.getenv("KOMMO_PG_DB", "kommo_sync"),
    )

# kommo users.id → bwipo users.id (org Comercial Cruzeiro)
KOMMO_UID_TO_BWIPO = {
    8261837: "cmu30un170001uawgopfset4j",    # Admin Sistema
    12158628: "cmu312ey7000tuap8u77jabco",  # Hugo
    14205944: "cmu312qbt0025uap8es3lziwt",  # Thainá G.
    14464488: "cmu312pf30021uap8jh9w4t9h",  # Tamires
    15352496: "cmu312c72000huap867nodrzx",  # Gabriel Messias
    12209212: "cmu312e1b000puap8gcztw6j9",  # Gabriela
    15352480: "cmu312nhc001tuap8eea87v5w",  # Rahi Costa
    8240438: "cmu3129do0005uap8fm9vpjoj",   # Claudia
    10729260: "cmu312fv6000xuap81auyx2qa",  # Jessica
    8240189: "cmu312hpw0015uap8lk6ud1p3",  # Juliana
    13018348: "cmu312imc0019uap84cwt7ajf",  # Kamilly
    15352492: "cmu312mhy001puap82wxp42q3",  # Paloma
    8239958: "cmu312b96000duap8hyp6rdit",   # Fran
    14482884: "cmu312ogm001xuap8q5hoycsm",  # Sabrina
    8240165: "cmu30uod70005uawg2u3574gd",   # Isabela
    13304804: "cmu30up9y0009uawgu5uagn55",  # TI
}


def _pipeline_principal(deal: dict) -> bool:
    stage = deal.get("stage") if isinstance(deal.get("stage"), dict) else {}
    pipe = stage.get("pipeline") if isinstance(stage.get("pipeline"), dict) else {}
    name = (pipe.get("name") or stage.get("pipelineName") or "").lower()
    if not name:
        return True
    return "principal" in name


def _created_at(deal: dict) -> Optional[datetime]:
    raw = deal.get("createdAt") or deal.get("created_at")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def kommo_owner_bwipo_id(phone: str | None) -> Optional[str]:
    """Id Bwipo do responsável do contato Kommo com esse telefone."""
    tail = phone_norm(phone)
    if not tail:
        return None
    conn = _pg_kommo()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.responsible_user_id
            FROM contact_custom_field_values ccf
            JOIN contacts c ON c.id = ccf.contact_id
             AND COALESCE(c.is_deleted, FALSE) = FALSE
            WHERE LOWER(ccf.field_name) = 'phone'
              AND right(regexp_replace(ccf.values_json->0->>'value', '[^0-9]', '', 'g'), 11) = %s
              AND c.responsible_user_id IS NOT NULL
            ORDER BY c.id DESC
            LIMIT 1
            """,
            (tail,),
        )
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()
    if not row:
        return None
    return KOMMO_UID_TO_BWIPO.get(int(row[0]))


def assign_recent_unowned(minutes: int = 90, max_puts: int = 15, max_pages: int = 2) -> dict[str, int]:
    """Grava owner nos negócios novos ainda sem responsável. Poucas páginas, para não brigar com o lote."""
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    since_iso = since.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    assigned = skipped = seen = 0
    page = 1
    while page <= max_pages and assigned < max_puts:
        try:
            payload = list_page("/api/deals", page=page, per_page=100, extra={"updatedSince": since_iso})
        except BwipoComercialError as e:
            logger.warning("bwipo owner list %s", e)
            break
        items = payload.get("items") or []
        if not items:
            break
        for deal in items:
            seen += 1
            if deal.get("ownerId") or not _pipeline_principal(deal):
                continue
            created = _created_at(deal)
            if created and created < since:
                continue
            contact = deal.get("contact") if isinstance(deal.get("contact"), dict) else {}
            oid = kommo_owner_bwipo_id(contact.get("phone"))
            if not oid:
                skipped += 1
                continue
            deal_id = deal.get("id")
            if not deal_id:
                continue
            try:
                request("PUT", f"/api/deals/{deal_id}", {"ownerId": oid})
            except BwipoComercialError as e:
                logger.warning("bwipo owner put %s %s", deal_id, e)
                if e.status == 429:
                    time.sleep(8)
                continue
            assigned += 1
            if assigned >= max_puts:
                break
            time.sleep(0.35)
        if len(items) < 100:
            break
        page += 1
        time.sleep(0.25)
    return {"seen": seen, "assigned": assigned, "sem_mapa": skipped}


def loop_forever(interval_s: int = 300) -> None:
    from pathlib import Path

    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    while True:
        try:
            stats = assign_recent_unowned()
            logger.info("bwipo owner recente %s", stats)
            print(f"owner recente {stats}", flush=True)
        except Exception as e:
            logger.exception("bwipo owner recente")
            print(f"owner recente erro {e}", flush=True)
        time.sleep(interval_s)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop_forever()
