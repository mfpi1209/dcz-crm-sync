"""
Sincronização de Leads do Kommo.
Suporta sync completo (primeira vez) e incremental (delta via updated_at).
Otimizado: batches menores, pausa entre páginas, sem raw_json.
"""

import logging
import time
from datetime import datetime, timezone, timedelta

from api_client import KommoAPIClient
from database import (
    upsert_leads_batch,
    update_sync_metadata,
    set_sync_status,
    get_last_sync,
)
from config import PAGE_SIZE, BATCH_SIZE, SLEEP_BETWEEN_PAGES, KOMMO_DELTA_LOOKBACK_DAYS

logger = logging.getLogger(__name__)


def sync_leads(client: KommoAPIClient, force_full: bool = False) -> dict:
    """
    Sincroniza leads do Kommo para o banco local.
    
    Estratégia:
    - 1ª execução (ou force_full=True): Busca todos os leads
    - Execuções seguintes: Busca apenas leads atualizados desde o último sync
    
    Endpoint: GET /api/v4/leads
    Parâmetros importantes:
      - with=contacts,catalog_elements  (dados vinculados)
      - limit=250 (máximo por página)
      - page=N (paginação)
      - filter[updated_at][from]=timestamp (delta sync)
    
    Returns:
        dict com estatísticas da sincronização
    """
    entity = "leads"
    stats = {"total": 0, "pages": 0, "is_full_sync": False}

    logger.info("=" * 60)
    logger.info("INICIANDO SINCRONIZAÇÃO DE LEADS")
    logger.info("=" * 60)

    set_sync_status(entity, "running")

    try:
        # Determinar se é full ou delta sync
        last_sync = get_last_sync(entity)
        is_full_sync = force_full or last_sync is None or last_sync.get("last_full_sync_at") is None
        stats["is_full_sync"] = is_full_sync

        # Montar parâmetros
        params = {
            "limit": PAGE_SIZE,
            "with": "contacts",  # Inclui contatos vinculados nos leads
        }

        if not is_full_sync:
            # Delta sync: buscar apenas atualizados desde o último sync
            last_sync_at = last_sync["last_sync_at"]
            try:
                dt = datetime.fromisoformat(last_sync_at.replace("Z", "+00:00"))
                # IMPORTANTE: last_sync_at é salvo com utcnow() (UTC sem timezone)
                # Precisamos informar que é UTC para .timestamp() converter corretamente
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                # Subtrair 5 minutos de margem para evitar perder registros
                from_ts = int(dt.timestamp()) - 300
                # Janela mínima: incluir alterações dos últimos N dias (evita PG defasado vs Kommo)
                if KOMMO_DELTA_LOOKBACK_DAYS > 0:
                    floor = int(
                        (datetime.now(timezone.utc) - timedelta(days=KOMMO_DELTA_LOOKBACK_DAYS)).timestamp()
                    )
                    from_ts = min(from_ts, floor)
            except (ValueError, AttributeError):
                logger.warning("Timestamp inválido no last_sync. Forçando full sync.")
                is_full_sync = True
                stats["is_full_sync"] = True
                from_ts = None

            if not is_full_sync:
                params["filter[updated_at][from]"] = from_ts
                logger.info(
                    "Delta sync: leads com updated_at >= ts %d (lookback %d dias, ref %s)",
                    from_ts, KOMMO_DELTA_LOOKBACK_DAYS, last_sync_at[:19],
                )
        
        if is_full_sync:
            logger.info("Full sync: buscando TODOS os leads...")

        # Paginação com pausa entre páginas (reduz pico de CPU/disco)
        page = 1
        batch = []

        while True:
            if page > 1:
                time.sleep(SLEEP_BETWEEN_PAGES)

            params["page"] = page
            if page % 10 == 1 or page <= 3:
                logger.info(
                    "Leads - página %d (acumulado: %d registros)...",
                    page, stats["total"]
                )

            data = client.get("leads", params=params)

            if data is None:
                logger.info("Sem mais dados (204). Finalizando paginação.")
                break

            embedded = data.get("_embedded", {})
            leads = embedded.get("leads", [])

            if not leads:
                logger.info("Página vazia. Finalizando paginação.")
                break

            batch.extend(leads)
            stats["total"] += len(leads)
            stats["pages"] += 1

            # Persistir em batches pequenos (menos travamento por transação)
            while len(batch) >= BATCH_SIZE:
                chunk = batch[:BATCH_SIZE]
                batch = batch[BATCH_SIZE:]
                upsert_leads_batch(chunk)

            if page % 10 == 0 or page <= 2:
                logger.info(
                    "Página %d: %d leads (total: %d)",
                    page, len(leads), stats["total"]
                )

            links = data.get("_links", {})
            if "next" not in links:
                break

            page += 1

        if batch:
            upsert_leads_batch(batch)

        # Atualizar metadados
        update_sync_metadata(entity, stats["total"], is_full_sync=is_full_sync)

        sync_type = "FULL" if is_full_sync else "DELTA"
        logger.info(
            "Leads sincronizados [%s]: %d registros em %d páginas",
            sync_type, stats["total"], stats["pages"]
        )

    except Exception as e:
        set_sync_status(entity, "failed")
        logger.error("Erro na sincronização de leads: %s", str(e))
        raise

    return stats
