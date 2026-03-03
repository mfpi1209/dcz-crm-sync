"""
Sincronização de Leads do Kommo.
Suporta sync completo (primeira vez) e incremental (delta via updated_at).
Inclui custom fields e dados embedded (tags, contatos vinculados).
"""

import logging
from datetime import datetime, timezone

from api_client import KommoAPIClient
from database import (
    upsert_leads_batch,
    update_sync_metadata,
    set_sync_status,
    get_last_sync,
)
from config import PAGE_SIZE

logger = logging.getLogger(__name__)

BATCH_SIZE = 50  # Registros por batch de escrita no banco


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
            except (ValueError, AttributeError):
                logger.warning("Timestamp inválido no last_sync. Forçando full sync.")
                is_full_sync = True
                stats["is_full_sync"] = True
                from_ts = None

            if not is_full_sync:
                params["filter[updated_at][from]"] = from_ts
                logger.info("Delta sync: buscando leads atualizados desde %s (ts: %d)", last_sync_at, from_ts)
        
        if is_full_sync:
            logger.info("Full sync: buscando TODOS os leads...")

        # Paginação
        page = 1
        batch = []

        while True:
            params["page"] = page
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

            # Persistir em batches para não acumular tudo em memória
            if len(batch) >= BATCH_SIZE:
                logger.debug("Persistindo batch de %d leads...", len(batch))
                upsert_leads_batch(batch)
                batch = []

            logger.info(
                "Página %d: %d leads recebidos (total acumulado: %d)",
                page, len(leads), stats["total"]
            )

            # Verificar próxima página
            links = data.get("_links", {})
            if "next" not in links:
                break

            page += 1

        # Persistir últimos registros do batch
        if batch:
            logger.debug("Persistindo batch final de %d leads...", len(batch))
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
