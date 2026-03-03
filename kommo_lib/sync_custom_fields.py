"""
Sincronização das definições de Custom Fields do Kommo.
Sincroniza os metadados dos campos (nome, tipo, opções enum) para leads e contatos.
Isso permite decodificar os valores dos custom fields armazenados nos leads/contatos.
"""

import logging

from api_client import KommoAPIClient
from database import (
    upsert_custom_fields_batch,
    update_sync_metadata,
    set_sync_status,
)

logger = logging.getLogger(__name__)


def sync_custom_fields(client: KommoAPIClient) -> dict:
    """
    Sincroniza as definições de custom fields para leads e contatos.
    
    Endpoints:
      - GET /api/v4/leads/custom_fields
      - GET /api/v4/contacts/custom_fields
    
    Paginação: Sim (podem haver muitos campos)
    
    Returns:
        dict com estatísticas da sincronização
    """
    stats = {"leads_fields": 0, "contacts_fields": 0}

    logger.info("=" * 60)
    logger.info("INICIANDO SINCRONIZAÇÃO DE CUSTOM FIELDS")
    logger.info("=" * 60)

    # === Custom Fields de Leads ===
    entity = "custom_fields_leads"
    set_sync_status(entity, "running")

    try:
        logger.info("Buscando custom fields de LEADS...")
        lead_fields = client.get_all_pages(
            "leads/custom_fields",
            embedded_key="custom_fields"
        )

        if lead_fields:
            upsert_custom_fields_batch(lead_fields, "leads")
            stats["leads_fields"] = len(lead_fields)
            logger.info("Custom fields de leads sincronizados: %d campos", len(lead_fields))
        else:
            logger.info("Nenhum custom field de lead encontrado.")

        update_sync_metadata(entity, stats["leads_fields"], is_full_sync=True)

    except Exception as e:
        set_sync_status(entity, "failed")
        logger.error("Erro sincronizando custom fields de leads: %s", str(e))
        raise

    # === Custom Fields de Contatos ===
    entity = "custom_fields_contacts"
    set_sync_status(entity, "running")

    try:
        logger.info("Buscando custom fields de CONTATOS...")
        contact_fields = client.get_all_pages(
            "contacts/custom_fields",
            embedded_key="custom_fields"
        )

        if contact_fields:
            upsert_custom_fields_batch(contact_fields, "contacts")
            stats["contacts_fields"] = len(contact_fields)
            logger.info("Custom fields de contatos sincronizados: %d campos", len(contact_fields))
        else:
            logger.info("Nenhum custom field de contato encontrado.")

        update_sync_metadata(entity, stats["contacts_fields"], is_full_sync=True)

    except Exception as e:
        set_sync_status(entity, "failed")
        logger.error("Erro sincronizando custom fields de contatos: %s", str(e))
        raise

    logger.info(
        "Custom fields totais: %d (leads) + %d (contatos)",
        stats["leads_fields"], stats["contacts_fields"]
    )

    return stats
