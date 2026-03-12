"""
Sincronização de Pipelines e Stages (Statuses) do Kommo.
Os pipelines são a base estrutural - sincronizamos primeiro.
"""

import logging

from api_client import KommoAPIClient
from database import (
    upsert_pipeline,
    upsert_pipeline_status,
    update_sync_metadata,
    set_sync_status,
)

logger = logging.getLogger(__name__)


def sync_pipelines(client: KommoAPIClient) -> dict:
    """
    Sincroniza todos os pipelines e seus respectivos stages.
    
    Endpoint: GET /api/v4/leads/pipelines
    
    A API retorna todos os pipelines com seus statuses embutidos.
    Não há paginação necessária para pipelines (quantidade limitada).
    
    Returns:
        dict com estatísticas da sincronização
    """
    entity = "pipelines"
    stats = {"pipelines": 0, "statuses": 0}

    logger.info("=" * 60)
    logger.info("INICIANDO SINCRONIZAÇÃO DE PIPELINES E STAGES")
    logger.info("=" * 60)

    set_sync_status(entity, "running")

    try:
        # Buscar todos os pipelines
        data = client.get("leads/pipelines")

        if data is None:
            logger.warning("Nenhum pipeline encontrado.")
            update_sync_metadata(entity, 0, is_full_sync=True)
            return stats

        pipelines = data.get("_embedded", {}).get("pipelines", [])

        for pipeline in pipelines:
            pipeline_id = pipeline.get("id")
            pipeline_name = pipeline.get("name")

            logger.info(
                "Pipeline: %s (ID: %d, Main: %s)",
                pipeline_name, pipeline_id, pipeline.get("is_main", False)
            )

            # Salvar pipeline
            upsert_pipeline(pipeline)
            stats["pipelines"] += 1

            # Processar statuses (stages) do pipeline
            embedded = pipeline.get("_embedded", {})
            statuses = embedded.get("statuses", [])

            for status in statuses:
                status_id = status.get("id")
                status_name = status.get("name")

                logger.debug(
                    "  Stage: %s (ID: %d, Sort: %d)",
                    status_name, status_id, status.get("sort", 0)
                )

                upsert_pipeline_status(status, pipeline_id)
                stats["statuses"] += 1

        update_sync_metadata(entity, stats["pipelines"], is_full_sync=True)

        logger.info(
            "Pipelines sincronizados: %d pipelines, %d stages",
            stats["pipelines"], stats["statuses"]
        )

    except Exception as e:
        set_sync_status(entity, "failed")
        logger.error("Erro na sincronização de pipelines: %s", str(e))
        raise

    return stats
