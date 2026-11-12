"""Uma execução da coleta: CRM (só leitura) → tabelas nps_. Roda fora de request (job ou thread)."""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Callable

from nps import razao_pg
from nps.coleta import BRT, POR_PAGINA, atendente_da_conversa, itens, montar_mapa, varrer

logger = logging.getLogger(__name__)


def coleta_habilitada() -> bool:
    return (os.getenv("NPS_COLETA_ENABLED") or "0").strip() == "1"


def coletar(conectar: Callable[[], Any], crm, disparada_por: str = "agendador") -> dict:
    """Devolve um resumo da execução. Nunca levanta: erro fica registrado em nps_coletas."""
    conn = conectar()
    try:
        if not razao_pg.tabelas_instaladas(conn):
            return {"ok": False, "motivo": "tabelas nps_ não instaladas"}
        if not razao_pg.pegar_trava(conn):
            return {"ok": False, "motivo": "já existe uma coleta em andamento"}
        coleta_id = razao_pg.abrir_coleta(conn, disparada_por)
        numeros = {"negocios_lidos": 0, "pesquisas_novas": 0, "respostas_novas": 0, "atendentes": 0}
        try:
            agora = datetime.now(BRT)
            mapa = montar_mapa(itens(crm.campos()))
            pipeline = (os.getenv("NPS_PIPELINE_ID") or "").strip()
            pesquisas, numeros["negocios_lidos"] = varrer(
                lambda pagina: crm.negocios(pagina, POR_PAGINA, pipeline), mapa, agora
            )
            numeros["pesquisas_novas"], numeros["respostas_novas"] = razao_pg.gravar(conn, pesquisas)
            numeros["atendentes"] = _resolver_atendentes(conn, crm)
        except Exception as e:  # noqa: BLE001 - a coleta registra e segue viva
            conn.rollback()
            logger.warning("nps: coleta %s falhou: %s", coleta_id, e)
            razao_pg.fechar_coleta(conn, coleta_id, erro=str(e), **numeros)
            return {"ok": False, "motivo": str(e), **numeros}
        razao_pg.fechar_coleta(conn, coleta_id, **numeros)
        return {"ok": True, **numeros}
    finally:
        conn.close()  # fechar a conexão solta a trava


def _resolver_atendentes(conn, crm) -> int:
    """Procura quem atendeu a conversa de origem de cada resposta nova. Uma requisição por resposta."""
    limite = max(0, min(int(os.getenv("NPS_ATENDENTES_POR_COLETA") or 200), 1000))
    achados = 0
    for pesquisa_id, conversa_id in razao_pg.sem_atendente(conn, limite):
        try:
            nome = atendente_da_conversa(itens(crm.mensagens(conversa_id)))
        except Exception as e:  # noqa: BLE001 - fica para a próxima coleta
            logger.warning("nps: mensagens da conversa de origem indisponíveis: %s", e)
            break
        razao_pg.gravar_atendente(conn, pesquisa_id, nome)
        achados += bool(nome)
    return achados
