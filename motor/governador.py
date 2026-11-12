"""Governador de fila: a fórmula de liberar, segurar ou pausar.

Decisão numérica, em código, sem IA (no teste de 12/11 o Jev deu 65/65 igual
à fórmula, então ela fica aqui e não custa nada).

  PAUSAR   fila acima do limite crítico, espera acima do SLA, falha de envio
           acima de 6% no lote, ou fim da janela
  SEGURAR  fila entre o teto e o crítico, poucos atendentes, lote anterior com
           menos de 30 min de vida, ou sem leitura recente da fila
  LIBERAR  o resto

Sem leitura recente da fila o motor nunca libera; com campanha em andamento e
fila às cegas, pausa.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Optional

from motor.calendario import eh_dia_util, para_brt
from motor.config import MotorConfig
from motor.modelos import Decisao, Fila, LoteAtivo


def dentro_da_janela(agora: datetime, cfg: MotorConfig) -> bool:
    """Envios só das 9h às 14h (horário de Brasília) em dia útil."""
    local = para_brt(agora)
    if not eh_dia_util(local.date(), cfg.feriados):
        return False
    return cfg.janela_inicio_h <= local.hour < cfg.janela_fim_h


def fila_vencida(fila: Optional[Fila], agora: datetime, cfg: MotorConfig) -> bool:
    if fila is None:
        return True
    return (para_brt(agora) - para_brt(fila.lida_em)) > timedelta(seconds=cfg.fila_validade_s)


def taxa_de_falha(lote: Optional[LoteAtivo]) -> float:
    if lote is None or lote.enviados <= 0:
        return 0.0
    return lote.falhas / lote.enviados


def decidir(
    fila: Optional[Fila],
    cfg: MotorConfig,
    *,
    agora: datetime,
    lote_ativo: Optional[LoteAtivo] = None,
    campanha_em_andamento: bool = False,
) -> tuple[Decisao, str]:
    """Devolve a decisão e o motivo, em português, para o diário."""
    if not dentro_da_janela(agora, cfg):
        return Decisao.PAUSAR, "fora da janela de envio"

    if lote_ativo and lote_ativo.enviados >= cfg.falha_amostra_min:
        taxa = taxa_de_falha(lote_ativo)
        if taxa > cfg.falha_envio_max:
            return Decisao.PAUSAR, f"falha de envio de {taxa:.1%} no lote (limite {cfg.falha_envio_max:.0%})"

    if fila_vencida(fila, agora, cfg):
        if campanha_em_andamento:
            return Decisao.PAUSAR, "sem leitura recente da fila com campanha em andamento"
        return Decisao.SEGURAR, "sem leitura recente da fila"

    assert fila is not None
    if fila.aguardando > cfg.fila_critica:
        return Decisao.PAUSAR, f"fila em {fila.aguardando}, acima do limite crítico ({cfg.fila_critica})"
    if fila.maior_espera_s > cfg.sla_espera_s:
        return Decisao.PAUSAR, (
            f"maior espera de {fila.maior_espera_s // 60} min, acima do SLA ({cfg.sla_espera_min} min)"
        )

    if fila.aguardando >= cfg.fila_teto:
        return Decisao.SEGURAR, f"fila em {fila.aguardando}, no teto ({cfg.fila_teto}) ou acima"
    if fila.atendentes_online < cfg.atendentes_min:
        return Decisao.SEGURAR, (
            f"{fila.atendentes_online} atendentes online, abaixo do mínimo ({cfg.atendentes_min})"
        )
    if lote_ativo is not None:
        vida = para_brt(agora) - para_brt(lote_ativo.liberado_em)
        if vida < timedelta(minutes=cfg.vida_min_lote_min):
            falta = cfg.vida_min_lote_min - int(vida.total_seconds() // 60)
            return Decisao.SEGURAR, f"lote anterior com menos de {cfg.vida_min_lote_min} min de vida (faltam {falta} min)"

    return Decisao.LIBERAR, (
        f"fila em {fila.aguardando} (teto {cfg.fila_teto}), {fila.atendentes_online} atendentes online"
    )


def tamanho_do_lote(fila: Optional[Fila], cfg: MotorConfig) -> int:
    """Atendimentos que a equipe absorve na próxima hora ÷ 0,04.

    Absorção = atendentes online × atendimentos por hora, menos o que já está
    aguardando. Ex.: 6 atendentes livres × 8 = 48 → lote de 1.200.
    O resultado nunca passa do tamanho máximo de lote.
    """
    if fila is None:
        return 0
    absorcao = fila.atendentes_online * cfg.atend_por_hora - fila.aguardando
    if absorcao <= 0:
        return 0
    return min(cfg.lote_max, int(math.floor(absorcao / cfg.fator_atendimento)))
