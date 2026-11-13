"""Governador de fila: a fórmula de liberar, segurar ou pausar.

Decisão numérica, em código, sem IA (no teste de 12/11 o Jev deu 65/65 igual
à fórmula, então ela fica aqui e não custa nada).

  PAUSAR   fora da janela, fila acima do limite crítico, espera acima do SLA,
           falha de envio acima de 6% no lote, trava de experiência no nível
           de encerrar o dia, ou campanha em andamento sem leitura da fila
  SEGURAR  depois do corte (último lote 1 h antes do fim da janela), fila
           entre o teto e o crítico, poucos atendentes, trava de experiência
           no nível de segurar, lote anterior com menos de 30 min de vida, ou
           sem leitura recente da fila
  LIBERAR  o resto

Sem leitura recente da fila o motor nunca libera.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from motor.calendario import eh_dia_util, para_brt
from motor.config import MotorConfig
from motor.modelos import Decisao, Experiencia, Fila, LoteAtivo


def dentro_da_janela(agora: datetime, cfg: MotorConfig) -> bool:
    """Envios só dentro da janela (padrão 9h às 14h, horário de Brasília) em dia útil."""
    local = para_brt(agora)
    if not eh_dia_util(local.date(), cfg.feriados):
        return False
    return cfg.janela_inicio_h <= local.hour < cfg.janela_fim_h


def antes_do_corte(agora: datetime, cfg: MotorConfig) -> bool:
    """Lote novo só até `corte_min` antes do fim da janela, para as respostas caberem no expediente."""
    local = para_brt(agora)
    limite = local.replace(hour=cfg.janela_fim_h, minute=0, second=0, microsecond=0) - timedelta(minutes=cfg.corte_min)
    return dentro_da_janela(agora, cfg) and local < limite


def fila_vencida(fila: Optional[Fila], agora: datetime, cfg: MotorConfig) -> bool:
    if fila is None:
        return True
    return (para_brt(agora) - para_brt(fila.lida_em)) > timedelta(seconds=cfg.fila_validade_s)


def taxa_de_falha(lote: Optional[LoteAtivo]) -> float:
    if lote is None or lote.enviados <= 0:
        return 0.0
    return lote.falhas / lote.enviados


def trava_de_experiencia(exp: Optional[Experiencia], cfg: MotorConfig) -> Optional[str]:
    """'encerrar', 'segurar' ou None, pelos pedidos para parar e reclamações do dia."""
    if exp is None or exp.casos < cfg.exp_min_casos:
        return None
    if exp.por_mil >= cfg.exp_encerra_por_mil:
        return "encerrar"
    if exp.por_mil >= cfg.exp_segura_por_mil:
        return "segurar"
    return None


def decidir(
    fila: Optional[Fila],
    cfg: MotorConfig,
    *,
    agora: datetime,
    lote_ativo: Optional[LoteAtivo] = None,
    campanha_em_andamento: bool = False,
    experiencia: Optional[Experiencia] = None,
) -> tuple[Decisao, str]:
    """Devolve a decisão e o motivo, em português, para o diário."""
    if not dentro_da_janela(agora, cfg):
        return Decisao.PAUSAR, "fora da janela de envio"

    if lote_ativo and lote_ativo.enviados >= cfg.falha_amostra_min:
        taxa = taxa_de_falha(lote_ativo)
        if taxa > cfg.falha_envio_max:
            return Decisao.PAUSAR, f"falha de envio de {taxa:.1%} no lote (limite {cfg.falha_envio_max:.0%})"

    trava = trava_de_experiencia(experiencia, cfg)
    if trava == "encerrar":
        return Decisao.PAUSAR, (
            f"trava de experiência: {experiencia.por_mil:.1f} pedidos para parar ou reclamações por 1.000 "
            f"(limite {cfg.exp_encerra_por_mil:g}); o dia é encerrado"
        )

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

    if not antes_do_corte(agora, cfg):
        return Decisao.SEGURAR, f"corte de {cfg.corte_min} min antes do fim da janela: sem lote novo hoje"
    if trava == "segurar":
        return Decisao.SEGURAR, (
            f"trava de experiência: {experiencia.por_mil:.1f} pedidos para parar ou reclamações por 1.000 "
            f"(limite {cfg.exp_segura_por_mil:g})"
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


def teto_da_rampa(lotes_liberados_hoje: int, cfg: MotorConfig) -> int:
    """Primeiros lotes do dia são menores (300, 600...); depois vale o tamanho máximo."""
    if lotes_liberados_hoje < len(cfg.rampa):
        return min(cfg.rampa[lotes_liberados_hoje], cfg.lote_max)
    return cfg.lote_max


def tamanho_do_lote(fila: Optional[Fila], cfg: MotorConfig, lotes_liberados_hoje: int = 0) -> int:
    """Tamanho do próximo lote pela folga da equipe (mesma conta do simulador).

      folga por hora  = atendentes online × atendimentos por hora − 1,1 × conversas que já chegam sem campanha
      conversas       = folga × intervalo entre lotes + metade do espaço até o teto da fila
      lote            = conversas ÷ conversas para atendente geradas por envio

    Arredondado de 50 em 50, nunca abaixo do lote mínimo nem acima do teto da
    rampa do dia. Sem folga nenhuma, devolve 0.
    """
    if fila is None:
        return 0
    folga_h = max(0.0, fila.atendentes_online * cfg.atend_por_hora - cfg.base_conversas_hora * 1.1)
    espaco = max(0, cfg.fila_teto - fila.aguardando)
    conversas = folga_h * cfg.vida_min_lote_min / 60 + espaco * 0.5
    if conversas <= 0:
        return 0
    lote = int(round(conversas / cfg.humanos_por_envio / 50.0)) * 50
    teto = teto_da_rampa(lotes_liberados_hoje, cfg)
    return min(teto, max(min(cfg.lote_min, teto), lote))
