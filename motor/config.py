"""Configuração do motor, lida do ambiente.

Os valores do ambiente nunca passam dos tetos absolutos definidos aqui: um
erro de digitação numa variável não vira um disparo de 100 mil mensagens.
Mudar limite é decisão humana (variável de ambiente no EasyPanel + registro
no AGENTS.md); o piloto não altera nada disto.

Os padrões são os do piloto de 24/11/2026 (lotes de 300, 600 e até 1.200,
teto de 4.000 no dia, só graduação).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date

from motor.calendario import parse_feriados

# Tetos absolutos (código). O ambiente só consegue apertar.
LOTE_ABSOLUTO = 1500
TETO_DIARIO_ABSOLUTO = 12000
JANELA_MAIS_CEDO_H = 9
JANELA_MAIS_TARDE_H = 22        # fim do expediente do atendimento (decisão do Marcelo, 13/11: "vai até as 22h")
ULTIMO_LOTE_MAIS_TARDE_H = 21   # nenhum lote novo sai depois das 21h, seja qual for a configuração
MAX_MSGS_CICLO_ABSOLUTO = 2
CRM_RPM_ABSOLUTO = 60           # o CRM corta em 240/min por organização; o motor reserva 60
FILA_SOBE_SEGURA_ABSOLUTO = 30  # conversas aguardando acima do nível de quando o lote saiu
FILA_SOBE_PAUSA_ABSOLUTO = 60
EXP_SEGURA_ABSOLUTO = 8.0       # pedidos para parar + reclamações por 1.000 entregues
EXP_ENCERRA_ABSOLUTO = 15.0
EXP_MIN_CASOS_ABSOLUTO = 5
VELOCIDADE_ABSOLUTA = 5         # mensagens por segundo na campanha do lote


def _bool(nome: str, padrao: bool) -> bool:
    bruto = os.getenv(nome)
    if bruto is None or not bruto.strip():
        return padrao
    return bruto.strip().lower() in ("1", "true", "sim", "yes", "on")


def _int(nome: str, padrao: int, minimo: int, maximo: int) -> int:
    try:
        v = int(float(os.getenv(nome, "") or padrao))
    except (TypeError, ValueError):
        v = padrao
    return max(minimo, min(maximo, v))


def _float(nome: str, padrao: float, minimo: float, maximo: float) -> float:
    try:
        v = float(os.getenv(nome, "") or padrao)
    except (TypeError, ValueError):
        v = padrao
    return max(minimo, min(maximo, v))


def _rampa(nome: str, padrao: tuple[int, ...], lote_max: int) -> tuple[int, ...]:
    bruto = (os.getenv(nome) or "").strip()
    if not bruto:
        return tuple(min(x, lote_max) for x in padrao)
    try:
        return tuple(max(1, min(lote_max, int(x))) for x in bruto.split(",") if x.strip())[:10]
    except ValueError:
        return tuple(min(x, lote_max) for x in padrao)


@dataclass(frozen=True)
class MotorConfig:
    # Entrada segura: desligado e em simulação até alguém mudar o ambiente.
    enabled: bool = False
    dry_run: bool = True

    # Janela de envio (dia útil, horário de Brasília) e volumes.
    janela_inicio_h: int = 9
    janela_fim_h: int = 22            # fim do expediente: depois disso o que estiver enviando é pausado
    corte_min: int = 60               # último lote sai até N minutos antes do fim do expediente (padrão: 21h)
    teto_diario: int = 4000
    lote_max: int = 1200
    lote_min: int = 200
    rampa: tuple[int, ...] = (300, 600)   # teto do 1º lote do dia, do 2º...; depois vale lote_max
    max_msgs_ciclo: int = 2
    nivel_alvo: str = "graduacao"     # vazio = qualquer nível
    dias_antes_vencimento: int = 3    # público dividido nos N dias úteis antes do vencimento
    velocidade: int = 1               # mensagens por segundo na campanha do lote

    # Grupo de controle.
    controle_pct: float = 5.0
    controle_sal: str = "motor-ativacoes-v1"

    # Governador de fila: mede a subida desde o último lote, não um teto fixo (o número
    # do CRM inclui conversas paradas há horas). A maior espera não entra nas regras.
    fila_sobe_segura: int = 30
    fila_sobe_pausa: int = 60
    fila_base_validade_min: int = 120
    atendentes_min: int = 5
    falha_envio_max: float = 0.06
    falha_amostra_min: int = 50
    vida_min_lote_min: int = 30
    fila_validade_s: int = 300
    atend_por_hora: float = 6.0       # atendimentos que um atendente absorve por hora
    base_conversas_hora: float = 28.0  # conversas que chegam por hora sem campanha
    humanos_por_envio: float = 0.025  # 91 respostas por 1.000 envios × 27% indo para atendente

    # Trava de experiência (pedidos para parar + reclamações por 1.000 entregues, no dia).
    exp_segura_por_mil: float = 8.0
    exp_encerra_por_mil: float = 15.0
    exp_min_casos: int = 5

    # Ciclo.
    folga_baixa_du: int = 3
    declarou_pagamento_du: int = 5
    carga_min_abertos: int = 8000
    carga_hora: int = 7
    plano_hora: int = 8
    plano_minuto: int = 30

    # Respostas.
    guardar_texto: bool = False       # D6: texto mascarado por 30 dias (desligado até a decisão)
    texto_dias: int = 30

    # Laço e API do CRM.
    tick_s: int = 30
    crm_rpm: int = 60
    crm_reserva_alta: int = 10        # chamadas por minuto reservadas a fila, resultado e pausa
    feriados: frozenset[date] = field(default_factory=frozenset)

    def publico(self) -> dict:
        """Configuração exibível na página (sem sal do sorteio)."""
        d = {k: getattr(self, k) for k in self.__dataclass_fields__ if k not in ("controle_sal", "feriados")}
        d["rampa"] = list(self.rampa)
        d["feriados_extras"] = sorted(x.isoformat() for x in self.feriados)
        return d


def carregar() -> MotorConfig:
    inicio = _int("MOTOR_JANELA_INICIO_H", 9, JANELA_MAIS_CEDO_H, JANELA_MAIS_TARDE_H - 1)
    fim = _int("MOTOR_JANELA_FIM_H", 22, inicio + 1, JANELA_MAIS_TARDE_H)
    lote_max = _int("MOTOR_LOTE_MAX", 1200, 1, LOTE_ABSOLUTO)
    sobe_segura = _int("MOTOR_FILA_SOBE_SEGURA", 30, 1, FILA_SOBE_SEGURA_ABSOLUTO)
    rpm = _int("MOTOR_CRM_RPM", 60, 2, CRM_RPM_ABSOLUTO)
    segura = _float("MOTOR_EXP_SEGURA_POR_MIL", 8.0, 0.5, EXP_SEGURA_ABSOLUTO)
    return MotorConfig(
        enabled=_bool("MOTOR_ENABLED", False),
        dry_run=_bool("MOTOR_DRY_RUN", True),
        janela_inicio_h=inicio,
        janela_fim_h=fim,
        corte_min=_int("MOTOR_CORTE_MIN", 60, 30, 240),
        teto_diario=_int("MOTOR_TETO_DIARIO", 4000, 0, TETO_DIARIO_ABSOLUTO),
        lote_max=lote_max,
        lote_min=_int("MOTOR_LOTE_MIN", 200, 1, lote_max),
        rampa=_rampa("MOTOR_RAMPA", (300, 600), lote_max),
        max_msgs_ciclo=_int("MOTOR_MAX_MSGS_CICLO", 2, 1, MAX_MSGS_CICLO_ABSOLUTO),
        nivel_alvo=(os.getenv("MOTOR_NIVEL_ALVO", "graduacao") or "").strip().lower(),
        dias_antes_vencimento=_int("MOTOR_DIAS_ANTES_VENCIMENTO", 3, 1, 5),
        velocidade=_int("MOTOR_VELOCIDADE", 1, 1, VELOCIDADE_ABSOLUTA),
        controle_pct=_float("MOTOR_CONTROLE_PCT", 5.0, 0.0, 50.0),
        controle_sal=(os.getenv("MOTOR_CONTROLE_SAL") or "motor-ativacoes-v1").strip(),
        fila_sobe_segura=sobe_segura,
        fila_sobe_pausa=_int("MOTOR_FILA_SOBE_PAUSA", 60, sobe_segura, FILA_SOBE_PAUSA_ABSOLUTO),
        fila_base_validade_min=_int("MOTOR_FILA_BASE_VALIDADE_MIN", 120, 30, 24 * 60),
        atendentes_min=_int("MOTOR_ATENDENTES_MIN", 5, 1, 1000),
        falha_envio_max=_float("MOTOR_FALHA_ENVIO_MAX", 0.06, 0.0, 0.06),
        falha_amostra_min=_int("MOTOR_FALHA_AMOSTRA_MIN", 50, 1, 100000),
        vida_min_lote_min=_int("MOTOR_VIDA_MIN_LOTE_MIN", 30, 5, 24 * 60),
        fila_validade_s=_int("MOTOR_FILA_VALIDADE_S", 300, 60, 3600),
        atend_por_hora=_float("MOTOR_ATEND_POR_HORA", 6.0, 0.1, 100.0),
        base_conversas_hora=_float("MOTOR_BASE_CONVERSAS_HORA", 28.0, 0.0, 10000.0),
        humanos_por_envio=_float("MOTOR_HUMANOS_POR_ENVIO", 0.025, 0.001, 1.0),
        exp_segura_por_mil=segura,
        exp_encerra_por_mil=_float("MOTOR_EXP_ENCERRA_POR_MIL", 15.0, segura, EXP_ENCERRA_ABSOLUTO),
        exp_min_casos=_int("MOTOR_EXP_MIN_CASOS", 5, 1, EXP_MIN_CASOS_ABSOLUTO),
        folga_baixa_du=_int("MOTOR_FOLGA_BAIXA_DU", 3, 0, 10),
        declarou_pagamento_du=_int("MOTOR_DECLAROU_PAGAMENTO_DU", 5, 1, 30),
        carga_min_abertos=_int("MOTOR_CARGA_MIN_ABERTOS", 8000, 0, 10_000_000),
        carga_hora=_int("MOTOR_CARGA_HORA", 7, 0, 23),
        plano_hora=_int("MOTOR_PLANO_HORA", 8, 0, 23),
        plano_minuto=_int("MOTOR_PLANO_MINUTO", 30, 0, 59),
        guardar_texto=_bool("MOTOR_GUARDAR_TEXTO", False),
        texto_dias=_int("MOTOR_TEXTO_DIAS", 30, 1, 30),
        tick_s=_int("MOTOR_TICK_S", 30, 15, 120),
        crm_rpm=rpm,
        crm_reserva_alta=_int("MOTOR_CRM_RESERVA_ALTA", 10, 1, max(1, rpm - 1)),
        feriados=parse_feriados(os.getenv("MOTOR_FERIADOS", "")),
    )
