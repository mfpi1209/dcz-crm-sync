"""Configuração do motor, lida do ambiente.

Os valores do ambiente nunca passam dos tetos absolutos definidos aqui: um
erro de digitação numa variável não vira um disparo de 100 mil mensagens.
Mudar limite é decisão humana (variável de ambiente no EasyPanel + registro
no AGENTS.md); o piloto não altera nada disto.
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
JANELA_MAIS_TARDE_H = 14
MAX_MSGS_CICLO_ABSOLUTO = 2
CRM_RPM_ABSOLUTO = 240


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


@dataclass(frozen=True)
class MotorConfig:
    # Entrada segura: desligado e em simulação até alguém mudar o ambiente.
    enabled: bool = False
    dry_run: bool = True

    # Janela de envio (dia útil, horário de Brasília) e volumes.
    janela_inicio_h: int = 9
    janela_fim_h: int = 14
    teto_diario: int = 7000
    lote_max: int = 1200
    lote_min: int = 100
    max_msgs_ciclo: int = 2

    # Grupo de controle.
    controle_pct: float = 5.0
    controle_sal: str = "motor-ativacoes-v1"

    # Governador de fila (limites provisórios até o usuário fechar os números).
    fila_teto: int = 60
    fila_critica: int = 100
    atendentes_min: int = 5
    sla_espera_min: int = 60
    falha_envio_max: float = 0.06
    falha_amostra_min: int = 50
    vida_min_lote_min: int = 30
    fila_validade_s: int = 300
    atend_por_hora: float = 8.0
    fator_atendimento: float = 0.04

    # Ciclo.
    folga_baixa_du: int = 3
    declarou_pagamento_du: int = 5
    carga_min_abertos: int = 8000
    carga_hora: int = 7
    plano_hora: int = 8
    plano_minuto: int = 30

    # Laço e API do CRM.
    tick_s: int = 90
    crm_rpm: int = 180
    feriados: frozenset[date] = field(default_factory=frozenset)

    @property
    def sla_espera_s(self) -> int:
        return self.sla_espera_min * 60

    def publico(self) -> dict:
        """Configuração exibível na página (sem sal do sorteio)."""
        d = {k: getattr(self, k) for k in self.__dataclass_fields__ if k not in ("controle_sal", "feriados")}
        d["feriados_extras"] = sorted(x.isoformat() for x in self.feriados)
        return d


def carregar() -> MotorConfig:
    inicio = _int("MOTOR_JANELA_INICIO_H", 9, JANELA_MAIS_CEDO_H, JANELA_MAIS_TARDE_H - 1)
    fim = _int("MOTOR_JANELA_FIM_H", 14, inicio + 1, JANELA_MAIS_TARDE_H)
    lote_max = _int("MOTOR_LOTE_MAX", 1200, 1, LOTE_ABSOLUTO)
    teto = _int("MOTOR_FILA_TETO", 60, 1, 10000)
    return MotorConfig(
        enabled=_bool("MOTOR_ENABLED", False),
        dry_run=_bool("MOTOR_DRY_RUN", True),
        janela_inicio_h=inicio,
        janela_fim_h=fim,
        teto_diario=_int("MOTOR_TETO_DIARIO", 7000, 0, TETO_DIARIO_ABSOLUTO),
        lote_max=lote_max,
        lote_min=_int("MOTOR_LOTE_MIN", 100, 1, lote_max),
        max_msgs_ciclo=_int("MOTOR_MAX_MSGS_CICLO", 2, 1, MAX_MSGS_CICLO_ABSOLUTO),
        controle_pct=_float("MOTOR_CONTROLE_PCT", 5.0, 0.0, 50.0),
        controle_sal=(os.getenv("MOTOR_CONTROLE_SAL") or "motor-ativacoes-v1").strip(),
        fila_teto=teto,
        fila_critica=_int("MOTOR_FILA_CRITICA", 100, teto, 100000),
        atendentes_min=_int("MOTOR_ATENDENTES_MIN", 5, 1, 1000),
        sla_espera_min=_int("MOTOR_SLA_ESPERA_MIN", 60, 1, 24 * 60),
        falha_envio_max=_float("MOTOR_FALHA_ENVIO_MAX", 0.06, 0.0, 1.0),
        falha_amostra_min=_int("MOTOR_FALHA_AMOSTRA_MIN", 50, 1, 100000),
        vida_min_lote_min=_int("MOTOR_VIDA_MIN_LOTE_MIN", 30, 5, 24 * 60),
        fila_validade_s=_int("MOTOR_FILA_VALIDADE_S", 300, 60, 3600),
        atend_por_hora=_float("MOTOR_ATEND_POR_HORA", 8.0, 0.1, 100.0),
        fator_atendimento=_float("MOTOR_FATOR_ATENDIMENTO", 0.04, 0.001, 1.0),
        folga_baixa_du=_int("MOTOR_FOLGA_BAIXA_DU", 3, 0, 10),
        declarou_pagamento_du=_int("MOTOR_DECLAROU_PAGAMENTO_DU", 5, 1, 30),
        carga_min_abertos=_int("MOTOR_CARGA_MIN_ABERTOS", 8000, 0, 10_000_000),
        carga_hora=_int("MOTOR_CARGA_HORA", 7, 0, 23),
        plano_hora=_int("MOTOR_PLANO_HORA", 8, 0, 23),
        plano_minuto=_int("MOTOR_PLANO_MINUTO", 30, 0, 59),
        tick_s=_int("MOTOR_TICK_S", 90, 30, 120),
        crm_rpm=_int("MOTOR_CRM_RPM", 180, 1, CRM_RPM_ABSOLUTO),
        feriados=parse_feriados(os.getenv("MOTOR_FERIADOS", "")),
    )
