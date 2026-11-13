"""Loteador: divide o público do dia em lotes, dentro do teto diário.

  - um lote tem uma única mensagem (uma campanha = um template)
  - as ações saem em ordem de prioridade; o aluno entra em no máximo um lote
    por dia (nunca duas campanhas no mesmo dia para o mesmo aluno)
  - dentro da ação a ordem é estável e sem viés por identificador
  - o que não cabe no teto diário fica para o próximo dia de envio
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Sequence

from motor.config import MotorConfig
from motor.modelos import Lote, Selecao
from motor.seletor import risco
from motor.sorteio import ordem_estavel


@dataclass
class PlanoLotes:
    lotes: list[Lote] = field(default_factory=list)
    adiados: int = 0          # elegíveis que não couberam no teto diário
    repetidos: int = 0        # já colocados hoje em lote de ação mais prioritária

    @property
    def total(self) -> int:
        return sum(l.tamanho for l in self.lotes)


def _tamanho_valido(tamanho: int, cfg: MotorConfig) -> int:
    return max(1, min(cfg.lote_max, tamanho))


def lotear(
    selecoes: Sequence[Selecao],
    *,
    dia: date,
    cfg: MotorConfig,
    tamanho: int,
    enviados_hoje: int = 0,
    primeira_ordem: int = 1,
    max_lotes: int | None = None,
    teto_do_lote: int | None = None,
) -> PlanoLotes:
    """Monta os lotes do dia a partir das seleções (uma por ação)."""
    tam = _tamanho_valido(tamanho, cfg)
    # Juntar o resto pequeno nunca pode passar do teto do lote (o da rampa do dia, quando houver).
    teto = min(cfg.lote_max, teto_do_lote) if teto_do_lote else cfg.lote_max
    plano = PlanoLotes()
    restante = max(0, cfg.teto_diario - enviados_hoje)
    ja_no_dia: set[str] = set()
    ordem = primeira_ordem

    for sel in sorted(selecoes, key=lambda s: s.acao.prioridade):
        chave = f"{dia.isoformat()}|{sel.acao.mensagem.value}"
        ids: list[str] = []
        # Maior risco primeiro; dentro do mesmo risco, ordem sorteada (quem fica de
        # fora pelo teto do dia é uma amostra ao acaso do seu grupo de risco).
        for aluno in sorted(sel.elegiveis, key=lambda a: (-risco(a), ordem_estavel(a.contact_id, chave), a.contact_id)):
            if aluno.contact_id in ja_no_dia:
                plano.repetidos += 1
                continue
            ja_no_dia.add(aluno.contact_id)
            ids.append(aluno.contact_id)

        cabem = ids[:restante]
        plano.adiados += len(ids) - len(cabem)
        restante -= len(cabem)

        pedacos = [cabem[i:i + tam] for i in range(0, len(cabem), tam)]
        # Resto pequeno junta com o lote anterior quando cabe no máximo.
        if len(pedacos) >= 2 and len(pedacos[-1]) < cfg.lote_min and len(pedacos[-2]) + len(pedacos[-1]) <= teto:
            resto = pedacos.pop()
            pedacos[-1] = pedacos[-1] + resto
        for pedaco in pedacos:
            if max_lotes is not None and len(plano.lotes) >= max_lotes:
                plano.adiados += len(pedaco)
                continue
            plano.lotes.append(Lote(ordem=ordem, frente=sel.acao.frente, mensagem=sel.acao.mensagem, contact_ids=tuple(pedaco)))
            ordem += 1
    return plano


def lotes_que_cabem_na_janela(cfg: MotorConfig) -> int:
    """Quantos lotes cabem numa manhã respeitando a vida mínima entre lotes."""
    minutos = (cfg.janela_fim_h - cfg.janela_inicio_h) * 60
    return max(1, minutos // cfg.vida_min_lote_min)
