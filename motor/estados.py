"""Estados do aluno, calculados do histórico de pagamento.

Classe de um ciclo (campo "Dia 10" do negócio, dado D-1):
  em dia     baixa até o corte do dia 10
  tardio     baixa depois do corte, dentro do ciclo
  não pagou  sem baixa no ciclo

A baixa chega de 1 a 3 dias úteis depois do pagamento. Por isso o corte do
"em dia" é o dia 10 mais uma folga em dias úteis (`folga_baixa_du`).

O aprendizado recomeça a cada semestre e para calouros: o histórico que entra
aqui já vem recortado no semestre do ciclo corrente.
"""
from __future__ import annotations

from datetime import date
from typing import Iterable, Mapping, Optional

from motor.calendario import data_do_ciclo, semestre_de, somar_dias_uteis
from motor.modelos import Classe, Estado


def corte_dia10(ciclo: str, folga_du: int, feriados: frozenset[date] = frozenset()) -> date:
    """Última data de baixa que ainda conta como pagamento até o dia 10."""
    return somar_dias_uteis(data_do_ciclo(ciclo, 10), folga_du, feriados)


def corte_dia25(ciclo: str, folga_du: int, feriados: frozenset[date] = frozenset()) -> date:
    return somar_dias_uteis(data_do_ciclo(ciclo, 25), folga_du, feriados)


def classificar_ciclo(
    ciclo: str,
    baixa_em: Optional[date],
    *,
    encerrado: bool,
    folga_du: int = 3,
    feriados: frozenset[date] = frozenset(),
) -> Classe:
    """Classe do ciclo a partir da data em que o Dia 10 mudou para Não."""
    if baixa_em is None:
        return Classe.NAO_PAGOU if encerrado else Classe.ABERTO
    if baixa_em <= corte_dia10(ciclo, folga_du, feriados):
        return Classe.EM_DIA
    return Classe.TARDIO


def historico_do_semestre(classes_por_ciclo: Mapping[str, Classe], ciclo_atual: str) -> tuple[Classe, ...]:
    """Ciclos fechados do mesmo semestre, do mais antigo ao mais recente."""
    semestre = semestre_de(ciclo_atual)
    ciclos = sorted(c for c in classes_por_ciclo if c < ciclo_atual and semestre_de(c) == semestre)
    return tuple(classes_por_ciclo[c] for c in ciclos if classes_por_ciclo[c] != Classe.ABERTO)


def _seguidos(historico: Iterable[Classe], classe: Classe) -> int:
    """Quantos ciclos mais recentes, em sequência, têm a classe dada."""
    n = 0
    for c in reversed(tuple(historico)):
        if c != classe:
            break
        n += 1
    return n


def calcular_estado(
    historico: Iterable[Classe],
    *,
    pediu_silencio: bool = False,
    lembretes_sem_reacao: int = 0,
) -> Estado:
    """Estado do aluno pelos ciclos fechados do semestre (mais antigo → mais recente).

    A ordem das regras importa: atraso vence aprendizado de bom pagador, e o
    pedido de silêncio só vale para quem não está em atraso.
    """
    hist = tuple(historico)
    sem_pagar = _seguidos(hist, Classe.NAO_PAGOU)
    if sem_pagar >= 2 or lembretes_sem_reacao >= 2:
        return Estado.NAO_REAGE
    if sem_pagar == 1:
        return Estado.PRIMEIRO_ATRASO
    em_dia = _seguidos(hist, Classe.EM_DIA)
    if pediu_silencio or em_dia >= 3:
        return Estado.SILENCIADO
    if em_dia == 2:
        return Estado.EM_DIA
    if hist and hist[-1] == Classe.TARDIO:
        return Estado.TARDIO
    return Estado.SEM_HISTORICO


def historico_com_ciclo_vencido(
    historico: Iterable[Classe],
    *,
    ciclo: str,
    hoje: date,
    financeira_aberta: bool,
    dia10_aberto: bool,
) -> tuple[Classe, ...]:
    """Depois do dia 25, o ciclo corrente sem baixa já conta como ciclo sem pagar.

    É o que faz o aluno entrar em Primeiro atraso (ou Não reage) na janela de
    recuperação dos dias 26 a 30, sem esperar a carga do mês seguinte.
    """
    hist = tuple(historico)
    if hoje > data_do_ciclo(ciclo, 25) and dia10_aberto and financeira_aberta:
        return hist + (Classe.NAO_PAGOU,)
    return hist
