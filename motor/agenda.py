"""Calendário do ciclo mensal: que mensagem sai em cada dia e para quais estados.

  3 dias úteis antes do dia 10   lembrete do dia 10 (tardios e sem histórico),
                                 aviso próprio do calouro na 1ª mensalidade e
                                 reconhecimento de quem está em dia
  3 dias úteis antes do dia 25   lembrete do dia 25 para quem segue em aberto
  dias 26 a 30                   conversa de recuperação (primeiro atraso) e
                                 mensagem firme (não reage, uma vez por ciclo)

O público dos lembretes é dividido pelos dias úteis antes do vencimento: no
primeiro dia entra um terço, no segundo dois terços (já com o que sobrou do
primeiro) e no último todos. Quem cai em cada dia é sorteio estável por aluno.
Mudar este calendário é decisão humana, registrada no AGENTS.md.
"""
from __future__ import annotations

from datetime import date, timedelta

from motor.calendario import ciclo_de, data_do_ciclo, eh_dia_util, somar_dias_uteis
from motor.modelos import Acao, Estado, Frente, Mensagem

DIA_VENCIMENTO = {Frente.DIA10: 10, Frente.DIA25: 25}
DIAS_RECUPERACAO = (26, 27, 28, 29, 30)

# Menor prioridade sai primeiro quando as frentes disputam a mesma fila.
ACAO_RECUPERACAO = Acao(Frente.RECUPERACAO, Mensagem.RECUPERACAO, frozenset({Estado.PRIMEIRO_ATRASO}), 10)
ACAO_AVISO_CALOURO = Acao(Frente.DIA10, Mensagem.AVISO_CALOURO, frozenset({Estado.SEM_HISTORICO}), 20)
ACAO_LEMBRETE_DIA25 = Acao(
    Frente.DIA25,
    Mensagem.LEMBRETE_DIA25,
    frozenset({Estado.SEM_HISTORICO, Estado.TARDIO, Estado.EM_DIA, Estado.SILENCIADO}),
    20,
)
ACAO_LEMBRETE_DIA10 = Acao(
    Frente.DIA10, Mensagem.LEMBRETE_DIA10, frozenset({Estado.TARDIO, Estado.SEM_HISTORICO}), 30
)
ACAO_FIRME = Acao(Frente.RECUPERACAO, Mensagem.FIRME, frozenset({Estado.NAO_REAGE}), 40)
ACAO_RECONHECIMENTO = Acao(Frente.DIA10, Mensagem.RECONHECIMENTO, frozenset({Estado.EM_DIA}), 50)


def datas_de_lembrete(ciclo: str, frente: Frente, n_dias: int = 3, feriados: frozenset[date] = frozenset()) -> tuple[date, ...]:
    """Os `n_dias` dias úteis imediatamente antes do vencimento da frente, em ordem."""
    vencimento = data_do_ciclo(ciclo, DIA_VENCIMENTO[frente])
    datas = [somar_dias_uteis(vencimento, -k, feriados) for k in range(1, n_dias + 1)]
    return tuple(sorted(d for d in datas if ciclo_de(d) == ciclo))


def datas_de_recuperacao(ciclo: str, feriados: frozenset[date] = frozenset()) -> tuple[date, ...]:
    datas = {data_do_ciclo(ciclo, d) for d in DIAS_RECUPERACAO}
    return tuple(sorted(d for d in datas if d.day >= 26 and eh_dia_util(d, feriados)))


def acoes_do_dia(hoje: date, feriados: frozenset[date] = frozenset(), n_dias: int = 3) -> list[Acao]:
    """Ações previstas para hoje, já em ordem de prioridade. Lista vazia = dia sem envio."""
    if not eh_dia_util(hoje, feriados):
        return []
    ciclo = ciclo_de(hoje)
    acoes: list[Acao] = []
    if hoje in datas_de_lembrete(ciclo, Frente.DIA10, n_dias, feriados):
        acoes += [ACAO_AVISO_CALOURO, ACAO_LEMBRETE_DIA10, ACAO_RECONHECIMENTO]
    if hoje in datas_de_lembrete(ciclo, Frente.DIA25, n_dias, feriados):
        acoes.append(ACAO_LEMBRETE_DIA25)
    if hoje in datas_de_recuperacao(ciclo, feriados):
        acoes += [ACAO_RECUPERACAO, ACAO_FIRME]
    return sorted(acoes, key=lambda a: a.prioridade)


def parcela_do_dia(hoje: date, frente: Frente, feriados: frozenset[date] = frozenset(), n_dias: int = 3) -> float:
    """Fração acumulada do público da frente que já pode receber até hoje (1/3, 2/3, 1)."""
    if frente not in DIA_VENCIMENTO:
        return 1.0
    datas = datas_de_lembrete(ciclo_de(hoje), frente, n_dias, feriados)
    if hoje not in datas:
        return 1.0
    return (datas.index(hoje) + 1) / len(datas)


def proximos_dias_de_envio(desde: date, feriados: frozenset[date] = frozenset(), limite: int = 6, n_dias: int = 3) -> list[tuple[date, list[Acao]]]:
    """Próximos dias com alguma ação, para a página mostrar o que vem pela frente."""
    out: list[tuple[date, list[Acao]]] = []
    d = desde
    for _ in range(62):
        acoes = acoes_do_dia(d, feriados, n_dias)
        if acoes:
            out.append((d, acoes))
            if len(out) >= limite:
                break
        d += timedelta(days=1)
    return out
