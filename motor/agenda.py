"""Calendário do ciclo mensal: que mensagem sai em cada dia e para quais estados.

  dias 8 e 9     lembrete do dia 10 (tardios e sem histórico), aviso próprio do
                 calouro na 1ª mensalidade e reconhecimento de quem está em dia
  dias 22 e 24   lembrete do dia 25 para quem segue em aberto
  dias 26 a 30   conversa de recuperação (primeiro atraso) e mensagem firme
                 (não reage, uma vez por ciclo)

Dia de lembrete que cai em fim de semana ou feriado é antecipado para o dia
útil anterior. Mudar este calendário é decisão humana, registrada no AGENTS.md.
"""
from __future__ import annotations

from datetime import date

from motor.calendario import ciclo_de, data_do_ciclo, dia_util_ate, eh_dia_util
from motor.modelos import Acao, Estado, Frente, Mensagem

DIAS_LEMBRETE_DIA10 = (8, 9)
DIAS_LEMBRETE_DIA25 = (22, 24)
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


def datas_de_lembrete(ciclo: str, dias: tuple[int, ...], feriados: frozenset[date] = frozenset()) -> tuple[date, ...]:
    """Dias nominais do mês, antecipados para o dia útil anterior quando preciso."""
    datas = {dia_util_ate(data_do_ciclo(ciclo, d), feriados) for d in dias}
    return tuple(sorted(d for d in datas if ciclo_de(d) == ciclo))


def datas_de_recuperacao(ciclo: str, feriados: frozenset[date] = frozenset()) -> tuple[date, ...]:
    datas = {data_do_ciclo(ciclo, d) for d in DIAS_RECUPERACAO}
    return tuple(sorted(d for d in datas if d.day >= 26 and eh_dia_util(d, feriados)))


def acoes_do_dia(hoje: date, feriados: frozenset[date] = frozenset()) -> list[Acao]:
    """Ações previstas para hoje, já em ordem de prioridade. Lista vazia = dia sem envio."""
    if not eh_dia_util(hoje, feriados):
        return []
    ciclo = ciclo_de(hoje)
    acoes: list[Acao] = []
    if hoje in datas_de_lembrete(ciclo, DIAS_LEMBRETE_DIA10, feriados):
        acoes += [ACAO_AVISO_CALOURO, ACAO_LEMBRETE_DIA10, ACAO_RECONHECIMENTO]
    if hoje in datas_de_lembrete(ciclo, DIAS_LEMBRETE_DIA25, feriados):
        acoes.append(ACAO_LEMBRETE_DIA25)
    if hoje in datas_de_recuperacao(ciclo, feriados):
        acoes += [ACAO_RECUPERACAO, ACAO_FIRME]
    return sorted(acoes, key=lambda a: a.prioridade)


def proximos_dias_de_envio(desde: date, feriados: frozenset[date] = frozenset(), limite: int = 6) -> list[tuple[date, list[Acao]]]:
    """Próximos dias com alguma ação, para a página mostrar o que vem pela frente."""
    from datetime import timedelta

    out: list[tuple[date, list[Acao]]] = []
    d = desde
    for _ in range(62):
        acoes = acoes_do_dia(d, feriados)
        if acoes:
            out.append((d, acoes))
            if len(out) >= limite:
                break
        d += timedelta(days=1)
    return out
