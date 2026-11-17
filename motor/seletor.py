"""Seletor de público: quem está no alvo de cada ação e quem os trilhos tiram.

A lista é recalculada a cada lote, não uma vez por campanha: o lote das 11h já
usa as baixas importadas de manhã.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Iterable

from motor.config import MotorConfig
from motor.estados import calcular_estado, historico_com_ciclo_vencido
from motor.modelos import Acao, Aluno, Estado, Mensagem, Selecao
from motor.sorteio import no_controle, ordem_estavel
from motor.trilhos import motivo_exclusao


def com_estado(aluno: Aluno, *, ciclo: str, hoje: date, cfg: MotorConfig, controle_gravado: bool | None = None) -> Aluno:
    """Devolve o aluno com estado e sorteio do controle calculados para hoje.

    `controle_gravado` é o sorteio já registrado no livro-razão para o ciclo;
    quando existe, vale ele (trocar o sal não muda um ciclo aberto).
    """
    hist = historico_com_ciclo_vencido(
        aluno.historico,
        ciclo=ciclo,
        hoje=hoje,
        financeira_aberta=aluno.financeira_aberta,
        dia10_aberto=aluno.dia10_aberto,
        feriados=cfg.feriados,
    )
    estado = calcular_estado(
        hist,
        pediu_silencio=aluno.pediu_silencio,
        lembretes_sem_reacao=aluno.lembretes_sem_reacao,
    )
    controle = (
        controle_gravado
        if controle_gravado is not None
        else no_controle(aluno.contact_id, ciclo, cfg.controle_sal, cfg.controle_pct)
    )
    return replace(aluno, estado=estado, controle=controle)


def no_alvo(aluno: Aluno, acao: Acao) -> bool:
    """O aluno pertence ao público desta ação (antes das exclusões dos trilhos)?"""
    if aluno.estado not in acao.estados:
        return False
    m = acao.mensagem
    if m == Mensagem.AVISO_CALOURO:
        return aluno.primeira_mensalidade and aluno.dia10_aberto
    if m == Mensagem.LEMBRETE_DIA10:
        # Calouro na 1ª mensalidade nunca recebe o texto do dia 10.
        return aluno.dia10_aberto and not aluno.primeira_mensalidade
    if m == Mensagem.RECONHECIMENTO:
        return aluno.dia10_aberto
    if m == Mensagem.LEMBRETE_DIA25:
        # Inclui o silenciado que passou do dia 10 sem baixa.
        return aluno.dia10_aberto
    if m in (Mensagem.RECUPERACAO, Mensagem.FIRME):
        return aluno.financeira_aberta
    return False


def no_dia(aluno_id: str, ciclo: str, frente: str, parcela: float) -> bool:
    """O aluno cai na parcela do público que já pode receber hoje?

    Sorteio estável por aluno, ciclo e frente: quem entra com 1/3 continua
    dentro com 2/3 e com o público inteiro.
    """
    if parcela >= 1.0:
        return True
    return ordem_estavel(aluno_id, f"parcela|{ciclo}|{frente}") / float(1 << 64) < parcela


def risco(aluno: Aluno) -> float:
    """Prioridade na ordem dos lotes (maior sai primeiro). Dentro do mesmo risco a ordem é sorteada.

    Premissa registrada no AGENTS.md: o risco de não pagar vem do estado do
    aluno, e o calouro pesa mais dentro do mesmo estado (34,6% dos calouros
    não pagaram no ciclo de outubro, contra 15,4% dos veteranos).
    """
    base = {
        Estado.PRIMEIRO_ATRASO: 5.0, Estado.NAO_REAGE: 4.0, Estado.TARDIO: 3.0,
        Estado.SEM_HISTORICO: 2.0, Estado.EM_DIA: 1.0, Estado.SILENCIADO: 0.0,
    }[aluno.estado]
    return base + (0.5 if aluno.calouro else 0.0)


def selecionar(alunos: Iterable[Aluno], acao: Acao, *, hoje: date, cfg: MotorConfig, parcela: float = 1.0) -> Selecao:
    sel = Selecao(acao=acao)
    ciclo = f"{hoje.year:04d}-{hoje.month:02d}"
    for aluno in alunos:
        if not no_alvo(aluno, acao):
            sel.fora_do_alvo += 1
            continue
        motivo = motivo_exclusao(aluno, frente=acao.frente, mensagem=acao.mensagem, hoje=hoje, cfg=cfg)
        if not motivo and not no_dia(aluno.contact_id, ciclo, acao.frente.value, parcela):
            motivo = "fica_para_outro_dia"
        if motivo:
            sel.exclusoes[motivo] = sel.exclusoes.get(motivo, 0) + 1
        else:
            sel.elegiveis.append(aluno)
    return sel


def contar_por_estado(alunos: Iterable[Aluno]) -> dict[str, int]:
    contagem = {e.value: 0 for e in Estado}
    for a in alunos:
        contagem[a.estado.value] += 1
    return contagem
